use std::collections::BTreeMap;

use keel_api::{Proposal, ProposalBody, decode};
use keel_raft::{Entry, EntryPayload, Index, NodeId, Term};
use keel_sm::{MemStore, StateMachine};

use crate::digest::{ChangedEntry, DiscardedEntry, LogDigest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub property: &'static str,
    pub detail: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.property, self.detail)
    }
}

/// The global picture no single node can see, against which every node's local
/// state is checked after every event.
///
/// Each of the five Raft safety properties reduces to a digest comparison here,
/// which is what makes checking after *every* event affordable.
pub struct Oracle {
    /// Election Safety: at most one leader per term.
    leaders: BTreeMap<Term, NodeId>,
    /// Leader Append-Only: the furthest a node's log reached while leading.
    leader_high_water: BTreeMap<(NodeId, Term), Index>,
    /// Log Matching: the cumulative digest anyone has ever had at `(index, term)`.
    log_digests: BTreeMap<(Index, Term), u64>,
    /// Every index that has ever been committed, and its cumulative digest.
    committed: BTreeMap<Index, (Term, u64)>,
    /// Every index that has ever been applied, and its cumulative digest.
    applied: BTreeMap<Index, (Term, u64)>,
    /// The state machine digest anyone has held at a given applied index, and
    /// which node held it. Not the log's digest: this is what applying those
    /// entries produced.
    applied_state: BTreeMap<Index, (NodeId, u64)>,
    /// A reference state machine, fed the committed log in index order.
    ///
    /// This is the model in "model oracle". Comparing nodes against each other
    /// catches a divergence between them and nothing else: five nodes that all
    /// double-apply the same entry agree perfectly. Comparing against a machine
    /// that applied the same entries exactly once, in order, with no crashes and
    /// no restarts, catches the case where they are all wrong together.
    model: StateMachine<MemStore>,
    /// Committed entries waiting for their predecessors, so the model applies in
    /// index order however the cluster discovered them.
    model_pending: BTreeMap<Index, Entry>,
    /// What the model held after applying through each index.
    model_digests: BTreeMap<Index, u64>,
    pub max_committed: Index,
    pub max_applied: Index,
    /// Entries the model has applied. Zero after a run means the model saw
    /// nothing and every comparison against it was vacuous.
    pub model_applied: u64,
}

impl Default for Oracle {
    fn default() -> Self {
        Self::new()
    }
}

impl Oracle {
    pub fn new() -> Self {
        Self {
            leaders: BTreeMap::new(),
            leader_high_water: BTreeMap::new(),
            log_digests: BTreeMap::new(),
            committed: BTreeMap::new(),
            applied: BTreeMap::new(),
            applied_state: BTreeMap::new(),
            model: StateMachine::new(MemStore::new()),
            model_pending: BTreeMap::new(),
            model_digests: BTreeMap::new(),
            max_committed: 0,
            max_applied: 0,
            model_applied: 0,
        }
    }

    pub fn terms_with_leaders(&self) -> u64 {
        self.leaders.len() as u64
    }

    /// Election Safety.
    pub fn observe_leader(&mut self, id: NodeId, term: Term) -> Option<Violation> {
        match self.leaders.get(&term) {
            Some(existing) if *existing != id => Some(Violation {
                property: "Election Safety",
                detail: format!("nodes {existing} and {id} were both leader in term {term}"),
            }),
            _ => {
                self.leaders.insert(term, id);
                None
            }
        }
    }

    /// Leader Append-Only: a leader's log only ever grows while it leads.
    pub fn observe_leader_log(
        &mut self,
        id: NodeId,
        term: Term,
        last_index: Index,
    ) -> Option<Violation> {
        let key = (id, term);
        let previous = self.leader_high_water.get(&key).copied().unwrap_or(0);
        if last_index < previous {
            return Some(Violation {
                property: "Leader Append-Only",
                detail: format!(
                    "node {id} led term {term} with last index {previous}, then shrank to {last_index}"
                ),
            });
        }
        self.leader_high_water.insert(key, last_index);
        None
    }

    /// Log Matching: two entries with the same index and term have identical
    /// prefixes, which the cumulative digest encodes directly.
    pub fn observe_entries(&mut self, id: NodeId, changed: &[ChangedEntry]) -> Option<Violation> {
        for (index, term, digest) in changed {
            match self.log_digests.get(&(*index, *term)) {
                Some(known) if known != digest => {
                    return Some(Violation {
                        property: "Log Matching",
                        detail: format!(
                            "node {id} has a different prefix at index {index} term {term} \
                             than another node did (digest {digest:x} vs {known:x})"
                        ),
                    });
                }
                _ => {
                    self.log_digests.insert((*index, *term), *digest);
                }
            }
        }
        None
    }

    /// A committed entry is never lost and never changes.
    pub fn observe_commit(
        &mut self,
        id: NodeId,
        commit: Index,
        digest: &LogDigest,
    ) -> Option<Violation> {
        if let Some(v) = self.check_against(
            id,
            commit,
            digest,
            &self.committed,
            "no committed entry lost",
        ) {
            return Some(v);
        }
        if let Some(entry) = digest.at(commit)
            && commit > 0
        {
            self.committed.insert(commit, entry);
            self.max_committed = self.max_committed.max(commit);
        }
        None
    }

    /// State Machine Safety: no two nodes apply different entries at the same
    /// index.
    /// Feed a committed entry to the model.
    ///
    /// Buffered until its predecessors have arrived, because the cluster
    /// discovers commitment in whatever order its fsyncs complete and the model
    /// must apply in index order — that being the whole of what it models.
    ///
    /// An entry that will not decode is not reported here. `apply_entry` in the
    /// world already treats that as a violation at the node that met it, which
    /// names the node; the model would only say it a second time.
    pub fn observe_committed_entry(&mut self, entry: &Entry) {
        if entry.index <= self.model.applied() {
            return;
        }
        self.model_pending.insert(entry.index, entry.clone());
        while let Some(entry) = self.model_pending.remove(&(self.model.applied() + 1)) {
            let proposal = match &entry.payload {
                EntryPayload::Noop | EntryPayload::ConfChange(_) => Proposal {
                    stamped_ms: 0,
                    session: None,
                    body: ProposalBody::KeepAlive,
                },
                EntryPayload::Normal(data) => match decode::<Proposal>(data) {
                    Ok(proposal) => proposal,
                    Err(_) => return,
                },
            };
            if self.model.apply(entry.index, &proposal).is_err() {
                return;
            }
            self.model_applied += 1;
            self.model_digests
                .insert(entry.index, state_digest(&self.model));
        }
    }

    /// A node that has applied through `index` must hold what the model holds
    /// there.
    ///
    /// The model applied the same entries, in order, exactly once, with no
    /// crashes and no restarts. Any difference is the cluster's.
    pub fn check_against_model(&self, id: NodeId, applied: Index, state: u64) -> Option<Violation> {
        // The model only knows indexes it has been told about *and* whose
        // predecessors arrived. A node ahead of the model is not wrong; it has
        // simply seen a commitment the oracle has not been shown yet.
        let expected = self.model_digests.get(&applied)?;
        if *expected == state {
            return None;
        }
        Some(Violation {
            property: "State Machine Safety",
            detail: format!(
                "node {id} applied through index {applied} and holds {state:016x}; a \
                 reference state machine fed the same committed entries in order, once \
                 each, holds {expected:016x}. The cluster and the model disagree about \
                 what this log means"
            ),
        })
    }

    /// Two nodes that have applied to the same index must hold the same state.
    ///
    /// The log-prefix check below says they applied the same *entries*. This
    /// says applying them produced the same *result*, which is a different
    /// claim and the one State Machine Safety is actually about: a session
    /// table that deduplicated on one node and not on another, or an entry that
    /// applied twice somewhere, agrees on every log digest and disagrees here.
    ///
    /// Keyed on the applied index, so nodes are compared only where they are
    /// comparable. A node that has applied further is not wrong for holding
    /// more.
    pub fn observe_applied_state(
        &mut self,
        id: NodeId,
        applied: Index,
        state: u64,
    ) -> Option<Violation> {
        if applied == 0 {
            return None;
        }
        match self.applied_state.get(&applied) {
            Some((other, expected)) if *expected != state => Some(Violation {
                property: "State Machine Safety",
                detail: format!(
                    "nodes {other} and {id} both applied through index {applied} and hold \
                     different state: {expected:016x} against {state:016x}. They agree about \
                     which entries they applied, so applying them produced different results"
                ),
            }),
            Some(_) => None,
            None => {
                self.applied_state.insert(applied, (id, state));
                None
            }
        }
    }

    pub fn observe_applied(
        &mut self,
        id: NodeId,
        applied: Index,
        digest: &LogDigest,
    ) -> Option<Violation> {
        if let Some(v) =
            self.check_against(id, applied, digest, &self.applied, "State Machine Safety")
        {
            return Some(v);
        }
        if let Some(entry) = digest.at(applied)
            && applied > 0
        {
            self.applied.insert(applied, entry);
            self.max_applied = self.max_applied.max(applied);
        }
        None
    }

    /// A node discarded log entries. If any of them was the entry that had
    /// actually been committed at that index, a committed entry has been lost.
    ///
    /// This is checked at the moment of the rewrite rather than inferred later
    /// from commit indices, because a node can overwrite a low index and then
    /// commit a high one whose digest nothing has on record — in which case the
    /// loss leaves no trace to compare against.
    ///
    /// The digest comparison is what makes the check precise. A follower
    /// discarding a *divergent* tail is a leader correcting it, which is exactly
    /// what is supposed to happen; only discarding the committed content is a
    /// violation.
    pub fn check_rewrite(&self, id: NodeId, discarded: &[DiscardedEntry]) -> Option<Violation> {
        for (index, old_digest) in discarded {
            if self
                .committed
                .get(index)
                .is_some_and(|(_, d)| d == old_digest)
            {
                return Some(Violation {
                    property: "no committed entry lost",
                    detail: format!(
                        "node {id} discarded index {index}, which had already been committed"
                    ),
                });
            }
        }
        None
    }

    /// Leader Completeness: a new leader holds every entry committed in an
    /// earlier term.
    pub fn check_leader_completeness(
        &self,
        id: NodeId,
        term: Term,
        digest: &LogDigest,
    ) -> Option<Violation> {
        let (&index, &(known_term, known_digest)) = self.committed.iter().next_back()?;
        match digest.at(index) {
            Some((_, d)) if d != known_digest => Some(Violation {
                property: "Leader Completeness",
                detail: format!(
                    "node {id} became leader in term {term} with a different prefix at \
                     committed index {index} (term {known_term})"
                ),
            }),
            // A log that does not reach the committed index at all is the same
            // failure wearing a different hat: the entry is simply missing.
            None if digest.last_index() < index => Some(Violation {
                property: "Leader Completeness",
                detail: format!(
                    "node {id} became leader in term {term} with last index {} but \
                     index {index} is already committed",
                    digest.last_index()
                ),
            }),
            _ => None,
        }
    }

    /// Compare a node's digest against the recorded global one at the highest
    /// recorded index the node can actually speak to.
    fn check_against(
        &self,
        id: NodeId,
        upto: Index,
        digest: &LogDigest,
        known: &BTreeMap<Index, (Term, u64)>,
        property: &'static str,
    ) -> Option<Violation> {
        let (&index, &(term, expected)) = known.range(..=upto).next_back()?;
        let (_, actual) = digest.at(index)?;
        if actual != expected {
            return Some(Violation {
                property,
                detail: format!(
                    "node {id} disagrees at index {index} (term {term}): \
                     digest {actual:x}, previously {expected:x}"
                ),
            });
        }
        None
    }
}

/// A hash of everything a state machine holds.
///
/// Shared with the world's own digest of a node, because the two are compared
/// against each other and a second implementation would only be a second thing
/// that could be wrong.
pub(crate) fn state_digest(sm: &StateMachine<MemStore>) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    let mut mix = |bytes: &[u8]| {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    if let Ok(rows) = sm.scan(None, None, usize::MAX) {
        for (key, value) in rows {
            mix(&key);
            mix(&value);
        }
    }
    if let Ok(clients) = sm.open_sessions() {
        for client in clients {
            mix(&client.to_be_bytes());
            if let Ok(Some(seq)) = sm.last_seq(client) {
                mix(&seq.to_be_bytes());
            }
        }
    }
    hash
}
