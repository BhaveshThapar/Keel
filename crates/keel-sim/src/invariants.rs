use std::collections::BTreeMap;

use keel_raft::{Index, NodeId, Term};

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
#[derive(Debug, Default)]
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
    pub max_committed: Index,
    pub max_applied: Index,
}

impl Oracle {
    pub fn new() -> Self {
        Self::default()
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
