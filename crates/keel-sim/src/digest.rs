use keel_raft::{Entry, EntryPayload, Index, RaftLog, Term};

/// An entry whose cumulative digest is new or different since the last sync.
pub type ChangedEntry = (Index, Term, u64);
/// An entry a node discarded, with the cumulative digest it used to have.
pub type DiscardedEntry = (Index, u64);

fn fnv(mut h: u64, bytes: &[u8]) -> u64 {
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}

/// A running hash of a log prefix.
///
/// The chaining is what makes the invariant checks cheap: if two nodes agree on
/// the cumulative digest at some index, they agree on *every* entry below it, so
/// Log Matching and Leader Completeness reduce to a single comparison instead of
/// a walk. Without it, checking after every event would cost O(log length) and
/// the simulator could not run millions of steps.
fn chain(prev: u64, entry: &Entry) -> u64 {
    let mut h = fnv(prev ^ 0xCBF2_9CE4_8422_2325, &entry.term.to_le_bytes());
    h = fnv(h, &entry.index.to_le_bytes());
    match &entry.payload {
        EntryPayload::Noop => fnv(h, b"noop"),
        EntryPayload::Normal(data) => fnv(fnv(h, b"normal"), data),
        EntryPayload::ConfChange(cc) => fnv(fnv(h, b"conf"), format!("{cc:?}").as_bytes()),
    }
}

/// Cumulative digests for one node's log, maintained incrementally.
#[derive(Debug, Clone)]
pub struct LogDigest {
    /// Index of `base_digest`; everything at or below it has been compacted away.
    base_index: Index,
    base_digest: u64,
    /// `entries[i]` describes index `base_index + 1 + i`.
    entries: Vec<(Term, u64)>,
    /// Set when the log's floor moved above everything this digest holds and no
    /// digest for that floor was supplied. Read by the caller, which turns it
    /// into a violation: continuing would compare a made-up number against real
    /// ones.
    floor_without_a_digest: Option<Index>,
}

impl LogDigest {
    pub fn new() -> Self {
        Self {
            base_index: 0,
            base_digest: 0,
            entries: Vec::new(),
            floor_without_a_digest: None,
        }
    }

    /// A digest that starts above zero, at a floor whose digest is known.
    ///
    /// This is what a node holds after a restart from a log that has been
    /// compacted: the entries below the snapshot are gone, so their cumulative
    /// hash cannot be recomputed — it has to be carried.
    ///
    /// Starting such a node at `(snapshot_index, 0)` instead is the shape of a
    /// false State Machine Safety violation, and it fires on *correct* code:
    /// its peers, which never restarted, hold the real cumulative hash at that
    /// index, and the oracle compares the two and finds them different. Nothing
    /// is wrong except the bookkeeping.
    pub fn rebased(base_index: Index, base_digest: u64) -> Self {
        Self {
            base_index,
            base_digest,
            entries: Vec::new(),
            floor_without_a_digest: None,
        }
    }

    /// Adopt a snapshot installed from a leader: the floor and the digest there.
    ///
    /// Unused until a profile installs one, which is P16's second half. Landed
    /// with the rebase because it is the same fix seen from the other side: a
    /// floor arrives, and its digest arrives with it rather than being guessed.
    #[allow(dead_code)]
    pub fn adopt_snapshot(&mut self, index: Index, digest: u64) {
        if index < self.base_index {
            return;
        }
        self.entries.clear();
        self.base_index = index;
        self.base_digest = digest;
        self.floor_without_a_digest = None;
    }

    /// The floor this digest could not account for, if there is one.
    pub fn floor_without_a_digest(&self) -> Option<Index> {
        self.floor_without_a_digest
    }

    /// The digest at the floor: what a node carries across a restart, and what
    /// a snapshot carries to a follower installing it.
    pub fn base(&self) -> (Index, u64) {
        (self.base_index, self.base_digest)
    }

    pub fn last_index(&self) -> Index {
        self.base_index + self.entries.len() as Index
    }

    pub fn at(&self, index: Index) -> Option<(Term, u64)> {
        if index == self.base_index {
            return Some((0, self.base_digest));
        }
        if index <= self.base_index || index > self.last_index() {
            return None;
        }
        self.entries
            .get((index - self.base_index - 1) as usize)
            .copied()
    }

    /// Bring the digests in line with the log, touching only what changed.
    /// Returns the indices whose digest is new or different, for the caller to
    /// check against the global picture, and every `(index, digest)` the log
    /// discarded. Overwriting history is the event the safety properties are
    /// ultimately about, so the discarded content is reported rather than
    /// inferred: discarding a *divergent* entry is healthy Raft, and only
    /// discarding one that was actually committed is a violation.
    pub fn sync(&mut self, log: &RaftLog) -> (Vec<ChangedEntry>, Vec<DiscardedEntry>) {
        // A snapshot may have moved the floor up.
        let snap = log.snapshot_index();
        if snap > self.base_index {
            match self.at(snap) {
                Some((_, digest)) => {
                    let drop = (snap - self.base_index) as usize;
                    self.entries.drain(..drop.min(self.entries.len()));
                    self.base_digest = digest;
                    self.base_index = snap;
                }
                // The floor jumped past everything this digest holds, so the
                // cumulative hash there cannot be computed from what is left.
                //
                // It is not zero, and pretending it is was the bug this branch
                // used to have: a node whose floor moved that way would compare
                // as different from every peer that had the entries, on
                // perfectly correct code. The digest has to be *carried* — by
                // `rebased` across a restart, by `adopt_snapshot` on an install —
                // and a caller that has not done so is told rather than quietly
                // given a wrong answer.
                None => {
                    self.floor_without_a_digest = Some(snap);
                }
            }
        }

        // Drop anything above the log's end, then anything the log rewrote.
        // A rewrite always replaces a suffix, so walking back from the end
        // costs only what actually changed.
        let mut discarded: Vec<DiscardedEntry> = Vec::new();
        while self.last_index() > log.last_index() {
            let idx = self.last_index();
            if let Some((_, d)) = self.at(idx) {
                discarded.push((idx, d));
            }
            self.entries.pop();
        }
        while self.last_index() > self.base_index {
            let idx = self.last_index();
            let (recorded_term, recorded_digest) = self.entries[self.entries.len() - 1];
            if log.term(idx) == Some(recorded_term) {
                break;
            }
            discarded.push((idx, recorded_digest));
            self.entries.pop();
        }

        let mut changed = Vec::new();
        let mut prev = self
            .at(self.last_index())
            .map_or(self.base_digest, |(_, d)| d);
        for idx in self.last_index() + 1..=log.last_index() {
            let Some(entry) = log.entry(idx) else { break };
            prev = chain(prev, entry);
            self.entries.push((entry.term, prev));
            changed.push((idx, entry.term, prev));
        }
        (changed, discarded)
    }
}

impl Default for LogDigest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod rebase_tests {
    use super::*;

    /// The property P16 turns on, and the reason its order is fixed.
    ///
    /// A node whose log has been compacted holds a digest that starts at the
    /// floor. It must agree, at every index above that floor, with a node that
    /// never compacted and holds the whole thing. Starting the compacted one at
    /// `(floor, 0)` instead makes them disagree everywhere — a State Machine
    /// Safety violation reported on correct code, on every node that ever
    /// restarts after a snapshot.
    #[test]
    fn a_rebased_digest_agrees_with_one_that_was_never_compacted() {
        let entries: Vec<Entry> = (1..=20)
            .map(|i| Entry::new(1, i, EntryPayload::Normal(vec![i as u8; 4].into())))
            .collect();

        let mut whole = LogDigest::new();
        let full_log = RaftLog::restore(None, entries.clone(), 20);
        whole.sync(&full_log);

        // The floor, and the digest there — the pair a real node carries in its
        // snapshot metadata.
        let (floor, floor_digest) = (10, whole.at(10).expect("a digest at the floor").1);

        let mut rebased = LogDigest::rebased(floor, floor_digest);
        let compacted = RaftLog::restore(Some((floor, 1)), entries[floor as usize..].to_vec(), 20);
        rebased.sync(&compacted);

        assert_eq!(rebased.base(), (floor, floor_digest));
        for index in floor + 1..=20 {
            assert_eq!(
                rebased.at(index),
                whole.at(index),
                "the two disagree at index {index}, so a node that restarted after \
                 a snapshot would be reported as violating State Machine Safety"
            );
        }
    }

    /// And the failure it replaces: a floor nobody carried is refused rather
    /// than answered with a made-up number.
    #[test]
    fn a_floor_nobody_carried_is_reported_rather_than_invented() {
        let entries: Vec<Entry> = (11..=20)
            .map(|i| Entry::new(1, i, EntryPayload::Normal(vec![i as u8; 4].into())))
            .collect();
        let compacted = RaftLog::restore(Some((10, 1)), entries, 20);

        // Started at zero, as a node that forgot to carry its floor would be.
        let mut orphaned = LogDigest::new();
        orphaned.sync(&compacted);
        assert_eq!(
            orphaned.floor_without_a_digest(),
            Some(10),
            "a digest that could not account for its floor said nothing about it"
        );

        // Carried properly, it says nothing is wrong.
        let mut carried = LogDigest::rebased(10, 0xabcd);
        carried.sync(&compacted);
        assert_eq!(carried.floor_without_a_digest(), None);
    }

    /// Installing a snapshot adopts the floor and the digest together.
    #[test]
    fn adopting_a_snapshot_takes_the_digest_with_the_floor() {
        let mut digest = LogDigest::new();
        digest.adopt_snapshot(50, 0x1234_5678);
        assert_eq!(digest.base(), (50, 0x1234_5678));
        assert_eq!(digest.at(50), Some((0, 0x1234_5678)));
        // A snapshot behind the floor is ignored rather than pulling it back.
        digest.adopt_snapshot(10, 0);
        assert_eq!(digest.base(), (50, 0x1234_5678));
    }
}
