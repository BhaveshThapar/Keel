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
    /// Returns every `(index, digest)` this discards, which — and this is the
    /// whole of [KEEL-13](../../../BUGS.md) — is *not* everything above the
    /// snapshot's index.
    ///
    /// An install is one of two things and the caller cannot tell them apart by
    /// looking at the meta alone. `RaftLog::restore_snapshot` decides by asking
    /// whether the log holds `index` at the snapshot's term:
    ///
    /// **It compacts.** The prefixes already agree, so any entries the node
    /// holds above the snapshot survive the install *and their digests do not
    /// move* — they were chained from this very value already. Nothing is
    /// discarded, and reporting them as discarded says a committed entry went
    /// missing when nothing went anywhere. That is what the check saw, and it
    /// is why `snapshot-hunt` could not sweep clean.
    ///
    /// **It replaces.** The node's history above the floor is gone, so those
    /// entries are discarded and their old digests go through the check that
    /// knows the difference: discarding a divergent entry is healthy Raft, and
    /// discarding one that was actually committed is the violation.
    ///
    /// The test here is the digest rather than the term, which is the same test
    /// and a stronger one: two logs agreeing at `(index, term)` agree on the
    /// whole prefix, which is exactly what the cumulative digest encodes.
    pub fn adopt_snapshot(&mut self, index: Index, digest: u64) -> Vec<DiscardedEntry> {
        if index < self.base_index {
            return Vec::new();
        }
        if self.at(index).is_some_and(|(_, held)| held == digest) {
            // A compaction. The floor moves; nothing above it does.
            let drop = (index - self.base_index) as usize;
            self.entries.drain(..drop.min(self.entries.len()));
            self.base_index = index;
            self.base_digest = digest;
            self.floor_without_a_digest = None;
            return Vec::new();
        }
        // A replacement. Only what the snapshot does *not* cover: entries
        // between the old floor and the snapshot's index are subsumed by it,
        // not lost, and the snapshot is exactly what preserves them.
        let discarded: Vec<DiscardedEntry> = (index + 1..=self.last_index())
            .filter_map(|i| self.at(i).map(|(_, d)| (i, d)))
            .collect();
        self.entries.clear();
        self.base_index = index;
        self.base_digest = digest;
        self.floor_without_a_digest = None;
        discarded
    }

    /// The floor this digest could not account for, if there is one.
    pub fn floor_without_a_digest(&self) -> Option<Index> {
        self.floor_without_a_digest
    }

    /// The digest at the floor: where this chain starts, and the value every
    /// index above it is chained from.
    ///
    /// Read by the tests rather than by the simulator. What a node carries
    /// across a restart is its *checkpoint's* index and digest, which are one
    /// object — the simulator kept a second copy of the pair here and the two
    /// drifted apart, which is [KEEL-18](../../../BUGS.md).
    #[cfg(test)]
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

        // A floor above the log's own end, which is a state the digest cannot
        // describe and must not try to.
        //
        // The loop below shrinks the digest to the log by popping entries, and
        // it cannot shrink past `base_index` — there is nothing under it to pop.
        // Written without this guard it spun there, pushing the same index into
        // `discarded` until the process was killed, which is how it was found:
        // one seed of `snapshot-hunt` at five nodes taking sixty gigabytes.
        //
        // Reported rather than repaired. The digest's base comes from what the
        // harness carried across a restart, and a log that does not reach it
        // means the two disagree about where this node's history starts —
        // comparing anything from here would compare invented numbers, which is
        // the same failure `floor_without_a_digest` exists to refuse.
        if self.base_index > log.last_index() {
            self.floor_without_a_digest = Some(self.base_index);
            return (Vec::new(), Vec::new());
        }

        // Drop anything above the log's end, then anything the log rewrote.
        // A rewrite always replaces a suffix, so walking back from the end
        // costs only what actually changed.
        let mut discarded: Vec<DiscardedEntry> = Vec::new();
        while self.last_index() > log.last_index() && !self.entries.is_empty() {
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

    /// [KEEL-13](../../../BUGS.md). An install that only compacts discards
    /// nothing, and the entries above the new floor keep the digests they had.
    #[test]
    fn adopting_a_snapshot_the_log_already_agrees_with_discards_nothing() {
        let entries: Vec<Entry> = (1..=20)
            .map(|i| Entry::new(1, i, EntryPayload::Normal(vec![i as u8; 4].into())))
            .collect();
        let mut digest = LogDigest::new();
        digest.sync(&RaftLog::restore(None, entries, 20));
        let before: Vec<Option<(Term, u64)>> = (11..=20).map(|i| digest.at(i)).collect();

        // The snapshot's digest at index 10 is the one this node already holds
        // there, which is what an install that compacts rather than replaces
        // means.
        let (floor, floor_digest) = (10, digest.at(10).expect("a digest at 10").1);
        let discarded = digest.adopt_snapshot(floor, floor_digest);

        assert!(
            discarded.is_empty(),
            "an install that only compacted reported {} entries as discarded, and              the committed ones among them read as lost",
            discarded.len()
        );
        assert_eq!(digest.base(), (floor, floor_digest));
        let after: Vec<Option<(Term, u64)>> = (11..=20).map(|i| digest.at(i)).collect();
        assert_eq!(
            after, before,
            "a compaction moved the digests above the floor"
        );
    }

    /// And an install that replaces history still reports what it replaced, so
    /// a committed entry going missing is still caught.
    #[test]
    fn adopting_a_snapshot_that_replaces_history_reports_what_it_replaced() {
        let entries: Vec<Entry> = (1..=20)
            .map(|i| Entry::new(1, i, EntryPayload::Normal(vec![i as u8; 4].into())))
            .collect();
        let mut digest = LogDigest::new();
        digest.sync(&RaftLog::restore(None, entries, 20));

        // A digest at index 10 that is not the one this node holds there: the
        // leader's history and this node's have diverged below the floor.
        let discarded = digest.adopt_snapshot(10, 0xfeed_face_dead_beef);
        assert_eq!(
            discarded.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            (11..=20).collect::<Vec<_>>()
        );
        assert_eq!(digest.base(), (10, 0xfeed_face_dead_beef));
        assert_eq!(digest.last_index(), 10);
    }

    /// Installing a snapshot adopts the floor and the digest together.
    #[test]
    fn adopting_a_snapshot_takes_the_digest_with_the_floor() {
        let mut digest = LogDigest::new();
        assert!(digest.adopt_snapshot(50, 0x1234_5678).is_empty());
        assert_eq!(digest.base(), (50, 0x1234_5678));
        assert_eq!(digest.at(50), Some((0, 0x1234_5678)));
        // A snapshot behind the floor is ignored rather than pulling it back.
        assert!(digest.adopt_snapshot(10, 0).is_empty());
        assert_eq!(digest.base(), (50, 0x1234_5678));
    }
}
