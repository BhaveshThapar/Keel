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
}

impl LogDigest {
    pub fn new() -> Self {
        Self {
            base_index: 0,
            base_digest: 0,
            entries: Vec::new(),
        }
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
                }
                None => {
                    // The snapshot jumped past anything we had; start fresh from it.
                    self.entries.clear();
                    self.base_digest = 0;
                }
            }
            self.base_index = snap;
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
