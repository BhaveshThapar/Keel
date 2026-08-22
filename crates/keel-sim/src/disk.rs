use keel_raft::{Entry, HardState, SnapshotMeta};

/// A node's durable storage, modelled at record granularity.
///
/// The point of the model is the gap between *written* and *durable*. A host
/// writes a `Ready`'s entries and hard state, and only later does the fsync
/// complete; a crash in between loses everything staged. That window is exactly
/// where the persist-before-send contract has to hold, so it is the window worth
/// simulating.
///
/// Writes are kept as an ordered list of pending batches rather than folded into
/// one mutable image, because an fsync must make durable exactly what had been
/// written when it was *issued*, not whatever has been written since. Folding
/// them together would let a later write ride along on an earlier fsync and
/// become durable for free — which would quietly hide the very hazard this
/// model exists to expose, since fsync latencies vary and several `Ready`s can
/// be in flight at once.
///
/// Byte-level torn writes are not modelled here — they belong to the real log's
/// own recovery tests, which parse actual bytes. This models the protocol's
/// response to losing an unsynced tail, not the parser's.
#[derive(Debug, Clone, Default)]
pub struct SimDisk {
    durable: Image,
    pending: Vec<(u64, Write)>,
    next_write: u64,
}

#[derive(Debug, Clone, Default)]
struct Image {
    entries: Vec<Entry>,
    hard_state: HardState,
    snapshot: Option<SnapshotMeta>,
}

#[derive(Debug, Clone)]
struct Write {
    entries: Vec<Entry>,
    hard_state: Option<HardState>,
    snapshot: Option<SnapshotMeta>,
}

impl Image {
    fn apply(&mut self, w: &Write) {
        if let Some(meta) = &w.snapshot {
            self.entries.retain(|e| e.index > meta.index);
            self.snapshot = Some(meta.clone());
        }
        if let Some(first) = w.entries.first() {
            // A rewrite replaces everything from its first index upward.
            self.entries.retain(|e| e.index < first.index);
            self.entries.extend_from_slice(&w.entries);
        }
        if let Some(hs) = w.hard_state {
            self.hard_state = hs;
        }
    }
}

impl SimDisk {
    pub fn new() -> Self {
        Self::default()
    }

    /// Issue a write. Returns the token to hand back to [`SimDisk::sync`] when
    /// this write's fsync completes.
    pub fn stage(
        &mut self,
        entries: &[Entry],
        hard_state: Option<HardState>,
        snapshot: Option<SnapshotMeta>,
    ) -> u64 {
        self.next_write += 1;
        self.pending.push((
            self.next_write,
            Write {
                entries: entries.to_vec(),
                hard_state,
                snapshot,
            },
        ));
        self.next_write
    }

    /// An fsync completes: everything written at or before `token` is durable.
    /// Anything written after it stays at risk, however long its own fsync takes.
    pub fn sync(&mut self, token: u64) {
        let (done, still_pending): (Vec<_>, Vec<_>) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|(t, _)| *t <= token);
        for (_, w) in &done {
            self.durable.apply(w);
        }
        self.pending = still_pending;
    }

    /// Power loss. Writes whose fsync had not completed are gone; what was
    /// fsynced stays.
    pub fn crash(&mut self) {
        self.pending.clear();
    }

    pub fn recovered_entries(&self) -> Vec<Entry> {
        self.durable.entries.clone()
    }

    pub fn recovered_hard_state(&self) -> HardState {
        self.durable.hard_state
    }

    pub fn recovered_snapshot(&self) -> Option<SnapshotMeta> {
        self.durable.snapshot.clone()
    }
}
