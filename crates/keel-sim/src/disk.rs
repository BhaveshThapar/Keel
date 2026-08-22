use keel_raft::{Entry, HardState, SnapshotMeta};

/// A node's durable storage, modelled at record granularity.
///
/// The point of the model is the gap between *written* and *durable*. A host
/// writes a `Ready`'s entries and hard state, and only later does the fsync
/// complete; a crash in between loses everything staged. That window is exactly
/// where the persist-before-send contract has to hold, so it is the window worth
/// simulating.
///
/// Byte-level torn writes are not modelled here — they belong to the real log's
/// own recovery tests, which parse actual bytes. This models the protocol's
/// response to losing an unsynced tail, not the parser's.
#[derive(Debug, Clone, Default)]
pub struct SimDisk {
    durable_entries: Vec<Entry>,
    durable_hard_state: HardState,
    durable_snapshot: Option<SnapshotMeta>,

    staged_entries: Vec<Entry>,
    staged_hard_state: HardState,
    staged_snapshot: Option<SnapshotMeta>,
}

impl SimDisk {
    pub fn new() -> Self {
        Self::default()
    }

    /// Write a `Ready`'s output. Not durable until [`SimDisk::sync`].
    pub fn stage(&mut self, entries: &[Entry], hard_state: Option<HardState>) {
        if let Some(first) = entries.first() {
            self.staged_entries.retain(|e| e.index < first.index);
            self.staged_entries.extend_from_slice(entries);
        }
        if let Some(hs) = hard_state {
            self.staged_hard_state = hs;
        }
    }

    pub fn stage_snapshot(&mut self, meta: SnapshotMeta) {
        self.staged_entries.retain(|e| e.index > meta.index);
        self.staged_snapshot = Some(meta);
    }

    /// An fsync completes: everything staged is now durable.
    pub fn sync(&mut self) {
        self.durable_entries.clone_from(&self.staged_entries);
        self.durable_hard_state = self.staged_hard_state;
        self.durable_snapshot.clone_from(&self.staged_snapshot);
    }

    /// Power loss. Staged-but-unsynced writes are gone; what was fsynced stays.
    pub fn crash(&mut self) {
        self.staged_entries.clone_from(&self.durable_entries);
        self.staged_hard_state = self.durable_hard_state;
        self.staged_snapshot.clone_from(&self.durable_snapshot);
    }

    pub fn recovered_entries(&self) -> Vec<Entry> {
        self.durable_entries.clone()
    }

    pub fn recovered_hard_state(&self) -> HardState {
        self.durable_hard_state
    }

    pub fn recovered_snapshot(&self) -> Option<SnapshotMeta> {
        self.durable_snapshot.clone()
    }
}
