//! Snapshots in the host loop: when to take one, how to send one, how to
//! install one.
//!
//! The core does none of this. It says "this follower has fallen behind my log's
//! floor, offer it a snapshot" and "the host has told me it installed one"; the
//! bytes are the host's problem, which is what lets the same core run under a
//! simulator with no disk and under a server with one.
//!
//! The division of labour that matters:
//!
//! **Taking** is triggered by the log growing, not by a timer. A node that
//! checkpoints every minute checkpoints an idle cluster for nothing and a busy
//! one too rarely; a node that checkpoints every *N* applied entries does work
//! proportional to the work it has done.
//!
//! **Sending** is one chunk per turn, not a loop. A leader that streamed a
//! gigabyte inside one turn would stop answering heartbeats for the duration and
//! be replaced by a follower that thought it had died — while sending that
//! follower a snapshot.
//!
//! **Installing** is staged and published by rename, and only after the digest
//! agrees. That is [`keel_sm::transfer`]'s business; what is here is deciding
//! when to start and when to give up.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use keel_raft::{Index, NodeId, SnapshotMeta};
use keel_sm::{Accepted, Chunk, Receiver, Sender, StateMachineError};

/// How many applied entries between checkpoints.
///
/// Proportional to work done rather than to time passed. Ten thousand entries
/// against the default 4 MiB MemTable is a few flushes' worth — often enough
/// that the log floor moves and rarely enough that the hard-link walk is not the
/// node's main activity.
pub const ENTRIES_BETWEEN_CHECKPOINTS: u64 = 10_000;

/// A snapshot being sent to one follower.
pub struct Outgoing {
    pub peer: NodeId,
    pub meta: SnapshotMeta,
    sender: Sender,
    /// What the receiver last said it had. A resume restarts from here.
    position: BTreeMap<String, u64>,
    /// Chunks handed to the transport. For metrics and for a test that wants to
    /// say a transfer was interrupted rather than restarted.
    pub chunks_sent: u64,
}

impl Outgoing {
    pub fn new(
        peer: NodeId,
        meta: SnapshotMeta,
        dir: impl AsRef<Path>,
    ) -> Result<Self, StateMachineError> {
        Self::with_chunk_bytes(peer, meta, dir, keel_sm::CHUNK_BYTES)
    }

    /// The same, at a chosen chunk size.
    pub fn with_chunk_bytes(
        peer: NodeId,
        meta: SnapshotMeta,
        dir: impl AsRef<Path>,
        chunk_bytes: usize,
    ) -> Result<Self, StateMachineError> {
        Ok(Self {
            peer,
            meta,
            sender: Sender::with_chunk_bytes(dir, chunk_bytes)?,
            position: BTreeMap::new(),
            chunks_sent: 0,
        })
    }

    /// Restart from what the receiver says it has.
    ///
    /// The whole point of a resume: a transfer that was interrupted at nine
    /// tenths does not send the first nine tenths again.
    pub fn resume_from(&mut self, position: BTreeMap<String, u64>) {
        self.sender.resume_from(&position);
        self.position = position;
    }

    pub fn next_chunk(&mut self) -> Result<Option<Chunk>, StateMachineError> {
        let chunk = self.sender.next_chunk()?;
        if chunk.is_some() {
            self.chunks_sent += 1;
        }
        Ok(chunk)
    }
}

/// A snapshot being received.
pub struct Incoming {
    pub from: NodeId,
    pub meta: SnapshotMeta,
    receiver: Receiver,
    staging: PathBuf,
    /// Chunks accepted. A resumed transfer's count continues rather than
    /// restarting, which is how a test tells the two apart.
    pub chunks_accepted: u64,
    pub chunks_rejected: u64,
}

impl Incoming {
    pub fn new(
        from: NodeId,
        meta: SnapshotMeta,
        staging: impl AsRef<Path>,
    ) -> Result<Self, StateMachineError> {
        let staging = staging.as_ref().to_path_buf();
        Ok(Self {
            from,
            meta,
            receiver: Receiver::new(&staging)?,
            staging,
            chunks_accepted: 0,
            chunks_rejected: 0,
        })
    }

    /// Recover the verified byte positions left by a killed receiver.
    pub fn resume(
        from: NodeId,
        meta: SnapshotMeta,
        staging: impl AsRef<Path>,
    ) -> Result<Self, StateMachineError> {
        let staging = staging.as_ref().to_path_buf();
        Ok(Self {
            from,
            meta,
            receiver: Receiver::resume(&staging)?,
            staging,
            chunks_accepted: 0,
            chunks_rejected: 0,
        })
    }

    /// What this transfer has verified, per file. Sent back to the leader on a
    /// resume.
    pub fn position(&self) -> BTreeMap<String, u64> {
        self.receiver.position().clone()
    }

    pub fn accept(&mut self, chunk: &Chunk) -> Result<Accepted, StateMachineError> {
        let outcome = self.receiver.accept(chunk)?;
        match outcome {
            Accepted::Written | Accepted::Complete => self.chunks_accepted += 1,
            Accepted::Rejected | Accepted::OutOfOrder => self.chunks_rejected += 1,
        }
        Ok(outcome)
    }

    pub fn is_complete(&self) -> bool {
        self.receiver.is_complete()
    }

    pub fn finish(&mut self) {
        self.receiver.finish();
    }

    /// Publish, once the digest agrees.
    pub fn publish(
        self,
        destination: impl AsRef<Path>,
        expected_digest: u64,
        verify: impl Fn(&Path) -> Result<u64, StateMachineError>,
    ) -> Result<(), StateMachineError> {
        self.receiver.publish(destination, expected_digest, verify)
    }

    pub fn staging(&self) -> &Path {
        &self.staging
    }
}

/// Whether enough has been applied since the last checkpoint to take another.
pub fn checkpoint_is_due(applied: Index, last_checkpoint: Index) -> bool {
    applied.saturating_sub(last_checkpoint) >= ENTRIES_BETWEEN_CHECKPOINTS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_checkpoint_is_due_by_entries_applied_rather_than_by_time() {
        assert!(!checkpoint_is_due(0, 0));
        assert!(!checkpoint_is_due(ENTRIES_BETWEEN_CHECKPOINTS - 1, 0));
        assert!(checkpoint_is_due(ENTRIES_BETWEEN_CHECKPOINTS, 0));
        // And it is the distance that matters, not the absolute position: a node
        // that has been up for a month is not perpetually overdue.
        assert!(!checkpoint_is_due(1_000_000, 1_000_000));
        assert!(checkpoint_is_due(
            1_000_000 + ENTRIES_BETWEEN_CHECKPOINTS,
            1_000_000
        ));
    }
}
