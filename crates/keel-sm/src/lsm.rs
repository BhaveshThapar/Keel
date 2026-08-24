//! The store backed by the real storage engine.
//!
//! Thin on purpose. Everything interesting about atomicity is the engine's
//! `write_batch`, which puts the applied index and the data it describes into
//! one write-ahead-log frame under one checksum — so a crash takes both or
//! neither, and there is no window in which the store has applied an entry and
//! does not know it.

use bytes::Bytes;
use keel_raft::Index;
use lsm_kv::{Db, Maintenance, Options, StdFs, SyncMode, WriteBatch};
use std::path::Path;

use crate::StateMachineError;
use crate::store::{Batch, Mutation, Space, Store, tagged, untagged};

/// The key the applied index lives under, in the internal namespace.
const APPLIED_KEY: &[u8] = b"applied-index";

fn store_err(e: lsm_kv::Error) -> StateMachineError {
    StateMachineError::Store(e.to_string())
}

/// A [`Store`] over an LSM database.
pub struct LsmStore {
    db: Db<StdFs>,
    applied: Index,
}

impl LsmStore {
    /// Open at `dir`, recovering the applied index from what is there.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, StateMachineError> {
        Self::open_with(dir, Self::default_options())
    }

    /// The options a Raft state machine wants.
    ///
    /// `Durable`, because a state machine that acknowledges a write it cannot
    /// prove survived a power cut is the one thing this whole project exists to
    /// avoid. `Manual`, because the host loop already has a turn structure and
    /// a background thread deciding when to flush makes a node's behaviour a
    /// function of its scheduler.
    pub fn default_options() -> Options {
        Options {
            sync_wal: SyncMode::Durable,
            maintenance: Maintenance::Manual,
            ..Options::default()
        }
    }

    pub fn open_with(dir: impl AsRef<Path>, opts: Options) -> Result<Self, StateMachineError> {
        let db = Db::open_with(dir, opts).map_err(store_err)?;
        let applied = db
            .get(&tagged(Space::Internal, APPLIED_KEY))
            .map_err(store_err)?
            .and_then(|b| b.as_slice().try_into().ok().map(Index::from_le_bytes))
            .unwrap_or(0);
        Ok(Self { db, applied })
    }

    /// Do one unit of the engine's deferred work, and say whether more remains.
    ///
    /// The host calls this on its own turn. Nothing else will: the engine spawns
    /// no threads here.
    pub fn maintain(&self) -> Result<bool, StateMachineError> {
        self.db.maintain().map_err(store_err)
    }

    pub fn pending_work(&self) -> bool {
        self.db.pending_work()
    }

    /// `Err` once the engine has latched a failure. A node that sees this must
    /// step down: it can no longer make an entry durable.
    pub fn health(&self) -> Result<(), StateMachineError> {
        self.db.health().map_err(store_err)
    }
}

impl Store for LsmStore {
    fn get(&self, space: Space, key: &[u8]) -> Result<Option<Bytes>, StateMachineError> {
        Ok(self
            .db
            .get(&tagged(space, key))
            .map_err(store_err)?
            .map(Bytes::from))
    }

    fn scan(
        &self,
        space: Space,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, StateMachineError> {
        let low = tagged(space, start.unwrap_or(&[]));
        // No end bound means "to the end of this namespace", not to the end of
        // the store: the next namespace's keys start one tag higher.
        let high = match end {
            Some(end) => tagged(space, end),
            None => vec![space as u8 + 1],
        };
        Ok(self
            .db
            .scan(Some(&low), Some(&high), limit)
            .map_err(store_err)?
            .into_iter()
            .filter_map(|(key, value)| {
                untagged(space, &key).map(|k| (Bytes::copy_from_slice(k), Bytes::from(value)))
            })
            .collect())
    }

    fn commit(&mut self, index: Index, batch: Batch) -> Result<(), StateMachineError> {
        let mut write = WriteBatch::new();
        for (key, mutation) in batch.ops() {
            match mutation {
                Mutation::Put(value) => write.put(key, value),
                Mutation::Delete => write.delete(key),
            };
        }
        // In the same batch, not after it. This line is the whole reason the
        // engine grew `write_batch`.
        write.put(&tagged(Space::Internal, APPLIED_KEY), &index.to_le_bytes());
        self.db.write_batch(&write).map_err(store_err)?;
        self.applied = self.applied.max(index);
        Ok(())
    }

    fn applied(&self) -> Index {
        self.applied
    }
}
