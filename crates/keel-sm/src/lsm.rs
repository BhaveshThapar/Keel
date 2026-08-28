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
use std::path::{Path, PathBuf};

use crate::StateMachineError;
use crate::store::{Batch, Mutation, Space, Store, tagged, untagged};

/// The key the applied index lives under, in the internal namespace.
const APPLIED_KEY: &[u8] = b"applied-index";

fn store_err(e: lsm_kv::Error) -> StateMachineError {
    StateMachineError::Store(e.to_string())
}

/// A [`Store`] over an LSM database.
pub struct LsmStore {
    db: Option<Db<StdFs>>,
    dir: PathBuf,
    opts: Options,
    applied: Index,
}

impl Clone for LsmStore {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            dir: self.dir.clone(),
            opts: self.opts.clone(),
            applied: self.applied,
        }
    }
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
        let dir = dir.as_ref().to_path_buf();
        let db = Db::open_with(&dir, opts.clone()).map_err(store_err)?;
        let applied = db
            .get(&tagged(Space::Internal, APPLIED_KEY))
            .map_err(store_err)?
            .and_then(|b| b.as_slice().try_into().ok().map(Index::from_le_bytes))
            .unwrap_or(0);
        Ok(Self {
            db: Some(db),
            dir,
            opts,
            applied,
        })
    }

    fn db(&self) -> Result<&Db<StdFs>, StateMachineError> {
        self.db
            .as_ref()
            .ok_or_else(|| StateMachineError::Store("the state store is being replaced".into()))
    }

    /// Do one unit of the engine's deferred work, and say whether more remains.
    ///
    /// The host calls this on its own turn. Nothing else will: the engine spawns
    /// no threads here.
    pub fn maintain(&self) -> Result<bool, StateMachineError> {
        self.db()?.maintain().map_err(store_err)
    }

    pub fn pending_work(&self) -> bool {
        self.db.as_ref().is_some_and(Db::pending_work)
    }

    /// Write a checkpoint of this store into `dir`.
    ///
    /// The engine hard-links its SSTables, so the cost is proportional to the
    /// number of tables rather than to the bytes in them — which is what makes
    /// FR-9's "stalls writes for under 50 ms on a 1 GB state" reachable at all.
    ///
    /// Everything the state machine keeps goes with it: user data, the session
    /// table, the nonce table, and the applied index, all in the same store and
    /// therefore all in the same checkpoint. A snapshot that carried the data
    /// and not the sessions would be a snapshot a client's retries could apply
    /// twice on top of.
    pub fn checkpoint(&self, dir: impl AsRef<Path>) -> Result<(), StateMachineError> {
        self.db()?.checkpoint(dir).map_err(store_err)
    }

    /// `Err` once the engine has latched a failure. A node that sees this must
    /// step down: it can no longer make an entry durable.
    pub fn health(&self) -> Result<(), StateMachineError> {
        self.db()?.health().map_err(store_err)
    }

    /// Close the live database, atomically publish a received checkpoint over
    /// it, and reopen it with the same durability options.
    ///
    /// The Raft log's snapshot floor must be durable before this is called. A
    /// crash in the opposite order can splice the old log onto the new state.
    pub fn replace_from_checkpoint(
        &mut self,
        publish: impl FnOnce(&Path) -> Result<(), StateMachineError>,
    ) -> Result<(), StateMachineError> {
        drop(self.db.take());
        if let Err(error) = publish(&self.dir) {
            self.db = Some(Db::open_with(&self.dir, self.opts.clone()).map_err(store_err)?);
            return Err(error);
        }
        let db = Db::open_with(&self.dir, self.opts.clone()).map_err(store_err)?;
        self.applied = db
            .get(&tagged(Space::Internal, APPLIED_KEY))
            .map_err(store_err)?
            .and_then(|b| b.as_slice().try_into().ok().map(Index::from_le_bytes))
            .unwrap_or(0);
        self.db = Some(db);
        Ok(())
    }
}

impl Store for LsmStore {
    fn get(&self, space: Space, key: &[u8]) -> Result<Option<Bytes>, StateMachineError> {
        Ok(self
            .db()?
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
            .db()?
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
        // engine grew `write_batch` (ADR-010).
        #[cfg(not(feature = "negative-demos"))]
        {
            write.put(&tagged(Space::Internal, APPLIED_KEY), &index.to_le_bytes());
            self.db()?.write_batch(&write).map_err(store_err)?;
        }

        // The rule removed: the data first, then the index, as two writes. Both
        // are still fsynced, so this is not a durability bug — what is gone is
        // exactly and only the atomicity between them, which is what makes it a
        // fair test of what the ADR claims.
        //
        // The window is announced and then held open, because a fault has to be
        // aimed to be reached: the gap between two fsyncs is a small target and
        // a uniform kill schedule finds it by luck if at all. Same argument
        // ADR-007 makes for the simulator's nemesis. A correct build prints
        // nothing here, so the two arms of the demonstration are otherwise
        // identical.
        #[cfg(feature = "negative-demos")]
        {
            use std::io::Write;
            self.db()?.write_batch(&write).map_err(store_err)?;
            let mut out = std::io::stdout().lock();
            let _ = writeln!(out, "SPLIT {index}");
            let _ = out.flush();
            drop(out);
            std::thread::sleep(std::time::Duration::from_millis(50));

            let mut index_only = WriteBatch::new();
            index_only.put(&tagged(Space::Internal, APPLIED_KEY), &index.to_le_bytes());
            self.db()?.write_batch(&index_only).map_err(store_err)?;
        }

        self.applied = self.applied.max(index);
        Ok(())
    }

    fn applied(&self) -> Index {
        self.applied
    }
}
