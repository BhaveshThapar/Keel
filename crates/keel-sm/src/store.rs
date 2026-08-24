//! The seam between the state machine and whatever holds its bytes.
//!
//! One method carries the whole safety argument: [`Store::commit`] writes the
//! applied index and the data it describes **in one atomic batch**. A crash
//! between them is what makes apply non-idempotent on replay — the log would
//! hand back entries the store had already applied, and a state machine that
//! cannot tell would apply them twice.
//!
//! Everything else here is ordinary. Two implementations exist so that the
//! simulator can run without a storage engine underneath it and the server can
//! run with one, and they are held to [one suite](crate::conformance) rather
//! than to whatever their own tests happened to check.

use std::collections::BTreeMap;

use bytes::Bytes;
use keel_raft::Index;

use crate::StateMachineError;

/// A namespace tag in front of every key the store sees.
///
/// The state machine keeps its own state — the applied index, the session
/// table, the nonce table — in the same store as user data, because that is the
/// only way an update to both can be one atomic write. Which means a user key
/// and an internal key could collide, and a client that wrote the right key
/// could corrupt the session table.
///
/// A one-byte tag settles it. Every key is prefixed on the way in and stripped
/// on the way out, so a user may use any key at all, including one that looks
/// exactly like an internal one.
///
/// This tag deliberately does not live in the storage engine. `lsm_kv` is a
/// general-purpose store and has no business reserving a prefix from everyone
/// who uses it; the mapping from client keys to stored keys belongs to whoever
/// owns that mapping, which is this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Space {
    /// A key a client wrote.
    User = 0,
    /// A key the state machine wrote about itself.
    Internal = 1,
}

/// Prefix `key` with its namespace.
pub fn tagged(space: Space, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + 1);
    out.push(space as u8);
    out.extend_from_slice(key);
    out
}

/// Strip the namespace tag, returning `None` if the key is not in `space`.
pub fn untagged(space: Space, key: &[u8]) -> Option<&[u8]> {
    match key.split_first() {
        Some((tag, rest)) if *tag == space as u8 => Some(rest),
        _ => None,
    }
}

/// One key's fate in a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    Put(Bytes),
    Delete,
}

/// Everything one applied entry changes, including where the log has got to.
///
/// Built by the state machine and handed to the store whole. There is no way to
/// write the data without the index or the index without the data, which is the
/// point.
#[derive(Debug, Clone, Default)]
pub struct Batch {
    ops: Vec<(Vec<u8>, Mutation)>,
}

impl Batch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, space: Space, key: &[u8], value: Bytes) -> &mut Self {
        self.ops.push((tagged(space, key), Mutation::Put(value)));
        self
    }

    pub fn delete(&mut self, space: Space, key: &[u8]) -> &mut Self {
        self.ops.push((tagged(space, key), Mutation::Delete));
        self
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// The mutations, in the order they were added.
    pub fn ops(&self) -> &[(Vec<u8>, Mutation)] {
        &self.ops
    }

    /// Whether this batch already changes `key`.
    ///
    /// Expiry needs it. Expiry decides what to drop by reading the store, and
    /// the store does not yet hold what this batch is about to write — so a
    /// client heard from in *this* entry still looks idle, and would be expired
    /// by the same batch that just renewed it. Skipping keys the batch touches
    /// is the fix, and it is exactly right: a client the entry mentions has been
    /// heard from.
    pub fn touches(&self, space: Space, key: &[u8]) -> bool {
        let tagged = tagged(space, key);
        self.ops.iter().any(|(k, _)| *k == tagged)
    }
}

/// Where the state machine's bytes live.
pub trait Store {
    /// Read one key.
    fn get(&self, space: Space, key: &[u8]) -> Result<Option<Bytes>, StateMachineError>;

    /// Read a key range within one namespace, ascending, at most `limit` of
    /// them. Keys come back without their tag.
    fn scan(
        &self,
        space: Space,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, StateMachineError>;

    /// Apply `batch` and record that the log has been applied through `index`,
    /// atomically.
    ///
    /// "Atomically" is the whole contract: after a crash, a reader sees either
    /// both or neither. An implementation that writes the data and then the
    /// index has a window in which a crash leaves a store that has applied an
    /// entry and does not know it, and the log will hand that entry back.
    fn commit(&mut self, index: Index, batch: Batch) -> Result<(), StateMachineError>;

    /// The highest log index this store has applied.
    fn applied(&self) -> Index;
}

/// A store in memory. What the simulator runs on, and what a test uses when the
/// storage engine is not the thing under test.
#[derive(Debug, Default)]
pub struct MemStore {
    data: BTreeMap<Vec<u8>, Bytes>,
    applied: Index,
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// How many keys are stored, across both namespaces. For tests that want to
    /// assert a write did not happen.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Store for MemStore {
    fn get(&self, space: Space, key: &[u8]) -> Result<Option<Bytes>, StateMachineError> {
        Ok(self.data.get(&tagged(space, key)).cloned())
    }

    fn scan(
        &self,
        space: Space,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, StateMachineError> {
        let low = tagged(space, start.unwrap_or(&[]));
        Ok(self
            .data
            .range(low..)
            .filter_map(|(key, value)| untagged(space, key).map(|k| (k, value)))
            .take_while(|(key, _)| match end {
                Some(e) => *key < e,
                None => true,
            })
            .take(limit)
            .map(|(key, value)| (Bytes::copy_from_slice(key), value.clone()))
            .collect())
    }

    fn commit(&mut self, index: Index, batch: Batch) -> Result<(), StateMachineError> {
        // In memory there is no crash to be atomic against, and applying the
        // batch to a temporary first would only be theatre. What matters is
        // that the index moves with the data, which it does because both happen
        // here.
        for (key, mutation) in batch.ops {
            match mutation {
                Mutation::Put(value) => {
                    self.data.insert(key, value);
                }
                Mutation::Delete => {
                    self.data.remove(&key);
                }
            }
        }
        self.applied = self.applied.max(index);
        Ok(())
    }

    fn applied(&self) -> Index {
        self.applied
    }
}
