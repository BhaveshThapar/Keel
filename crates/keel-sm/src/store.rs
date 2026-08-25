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
    /// Where each key's mutation sits in `ops`.
    ///
    /// A batch used to be append-only and searched by walking it, which was
    /// fine while a batch described one entry and held a handful of keys. A
    /// batch now describes a whole `Ready` — that is what lets one fsync retire
    /// the lot — and every apply in it reads what the applies before it wrote,
    /// so the walk would be quadratic in the batch.
    index: std::collections::HashMap<Vec<u8>, usize>,
}

impl Batch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, space: Space, key: &[u8], value: Bytes) -> &mut Self {
        self.set(tagged(space, key), Mutation::Put(value));
        self
    }

    pub fn delete(&mut self, space: Space, key: &[u8]) -> &mut Self {
        self.set(tagged(space, key), Mutation::Delete);
        self
    }

    /// One mutation per key, the last one winning.
    ///
    /// Keeping both would leave the store to resolve them by order, which every
    /// implementation of it would then have to get right; and it would let a
    /// batch grow without bound while one entry rewrote one key.
    fn set(&mut self, key: Vec<u8>, mutation: Mutation) {
        match self.index.get(&key) {
            Some(&at) => self.ops[at].1 = mutation,
            None => {
                self.index.insert(key.clone(), self.ops.len());
                self.ops.push((key, mutation));
            }
        }
    }

    /// What this batch will leave at `key`, if it says anything about it.
    pub fn get(&self, space: Space, key: &[u8]) -> Option<&Mutation> {
        self.index
            .get(&tagged(space, key))
            .and_then(|at| self.ops.get(*at))
            .map(|(_, mutation)| mutation)
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
        self.index.contains_key(&tagged(space, key))
    }
}

/// Read `key` as this batch will leave it: the batch's own mutation where it has
/// one, and the store otherwise.
///
/// Every read on the apply path goes through here, and that is the whole of what
/// makes applying several entries into one batch mean the same thing as applying
/// them one at a time. Two increments of the same key in one batch that both
/// read the store would both read the old value, and the second would overwrite
/// the first rather than add to it.
pub fn read_through<S: Store + ?Sized>(
    store: &S,
    batch: &Batch,
    space: Space,
    key: &[u8],
) -> Result<Option<Bytes>, StateMachineError> {
    match batch.get(space, key) {
        Some(Mutation::Put(value)) => Ok(Some(value.clone())),
        Some(Mutation::Delete) => Ok(None),
        None => store.get(space, key),
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

    /// Everything this store holds, as bytes.
    ///
    /// The simulator's stand-in for a checkpoint directory. A real node
    /// hard-links SSTables and streams files; there is nothing to link here, so
    /// the equivalent is the whole map — and the point of having it is that the
    /// simulator can then stream a snapshot through the same chunking, the same
    /// checksums and the same resume logic as a real one, rather than modelling
    /// the transfer as instantaneous and testing nothing about it.
    ///
    /// Length-prefixed rather than delimited, because a key or a value may
    /// contain any byte including whatever delimiter looked safe.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend((self.applied).to_le_bytes());
        out.extend((self.data.len() as u64).to_le_bytes());
        for (key, value) in &self.data {
            out.extend((key.len() as u64).to_le_bytes());
            out.extend(key);
            out.extend((value.len() as u64).to_le_bytes());
            out.extend(value);
        }
        out
    }

    /// Rebuild a store from [`MemStore::to_bytes`].
    ///
    /// Refuses anything malformed rather than reconstructing what it can: a
    /// partially decoded snapshot is a state machine that silently disagrees
    /// with the node that sent it.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, StateMachineError> {
        struct Cursor<'a> {
            bytes: &'a [u8],
            at: usize,
        }

        impl<'a> Cursor<'a> {
            fn take(&mut self, n: usize) -> Result<&'a [u8], StateMachineError> {
                let end = self
                    .at
                    .checked_add(n)
                    .filter(|e| *e <= self.bytes.len())
                    .ok_or_else(|| {
                        StateMachineError::Malformed(format!(
                            "a snapshot ended after {} bytes with {n} more expected",
                            self.at
                        ))
                    })?;
                let slice = &self.bytes[self.at..end];
                self.at = end;
                Ok(slice)
            }

            fn take_u64(&mut self) -> Result<u64, StateMachineError> {
                self.take(8).and_then(|b| {
                    b.try_into()
                        .map(u64::from_le_bytes)
                        .map_err(|_| StateMachineError::Malformed("a truncated length".into()))
                })
            }
        }

        let mut cursor = Cursor { bytes, at: 0 };
        let applied = cursor.take_u64()?;
        let count = cursor.take_u64()?;
        let mut data = BTreeMap::new();
        for _ in 0..count {
            let key_len = cursor.take_u64()? as usize;
            let key = cursor.take(key_len)?.to_vec();
            let value_len = cursor.take_u64()? as usize;
            let value = Bytes::copy_from_slice(cursor.take(value_len)?);
            data.insert(key, value);
        }
        if cursor.at != bytes.len() {
            return Err(StateMachineError::Malformed(format!(
                "{} bytes left over after a complete snapshot",
                bytes.len() - cursor.at
            )));
        }
        Ok(Self { data, applied })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn populated() -> MemStore {
        let mut store = MemStore::new();
        let mut batch = Batch::new();
        for i in 0..50u32 {
            batch.put(
                Space::User,
                format!("k{i:03}").as_bytes(),
                Bytes::from(format!("v{i}")),
            );
        }
        batch.put(Space::Internal, b"session/1", Bytes::from_static(b"s"));
        // A key and a value with every awkward byte in them, because a snapshot
        // that mangled one would be a snapshot that silently disagreed.
        batch.put(
            Space::User,
            &[0u8, 0xff, b'\n'],
            Bytes::from_static(&[0, 1, 2]),
        );
        store.commit(77, batch).unwrap();
        store
    }

    #[test]
    fn a_store_round_trips_through_its_bytes() {
        let store = populated();
        let restored = MemStore::from_bytes(&store.to_bytes()).unwrap();

        assert_eq!(restored.applied(), store.applied());
        assert_eq!(restored.len(), store.len());
        for i in 0..50u32 {
            assert_eq!(
                restored
                    .get(Space::User, format!("k{i:03}").as_bytes())
                    .unwrap(),
                store
                    .get(Space::User, format!("k{i:03}").as_bytes())
                    .unwrap()
            );
        }
        assert_eq!(
            restored.get(Space::Internal, b"session/1").unwrap(),
            Some(Bytes::from_static(b"s")),
            "the internal namespace did not survive, so a snapshot would lose \
             the session table"
        );
        assert_eq!(
            restored.get(Space::User, &[0u8, 0xff, b'\n']).unwrap(),
            Some(Bytes::from_static(&[0, 1, 2]))
        );
    }

    /// Every prefix of a snapshot is refused. A partially decoded one is a state
    /// machine that silently disagrees with the node that sent it, which is
    /// worse than one that will not open.
    #[test]
    fn every_truncation_of_a_snapshot_is_refused() {
        let bytes = populated().to_bytes();
        for cut in 0..bytes.len() {
            assert!(
                MemStore::from_bytes(&bytes[..cut]).is_err(),
                "a snapshot truncated to {cut} of {} bytes decoded",
                bytes.len()
            );
        }
        assert!(MemStore::from_bytes(&bytes).is_ok());
    }

    #[test]
    fn trailing_bytes_after_a_snapshot_are_refused() {
        let mut bytes = populated().to_bytes();
        bytes.push(0);
        assert!(
            MemStore::from_bytes(&bytes).is_err(),
            "a snapshot with something appended was accepted"
        );
    }

    #[test]
    fn an_empty_store_round_trips() {
        let store = MemStore::new();
        let restored = MemStore::from_bytes(&store.to_bytes()).unwrap();
        assert!(restored.is_empty());
        assert_eq!(restored.applied(), 0);
    }
}
