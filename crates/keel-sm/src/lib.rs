//! Keel's replicated state machine: what a committed entry means.
//!
//! Two things here are the reason the crate exists.
//!
//! **The applied index is written in the same batch as the data it describes.**
//! Not after it, not before it — in it. A crash between the two leaves a store
//! that has applied an entry and does not know it, so the log hands that entry
//! back on restart and it applies a second time. Every mutation an entry makes
//! goes into one [`Batch`] alongside the index, and [`Store::commit`] is the
//! only way to write anything (ADR-010).
//!
//! **A retried command applies once.** Every command carries `(client, seq)`,
//! and the state machine keeps the last sequence number it applied for each
//! client together with the response it produced. A duplicate returns that
//! response and writes nothing at all — not the same value again, nothing.
//! Which matters because a client that never saw its acknowledgement cannot
//! tell a lost response from a lost request, and Raft's own delivery guarantee
//! is at-least-once (FR-7, ADR-012).
//!
//! ```
//! use keel_api::{Command, Proposal, ProposalBody};
//! use keel_sm::{MemStore, StateMachine};
//!
//! let mut sm = StateMachine::new(MemStore::new());
//! let client = sm.register(1, 0, 7)?;
//!
//! let put = Proposal {
//!     stamped_ms: 0,
//!     session: Some((client, 1)),
//!     body: ProposalBody::Command(Command::Incr { key: b"n".to_vec().into(), by: 1 }),
//! };
//! sm.apply(2, &put)?;
//! // The same entry again — a retry the leader replicated twice.
//! sm.apply(3, &put)?;
//! assert_eq!(sm.counter(b"n")?, 1, "the retry applied a second time");
//! # Ok::<(), keel_sm::StateMachineError>(())
//! ```

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

#[cfg(feature = "conformance")]
pub mod conformance;
#[cfg(feature = "lsm")]
mod lsm;
mod session;
mod store;
pub mod transfer;

use bytes::Bytes;
use keel_api::{ApiError, ClientId, Command, Proposal, ProposalBody, Response, Seq};
use keel_raft::Index;

#[cfg(feature = "lsm")]
pub use lsm::LsmStore;
pub use session::{SESSION_TIMEOUT_MS, Session};
pub use store::{Batch, MemStore, Mutation, Space, Store, read_through, tagged, untagged};
pub use transfer::{Accepted, CHUNK_BYTES, Chunk, Receiver, Sender};

#[derive(Debug, thiserror::Error)]
pub enum StateMachineError {
    #[error("the store failed: {0}")]
    Store(String),
    /// A stored value this state machine wrote does not decode. Not a client's
    /// fault and not recoverable in process: something below has corrupted, or
    /// a build wrote a format this one does not read.
    #[error("stored state is malformed: {0}")]
    Malformed(String),
}

/// A committed entry, applied.
pub struct StateMachine<S: Store> {
    store: S,
    /// Handed out in order and persisted, so two nodes applying the same log
    /// hand out the same ids.
    next_client: ClientId,
}

impl<S: Store> StateMachine<S> {
    /// Build on `store`, recovering the client counter from it.
    pub fn new(store: S) -> Self {
        let next_client = store
            .get(Space::Internal, session::NEXT_CLIENT_KEY)
            .ok()
            .flatten()
            .and_then(|b| b.as_ref().try_into().ok().map(u64::from_le_bytes))
            .unwrap_or(1);
        Self { store, next_client }
    }

    pub fn applied(&self) -> Index {
        self.store.applied()
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    /// Apply one committed entry.
    ///
    /// Idempotent in both senses that matter. An `index` at or below what the
    /// store has already applied is ignored, which covers a restart replaying
    /// the log. A `(client, seq)` already applied returns its cached response
    /// and writes nothing, which covers a client retrying.
    pub fn apply(
        &mut self,
        index: Index,
        proposal: &Proposal,
    ) -> Result<Response, StateMachineError> {
        let mut responses = self.apply_batch(std::slice::from_ref(&(index, proposal.clone())))?;
        Ok(responses.pop().unwrap_or(Response::Applied))
    }

    /// Apply a run of committed entries as one atomic write.
    ///
    /// One [`Store::commit`], and therefore one fsync, for the whole run. That
    /// is the whole point of it: with a durable state machine, committing each
    /// entry on its own costs one full disk flush per operation, and on this
    /// machine that pinned write throughput at about a hundred a second no
    /// matter what else improved — the log's own fsync per `Ready` was already
    /// amortised across a batch of thirty entries while the state machine's was
    /// not (ADR-035).
    ///
    /// Atomicity is unchanged and is still the contract ADR-010 describes: the
    /// applied index goes into the same batch as the data, so a crash leaves a
    /// store that has applied everything through the highest index here or
    /// nothing of it, and the log replays from whichever it finds.
    ///
    /// What batching *does* change is that an entry now has to see the entries
    /// ahead of it in the same batch, which the store does not hold yet. Every
    /// read on this path goes through [`read_through`] for that reason: two
    /// increments of one key in one batch that both read the store would both
    /// read the old value.
    ///
    /// Returns one response per entry, in order.
    pub fn apply_batch(
        &mut self,
        entries: &[(Index, Proposal)],
    ) -> Result<Vec<Response>, StateMachineError> {
        let mut batch = Batch::new();
        let mut responses = Vec::with_capacity(entries.len());
        let mut highest = 0;
        let floor = self.store.applied();

        for (index, proposal) in entries {
            // The log replaying below the store's watermark. Nothing to do, and
            // nothing to say: the response for this entry went to a client that
            // has long since had it.
            if *index <= floor {
                responses.push(Response::Applied);
                continue;
            }
            let response = match &proposal.body {
                ProposalBody::Register { nonce } => {
                    self.apply_register(*nonce, proposal.stamped_ms, &mut batch)?
                }
                ProposalBody::KeepAlive => self.apply_keep_alive(proposal, &mut batch)?,
                ProposalBody::Command(command) => {
                    self.apply_command(command, proposal, &mut batch)?
                }
            };
            session::expire(&self.store, proposal.stamped_ms, &mut batch)?;
            responses.push(response);
            highest = highest.max(*index);
        }

        if highest > 0 {
            self.store.commit(highest, batch)?;
        }
        Ok(responses)
    }

    /// Register a client outside the log. For tests and for the doctest above;
    /// a real cluster registers by proposing.
    #[doc(hidden)]
    pub fn register(
        &mut self,
        index: Index,
        stamped_ms: u64,
        nonce: u64,
    ) -> Result<ClientId, StateMachineError> {
        let response = self.apply(
            index,
            &Proposal {
                stamped_ms,
                session: None,
                body: ProposalBody::Register { nonce },
            },
        )?;
        match response {
            Response::Registered { client } => Ok(client),
            other => Err(StateMachineError::Malformed(format!(
                "register produced {other:?}"
            ))),
        }
    }

    fn apply_register(
        &mut self,
        nonce: u64,
        now_ms: u64,
        batch: &mut Batch,
    ) -> Result<Response, StateMachineError> {
        // A nonce already seen means the client never got its answer and asked
        // again. Hand back the identity it was given the first time: allocating
        // a second one would leave it holding two, whose sequence numbers
        // deduplicate against different rows, and the retries it sends under
        // the new one would apply twice.
        if let Some(existing) = session::client_for_nonce(&self.store, batch, nonce)? {
            return Ok(Response::Registered { client: existing });
        }

        let client = self.next_client;
        self.next_client += 1;
        batch.put(
            Space::Internal,
            session::NEXT_CLIENT_KEY,
            Bytes::copy_from_slice(&self.next_client.to_le_bytes()),
        );
        session::record_nonce(nonce, client, batch);
        session::write(
            client,
            &Session {
                last_seq: 0,
                last_seen_ms: now_ms,
                cached: None,
            },
            batch,
        );
        Ok(Response::Registered { client })
    }

    fn apply_keep_alive(
        &mut self,
        proposal: &Proposal,
        batch: &mut Batch,
    ) -> Result<Response, StateMachineError> {
        let Some((client, _)) = proposal.session else {
            return Ok(Response::Error(ApiError::SessionExpired));
        };
        match session::read(&self.store, batch, client)? {
            Some(mut session) => {
                session.last_seen_ms = proposal.stamped_ms;
                session::write(client, &session, batch);
                Ok(Response::Applied)
            }
            None => Ok(Response::Error(ApiError::SessionExpired)),
        }
    }

    fn apply_command(
        &mut self,
        command: &Command,
        proposal: &Proposal,
        batch: &mut Batch,
    ) -> Result<Response, StateMachineError> {
        let Some((client, seq)) = proposal.session else {
            return Ok(Response::Error(ApiError::SessionExpired));
        };
        let Some(mut session) = session::read(&self.store, batch, client)? else {
            // Expired, or never registered. The client re-registers; whether
            // its earlier commands applied is exactly what the session existed
            // to make knowable, and it is now unknowable.
            return Ok(Response::Error(ApiError::SessionExpired));
        };

        match session.replay(seq) {
            session::Replay::Fresh => {}
            // The whole point: the response comes back and nothing is written.
            session::Replay::Duplicate(cached) => return Ok(cached),
            session::Replay::TooOld => {
                return Ok(Response::Error(ApiError::SequenceTooOld {
                    got: seq,
                    floor: session.last_seq,
                }));
            }
        }

        let response = self.execute(command, batch)?;

        session.last_seq = seq;
        session.last_seen_ms = proposal.stamped_ms;
        session.cached = Some(response.clone());
        session::write(client, &session, batch);
        Ok(response)
    }

    fn execute(
        &mut self,
        command: &Command,
        batch: &mut Batch,
    ) -> Result<Response, StateMachineError> {
        Ok(match command {
            Command::Put { key, value } => {
                batch.put(Space::User, key, value.clone());
                Response::Applied
            }
            Command::Delete { key } => {
                batch.delete(Space::User, key);
                Response::Applied
            }
            Command::Cas { key, expect, value } => {
                let actual = read_through(&self.store, batch, Space::User, key)?;
                if actual.as_ref() != expect.as_ref() {
                    Response::CasMismatch { actual }
                } else {
                    match value {
                        Some(v) => batch.put(Space::User, key, v.clone()),
                        None => batch.delete(Space::User, key),
                    };
                    Response::Applied
                }
            }
            Command::Incr { key, by } => match self.counter_in(batch, key) {
                Ok(current) => {
                    let next = current.saturating_add(*by);
                    batch.put(
                        Space::User,
                        key,
                        Bytes::copy_from_slice(&next.to_le_bytes()),
                    );
                    Response::Counter(next)
                }
                // A key holding something that is not a counter is a client
                // error, not a corruption: it wrote bytes there with `put`.
                Err(_) => Response::Error(ApiError::NotACounter),
            },
        })
    }

    /// Read a user key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>, StateMachineError> {
        self.store.get(Space::User, key)
    }

    /// Read a user key range.
    pub fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, StateMachineError> {
        self.store.scan(Space::User, start, end, limit)
    }

    /// The counter at `key`, zero if absent.
    pub fn counter(&self, key: &[u8]) -> Result<i64, StateMachineError> {
        self.counter_in(&Batch::new(), key)
    }

    /// The counter at `key` as `batch` will leave it.
    fn counter_in(&self, batch: &Batch, key: &[u8]) -> Result<i64, StateMachineError> {
        match read_through(&self.store, batch, Space::User, key)? {
            None => Ok(0),
            Some(bytes) => bytes
                .as_ref()
                .try_into()
                .map(i64::from_le_bytes)
                .map_err(|_| {
                    StateMachineError::Malformed(format!(
                        "the value at this key is {} bytes, not a counter",
                        bytes.len()
                    ))
                }),
        }
    }

    /// A hash of everything this state machine holds.
    ///
    /// The number a snapshot transfer carries, so a receiver can say whether
    /// what it installed is what the sender meant to send. A chunk stream that
    /// is checksummed per chunk still proves only that each chunk arrived
    /// intact; this proves the *set* is right, which is the claim that matters
    /// when a transfer resumes and a chunk could have been skipped.
    ///
    /// Deliberately covers the session table as well as the user data. A
    /// snapshot that carried the data and not the sessions would be one a
    /// client's retries could apply a second time on top of, and the digests
    /// would agree about it.
    pub fn state_digest(&self) -> Result<u64, StateMachineError> {
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        let mut mix = |bytes: &[u8]| {
            for byte in bytes {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
        };
        for (key, value) in self.store.scan(Space::User, None, None, usize::MAX)? {
            mix(&key);
            mix(&value);
        }
        for client in session::all(&self.store)? {
            mix(&client.to_be_bytes());
            if let Some(session) = session::read(&self.store, &Batch::new(), client)? {
                mix(&session.last_seq.to_be_bytes());
            }
        }
        Ok(hash)
    }

    /// The session for `client`, if it is still open.
    pub fn session(&self, client: ClientId) -> Result<Option<Session>, StateMachineError> {
        session::read(&self.store, &Batch::new(), client)
    }

    /// Every open session's client id. For metrics and for tests that assert
    /// expiry happened.
    pub fn open_sessions(&self) -> Result<Vec<ClientId>, StateMachineError> {
        session::all(&self.store)
    }

    /// The sequence number `client` has applied through.
    pub fn last_seq(&self, client: ClientId) -> Result<Option<Seq>, StateMachineError> {
        Ok(session::read(&self.store, &Batch::new(), client)?.map(|s| s.last_seq))
    }
}
