//! What Keel puts on a wire: the client protocol, the peer protocol, and the
//! one encoding both use.
//!
//! Two things here are load-bearing rather than stylistic.
//!
//! **A command and a query are different types.** A command changes state and
//! goes through the log; a query does not and must not. Making them one enum
//! with a `is_read()` predicate would put a single missing branch between a read
//! and an unreplicated write, and that is not a mistake worth leaving reachable.
//! [`Request`] can carry either, and nothing can carry a [`Query`] where a
//! [`Command`] is required.
//!
//! **A session is `(client, seq)` plus a nonce.** The pair is what makes a
//! retried command apply exactly once (FR-7). The nonce is what makes the
//! *registration* retryable: without it a client that never saw its
//! `Registered` response and asked again would be given a second identity, and
//! the retries it then sent under the new one would apply a second time — the
//! exactly-once guarantee defeated by the one request that sets it up.
//!
//! Framing is deliberately not here. A length prefix is a property of a byte
//! stream, and `keel-net` owns byte streams; the Maelstrom adapter carries these
//! same types over line-delimited JSON on stdin and stdout and wants none of it.
//! What this crate does own is a bound on how large a decoded payload may be, so
//! that a transport which forgot to check is still not the only thing standing
//! between a corrupt length and an allocation.
//!
//! ```
//! use keel_api::{Command, Request, encode, decode};
//!
//! let request = Request::Command {
//!     client: 7,
//!     seq: 3,
//!     command: Command::Put { key: b"k".to_vec().into(), value: b"v".to_vec().into() },
//! };
//! let bytes = encode(&request).unwrap();
//! assert_eq!(decode::<Request>(&bytes).unwrap(), request);
//! ```

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub use keel_raft::{Index, Message, NodeId, Term};

/// The largest payload this crate will decode.
///
/// A bound belongs at the decoder rather than only at the transport, because a
/// decoder is reachable from more than one transport and only one of them has to
/// forget. Sixteen mebibytes is far above any legitimate message: the largest
/// thing on this wire is an `AppendEntries` batch, which the core already bounds
/// by entry count and payload size, and a snapshot chunk, which is bounded by
/// the chunk size the sender chose.
pub const MAX_PAYLOAD_BYTES: usize = 16 << 20;

/// A client's identity, handed out by [`Request::Register`] and valid until the
/// session expires.
pub type ClientId = u64;

/// Position within one client's stream of commands. Strictly increasing, and the
/// second half of the key the state machine deduplicates on.
pub type Seq = u64;

// ---------------------------------------------------------------- commands

/// A request that changes state. Every one of these goes through the log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Put {
        key: Bytes,
        value: Bytes,
    },
    Delete {
        key: Bytes,
    },
    /// Compare and swap. `expect` of `None` means "only if absent", and `value`
    /// of `None` means "delete", so this subsumes create-if-absent and
    /// delete-if-unchanged without a separate verb for each.
    Cas {
        key: Bytes,
        expect: Option<Bytes>,
        value: Option<Bytes>,
    },
}

/// A request that does not change state, and therefore must never reach the log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Query {
    Get {
        key: Bytes,
    },
    /// Half-open: `start` included, `end` excluded, `None` meaning unbounded.
    Scan {
        start: Option<Bytes>,
        end: Option<Bytes>,
        limit: u32,
    },
}

/// How fresh a read has to be.
///
/// The default is the safe one. A lease read is correct only inside a clock
/// assumption, and a stale read is correct only if the caller asked for it —
/// neither is something to fall into by leaving a field off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Consistency {
    /// ReadIndex: the leader confirms it is still the leader by heartbeat before
    /// answering, and will not answer at all until its own term's no-op has
    /// committed.
    #[default]
    Linearizable,
    /// Answered from the leader's lease without a round trip. Correct while the
    /// clock drift between nodes stays inside the bound the lease was sized for,
    /// and not otherwise (ADR-005).
    Lease,
    /// Answered by whichever node received it, from whatever it has applied. May
    /// return a value that has since been overwritten, or miss one that has
    /// already been acknowledged.
    Stale,
}

// ---------------------------------------------------------------- requests

/// What a client sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Request {
    /// Open a session. The nonce makes this retryable: a server that has already
    /// registered this nonce returns the identity it handed out the first time
    /// rather than allocating a second one.
    ///
    /// Without that, the request that establishes exactly-once delivery would be
    /// the one request delivered at-least-once, and a client that retried it
    /// would hold two identities whose sequence numbers deduplicate against
    /// different rows.
    Register { nonce: u64 },
    Command {
        client: ClientId,
        seq: Seq,
        command: Command,
    },
    Query {
        consistency: Consistency,
        query: Query,
    },
    /// Report liveness so the session's expiry does not run out under an idle
    /// client. Carries no `seq`: it is not deduplicated because applying it
    /// twice is applying it once.
    KeepAlive { client: ClientId },
}

/// What a server sends back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    Registered {
        client: ClientId,
    },
    /// A `Put` or `Delete` that applied, or a `Cas` that took effect.
    Applied,
    /// A `Cas` whose `expect` did not match. Carries what was actually there, so
    /// a caller retrying a read-modify-write does not need a second round trip.
    CasMismatch {
        actual: Option<Bytes>,
    },
    Value(Option<Bytes>),
    Scanned(Vec<(Bytes, Bytes)>),
    /// This node is not the leader. `leader` is a hint and may be stale or
    /// absent; a client treats it as somewhere to try next, not as truth.
    NotLeader {
        leader: Option<NodeId>,
    },
    Error(ApiError),
}

/// Why a request could not be answered.
///
/// Each of these is a condition the client is expected to handle, which is why
/// they are an enum and not a string: `SessionExpired` means re-register,
/// `Unavailable` means retry elsewhere, and `TooLarge` means do not.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum ApiError {
    /// The session is gone — expired, or never registered. Register again; the
    /// commands sent under the old identity may or may not have applied, which
    /// is exactly what the session existed to make knowable.
    #[error("session expired or unknown")]
    SessionExpired,
    /// A sequence number at or below one already applied for this client, and
    /// whose cached response has been evicted. Not the same as a duplicate: a
    /// duplicate returns the cached response.
    #[error("sequence number {got} is below the session's floor {floor}")]
    SequenceTooOld { got: Seq, floor: Seq },
    /// No quorum, or a leader that has not yet committed its own term's no-op.
    #[error("no quorum available")]
    Unavailable,
    /// The request would exceed a bound the server enforces.
    #[error("request too large: {got} bytes against a limit of {limit}")]
    TooLarge { got: usize, limit: usize },
    /// The node has latched a fatal storage error and is refusing everything. It
    /// is not going to recover without an operator.
    #[error("this node has failed and is refusing requests: {0}")]
    NodeFailed(String),
}

// ------------------------------------------------------------------- peers

/// What one node sends another.
///
/// Peer traffic and client traffic are separate enums because they are separate
/// protocols with separate compatibility stories: a client older than the
/// cluster is ordinary, and a node older than its peers is an upgrade in
/// progress. Sharing one enum would make every change to either a change to
/// both.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Peer {
    /// A consensus message, straight from the core.
    Raft(Message),
    /// One chunk of a snapshot stream (M2). Kept out of `Raft` because a chunk
    /// is bulk data on its own flow-control regime, and putting it in the
    /// consensus enum would make every heartbeat carry the size of the largest
    /// variant.
    SnapshotChunk {
        index: Index,
        term: Term,
        offset: u64,
        last: bool,
        data: Bytes,
    },
}

// ---------------------------------------------------------------- encoding

/// Why a payload could not be turned into bytes, or back.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("payload is {got} bytes, above the {MAX_PAYLOAD_BYTES}-byte limit")]
    TooLarge { got: usize },
    #[error("malformed payload: {0}")]
    Malformed(#[from] postcard::Error),
    /// Bytes decoded, but not all of them were consumed. A well-formed payload
    /// with something appended is not a well-formed payload, and accepting it
    /// would let a sender smuggle bytes past every length check upstream.
    #[error("{trailing} trailing bytes after a complete payload")]
    Trailing { trailing: usize },
}

/// Encode a payload.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    let bytes = postcard::to_stdvec(value)?;
    if bytes.len() > MAX_PAYLOAD_BYTES {
        return Err(CodecError::TooLarge { got: bytes.len() });
    }
    Ok(bytes)
}

/// Decode a payload, refusing anything oversized *before* postcard sees it and
/// anything with bytes left over after it.
pub fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, CodecError> {
    if bytes.len() > MAX_PAYLOAD_BYTES {
        return Err(CodecError::TooLarge { got: bytes.len() });
    }
    let (value, rest) = postcard::take_from_bytes(bytes)?;
    if !rest.is_empty() {
        return Err(CodecError::Trailing {
            trailing: rest.len(),
        });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use keel_raft::{Entry, EntryPayload, MessageBody};

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn every_request() -> Vec<Request> {
        vec![
            Request::Register { nonce: 0xdead_beef },
            Request::Command {
                client: 1,
                seq: 2,
                command: Command::Put {
                    key: b("k"),
                    value: b("v"),
                },
            },
            Request::Command {
                client: 1,
                seq: 3,
                command: Command::Delete { key: b("k") },
            },
            Request::Command {
                client: 1,
                seq: 4,
                command: Command::Cas {
                    key: b("k"),
                    expect: None,
                    value: Some(b("v")),
                },
            },
            Request::Command {
                client: 1,
                seq: 5,
                command: Command::Cas {
                    key: b("k"),
                    expect: Some(b("v")),
                    value: None,
                },
            },
            Request::Query {
                consistency: Consistency::Linearizable,
                query: Query::Get { key: b("k") },
            },
            Request::Query {
                consistency: Consistency::Lease,
                query: Query::Scan {
                    start: Some(b("a")),
                    end: None,
                    limit: 100,
                },
            },
            Request::Query {
                consistency: Consistency::Stale,
                query: Query::Scan {
                    start: None,
                    end: Some(b("z")),
                    limit: 0,
                },
            },
            Request::KeepAlive { client: 9 },
        ]
    }

    fn every_response() -> Vec<Response> {
        vec![
            Response::Registered { client: 4 },
            Response::Applied,
            Response::CasMismatch {
                actual: Some(b("other")),
            },
            Response::CasMismatch { actual: None },
            Response::Value(Some(b("v"))),
            Response::Value(None),
            Response::Scanned(vec![(b("a"), b("1")), (b("b"), b("2"))]),
            Response::Scanned(vec![]),
            Response::NotLeader { leader: Some(3) },
            Response::NotLeader { leader: None },
            Response::Error(ApiError::SessionExpired),
            Response::Error(ApiError::SequenceTooOld { got: 1, floor: 9 }),
            Response::Error(ApiError::Unavailable),
            Response::Error(ApiError::TooLarge {
                got: 1 << 30,
                limit: MAX_PAYLOAD_BYTES,
            }),
            Response::Error(ApiError::NodeFailed("disk is gone".into())),
        ]
    }

    fn every_peer_message() -> Vec<Peer> {
        let entries = vec![
            Entry::new(3, 7, EntryPayload::Noop),
            Entry::new(3, 8, EntryPayload::Normal(b("payload"))),
        ];
        vec![
            Peer::Raft(Message {
                from: 1,
                to: 2,
                term: 3,
                body: MessageBody::AppendReq {
                    prev_log_index: 6,
                    prev_log_term: 2,
                    entries,
                    leader_commit: 6,
                },
            }),
            Peer::SnapshotChunk {
                index: 900,
                term: 4,
                offset: 4096,
                last: false,
                data: b("chunk"),
            },
        ]
    }

    #[test]
    fn every_request_round_trips() {
        for request in every_request() {
            let bytes = encode(&request).unwrap();
            assert_eq!(decode::<Request>(&bytes).unwrap(), request);
        }
    }

    #[test]
    fn every_response_round_trips() {
        for response in every_response() {
            let bytes = encode(&response).unwrap();
            assert_eq!(decode::<Response>(&bytes).unwrap(), response);
        }
    }

    #[test]
    fn every_peer_message_round_trips() {
        for peer in every_peer_message() {
            let bytes = encode(&peer).unwrap();
            assert_eq!(decode::<Peer>(&bytes).unwrap(), peer);
        }
    }

    /// Two payloads that differ must not encode to the same bytes, or a
    /// deduplicating cache keyed on the encoding would confuse them.
    #[test]
    fn distinct_requests_encode_distinctly() {
        let mut seen = std::collections::HashSet::new();
        for request in every_request() {
            assert!(
                seen.insert(encode(&request).unwrap()),
                "two distinct requests encoded identically: {request:?}"
            );
        }
    }

    #[test]
    fn a_payload_with_bytes_appended_is_refused() {
        let mut bytes = encode(&Request::KeepAlive { client: 1 }).unwrap();
        bytes.push(0);
        assert!(matches!(
            decode::<Request>(&bytes),
            Err(CodecError::Trailing { trailing: 1 })
        ));
    }

    #[test]
    fn a_truncated_payload_is_refused_rather_than_guessed() {
        let bytes = encode(&Request::Command {
            client: 1,
            seq: 2,
            command: Command::Put {
                key: b("key"),
                value: b("value"),
            },
        })
        .unwrap();
        for cut in 0..bytes.len() {
            // Whatever it does, it does not panic and it does not return the
            // original: a prefix of a message is not a message.
            match decode::<Request>(&bytes[..cut]) {
                Err(_) => {}
                Ok(other) => assert_ne!(
                    encode(&other).unwrap(),
                    bytes,
                    "a {cut}-byte prefix decoded back to the whole request"
                ),
            }
        }
    }

    #[test]
    fn an_oversized_payload_is_refused_before_it_is_parsed() {
        let huge = vec![0u8; MAX_PAYLOAD_BYTES + 1];
        assert!(matches!(
            decode::<Request>(&huge),
            Err(CodecError::TooLarge { .. })
        ));
    }

    /// Arbitrary bytes must not panic the decoder. This is the shape the fuzz
    /// target at P22 generalises; having it as a unit test now means the target
    /// starts from a decoder that already survives the obvious cases.
    #[test]
    fn arbitrary_bytes_do_not_panic_the_decoder() {
        let mut rng = keel_rand_stub(0x5eed);
        for _ in 0..2_000 {
            let len = (rng() % 64) as usize;
            let bytes: Vec<u8> = (0..len).map(|_| (rng() & 0xff) as u8).collect();
            let _ = decode::<Request>(&bytes);
            let _ = decode::<Response>(&bytes);
            let _ = decode::<Peer>(&bytes);
        }
    }

    /// A four-line generator rather than a dependency: this crate's manifest is
    /// part of what it promises, and it has no business pulling in a random
    /// number generator to run one test.
    fn keel_rand_stub(seed: u64) -> impl FnMut() -> u64 {
        let mut state = seed;
        move || {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
    }
}
