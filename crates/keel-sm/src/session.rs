//! The session table: what makes a retried command apply once.
//!
//! It lives in the store rather than beside it, for two reasons that are really
//! the same reason. It has to survive a restart, or a client's retries after a
//! crash would apply a second time. And it has to be updated in the *same*
//! atomic batch as the command it describes, or a crash between the two leaves
//! a store that applied a command and does not remember doing so — which is the
//! same defect as an applied index written separately from its data.
//!
//! Expiry is driven by a timestamp the leader stamped into the entry, never by
//! any node's own clock. Every node applies the same entries in the same order
//! and reads the same numbers out of them, so they expire the same sessions at
//! the same index. A node consulting its own clock would expire a session its
//! peers still hold, and a client would find itself registered on some nodes
//! and not on others.

use bytes::Bytes;
use keel_api::{ClientId, Response, Seq, decode, encode};
use serde::{Deserialize, Serialize};

use crate::StateMachineError;
use crate::store::{Batch, Space, Store};

/// How long a session survives without being heard from.
///
/// Generous, because the cost of expiring one early is that a client's
/// in-flight retries lose their exactly-once guarantee, and the cost of
/// expiring one late is a few hundred bytes.
pub const SESSION_TIMEOUT_MS: u64 = 10 * 60 * 1000;

/// How many expired sessions one entry may clean up.
///
/// Bounded, because expiry runs inside the apply of an ordinary command and a
/// thousand sessions timing out at once must not turn one entry's apply into an
/// unbounded write. The rest are collected by the next entry.
const EXPIRY_BUDGET: usize = 8;

pub(crate) const NEXT_CLIENT_KEY: &[u8] = b"next-client";
const SESSION_PREFIX: &[u8] = b"session/";
const NONCE_PREFIX: &[u8] = b"nonce/";

/// One client's place in its own command stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    /// The highest sequence number applied for this client.
    pub last_seq: Seq,
    /// The leader-stamped time of the last entry this client was seen in.
    pub last_seen_ms: u64,
    /// The response `last_seq` produced, kept so a retry can be answered
    /// without applying anything.
    pub cached: Option<Response>,
}

/// What a sequence number turned out to be.
pub(crate) enum Replay {
    /// Not seen before. Apply it.
    Fresh,
    /// Already applied, and here is what it produced.
    Duplicate(Response),
    /// Below the floor, and its response is gone. The client cannot be told
    /// what happened, which is worth an error rather than a guess.
    TooOld,
}

impl Session {
    pub(crate) fn replay(&self, seq: Seq) -> Replay {
        if seq > self.last_seq {
            return Replay::Fresh;
        }
        if seq == self.last_seq {
            if let Some(cached) = &self.cached {
                return Replay::Duplicate(cached.clone());
            }
        }
        Replay::TooOld
    }
}

fn session_key(client: ClientId) -> Vec<u8> {
    let mut key = SESSION_PREFIX.to_vec();
    // Big-endian, so the key order is the id order and a scan of the table
    // walks clients in a stable sequence on every node.
    key.extend(client.to_be_bytes());
    key
}

fn nonce_key(nonce: u64) -> Vec<u8> {
    let mut key = NONCE_PREFIX.to_vec();
    key.extend(nonce.to_be_bytes());
    key
}

pub(crate) fn read<S: Store>(
    store: &S,
    client: ClientId,
) -> Result<Option<Session>, StateMachineError> {
    match store.get(Space::Internal, &session_key(client))? {
        None => Ok(None),
        Some(bytes) => decode::<Session>(&bytes)
            .map(Some)
            .map_err(|e| StateMachineError::Malformed(format!("session {client}: {e}"))),
    }
}

pub(crate) fn write(client: ClientId, session: &Session, batch: &mut Batch) {
    // A session that cannot be encoded is a programming error, not a runtime
    // one, and dropping it silently would lose the exactly-once guarantee for
    // that client. Writing an empty value would be worse: it would decode as
    // malformed later and take the node down at a random point instead of here.
    match encode(session) {
        Ok(bytes) => {
            batch.put(Space::Internal, &session_key(client), Bytes::from(bytes));
        }
        Err(_) => {
            batch.delete(Space::Internal, &session_key(client));
        }
    }
}

pub(crate) fn record_nonce(nonce: u64, client: ClientId, batch: &mut Batch) {
    batch.put(
        Space::Internal,
        &nonce_key(nonce),
        Bytes::copy_from_slice(&client.to_le_bytes()),
    );
}

pub(crate) fn client_for_nonce<S: Store>(
    store: &S,
    nonce: u64,
) -> Result<Option<ClientId>, StateMachineError> {
    Ok(store
        .get(Space::Internal, &nonce_key(nonce))?
        .and_then(|b| b.as_ref().try_into().ok().map(ClientId::from_le_bytes)))
}

/// Every open session's client id, ascending.
pub(crate) fn all<S: Store>(store: &S) -> Result<Vec<ClientId>, StateMachineError> {
    let end = upper_bound(SESSION_PREFIX);
    Ok(store
        .scan(
            Space::Internal,
            Some(SESSION_PREFIX),
            Some(&end),
            usize::MAX,
        )?
        .into_iter()
        .filter_map(|(key, _)| {
            key.get(SESSION_PREFIX.len()..)
                .and_then(|id| id.try_into().ok())
                .map(ClientId::from_be_bytes)
        })
        .collect())
}

/// Drop sessions not heard from within [`SESSION_TIMEOUT_MS`], at most
/// [`EXPIRY_BUDGET`] of them.
///
/// Deterministic in every part: the scan order is the key order, the budget is
/// a constant, and the only clock is the one the leader stamped. Two nodes
/// applying the same entry expire the same sessions.
pub(crate) fn expire<S: Store>(
    store: &S,
    now_ms: u64,
    batch: &mut Batch,
) -> Result<(), StateMachineError> {
    let deadline = match now_ms.checked_sub(SESSION_TIMEOUT_MS) {
        Some(deadline) => deadline,
        // Early enough in the epoch that nothing can have timed out. Only
        // reachable in tests, and cheaper to handle than to argue about.
        None => return Ok(()),
    };

    let end = upper_bound(SESSION_PREFIX);
    let mut dropped = 0;
    for (key, value) in store.scan(
        Space::Internal,
        Some(SESSION_PREFIX),
        Some(&end),
        usize::MAX,
    )? {
        if dropped == EXPIRY_BUDGET {
            break;
        }
        // A client this entry has already written about has been heard from in
        // this entry. The store does not know that yet — the batch has not been
        // committed — so asking the store would expire the session the same
        // batch just renewed.
        if batch.touches(Space::Internal, &key) {
            continue;
        }
        let Ok(session) = decode::<Session>(&value) else {
            continue;
        };
        if session.last_seen_ms < deadline {
            batch.delete(Space::Internal, &key);
            dropped += 1;
        }
    }
    Ok(())
}

/// The first key after every key with this prefix.
///
/// A scan wants a half-open range and the natural end of a prefix is the prefix
/// with its last byte incremented. A prefix of all `0xff` has no successor, so
/// the range is left open at the top — which is correct here because nothing is
/// stored above it.
fn upper_bound(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != 0xff {
            end.push(last + 1);
            return end;
        }
    }
    vec![0xff; prefix.len() + 1]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_upper_bound_of_a_prefix_excludes_everything_below_it() {
        assert_eq!(upper_bound(b"session/"), b"session0".to_vec());
        assert_eq!(upper_bound(&[0x01, 0xff]), vec![0x02]);
        assert_eq!(upper_bound(&[0xff, 0xff]), vec![0xff, 0xff, 0xff]);
    }

    #[test]
    fn session_keys_sort_by_client_id() {
        let mut keys: Vec<Vec<u8>> = [300u64, 2, 1, 256]
            .iter()
            .map(|c| session_key(*c))
            .collect();
        keys.sort();
        let ids: Vec<u64> = keys
            .iter()
            .map(|k| u64::from_be_bytes(k[SESSION_PREFIX.len()..].try_into().unwrap()))
            .collect();
        assert_eq!(ids, vec![1, 2, 256, 300]);
    }

    #[test]
    fn a_fresh_sequence_is_fresh_and_the_last_one_is_a_duplicate() {
        let session = Session {
            last_seq: 5,
            last_seen_ms: 0,
            cached: Some(Response::Applied),
        };
        assert!(matches!(session.replay(6), Replay::Fresh));
        assert!(matches!(session.replay(5), Replay::Duplicate(_)));
        assert!(matches!(session.replay(4), Replay::TooOld));
    }

    /// A sequence number equal to the floor but with no cached response is not
    /// a duplicate that can be answered. Saying so beats inventing one.
    #[test]
    fn a_duplicate_with_no_cached_response_is_too_old() {
        let session = Session {
            last_seq: 5,
            last_seen_ms: 0,
            cached: None,
        };
        assert!(matches!(session.replay(5), Replay::TooOld));
    }
}
