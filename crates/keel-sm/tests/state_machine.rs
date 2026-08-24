#![allow(clippy::unwrap_used, clippy::expect_used)]

//! What a committed entry means, and what a retried one does not.

use bytes::Bytes;
use keel_api::{ApiError, Command, Proposal, ProposalBody, Response};
use keel_sm::{LsmStore, MemStore, SESSION_TIMEOUT_MS, StateMachine, Store, conformance};

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn command(client: u64, seq: u64, at_ms: u64, command: Command) -> Proposal {
    Proposal {
        stamped_ms: at_ms,
        session: Some((client, seq)),
        body: ProposalBody::Command(command),
    }
}

// ------------------------------------------------------------- conformance

#[test]
fn the_memory_store_meets_the_contract() {
    conformance::check(MemStore::new);
}

#[test]
fn the_lsm_store_meets_the_same_contract() {
    // Each store needs its own directory, and each has to outlive the closure
    // that made it, so the directories are kept alive alongside.
    let mut dirs = Vec::new();
    conformance::check(|| {
        let dir = tempfile::tempdir().unwrap();
        let store = LsmStore::open(dir.path()).unwrap();
        dirs.push(dir);
        store
    });
}

// ------------------------------------------------------------ exactly once

/// FR-7, and the reason `Command::Incr` exists. A hundred increments and one
/// retry of each leaves the counter at a hundred, not two hundred.
#[test]
fn a_retry_storm_applies_each_command_exactly_once() {
    let mut sm = StateMachine::new(MemStore::new());
    let client = sm.register(1, 0, 7).unwrap();

    let mut index = 2;
    for seq in 1..=100u64 {
        let entry = command(client, seq, 0, Command::Incr { key: b("n"), by: 1 });
        // The leader replicated it, the client never saw the answer, and it
        // sent the same command again.
        sm.apply(index, &entry).unwrap();
        index += 1;
        sm.apply(index, &entry).unwrap();
        index += 1;
    }

    assert_eq!(
        sm.counter(b"n").unwrap(),
        100,
        "a hundred increments retried once each did not apply a hundred times"
    );
}

/// And the retry is answered, not refused: the client gets the same response it
/// missed the first time.
#[test]
fn a_duplicate_returns_the_cached_response_and_writes_nothing() {
    let mut sm = StateMachine::new(MemStore::new());
    let client = sm.register(1, 0, 7).unwrap();
    let entry = command(client, 1, 0, Command::Incr { key: b("n"), by: 5 });

    let first = sm.apply(2, &entry).unwrap();
    let keys_after_first = sm.store().len();
    let second = sm.apply(3, &entry).unwrap();

    assert_eq!(first, Response::Counter(5));
    assert_eq!(second, first, "the retry got a different answer");
    assert_eq!(
        sm.store().len(),
        keys_after_first,
        "the retry wrote something"
    );
}

/// A sequence number below the floor cannot be answered — its response has been
/// replaced by a newer one — and saying so beats inventing an answer.
#[test]
fn a_sequence_below_the_floor_is_refused_rather_than_guessed_at() {
    let mut sm = StateMachine::new(MemStore::new());
    let client = sm.register(1, 0, 7).unwrap();
    sm.apply(
        2,
        &command(
            client,
            1,
            0,
            Command::Put {
                key: b("k"),
                value: b("v"),
            },
        ),
    )
    .unwrap();
    sm.apply(
        3,
        &command(
            client,
            2,
            0,
            Command::Put {
                key: b("k"),
                value: b("w"),
            },
        ),
    )
    .unwrap();

    let stale = sm
        .apply(
            4,
            &command(
                client,
                1,
                0,
                Command::Put {
                    key: b("k"),
                    value: b("x"),
                },
            ),
        )
        .unwrap();
    assert!(
        matches!(
            stale,
            Response::Error(ApiError::SequenceTooOld { got: 1, floor: 2 })
        ),
        "a stale sequence gave {stale:?}"
    );
    assert_eq!(
        sm.get(b"k").unwrap(),
        Some(b("w")),
        "the stale command applied anyway"
    );
}

// -------------------------------------------------------------- the index

/// The log replaying below the store's watermark changes nothing. This is what
/// makes a restart safe without the state machine having to think about it.
#[test]
fn replaying_the_log_below_the_watermark_applies_nothing() {
    let mut sm = StateMachine::new(MemStore::new());
    let client = sm.register(1, 0, 7).unwrap();
    for seq in 1..=5u64 {
        sm.apply(
            seq + 1,
            &command(client, seq, 0, Command::Incr { key: b("n"), by: 1 }),
        )
        .unwrap();
    }
    assert_eq!(sm.counter(b"n").unwrap(), 5);
    assert_eq!(sm.applied(), 6);

    // The host hands back everything from index 1, as it would after a restart
    // whose log floor is lower than its applied index.
    for seq in 1..=5u64 {
        sm.apply(
            seq + 1,
            &command(client, seq, 0, Command::Incr { key: b("n"), by: 1 }),
        )
        .unwrap();
    }
    assert_eq!(
        sm.counter(b"n").unwrap(),
        5,
        "replaying below the watermark applied a second time"
    );
}

/// A restart reads the applied index back out of the store, because it was
/// written in the same batch as the data.
#[test]
fn a_restart_recovers_the_applied_index_from_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let client;
    {
        let mut sm = StateMachine::new(LsmStore::open(dir.path()).unwrap());
        client = sm.register(1, 0, 7).unwrap();
        for seq in 1..=20u64 {
            sm.apply(
                seq + 1,
                &command(client, seq, 0, Command::Incr { key: b("n"), by: 1 }),
            )
            .unwrap();
        }
        assert_eq!(sm.applied(), 21);
    }

    let sm = StateMachine::new(LsmStore::open(dir.path()).unwrap());
    assert_eq!(sm.applied(), 21, "the applied index did not survive");
    assert_eq!(sm.counter(b"n").unwrap(), 20, "the data did not survive");
    assert_eq!(
        sm.last_seq(client).unwrap(),
        Some(20),
        "the session table did not survive, so retries would apply twice"
    );
}

// ------------------------------------------------------------- registration

/// The nonce is what makes registration retryable. Without it the request that
/// establishes exactly-once delivery would be the one delivered at-least-once.
#[test]
fn re_registering_with_the_same_nonce_returns_the_same_identity() {
    let mut sm = StateMachine::new(MemStore::new());
    let first = sm.register(1, 0, 12345).unwrap();
    let again = sm.register(2, 0, 12345).unwrap();
    assert_eq!(
        first, again,
        "a retried registration allocated a second identity"
    );

    let other = sm.register(3, 0, 999).unwrap();
    assert_ne!(other, first, "two different clients share an identity");
}

/// Two nodes applying the same log hand out the same identities, or a client
/// registered on one would be unknown on another.
#[test]
fn identities_are_a_function_of_the_log() {
    let log: Vec<u64> = vec![11, 22, 33];
    let run = || {
        let mut sm = StateMachine::new(MemStore::new());
        log.iter()
            .enumerate()
            .map(|(i, nonce)| sm.register(i as u64 + 1, 0, *nonce).unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(run(), run());
}

// ------------------------------------------------------------------ expiry

/// Expiry reads the leader's stamp, never a local clock — so it is a function
/// of the log and two nodes expire the same sessions.
#[test]
fn a_session_expires_on_the_leaders_clock_and_only_on_it() {
    let mut sm = StateMachine::new(MemStore::new());
    let idle = sm.register(1, 1_000, 1).unwrap();
    let busy = sm.register(2, 1_000, 2).unwrap();

    let long_after = 1_000 + SESSION_TIMEOUT_MS + 1;
    // The busy client is heard from at the later time; the idle one is not.
    sm.apply(
        3,
        &Proposal {
            stamped_ms: long_after,
            session: Some((busy, 0)),
            body: ProposalBody::KeepAlive,
        },
    )
    .unwrap();

    assert_eq!(
        sm.open_sessions().unwrap(),
        vec![busy],
        "the idle session was not expired, or the busy one was"
    );
    assert!(sm.session(idle).unwrap().is_none());
}

/// A command from an expired session is refused rather than applied without
/// deduplication. Applying it would be worse than refusing it: the client would
/// believe it had exactly-once delivery it no longer has.
#[test]
fn a_command_from_an_expired_session_is_refused() {
    let mut sm = StateMachine::new(MemStore::new());
    let client = sm.register(1, 1_000, 1).unwrap();
    let long_after = 1_000 + SESSION_TIMEOUT_MS + 1;

    // Something else happens later, which expires the idle session.
    let other = sm.register(2, long_after, 2).unwrap();
    assert!(sm.session(client).unwrap().is_none());
    assert!(sm.session(other).unwrap().is_some());

    let refused = sm
        .apply(
            3,
            &command(
                client,
                1,
                long_after,
                Command::Put {
                    key: b("k"),
                    value: b("v"),
                },
            ),
        )
        .unwrap();
    assert!(matches!(refused, Response::Error(ApiError::SessionExpired)));
    assert_eq!(sm.get(b"k").unwrap(), None, "the refused command applied");
}

// ---------------------------------------------------------------- commands

#[test]
fn compare_and_swap_takes_effect_only_when_it_matches() {
    let mut sm = StateMachine::new(MemStore::new());
    let client = sm.register(1, 0, 7).unwrap();

    // Create-if-absent.
    let created = sm
        .apply(
            2,
            &command(
                client,
                1,
                0,
                Command::Cas {
                    key: b("k"),
                    expect: None,
                    value: Some(b("v")),
                },
            ),
        )
        .unwrap();
    assert_eq!(created, Response::Applied);
    assert_eq!(sm.get(b"k").unwrap(), Some(b("v")));

    // The same again, now that it exists.
    let refused = sm
        .apply(
            3,
            &command(
                client,
                2,
                0,
                Command::Cas {
                    key: b("k"),
                    expect: None,
                    value: Some(b("w")),
                },
            ),
        )
        .unwrap();
    assert_eq!(
        refused,
        Response::CasMismatch {
            actual: Some(b("v"))
        },
        "a mismatched compare-and-swap did not report what was actually there"
    );
    assert_eq!(sm.get(b"k").unwrap(), Some(b("v")), "it applied anyway");

    // Delete-if-unchanged.
    let deleted = sm
        .apply(
            4,
            &command(
                client,
                3,
                0,
                Command::Cas {
                    key: b("k"),
                    expect: Some(b("v")),
                    value: None,
                },
            ),
        )
        .unwrap();
    assert_eq!(deleted, Response::Applied);
    assert_eq!(sm.get(b"k").unwrap(), None);
}

#[test]
fn incrementing_a_key_that_is_not_a_counter_is_a_client_error() {
    let mut sm = StateMachine::new(MemStore::new());
    let client = sm.register(1, 0, 7).unwrap();
    sm.apply(
        2,
        &command(
            client,
            1,
            0,
            Command::Put {
                key: b("k"),
                value: b("not a number"),
            },
        ),
    )
    .unwrap();

    let refused = sm
        .apply(
            3,
            &command(client, 2, 0, Command::Incr { key: b("k"), by: 1 }),
        )
        .unwrap();
    assert!(matches!(refused, Response::Error(ApiError::NotACounter)));
    assert_eq!(
        sm.get(b"k").unwrap(),
        Some(b("not a number")),
        "a refused increment overwrote the value"
    );
}

#[test]
fn a_scan_returns_user_keys_and_not_the_machines_own() {
    let mut sm = StateMachine::new(MemStore::new());
    let client = sm.register(1, 0, 7).unwrap();
    for (i, key) in ["a", "b", "c"].iter().enumerate() {
        sm.apply(
            i as u64 + 2,
            &command(
                client,
                i as u64 + 1,
                0,
                Command::Put {
                    key: b(key),
                    value: b("v"),
                },
            ),
        )
        .unwrap();
    }

    let got = sm.scan(None, None, usize::MAX).unwrap();
    assert_eq!(
        got.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        vec![b("a"), b("b"), b("c")],
        "a scan of user keys returned the session table too"
    );
}

/// The two stores agree about what a log means, not only about what a key
/// lookup returns. Generic rather than boxed, because the point is that the
/// same code drives both.
#[test]
fn both_stores_apply_the_same_log_to_the_same_state() {
    fn drive<S: Store>(mut sm: StateMachine<S>) -> (i64, Vec<u64>) {
        let client = sm.register(1, 5_000, 7).unwrap();
        for seq in 1..=30u64 {
            sm.apply(
                seq + 1,
                &command(client, seq, 5_000, Command::Incr { key: b("n"), by: 2 }),
            )
            .unwrap();
        }
        (
            sm.counter(b("n").as_ref()).unwrap(),
            sm.open_sessions().unwrap(),
        )
    }

    let dir = tempfile::tempdir().unwrap();
    let in_memory = drive(StateMachine::new(MemStore::new()));
    let on_disk = drive(StateMachine::new(LsmStore::open(dir.path()).unwrap()));
    assert_eq!(
        in_memory, on_disk,
        "the two stores diverged on the same log"
    );
}
