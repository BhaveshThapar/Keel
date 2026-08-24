#![allow(clippy::unwrap_used, clippy::expect_used)]

//! P15's exit criterion: a fresh node is brought up past a compacted log floor
//! by a snapshot, killed mid-stream, and completes **without restarting the
//! transfer**.
//!
//! The last clause is the one worth testing. A transfer that restarts is a
//! transfer that works; a transfer that resumes is one that will still finish
//! when the state is a gigabyte and the link keeps dropping.

use std::collections::BTreeMap;

use bytes::Bytes;
use keel_api::{Command, Proposal, ProposalBody};
use keel_node::{ENTRIES_BETWEEN_CHECKPOINTS, Incoming, Outgoing, checkpoint_is_due};

/// Small enough that a few thousand keys make dozens of chunks, so a resume is
/// exercised without building a gigabyte to exercise it.
const CHUNK: usize = 4 * 1024;
use keel_raft::{ConfState, SnapshotMeta};
use keel_sm::{Accepted, LsmStore, StateMachine};

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn put(client: u64, seq: u64, key: &str, value: &str) -> Proposal {
    Proposal {
        stamped_ms: 5_000,
        session: Some((client, seq)),
        body: ProposalBody::Command(Command::Put {
            key: b(key),
            value: b(value),
        }),
    }
}

fn digest_of(dir: &std::path::Path) -> Result<u64, keel_sm::StateMachineError> {
    StateMachine::new(LsmStore::open(dir)?).state_digest()
}

/// A leader with state worth snapshotting, and the checkpoint it took.
struct Leader {
    _dir: tempfile::TempDir,
    checkpoint: std::path::PathBuf,
    digest: u64,
    applied: u64,
    keys: usize,
}

fn leader_with_state(keys: usize) -> Leader {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = StateMachine::new(LsmStore::open(dir.path().join("state")).unwrap());
    let client = sm.register(1, 5_000, 7).unwrap();
    for seq in 1..=keys as u64 {
        sm.apply(
            seq + 1,
            &put(
                client,
                seq,
                &format!("k{seq:06}"),
                "a value with enough bytes in it to make a snapshot worth chunking",
            ),
        )
        .unwrap();
        if sm.store().pending_work() {
            sm.store().maintain().unwrap();
        }
    }
    let checkpoint = dir.path().join("checkpoint");
    sm.store().checkpoint(&checkpoint).unwrap();
    Leader {
        digest: sm.state_digest().unwrap(),
        applied: sm.applied(),
        checkpoint,
        keys,
        _dir: dir,
    }
}

/// The exit criterion.
#[test]
fn a_fresh_node_is_brought_up_by_a_snapshot_that_is_killed_mid_stream() {
    let leader = leader_with_state(2_500);
    let follower = tempfile::tempdir().unwrap();
    let installed = follower.path().join("state");
    let staging = follower.path().join("staging");

    let meta = SnapshotMeta {
        index: leader.applied,
        term: 4,
        conf: ConfState {
            voters: vec![1, 2],
            ..ConfState::default()
        },
    };

    // --- the first attempt, killed part-way
    let mut sending =
        Outgoing::with_chunk_bytes(2, meta.clone(), &leader.checkpoint, CHUNK).unwrap();
    let mut receiving = Incoming::new(1, meta.clone(), &staging).unwrap();

    let mut delivered = 0;
    while delivered < 4 {
        let Some(chunk) = sending.next_chunk().unwrap() else {
            break;
        };
        receiving.accept(&chunk).unwrap();
        delivered += 1;
    }
    let position = receiving.position();
    let accepted_before = receiving.chunks_accepted;
    assert_eq!(
        accepted_before, 4,
        "the first attempt did not deliver four chunks"
    );
    assert!(
        !receiving.is_complete(),
        "the whole snapshot fitted in four chunks, so there is nothing to resume"
    );
    assert!(
        position.values().sum::<u64>() > 0,
        "the receiver verified nothing, so there is no position to resume from"
    );

    // The leader is replaced, or simply forgets. Everything about the transfer
    // that survives is what the receiver has on disk and can describe.
    drop(sending);

    // --- the resume, on a fresh sender
    let mut sending =
        Outgoing::with_chunk_bytes(2, meta.clone(), &leader.checkpoint, CHUNK).unwrap();
    sending.resume_from(position.clone());

    while let Some(chunk) = sending.next_chunk().unwrap() {
        let outcome = receiving.accept(&chunk).unwrap();
        assert!(
            matches!(outcome, Accepted::Written | Accepted::Complete),
            "a resumed chunk was {outcome:?}"
        );
    }
    assert!(
        receiving.is_complete(),
        "the resumed transfer never finished"
    );
    assert_eq!(
        receiving.chunks_rejected, 0,
        "the resume sent chunks the receiver already had"
    );

    // The clause the criterion is about: the second attempt sent fewer chunks
    // than the whole snapshot, because it did not start again.
    let total_chunks = {
        let mut whole =
            Outgoing::with_chunk_bytes(2, meta.clone(), &leader.checkpoint, CHUNK).unwrap();
        let mut n = 0;
        while whole.next_chunk().unwrap().is_some() {
            n += 1;
        }
        n
    };
    assert!(
        sending.chunks_sent < total_chunks,
        "the resume sent {} chunks and the whole snapshot is {total_chunks}; it \
         restarted rather than resumed",
        sending.chunks_sent
    );
    assert_eq!(
        sending.chunks_sent + accepted_before,
        total_chunks,
        "the two attempts together did not cover the snapshot exactly once"
    );

    // --- and what arrived is what was sent
    receiving
        .publish(&installed, leader.digest, digest_of)
        .unwrap();
    let restored = StateMachine::new(LsmStore::open(&installed).unwrap());
    assert_eq!(restored.state_digest().unwrap(), leader.digest);
    assert_eq!(
        restored.applied(),
        leader.applied,
        "the installed snapshot is not at the index it was taken at"
    );
    for seq in 1..=leader.keys as u64 {
        assert!(
            restored
                .get(format!("k{seq:06}").as_bytes())
                .unwrap()
                .is_some(),
            "key {seq} was not in the installed snapshot"
        );
    }
}

/// A transfer interrupted more than once still converges, and still never
/// re-sends what has been verified.
#[test]
fn a_transfer_interrupted_repeatedly_still_converges() {
    let leader = leader_with_state(2_000);
    let follower = tempfile::tempdir().unwrap();
    let staging = follower.path().join("staging");
    let installed = follower.path().join("state");

    let meta = SnapshotMeta {
        index: leader.applied,
        term: 2,
        conf: ConfState::single([1]),
    };
    let mut receiving = Incoming::new(1, meta.clone(), &staging).unwrap();

    let mut attempts = 0;
    let mut sent_in_total = 0u64;
    while !receiving.is_complete() {
        attempts += 1;
        assert!(attempts < 100, "the transfer never converged");

        let mut sending =
            Outgoing::with_chunk_bytes(2, meta.clone(), &leader.checkpoint, CHUNK).unwrap();
        sending.resume_from(receiving.position());
        // Two chunks per attempt, then the link drops again.
        for _ in 0..2 {
            let Some(chunk) = sending.next_chunk().unwrap() else {
                break;
            };
            receiving.accept(&chunk).unwrap();
        }
        sent_in_total += sending.chunks_sent;
    }

    assert!(
        attempts > 3,
        "only {attempts} attempts; nothing was interrupted"
    );
    assert_eq!(
        sent_in_total, receiving.chunks_accepted,
        "chunks were sent that the receiver did not accept, so a resume \
         re-sent what was already verified"
    );

    receiving
        .publish(&installed, leader.digest, digest_of)
        .unwrap();
    assert_eq!(digest_of(&installed).unwrap(), leader.digest);
}

/// Taking a checkpoint is triggered by work done, not by time passed.
#[test]
fn checkpoints_are_due_by_entries_applied() {
    assert!(!checkpoint_is_due(5, 0));
    assert!(checkpoint_is_due(ENTRIES_BETWEEN_CHECKPOINTS + 1, 0));
    assert!(!checkpoint_is_due(
        ENTRIES_BETWEEN_CHECKPOINTS + 1,
        ENTRIES_BETWEEN_CHECKPOINTS
    ));
}

/// A leader offers the position it was told, and a receiver that has verified
/// nothing gets the whole snapshot.
#[test]
fn a_receiver_that_has_verified_nothing_gets_the_whole_snapshot() {
    let leader = leader_with_state(500);
    let meta = SnapshotMeta {
        index: leader.applied,
        term: 1,
        conf: ConfState::single([1]),
    };

    let whole = {
        let mut sending =
            Outgoing::with_chunk_bytes(2, meta.clone(), &leader.checkpoint, CHUNK).unwrap();
        while sending.next_chunk().unwrap().is_some() {}
        sending.chunks_sent
    };

    let mut sending = Outgoing::with_chunk_bytes(2, meta, &leader.checkpoint, CHUNK).unwrap();
    sending.resume_from(BTreeMap::new());
    while sending.next_chunk().unwrap().is_some() {}
    assert_eq!(
        sending.chunks_sent, whole,
        "resuming from nothing skipped part of the snapshot"
    );
}
