#![allow(clippy::unwrap_used, clippy::expect_used)]

//! P14's exit criterion: a transfer killed at an arbitrary byte offset resumes
//! at the first chunk whose checksum fails, and completes with the sender's
//! state digest.
//!
//! "An arbitrary offset" is taken literally. The transfer is cut at every chunk
//! boundary in turn rather than at one that happened to work.

use bytes::Bytes;
use keel_api::{Command, Proposal, ProposalBody};
use keel_sm::{Accepted, LsmStore, Receiver, Sender, StateMachine};

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn command(client: u64, seq: u64, key: &str, value: &str) -> Proposal {
    Proposal {
        stamped_ms: 5_000,
        session: Some((client, seq)),
        body: ProposalBody::Command(Command::Put {
            key: b(key),
            value: b(value),
        }),
    }
}

/// Build a checkpoint worth transferring, and return it with the digest of what
/// it holds.
///
/// Large enough to be several chunks: a transfer that fits in one chunk cannot
/// be cut, and every resume test below would pass without testing anything.
/// `tag` varies the contents, so two checkpoints can be told apart.
fn checkpoint_tagged(into: &std::path::Path, tag: &str) -> u64 {
    let source = tempfile::tempdir().unwrap();
    let mut sm = StateMachine::new(LsmStore::open(source.path()).unwrap());
    let client = sm.register(1, 5_000, 7).unwrap();
    for seq in 1..=2_500u64 {
        sm.apply(
            seq + 1,
            &command(
                client,
                seq,
                &format!("k{seq:06}"),
                &format!(
                    "{tag}: a value with enough bytes in it that a few thousand \
                     of them make a snapshot worth chunking {seq}"
                ),
            ),
        )
        .unwrap();
        // The engine spawns no threads here, so a flush happens when asked. One
        // unit per write rather than draining: draining after every write turns
        // building the fixture into a quadratic merge.
        if sm.store().pending_work() {
            sm.store().maintain().unwrap();
        }
    }
    sm.store().checkpoint(into).unwrap();
    sm.state_digest().unwrap()
}

fn checkpoint(into: &std::path::Path) -> u64 {
    checkpoint_tagged(into, "one")
}

fn digest_of(dir: &std::path::Path) -> Result<u64, keel_sm::StateMachineError> {
    StateMachine::new(LsmStore::open(dir)?).state_digest()
}

/// The whole thing, uninterrupted.
#[test]
fn a_transfer_installs_a_snapshot_that_matches_the_senders_digest() {
    let holder = tempfile::tempdir().unwrap();
    let snapshot = holder.path().join("snapshot");
    let digest = checkpoint(&snapshot);

    let mut sender = Sender::new(&snapshot).unwrap();
    let (files, bytes) = sender.size();
    assert!(
        files > 1 && bytes > 0,
        "the snapshot is too small to test with"
    );

    let staging = holder.path().join("staging");
    let mut receiver = Receiver::new(&staging).unwrap();
    while let Some(chunk) = sender.next_chunk().unwrap() {
        assert!(matches!(
            receiver.accept(&chunk).unwrap(),
            Accepted::Written | Accepted::Complete
        ));
    }
    assert!(receiver.is_complete());

    let installed = holder.path().join("installed");
    receiver.publish(&installed, digest, digest_of).unwrap();
    assert_eq!(digest_of(&installed).unwrap(), digest);
}

/// Cut the transfer at every chunk boundary in turn, resume, and check the
/// result. Not one interleaving that worked — all of them.
#[test]
fn a_transfer_cut_at_any_chunk_resumes_and_completes() {
    let holder = tempfile::tempdir().unwrap();
    let snapshot = holder.path().join("snapshot");
    let digest = checkpoint(&snapshot);

    let total_chunks = {
        let mut sender = Sender::new(&snapshot).unwrap();
        let mut n = 0;
        while sender.next_chunk().unwrap().is_some() {
            n += 1;
        }
        n
    };
    assert!(
        total_chunks > 3,
        "only {total_chunks} chunks to cut between"
    );

    for cut in 0..total_chunks {
        let staging = holder.path().join(format!("staging{cut}"));
        let installed = holder.path().join(format!("installed{cut}"));

        // First attempt: send `cut` chunks and stop, as if the receiver died.
        let mut receiver = Receiver::new(&staging).unwrap();
        let mut sender = Sender::new(&snapshot).unwrap();
        for _ in 0..cut {
            let Some(chunk) = sender.next_chunk().unwrap() else {
                break;
            };
            receiver.accept(&chunk).unwrap();
        }
        let position = receiver.position().clone();

        // Second attempt: a fresh sender, resumed from where the receiver got
        // to. The receiver keeps its staging directory and its position.
        let mut sender = Sender::new(&snapshot).unwrap();
        sender.resume_from(&position);
        while let Some(chunk) = sender.next_chunk().unwrap() {
            let outcome = receiver.accept(&chunk).unwrap();
            assert!(
                matches!(outcome, Accepted::Written | Accepted::Complete),
                "cut {cut}: a resumed chunk was {outcome:?}"
            );
        }

        assert!(
            receiver.is_complete(),
            "cut {cut}: the resume never finished"
        );
        receiver
            .publish(&installed, digest, digest_of)
            .unwrap_or_else(|e| panic!("cut {cut}: publishing failed: {e}"));
        assert_eq!(
            digest_of(&installed).unwrap(),
            digest,
            "cut {cut}: the installed snapshot is not the one that was sent"
        );
    }
}

/// A chunk whose checksum fails is not written, and the position does not move
/// past it — so the next attempt sends that chunk again rather than the one
/// after it.
#[test]
fn a_corrupt_chunk_is_rejected_and_the_position_does_not_advance() {
    let holder = tempfile::tempdir().unwrap();
    let snapshot = holder.path().join("snapshot");
    let digest = checkpoint(&snapshot);

    let staging = holder.path().join("staging");
    let mut receiver = Receiver::new(&staging).unwrap();
    let mut sender = Sender::new(&snapshot).unwrap();

    // Two good chunks, then a corrupt one, then the rest.
    let mut sent = 0;
    let mut rejected = 0;
    while let Some(mut chunk) = sender.next_chunk().unwrap() {
        if sent == 2 {
            let before = receiver.position().clone();
            let good = chunk.bytes.clone();
            chunk.bytes[0] ^= 0xff;
            assert_eq!(
                receiver.accept(&chunk).unwrap(),
                Accepted::Rejected,
                "a chunk with a flipped byte was accepted"
            );
            assert_eq!(
                receiver.position(),
                &before,
                "a rejected chunk moved the position, so the resume would skip it"
            );
            rejected += 1;
            chunk.bytes = good;
        }
        receiver.accept(&chunk).unwrap();
        sent += 1;
    }
    assert_eq!(rejected, 1, "the corruption was never injected");

    let installed = holder.path().join("installed");
    receiver.publish(&installed, digest, digest_of).unwrap();
    assert_eq!(digest_of(&installed).unwrap(), digest);
}

/// The check that says the *set* was complete rather than that each chunk
/// arrived intact. A snapshot missing a file publishes nothing.
#[test]
fn a_snapshot_whose_digest_disagrees_is_thrown_away_rather_than_published() {
    let holder = tempfile::tempdir().unwrap();
    let snapshot = holder.path().join("snapshot");
    let digest = checkpoint(&snapshot);

    let staging = holder.path().join("staging");
    let mut receiver = Receiver::new(&staging).unwrap();
    let mut sender = Sender::new(&snapshot).unwrap();
    while let Some(chunk) = sender.next_chunk().unwrap() {
        receiver.accept(&chunk).unwrap();
    }

    let installed = holder.path().join("installed");
    let err = receiver
        .publish(&installed, digest ^ 0xdead_beef, digest_of)
        .expect_err("a snapshot whose digest disagreed was published");
    assert!(
        format!("{err}").contains("the set was wrong"),
        "the refusal did not say what it means: {err}"
    );
    assert!(
        !installed.exists(),
        "a refused snapshot was published anyway"
    );
    assert!(
        !staging.exists(),
        "a refused snapshot's staging directory was left behind for a later \
         transfer to resume into"
    );
}

/// An incomplete transfer cannot be published at all.
#[test]
fn an_incomplete_transfer_refuses_to_publish() {
    let holder = tempfile::tempdir().unwrap();
    let snapshot = holder.path().join("snapshot");
    let digest = checkpoint(&snapshot);

    let staging = holder.path().join("staging");
    let mut receiver = Receiver::new(&staging).unwrap();
    let mut sender = Sender::new(&snapshot).unwrap();
    for _ in 0..2 {
        let chunk = sender.next_chunk().unwrap().unwrap();
        receiver.accept(&chunk).unwrap();
    }
    assert!(!receiver.is_complete());

    let installed = holder.path().join("installed");
    assert!(
        receiver.publish(&installed, digest, digest_of).is_err(),
        "a partial snapshot was published"
    );
    assert!(!installed.exists());
}

/// Publishing replaces an existing snapshot atomically: a reader sees the old
/// one or the new one, never a directory half-way between.
#[test]
fn publishing_over_an_existing_snapshot_replaces_it_whole() {
    let holder = tempfile::tempdir().unwrap();
    let installed = holder.path().join("installed");

    // An older snapshot already in place.
    let first = holder.path().join("first");
    let first_digest = checkpoint(&first);
    std::fs::rename(&first, &installed).unwrap();
    assert_eq!(digest_of(&installed).unwrap(), first_digest);

    // A newer one, transferred over it.
    let second = holder.path().join("second");
    let second_digest = checkpoint_tagged(&second, "two");
    assert_ne!(
        first_digest, second_digest,
        "the two snapshots are identical, so this test cannot tell them apart"
    );

    let staging = holder.path().join("staging");
    let mut receiver = Receiver::new(&staging).unwrap();
    let mut sender = Sender::new(&second).unwrap();
    while let Some(chunk) = sender.next_chunk().unwrap() {
        receiver.accept(&chunk).unwrap();
    }
    receiver
        .publish(&installed, second_digest, digest_of)
        .unwrap();

    assert_eq!(
        digest_of(&installed).unwrap(),
        second_digest,
        "the new snapshot did not replace the old one"
    );
    let leftovers: Vec<String> = std::fs::read_dir(holder.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains("retired"))
        .collect();
    assert!(leftovers.is_empty(), "left behind {leftovers:?}");
}

/// A chunk naming a path outside the snapshot is refused.
#[test]
fn a_chunk_that_names_an_escape_is_refused() {
    let holder = tempfile::tempdir().unwrap();
    let staging = holder.path().join("staging");
    let mut receiver = Receiver::new(&staging).unwrap();

    for name in ["../escape", "/etc/passwd", "a/b", ".."] {
        let bytes = b"payload".to_vec();
        let chunk = keel_sm::Chunk {
            file: name.into(),
            offset: 0,
            crc: crc32c::crc32c(&bytes),
            bytes,
            last: false,
        };
        assert_eq!(
            receiver.accept(&chunk).unwrap(),
            Accepted::Rejected,
            "{name} was accepted"
        );
    }
    assert!(receiver.position().is_empty());
}
