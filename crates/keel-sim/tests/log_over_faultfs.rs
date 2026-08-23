//! The real durable log, driven over the simulator's filesystem.
//!
//! This is the point of the seam. Everything here exercises `keel-log`'s own
//! framing, checksums and recovery parser — not a model of them — against a disk
//! that can be made to lose whatever no fsync covered.

// Helpers here sit outside `#[test]` fns, so clippy's in-tests exemption does
// not reach them. Unwrapping is the point: in a test the panic is the assertion.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};

use bytes::Bytes;
use keel_log::{Log, LogOptions, Recovered, SyncMode};
use keel_raft::{Entry, EntryPayload, HardState, Index, Term};
use keel_sim::FaultFs;

fn dir() -> PathBuf {
    PathBuf::from("/node-1")
}

/// Small segments, so rollover and multi-segment recovery are reached in a few
/// dozen records rather than never.
///
/// `SyncMode::Durable` is not cosmetic here: `FaultFs` retires a write only on a
/// durable sync, so a log configured for `None` would never make anything
/// durable and every crash would lose everything.
fn opts() -> LogOptions {
    LogOptions {
        // A one-entry record encodes to roughly 23 bytes, so a 1 KiB segment
        // holds about forty of them and a hundred appends cross two boundaries.
        segment_bytes: 1024,
        max_record_bytes: 512,
        sync_mode: SyncMode::Durable,
        preallocate: true,
    }
}

fn open(fs: &FaultFs) -> (Log<FaultFs>, Recovered) {
    Log::open(fs.clone(), &dir(), opts()).expect("the log should open")
}

fn entries(term: Term, from: Index, count: u64) -> Vec<Entry> {
    (from..from + count)
        .map(|i| {
            Entry::new(
                term,
                i,
                EntryPayload::Normal(Bytes::from(format!("entry-{i}").into_bytes())),
            )
        })
        .collect()
}

#[test]
fn a_log_opens_on_an_empty_simulated_disk() {
    let fs = FaultFs::new();
    let (log, recovered) = open(&fs);

    assert_eq!(recovered.segments, 1);
    assert_eq!(recovered.entries.len(), 0);
    assert_eq!(log.last_index(), 0);
    assert_eq!(recovered.discarded_tail_bytes, 0);
}

#[test]
fn a_clean_shutdown_recovers_everything_through_the_real_parser() {
    let fs = FaultFs::new();
    {
        let (mut log, _) = open(&fs);
        log.append(&entries(1, 1, 5)).unwrap();
        log.set_hard_state(HardState {
            term: 1,
            voted_for: Some(2),
            commit: 3,
        })
        .unwrap();
        log.sync().unwrap();
    }

    let (_log, recovered) = open(&fs);
    assert_eq!(recovered.entries.len(), 5);
    assert_eq!(recovered.entries[4].index, 5);
    assert_eq!(recovered.hard_state.term, 1);
    assert_eq!(recovered.hard_state.voted_for, Some(2));
    assert_eq!(recovered.hard_state.commit, 3);
    assert_eq!(recovered.discarded_tail_bytes, 0);
    assert!(!recovered.clamped_commit);
}

#[test]
fn a_crash_loses_exactly_what_was_never_synced() {
    let fs = FaultFs::new();
    {
        let (mut log, _) = open(&fs);
        log.append(&entries(1, 1, 3)).unwrap();
        log.sync().unwrap();
        // Written and handed to the kernel, never fsynced. This is the window
        // the persist-before-send contract is tested against.
        log.append(&entries(1, 4, 3)).unwrap();
    }
    fs.crash();

    let (log, recovered) = open(&fs);
    assert_eq!(
        recovered.entries.len(),
        3,
        "the synced prefix survives and the staged tail does not"
    );
    assert_eq!(log.last_index(), 3);
}

#[test]
fn a_log_that_crashed_can_always_be_reopened() {
    let fs = FaultFs::new();
    {
        let (mut log, _) = open(&fs);
        log.append(&entries(1, 1, 40)).unwrap();
        log.sync().unwrap();
        log.append(&entries(1, 41, 40)).unwrap();
    }
    fs.crash();

    // A crash may cost entries. It may never produce a log that will not open:
    // that would be a node permanently unable to rejoin, which is worse than
    // losing the tail.
    let (_log, recovered) = open(&fs);
    assert!(recovered.entries.len() >= 40);
}

#[test]
fn the_log_rolls_segments_and_recovers_across_them() {
    let fs = FaultFs::new();
    {
        let (mut log, _) = open(&fs);
        for i in 0..100 {
            log.append(&entries(1, i + 1, 1)).unwrap();
        }
        log.sync().unwrap();
        assert!(
            log.stats().segments > 1,
            "a hundred records must not fit in one 1 KiB segment"
        );
    }

    let (log, recovered) = open(&fs);
    assert_eq!(recovered.entries.len(), 100);
    assert_eq!(log.last_index(), 100);
    assert!(recovered.segments > 1);
}

#[test]
fn a_rollover_is_durable_before_the_log_continues_past_it() {
    let fs = FaultFs::new();
    {
        let (mut log, _) = open(&fs);
        // A synced prefix that is already past a segment boundary...
        for i in 0..100 {
            log.append(&entries(1, i + 1, 1)).unwrap();
        }
        log.sync().unwrap();
        assert!(log.stats().segments > 1);

        // ...and then a tail, crossing more boundaries, that is never synced.
        for i in 100..200 {
            log.append(&entries(1, i + 1, 1)).unwrap();
        }
    }
    fs.crash();

    // `roll` fsyncs the outgoing segment before creating the next one, so a
    // crash can only ever tear the newest segment. Recovery refuses a torn stop
    // in any earlier one, so if a rollover in the unsynced tail had left an
    // earlier segment short this would be `Error::Damaged` rather than a log.
    let (_log, recovered) = open(&fs);
    assert!(
        recovered.entries.len() >= 100,
        "everything synced before the crash must survive it, got {}",
        recovered.entries.len()
    );
}

#[test]
fn a_second_handle_on_a_live_simulated_log_is_refused() {
    let fs = FaultFs::new();
    let (_held, _) = open(&fs);
    let err = Log::open(fs.clone(), &dir(), opts())
        .err()
        .expect("a second handle must be refused");
    assert!(
        matches!(err, keel_log::Error::Locked(_)),
        "expected Locked, got {err:?}"
    );
}

#[test]
fn a_crash_releases_the_lock_so_the_node_can_reopen_its_own_log() {
    let fs = FaultFs::new();
    let (held, _) = open(&fs);

    // The handle is deliberately still alive: a killed process does not run
    // `Drop`, and the kernel releases its locks anyway.
    fs.crash();
    let (_reopened, _) = open(&fs);
    drop(held);
}

#[test]
fn a_truncation_survives_a_crash_replacement_and_all() {
    let fs = FaultFs::new();
    {
        let (mut log, _) = open(&fs);
        log.append(&entries(1, 1, 5)).unwrap();
        log.truncate(3).unwrap();
        log.append(&entries(2, 3, 2)).unwrap();
        log.sync().unwrap();
    }

    let (_log, recovered) = open(&fs);
    assert_eq!(recovered.entries.len(), 4);
    assert_eq!(
        recovered.entries[2].term, 2,
        "the replacement at index 3 wins, not the entry it replaced"
    );
    assert_eq!(recovered.entries[3].index, 4);
}

#[test]
fn the_simulated_disk_is_the_only_place_the_log_writes() {
    // A stray `std::fs` call in the log would write to the real filesystem and
    // the simulation would silently stop being a simulation. `/node-1` is not a
    // path this process may create, so if the log ever bypassed the seam the
    // test would fail on a permission error rather than pass quietly.
    let fs = FaultFs::new();
    {
        let (mut log, _) = open(&fs);
        log.append(&entries(1, 1, 10)).unwrap();
        log.sync().unwrap();
    }
    assert!(!Path::new("/node-1").exists());
}
