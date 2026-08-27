// Helpers here sit outside `#[test]` fns, so clippy's in-tests exemption does
// not reach them. Unwrapping is the point: in a test the panic is the
// assertion.
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! What the log does with a directory that a crash left behind.
//!
//! These tests corrupt real files on a real filesystem, because the recovery
//! parser is where torn-tail bugs actually live and a model of a disk cannot
//! reproduce one. The simulator will drive this same code over a
//! fault-injecting filesystem (M1 Phase 2); this is the fixed, hand-written
//! half.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use keel_log::{Error, LogOptions, Recovered, StdFs, StdLog, SyncMode};
use keel_raft::{ConfState, Entry, EntryPayload, HardState, Index, SnapshotMeta, Term};
use tempfile::TempDir;

fn opts() -> LogOptions {
    LogOptions {
        // Small enough that a few dozen records roll a segment, so the
        // multi-segment paths are exercised without writing megabytes.
        segment_bytes: 1024,
        max_record_bytes: 512,
        sync_mode: SyncMode::None,
        preallocate: true,
        ..LogOptions::default()
    }
}

fn entry(term: Term, index: Index) -> Entry {
    Entry::new(term, index, EntryPayload::Noop)
}

fn open(dir: &Path) -> (StdLog, Recovered) {
    StdLog::open(StdFs, dir, opts()).expect("open should succeed")
}

fn indices(r: &Recovered) -> Vec<Index> {
    r.entries.iter().map(|e| e.index).collect()
}

fn segments(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            let name = p.file_name()?.to_str()?.to_string();
            name.starts_with("seg-").then_some(p)
        })
        .collect();
    v.sort();
    v
}

/// Overwrite `len` bytes at `off` with `byte`, the way a torn or shredded write
/// leaves them.
fn scribble(path: &Path, off: u64, len: usize, byte: u8) {
    let mut f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(off)).unwrap();
    f.write_all(&vec![byte; len]).unwrap();
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

/// The offset just past the last non-zero byte — where the writer had got to.
fn written_end(path: &Path) -> u64 {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    bytes.iter().rposition(|b| *b != 0).map_or(0, |i| i + 1) as u64
}

#[test]
fn an_empty_directory_opens_clean() {
    let dir = TempDir::new().unwrap();
    let (log, rec) = open(dir.path());

    assert!(rec.entries.is_empty());
    assert_eq!(rec.segments, 1);
    assert_eq!(rec.discarded_tail_bytes, 0);
    assert_eq!(log.last_index(), 0);
}

#[test]
fn a_clean_shutdown_recovers_everything() {
    let dir = TempDir::new().unwrap();
    {
        let (mut log, _) = open(dir.path());
        log.append(&[entry(1, 1), entry(1, 2)]).unwrap();
        log.set_hard_state(HardState {
            term: 3,
            voted_for: Some(7),
            commit: 2,
        })
        .unwrap();
        log.append(&[entry(3, 3)]).unwrap();
        log.sync().unwrap();
    }

    let (log, rec) = open(dir.path());
    assert_eq!(indices(&rec), vec![1, 2, 3]);
    assert_eq!(rec.hard_state.term, 3);
    assert_eq!(rec.hard_state.voted_for, Some(7));
    assert_eq!(rec.hard_state.commit, 2);
    assert_eq!(rec.discarded_tail_bytes, 0);
    assert!(!rec.clamped_commit);
    assert_eq!(log.last_index(), 3);
}

#[test]
fn a_truncation_survives_a_restart() {
    let dir = TempDir::new().unwrap();
    {
        let (mut log, _) = open(dir.path());
        log.append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .unwrap();
        log.truncate(2).unwrap();
        log.append(&[entry(4, 2)]).unwrap();
        log.sync().unwrap();
    }

    let (_, rec) = open(dir.path());
    assert_eq!(indices(&rec), vec![1, 2]);
    assert_eq!(
        rec.entries[1].term, 4,
        "the replacement won, not the original"
    );
}

#[test]
fn a_torn_trailing_record_is_discarded_and_the_prefix_survives() {
    let dir = TempDir::new().unwrap();
    let seg = {
        let (mut log, _) = open(dir.path());
        log.append(&[entry(1, 1), entry(1, 2)]).unwrap();
        log.sync().unwrap();
        let seg = segments(dir.path())[0].clone();
        // A third record lands half-written, the way a crash mid-append leaves it.
        log.append(&[entry(1, 3)]).unwrap();
        seg
    };
    let end = written_end(&seg);
    scribble(&seg, end - 4, 4, 0xab);

    let (log, rec) = open(dir.path());
    assert_eq!(indices(&rec), vec![1, 2], "the intact prefix is kept");
    assert!(rec.discarded_tail_bytes > 0, "and the tear is reported");
    assert_eq!(log.last_index(), 2);
}

/// The reason recovery erases the tail rather than just moving the cursor back.
#[test]
fn a_torn_record_cannot_be_resurrected_by_a_shorter_one_written_over_it() {
    let dir = TempDir::new().unwrap();
    let seg = {
        let (mut log, _) = open(dir.path());
        log.append(&[entry(1, 1)]).unwrap();
        log.sync().unwrap();
        // A long record, then damage to its first byte only. Its *tail* stays
        // on disk and is a valid-looking frame boundary for whatever the next
        // recovery writes there.
        let payload = EntryPayload::Normal(vec![0xcd; 200].into());
        log.append(&[Entry::new(1, 2, payload)]).unwrap();
        segments(dir.path())[0].clone()
    };
    let before = written_end(&seg);
    scribble(&seg, before - 210, 1, 0x00); // corrupt the long record's length

    let (mut log, rec) = open(dir.path());
    assert_eq!(indices(&rec), vec![1]);

    // Write a much shorter record over the same region and reopen. If the old
    // record's tail were still there, this scan would run straight off the end
    // of the new one into it.
    log.append(&[entry(9, 2)]).unwrap();
    log.sync().unwrap();
    drop(log);

    let (_, rec) = open(dir.path());
    assert_eq!(indices(&rec), vec![1, 2]);
    assert_eq!(rec.entries[1].term, 9);
    assert_eq!(rec.discarded_tail_bytes, 0, "nothing is left to trip over");
}

/// Append a prefix, then two more records, then lose the first of the two the
/// way a crash loses a page that never reached the device. Returns the segment,
/// the offset the hole starts at, and the offset the surviving record sits at.
fn a_hole_with_a_record_above_it(dir: &Path) -> (PathBuf, u64, u64) {
    let (mut log, _) = open(dir);
    log.append(&[entry(1, 1)]).unwrap();
    log.sync().unwrap();

    let seg = segments(dir)[0].clone();
    let hole = written_end(&seg);
    log.append(&[entry(1, 2)]).unwrap();
    let above = written_end(&seg);
    log.append(&[entry(1, 3)]).unwrap();
    log.sync().unwrap();
    drop(log);

    // The earlier write is gone and the later one survived. A device commits
    // whole blocks and nothing orders those completions, so a hole is a state a
    // real crash reaches — and the bytes it leaves behind are indistinguishable
    // from the preallocated zeros the scan is meant to stop at.
    scribble(&seg, hole, (above - hole) as usize, 0x00);
    (seg, hole, above)
}

#[test]
fn a_hole_a_crash_left_is_not_read_as_the_clean_end_it_looks_like() {
    let dir = TempDir::new().unwrap();
    let (seg, hole, _) = a_hole_with_a_record_above_it(dir.path());
    assert!(
        written_end(&seg) > hole,
        "the fixture is wrong: nothing survived above the hole, so this test \
         is not about the case it says it is"
    );

    let (log, rec) = open(dir.path());

    assert_eq!(indices(&rec), vec![1], "the fold stops at the hole");
    assert_eq!(log.last_index(), 1);
    assert_eq!(
        written_end(&seg),
        hole,
        "the survivor is still on disk after recovery, so the next record \
         written at the cursor can be read straight past into it"
    );
    assert!(
        rec.discarded_tail_bytes > 0,
        "recovery called this crash clean. Whether bytes sit above the cursor \
         does not depend on how the scan stopped, and reporting zero here is \
         what lets them survive to be misread on a later open"
    );
}

/// How many bytes one `append` of `e` writes, as the second record of a log.
fn appended_len(e: Entry) -> u64 {
    let dir = TempDir::new().unwrap();
    let (mut log, _) = open(dir.path());
    log.append(&[entry(1, 1)]).unwrap();
    let seg = segments(dir.path())[0].clone();
    let before = written_end(&seg);
    log.append(&[e]).unwrap();
    written_end(&seg) - before
}

/// The other half of [`a_torn_record_cannot_be_resurrected_by_a_shorter_one_written_over_it`],
/// for the tear that takes the *head* of a write rather than its tail.
#[test]
fn a_record_above_a_hole_is_erased_rather_than_read_on_the_next_open() {
    let dir = TempDir::new().unwrap();
    let (_, hole, above) = a_hole_with_a_record_above_it(dir.path());

    // A replacement of exactly the encoded length of the record that was lost.
    // That is the common case rather than the corner: records of one shape are
    // one length, so a log re-appending what it lost puts its cursor back on
    // the survivor's first byte.
    assert_eq!(
        appended_len(entry(9, 2)),
        above - hole,
        "the fixture is wrong: the replacement is a different length from the \
         record it replaces, so the cursor never reaches the survivor and \
         nothing here is being tested"
    );

    let (mut log, _) = open(dir.path());
    log.append(&[entry(9, 2)]).unwrap();
    log.sync().unwrap();
    drop(log);

    let (log, rec) = open(dir.path());

    assert_eq!(
        indices(&rec),
        vec![1, 2],
        "an entry from before the crash was read back as though this \
         incarnation had written it, which is a Raft log inventing an entry"
    );
    assert_eq!(
        rec.entries[1].term, 9,
        "and the replacement is what survived"
    );
    assert_eq!(log.last_index(), 2);
}

#[test]
fn a_commit_index_the_tear_took_with_it_is_clamped_and_reported() {
    let dir = TempDir::new().unwrap();
    let seg = {
        let (mut log, _) = open(dir.path());
        log.append(&[entry(1, 1), entry(1, 2)]).unwrap();
        // The hard state is durable; the entries it names are about to not be.
        log.set_hard_state(HardState {
            term: 1,
            voted_for: None,
            commit: 4,
        })
        .unwrap();
        log.sync().unwrap();
        segments(dir.path())[0].clone()
    };
    let _ = seg;

    let (_, rec) = open(dir.path());
    assert_eq!(
        rec.hard_state.commit, 2,
        "clamped to what is actually there"
    );
    assert!(rec.clamped_commit, "and reported, because it is evidence");
    assert_eq!(rec.hard_state.term, 1, "the term and vote are untouched");
}

#[test]
fn a_segment_whose_header_never_landed_is_discarded() {
    let dir = TempDir::new().unwrap();
    {
        let (mut log, _) = open(dir.path());
        log.append(&[entry(1, 1)]).unwrap();
        log.sync().unwrap();
    }
    // A crash inside rollover: the file exists, its header does not.
    std::fs::write(dir.path().join("seg-0000000001.log"), vec![0u8; 1024]).unwrap();

    let (_, rec) = open(dir.path());
    assert_eq!(indices(&rec), vec![1]);
    assert_eq!(rec.segments, 1, "the headerless tail segment was removed");
    assert!(!dir.path().join("seg-0000000001.log").exists());
}

#[test]
fn damage_below_the_end_of_the_log_is_refused_rather_than_guessed_past() {
    let dir = TempDir::new().unwrap();
    {
        let (mut log, _) = open(dir.path());
        // Enough to roll past the first segment.
        for i in 1..=200u64 {
            log.append(&[entry(1, i)]).unwrap();
        }
        log.sync().unwrap();
    }
    let segs = segments(dir.path());
    assert!(segs.len() > 1, "the test needs more than one segment");

    // Corrupt the *first* segment. This is not a crash artifact — a torn tail
    // can only be at the end — so reading past it would silently produce a log
    // with a hole in it.
    let end = written_end(&segs[0]);
    scribble(&segs[0], end / 2, 8, 0xff);

    match StdLog::open(StdFs, dir.path(), opts()) {
        Err(Error::Damaged { .. }) => {}
        Err(e) => panic!("expected Damaged, got {e:?}"),
        Ok(_) => panic!("expected the open to be refused"),
    }
}

#[test]
fn a_headerless_segment_below_the_end_is_also_refused() {
    let dir = TempDir::new().unwrap();
    {
        let (mut log, _) = open(dir.path());
        for i in 1..=200u64 {
            log.append(&[entry(1, i)]).unwrap();
        }
        log.sync().unwrap();
    }
    let segs = segments(dir.path());
    assert!(segs.len() > 1);
    std::fs::write(&segs[0], vec![0u8; 1024]).unwrap();

    assert!(matches!(
        StdLog::open(StdFs, dir.path(), opts()),
        Err(Error::Damaged { .. })
    ));
}

#[test]
fn the_log_rolls_segments_and_reads_back_across_them() {
    let dir = TempDir::new().unwrap();
    {
        let (mut log, _) = open(dir.path());
        for i in 1..=300u64 {
            log.append(&[entry(1, i)]).unwrap();
        }
        log.sync().unwrap();
        assert!(log.stats().segments > 1, "the test needs a rollover");
    }

    let (log, rec) = open(dir.path());
    assert_eq!(rec.entries.len(), 300);
    assert_eq!(log.last_index(), 300);
    let slice = log.read(100, 105).unwrap();
    assert_eq!(
        slice.iter().map(|e| e.index).collect::<Vec<_>>(),
        vec![100, 101, 102, 103, 104, 105]
    );
}

#[test]
fn a_snapshot_replaces_the_log_it_covers() {
    let dir = TempDir::new().unwrap();
    {
        let (mut log, _) = open(dir.path());
        log.append(&[entry(1, 1), entry(1, 2), entry(1, 3)])
            .unwrap();
        log.install_snapshot(&SnapshotMeta {
            index: 2,
            term: 1,
            conf: ConfState::single([1]),
        })
        .unwrap();
        log.sync().unwrap();
    }

    let (log, rec) = open(dir.path());
    assert_eq!(indices(&rec), vec![3]);
    assert_eq!(rec.snapshot.as_ref().map(|s| s.index), Some(2));
    assert_eq!(log.last_index(), 3);
}

#[test]
fn compaction_drops_only_segments_the_snapshot_covers() {
    let dir = TempDir::new().unwrap();
    let (mut log, _) = open(dir.path());
    for i in 1..=300u64 {
        log.append(&[entry(1, i)]).unwrap();
    }
    log.sync().unwrap();
    let before = log.stats().segments;
    assert!(before > 2, "the test needs several segments");

    log.install_snapshot(&SnapshotMeta {
        index: 100,
        term: 1,
        conf: ConfState::single([1]),
    })
    .unwrap();
    let removed = log.compact_to(100).unwrap();
    assert!(removed > 0, "at least one whole segment is below index 100");
    assert!(log.stats().segments < before);
    drop(log);

    // The hard state and the snapshot were re-emitted before the drop, so they
    // survive losing the segments that originally carried them.
    let (log, rec) = open(dir.path());
    assert_eq!(rec.snapshot.as_ref().map(|s| s.index), Some(100));
    assert_eq!(log.last_index(), 300);
    assert_eq!(rec.entries.first().map(|e| e.index), Some(101));
}

#[test]
fn a_second_handle_on_a_live_directory_is_refused() {
    let dir = TempDir::new().unwrap();
    let (log, _) = open(dir.path());

    match StdLog::open(StdFs, dir.path(), opts()) {
        Err(Error::Locked(p)) => assert_eq!(p, dir.path()),
        other => panic!("expected Locked, got {:?}", other.map(|_| "Log")),
    }

    drop(log);
    StdLog::open(StdFs, dir.path(), opts()).expect("released on drop");
}

#[test]
fn entries_that_do_not_continue_the_log_are_refused_at_the_door() {
    let dir = TempDir::new().unwrap();
    let (mut log, _) = open(dir.path());
    log.append(&[entry(1, 1), entry(1, 2)]).unwrap();

    let err = log.append(&[entry(4, 2)]).unwrap_err();
    assert!(
        matches!(
            err,
            Error::Discontiguous {
                expected: 3,
                found: 2
            }
        ),
        "got {err:?}"
    );
}

#[test]
fn a_record_larger_than_the_limit_is_refused() {
    let dir = TempDir::new().unwrap();
    let (mut log, _) = open(dir.path());
    let big = Entry::new(1, 1, EntryPayload::Normal(vec![0u8; 1024].into()));

    assert!(matches!(
        log.append(&[big]),
        Err(Error::RecordTooLarge { .. })
    ));
}

#[test]
fn a_large_ready_is_split_into_recoverable_records() {
    let dir = TempDir::new().unwrap();
    let (mut log, _) = open(dir.path());
    let entries: Vec<_> = (1..=6)
        .map(|index| Entry::new(1, index, EntryPayload::Normal(vec![0u8; 180].into())))
        .collect();

    log.append(&entries).unwrap();
    log.sync().unwrap();
    drop(log);

    let (_, recovered) = open(dir.path());
    assert_eq!(indices(&recovered), (1..=6).collect::<Vec<_>>());
}

/// A sync covers what had been written when it was issued, and nothing later.
/// The token ordering is what the host uses to decide when a `Ready`'s messages
/// may go out, and getting it loose is what KEEL-5 was.
#[test]
fn a_sync_covers_exactly_what_was_written_before_it() {
    let dir = TempDir::new().unwrap();
    let (mut log, _) = open(dir.path());

    let first = log.append(&[entry(1, 1)]).unwrap();
    let durable = log.sync().unwrap();
    assert!(durable >= first);

    let second = log.append(&[entry(1, 2)]).unwrap();
    assert!(
        second > durable,
        "a later write is not covered by an earlier sync"
    );
    assert_eq!(log.durable(), durable);

    let durable = log.sync().unwrap();
    assert!(durable >= second);
}

#[test]
fn preallocation_leaves_the_segment_at_its_full_size_from_the_start() {
    let dir = TempDir::new().unwrap();
    let (mut log, _) = open(dir.path());
    let seg = segments(dir.path())[0].clone();
    assert_eq!(file_len(&seg), opts().segment_bytes);

    log.append(&[entry(1, 1)]).unwrap();
    log.sync().unwrap();
    assert_eq!(
        file_len(&seg),
        opts().segment_bytes,
        "an append must never change the file size — that is what keeps a \
         directory fsync off the append path"
    );
}
