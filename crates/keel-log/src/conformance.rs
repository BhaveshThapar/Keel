//! The behaviour every [`Fs`] implementation must share.
//!
//! The seam's real cost is that two implementations of the same nine methods
//! have to agree. If they drift, the simulator stops testing the log and starts
//! testing a log-shaped thing, and — worse — the drift shows up as a *passing*
//! run rather than a failing one. So the assertions live here once, and both
//! implementations are held to them: `StdFs` by this crate's own tests, and the
//! simulator's fault-injecting filesystem by the simulator's.
//!
//! Enabled by the `conformance` feature, so none of it is in a production
//! build.

use std::io::ErrorKind;
use std::path::Path;

use crate::fs::{File, Fs, OpenMode, SyncMode};

/// How many assertions [`check`] runs.
///
/// The table below is typed `[_; ASSERTIONS]`, so adding an assertion without
/// bumping this — or deleting one and leaving it — fails to compile rather than
/// leaving a documented count quietly wrong. A suite that has silently shrunk
/// is the same hazard as a suite that has silently drifted.
pub const ASSERTIONS: usize = 14;

/// Run every conformance assertion against `fs`, using `dir` as scratch. `dir`
/// must exist and be empty.
///
/// Panics with a message naming the assertion that failed, because this is a
/// test harness and a returned error would just be unwrapped by the caller.
pub fn check<F: Fs>(fs: &F, dir: &Path) {
    // Each assertion names itself in its own panic message, so the table
    // carries no names. What it does carry is the count.
    let assertions: [fn(&F, &Path); ASSERTIONS] = [
        open_create_does_not_truncate,
        read_at_is_short_at_the_end_and_empty_past_it,
        write_past_the_end_zero_fills_the_gap,
        allocate_makes_the_region_read_as_zeros,
        allocate_never_shrinks,
        size_is_the_high_water_mark_of_allocate_and_write,
        two_handles_on_one_path_see_one_file,
        list_names_what_was_created_and_not_what_was_removed,
        read_of_a_missing_file_fails,
        remove_of_a_missing_file_fails,
        a_second_lock_is_refused_with_would_block,
        a_lock_is_released_when_its_handle_is_dropped,
        sync_dir_succeeds,
        every_sync_mode_is_accepted,
    ];
    for assertion in assertions {
        assertion(fs, dir);
    }
}

fn fresh<F: Fs>(fs: &F, dir: &Path, name: &str) -> F::File {
    let path = dir.join(name);
    let _ = fs.remove(&path);
    match fs.open(&path, OpenMode::Create) {
        Ok(f) => f,
        Err(e) => panic!("open({name}, Create) failed: {e}"),
    }
}

fn open_create_does_not_truncate<F: Fs>(fs: &F, dir: &Path) {
    let mut f = fresh(fs, dir, "create.bin");
    f.write_at(0, b"hello").unwrap_or_else(|e| panic!("{e}"));
    drop(f);

    // Reopening a file the log is still appending to must not lose it.
    let f = fs
        .open(&dir.join("create.bin"), OpenMode::Create)
        .unwrap_or_else(|e| panic!("{e}"));
    let mut buf = [0u8; 5];
    let n = f.read_at(0, &mut buf).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        (n, &buf),
        (5, b"hello"),
        "OpenMode::Create must not truncate"
    );
}

fn read_at_is_short_at_the_end_and_empty_past_it<F: Fs>(fs: &F, dir: &Path) {
    let mut f = fresh(fs, dir, "short.bin");
    f.write_at(0, b"abcd").unwrap_or_else(|e| panic!("{e}"));

    let mut buf = [0u8; 16];
    let n = f.read_at(2, &mut buf).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(n, 2, "a read that runs off the end returns what is there");
    assert_eq!(&buf[..2], b"cd");

    let n = f.read_at(99, &mut buf).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(n, 0, "a read entirely past the end returns nothing");
}

fn write_past_the_end_zero_fills_the_gap<F: Fs>(fs: &F, dir: &Path) {
    let mut f = fresh(fs, dir, "sparse.bin");
    f.write_at(8, b"xy").unwrap_or_else(|e| panic!("{e}"));

    assert_eq!(f.size().unwrap_or_else(|e| panic!("{e}")), 10);
    let mut buf = [0xffu8; 10];
    let n = f.read_at(0, &mut buf).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(n, 10);
    assert_eq!(
        &buf[..8],
        &[0u8; 8],
        "the skipped region must read as zeros — the record format treats a \
         zero length as the end of the written region, so garbage there would \
         be read as a record"
    );
    assert_eq!(&buf[8..], b"xy");
}

fn allocate_makes_the_region_read_as_zeros<F: Fs>(fs: &F, dir: &Path) {
    let mut f = fresh(fs, dir, "alloc.bin");
    f.allocate(4096).unwrap_or_else(|e| panic!("{e}"));

    assert_eq!(f.size().unwrap_or_else(|e| panic!("{e}")), 4096);
    let mut buf = vec![0xffu8; 4096];
    let n = f.read_at(0, &mut buf).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(n, 4096);
    assert!(
        buf.iter().all(|b| *b == 0),
        "preallocated space must read as zeros: that is what makes \
         preallocation and torn-tail detection the same mechanism"
    );
}

fn allocate_never_shrinks<F: Fs>(fs: &F, dir: &Path) {
    let mut f = fresh(fs, dir, "shrink.bin");
    f.allocate(2048).unwrap_or_else(|e| panic!("{e}"));
    f.write_at(0, b"keep me").unwrap_or_else(|e| panic!("{e}"));

    f.allocate(512).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        f.size().unwrap_or_else(|e| panic!("{e}")),
        2048,
        "allocate grows; it must never truncate a file with records in it"
    );
    let mut buf = [0u8; 7];
    f.read_at(0, &mut buf).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(&buf, b"keep me");
}

fn size_is_the_high_water_mark_of_allocate_and_write<F: Fs>(fs: &F, dir: &Path) {
    let mut f = fresh(fs, dir, "highwater.bin");
    f.allocate(2048).unwrap_or_else(|e| panic!("{e}"));
    f.write_at(4096, b"beyond")
        .unwrap_or_else(|e| panic!("{e}"));

    // `Log::read_all` sizes its buffer from `size()` and reads the whole file in
    // one call, so a byte `size()` does not cover is a byte the recovery parser
    // cannot see — it would read as a short file and the tail would look like a
    // clean end rather than the record it is.
    assert_eq!(
        f.size().unwrap_or_else(|e| panic!("{e}")),
        4102,
        "size must cover the furthest write, not just the allocation"
    );

    f.allocate(1024).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        f.size().unwrap_or_else(|e| panic!("{e}")),
        4102,
        "an allocation below the high-water mark moves nothing"
    );
}

fn two_handles_on_one_path_see_one_file<F: Fs>(fs: &F, dir: &Path) {
    let path = dir.join("shared.bin");
    let _ = fs.remove(&path);
    let mut writer = fs
        .open(&path, OpenMode::Create)
        .unwrap_or_else(|e| panic!("{e}"));
    writer.allocate(1024).unwrap_or_else(|e| panic!("{e}"));
    writer
        .write_at(0, b"first")
        .unwrap_or_else(|e| panic!("{e}"));

    // `Log::open` reads every segment through a fresh `Read` handle while the
    // newest segment is still open for writing, and `Log::read` does it again on
    // a live log. A write visible only through the handle that made it would
    // make recovery parse a stale file and report a torn tail that never
    // happened.
    let reader = fs
        .open(&path, OpenMode::Read)
        .unwrap_or_else(|e| panic!("{e}"));
    let mut buf = [0u8; 5];
    let n = reader
        .read_at(0, &mut buf)
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        (n, &buf),
        (5, b"first"),
        "a write must be visible through another handle on the same path"
    );
    assert_eq!(
        reader.size().unwrap_or_else(|e| panic!("{e}")),
        1024,
        "and so must an allocation"
    );
}

fn a_second_lock_is_refused_with_would_block<F: Fs>(fs: &F, dir: &Path) {
    let path = dir.join("contended.lock");
    let _ = fs.remove(&path);
    let _held = fs
        .open(&path, OpenMode::Lock)
        .unwrap_or_else(|e| panic!("taking a free lock failed: {e}"));

    let err = fs
        .open(&path, OpenMode::Lock)
        .err()
        .unwrap_or_else(|| panic!("a second exclusive lock must be refused"));

    // The kind is part of the contract rather than an implementation detail:
    // `Log::open` maps exactly `WouldBlock` to `Error::Locked` and everything
    // else to `Error::Io`, so an implementation that refused with a different
    // kind would turn "another process holds this log" into "the disk failed".
    assert_eq!(
        err.kind(),
        ErrorKind::WouldBlock,
        "a held lock must be refused with WouldBlock specifically"
    );
}

fn a_lock_is_released_when_its_handle_is_dropped<F: Fs>(fs: &F, dir: &Path) {
    let path = dir.join("released.lock");
    let _ = fs.remove(&path);
    let held = fs
        .open(&path, OpenMode::Lock)
        .unwrap_or_else(|e| panic!("{e}"));
    drop(held);

    // A killed process releases its lock because the kernel closes its
    // descriptors, and a simulated crash has to do the same or a restarted node
    // could never reopen its own log.
    let _retaken = fs
        .open(&path, OpenMode::Lock)
        .unwrap_or_else(|e| panic!("a lock must be retakeable once its holder is dropped: {e}"));
}

fn list_names_what_was_created_and_not_what_was_removed<F: Fs>(fs: &F, dir: &Path) {
    let sub = dir.join("listing");
    let _ = fs.remove(&sub);
    let a = dir.join("list-a.bin");
    let b = dir.join("list-b.bin");
    let _ = fs.remove(&a);
    let _ = fs.remove(&b);
    drop(fresh(fs, dir, "list-a.bin"));
    drop(fresh(fs, dir, "list-b.bin"));

    let names: Vec<String> = fs
        .list(dir)
        .unwrap_or_else(|e| panic!("{e}"))
        .iter()
        .filter_map(|p| p.file_name()?.to_str().map(str::to_owned))
        .collect();
    assert!(names.contains(&"list-a.bin".to_string()));
    assert!(names.contains(&"list-b.bin".to_string()));

    fs.remove(&a).unwrap_or_else(|e| panic!("{e}"));
    let names: Vec<String> = fs
        .list(dir)
        .unwrap_or_else(|e| panic!("{e}"))
        .iter()
        .filter_map(|p| p.file_name()?.to_str().map(str::to_owned))
        .collect();
    assert!(!names.contains(&"list-a.bin".to_string()));
    assert!(names.contains(&"list-b.bin".to_string()));
}

fn read_of_a_missing_file_fails<F: Fs>(fs: &F, dir: &Path) {
    let err = fs
        .open(&dir.join("nope.bin"), OpenMode::Read)
        .err()
        .unwrap_or_else(|| panic!("opening a missing file for Read must fail"));
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

fn remove_of_a_missing_file_fails<F: Fs>(fs: &F, dir: &Path) {
    let err = fs
        .remove(&dir.join("nope.bin"))
        .err()
        .unwrap_or_else(|| panic!("removing a missing file must fail"));
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

fn sync_dir_succeeds<F: Fs>(fs: &F, dir: &Path) {
    fs.sync_dir(dir).unwrap_or_else(|e| panic!("sync_dir: {e}"));
}

fn every_sync_mode_is_accepted<F: Fs>(fs: &F, dir: &Path) {
    let mut f = fresh(fs, dir, "sync.bin");
    f.write_at(0, b"data").unwrap_or_else(|e| panic!("{e}"));
    for mode in [SyncMode::None, SyncMode::Barrier, SyncMode::Durable] {
        f.sync(mode)
            .unwrap_or_else(|e| panic!("sync({mode:?}): {e}"));
    }
    assert!(SyncMode::Durable.is_durable());
    assert!(
        !SyncMode::Barrier.is_durable(),
        "a barrier orders writes; it does not survive power loss, and a \
         published number may not be measured under it"
    );
    assert!(!SyncMode::None.is_durable());
}
