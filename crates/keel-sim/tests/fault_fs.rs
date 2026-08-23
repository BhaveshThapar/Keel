//! The simulator's filesystem, held to the same assertions as the real one and
//! then to the ones only it can answer.
//!
//! `StdFs` cannot be asked to lose power, so everything about the distance
//! between written and durable lives here rather than in the shared suite.

// Helpers here sit outside `#[test]` fns, so clippy's in-tests exemption does
// not reach them. Unwrapping is the point: in a test the panic is the assertion.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};

use keel_log::{File, Fs, OpenMode, SyncMode};
use keel_sim::FaultFs;

fn dir() -> PathBuf {
    PathBuf::from("/node-1")
}

fn path(name: &str) -> PathBuf {
    dir().join(name)
}

/// Write `bytes` at `off` and return the handle, so a test reads as the sequence
/// of disk operations it is about rather than as handle bookkeeping.
fn write(fs: &FaultFs, name: &str, off: u64, bytes: &[u8]) {
    let mut f = fs.open(&path(name), OpenMode::Create).unwrap();
    f.write_at(off, bytes).unwrap();
}

fn read(fs: &FaultFs, name: &str, off: u64, len: usize) -> Vec<u8> {
    let f = fs.open(&path(name), OpenMode::Read).unwrap();
    let mut buf = vec![0u8; len];
    let n = f.read_at(off, &mut buf).unwrap();
    buf.truncate(n);
    buf
}

fn sync(fs: &FaultFs, name: &str) {
    let mut f = fs.open(&path(name), OpenMode::Create).unwrap();
    f.sync(SyncMode::Durable).unwrap();
}

/// Make a file's *name* durable, which is a separate act from making its bytes
/// durable and is the whole reason `sync_dir` is in the seam.
fn link(fs: &FaultFs) {
    fs.sync_dir(&dir()).unwrap();
}

#[test]
fn the_fault_injecting_filesystem_conforms() {
    let fs = FaultFs::new();
    keel_log::conformance::check(&fs, Path::new("/scratch"));
}

#[test]
fn a_crash_takes_back_every_write_no_sync_covered() {
    let fs = FaultFs::new();
    write(&fs, "a.bin", 0, b"durable");
    sync(&fs, "a.bin");
    link(&fs);
    write(&fs, "a.bin", 7, b"-staged");

    assert_eq!(
        read(&fs, "a.bin", 0, 32),
        b"durable-staged",
        "a read sees the write it just made, synced or not"
    );

    fs.crash();

    assert_eq!(
        read(&fs, "a.bin", 0, 32),
        b"durable",
        "and a crash takes back exactly the part no sync covered"
    );
}

#[test]
fn a_sync_of_one_file_does_not_make_another_durable() {
    let fs = FaultFs::new();
    write(&fs, "a.bin", 0, b"aaaa");
    write(&fs, "b.bin", 0, b"bbbb");
    link(&fs);

    // `Log::roll` makes the outgoing segment durable before continuing into the
    // next one. If a sync covered every file, that ordering would hold for free
    // and the rule would never be tested.
    sync(&fs, "a.bin");
    fs.crash();

    assert_eq!(read(&fs, "a.bin", 0, 4), b"aaaa");
    assert_eq!(
        read(&fs, "b.bin", 0, 4),
        b"",
        "syncing one descriptor must not flush another"
    );
}

#[test]
fn a_barrier_sync_does_not_survive_a_crash() {
    let fs = FaultFs::new();
    write(&fs, "a.bin", 0, b"ordered");
    link(&fs);
    let mut f = fs.open(&path("a.bin"), OpenMode::Create).unwrap();
    f.sync(SyncMode::Barrier).unwrap();
    drop(f);

    fs.crash();

    // ADR-013: a barrier orders writes, it does not survive power loss. macOS
    // `fsync` is that operation, which is why it is not the mode a published
    // number may be measured under.
    assert_eq!(read(&fs, "a.bin", 0, 8), b"");
}

#[test]
fn a_directory_entry_is_not_durable_until_its_directory_is_synced() {
    let fs = FaultFs::new();
    write(&fs, "orphan.bin", 0, b"bytes");
    sync(&fs, "orphan.bin");

    // The bytes are durable and the name is not, so the file is still gone. This
    // is the hazard the directory fsync in `create_segment` exists to close, and
    // the reason it has to happen before any caller-visible record lands in the
    // segment.
    fs.crash();

    assert!(
        fs.open(&path("orphan.bin"), OpenMode::Read).is_err(),
        "a file whose directory was never synced does not survive a crash"
    );
}

#[test]
fn an_unlink_the_directory_never_synced_comes_back() {
    let fs = FaultFs::new();
    write(&fs, "seg.bin", 0, b"bytes");
    sync(&fs, "seg.bin");
    link(&fs);

    fs.remove(&path("seg.bin")).unwrap();
    assert!(fs.open(&path("seg.bin"), OpenMode::Read).is_err());

    fs.crash();

    // `Log::compact_to` says a lost unlink is a space leak the next open
    // reclaims rather than a correctness problem. That claim is only worth
    // making if the unlink can actually be lost.
    assert_eq!(read(&fs, "seg.bin", 0, 8), b"bytes");
}

#[test]
fn an_unlink_survives_once_the_directory_is_synced() {
    let fs = FaultFs::new();
    write(&fs, "seg.bin", 0, b"bytes");
    sync(&fs, "seg.bin");
    link(&fs);

    fs.remove(&path("seg.bin")).unwrap();
    link(&fs);
    fs.crash();

    assert!(fs.open(&path("seg.bin"), OpenMode::Read).is_err());
}

#[test]
fn a_crash_releases_every_lock_the_way_a_killed_process_does() {
    let fs = FaultFs::new();
    let held = fs.open(&path("LOCK"), OpenMode::Lock).unwrap();
    sync(&fs, "LOCK");
    link(&fs);
    assert!(fs.open(&path("LOCK"), OpenMode::Lock).is_err());

    // The handle is deliberately still alive: a killed process does not run
    // `Drop`, and the kernel releases its locks anyway. A restarted node has to
    // be able to reopen its own log.
    fs.crash();
    let retaken = fs.open(&path("LOCK"), OpenMode::Lock);
    assert!(retaken.is_ok(), "a crash must release the log's lock");
    drop(held);
}

#[test]
fn a_crash_with_nothing_pending_changes_nothing() {
    let fs = FaultFs::new();
    write(&fs, "a.bin", 0, b"settled");
    sync(&fs, "a.bin");
    link(&fs);

    assert_eq!(fs.pending_writes(), 0);
    let before = fs.durable_digest();
    fs.crash();

    assert_eq!(fs.durable_digest(), before);
    assert_eq!(read(&fs, "a.bin", 0, 8), b"settled");
}

#[test]
fn the_durable_digest_notices_a_byte() {
    let fs = FaultFs::new();
    write(&fs, "a.bin", 0, b"aaaa");
    sync(&fs, "a.bin");
    link(&fs);
    let before = fs.durable_digest();

    write(&fs, "a.bin", 2, b"b");
    assert_eq!(
        fs.durable_digest(),
        before,
        "a staged write is not durable state"
    );

    sync(&fs, "a.bin");
    assert_ne!(
        fs.durable_digest(),
        before,
        "the fingerprint has to see the disk, or the determinism gate stops covering it"
    );
}

#[test]
fn a_visible_read_is_the_durable_bytes_with_the_pending_ones_folded_on_top() {
    let fs = FaultFs::new();
    write(&fs, "a.bin", 0, b"0123456789");
    sync(&fs, "a.bin");
    link(&fs);

    write(&fs, "a.bin", 2, b"XX");
    write(&fs, "a.bin", 6, b"YY");
    assert_eq!(read(&fs, "a.bin", 0, 10), b"01XX45YY89");

    // Folding is order-sensitive, so a later write to the same offset has to win
    // both before and after the crash puts the earlier picture back.
    write(&fs, "a.bin", 2, b"ZZ");
    assert_eq!(read(&fs, "a.bin", 0, 10), b"01ZZ45YY89");

    fs.crash();
    assert_eq!(read(&fs, "a.bin", 0, 10), b"0123456789");
}
