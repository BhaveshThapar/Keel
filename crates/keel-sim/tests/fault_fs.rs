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
use keel_sim::{FaultFs, Rng, TearPolicy};

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
fn an_acknowledged_but_lost_fsync_is_exposed_by_the_next_crash() {
    let fs = FaultFs::new();
    write(&fs, "a.bin", 0, b"firmware-lied");
    link(&fs);
    fs.lose_next_sync();
    sync(&fs, "a.bin");
    assert_eq!(read(&fs, "a.bin", 0, 13), b"firmware-lied");

    fs.crash();

    assert_eq!(read(&fs, "a.bin", 0, 13), b"");
    assert_eq!(fs.fault_stats().syncs_lost, 1);
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

// --- the device model -------------------------------------------------------

/// A disk that models a device, plus the one file every tear test needs: linked,
/// so a crash keeps it, and durable up to `durable_len` bytes of `0xAA`.
fn tearing(sector_bytes: u64, sector_lands_pct: u32, seed: u64, durable_len: usize) -> FaultFs {
    let fs = FaultFs::tearing(
        TearPolicy {
            sector_bytes,
            sector_lands_pct,
        },
        Rng::new(seed),
    );
    link(&fs);
    if durable_len > 0 {
        write(&fs, "seg", 0, &vec![0xAA; durable_len]);
    }
    sync(&fs, "seg");
    link(&fs);
    fs
}

/// The whole durable image of the one file, after a crash.
fn after_crash(fs: &FaultFs) -> Vec<u8> {
    fs.crash();
    let len = fs
        .open(&path("seg"), OpenMode::Read)
        .unwrap()
        .size()
        .unwrap();
    read(fs, "seg", 0, len as usize)
}

#[test]
fn the_fault_injecting_filesystem_conforms_while_tearing() {
    let fs = FaultFs::tearing(
        TearPolicy {
            sector_bytes: 512,
            sector_lands_pct: 50,
        },
        Rng::new(1),
    );
    keel_log::conformance::check(&fs, Path::new("/scratch"));
}

#[test]
fn a_write_inside_one_sector_lands_whole_or_not_at_all() {
    for seed in 0..200 {
        let fs = tearing(4096, 50, seed, 0);
        write(&fs, "seg", 0, b"twenty-three bytes long");
        let image = after_crash(&fs);

        assert!(
            image.is_empty() || image == b"twenty-three bytes long",
            "a device commits a sector atomically, so a byte-granular loss \
             inside one sector is a fault this model invented rather than one \
             a disk produces (seed {seed}, got {image:?})"
        );
    }
}

/// Sweep seeds until each straddling shape has been seen at least once, and say
/// how many of each. A shape that never appears is a shape the model cannot
/// produce, whatever the policy says.
fn straddle_shapes(sector_bytes: u64, off: u64, len: usize) -> (u64, u64, u64) {
    let (mut head, mut tail, mut pieces) = (0, 0, 0);
    for seed in 0..400 {
        let fs = tearing(sector_bytes, 50, seed, 0);
        write(&fs, "seg", off, &vec![0xCD; len]);
        fs.crash();
        let s = fs.fault_stats();
        head += s.writes_that_landed_head_first;
        tail += s.writes_that_landed_tail_first;
        pieces += s.writes_that_landed_in_pieces;
    }
    (head, tail, pieces)
}

#[test]
fn a_write_that_straddles_a_sector_boundary_can_land_head_first() {
    let (head, _, _) = straddle_shapes(512, 508, 8);

    assert!(
        head > 0,
        "no seed landed the leading sector and dropped the trailing one, so \
         the model cuts nowhere and every crash is all-or-nothing however the \
         policy is configured"
    );
}

#[test]
fn a_write_that_straddles_a_sector_boundary_can_land_tail_first() {
    let (_, tail, _) = straddle_shapes(512, 508, 8);

    assert!(
        tail > 0,
        "this is the landing recovery reads as a clean end, so a model that \
         cannot produce it cannot reach the state KEEL-7 lived in"
    );
}

#[test]
fn the_cut_falls_at_a_multiple_of_the_sector_size_from_the_start_of_the_file() {
    // A write at an offset that is not itself a multiple of the sector size.
    // If the model cut relative to the write, the boundary would land at 700.
    for seed in 0..200 {
        let fs = tearing(512, 50, seed, 0);
        write(&fs, "seg", 700, &vec![0xCD; 400]);
        let image = after_crash(&fs);
        if image.is_empty() {
            continue;
        }

        let cuts: Vec<usize> = image
            .windows(2)
            .enumerate()
            .filter(|(_, w)| (w[0] == 0xCD) != (w[1] == 0xCD))
            .map(|(i, _)| i + 1)
            .filter(|i| *i != 700 && *i != 1100)
            .collect();
        for cut in cuts {
            assert_eq!(
                cut % 512,
                0,
                "a cut measured from the write's own start would make this an \
                 API model, and an API model cannot say anything about a \
                 device (seed {seed})"
            );
        }
    }
}

#[test]
fn a_sector_a_write_only_partly_covers_keeps_the_bytes_it_did_not_touch() {
    let mut landed = 0;
    for seed in 0..200 {
        // A durable sector of 0xAA, then a small pending write over its middle.
        let fs = tearing(512, 50, seed, 512);
        write(&fs, "seg", 200, b"xyz");
        let image = after_crash(&fs);

        if &image[200..203] != b"xyz" {
            assert_eq!(&image[200..203], &[0xAA; 3], "the sector did not land");
            continue;
        }
        landed += 1;
        assert!(
            image[..200].iter().all(|b| *b == 0xAA) && image[203..512].iter().all(|b| *b == 0xAA),
            "a sector is read, modified and written back whole, so zeroing the \
             part the write did not cover would model a device that forgets \
             what it already held (seed {seed})"
        );
    }
    assert!(
        landed > 0,
        "no seed landed the sector, so nothing was checked"
    );
}

#[test]
fn an_earlier_write_can_be_lost_while_a_later_one_survives() {
    let mut holes = 0;
    for seed in 0..200 {
        let fs = tearing(512, 50, seed, 0);
        write(&fs, "seg", 0, b"first");
        write(&fs, "seg", 512, b"second");
        let image = after_crash(&fs);

        if image.len() >= 518 && &image[512..518] == b"second" && &image[..5] != b"first" {
            holes += 1;
        }
    }

    assert!(
        holes > 0,
        "no seed produced a hole with bytes on the far side of it. That is the \
         state KEEL-7 lived in, and it is reachable without permuting anything"
    );
}

#[test]
fn a_crash_that_lands_no_sector_leaves_exactly_what_a_crash_always_left() {
    let torn = FaultFs::tearing(TearPolicy::default(), Rng::new(7));
    let plain = FaultFs::new();
    for fs in [&torn, &plain] {
        link(fs);
        write(fs, "seg", 0, b"durable");
        sync(fs, "seg");
        write(fs, "seg", 7, b"staged");
        fs.crash();
    }

    assert_eq!(
        torn.durable_digest(),
        plain.durable_digest(),
        "the device model is opt-in, and a default disk that quietly started \
         tearing would change what every other assertion in this file means \
         without any of them saying so"
    );
}

#[test]
fn two_disks_with_the_same_seed_tear_identically() {
    let run = || {
        let fs = tearing(512, 50, 42, 0);
        write(&fs, "seg", 500, &vec![0xCD; 600]);
        fs.crash();
        (fs.durable_digest(), fs.fault_stats())
    };

    assert_eq!(
        run(),
        run(),
        "a tear a seed does not reproduce is a failure nobody can debug, which \
         is the same as no failure at all"
    );
}

#[test]
fn a_file_with_nothing_staged_does_not_shift_the_tear_stream() {
    let with_quiet_file = {
        let fs = tearing(512, 50, 3, 0);
        // Durable, linked, and never written to again: the shape of the lock
        // file and of every segment the log has rolled past.
        write(&fs, "quiet", 0, b"sealed");
        sync(&fs, "quiet");
        link(&fs);
        write(&fs, "seg", 500, &vec![0xCD; 600]);
        after_crash(&fs)
    };
    let without = {
        let fs = tearing(512, 50, 3, 0);
        write(&fs, "seg", 500, &vec![0xCD; 600]);
        after_crash(&fs)
    };

    assert_eq!(
        with_quiet_file, without,
        "a draw spent on a file with nothing staged would make every repro \
         depend on how many segments happened to exist when it was captured, \
         so a rollover would invalidate it"
    );
}

#[test]
fn a_pending_allocation_is_all_or_nothing() {
    for seed in 0..200 {
        let fs = tearing(512, 50, seed, 0);
        let mut f = fs.open(&path("seg"), OpenMode::Create).unwrap();
        f.allocate(4096).unwrap();
        drop(f);
        fs.crash();
        let len = fs
            .open(&path("seg"), OpenMode::Read)
            .unwrap()
            .size()
            .unwrap();

        assert!(
            len == 0 || len == 4096,
            "a set_len is one metadata update; half a file length is not a \
             state a filesystem has (seed {seed}, got {len})"
        );
    }
}

#[test]
fn a_crash_counts_the_shapes_it_produced() {
    let fs = tearing(512, 50, 11, 0);
    for seed in 0..200u64 {
        write(&fs, "seg", 500, &vec![0xCD; 600]);
        write(&fs, "seg", 2000 + seed, b"more");
        fs.crash();
    }
    let s = fs.fault_stats();

    assert_eq!(s.crashes, 200);
    assert!(s.crashes_with_writes_in_flight > 0);
    assert!(s.bytes_in_flight_at_crash > 0);
    assert!(s.sectors_that_reached_the_device > 0);
    assert!(s.sectors_the_crash_took_back > 0);
    assert!(s.writes_lost_whole > 0);
    assert!(s.writes_that_landed_whole > 0);
    assert!(s.writes_that_landed_head_first > 0);
    assert!(s.writes_that_landed_tail_first > 0);
    assert!(
        s.files_a_crash_left_a_hole_in > 0,
        "a tear model with no counters is a model nobody can show ever fired, \
         and a zero here is the one that means the hazard was never reached"
    );
}

#[test]
fn a_four_kilobyte_sector_over_a_one_kilobyte_segment_can_never_tear() {
    let fs = tearing(4096, 50, 5, 0);
    for seed in 0..200u64 {
        // Every offset a 1 KiB segment has lies in sector zero, so one draw is
        // made and the only outcomes are lost and whole.
        write(&fs, "seg", seed % 1000, b"twenty-three bytes long");
        fs.crash();
    }
    let s = fs.fault_stats();

    assert_eq!(
        (
            s.writes_that_landed_head_first,
            s.writes_that_landed_tail_first,
            s.writes_that_landed_in_pieces
        ),
        (0, 0, 0),
        "this is the profile the sizing arithmetic calls inert. Naming it here \
         is what stops someone configuring it by accident and reading the \
         clean sweep it produces as evidence"
    );
    assert!(s.writes_lost_whole > 0 && s.writes_that_landed_whole > 0);
}
