#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Kill a node in the middle of applying, over and over, and check what it
//! believed afterwards.
//!
//! The property is narrow and it is the one ADR-010 exists for: after a crash,
//! the applied index and the data agree. Concretely, a counter incremented once
//! per log entry must read exactly `applied` after every restart — no more,
//! which would mean an entry applied twice, and no less, which would mean the
//! index ran ahead of the data.
//!
//! A child process applies entries in a loop and prints how far it has got. The
//! parent kills it at an arbitrary moment, reopens the store, checks the
//! invariant, and does it again. The child never exits cleanly, so nothing is
//! ever flushed on the way out and every restart is a recovery.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};

use bytes::Bytes;
use keel_api::{Command as KvCommand, Proposal, ProposalBody};
use keel_sm::{LsmStore, StateMachine};
use lsm_kv::Options;

const CHILD_ENV: &str = "KEEL_SM_KILL_CHILD";
const DIR_ENV: &str = "KEEL_SM_KILL_DIR";
const COUNTER: &[u8] = b"n";

/// How many kill/restart cycles the recorded run performs.
///
/// The exit criterion is a thousand. That takes minutes, which is a nightly's
/// business and not every developer's, so the committed number here is smaller
/// and the script runs the full thousand.
/// Only the control arm reads this: the experiment's budget is the exit
/// criterion's hundred, and is not a knob.
#[cfg(not(feature = "negative-demos"))]
fn cycles() -> usize {
    std::env::var("KEEL_SM_KILL_CYCLES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60)
}

/// A small MemTable, so a few dozen entries cross a flush and the kill lands
/// inside real storage work rather than only inside the write-ahead log.
fn opts() -> Options {
    Options {
        memtable_threshold: 2 * 1024,
        compaction_threshold: 2,
        ..LsmStore::default_options()
    }
}

fn increment(index: u64, client: u64) -> Proposal {
    Proposal {
        stamped_ms: 1_000_000,
        session: Some((client, index)),
        body: ProposalBody::Command(KvCommand::Incr {
            key: Bytes::from_static(COUNTER),
            by: 1,
        }),
    }
}

/// Apply entries forever, printing the index of each one as it commits.
fn run_child_if_requested() {
    let Ok(dir) = std::env::var(DIR_ENV) else {
        return;
    };
    if std::env::var(CHILD_ENV).is_err() {
        return;
    }

    let store = LsmStore::open_with(&dir, opts()).expect("child: open");
    let mut sm = StateMachine::new(store);

    // One client, registered at index 1, so every later index is a command and
    // the arithmetic below is `applied - 1`.
    let client = match sm.session(1).expect("child: session") {
        Some(_) => 1,
        None => sm.register(1, 1_000_000, 1).expect("child: register"),
    };

    let stdout = std::io::stdout();
    for index in (sm.applied() + 1)..=u64::MAX {
        sm.apply(index, &increment(index, client))
            .expect("child: apply");
        // The engine spawns no threads here, so flushes and compactions only
        // happen when they are asked for. Doing one unit per entry keeps a
        // crash landing in the middle of real storage work rather than only in
        // the write-ahead log.
        let _ = sm.store().maintain();

        let mut lock = stdout.lock();
        writeln!(lock, "APPLIED {index}").unwrap();
        lock.flush().unwrap();
    }
    unreachable!();
}

fn spawn(test_name: &str, dir: &Path) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test_name, "--nocapture"])
        .env(CHILD_ENV, "1")
        .env(DIR_ENV, dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn child")
}

/// Run one kill cycle: let the child get somewhere, kill it, and report the
/// highest index it said it had applied.
fn one_cycle(test_name: &str, dir: &Path, entries: usize) -> u64 {
    let mut child = spawn(test_name, dir);
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    let mut acked = 0u64;
    let mut line = String::new();
    let mut seen = 0;

    while seen < entries {
        line.clear();
        let n = reader.read_line(&mut line).expect("read child stdout");
        assert!(n > 0, "the child exited instead of applying");
        if let Some(index) = line.trim().strip_prefix("APPLIED ") {
            acked = index.parse().expect("malformed APPLIED line");
            seen += 1;
        }
        // Only a build with the atomicity removed announces this: it has
        // written the data and not yet the index. A uniform kill schedule finds
        // that window by luck if at all, so the harness aims at it — the same
        // argument ADR-007 makes about the simulator's nemesis. A correct build
        // never prints it, so this branch is unreachable in the control arm and
        // the two arms are otherwise identical.
        if line.trim().starts_with("SPLIT ") {
            break;
        }
    }

    // No destructors, no flush, no clean shutdown. The kill lands wherever the
    // child happens to be, which is the point.
    child.kill().expect("kill child");
    child.wait().expect("reap child");
    acked
}

/// What every restart must find true, or a description of how it did not.
///
/// Returns rather than asserts, because the same loop drives both arms of the
/// demonstration: one requires this to hold every time, the other requires it
/// to break.
fn check(dir: &Path, at_least: u64) -> Result<u64, String> {
    let sm = StateMachine::new(LsmStore::open_with(dir, opts()).expect("parent: reopen"));
    let applied = sm.applied();
    let counter = sm.counter(COUNTER).expect("parent: counter");

    if applied < at_least {
        return Err(format!(
            "the applied index went backwards across a restart: {applied} < {at_least}"
        ));
    }
    // Index 1 is the registration and every index above it is one increment, so
    // the counter is exactly one less than the applied index — except before the
    // registration itself has landed, where both are zero.
    let expected = applied.saturating_sub(1) as i64;
    if counter != expected {
        return Err(format!(
            "after a crash the store had applied through {applied} and the counter read \
             {counter}; it must read {expected}. A higher counter means the data went in \
             without the index that describes it, so the entry will apply twice; a lower \
             one means the index ran ahead of its data"
        ));
    }
    Ok(applied)
}

/// Kill and restart until the invariant breaks or the cycles run out.
///
/// Returns the cycle it broke on and why, or how far it got.
fn kill_loop(test_name: &str, dir: &Path, cycles: usize) -> Result<u64, (usize, String)> {
    let mut floor = 0;
    for cycle in 0..cycles {
        // A varying number of entries before the kill, so the crash does not
        // always land at the same point in the engine's own rhythm of flushes
        // and compactions.
        let entries = 3 + (cycle % 17);
        one_cycle(test_name, dir, entries);
        match check(dir, floor) {
            Ok(applied) => floor = applied,
            Err(why) => return Err((cycle, why)),
        }
    }
    Ok(floor)
}

/// A correct build survives every kill.
#[cfg(not(feature = "negative-demos"))]
#[test]
fn a_kill_mid_apply_never_double_applies_or_regresses() {
    run_child_if_requested();

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    let floor = match kill_loop(
        "a_kill_mid_apply_never_double_applies_or_regresses",
        &dir,
        cycles(),
    ) {
        Ok(floor) => floor,
        Err((cycle, why)) => panic!("cycle {cycle}: {why}"),
    };

    // A run that never flushed would be testing the write-ahead log alone, and
    // the interesting crashes are the ones that land while an SSTable is being
    // published.
    let flushed = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".db"))
        .count();
    assert!(
        flushed > 0,
        "after {} cycles reaching index {floor}, nothing was ever flushed — every \
         kill landed in the write-ahead log and the storage paths went untested",
        cycles()
    );
}

/// With the atomicity removed, the same loop must find the double-apply.
///
/// This is the half that makes the other half evidence. A clean run only means
/// something if a build without the rule produces a dirty one, and this test
/// fails — loudly, in the ordinary suite — the day the loop stops being able to
/// tell the difference.
#[cfg(feature = "negative-demos")]
#[test]
fn without_the_atomic_index_a_kill_leaves_an_entry_that_will_apply_twice() {
    run_child_if_requested();

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    // The exit criterion says caught inside a hundred cycles. It is caught in
    // far fewer, because the window is announced and aimed at.
    let budget = 100;
    match kill_loop(
        "without_the_atomic_index_a_kill_leaves_an_entry_that_will_apply_twice",
        &dir,
        budget,
    ) {
        Err((cycle, why)) => {
            assert!(
                cycle < budget,
                "caught, but only at cycle {cycle} of {budget}"
            );
            println!("caught at cycle {cycle}: {why}");
        }
        Ok(floor) => panic!(
            "{budget} kill cycles reaching index {floor} found nothing wrong with a \
             build that writes the applied index separately from its data. The loop \
             cannot detect this class of bug, so a clean run means nothing."
        ),
    }
}
