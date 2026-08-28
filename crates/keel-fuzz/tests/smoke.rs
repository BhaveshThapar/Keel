#![allow(clippy::unwrap_used, clippy::expect_used)]

//! P22's exit criterion: every target compiles and smoke-runs, and a
//! checksum demonstration that says the harness would notice if one stopped
//! catching things.

use keel_fuzz::{TARGETS, smoke};

/// Every target survives a few thousand generated inputs.
///
/// The contract a target is held to is that it does not panic. Returning an
/// error is correct, refusing to decode is correct, and producing nonsense from
/// nonsense is correct — every one of these parsers is reached from a network
/// or a disk, so a panic is a node a stranger can stop with one bad byte.
#[test]
fn every_target_survives_a_smoke_run() {
    assert_eq!(TARGETS.len(), 7, "the target list changed");
    let report = smoke::run(1, 400);
    assert_eq!(report.inputs, 400 * TARGETS.len() as u64);
    // The counter that stops this being a test of a length check. Uniformly
    // random bytes fail the first gate in every parser and reach nothing, which
    // is the classic way a fuzzing harness reports millions of executions and
    // zero coverage.
    assert!(
        report.structured > report.inputs / 4,
        "only {} of {} inputs had any structure; the generator is producing \
         noise that no parser gets past",
        report.structured,
        report.inputs
    );
}

/// The same run, twice, is the same run.
///
/// A fuzzing harness whose failures cannot be replayed produces bug reports
/// nobody can act on. This one is a pure function of its seed for the same
/// reason the simulator is.
#[test]
fn a_smoke_run_is_a_pure_function_of_its_seed() {
    assert_eq!(smoke::run(7, 50), smoke::run(7, 50));
    assert_ne!(smoke::run(7, 50), smoke::run(8, 50));
}

/// Every target is reachable by name, so one that is written and never wired
/// into the list is a failure here rather than a silent omission.
#[test]
fn every_target_is_named_once() {
    let mut names: Vec<&str> = TARGETS.iter().map(|(n, _)| *n).collect();
    names.sort_unstable();
    let unique = {
        let mut n = names.clone();
        n.dedup();
        n
    };
    assert_eq!(names, unique, "a target is listed twice");
    assert_eq!(
        names,
        vec![
            "api_proposal",
            "api_response",
            "core_events",
            "log_records",
            "net_frames",
            "raft_message",
            "store_snapshot",
        ]
    );
}

/// One byte flipped inside a written record, and the log must not hand the
/// record back.
///
/// This is the intact half of the checksum demonstration. The other half is the
/// same test compiled with `--features negative-demos`, where the record
/// checksum is gone and the corrupted record is accepted — see
/// `the_corruption_is_accepted_without_the_checksum` below, which is compiled
/// only in that build.
///
/// A budget rather than one attempt: a flipped byte can land in padding, or in
/// a region a structural check rejects for another reason, and the claim is
/// about the checksum rather than about one offset.
#[cfg(not(feature = "negative-demos"))]
#[test]
fn a_corrupted_record_is_rejected() {
    let budget = 60;
    let mut accepted = Vec::new();
    let mut examined = 0;
    for seed in 0..budget {
        let outcome = match smoke::corrupt_a_valid_record(seed) {
            Ok(outcome) => outcome,
            Err(_) => continue,
        };
        examined += 1;
        if outcome.accepted_the_corruption() {
            accepted.push(outcome);
        }
    }
    assert!(
        examined > budget / 2,
        "only {examined} of {budget} attempts produced a corruptible segment"
    );
    assert!(
        accepted.is_empty(),
        "the log handed back every entry despite a corrupted byte, in {} of \
         {examined} attempts: {:?}",
        accepted.len(),
        accepted.first()
    );
}

/// And with the checksum compiled out, the same corruption gets through.
///
/// Without this arm the test above is a test that a log rejects something, and
/// a log that rejected everything would pass it. This is what makes it a test
/// of the checksum.
#[cfg(feature = "negative-demos")]
#[test]
fn the_corruption_is_accepted_without_the_checksum() {
    let budget = 60;
    let mut accepted = 0;
    let mut examined = 0;
    for seed in 0..budget {
        let Ok(outcome) = smoke::corrupt_a_valid_record(seed) else {
            continue;
        };
        examined += 1;
        if outcome.accepted_the_corruption() {
            accepted += 1;
        }
    }
    assert!(
        examined > budget / 2,
        "only {examined} attempts were usable"
    );
    assert!(
        accepted > 0,
        "with the record checksum compiled out, {examined} corrupted segments \
         were still all rejected — so the checksum is not what was rejecting \
         them, and the intact-build test is proving something else"
    );
}
