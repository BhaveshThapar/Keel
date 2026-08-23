#![allow(clippy::unwrap_used, clippy::expect_used)]

use keel_sim::{SimConfig, World, run_seed};

/// A short sweep, cheap enough to run on every change. The real sweeps live in
/// CI; this one exists so a change that breaks safety fails locally in seconds.
#[test]
fn seeds_pass_under_the_default_profile() {
    for seed in 0..12 {
        let outcome = run_seed(seed, 20_000, SimConfig::default());
        assert!(outcome.passed(), "{}", outcome.report.unwrap_or_default());
    }
}

#[test]
fn seeds_pass_under_heavy_faults() {
    for seed in 0..12 {
        let outcome = run_seed(seed, 20_000, SimConfig::chaos(5));
        assert!(outcome.passed(), "{}", outcome.report.unwrap_or_default());
    }
}

#[test]
fn seeds_pass_when_faults_target_the_figure_8_window() {
    for seed in 0..8 {
        let outcome = run_seed(seed, 30_000, SimConfig::fig8_hunt(3));
        assert!(outcome.passed(), "{}", outcome.report.unwrap_or_default());
    }
}

#[test]
fn a_seed_replays_exactly() {
    for seed in 0..8 {
        let a = run_seed(seed, 20_000, SimConfig::chaos(5));
        let b = run_seed(seed, 20_000, SimConfig::chaos(5));
        assert_eq!(
            a.fingerprint, b.fingerprint,
            "seed {seed} did not replay identically"
        );
    }
}

#[test]
fn different_seeds_produce_different_runs() {
    let a = run_seed(1, 20_000, SimConfig::chaos(5));
    let b = run_seed(2, 20_000, SimConfig::chaos(5));
    assert_ne!(a.fingerprint, b.fingerprint);
}

/// A simulator that deadlocks passes every safety check and proves nothing. The
/// run has to actually be doing something.
#[test]
fn the_cluster_makes_progress() {
    let outcome = run_seed(1, 30_000, SimConfig::default());
    assert!(
        outcome.stats.committed > 100,
        "only {} entries committed; the cluster is not making progress",
        outcome.stats.committed
    );
    assert!(outcome.stats.applied > 100);
}

/// Coverage, not correctness: confirm the run reaches the states the safety
/// rules exist to guard. A clean pass over a fault schedule that never
/// partitioned anything would be worthless.
#[test]
fn heavy_faults_actually_reach_the_interesting_states() {
    let mut world = World::new(3, SimConfig::chaos(5));
    world.run(60_000);
    let s = &world.stats;

    assert!(s.partitions > 5, "only {} partitions", s.partitions);
    assert!(s.crashes > 2, "only {} crashes", s.crashes);
    assert!(
        s.messages_dropped > 50,
        "only {} messages lost",
        s.messages_dropped
    );
    assert!(
        s.terms_with_leaders > 2,
        "only {} terms produced a leader; leadership never changed hands",
        s.terms_with_leaders
    );
    assert!(
        s.entries_rewritten > 0,
        "no follower ever had a divergent tail overwritten"
    );
    assert!(
        s.old_term_commit_windows > 0,
        "no leader ever held an earlier term's entry at its commit index, so the \
         Figure 8 rule was never under any pressure"
    );
}

/// In a correct build this must be exactly zero: committing an earlier term's
/// entry on replica count alone is precisely what Figure 8 forbids.
#[test]
fn no_leader_ever_commits_an_old_term_entry_by_counting() {
    for seed in 0..8 {
        let outcome = run_seed(seed, 30_000, SimConfig::fig8_hunt(3));
        assert_eq!(
            outcome.stats.fig8_bypasses, 0,
            "seed {seed} committed an earlier term's entry on replica count alone"
        );
    }
}

/// Sum the disk counters over a short sweep.
///
/// A sweep and not one run: a tear needs a crash to land in the window between
/// a write and the fsync covering it, and how often a single seed reaches that
/// varies. Asserting per-run would either be flaky or have to be set so low it
/// asserted nothing.
fn disk_coverage(profile: &str, seeds: u64) -> (keel_sim::Stats, keel_sim::FaultStats) {
    let mut totals = keel_sim::Stats::default();
    let mut disk = keel_sim::FaultStats::default();
    for seed in 0..seeds {
        let cfg = SimConfig::named(profile, 3).expect("the profile exists");
        let mut world = World::new(seed, cfg);
        world.run(30_000);
        assert!(!world.is_broken(), "{}", world.failure_report());
        let s = &world.stats;
        totals.crashes += s.crashes;
        totals.torn_tails += s.torn_tails;
        totals.bytes_discarded_by_tears += s.bytes_discarded_by_tears;
        totals.segments_recovered += s.segments_recovered;
        totals.tears_during_partition += s.tears_during_partition;
        let d = world.disk_stats();
        disk.crashes_with_writes_in_flight += d.crashes_with_writes_in_flight;
        disk.bytes_in_flight_at_crash += d.bytes_in_flight_at_crash;
        disk.writes_that_landed_head_first += d.writes_that_landed_head_first;
        disk.writes_that_landed_tail_first += d.writes_that_landed_tail_first;
        disk.files_a_crash_left_a_hole_in += d.files_a_crash_left_a_hole_in;
    }
    (totals, disk)
}

/// Coverage, not correctness, for the disk. The arithmetic says a badly sized
/// profile can make the tear model *provably* inert — a write only tears if it
/// straddles a sector boundary — so a clean sweep means nothing on its own.
#[test]
fn heavy_disk_faults_actually_tear_the_log() {
    let (s, d) = disk_coverage("disk-hunt", 20);

    assert!(
        d.crashes_with_writes_in_flight > 0,
        "no crash caught a write in flight, out of {} crashes. The window \
         between a write and the fsync covering it is the only place a crash \
         has anything to tear",
        s.crashes
    );
    assert!(
        d.bytes_in_flight_at_crash > 0,
        "nothing was ever unsynced at a crash"
    );
    assert!(
        d.writes_that_landed_head_first + d.writes_that_landed_tail_first > 0,
        "every crash lost its writes whole, so the sector model never cut one \
         and the profile is inert however it is configured"
    );
    assert!(
        d.files_a_crash_left_a_hole_in > 0,
        "no crash left bytes above a gap, which is the state KEEL-7 lived in"
    );
    assert!(
        s.torn_tails > 0,
        "the real recovery parser never met a torn tail, so nothing the tear \
         model produced ever reached the code it exists to exercise"
    );
    assert!(
        s.bytes_discarded_by_tears > 0,
        "tears were counted but cost nothing"
    );
}

/// The claim the durability bullet in the README turns on. It is not enough
/// that tears happen and partitions happen; what has to be shown is that they
/// met.
#[test]
fn a_tear_meets_a_partition() {
    let (s, _) = disk_coverage("disk-hunt", 20);

    assert!(
        s.tears_during_partition > 0,
        "{} tears, {} crashes, and not one of them on a node that was inside a \
         partition at the time",
        s.torn_tails,
        s.crashes
    );
}

/// Multi-segment recovery is on the path of every restart now, so a run that
/// never rolled a segment would be exercising a much smaller parser than the
/// one the log actually has.
#[test]
fn restarts_recover_across_more_than_one_segment() {
    let (s, _) = disk_coverage("disk-hunt", 8);

    assert!(
        s.segments_recovered > s.crashes,
        "{} segments recovered over {} crashes: recovery never saw more than \
         one segment, so the multi-segment path went untested",
        s.segments_recovered,
        s.crashes
    );
}

#[test]
fn the_profile_list_and_the_named_constructor_cannot_drift() {
    for name in SimConfig::PROFILES {
        assert!(
            SimConfig::named(name, 3).is_some(),
            "{name} is offered in the error message and refused by the constructor"
        );
    }
    assert!(SimConfig::named("no-such-profile", 3).is_none());
}
