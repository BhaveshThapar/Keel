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
