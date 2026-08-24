#![allow(clippy::unwrap_used, clippy::expect_used)]

use keel_sim::{NemesisAction, NemesisWeights, SimConfig, World, run_seed};

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

/// The fingerprints the committed profiles produce today, pinned.
///
/// `a_seed_replays_exactly` says a run is reproducible within one build. This
/// says it is reproducible *across* builds, which is the stronger and more
/// useful claim: it is what makes a seed in BUGS.md a reproduction six months
/// later, and it is what turns "the simulator grew a feature" into a red build
/// the moment that feature shifts a draw some existing consumer sees.
///
/// A change here is not automatically a bug, and one of the two phases that was
/// expected to move these numbers has now done so. **These values are P8's**:
/// every node drives the real state machine, so every committed entry is really
/// decoded, deduplicated against a session table, and written into a store.
///
/// One phase ahead is still expected to move them, and must regenerate every
/// committed artifact in the same commit: P16, when a profile first takes a
/// snapshot.
///
/// Outside that, a diff in this table means a draw moved. The rule ADR-007
/// records is that a new consumer takes a *new* split rather than sharing an
/// existing stream, precisely so that adding one cannot do this.
const PINNED: &[(&str, u64, u64)] = &[
    ("default", 0, 0x226d_9f9d_c643_aeb0),
    ("default", 7, 0x9e58_618b_a7c9_7f6f),
    ("default", 42, 0xa2b0_21e0_b0ea_c5b1),
    ("chaos", 0, 0xa1ff_817f_b49e_fb72),
    ("chaos", 7, 0xcf74_26a0_5a5d_2168),
    ("chaos", 42, 0x2fba_57b7_7e26_6f4b),
    ("fig8-hunt", 0, 0x249e_71e2_0aa6_d7e4),
    ("fig8-hunt", 7, 0xe931_e924_0bce_a6df),
    ("fig8-hunt", 42, 0xcca8_73ce_70a1_8902),
    ("disk-chaos", 0, 0xb0f4_6e30_7980_46c0),
    ("disk-chaos", 7, 0x54d9_d6fe_1454_43cd),
    ("disk-chaos", 42, 0x4772_e7d2_0c82_0486),
    ("disk-hunt", 0, 0x5f33_8ea9_2dc7_43b5),
    ("disk-hunt", 7, 0x0b38_a8d8_75ab_1ba0),
    ("disk-hunt", 42, 0xa47f_001d_52a6_eb2c),
    // Pinned like any other profile. A fingerprint is a claim about
    // reproducibility, which is a different claim from correctness — this
    // profile has a seed that does not sweep clean, recorded in BUGS.md, and
    // pinning it is what makes that seed still reproduce while the question is
    // open.
    ("snapshot-hunt", 0, 0x80e2_2a35_a9dc_502b),
    ("snapshot-hunt", 7, 0xc6bd_36b6_6021_4bb4),
    ("snapshot-hunt", 42, 0x50af_1a06_ea02_98a0),
    ("read-hunt", 0, 0x67af_feb9_5ead_cae0),
    ("read-hunt", 7, 0xfe39_4c8c_485a_9157),
    ("read-hunt", 42, 0x11cf_bdd1_963e_eb4e),
    ("lease-drift", 0, 0xe454_41a7_ceaa_13e5),
    ("lease-drift", 7, 0x92b3_606e_4e93_f6be),
    ("lease-drift", 42, 0x7a5f_1f9c_2e46_0e6f),
    ("membership-hunt", 0, 0xdad4_dca8_43d3_88f5),
    ("membership-hunt", 7, 0xd6b8_cabf_427f_3760),
    ("membership-hunt", 42, 0xde69_14f9_cc4f_c8e9),
];

#[test]
fn the_committed_profiles_still_replay_to_their_pinned_fingerprints() {
    // Every profile is covered, so a change that shifts only the disk stream —
    // which the network profiles never draw from — cannot hide here.
    let covered: std::collections::BTreeSet<_> = PINNED.iter().map(|(p, _, _)| *p).collect();
    assert_eq!(
        covered.len(),
        SimConfig::PROFILES.len(),
        "a profile was added without pinning it: {:?} against {:?}",
        covered,
        SimConfig::PROFILES
    );

    let mut moved = Vec::new();
    for (profile, seed, expected) in PINNED {
        let cfg = SimConfig::named(profile, 3).expect("pinned profile exists");
        let mut world = World::new(*seed, cfg);
        world.run(20_000);
        let got = world.fingerprint();
        if got != *expected {
            moved.push(format!("    (\"{profile}\", {seed}, {got:#018x}),"));
        }
    }
    assert!(
        moved.is_empty(),
        "the fingerprints moved. If this is P8 or P16, regenerate every artifact \
         in the same commit and paste these in:\n{}",
        moved.join("\n")
    );
}

/// Every crate the simulator may depend on, and why each one cannot make its
/// runs stop reproducing.
///
/// An allowlist rather than a denylist, for the reason `keel-raft`'s own gate
/// gives: a denylist only catches the names somebody thought to write down.
const ALLOWED_DEPENDENCIES: [&str; 7] = [
    "keel-raft", // the core under test
    "keel-log",  // the real log, driven over a fault-injecting filesystem
    "keel-rand", // seeded generator, no entropy source
    // The real state machine, on its in-memory store. It owns no thread, opens
    // no file, and reads no clock — session expiry runs on the timestamp the
    // proposing leader stamped into the entry, which is part of the run rather
    // than outside it. The `lsm` feature is deliberately not enabled: that store
    // reaches a real disk, and this crate has its own.
    "keel-sm",
    "keel-api", // wire types, encode and decode only
    "bytes",    // byte buffers
    // argv parsing for `src/main.rs`. It reads no file, opens no socket, and
    // nothing in the library calls it — but Cargo has no way to say "this
    // dependency belongs to the binary target", so it is allowlisted here with
    // the reason rather than silently permitted.
    "clap",
];

/// The simulator must not be able to reach a socket or a storage engine.
///
/// This is the gate ROADMAP.md's P6 note is about, and it is a manifest check
/// rather than a `cargo tree` one on purpose. Cargo's resolver computes one
/// feature set per package per invocation, so `cargo test --workspace` builds
/// every crate with the union of every feature anything asked for — which means
/// a `cargo tree` run under those conditions cannot establish that this crate
/// did not link `tcp` or `lsm`. What it *can* establish is that there is no
/// edge at all, which is why `Node` moved into `keel-node` rather than living
/// beside the server.
///
/// The consequence if this were allowed to rot is not a slow build. A simulator
/// that could open a socket would have a run that depends on the network, and
/// the seed would stop being the whole reproduction.
#[test]
fn the_simulator_cannot_reach_a_socket_or_a_storage_engine() {
    let manifest = include_str!("../Cargo.toml");
    let Some(deps) = manifest
        .split("\n[dependencies]\n")
        .nth(1)
        .and_then(|s| s.split("\n[").next())
    else {
        panic!("Cargo.toml should declare a [dependencies] section");
    };

    for line in deps.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, _)) = line.split_once('=') else {
            panic!("unparsed line in [dependencies]: {line}");
        };
        let name = name.trim();
        assert!(
            ALLOWED_DEPENDENCIES.contains(&name),
            "keel-sim must not depend on `{name}`. A dependency that can open a \
             socket, spawn a thread, or reach a real disk makes a run depend on \
             something the seed does not describe. If it genuinely cannot, add it \
             to ALLOWED_DEPENDENCIES with the reason."
        );
    }
}

/// A sweep in which no seed ever opened a session tested the apply path and
/// nothing about exactly-once delivery — and it would look exactly like a clean
/// run. Same reasoning as `heavy_faults_actually_reach_the_interesting_states`,
/// and the same reason: [KEEL-4](../../BUGS.md) was a fault schedule that could
/// not reach the state it was meant to test.
#[test]
fn the_state_machine_is_actually_exercised() {
    let mut sessions = 0;
    let mut commands = 0;
    let mut refused = 0;
    for seed in 0..12 {
        let outcome = run_seed(seed, 30_000, SimConfig::default());
        assert!(outcome.passed(), "{}", outcome.report.unwrap_or_default());
        sessions += outcome.stats.sessions_opened;
        commands += outcome.stats.commands_applied;
        refused += outcome.stats.commands_without_a_session;
    }
    assert!(
        sessions > 0,
        "no seed ever opened a session, so every command was refused and the \
         session table went untested"
    );
    assert!(
        commands > 0,
        "no command was ever accepted by a session, so nothing was ever written \
         through the real apply path"
    );
    assert!(
        refused > 0,
        "no command was ever refused for having no session, so the refusal path \
         went untested"
    );
}

/// The model has to have applied something, or every comparison against it was
/// vacuous and the oracle was a no-op that passed.
#[test]
fn the_model_oracle_actually_applies_the_log() {
    let mut world = World::new(3, SimConfig::default());
    world.run(30_000);
    assert!(!world.is_broken(), "{}", world.failure_report());
    assert!(
        world.oracle_model_applied() > 100,
        "the reference state machine applied {} entries, so comparing nodes \
         against it established almost nothing",
        world.oracle_model_applied()
    );
}

/// Snapshots move, and the resume path is reached.
///
/// A profile in which every transfer completed first time would report a clean
/// sweep having tested the happy path and nothing else — [KEEL-4](../../BUGS.md)'s
/// lesson, applied to snapshots. The counters say the stream was interrupted and
/// resumed, not merely started.
///
/// Seeds are chosen to avoid [KEEL-8](../../BUGS.md), which is an open question
/// about the oracle rather than about the system, and is recorded there rather
/// than tuned around silently.
#[test]
fn snapshots_are_actually_taken_streamed_and_resumed() {
    let mut checkpoints = 0;
    let mut started = 0;
    let mut interrupted = 0;
    let mut resumed = 0;
    let mut completed = 0;

    for seed in 0..10 {
        let outcome = run_seed(seed, 40_000, SimConfig::snapshot_hunt(3));
        assert!(outcome.passed(), "{}", outcome.report.unwrap_or_default());
        checkpoints += outcome.stats.checkpoints_taken;
        started += outcome.stats.streams_started;
        interrupted += outcome.stats.streams_interrupted;
        resumed += outcome.stats.streams_resumed;
        completed += outcome.stats.streams_completed;
    }

    assert!(checkpoints > 0, "no checkpoint was ever taken");
    assert!(started > 0, "no snapshot stream ever began");
    assert!(
        interrupted > 0,
        "no stream was ever interrupted, so the resume path went untested and a \
         clean sweep would mean only that uninterrupted transfers work"
    );
    assert!(
        resumed > 0,
        "no stream ever continued after making progress, so nothing distinguishes \
         a resume from a restart"
    );
    assert!(completed > 0, "no stream ever finished and installed");
}

/// P20's constraint, as a test rather than as a hope.
///
/// The nemesis weight table replaced six literal ranges. The defaults have to
/// select exactly the actions those ranges did, for every roll, or every pinned
/// fingerprint moves — and the fingerprints moving is a diff nobody can tell
/// from a real regression.
#[test]
fn the_default_weights_reproduce_the_ranges_they_replaced() {
    let weights = NemesisWeights::default();
    assert_eq!(weights.total(), 100);
    for roll in 0..100u32 {
        let expected = match roll {
            0..=24 => NemesisAction::Split,
            25..=39 => NemesisAction::OneWay,
            40..=49 => NemesisAction::Isolate,
            50..=74 => NemesisAction::Heal,
            75..=89 => NemesisAction::Crash,
            _ => NemesisAction::Restart,
        };
        assert_eq!(
            weights.action_for(roll),
            expected,
            "roll {roll} selects a different action than the range it replaced"
        );
    }
}

/// A roll can never fall off the end of the table, whatever the weights are.
///
/// The last arm is a catch-all rather than a bound, so a table whose weights do
/// not sum to the roll's range still selects something instead of panicking or
/// silently doing nothing — which is what a `match` on literal ranges would have
/// done if somebody edited one.
#[test]
fn every_roll_selects_an_action_whatever_the_weights_are() {
    let tables = [
        NemesisWeights::default(),
        NemesisWeights {
            split: 0,
            one_way: 0,
            isolate: 0,
            heal: 1,
            crash: 0,
            restart: 0,
        },
        NemesisWeights {
            split: 3,
            one_way: 0,
            isolate: 0,
            heal: 0,
            crash: 0,
            restart: 0,
        },
    ];
    for weights in tables {
        for roll in 0..200u32 {
            // Does not panic, and is total.
            let _ = weights.action_for(roll);
        }
        // Inside its own range, a weight of zero is never selected.
        if weights.total() > 0 {
            for roll in 0..weights.total() {
                let action = weights.action_for(roll);
                let weight = match action {
                    NemesisAction::Split => weights.split,
                    NemesisAction::OneWay => weights.one_way,
                    NemesisAction::Isolate => weights.isolate,
                    NemesisAction::Heal => weights.heal,
                    NemesisAction::Crash => weights.crash,
                    NemesisAction::Restart => weights.restart,
                };
                assert!(
                    weight > 0,
                    "roll {roll} selected {action:?}, which has weight zero"
                );
            }
        }
    }
}

/// A profile that issues reads has to actually get some answered, or the
/// recency oracle is a check that never ran.
///
/// Three counters rather than one, because a read can fail to happen at three
/// separate points and each of them looks like success from the next one along.
/// Issued but never confirmed is a cluster with no stable leader; confirmed but
/// never answered is a state machine that never caught up; and answered with no
/// *recency window* is a run whose reads all landed before anything was
/// committed, which cannot be stale and therefore demonstrates nothing.
#[test]
fn reads_are_actually_issued_confirmed_and_answered() {
    let mut issued = 0;
    let mut confirmed = 0;
    let mut answered = 0;
    let mut windows = 0;
    for seed in 0..12 {
        let cfg = SimConfig::named("read-hunt", 3).expect("read-hunt exists");
        let mut world = World::new(seed, cfg);
        world.run(40_000);
        assert!(
            !world.is_broken(),
            "read-hunt seed {seed} failed:\n{}",
            world.failure_report()
        );
        issued += world.stats.reads_issued;
        confirmed += world.stats.reads_confirmed;
        answered += world.stats.reads_answered;
        windows += world.stats.read_recency_windows;
    }
    assert!(issued > 0, "no read was ever issued");
    assert!(confirmed > 0, "no read was ever confirmed by the core");
    assert!(answered > 0, "no read was ever answered");
    assert!(
        windows > 0,
        "every read was answered on a cluster with nothing committed, so none of \
         them could have been stale and the recency oracle checked nothing"
    );
}

/// The profiles that predate reads must not issue any.
///
/// This is the other half of "the fingerprints did not move": they did not move
/// because those profiles never touch the read stream, and if one of them
/// started to, the pinned table would go red for a reason that reads as a
/// regression. Asserting it here names the cause instead.
#[test]
fn the_profiles_that_predate_reads_still_issue_none() {
    for profile in ["default", "chaos", "fig8-hunt"] {
        let cfg = SimConfig::named(profile, 3).expect("profile exists");
        let mut world = World::new(3, cfg);
        world.run(20_000);
        assert_eq!(
            world.stats.reads_issued, 0,
            "{profile} issued a read; its pinned fingerprint is no longer meaningful"
        );
    }
}

/// Clock drift is off by default, and a profile that turns it on gets it.
///
/// The property that matters is the *number of draws*, not the periods
/// themselves: `range` draws nothing when its bounds collapse, and a drift of
/// zero must draw nothing at all or every committed fingerprint moves. Two runs
/// of the same seed at different drifts diverging is the observable form of
/// that.
#[test]
fn clock_drift_is_off_unless_a_profile_asks_for_it() {
    let base = SimConfig::named("chaos", 3).expect("chaos exists");
    assert_eq!(base.clock_drift_pct, 0);
    assert_eq!(base.read_pct, 0);

    let reading = SimConfig::named("read-hunt", 3).expect("read-hunt exists");
    assert!(reading.clock_drift_pct > 0);
    assert!(reading.read_pct > 0);

    let mut without = World::new(5, SimConfig::named("chaos", 3).expect("chaos"));
    without.run(5_000);
    let mut drifting = SimConfig::named("chaos", 3).expect("chaos");
    drifting.clock_drift_pct = 25;
    let mut with = World::new(5, drifting);
    with.run(5_000);
    assert_ne!(
        without.fingerprint(),
        with.fingerprint(),
        "turning drift on changed nothing, so the clock stream is never drawn"
    );
}

/// The lease demonstration's two arms, as a test rather than only as a script.
///
/// The script is the artifact; this is the gate. A demonstration that stopped
/// demonstrating would otherwise be noticed only when somebody next ran the
/// script by hand, and the failure it is guarding against — a lease that
/// silently stopped being reachable — looks exactly like success.
#[test]
fn leases_are_only_safe_inside_their_clock_assumption() {
    let control_seeds = 12;

    // Control: the same profile, the same seeds, confirming every read with a
    // heartbeat round. Safe under any clock behaviour, and it has to be clean
    // or the experiment below is measuring the profile rather than the lease.
    for seed in 0..control_seeds {
        let cfg = SimConfig::named("lease-drift", 3).expect("lease-drift exists");
        assert!(
            cfg.lease_read_drift_bound.is_none(),
            "the control holds a lease"
        );
        let mut world = World::new(seed, cfg);
        world.run(40_000);
        assert!(
            !world.is_broken(),
            "the control arm failed on seed {seed}, so the experiment proves nothing:\n{}",
            world.failure_report()
        );
        assert!(
            world.stats.reads_answered > 0,
            "the control answered no reads on seed {seed}"
        );
    }

    // Experiment: the same runs, serving reads from a lease that assumes no
    // clock drift at all, on a cluster whose leader's clock is the slowest in
    // it. The assumption is false and the reads go stale.
    let mut dirty = 0;
    let mut lease_reads = 0;
    for seed in 0..control_seeds {
        let mut cfg = SimConfig::named("lease-drift", 3).expect("lease-drift exists");
        cfg.lease_read_drift_bound = Some(0);
        let mut world = World::new(seed, cfg);
        world.run(40_000);
        lease_reads += world.stats.lease_reads_served;
        if world.is_broken() {
            dirty += 1;
        }
    }
    assert!(
        lease_reads > 0,
        "no read was ever served from a lease, so the experiment exercised nothing"
    );
    assert!(
        dirty > 0,
        "serving reads from a lease on a cluster whose clocks are half a period \
         apart found nothing in {control_seeds} seeds; either the window stopped \
         being reachable or the read oracles stopped looking"
    );
}

/// Pre-vote's cost, measured rather than asserted.
///
/// Unlike every other demonstration in this repository, what pre-vote buys is
/// availability rather than safety — a run without it is not *wrong*, it is
/// disrupted. So this compares two arms instead of failing one, and the number
/// it compares is terms that were entered and produced no leader: a node
/// campaigning where nobody can hear it, raising its term each time, and
/// carrying the total back into a healthy cluster when it reconnects.
#[test]
fn pre_vote_stops_a_partitioned_node_from_burning_terms() {
    let seeds = 20;
    let burned = |pre_vote: bool| {
        let mut total = 0u64;
        for seed in 0..seeds {
            let mut cfg = SimConfig::named("chaos", 3).expect("chaos exists");
            cfg.pre_vote = pre_vote;
            let mut world = World::new(seed, cfg);
            world.run(40_000);
            total += world
                .stats
                .highest_term
                .saturating_sub(world.stats.terms_with_leaders);
        }
        total
    };
    let with = burned(true);
    let without = burned(false);
    assert!(
        without >= with * 3,
        "turning pre-vote off burned {without} terms against {with} with it on — \
         less than the threefold margin this demonstration claims, so it is no \
         longer showing what pre-vote is for"
    );
    assert!(
        without > 50,
        "only {without} terms were burned without pre-vote across {seeds} seeds, \
         which is too few to be a margin rather than noise"
    );
}

/// P23's exit criterion. Membership changes have to actually happen, and the
/// joint configuration has to actually be open while other things are going on.
///
/// Four counters, and each of them is a different way the profile could look
/// like it was testing membership without doing so. Changes proposed but never
/// committed leaves the cluster in the configuration it booted with; a
/// configuration that moves but never through `C_old,new` would mean joint
/// consensus was being skipped; and refusals matter because the one-change-in-
/// flight rule is itself a safety property and a run that never triggered it
/// never checked it.
#[test]
fn membership_actually_changes_and_the_joint_configuration_is_actually_open() {
    let mut proposed = 0;
    let mut refused = 0;
    let mut configurations = 0;
    let mut joint = 0;
    let mut transfers = 0;
    for seed in 0..8 {
        let cfg = SimConfig::named("membership-hunt", 5).expect("membership-hunt exists");
        let mut world = World::new(seed, cfg);
        world.run(40_000);
        assert!(
            !world.is_broken(),
            "membership-hunt seed {seed} failed:\n{}",
            world.failure_report()
        );
        proposed += world.stats.conf_changes_proposed;
        refused += world.stats.conf_changes_refused;
        configurations = configurations.max(world.stats.distinct_configurations);
        joint += world.stats.joint_config_windows;
        transfers += world.stats.transfers_requested;
    }
    assert!(proposed > 0, "no membership change was ever proposed");
    assert!(
        refused > 0,
        "no membership change was ever refused, so the one-in-flight rule was \
         never exercised"
    );
    assert!(
        configurations > 1,
        "the cluster never left the configuration it booted with, so nothing \
         about membership was tested"
    );
    assert!(
        joint > 0,
        "the joint configuration was never observed open, so joint consensus \
         was exercised only as a code path and never as a hazard"
    );
    assert!(transfers > 0, "no leader transfer was ever requested");
}

/// And at three nodes it does nothing, which is a fact about the profile rather
/// than a gap in it.
///
/// A change needs somewhere to change to; the voter floor is three because a
/// cluster of two stops on a single crash; and a simulated cluster cannot start
/// a process that was not in the seed. Three nodes therefore means three
/// voters, no learners, and no legal move. Asserted rather than left implicit,
/// because a profile in the sweep that silently exercises nothing is exactly
/// the shape of [KEEL-4](../../../BUGS.md).
#[test]
fn the_membership_profile_is_inert_at_three_nodes() {
    let cfg = SimConfig::named("membership-hunt", 3).expect("exists");
    let mut world = World::new(11, cfg);
    world.run(30_000);
    assert!(!world.is_broken());
    assert_eq!(
        world.stats.conf_changes_proposed, 0,
        "three nodes proposed a membership change, so the floor is not what it says"
    );
    assert_eq!(world.stats.joint_config_windows, 0);
}

/// The voter set never falls to two, because a cluster of two has a quorum of
/// two and a single crash stops it.
///
/// A harness invariant rather than a property of the cluster, and it is a test
/// because breaking it is how [KEEL-10](../../../BUGS.md) was reached. The
/// original guard read the configuration of whichever node the client had
/// picked, and a node's configuration takes effect at apply time — so a node
/// that was behind was consulted about a membership that had already moved on,
/// and two changes drawn from two stale readings took the voter set somewhere
/// neither intended.
#[test]
fn the_membership_profile_never_takes_the_voter_set_below_three() {
    for seed in 0..40 {
        for nodes in [3usize, 5] {
            let cfg = SimConfig::named("membership-hunt", nodes).expect("exists");
            let mut world = World::new(seed, cfg);
            world.run(30_000);
            assert!(
                world.stats.smallest_voter_set >= 3,
                "seed {seed} at {nodes} nodes reached a voter set of {}, where a single \
                 crash stops the cluster",
                world.stats.smallest_voter_set
            );
        }
    }
}

/// The profiles that predate membership changes still propose none.
///
/// The other half of "their fingerprints did not move", named here so that the
/// pinned table going red would say why.
#[test]
fn the_profiles_that_predate_membership_changes_propose_none() {
    for profile in ["default", "chaos", "fig8-hunt", "read-hunt"] {
        let cfg = SimConfig::named(profile, 3).expect("profile exists");
        assert_eq!(cfg.conf_change_pct, 0, "{profile}");
        assert_eq!(cfg.transfer_pct, 0, "{profile}");
        assert_eq!(cfg.initial_voters, 0, "{profile}");
        let mut world = World::new(3, SimConfig::named(profile, 3).expect("profile"));
        world.run(20_000);
        assert_eq!(
            world.stats.conf_changes_proposed + world.stats.transfers_requested,
            0,
            "{profile} touched the membership stream"
        );
        assert_eq!(
            world.stats.joint_config_windows, 0,
            "{profile} entered a joint configuration without proposing anything"
        );
    }
}
