use std::process::ExitCode;

use clap::{Parser, Subcommand};
use keel_sim::{SimConfig, World, run_seed};

#[derive(Parser)]
#[command(
    name = "keel-sim",
    about = "Deterministic simulator for the Keel Raft core",
    long_about = "Drives seeded clusters through partitions, crashes, message loss, and \
                  clock skew, checking every Raft safety property after every event. A \
                  failure prints the seed that produced it; rerunning that seed reproduces \
                  the run exactly."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Sweep a range of seeds.
    Run {
        #[arg(long, default_value_t = 0)]
        from: u64,
        #[arg(long, default_value_t = 200)]
        count: u64,
        #[arg(long, default_value_t = 50_000)]
        steps: u64,
        #[arg(long, default_value_t = 5)]
        nodes: usize,
        /// "default" for steady traffic with occasional faults, "chaos" for
        /// heavy loss and one entry per message.
        #[arg(long, default_value = "default")]
        profile: String,
        /// Stop at the first failing seed instead of sweeping to the end.
        #[arg(long)]
        fail_fast: bool,
        /// Compile out the Figure 8 current-term commit rule. The simulator
        /// should then find a State Machine Safety violation, which is how the
        /// checker is shown to have teeth (TR-8c). Requires --features
        /// negative-demos.
        #[arg(long)]
        disable_fig8_guard: bool,
        /// Leave a torn tail on disk instead of zeroing it. Requires
        /// --features negative-demos.
        #[arg(long)]
        skip_tail_erase: bool,
        /// Accept a record whose checksum does not match. Requires
        /// --features negative-demos.
        #[arg(long)]
        skip_record_crc: bool,
        /// Apply committed entries in fsync-completion order rather than index
        /// order. Requires --features negative-demos.
        #[arg(long)]
        skip_apply_ordering: bool,
        /// Turn pre-vote off.
        ///
        /// Not a removed safety rule and not behind a feature: pre-vote is a
        /// shipped option, and what it buys is availability rather than safety.
        /// The demonstration is therefore a comparison of two arms rather than
        /// one arm failing.
        #[arg(long)]
        no_pre_vote: bool,
        /// Serve reads from the leader's lease, assuming clock drift stays
        /// under this percentage.
        ///
        /// Also a shipped option. A smaller bound means a *longer* lease, so
        /// `--lease-reads 0` is the most optimistic setting there is and the
        /// one a clock that actually drifts falsifies fastest.
        #[arg(long)]
        lease_reads: Option<u8>,
        /// Redraw each node's tick period by this percentage on every tick.
        #[arg(long)]
        clock_drift_pct: Option<u64>,
        /// Draw each node's tick period this far from the nominal one, once, at
        /// construction. Persistent disagreement about how fast time passes,
        /// as opposed to the wandering `--clock-drift-pct` produces.
        #[arg(long)]
        clock_skew_pct: Option<u64>,
        /// Give node 1 the slowest clock and everybody else the fastest.
        #[arg(long)]
        slowest_node_first: bool,
        /// What fraction of client operations are linearizable reads.
        #[arg(long)]
        read_pct: Option<u32>,
    },
    /// Replay one seed and print what it did.
    Repro {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value_t = 50_000)]
        steps: u64,
        #[arg(long, default_value_t = 5)]
        nodes: usize,
        #[arg(long, default_value = "default")]
        profile: String,
        #[arg(long)]
        disable_fig8_guard: bool,
        #[arg(long)]
        skip_tail_erase: bool,
        #[arg(long)]
        skip_record_crc: bool,
        #[arg(long)]
        skip_apply_ordering: bool,
        #[arg(long)]
        no_pre_vote: bool,
        #[arg(long)]
        lease_reads: Option<u8>,
        #[arg(long)]
        clock_drift_pct: Option<u64>,
        #[arg(long)]
        clock_skew_pct: Option<u64>,
        /// Give node 1 the slowest clock and everybody else the fastest.
        #[arg(long)]
        slowest_node_first: bool,
        /// What fraction of client operations are linearizable reads.
        #[arg(long)]
        read_pct: Option<u32>,
    },
    /// Run a seed twice and require identical results.
    Determinism {
        #[arg(long, default_value_t = 0)]
        from: u64,
        #[arg(long, default_value_t = 20)]
        count: u64,
        #[arg(long, default_value_t = 20_000)]
        steps: u64,
        #[arg(long, default_value_t = 5)]
        nodes: usize,
        /// The gate exists to catch nondeterminism, and the disk is now inside
        /// the fingerprint — so it can only cover the tear model if it is
        /// pointed at a profile that tears.
        #[arg(long, default_value = "default")]
        profile: String,
    },
}

/// Which safety rules a run has had removed, and which shipped options it has
/// been asked to run with.
///
/// The two are kept in one struct and told apart by `any_rule_removed`. A
/// removed rule is a build that must never ship and is gated behind a cargo
/// feature; an option is behaviour a real deployment can choose, and a
/// demonstration that turns one on is falsifying the *assumption* it comes
/// with rather than the code.
#[derive(Clone, Copy, Default)]
struct Removed {
    fig8_guard: bool,
    tail_erase: bool,
    record_crc: bool,
    apply_ordering: bool,
    no_pre_vote: bool,
    lease_reads: Option<u8>,
    clock_drift_pct: Option<u64>,
    clock_skew_pct: Option<u64>,
    slowest_node_first: bool,
    read_pct: Option<u32>,
}

fn config_with_guard(profile: &str, nodes: usize, removed: Removed) -> Option<SimConfig> {
    let any =
        removed.fig8_guard || removed.tail_erase || removed.record_crc || removed.apply_ordering;
    if any && !cfg!(feature = "negative-demos") {
        eprintln!(
            "a --skip/--disable flag does nothing unless keel-sim is built with \
             --features negative-demos"
        );
    }
    let mut cfg = SimConfig::named(profile, nodes)?;
    cfg.disable_fig8_guard = removed.fig8_guard;
    cfg.skip_tail_erase = removed.tail_erase;
    cfg.skip_record_crc = removed.record_crc;
    cfg.skip_apply_ordering = removed.apply_ordering;
    if removed.no_pre_vote {
        cfg.pre_vote = false;
    }
    if let Some(bound) = removed.lease_reads {
        cfg.lease_read_drift_bound = Some(bound);
    }
    if let Some(pct) = removed.clock_drift_pct {
        cfg.clock_drift_pct = pct;
    }
    if let Some(pct) = removed.clock_skew_pct {
        cfg.clock_skew_pct = pct;
    }
    if removed.slowest_node_first {
        cfg.slowest_node_first = true;
    }
    if let Some(pct) = removed.read_pct {
        cfg.read_pct = pct;
    }
    Some(cfg)
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Run {
            from,
            count,
            steps,
            nodes,
            profile,
            fail_fast,
            disable_fig8_guard,
            skip_tail_erase,
            skip_record_crc,
            skip_apply_ordering,
            no_pre_vote,
            lease_reads,
            clock_drift_pct,
            clock_skew_pct,
            slowest_node_first,
            read_pct,
        } => {
            let removed = Removed {
                fig8_guard: disable_fig8_guard,
                tail_erase: skip_tail_erase,
                record_crc: skip_record_crc,
                apply_ordering: skip_apply_ordering,
                no_pre_vote,
                lease_reads,
                clock_drift_pct,
                clock_skew_pct,
                slowest_node_first,
                read_pct,
            };
            let Some(cfg) = config_with_guard(&profile, nodes, removed) else {
                eprintln!(
                    "unknown profile {profile:?}; expected one of {:?}",
                    SimConfig::PROFILES
                );
                return ExitCode::FAILURE;
            };
            let mut failures = Vec::new();
            let mut committed_total = 0u64;
            for seed in from..from + count {
                let outcome = run_seed(seed, steps, cfg.clone());
                committed_total += outcome.stats.committed;
                if !outcome.passed() {
                    if let Some(report) = &outcome.report {
                        eprintln!("{report}");
                    }
                    failures.push(seed);
                    if fail_fast {
                        break;
                    }
                }
            }
            let checked = count.max(1);
            println!(
                "{} seeds x {steps} steps, {nodes} nodes, {profile} profile: {} passed, {} failed \
                 (mean {} entries committed per run)",
                count,
                count - failures.len() as u64,
                failures.len(),
                committed_total / checked
            );
            if failures.is_empty() {
                ExitCode::SUCCESS
            } else {
                eprintln!("failing seeds: {failures:?}");
                ExitCode::FAILURE
            }
        }

        Command::Repro {
            seed,
            steps,
            nodes,
            profile,
            disable_fig8_guard,
            skip_tail_erase,
            skip_record_crc,
            skip_apply_ordering,
            no_pre_vote,
            lease_reads,
            clock_drift_pct,
            clock_skew_pct,
            slowest_node_first,
            read_pct,
        } => {
            let removed = Removed {
                fig8_guard: disable_fig8_guard,
                tail_erase: skip_tail_erase,
                record_crc: skip_record_crc,
                apply_ordering: skip_apply_ordering,
                no_pre_vote,
                lease_reads,
                clock_drift_pct,
                clock_skew_pct,
                slowest_node_first,
                read_pct,
            };
            let Some(cfg) = config_with_guard(&profile, nodes, removed) else {
                eprintln!(
                    "unknown profile {profile:?}; expected one of {:?}",
                    SimConfig::PROFILES
                );
                return ExitCode::FAILURE;
            };
            let mut world = World::new(seed, cfg);
            world.run(steps);
            if world.is_broken() {
                eprintln!("{}", world.failure_report());
                return ExitCode::FAILURE;
            }
            let s = &world.stats;
            println!("seed {seed} passed");
            println!("  virtual time     {} ms", world.now() / 1_000_000);
            println!("  events           {}", s.events);
            println!(
                "  messages         {} sent, {} lost",
                s.messages_sent, s.messages_dropped
            );
            println!("  elections        {}", s.elections);
            println!("  crashes          {}", s.crashes);
            println!("  partitions       {}", s.partitions);
            println!("  proposals        {}", s.proposals);
            println!("  sessions opened  {}", s.sessions_opened);
            println!("  commands applied {}", s.commands_applied);
            println!(
                "  commands refused no-session/stale {}/{}",
                s.commands_without_a_session, s.commands_with_a_stale_sequence
            );
            println!(
                "  reads issued/confirmed/answered {}/{}/{}",
                s.reads_issued, s.reads_confirmed, s.reads_answered
            );
            println!("  read recency windows {}", s.read_recency_windows);
            println!(
                "  lease reads served {} ({} behind the cluster)",
                s.lease_reads_served, s.lease_reads_behind_the_cluster
            );
            println!(
                "  leaders deposed with a quorum {}",
                s.leaders_deposed_with_a_quorum
            );
            println!("  committed index  {}", s.committed);
            println!("  applied index    {}", s.applied);
            println!("  live nodes       {}/{nodes}", world.live_nodes());
            println!("  terms led        {}", s.terms_with_leaders);
            println!("  highest term     {}", s.highest_term);
            println!(
                "  terms burned without a leader {}",
                s.highest_term.saturating_sub(s.terms_with_leaders)
            );
            println!("  entries rewritten {}", s.entries_rewritten);
            println!("  old-term commit windows {}", s.old_term_commit_windows);
            println!("  figure-8 bypasses {}", s.fig8_bypasses);
            println!("  torn tails       {}", s.torn_tails);
            println!("  bytes discarded  {}", s.bytes_discarded_by_tears);
            println!("  commits clamped  {}", s.commits_clamped);
            println!("  segments recovered {}", s.segments_recovered);
            println!("  tears during partition {}", s.tears_during_partition);
            let d = world.disk_stats();
            println!(
                "  crashes with writes in flight {}",
                d.crashes_with_writes_in_flight
            );
            println!("  bytes in flight at crash {}", d.bytes_in_flight_at_crash);
            println!(
                "  sectors landed/lost {}/{}",
                d.sectors_that_reached_the_device, d.sectors_the_crash_took_back
            );
            println!(
                "  writes lost/whole/head/tail/pieces {}/{}/{}/{}/{}",
                d.writes_lost_whole,
                d.writes_that_landed_whole,
                d.writes_that_landed_head_first,
                d.writes_that_landed_tail_first,
                d.writes_that_landed_in_pieces
            );
            println!(
                "  files left with a hole {}",
                d.files_a_crash_left_a_hole_in
            );
            println!("  fingerprint      {:016x}", world.fingerprint());
            ExitCode::SUCCESS
        }

        Command::Determinism {
            from,
            count,
            steps,
            nodes,
            profile,
        } => {
            let Some(cfg) = SimConfig::named(&profile, nodes) else {
                eprintln!(
                    "unknown profile {profile:?}; expected one of {:?}",
                    SimConfig::PROFILES
                );
                return ExitCode::FAILURE;
            };
            for seed in from..from + count {
                let a = run_seed(seed, steps, cfg.clone());
                let b = run_seed(seed, steps, cfg.clone());
                if a.fingerprint != b.fingerprint {
                    eprintln!(
                        "seed {seed} is not deterministic: {:016x} then {:016x}",
                        a.fingerprint, b.fingerprint
                    );
                    return ExitCode::FAILURE;
                }
            }
            println!("{count} seeds replayed identically");
            ExitCode::SUCCESS
        }
    }
}
