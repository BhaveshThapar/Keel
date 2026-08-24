//! `keel-chaos` — break a real cluster on a seeded schedule.
//!
//! Five subcommands, and the split between them is deliberate. `plan` prints
//! what a seed would do without doing it, so a schedule can be read before an
//! hour is spent on it. `run` injects, and with `--history` records what real
//! clients observed instead of counting a counter — the file an external
//! checker is handed. `kill-loop` does one fault a thousand times, because a
//! window one cycle in five hundred wide is not something a short run should be
//! trusted to have found. `probe` and `clock-check` are the two halves of the
//! one demonstration that needs its own proof: that a clock jump reached
//! `CLOCK_MONOTONIC` rather than only the wall clock nothing in Raft reads.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use keel_chaos::ChaosError;
use keel_chaos::clock::{self, Faketime, Observed};
use keel_chaos::cluster::{Cluster, ClusterConfig};
use keel_chaos::schedule::{Action, Schedule};

#[derive(Parser)]
#[command(
    name = "keel-chaos",
    about = "Partition, pause, kill and clock-jump a real cluster",
    long_about = "Draws a fault schedule from a seed, prints it, and injects it into a \
                  cluster of real keel-server processes. The schedule replays; the \
                  interleaving does not, which is why the simulator exists and why this \
                  is not a substitute for it."
)]
struct Cli {
    #[command(subcommand)]
    command: Verb,
}

#[derive(Subcommand)]
enum Verb {
    /// Print the schedule a seed produces, and inject nothing.
    Plan {
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 3)]
        nodes: usize,
        #[arg(long, default_value_t = 60)]
        secs: u64,
    },
    /// Stand up a cluster and run the schedule against it.
    Run {
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 3)]
        nodes: usize,
        #[arg(long, default_value_t = 60)]
        secs: u64,
        /// Where the nodes keep their logs and state machines.
        #[arg(long)]
        dir: PathBuf,
        #[arg(long)]
        server_bin: PathBuf,
        /// The `kv` client, used for the workload. Without it the run injects
        /// faults into a cluster nobody is talking to, which proves nothing.
        #[arg(long)]
        kv_bin: PathBuf,
        #[arg(long, default_value = "durable")]
        sync: String,
        /// Record a history here instead of running the counter workload.
        ///
        /// The counter workload answers one question — did an acknowledged
        /// write survive — and answers it with our own arithmetic. A history
        /// answers a different one, and answers it with somebody else's
        /// checker: was what these clients observed consistent with *any*
        /// sequential order at all.
        #[arg(long)]
        history: Option<PathBuf>,
    },
    /// Kill one node at a time, over and over, while clients keep writing.
    ///
    /// A partition is a fault the cluster can wait out. A kill is not: the node
    /// loses everything it had not made durable, and the question is whether
    /// the cluster still holds what it told a client it held. Doing it a
    /// thousand times is how a window one cycle in five hundred wide stops
    /// being something the test got lucky about.
    KillLoop {
        #[arg(long, default_value_t = 1_000)]
        cycles: u64,
        #[arg(long, default_value_t = 3)]
        nodes: usize,
        #[arg(long)]
        dir: PathBuf,
        #[arg(long)]
        server_bin: PathBuf,
        #[arg(long)]
        kv_bin: PathBuf,
        #[arg(long, default_value = "durable")]
        sync: String,
    },
    /// Print a fixed number of `CLOCK_MONOTONIC` readings. Started by
    /// `clock-check` under the faketime preload.
    Probe {
        /// A count rather than a duration, and that is the whole subtlety. A
        /// probe that ran "for four seconds" would measure the deadline against
        /// the clock being faked, so the jump it exists to observe would push it
        /// past its own deadline and it would exit at the moment of the jump —
        /// having recorded every reading except the one that matters.
        #[arg(long, default_value_t = 60)]
        samples: u64,
        #[arg(long, default_value_t = 50)]
        every_ms: u64,
    },
    /// Jump the clock and confirm the jump reached `CLOCK_MONOTONIC`.
    ClockCheck {
        #[arg(long, default_value_t = 30)]
        by_secs: u64,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let outcome = match cli.command {
        Verb::Plan { seed, nodes, secs } => plan(seed, nodes, secs),
        Verb::Run {
            seed,
            nodes,
            secs,
            dir,
            server_bin,
            kv_bin,
            sync,
            history,
        } => run(RunOptions {
            seed,
            nodes,
            secs,
            dir,
            server_bin,
            kv_bin,
            sync,
            history,
        }),
        Verb::KillLoop {
            cycles,
            nodes,
            dir,
            server_bin,
            kv_bin,
            sync,
        } => kill_loop(cycles, nodes, dir, server_bin, kv_bin, sync),
        Verb::Probe { samples, every_ms } => probe(samples, every_ms),
        Verb::ClockCheck { by_secs } => clock_check(by_secs),
    };
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

fn plan(seed: u64, nodes: usize, secs: u64) -> Result<(), ChaosError> {
    let clocks = Faketime::available().is_ok();
    let s = Schedule::draw(seed, nodes, Duration::from_secs(secs), clocks);
    print!("{}", s.render());
    println!("{}", s.counts());
    if !clocks {
        // Said out loud rather than left as a silently shorter schedule.
        if let Err(why) = Faketime::available() {
            println!("clock jumps omitted: {why}");
        }
    }
    Ok(())
}

fn probe(samples: u64, every_ms: u64) -> Result<(), ChaosError> {
    let mut out = std::io::stdout();
    // Flushed every line: the parent reads this stream while the child is still
    // alive, and a buffered probe would deliver every reading at once, after
    // the jump it was supposed to bracket.
    for _ in 0..samples {
        writeln!(out, "t={}", clock::monotonic_ms())?;
        out.flush()?;
        std::thread::sleep(Duration::from_millis(every_ms));
    }
    Ok(())
}

/// The demonstration: a child under the preload, a jump halfway through, and an
/// assertion that what it saw is a discontinuity rather than elapsed time.
fn clock_check(by_secs: u64) -> Result<(), ChaosError> {
    let dir = std::env::temp_dir().join(format!("keel-chaos-clock-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let mut faketime = Faketime::new(&dir)?;

    let me = std::env::current_exe()?;
    let mut cmd = Command::new(&me);
    cmd.args(["probe", "--samples", "60", "--every-ms", "50"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    for (k, v) in faketime.env() {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn()?;
    let Some(stdout) = child.stdout.take() else {
        return Err(ChaosError::Io(std::io::Error::other("probe had no stdout")));
    };

    let started = Instant::now();
    let reader = BufReader::new(stdout);
    let mut readings: Vec<u64> = Vec::new();
    let mut jumped = false;

    for line in reader.lines() {
        let line = line?;
        let Some(value) = line.strip_prefix("t=").and_then(|v| v.parse::<u64>().ok()) else {
            continue;
        };
        readings.push(value);
        // Halfway: late enough that the probe has a baseline, early enough that
        // it keeps reading afterwards.
        if !jumped && readings.len() >= 10 {
            faketime.jump(Duration::from_secs(by_secs), true)?;
            jumped = true;
        }
    }
    let real_delta_ms = started.elapsed().as_millis() as u64;
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);

    let (Some(first), Some(last)) = (readings.first(), readings.last()) else {
        return Err(ChaosError::Violation(
            "the probe produced no readings".into(),
        ));
    };
    let observed = Observed {
        monotonic_delta_ms: last.saturating_sub(*first),
        real_delta_ms,
    };
    println!(
        "probe: {} readings, CLOCK_MONOTONIC advanced {} ms in {} ms of real time",
        readings.len(),
        observed.monotonic_delta_ms,
        observed.real_delta_ms
    );
    if !observed.confirms(by_secs * 1_000) {
        return Err(ChaosError::Violation(format!(
            "the clock jump did not reach CLOCK_MONOTONIC: asked for {} ms, \
             the probe saw {} ms of monotonic time in {} ms of real time",
            by_secs * 1_000,
            observed.monotonic_delta_ms,
            observed.real_delta_ms
        )));
    }
    println!("PASS the jump reached CLOCK_MONOTONIC");
    Ok(())
}

/// A counter, incremented as fast as one client can, with the acknowledgements
/// counted.
///
/// Counted acknowledgements are the only thing a chaos run can assert against
/// afterwards. An unacknowledged write may or may not have applied — that is
/// not a bug, it is what a timeout means — so the property is one-sided: the
/// final value may exceed the acknowledgements, and may never fall short.
struct Workload {
    acked: Arc<AtomicU64>,
    attempted: Arc<AtomicU64>,
    stop: Arc<std::sync::atomic::AtomicBool>,
}

impl Workload {
    fn start(kv_bin: PathBuf, nodes: Vec<String>) -> (Self, std::thread::JoinHandle<()>) {
        let acked = Arc::new(AtomicU64::new(0));
        let attempted = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handle = {
            let acked = Arc::clone(&acked);
            let attempted = Arc::clone(&attempted);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut nonce = 1u64;
                while !stop.load(Ordering::SeqCst) {
                    nonce += 1;
                    attempted.fetch_add(1, Ordering::SeqCst);
                    let mut cmd = Command::new(&kv_bin);
                    for n in &nodes {
                        cmd.arg("--node").arg(n);
                    }
                    // A fresh nonce per attempt. Reusing one would reopen the
                    // same session with the sequence number back at 1, and the
                    // dedup cache would answer from the previous run — an
                    // acknowledgement for a write that never happened, which
                    // would make the run's central assertion meaningless.
                    let out = cmd
                        .arg("--nonce")
                        .arg(nonce.to_string())
                        .args(["incr", "chaos-counter", "--by", "1"])
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status();
                    if matches!(out, Ok(status) if status.success()) {
                        acked.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        };
        (
            Self {
                acked,
                attempted,
                stop,
            },
            handle,
        )
    }

    fn finish(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

/// A history recorded by real clients while the faults are landing.
///
/// Started as a child process rather than in a thread, because the recorder is
/// `kv workload` — the same client a person would use, not a special one
/// written to be easy to check. A checker handed a history produced by a
/// purpose-built client is checking the purpose-built client.
fn start_recorder(
    kv_bin: &Path,
    nodes: &[String],
    secs: u64,
    out: &Path,
) -> Result<std::process::Child, ChaosError> {
    let mut cmd = Command::new(kv_bin);
    for n in nodes {
        cmd.arg("--node").arg(n);
    }
    cmd.args(["workload", "--clients", "8", "--keys", "4", "--secs"])
        // Past the last fault, so the history covers the recovery as well as
        // the damage. A history that stopped at the last kill would never show
        // whether the cluster came back consistent.
        .arg((secs + 8).to_string())
        .arg("--out")
        .arg(out)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    Ok(cmd.spawn()?)
}

struct RunOptions {
    seed: u64,
    nodes: usize,
    secs: u64,
    dir: PathBuf,
    server_bin: PathBuf,
    kv_bin: PathBuf,
    sync: String,
    history: Option<PathBuf>,
}

fn run(opts: RunOptions) -> Result<(), ChaosError> {
    let RunOptions {
        seed,
        nodes,
        secs,
        dir,
        server_bin,
        kv_bin,
        sync,
        history,
    } = opts;
    if !server_bin.exists() {
        return Err(ChaosError::NoBinary(server_bin.display().to_string()));
    }
    if !kv_bin.exists() {
        return Err(ChaosError::NoBinary(kv_bin.display().to_string()));
    }
    std::fs::create_dir_all(&dir)?;

    // Clock control is decided before the schedule is drawn, so the plan
    // printed below is the plan that runs.
    let mut faketime = match Faketime::new(&dir) {
        Ok(f) => Some(f),
        Err(e) => {
            println!("clock nemesis unavailable: {e}");
            None
        }
    };
    let duration = Duration::from_secs(secs);
    let schedule = Schedule::draw(seed, nodes, duration, faketime.is_some());
    print!("{}", schedule.render());
    println!("{}", schedule.counts());
    if !schedule.ends_healthy() {
        return Err(ChaosError::Violation(
            "the schedule does not end with a healthy cluster; the final check would be \
             measuring the harness"
                .into(),
        ));
    }

    let mut cfg = ClusterConfig::new(nodes, &dir, &server_bin);
    cfg.sync = sync;
    if let Some(f) = faketime.as_ref() {
        cfg.env = f.env();
    }
    let mut cluster = Cluster::start(cfg)?;
    println!("cluster up: {} nodes", cluster.nodes());

    let client_nodes: Vec<String> = cluster.client_addrs.iter().map(|a| a.to_string()).collect();
    // One workload or the other, never both: two workloads would make each
    // other's timings, and the history handed to a checker would be a history
    // of a cluster busy with something the history does not mention.
    let mut recorder = match history.as_deref() {
        Some(path) => Some(start_recorder(&kv_bin, &client_nodes, secs, path)?),
        None => None,
    };
    let counter = recorder
        .is_none()
        .then(|| Workload::start(kv_bin.clone(), client_nodes.clone()));

    // Inject. Faults that fail because the target is already down are recorded
    // rather than fatal: a schedule drawn before the run cannot know that a
    // node it wants to pause is one the previous fault killed.
    let started = Instant::now();
    let mut injected = 0u64;
    let mut skipped = 0u64;
    for event in &schedule.events {
        let due = started + event.at;
        while Instant::now() < due {
            std::thread::sleep(Duration::from_millis(5));
        }
        let outcome = match (&event.action, faketime.as_mut()) {
            (Action::ClockJump { forward, by }, Some(f)) => f.jump(*by, *forward).map(|offset| {
                println!(
                    "  {:>8.3}s  {} (offset now {offset:+}s)",
                    event.at.as_secs_f64(),
                    event.action
                );
            }),
            (Action::ClockJump { .. }, None) => {
                skipped += 1;
                continue;
            }
            (action, _) => cluster.apply(action),
        };
        match outcome {
            Ok(()) => {
                injected += 1;
                if !matches!(event.action, Action::ClockJump { .. }) {
                    println!("  {:>8.3}s  {}", event.at.as_secs_f64(), event.action);
                }
            }
            Err(e) => {
                skipped += 1;
                println!(
                    "  {:>8.3}s  {} skipped: {e}",
                    event.at.as_secs_f64(),
                    event.action
                );
            }
        }
    }

    // Heal, and give the cluster long enough to elect and catch up before
    // asking it anything. A read taken during the election that follows the
    // last fault reports a timeout and blames the code.
    cluster.heal();
    for i in 0..cluster.nodes() {
        let _ = cluster.process(i).and_then(|p| p.resume());
    }
    std::thread::sleep(Duration::from_secs(5));

    let (carried, refused, severed) = cluster.traffic();
    println!("faults injected {injected}, skipped {skipped}");
    println!("proxy carried {carried} bytes, refused {refused} connections, severed {severed}");

    // The ways this run could have proved nothing, refused before anything is
    // asserted about the cluster.
    if injected == 0 {
        return Err(ChaosError::Violation(
            "no fault was successfully injected".into(),
        ));
    }
    if refused + severed == 0 && schedule.counts().isolations + schedule.counts().splits > 0 {
        return Err(ChaosError::Violation(
            "the schedule contained partitions but the proxy never dropped a connection; \
             the nodes are not talking through the mesh"
                .into(),
        ));
    }

    if let (Some(child), Some(path)) = (recorder.take(), history.as_deref()) {
        let output = child.wait_with_output()?;
        print!("{}", String::from_utf8_lossy(&output.stdout));
        if !output.status.success() {
            return Err(ChaosError::Violation(format!(
                "the recorder failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let recorded = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        if recorded == 0 {
            return Err(ChaosError::Violation(
                "the history file is empty, so there is nothing for a checker to check".into(),
            ));
        }
        println!(
            "PASS a history of {recorded} bytes is at {}",
            path.display()
        );
        return Ok(());
    }

    let Some((workload, handle)) = counter else {
        return Err(ChaosError::Violation("no workload ran".into()));
    };
    workload.finish();
    let _ = handle.join();
    let acked = workload.acked.load(Ordering::SeqCst);
    let attempted = workload.attempted.load(Ordering::SeqCst);
    println!("workload attempted {attempted}, acknowledged {acked}");
    if acked == 0 {
        return Err(ChaosError::Violation(
            "the workload never got an acknowledgement, so there is nothing to check".into(),
        ));
    }

    let final_value = read_counter(&kv_bin, &client_nodes)?;
    println!("counter: acknowledged {acked}, final value {final_value}");
    // The counter is signed because `incr` is; a negative one would mean the
    // state machine applied something this workload never proposed.
    if final_value < 0 || (final_value as u64) < acked {
        return Err(ChaosError::Violation(format!(
            "{} acknowledged increments were lost: the counter reads {final_value}",
            acked.saturating_sub(final_value.max(0) as u64)
        )));
    }
    println!("PASS no acknowledged write was lost");
    Ok(())
}

/// Kill one node at a time, a thousand times, while clients keep writing.
///
/// Round robin rather than random. A thousand random kills over three nodes
/// leaves every node killed about three hundred times, which is fine, but it
/// also leaves the *sequence* different on every run — and the interesting
/// cycles are the ones where the node killed is the one that had just become
/// leader. Round robin makes that reachable on a schedule anybody can rerun.
///
/// The wait between kill and restart is deliberately short. A loop that waited
/// for the cluster to settle before each kill would be testing a healthy
/// cluster a thousand times; killing the next node while the last one is still
/// catching up is the case where a node's log and its state machine can
/// disagree about what has been applied.
fn kill_loop(
    cycles: u64,
    nodes: usize,
    dir: PathBuf,
    server_bin: PathBuf,
    kv_bin: PathBuf,
    sync: String,
) -> Result<(), ChaosError> {
    if !server_bin.exists() {
        return Err(ChaosError::NoBinary(server_bin.display().to_string()));
    }
    if !kv_bin.exists() {
        return Err(ChaosError::NoBinary(kv_bin.display().to_string()));
    }
    std::fs::create_dir_all(&dir)?;

    let mut cfg = ClusterConfig::new(nodes, &dir, &server_bin);
    cfg.sync = sync.clone();
    let mut cluster = Cluster::start(cfg)?;
    let client_nodes: Vec<String> = cluster.client_addrs.iter().map(|a| a.to_string()).collect();
    println!("cluster up: {nodes} nodes, sync {sync}, {cycles} cycles");

    let (workload, handle) = Workload::start(kv_bin.clone(), client_nodes.clone());
    let started = Instant::now();
    let mut restart_failures = 0u64;

    for cycle in 0..cycles {
        let victim = (cycle as usize) % nodes;
        if let Err(e) = cluster.process(victim).and_then(|p| p.kill()) {
            // A node that was already down is not a cycle that did nothing:
            // the previous restart failed, and that is worth counting rather
            // than swallowing.
            println!("cycle {cycle}: n{victim} could not be killed: {e}");
        }
        if let Err(e) = cluster.start_node(victim) {
            restart_failures += 1;
            println!("cycle {cycle}: n{victim} did not come back: {e}");
            // One that never comes back takes the cluster down with it after
            // enough cycles, and every later cycle would report the same thing.
            if restart_failures > 3 {
                return Err(ChaosError::Violation(format!(
                    "{restart_failures} nodes failed to restart; the run stopped at cycle {cycle}"
                )));
            }
        }
        if cycle % 100 == 99 {
            println!(
                "  {} cycles, {} acknowledged writes, {:.0}s elapsed",
                cycle + 1,
                workload.acked.load(Ordering::SeqCst),
                started.elapsed().as_secs_f64()
            );
        }
    }

    // Let the last restarted node catch up before the final read, and let the
    // workload get its last acknowledgement in.
    std::thread::sleep(Duration::from_secs(5));
    workload.finish();
    let _ = handle.join();

    let acked = workload.acked.load(Ordering::SeqCst);
    let attempted = workload.attempted.load(Ordering::SeqCst);
    println!(
        "{cycles} kill cycles in {:.0}s; workload attempted {attempted}, acknowledged {acked}; \
         {restart_failures} restarts failed",
        started.elapsed().as_secs_f64()
    );
    if acked == 0 {
        return Err(ChaosError::Violation(
            "the workload never got an acknowledgement, so there is nothing to check".into(),
        ));
    }

    let final_value = read_counter(&kv_bin, &client_nodes)?;
    println!("counter: acknowledged {acked}, final value {final_value}");
    if final_value < 0 || (final_value as u64) < acked {
        return Err(ChaosError::Violation(format!(
            "{} acknowledged increments were lost over {cycles} kill cycles: \
             the counter reads {final_value}",
            acked.saturating_sub(final_value.max(0) as u64)
        )));
    }
    println!("PASS {cycles} kill cycles, no acknowledged write lost");
    Ok(())
}

fn read_counter(kv_bin: &PathBuf, nodes: &[String]) -> Result<i64, ChaosError> {
    // Retried: the first read after a heal can land during an election.
    let mut last = String::new();
    let mut nonce = u64::MAX / 2;
    for _ in 0..20 {
        nonce += 1;
        let mut cmd = Command::new(kv_bin);
        for n in nodes {
            cmd.arg("--node").arg(n);
        }
        // `incr --by 0` rather than `get`, because a counter is stored as eight
        // little-endian bytes and `get` prints them through a lossy UTF-8
        // conversion — which turns 1346 into "B\u{5}\0\0\0\0\0\0" and then into
        // a parse failure reported as "the cluster never answered".
        let out = cmd
            .arg("--nonce")
            .arg(nonce.to_string())
            .args(["incr", "chaos-counter", "--by", "0"])
            .output()?;
        if out.status.success() {
            let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if let Ok(v) = text.parse::<i64>() {
                return Ok(v);
            }
            last = text;
        } else {
            last = String::from_utf8_lossy(&out.stderr).trim().to_string();
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    Err(ChaosError::Violation(format!(
        "the cluster never answered a read after healing; last reply: {last:?}"
    )))
}
