//! `keel-chaos` — break a real cluster on a seeded schedule.
//!
//! Four subcommands, and the split between them is deliberate. `plan` prints
//! what a seed would do without doing it, so a schedule can be read before an
//! hour is spent on it. `run` injects. `probe` and `clock-check` are the two
//! halves of the one demonstration that needs its own proof: that a clock jump
//! reached `CLOCK_MONOTONIC` rather than only the wall clock nothing in Raft
//! reads.

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
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
        } => run(seed, nodes, secs, dir, server_bin, kv_bin, sync),
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

#[allow(clippy::too_many_arguments)]
fn run(
    seed: u64,
    nodes: usize,
    secs: u64,
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
    let (workload, handle) = Workload::start(kv_bin.clone(), client_nodes.clone());

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
    workload.finish();
    let _ = handle.join();

    let acked = workload.acked.load(Ordering::SeqCst);
    let attempted = workload.attempted.load(Ordering::SeqCst);
    let (carried, refused, severed) = cluster.traffic();
    println!(
        "faults injected {injected}, skipped {skipped}; \
         workload attempted {attempted}, acknowledged {acked}; \
         proxy carried {carried} bytes, refused {refused} connections, severed {severed}"
    );

    // The two ways this run could have proved nothing, both refused.
    if injected == 0 {
        return Err(ChaosError::Violation(
            "no fault was successfully injected".into(),
        ));
    }
    if acked == 0 {
        return Err(ChaosError::Violation(
            "the workload never got an acknowledgement, so there is nothing to check".into(),
        ));
    }
    if refused + severed == 0 && schedule.counts().isolations + schedule.counts().splits > 0 {
        return Err(ChaosError::Violation(
            "the schedule contained partitions but the proxy never dropped a connection; \
             the nodes are not talking through the mesh"
                .into(),
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
