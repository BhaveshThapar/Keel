//! `keel-bench` — measure a running cluster, and refuse to publish what should
//! not be published.
//!
//! Five subcommands. `gate` says what this host is allowed to produce and why,
//! measuring nothing, so the answer is knowable before an hour is spent on a
//! number that cannot be used. `run` is one measurement at one offered rate.
//! `campaign` sweeps a range of rates and writes the curve — PR-2, because a
//! single throughput figure is a claim about a saturation point whose latency
//! nobody quoted. `failover` kills the leader repeatedly and times the recovery.
//!
//! `campaign`, `failover`, and `snapshot` start their own cluster, so a
//! measurement is one command rather than a procedure. That matters more than
//! it sounds: a benchmark that needs three terminals is a benchmark that gets
//! run once.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand, ValueEnum};
use keel_api::{Command, Query};
use keel_bench::failover::Trial;
use keel_bench::plot::{Point, Series, throughput_vs_latency};
use keel_bench::workload::{Loop, Mix, Op, key_bytes, parse_nodes, value_bytes};
use keel_bench::{Admitted, Environment, Publishable, Tier, workload, write_result};
use keel_chaos::cluster::{Cluster, ClusterConfig};
use keel_client::{Client, Pipeline};
use keel_log::SyncMode;

#[derive(Parser)]
#[command(
    name = "keel-bench",
    about = "Measure a Keel cluster, under a gate that refuses unpublishable numbers",
    long_about = "Every result carries the host it ran on, the filesystem its data \
                  directory was on, and the tier it may be quoted at. A run on memory \
                  or with fsync off is refused, or recorded as an admitted control with \
                  the reason stamped into it."
)]
struct Cli {
    #[command(subcommand)]
    command: Verb,
}

#[derive(Clone, Copy, ValueEnum)]
enum TierArg {
    Exploratory,
    Reference,
}

impl From<TierArg> for Tier {
    fn from(value: TierArg) -> Self {
        match value {
            TierArg::Exploratory => Self::Exploratory,
            TierArg::Reference => Self::Reference,
        }
    }
}

#[derive(Subcommand)]
enum Verb {
    /// Say what this host may publish, and measure nothing.
    Gate {
        #[arg(long, default_value = ".")]
        dir: String,
        #[arg(long, value_enum, default_value_t = TierArg::Exploratory)]
        tier: TierArg,
    },
    /// One measurement at one offered rate, against a cluster somebody else is
    /// running.
    Run {
        #[arg(long)]
        nodes: String,
        #[arg(long, default_value = "writes")]
        mix: String,
        /// Offered operations per second. Zero means closed-loop instead.
        #[arg(long, default_value_t = 0)]
        rate: u64,
        #[arg(long, default_value_t = 8)]
        clients: usize,
        #[arg(long, default_value_t = 10)]
        secs: u64,
        #[arg(long, default_value_t = 128)]
        value_bytes: usize,
        #[arg(long, default_value_t = 10_000)]
        keys: u64,
        /// How many requests one sender may keep outstanding.
        ///
        /// One is a closed client: send, wait for the answer, send again. That
        /// caps achievable throughput at senders divided by per-request
        /// latency whatever the cluster could do, so any number measured at
        /// depth 1 is partly a measurement of this harness. It is carried into
        /// the result header for that reason.
        #[arg(long, default_value_t = 1)]
        depth: usize,
        #[arg(long, default_value_t = 1)]
        seed: u64,
    },
    /// A sweep of offered rates, and the curve it makes.
    Campaign {
        #[arg(long, default_value = "writes")]
        mix: String,
        /// Offered rates, comma separated. Each becomes one point.
        #[arg(long, default_value = "200,500,1000,2000,4000")]
        rates: String,
        #[arg(long, default_value_t = 8)]
        clients: usize,
        #[arg(long, default_value_t = 6)]
        secs: u64,
        #[arg(long, default_value_t = 128)]
        value_bytes: usize,
        #[arg(long, default_value_t = 10_000)]
        keys: u64,
        /// How many requests one sender may keep outstanding.
        ///
        /// One is a closed client: send, wait for the answer, send again. That
        /// caps achievable throughput at senders divided by per-request
        /// latency whatever the cluster could do, so any number measured at
        /// depth 1 is partly a measurement of this harness. It is carried into
        /// the result header for that reason.
        #[arg(long, default_value_t = 1)]
        depth: usize,
        /// Independent repetitions per rate. Three is the floor the gate
        /// enforces; the median of them is what is plotted.
        #[arg(long, default_value_t = 3)]
        runs: usize,
        #[arg(long, default_value_t = 3)]
        cluster_nodes: usize,
        /// Milliseconds per tick of the consensus clock.
        ///
        /// Not a detail. The daemon's loop turns on this granularity, and a
        /// parked client request needs several turns to be answered — propose,
        /// commit, apply — so the tick is an upper bound on write throughput
        /// that has nothing to do with the disk. The fsync-off ablation is what
        /// showed that: it reads the same number.
        // Checkpoint creation and digesting are synchronous by design. A
        // large Reference checkpoint must not outlast the 100--200 ms election
        // window and turn this transfer benchmark into ordinary log catch-up.
        #[arg(long, default_value_t = 100)]
        tick_ms: u64,
        /// Where the cluster's data lives, which is also the filesystem the
        /// gate probes.
        #[arg(long)]
        dir: String,
        #[arg(long)]
        server_bin: String,
        #[arg(long, default_value = "durable")]
        sync: String,
        /// A file name. It goes under results/bench/, and the directory is not
        /// this flag's to choose.
        #[arg(long, default_value = "campaign.txt")]
        out: String,
        #[arg(long, default_value = "campaign.svg")]
        svg: String,
        #[arg(long, default_value = ".")]
        root: String,
        /// Record even though the gate refuses, saying what the run is for.
        #[arg(long)]
        admit: Option<String>,
        #[arg(long, value_enum, default_value_t = TierArg::Exploratory)]
        tier: TierArg,
    },
    /// Kill the leader at steady state, repeatedly, and time the recovery.
    Failover {
        /// PR-5 asks for at least a hundred. Failover time is dominated by a
        /// randomised election timeout — that is what stops two candidates
        /// splitting the vote forever — so a handful of trials samples a
        /// distribution whose whole purpose is to be wide.
        #[arg(long, default_value_t = 100)]
        trials: usize,
        #[arg(long, default_value_t = 3)]
        cluster_nodes: usize,
        #[arg(long)]
        dir: String,
        #[arg(long)]
        server_bin: String,
        #[arg(long, default_value = "durable")]
        sync: String,
        /// Milliseconds per tick. The election timeout is a multiple of this.
        #[arg(long, default_value_t = 30)]
        tick_ms: u64,
        #[arg(long, default_value = "failover.txt")]
        out: String,
        #[arg(long, default_value = ".")]
        root: String,
        #[arg(long)]
        admit: Option<String>,
        #[arg(long, value_enum, default_value_t = TierArg::Exploratory)]
        tier: TierArg,
    },
    /// Build a checkpointed state, then time creation and transfer to a
    /// follower whose log is behind the compacted floor.
    Snapshot {
        #[arg(long, default_value_t = 1 << 30)]
        state_bytes: usize,
        #[arg(long, default_value_t = 1 << 20)]
        value_bytes: usize,
        #[arg(long, default_value_t = 32)]
        depth: usize,
        #[arg(long, default_value_t = 3)]
        runs: usize,
        #[arg(long)]
        dir: String,
        #[arg(long)]
        server_bin: String,
        #[arg(long, default_value = "durable")]
        sync: String,
        #[arg(long, default_value_t = 10)]
        tick_ms: u64,
        #[arg(long, default_value = "snapshot.txt")]
        out: String,
        #[arg(long, default_value = ".")]
        root: String,
        #[arg(long)]
        admit: Option<String>,
        #[arg(long, value_enum, default_value_t = TierArg::Exploratory)]
        tier: TierArg,
    },
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Verb::Gate { dir, tier } => gate(&dir, tier.into()),
        Verb::Run {
            nodes,
            mix,
            rate,
            clients,
            secs,
            value_bytes,
            keys,
            depth,
            seed,
        } => single(SingleArgs {
            nodes,
            mix,
            rate,
            clients,
            secs,
            value_len: value_bytes,
            keys,
            depth,
            seed,
        }),
        Verb::Campaign {
            mix,
            rates,
            clients,
            secs,
            value_bytes,
            keys,
            depth,
            runs,
            cluster_nodes,
            tick_ms,
            dir,
            server_bin,
            sync,
            out,
            svg,
            root,
            admit,
            tier,
        } => campaign(CampaignArgs {
            mix,
            rates,
            clients,
            secs,
            value_len: value_bytes,
            keys,
            depth,
            runs,
            cluster_nodes,
            tick_ms,
            dir,
            server_bin,
            sync,
            out,
            svg,
            root,
            admit,
            tier: tier.into(),
        }),
        Verb::Failover {
            trials,
            cluster_nodes,
            dir,
            server_bin,
            sync,
            tick_ms,
            out,
            root,
            admit,
            tier,
        } => failover_campaign(FailoverArgs {
            trials,
            cluster_nodes,
            dir,
            server_bin,
            sync,
            tick_ms,
            out,
            root,
            admit,
            tier: tier.into(),
        }),
        Verb::Snapshot {
            state_bytes,
            value_bytes,
            depth,
            runs,
            dir,
            server_bin,
            sync,
            tick_ms,
            out,
            root,
            admit,
            tier,
        } => snapshot_campaign(SnapshotArgs {
            state_bytes,
            value_bytes,
            depth,
            runs,
            dir,
            server_bin,
            sync,
            tick_ms,
            out,
            root,
            admit,
            tier: tier.into(),
        }),
    }
}

fn gate(dir: &str, tier: Tier) -> ExitCode {
    let Some(env) = Environment::probe(dir) else {
        eprintln!("this host could not be probed, so it can publish nothing");
        return ExitCode::FAILURE;
    };
    println!("{}", env.render());
    println!();
    match Publishable::check(&env, tier, 3) {
        Ok(p) => println!("publishable at {} tier:\n{}", tier.name(), p.header()),
        Err(why) => println!("NOT publishable: {why}"),
    }
    ExitCode::SUCCESS
}

// -------------------------------------------------------------- shared parts

fn parse_sync(name: &str) -> Option<SyncMode> {
    match name {
        "durable" => Some(SyncMode::Durable),
        "barrier" => Some(SyncMode::Barrier),
        "none" => Some(SyncMode::None),
        _ => None,
    }
}

/// The gate, applied before anything is measured.
///
/// A campaign that is going to be refused should be refused in the second it
/// starts rather than in the hour it finishes, so this runs before a cluster is
/// even brought up.
fn decide(
    env: &Environment,
    tier: Tier,
    sync: SyncMode,
    runs: usize,
    admit: &Option<String>,
) -> Option<(Result<Publishable, keel_bench::Refusal>, Option<Admitted>)> {
    let verdict = Publishable::check_with_sync(env, tier, sync, runs);
    match (&verdict, admit) {
        (Ok(_), _) => Some((verdict, None)),
        (Err(why), Some(purpose)) => {
            let admitted = Admitted::new(env, why.clone(), purpose);
            Some((verdict, Some(admitted)))
        }
        (Err(why), None) => {
            eprintln!("refused: {why}");
            eprintln!(
                "Nothing was measured. If this run is a control — an ablation, a smoke \
                 test — pass --admit with what it is for, and it will be recorded with \
                 the reason it cannot be published stamped into it."
            );
            None
        }
    }
}

fn record(
    root: &str,
    name: &str,
    verdict: &Result<Publishable, keel_bench::Refusal>,
    admitted: &Option<Admitted>,
    body: &str,
) -> Result<PathBuf, keel_bench::RecordError> {
    match (verdict, admitted) {
        (Ok(p), _) => write_result(root, name, p, body),
        (Err(_), Some(a)) => write_result(root, name, a, body),
        // `decide` returns None in this case and the caller has already exited.
        (Err(_), None) => unreachable!("a refused run without an admission never gets here"),
    }
}

fn median(values: &mut [u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[values.len() / 2]
}

fn ms(ns: u64) -> f64 {
    ns as f64 / 1_000_000.0
}

fn shape_for(rate: u64, clients: usize, depth: usize) -> Loop {
    if rate == 0 {
        Loop::Closed { clients, depth }
    } else {
        Loop::Open {
            rate_per_s: rate,
            clients,
            depth,
        }
    }
}

/// A session nonce nothing else in this process will use.
///
/// Not hygiene — correctness, and getting it wrong cost an afternoon and a
/// wrong conclusion about the system. The same nonce reopens the *same
/// session*, and a fresh `Client` starts its sequence numbers at one. So a
/// client built with a nonce some earlier run used replays sequence numbers
/// below that session's floor, and the state machine refuses every one of them
/// as stale — correctly.
///
/// The benchmark then reports a cluster that fails half its requests, which is
/// a statement about the harness wearing the clothes of a statement about the
/// system. It is [KEEL-9](../../../BUGS.md)'s shape seen from the client side,
/// and the acknowledged-fraction column is what made it visible.
fn fresh_nonce() -> u64 {
    reserve_nonces(1)
}

/// Reserve `count` consecutive nonces, so a pipeline's sessions cannot collide
/// with another sender's.
fn reserve_nonces(count: u64) -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(900_000);
    NEXT.fetch_add(count.max(1), Ordering::SeqCst)
}

/// A pipelined client, as the driver sees it.
///
/// The whole of what depth buys: `submit` puts an operation on the wire and
/// returns, and `poll` collects whatever the cluster has finished. Nothing here
/// waits for a specific answer, which is what lets one sender have sixteen
/// operations outstanding without sixteen threads.
struct Pipelined {
    pipeline: Pipeline,
    value_len: usize,
}

impl workload::Sender for Pipelined {
    fn capacity(&self) -> usize {
        self.pipeline.depth()
    }

    fn inflight(&self) -> usize {
        self.pipeline.inflight()
    }

    fn submit(&mut self, op: Op, seq: u64) -> Option<u64> {
        match op {
            Op::Read { key } => self
                .pipeline
                .submit_query(Query::Get {
                    key: key_bytes(key).into(),
                })
                .ok(),
            Op::Write { key } => self
                .pipeline
                .submit(Command::Put {
                    key: key_bytes(key).into(),
                    value: value_bytes(self.value_len, seq).into(),
                })
                .ok(),
        }
    }

    fn poll(&mut self, timeout: Duration) -> Vec<(u64, bool)> {
        self.pipeline
            .poll(timeout)
            .into_iter()
            .map(|c| (c.id, c.result.is_ok()))
            .collect()
    }
}

/// One run against a real cluster.
fn measure(
    addrs: &[SocketAddr],
    shape: Loop,
    mix: Mix,
    seed: u64,
    keys: u64,
    value_len: usize,
    secs: u64,
) -> workload::Run {
    let addrs = addrs.to_vec();
    // Depth one is the closed client, unchanged: one request on the wire, the
    // thread blocked until it is answered. Everything measured before ADR-033
    // was measured this way, and the etcd baseline still is, so the two remain
    // comparable at that depth.
    if shape.depth() > 1 {
        let depth = shape.depth();
        return workload::run_with(
            shape,
            mix,
            seed,
            keys,
            value_len,
            Duration::from_secs(secs),
            move |_client| {
                // One pipeline per sender thread, with `depth` sessions of its
                // own. Two senders sharing a nonce would share a session, and
                // the second one's writes would be answered out of the first
                // one's exactly-once cache and never applied — KEEL-9 from the
                // client side, reported as a cluster failing half its requests.
                let base = reserve_nonces(depth as u64);
                let pipeline = Pipeline::open(&addrs, base, depth)
                    .unwrap_or_else(|e| panic!("could not open a pipeline of {depth}: {e}"));
                Box::new(Pipelined {
                    pipeline,
                    value_len,
                })
            },
        );
    }
    workload::run(
        shape,
        mix,
        seed,
        keys,
        value_len,
        Duration::from_secs(secs),
        move |_client, op, seq| {
            // One client per thread, with a nonce per thread. Two threads
            // sharing a nonce share a session, and the second one's sequence
            // numbers replay the first one's — so its writes are answered from
            // the exactly-once cache and never applied, and the benchmark
            // measures a cache hit. KEEL-9 is what that looks like when nobody
            // notices.
            thread_local! {
                static CLIENT: std::cell::RefCell<Option<Client>> =
                    const { std::cell::RefCell::new(None) };
            }
            CLIENT.with(|slot| {
                let mut slot = slot.borrow_mut();
                let handle = slot.get_or_insert_with(|| Client::new(&addrs, fresh_nonce()));
                match op {
                    Op::Read { key } => handle.get(&key_bytes(key)).is_ok(),
                    Op::Write { key } => handle
                        .put(&key_bytes(key), &value_bytes(value_len, seq))
                        .is_ok(),
                }
            })
        },
    )
}

fn render_run(run: &workload::Run) -> String {
    let q = |p: f64| ms(run.latency.quantile(p));
    format!(
        "mix {}\nshape {}\nvalue {} B, key space {}\n\
         acknowledged {} of {} attempted in {:.1}s -> {} ops/s\n\
         latency ms  p50 {:.3}  p90 {:.3}  p99 {:.3}  p999 {:.3}  max {:.3}\n\
         late {}{}",
        run.mix.name(),
        run.shape.name(),
        run.value_bytes,
        run.key_space,
        run.acknowledged,
        run.attempted,
        run.duration.as_secs_f64(),
        run.throughput(),
        q(0.5),
        q(0.9),
        q(0.99),
        q(0.999),
        ms(run.latency.max()),
        run.late,
        if run.offered_what_it_claimed() {
            ""
        } else {
            "\n** the load generator could not offer this rate; the throughput above \
             is a statement about the harness **"
        },
    )
}

// ------------------------------------------------------------------- one run

struct SingleArgs {
    nodes: String,
    mix: String,
    rate: u64,
    clients: usize,
    secs: u64,
    value_len: usize,
    keys: u64,
    depth: usize,
    seed: u64,
}

fn single(args: SingleArgs) -> ExitCode {
    let Some(mix) = Mix::parse(&args.mix) else {
        eprintln!("unknown mix {:?}: expected a, b, c or writes", args.mix);
        return ExitCode::FAILURE;
    };
    let addrs = parse_nodes(&args.nodes);
    if addrs.is_empty() {
        eprintln!("no usable node addresses in {:?}", args.nodes);
        return ExitCode::FAILURE;
    }
    let run = measure(
        &addrs,
        shape_for(args.rate, args.clients, args.depth),
        mix,
        args.seed,
        args.keys,
        args.value_len,
        args.secs,
    );
    println!("{}", render_run(&run));
    ExitCode::SUCCESS
}

// ------------------------------------------------------------------ campaign

struct CampaignArgs {
    mix: String,
    rates: String,
    clients: usize,
    secs: u64,
    value_len: usize,
    keys: u64,
    depth: usize,
    runs: usize,
    cluster_nodes: usize,
    tick_ms: u64,
    dir: String,
    server_bin: String,
    sync: String,
    out: String,
    svg: String,
    root: String,
    admit: Option<String>,
    tier: Tier,
}

fn campaign(args: CampaignArgs) -> ExitCode {
    let Some(mix) = Mix::parse(&args.mix) else {
        eprintln!("unknown mix {:?}: expected a, b, c or writes", args.mix);
        return ExitCode::FAILURE;
    };
    let Some(sync) = parse_sync(&args.sync) else {
        eprintln!("unknown sync mode {:?}", args.sync);
        return ExitCode::FAILURE;
    };
    let rates: Vec<u64> = args
        .rates
        .split(',')
        .filter_map(|r| r.trim().parse().ok())
        .collect();
    if rates.len() < 2 {
        eprintln!("a campaign needs at least two rates; a single point is not a curve");
        return ExitCode::FAILURE;
    }
    let server_bin = PathBuf::from(&args.server_bin);
    if !server_bin.exists() {
        eprintln!("{} does not exist", args.server_bin);
        return ExitCode::FAILURE;
    }
    if std::fs::create_dir_all(&args.dir).is_err() {
        eprintln!("could not create {}", args.dir);
        return ExitCode::FAILURE;
    }
    let Some(env) = Environment::probe(&args.dir) else {
        eprintln!("could not probe {}", args.dir);
        return ExitCode::FAILURE;
    };
    let Some((verdict, admitted)) = decide(&env, args.tier, sync, args.runs, &args.admit) else {
        return ExitCode::FAILURE;
    };

    let mut cfg = ClusterConfig::new(args.cluster_nodes, &args.dir, &server_bin);
    cfg.sync = args.sync.clone();
    cfg.tick_ms = args.tick_ms;
    let cluster = match Cluster::start(cfg) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("could not start the cluster: {e}");
            return ExitCode::FAILURE;
        }
    };
    let addrs = cluster.client_addrs.clone();
    println!("{}\n", env.render());

    let mut body = format!(
        "mix          {}\nvalue        {} B\nkey space    {}\nclients      {}\n\
         nodes        {}\ntick         {} ms\nseconds      {} per run\n\
         repetitions  {} per rate, median reported\n\n",
        mix.name(),
        args.value_len,
        args.keys,
        args.clients,
        args.cluster_nodes,
        args.tick_ms,
        args.secs,
        args.runs,
    );
    body.push_str(concat!(
        "offered   achieved      acked       p50       p99      p999       max   late\n",
        "  ops/s      ops/s  /attempt        ms        ms        ms        ms\n",
    ));
    let mut any_late = false;

    let mut points = Vec::new();
    for rate in &rates {
        let shape = shape_for(*rate, args.clients, args.depth);
        // Independent repetitions, and the *median* rather than the best. The
        // best of three is a number about luck.
        let (mut tp, mut p50, mut p99, mut p999, mut top) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let mut late = 0;
        let mut attempted = 0;
        let mut acknowledged = 0;
        for run_index in 0..args.runs.max(1) {
            let run = measure(
                &addrs,
                shape,
                mix,
                1 + run_index as u64,
                args.keys,
                args.value_len,
                args.secs,
            );
            tp.push(run.throughput());
            p50.push(run.latency.quantile(0.5));
            p99.push(run.latency.quantile(0.99));
            p999.push(run.latency.quantile(0.999));
            top.push(run.latency.max());
            late += run.late;
            attempted += run.attempted;
            acknowledged += run.acknowledged;
        }
        let achieved = median(&mut tp);
        let tail = median(&mut p99);
        // A row where the senders could not keep to the schedule is marked. Its
        // "offered" column is then a request rather than a fact, and its
        // achieved throughput says as much about the load generator as about
        // the cluster — which is exactly the row a reader would otherwise quote
        // as the saturation point.
        let behind = late * 20 > attempted.max(1);
        if behind {
            any_late = true;
        }
        // The acknowledged fraction, because a row that achieved half its
        // offered rate looks like saturation and may be nothing of the kind: a
        // cluster refusing half the requests and a cluster serving all of them
        // slowly produce the same "achieved" number, and only this column tells
        // them apart.
        let acked_pct = if attempted == 0 {
            0.0
        } else {
            100.0 * acknowledged as f64 / attempted as f64
        };
        body.push_str(&format!(
            "{:>7}  {:>9}  {:>7.1}%  {:>8.3}  {:>8.3}  {:>8.3}  {:>8.3}  {:>5}{}\n",
            rate,
            achieved,
            acked_pct,
            ms(median(&mut p50)),
            ms(tail),
            ms(median(&mut p999)),
            ms(median(&mut top)),
            late,
            if behind { "  *" } else { "" },
        ));
        println!(
            "  {rate:>7} offered -> {achieved:>8} ops/s, p99 {:.3} ms",
            ms(tail)
        );
        points.push(Point {
            throughput: achieved,
            latency_ns: tail,
        });
    }

    let caption = match &verdict {
        Ok(p) => format!(
            "{} tier — {}, {} — not a claim about how fast Keel is in general",
            p.tier().name(),
            env.cpu,
            env.filesystem.name()
        ),
        Err(why) => format!("NOT PUBLISHABLE — {why}"),
    };
    let series = vec![Series {
        name: format!("{} nodes, {}", args.cluster_nodes, mix.name()),
        points,
    }];
    let drawing = match throughput_vs_latency("Throughput versus p99 latency", &caption, &series) {
        Ok(svg) => svg,
        Err(e) => {
            eprintln!("could not draw the curve: {e}");
            return ExitCode::FAILURE;
        }
    };
    if any_late {
        body.push_str(
            "\n*  the senders could not keep to this rate's schedule. The offered\n             *  column is a request rather than a fact on those rows, and the\n             *  achieved throughput says as much about the load generator as about\n             *  the cluster. They are printed rather than dropped because where a\n             *  harness stops keeping up is itself worth knowing.\n",
        );
    }
    body.push_str(
        "\nPercentiles are the upper edge of the histogram bucket a sample fell in,\n         so they are never optimistic — and so a p99 can read slightly above the\n         max, which is the one raw value in the table.\n",
    );
    body.push_str(&format!("\ncurve: results/bench/{}\n", args.svg));

    // The picture goes through the same door as the numbers, and carries the
    // same header: a picture travels further than the file it came from.
    for (name, contents) in [(&args.out, &body), (&args.svg, &drawing)] {
        match record(&args.root, name, &verdict, &admitted, contents) {
            Ok(path) => println!("wrote {}", path.display()),
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        }
    }
    ExitCode::SUCCESS
}

// ------------------------------------------------------------------ failover

struct FailoverArgs {
    trials: usize,
    cluster_nodes: usize,
    dir: String,
    server_bin: String,
    sync: String,
    tick_ms: u64,
    out: String,
    root: String,
    admit: Option<String>,
    tier: Tier,
}

fn failover_campaign(args: FailoverArgs) -> ExitCode {
    let Some(sync) = parse_sync(&args.sync) else {
        eprintln!("unknown sync mode {:?}", args.sync);
        return ExitCode::FAILURE;
    };
    let server_bin = PathBuf::from(&args.server_bin);
    if !server_bin.exists() {
        eprintln!("{} does not exist", args.server_bin);
        return ExitCode::FAILURE;
    }
    if std::fs::create_dir_all(&args.dir).is_err() {
        eprintln!("could not create {}", args.dir);
        return ExitCode::FAILURE;
    }
    let Some(env) = Environment::probe(&args.dir) else {
        eprintln!("could not probe {}", args.dir);
        return ExitCode::FAILURE;
    };
    // Three, because that is the gate's floor for repetitions, and a hundred
    // trials is far past it.
    let Some((verdict, admitted)) = decide(&env, args.tier, sync, 3, &args.admit) else {
        return ExitCode::FAILURE;
    };

    let mut cfg = ClusterConfig::new(args.cluster_nodes, &args.dir, &server_bin);
    cfg.sync = args.sync.clone();
    cfg.tick_ms = args.tick_ms;
    let mut cluster = match Cluster::start(cfg) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("could not start the cluster: {e}");
            return ExitCode::FAILURE;
        }
    };
    let addrs = cluster.client_addrs.clone();
    let admin = cluster.admin_addrs.clone();
    println!("{}", env.render());
    println!(
        "cluster up: {} nodes, tick {} ms\n",
        args.cluster_nodes, args.tick_ms
    );

    let mut outcomes = Vec::with_capacity(args.trials);
    for i in 0..args.trials {
        let Some(victim) = leader_index(&admin) else {
            // No leader to kill. Recorded as an unusable trial rather than
            // skipped silently, because a run made mostly of these has not
            // measured failover.
            outcomes.push(Trial::NotHealthyBefore);
            std::thread::sleep(Duration::from_millis(200));
            continue;
        };
        let killed = std::cell::Cell::new(false);
        let outcome = keel_bench::failover::trial(
            &addrs,
            2_000_000 + i as u64 * 97,
            Duration::from_secs(10),
            || {
                let ok = cluster.process(victim).and_then(|p| p.kill()).is_ok();
                killed.set(ok);
                ok
            },
        );
        outcomes.push(outcome);
        // Put it back before the next trial, or the cluster shrinks until it
        // cannot elect and every later trial measures that instead.
        if killed.get() && cluster.start_node(victim).is_err() {
            eprintln!("node {victim} did not come back at trial {i}");
        }
        if i % 20 == 19 {
            let so_far = keel_bench::failover::summarise(&outcomes);
            println!(
                "  {} trials, {} recovered, median {:.1} ms",
                so_far.trials,
                so_far.recovered,
                so_far.median().as_secs_f64() * 1000.0
            );
        }
    }

    let report = keel_bench::failover::summarise(&outcomes);
    println!("\n{}", report.render());

    let mut body = format!(
        "nodes        {}\ntick         {} ms\nsync         {}\n\n",
        args.cluster_nodes, args.tick_ms, args.sync
    );
    body.push_str(&report.render());
    body.push_str(
        "\nThe clock starts when the kill signal is sent and stops when a client's\n\
         write is acknowledged — not when a new leader is elected. Election is an\n\
         internal event a client cannot observe, and it is strictly earlier: the\n\
         new leader must also commit its own term's no-op before it can serve.\n",
    );

    match record(&args.root, &args.out, &verdict, &admitted, &body) {
        Ok(path) => {
            println!("wrote {}", path.display());
            if report.has_enough_trials() {
                ExitCode::SUCCESS
            } else {
                // Non-zero, because a report whose percentiles describe the
                // draw rather than the system is not a result.
                eprintln!("only {} usable trials of {}", report.recovered, args.trials);
                ExitCode::FAILURE
            }
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

// ---------------------------------------------------------------- snapshots

struct SnapshotArgs {
    state_bytes: usize,
    value_bytes: usize,
    depth: usize,
    runs: usize,
    dir: String,
    server_bin: String,
    sync: String,
    tick_ms: u64,
    out: String,
    root: String,
    admit: Option<String>,
    tier: Tier,
}

fn snapshot_campaign(args: SnapshotArgs) -> ExitCode {
    let Some(sync) = parse_sync(&args.sync) else {
        eprintln!("unknown sync mode {:?}", args.sync);
        return ExitCode::FAILURE;
    };
    let server_bin = PathBuf::from(&args.server_bin);
    if !server_bin.exists() || std::fs::create_dir_all(&args.dir).is_err() {
        eprintln!("could not prepare the snapshot benchmark");
        return ExitCode::FAILURE;
    }
    let Some(env) = Environment::probe(&args.dir) else {
        eprintln!("could not probe {}", args.dir);
        return ExitCode::FAILURE;
    };
    let Some((verdict, admitted)) = decide(&env, args.tier, sync, args.runs, &args.admit) else {
        return ExitCode::FAILURE;
    };
    let value_len = args.value_bytes.clamp(16, 8 << 20);
    let values = args.state_bytes.div_ceil(value_len).max(1);
    let mut creation_ms = Vec::new();
    let mut transfer_ms = Vec::new();
    let mut snapshot_sizes = Vec::new();

    for run in 0..args.runs.max(1) {
        let run_dir = PathBuf::from(&args.dir).join(format!("snapshot-run-{run}"));
        if run_dir.exists() && std::fs::remove_dir_all(&run_dir).is_err() {
            eprintln!("could not clear {}", run_dir.display());
            return ExitCode::FAILURE;
        }
        let mut cfg = ClusterConfig::new(3, &run_dir, &server_bin);
        cfg.sync = args.sync.clone();
        cfg.tick_ms = args.tick_ms;
        cfg.checkpoint_entries = values as u64 + 10_000;
        let mut cluster = match Cluster::start(cfg) {
            Ok(cluster) => cluster,
            Err(error) => {
                eprintln!("could not start run {run}: {error}");
                return ExitCode::FAILURE;
            }
        };
        let Some(leader) = wait_for_value(Duration::from_secs(30), || {
            leader_index(&cluster.admin_addrs)
        }) else {
            eprintln!("run {run}: no leader");
            return ExitCode::FAILURE;
        };
        let laggard = (0..3).find(|node| *node != leader).unwrap_or(0);
        if cluster
            .process(laggard)
            .and_then(|process| process.kill())
            .is_err()
        {
            eprintln!("run {run}: could not stop lagging follower");
            return ExitCode::FAILURE;
        }
        let addrs: Vec<SocketAddr> = cluster
            .client_addrs
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != laggard)
            .map(|(_, addr)| *addr)
            .collect();
        if fill_state(&addrs, run as u64 + 1, values, value_len, args.depth).is_err() {
            eprintln!("run {run}: the state could not be filled");
            return ExitCode::FAILURE;
        }

        let checkpoint_started = Instant::now();
        let Some(leader) = wait_for_value(Duration::from_secs(30), || {
            let leader = leader_index(&cluster.admin_addrs)?;
            post_admin(cluster.admin_addrs[leader], "/snapshot")
                .ok()
                .map(|()| leader)
        }) else {
            eprintln!("run {run}: no leader accepted the checkpoint");
            return ExitCode::FAILURE;
        };
        let leader_root = run_dir.join(format!("n{leader}/snapshots"));
        let Some(leader_checkpoint) = wait_for_value(Duration::from_secs(600), || {
            published_checkpoint(&leader_root)
        }) else {
            eprintln!("run {run}: checkpoint creation did not finish");
            return ExitCode::FAILURE;
        };
        creation_ms.push(checkpoint_started.elapsed().as_millis() as u64);
        snapshot_sizes.push(directory_size(&leader_checkpoint));

        let transfer_started = Instant::now();
        if cluster.start_node(laggard).is_err() {
            eprintln!("run {run}: the lagging follower did not restart");
            return ExitCode::FAILURE;
        }
        let follower_root = run_dir.join(format!("n{laggard}/snapshots"));
        // LSM checkpoints can be far larger than logical state while compaction
        // is in flight. Keep the benchmark's transfer deadline above that
        // storage case; this is a measurement timeout, not a protocol timer.
        if wait_for_value(Duration::from_secs(3_600), || {
            published_checkpoint(&follower_root)
        })
        .is_none()
        {
            eprintln!("run {run}: snapshot transfer did not finish");
            return ExitCode::FAILURE;
        }
        transfer_ms.push(transfer_started.elapsed().as_millis() as u64);
        println!(
            "run {}: checkpoint {} ms, transfer {} ms, {} bytes",
            run + 1,
            creation_ms[run],
            transfer_ms[run],
            snapshot_sizes[run]
        );
    }

    let create = median(&mut creation_ms);
    let transfer = median(&mut transfer_ms);
    let size = median(&mut snapshot_sizes);
    let mib_per_second = if transfer == 0 {
        0.0
    } else {
        size as f64 / (1024.0 * 1024.0) / (transfer as f64 / 1000.0)
    };
    let body = format!(
        "requested state  {} bytes\nvalue            {} bytes\nvalues           {}\n\
         nodes            3\nsync             {}\nruns             {}, median reported\n\n\
         checkpoint       {} ms\ncheckpoint bytes {}\ntransfer         {} ms\ntransfer rate    {:.2} MiB/s\n\n\
         The transfer clock starts immediately before the compacted follower is\n\
         restarted and stops only after that real process publishes the received\n\
         checkpoint. The receiver uses the same resumable TCP path as production.\n",
        args.state_bytes,
        value_len,
        values,
        args.sync,
        args.runs,
        create,
        size,
        transfer,
        mib_per_second,
    );
    match record(&args.root, &args.out, &verdict, &admitted, &body) {
        Ok(path) => {
            println!("wrote {}", path.display());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn fill_state(
    nodes: &[SocketAddr],
    nonce: u64,
    total: usize,
    value_len: usize,
    depth: usize,
) -> Result<(), String> {
    let mut pipeline = Pipeline::open(nodes, 8_000_000 + nonce * 10_000, depth.max(1))
        .map_err(|error| error.to_string())?;
    let mut submitted = 0usize;
    let mut completed = 0usize;
    let deadline = Instant::now() + Duration::from_secs(1_800);
    while completed < total && Instant::now() < deadline {
        while submitted < total && !pipeline.is_full() {
            let seq = submitted as u64 + 1;
            let command = Command::Put {
                key: format!("snapshot-{submitted:010}").into_bytes().into(),
                value: snapshot_value(value_len, seq).into(),
            };
            pipeline
                .submit(command)
                .map_err(|error| error.to_string())?;
            submitted += 1;
        }
        for completion in pipeline.poll(Duration::from_millis(20)) {
            completion.result.map_err(|error| error.to_string())?;
            completed += 1;
        }
    }
    if completed == total {
        Ok(())
    } else {
        Err(format!("only {completed} of {total} writes completed"))
    }
}

fn snapshot_value(size: usize, seed: u64) -> Vec<u8> {
    let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
    let mut bytes = Vec::with_capacity(size);
    while bytes.len() < size {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let take = (size - bytes.len()).min(8);
        bytes.extend_from_slice(&state.to_le_bytes()[..take]);
    }
    bytes
}

fn post_admin(addr: SocketAddr, path: &str) -> Result<(), String> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))
        .map_err(|error| error.to_string())?;
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: keel\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
    )
    .map_err(|error| error.to_string())?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| error.to_string())?;
    if response.starts_with("HTTP/1.1 202 Accepted") {
        Ok(())
    } else {
        Err(response.lines().next().unwrap_or("no response").into())
    }
}

fn published_checkpoint(root: &std::path::Path) -> Option<PathBuf> {
    std::fs::read_dir(root)
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.is_dir()
                && path.file_name().is_some_and(|name| {
                    name.to_string_lossy()
                        .starts_with(|c: char| c.is_ascii_digit())
                })
        })
}

fn directory_size(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.metadata().ok())
        .filter(|metadata| metadata.is_file())
        .map(|metadata| metadata.len())
        .sum()
}

fn wait_for_value<T>(limit: Duration, mut get: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if let Some(value) = get() {
            return Some(value);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    None
}

/// Ask each node who it thinks it is, and return the index of the one that says
/// leader.
///
/// Asked rather than inferred. A trial that killed a follower would measure
/// nothing and would average in as a very fast recovery, which is the most
/// misleading direction to be wrong in.
fn leader_index(admin: &[SocketAddr]) -> Option<usize> {
    use std::io::{Read, Write};
    for (index, addr) in admin.iter().enumerate() {
        let Ok(mut stream) = std::net::TcpStream::connect_timeout(addr, Duration::from_millis(500))
        else {
            continue;
        };
        let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
        if stream.write_all(b"GET /status HTTP/1.0\r\n\r\n").is_err() {
            continue;
        }
        let mut body = String::new();
        if stream.read_to_string(&mut body).is_err() {
            continue;
        }
        // The field is rendered lower case by `keel-server`'s status writer.
        // Matched with the quotes and the colon so a node whose *leader hint*
        // happens to contain the word cannot be mistaken for the leader itself.
        let compact = body.replace(' ', "");
        if compact.contains("\"role\":\"leader\"") {
            return Some(index);
        }
    }
    None
}
