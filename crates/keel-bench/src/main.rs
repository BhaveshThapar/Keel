//! `keel-bench` — measure a running cluster, and refuse to publish what should
//! not be published.
//!
//! Four subcommands. `gate` says what this host is allowed to produce and why,
//! measuring nothing, so the answer is knowable before an hour is spent on a
//! number that cannot be used. `run` is one measurement at one offered rate.
//! `campaign` sweeps a range of rates and writes the curve — PR-2, because a
//! single throughput figure is a claim about a saturation point whose latency
//! nobody quoted. `failover` kills the leader repeatedly and times the recovery.
//!
//! `campaign` and `failover` start their own cluster, so a measurement is one
//! command rather than a procedure. That matters more than it sounds: a
//! benchmark that needs three terminals is a benchmark that gets run once.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use keel_bench::failover::Trial;
use keel_bench::plot::{Point, Series, throughput_vs_latency};
use keel_bench::workload::{Loop, Mix, Op, key_bytes, parse_nodes, value_bytes};
use keel_bench::{Admitted, Environment, Publishable, Tier, workload, write_result};
use keel_chaos::cluster::{Cluster, ClusterConfig};
use keel_client::Client;
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

#[derive(Subcommand)]
enum Verb {
    /// Say what this host may publish, and measure nothing.
    Gate {
        #[arg(long, default_value = ".")]
        dir: String,
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
        /// Independent repetitions per rate. Three is the floor the gate
        /// enforces; the median of them is what is plotted.
        #[arg(long, default_value_t = 3)]
        runs: usize,
        #[arg(long, default_value_t = 3)]
        cluster_nodes: usize,
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
    },
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Verb::Gate { dir } => gate(&dir),
        Verb::Run {
            nodes,
            mix,
            rate,
            clients,
            secs,
            value_bytes,
            keys,
            seed,
        } => single(SingleArgs {
            nodes,
            mix,
            rate,
            clients,
            secs,
            value_len: value_bytes,
            keys,
            seed,
        }),
        Verb::Campaign {
            mix,
            rates,
            clients,
            secs,
            value_bytes,
            keys,
            runs,
            cluster_nodes,
            dir,
            server_bin,
            sync,
            out,
            svg,
            root,
            admit,
        } => campaign(CampaignArgs {
            mix,
            rates,
            clients,
            secs,
            value_len: value_bytes,
            keys,
            runs,
            cluster_nodes,
            dir,
            server_bin,
            sync,
            out,
            svg,
            root,
            admit,
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
        }),
    }
}

fn gate(dir: &str) -> ExitCode {
    let Some(env) = Environment::probe(dir) else {
        eprintln!("this host could not be probed, so it can publish nothing");
        return ExitCode::FAILURE;
    };
    println!("{}", env.render());
    println!();
    match Publishable::check(&env, Tier::Exploratory, 3) {
        Ok(p) => println!("publishable at Exploratory tier:\n{}", p.header()),
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
    sync: SyncMode,
    runs: usize,
    admit: &Option<String>,
) -> Option<(Result<Publishable, keel_bench::Refusal>, Option<Admitted>)> {
    let verdict = Publishable::check_with_sync(env, Tier::Exploratory, sync, runs);
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

fn shape_for(rate: u64, clients: usize) -> Loop {
    if rate == 0 {
        Loop::Closed { clients }
    } else {
        Loop::Open {
            rate_per_s: rate,
            clients,
        }
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
    workload::run(
        shape,
        mix,
        seed,
        keys,
        value_len,
        Duration::from_secs(secs),
        move |client, op, seq| {
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
                let handle = slot.get_or_insert_with(|| {
                    Client::new(&addrs, 900_000 + client as u64 + seed * 1_000)
                });
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
        shape_for(args.rate, args.clients),
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
    runs: usize,
    cluster_nodes: usize,
    dir: String,
    server_bin: String,
    sync: String,
    out: String,
    svg: String,
    root: String,
    admit: Option<String>,
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
    let Some((verdict, admitted)) = decide(&env, sync, args.runs, &args.admit) else {
        return ExitCode::FAILURE;
    };

    let mut cfg = ClusterConfig::new(args.cluster_nodes, &args.dir, &server_bin);
    cfg.sync = args.sync.clone();
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
         nodes        {}\nseconds      {} per run\nrepetitions  {} per rate, median reported\n\n",
        mix.name(),
        args.value_len,
        args.keys,
        args.clients,
        args.cluster_nodes,
        args.secs,
        args.runs,
    );
    body.push_str(
        "offered    achieved       p50       p99      p999       max   late\n\
         ops/s        ops/s        ms        ms        ms        ms\n",
    );
    let mut any_late = false;

    let mut points = Vec::new();
    for rate in &rates {
        let shape = shape_for(*rate, args.clients);
        // Independent repetitions, and the *median* rather than the best. The
        // best of three is a number about luck.
        let (mut tp, mut p50, mut p99, mut p999, mut top) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let mut late = 0;
        let mut attempted = 0;
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
        body.push_str(&format!(
            "{:>7}  {:>10}  {:>8.3}  {:>8.3}  {:>8.3}  {:>8.3}  {:>5}{}\n",
            rate,
            achieved,
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
    let Some((verdict, admitted)) = decide(&env, sync, 3, &args.admit) else {
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
