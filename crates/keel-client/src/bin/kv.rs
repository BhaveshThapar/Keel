//! `kv` — the command-line client.
//!
//! Read and write a Keel cluster from a shell. Every subcommand goes through
//! the same client the library exposes, so a redirect, a retry and a session
//! behave here exactly as they do in a program.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use keel_client::Client;

#[derive(Parser)]
#[command(
    name = "kv",
    about = "Read and write a Keel cluster",
    long_about = "Talks to any node and follows redirects to the leader. A command is \
                  retried under the same sequence number, so a retry after a timeout \
                  applies once rather than twice."
)]
struct Cli {
    /// A node's client address. Repeat for each; any one is enough to start.
    #[arg(long = "node", required = true)]
    nodes: Vec<SocketAddr>,
    /// This client's registration nonce. The same nonce reopens the same
    /// session, so a script that is re-run does not leak identities.
    #[arg(long, default_value_t = 1)]
    nonce: u64,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    /// Compare and swap. Omit --expect to mean "only if absent", and --value to
    /// mean "delete".
    Cas {
        key: String,
        #[arg(long)]
        expect: Option<String>,
        #[arg(long)]
        value: Option<String>,
    },
    /// Add to a counter and print the result.
    Incr {
        key: String,
        #[arg(long, default_value_t = 1)]
        by: i64,
    },
    Scan {
        #[arg(long)]
        start: Option<String>,
        #[arg(long)]
        end: Option<String>,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    /// Open a session and print its id.
    Register,
    /// Run concurrent clients and write the history they observed.
    ///
    /// The output is what an external linearizability checker reads. It is
    /// deliberately not checked here: a checker written by the same people as
    /// the code it checks shares their blind spots, and the whole point of the
    /// file is to be handed to one that does not.
    Workload {
        /// Concurrent clients. One is enough to record a history and not enough
        /// to record an interesting one: linearizability is a property about
        /// *overlapping* operations, and a single client never overlaps itself.
        #[arg(long, default_value_t = 8)]
        clients: usize,
        #[arg(long, default_value_t = 10)]
        secs: u64,
        /// How many distinct keys. A small number is deliberate: the checker
        /// partitions by key, and one key with a hundred operations is a much
        /// harder question than a hundred keys with one each.
        #[arg(long, default_value_t = 4)]
        keys: u64,
        #[arg(long)]
        out: PathBuf,
    },
}

/// One client thread's share of the workload.
///
/// Reads and writes in roughly equal measure. A write-only history is trivially
/// linearizable — there is nothing to contradict — and a read-only one over a
/// key nobody writes is worse.
fn workload_thread(
    nodes: Vec<SocketAddr>,
    id: u64,
    keys: u64,
    origin: Instant,
    deadline: Instant,
) -> Option<keel_client::History> {
    // A nonce per thread, and none of them reused. Two clients sharing a nonce
    // share a session, and the second one's sequence numbers replay the first
    // one's — so its writes are answered from the dedup cache and never applied,
    // and the history records acknowledgements for operations that did not
    // happen.
    let mut client = Client::new(&nodes, 1_000 + id).recording_since(origin);
    let mut n = 0u64;
    while Instant::now() < deadline {
        n += 1;
        let key = format!("k{}", (id.wrapping_mul(7) + n) % keys);
        if n % 2 == 0 {
            // A value nothing else will ever write, so the checker can tell
            // which write a read observed.
            let value = format!("c{id}-{n}");
            let _ = client.put(key.as_bytes(), value.as_bytes());
        } else {
            let _ = client.get(key.as_bytes());
        }
    }
    client.take_history()
}

fn run_workload(
    nodes: &[SocketAddr],
    clients: usize,
    secs: u64,
    keys: u64,
    out: &PathBuf,
) -> Result<String, String> {
    let origin = Instant::now();
    let deadline = origin + Duration::from_secs(secs);
    let handles: Vec<_> = (0..clients as u64)
        .map(|id| {
            let nodes = nodes.to_vec();
            std::thread::spawn(move || workload_thread(nodes, id, keys, origin, deadline))
        })
        .collect();

    let mut merged = keel_client::History::starting_at(origin);
    for handle in handles {
        match handle.join() {
            Ok(Some(history)) => merged.absorb(history),
            // A thread that panicked took its history with it, and a history
            // with a client's operations missing is not a history of this run.
            // Said out loud rather than checked as if it were complete.
            Ok(None) => return Err("a client thread recorded nothing".into()),
            Err(_) => return Err("a client thread panicked".into()),
        }
    }
    if merged.is_empty() {
        return Err("the workload recorded no operations at all".into());
    }
    std::fs::write(out, merged.to_jsonl())
        .map_err(|e| format!("writing {}: {e}", out.display()))?;
    Ok(format!(
        "{} operations from {clients} clients over {keys} keys, {} still pending, written to {}",
        merged.len(),
        merged.pending(),
        out.display()
    ))
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    if let Command::Workload {
        clients,
        secs,
        keys,
        out,
    } = &cli.command
    {
        return match run_workload(&cli.nodes, *clients, *secs, *keys, out) {
            Ok(summary) => {
                println!("{summary}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("{e}");
                ExitCode::FAILURE
            }
        };
    }

    let mut client = Client::new(&cli.nodes, cli.nonce);

    let result = match &cli.command {
        Command::Register => client.register().map(|id| format!("client {id}")),
        Command::Get { key } => client.get(key.as_bytes()).map(|value| match value {
            Some(value) => String::from_utf8_lossy(&value).into_owned(),
            // Distinguishable from an empty value, which is a legitimate thing
            // to have stored.
            None => "(absent)".into(),
        }),
        Command::Put { key, value } => client
            .put(key.as_bytes(), value.as_bytes())
            .map(|()| "ok".into()),
        Command::Delete { key } => client.delete(key.as_bytes()).map(|()| "ok".into()),
        Command::Cas { key, expect, value } => client
            .cas(
                key.as_bytes(),
                expect.as_ref().map(|e| e.as_bytes()),
                value.as_ref().map(|v| v.as_bytes()),
            )
            .map(|outcome| match outcome {
                None => "ok".into(),
                Some(None) => "mismatch: absent".into(),
                Some(Some(actual)) => {
                    format!("mismatch: {}", String::from_utf8_lossy(&actual))
                }
            }),
        Command::Incr { key, by } => client.incr(key.as_bytes(), *by).map(|v| v.to_string()),
        Command::Scan { start, end, limit } => client
            .scan(
                start.as_ref().map(|s| s.as_bytes()),
                end.as_ref().map(|e| e.as_bytes()),
                *limit,
            )
            .map(|rows| {
                rows.iter()
                    .map(|(k, v)| {
                        format!(
                            "{}\t{}",
                            String::from_utf8_lossy(k),
                            String::from_utf8_lossy(v)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            }),
        // Handled above, before a session is opened: it runs its own clients.
        Command::Workload { .. } => unreachable!("workload returns before this match"),
    };

    match result {
        Ok(output) => {
            println!("{output}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
