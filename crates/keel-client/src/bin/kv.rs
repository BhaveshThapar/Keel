//! `kv` — the command-line client.
//!
//! Read and write a Keel cluster from a shell. Every subcommand goes through
//! the same client the library exposes, so a redirect, a retry and a session
//! behave here exactly as they do in a program.

use std::net::SocketAddr;
use std::process::ExitCode;

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
}

fn main() -> ExitCode {
    let cli = Cli::parse();
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
