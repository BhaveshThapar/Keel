//! The node daemon.
//!
//! One process, one node, one loop, one thread. It recovers, binds, announces
//! itself with a ready file, and turns until it is killed.

use std::collections::BTreeMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use keel_log::SyncMode;
use keel_server::{Busy, NodeConfig, Server};

#[derive(Parser)]
#[command(
    name = "keel-server",
    about = "One Keel node",
    long_about = "Recovers its log and state machine, joins its peers, and serves. \
                  Writes a ready file once recovery is done — a supervisor that waits \
                  for the port instead learns only that a socket was bound, which \
                  happens before a large log has been replayed."
)]
struct Cli {
    /// This node's id. Must be unique in the cluster and stable across restarts.
    #[arg(long)]
    id: u64,
    /// Where this node's log and state machine live.
    #[arg(long)]
    dir: PathBuf,
    /// Where peers reach this node.
    #[arg(long)]
    listen: SocketAddr,
    /// Where operators reach this node: /status and /metrics.
    #[arg(long)]
    admin: SocketAddr,
    /// Where clients reach this node.
    #[arg(long)]
    client: SocketAddr,
    /// A peer, as `id=host:port`. Repeat for each, including this node.
    ///
    /// `host` may be a name. It is resolved at startup rather than parsed as a
    /// literal address, because every real deployment names its peers — a
    /// Compose service, a StatefulSet pod, a DNS record — and a flag that only
    /// accepted dotted quads would make this daemon unusable in all three.
    #[arg(long = "peer", value_parser = parse_peer)]
    peers: Vec<(u64, String)>,
    /// Initial voter id. Repeat to start routed peers as learners/non-voters.
    /// When omitted, every `--peer` is a voter for backward compatibility.
    #[arg(long = "voter")]
    voters: Vec<u64>,
    /// How hard to fsync. `durable` is the only mode under which a durability
    /// claim may be made; `barrier` is ordering without power-loss durability,
    /// and `none` is neither.
    #[arg(long, default_value = "durable")]
    sync: String,
    /// Milliseconds per tick of the consensus clock.
    #[arg(long, default_value_t = 10)]
    tick_ms: u64,
    /// Applied entries between checkpoints. Lower values are useful for tests;
    /// production defaults to the policy in `keel-node`.
    #[arg(long, default_value_t = keel_node::ENTRIES_BETWEEN_CHECKPOINTS)]
    checkpoint_entries: u64,
}

fn parse_peer(raw: &str) -> Result<(u64, String), String> {
    let (id, addr) = raw
        .split_once('=')
        .ok_or_else(|| format!("expected id=host:port, got {raw:?}"))?;
    let id: u64 = id.parse().map_err(|_| format!("bad peer id {id:?}"))?;
    if !addr.contains(':') {
        return Err(format!("expected host:port, got {addr:?}"));
    }
    Ok((id, addr.to_string()))
}

/// How long to keep trying to resolve a peer's name.
///
/// Not zero, and not forever. Every node in a cluster is usually started at
/// once, so the first one up will find that its peers' names do not resolve yet
/// — a container that has not started has no DNS record. Failing immediately
/// would make a three-node cluster a race that the first node loses; waiting
/// forever would turn a genuine typo into a process that hangs and says nothing.
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(30);

/// Resolve every peer, waiting for names that are not up yet.
fn resolve_peers(raw: &[(u64, String)]) -> Result<BTreeMap<u64, SocketAddr>, String> {
    let deadline = std::time::Instant::now() + RESOLVE_TIMEOUT;
    let mut resolved = BTreeMap::new();
    for (id, name) in raw {
        loop {
            match name.to_socket_addrs().ok().and_then(|mut a| a.next()) {
                Some(addr) => {
                    resolved.insert(*id, addr);
                    break;
                }
                None if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(500));
                }
                None => {
                    return Err(format!(
                        "could not resolve peer {id} at {name:?} within {}s",
                        RESOLVE_TIMEOUT.as_secs()
                    ));
                }
            }
        }
    }
    Ok(resolved)
}

fn parse_sync(raw: &str) -> Option<SyncMode> {
    match raw {
        "durable" => Some(SyncMode::Durable),
        "barrier" => Some(SyncMode::Barrier),
        "none" => Some(SyncMode::None),
        _ => None,
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let Some(sync_mode) = parse_sync(&cli.sync) else {
        eprintln!(
            "unknown sync mode {:?}: expected durable, barrier or none",
            cli.sync
        );
        return ExitCode::FAILURE;
    };
    if cli.peers.is_empty() {
        eprintln!("a cluster needs at least one --peer, including this node");
        return ExitCode::FAILURE;
    }
    let peers: BTreeMap<u64, SocketAddr> = match resolve_peers(&cli.peers) {
        Ok(peers) => peers,
        Err(why) => {
            eprintln!("{why}");
            return ExitCode::FAILURE;
        }
    };
    if !peers.contains_key(&cli.id) {
        eprintln!(
            "node {} is not in its own peer list, so no peer could reach it",
            cli.id
        );
        return ExitCode::FAILURE;
    }
    let voters = if cli.voters.is_empty() {
        peers.keys().copied().collect()
    } else {
        if let Some(unknown) = cli.voters.iter().find(|id| !peers.contains_key(id)) {
            eprintln!("initial voter {unknown} has no --peer route");
            return ExitCode::FAILURE;
        }
        cli.voters.clone()
    };

    let cfg = NodeConfig {
        id: cli.id,
        dir: cli.dir,
        peer_addr: cli.listen,
        admin_addr: cli.admin,
        client_addr: cli.client,
        voters,
        peers,
        sync_mode,
        tick: Duration::from_millis(cli.tick_ms),
        checkpoint_entries: cli.checkpoint_entries.max(1),
    };

    let mut server = match Server::start(cfg) {
        Ok(server) => server,
        Err(e) => {
            eprintln!("node {} could not start: {e}", cli.id);
            return ExitCode::FAILURE;
        }
    };

    loop {
        match server.turn() {
            // Nothing to do, so stay off the CPU until there might be.
            Ok(Busy::No) => std::thread::sleep(Server::IDLE_PAUSE),
            // Something happened, so come straight back: this is the only
            // thread that proposes, replicates, applies and answers, and a
            // pause here is a pause in all four.
            Ok(Busy::Yes) => {}
            Err(e) => {
                // A node that cannot turn cannot serve, and carrying on would
                // mean answering reads from state it can no longer extend.
                eprintln!("node {} failed: {e}", cli.id);
                return ExitCode::FAILURE;
            }
        }
    }
}
