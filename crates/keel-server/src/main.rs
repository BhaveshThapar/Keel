//! The node daemon.
//!
//! One process, one node, one loop, one thread. It recovers, binds, announces
//! itself with a ready file, and turns until it is killed.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use keel_log::SyncMode;
use keel_server::{NodeConfig, Server};

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
    #[arg(long = "peer", value_parser = parse_peer)]
    peers: Vec<(u64, SocketAddr)>,
    /// How hard to fsync. `durable` is the only mode under which a durability
    /// claim may be made; `barrier` is ordering without power-loss durability,
    /// and `none` is neither.
    #[arg(long, default_value = "durable")]
    sync: String,
    /// Milliseconds per tick of the consensus clock.
    #[arg(long, default_value_t = 10)]
    tick_ms: u64,
}

fn parse_peer(raw: &str) -> Result<(u64, SocketAddr), String> {
    let (id, addr) = raw
        .split_once('=')
        .ok_or_else(|| format!("expected id=host:port, got {raw:?}"))?;
    let id: u64 = id.parse().map_err(|_| format!("bad peer id {id:?}"))?;
    let addr: SocketAddr = addr.parse().map_err(|_| format!("bad address {addr:?}"))?;
    Ok((id, addr))
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
    let peers: BTreeMap<u64, SocketAddr> = cli.peers.iter().copied().collect();
    if !peers.contains_key(&cli.id) {
        eprintln!(
            "node {} is not in its own peer list, so no peer could reach it",
            cli.id
        );
        return ExitCode::FAILURE;
    }

    let cfg = NodeConfig {
        id: cli.id,
        dir: cli.dir,
        peer_addr: cli.listen,
        admin_addr: cli.admin,
        client_addr: cli.client,
        voters: peers.keys().copied().collect(),
        peers,
        sync_mode,
        tick: Duration::from_millis(cli.tick_ms),
    };

    let mut server = match Server::start(cfg) {
        Ok(server) => server,
        Err(e) => {
            eprintln!("node {} could not start: {e}", cli.id);
            return ExitCode::FAILURE;
        }
    };

    loop {
        if let Err(e) = server.turn() {
            // A node that cannot turn cannot serve, and carrying on would mean
            // answering reads from state it can no longer extend.
            eprintln!("node {} failed: {e}", cli.id);
            return ExitCode::FAILURE;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}
