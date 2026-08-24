//! Wiring: one node's worth of consensus, storage, network and admin surface,
//! driven by one loop on one thread.
//!
//! One thread on purpose. The consensus core is a pure function of its inputs,
//! the log owns no thread, the storage engine spawns none under
//! `Maintenance::Manual`, and the admin surface is polled rather than served —
//! so the whole node is a sequence of turns, and a turn is the same sequence of
//! events the simulator drives. That is what makes a bug found in a seed
//! reproducible here, and it is worth more than the throughput a second thread
//! would buy.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use keel_log::{LogOptions, StdFs, StdLog, SyncMode};
use keel_net::TcpTransport;
use keel_node::Node;
use keel_raft::{ConfState, Config, NodeId, Role};
use keel_sm::{LsmStore, StateMachine};

use crate::clients::{Clients, Incoming};
use crate::{Admin, Kind, Metric, Observable, ServerError, Status, write_ready_file};

/// Everything a node needs to be told.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: NodeId,
    pub dir: PathBuf,
    /// Where peers reach this node.
    pub peer_addr: SocketAddr,
    /// Where operators reach this node.
    pub admin_addr: SocketAddr,
    /// Where clients reach this node.
    pub client_addr: SocketAddr,
    /// Every voter, including this node.
    pub voters: Vec<NodeId>,
    /// Where to find each peer.
    pub peers: BTreeMap<NodeId, SocketAddr>,
    pub sync_mode: SyncMode,
    /// How much wall-clock time one tick of the consensus clock represents.
    pub tick: Duration,
}

/// A running node.
pub struct Server {
    node: Node<StdFs, LsmStore, TcpTransport>,
    admin: Admin,
    clients: Clients,
    /// Whether this node was the leader last turn, so a step-down can refuse
    /// everything parked rather than leaving it to time out.
    was_leader: bool,
    cfg: NodeConfig,
    last_tick: Instant,
    /// Set once the ready file has been written, so it is written once.
    announced: bool,
}

impl Server {
    /// Recover, bind, and be ready to turn.
    pub fn start(cfg: NodeConfig) -> Result<Self, ServerError> {
        // Both, not just the parent: the log and the storage engine each open
        // a directory that has to be there already, and a node that failed here
        // reported it as an admin-listener failure until it did.
        let log_dir = cfg.dir.join("log");
        let state_dir = cfg.dir.join("state");
        std::fs::create_dir_all(&log_dir)?;
        std::fs::create_dir_all(&state_dir)?;

        let (log, recovered) = StdLog::open(
            StdFs,
            &log_dir,
            LogOptions {
                sync_mode: cfg.sync_mode,
                ..LogOptions::default()
            },
        )
        .map_err(|e| ServerError::Recovery {
            what: "the log",
            why: e.to_string(),
        })?;

        let store = LsmStore::open_with(
            &state_dir,
            lsm_kv::Options {
                sync_wal: match cfg.sync_mode {
                    SyncMode::Durable => lsm_kv::SyncMode::Durable,
                    SyncMode::Barrier => lsm_kv::SyncMode::Barrier,
                    SyncMode::None => lsm_kv::SyncMode::None,
                },
                ..LsmStore::default_options()
            },
        )
        .map_err(|e| ServerError::Recovery {
            what: "the state machine",
            why: e.to_string(),
        })?;

        let mut transport = TcpTransport::bind(cfg.id, cfg.peer_addr)?;
        for (peer, addr) in &cfg.peers {
            if *peer != cfg.id {
                transport.route(*peer, *addr);
            }
        }

        let node = Node::new(
            Config::new(cfg.id),
            ConfState {
                voters: cfg.voters.clone(),
                ..ConfState::default()
            },
            log,
            recovered,
            StateMachine::new(store),
            transport,
        );
        let admin = Admin::bind(cfg.admin_addr)?;
        let clients = Clients::bind(cfg.client_addr)?;

        Ok(Self {
            node,
            admin,
            clients,
            was_leader: false,
            cfg,
            last_tick: Instant::now(),
            announced: false,
        })
    }

    pub fn admin_addr(&self) -> Result<SocketAddr, ServerError> {
        self.admin.local_addr()
    }

    pub fn client_addr(&self) -> Result<SocketAddr, ServerError> {
        self.clients.local_addr()
    }

    pub fn node(&mut self) -> &mut Node<StdFs, LsmStore, TcpTransport> {
        &mut self.node
    }

    /// Milliseconds since the epoch, for stamping a proposal.
    ///
    /// The only clock in the node, and it is read exactly here: a leader stamps
    /// the entries it proposes and every node reads the stamp back out of the
    /// log. Session expiry is then a function of the log rather than of any
    /// node's opinion about the time.
    pub fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// One turn: tick if the clock says so, drive the loop, answer scrapes.
    pub fn turn(&mut self) -> Result<(), ServerError> {
        if self.last_tick.elapsed() >= self.cfg.tick {
            self.last_tick = Instant::now();
            self.node.tick();
        }
        // Clients first, so anything that arrived joins this turn's `Ready`
        // and shares its fsync. Accepting after the pump would make every
        // request wait a turn for no reason.
        let status = self.node.status();
        let is_leader = status.role == Role::Leader;
        for item in self.clients.poll(is_leader, status.leader)? {
            match item {
                Incoming::Propose { request, .. } | Incoming::Register { request, .. } => {
                    if let Some(proposal) = proposal_of(request, Self::now_ms()) {
                        self.node.propose(proposal);
                    }
                }
                Incoming::Read { ctx } => self.node.read_index(ctx),
            }
        }

        self.node
            .turn()
            .map_err(|e| ServerError::Io(std::io::Error::other(e.to_string())))?;

        for answer in self.node.take_answers() {
            self.clients
                .answer_write(answer.session, answer.registration, &answer.response);
        }
        for (ctx, index) in self.node.take_reads() {
            self.clients.confirm_read(ctx, index);
        }
        {
            let sm = self.node.state_machine();
            self.clients
                .answer_reads(self.node.applied(), |query| resolve(sm, query));
        }
        // A node that has stopped leading cannot answer what it parked while it
        // was. Saying so beats holding the connection until it times out.
        if self.was_leader && !is_leader {
            self.clients.refuse_all(status.leader);
        }
        self.was_leader = is_leader;
        self.clients.expire();

        // The engine spawns no threads here, so its deferred work happens on
        // this turn or not at all. One unit, so a merge cannot stall the timer.
        let _ = self.node.state_machine().store().maintain();

        let reported = self.status();
        self.admin.poll(&StatusOnly(reported.clone()))?;
        let status = reported;

        if !self.announced {
            write_ready_file(&self.cfg.dir.join("keel.ready"), &status)?;
            self.announced = true;
        }
        Ok(())
    }

    /// Turn until `deadline`, sleeping briefly when there is nothing to do.
    ///
    /// The sleep is what keeps an idle node off the CPU. It is short enough
    /// that a tick is never late by more than itself, which matters because a
    /// late tick is an election timeout that fires late.
    pub fn run_until(&mut self, deadline: Instant) -> Result<(), ServerError> {
        while Instant::now() < deadline {
            self.turn()?;
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    }

    pub fn status(&self) -> Status {
        let s = self.node.status();
        let stats = self.node.log().stats();
        Status {
            id: s.id,
            term: s.term,
            role: s.role,
            leader: s.leader,
            commit: s.commit,
            applied: s.applied,
            persisted: s.persisted,
            last_index: s.last_index,
            voters: s.conf.voters.clone(),
            learners: s.conf.learners.clone(),
            voters_outgoing: s.conf.voters_outgoing.clone(),
            sync_mode: stats.sync_mode,
            segments: stats.segments,
            failure: self
                .node
                .state_machine()
                .store()
                .health()
                .err()
                .map(|e| e.to_string()),
        }
    }

    pub fn is_leader(&self) -> bool {
        self.node.status().role == Role::Leader
    }

    pub fn ready_file(dir: &Path) -> PathBuf {
        dir.join("keel.ready")
    }
}

/// Turn a client request into the proposal that goes in the log.
///
/// The leader stamps the time here and nowhere else. Every node then reads that
/// stamp back out of the entry, so session expiry is a function of the log
/// rather than of any node's opinion about the clock (ADR-021).
fn proposal_of(request: keel_api::Request, now_ms: u64) -> Option<keel_api::Proposal> {
    use keel_api::{Proposal, ProposalBody, Request};
    match request {
        Request::Register { nonce } => Some(Proposal {
            stamped_ms: now_ms,
            session: None,
            body: ProposalBody::Register { nonce },
        }),
        Request::Command {
            client,
            seq,
            command,
        } => Some(Proposal {
            stamped_ms: now_ms,
            session: Some((client, seq)),
            body: ProposalBody::Command(command),
        }),
        // Neither of these goes through the log: a query is answered from
        // applied state after a read index, and a keep-alive is answered where
        // it stands.
        Request::Query { .. } | Request::KeepAlive { .. } => None,
    }
}

/// Answer a query from applied state.
///
/// Only ever called once the state machine has applied through the index the
/// read was confirmed at, which is what makes it linearizable rather than
/// simply local.
fn resolve(sm: &StateMachine<LsmStore>, query: &keel_api::Query) -> keel_api::Response {
    use keel_api::{ApiError, Query, Response};
    match query {
        Query::Get { key } => match sm.get(key) {
            Ok(value) => Response::Value(value),
            Err(e) => Response::Error(ApiError::NodeFailed(e.to_string())),
        },
        Query::Scan { start, end, limit } => {
            match sm.scan(
                start.as_ref().map(|s| s.as_ref()),
                end.as_ref().map(|e| e.as_ref()),
                *limit as usize,
            ) {
                Ok(rows) => Response::Scanned(rows),
                Err(e) => Response::Error(ApiError::NodeFailed(e.to_string())),
            }
        }
    }
}

/// The admin surface needs an [`Observable`], and the node is already borrowed
/// mutably by the turn that is answering. A snapshot of the status is enough:
/// a scrape is a point in time by definition.
struct StatusOnly(Status);

impl Observable for StatusOnly {
    fn status(&self) -> Status {
        self.0.clone()
    }

    fn metrics(&self) -> Vec<Metric> {
        let s = &self.0;
        vec![
            Metric {
                name: "keel_term",
                help: "Current Raft term",
                kind: Kind::Gauge,
                value: s.term as f64,
            },
            Metric {
                name: "keel_is_leader",
                help: "1 if this node is the leader",
                kind: Kind::Gauge,
                value: if s.role == Role::Leader { 1.0 } else { 0.0 },
            },
            Metric {
                name: "keel_commit_index",
                help: "Highest committed log index",
                kind: Kind::Gauge,
                value: s.commit as f64,
            },
            Metric {
                name: "keel_applied_index",
                help: "Highest log index applied to the state machine",
                kind: Kind::Gauge,
                value: s.applied as f64,
            },
            Metric {
                name: "keel_persisted_index",
                help: "Highest log index this node has made durable",
                kind: Kind::Gauge,
                value: s.persisted as f64,
            },
            Metric {
                name: "keel_log_segments",
                help: "Segment files in the durable log",
                kind: Kind::Gauge,
                value: s.segments as f64,
            },
            Metric {
                name: "keel_voters",
                help: "Voting members of the current configuration",
                kind: Kind::Gauge,
                value: s.voters.len() as f64,
            },
            Metric {
                name: "keel_sync_durable",
                help: "1 if this node's fsyncs survive power loss, 0 otherwise",
                kind: Kind::Gauge,
                value: if s.sync_mode.is_durable() { 1.0 } else { 0.0 },
            },
            Metric {
                name: "keel_failed",
                help: "1 if this node has latched a fatal storage error",
                kind: Kind::Gauge,
                value: if s.failure.is_some() { 1.0 } else { 0.0 },
            },
        ]
    }
}
