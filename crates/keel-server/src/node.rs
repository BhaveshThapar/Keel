//! Wiring: one node's worth of consensus, storage, network and admin surface,
//! driven by one loop on one thread.
//!
//! The consensus core is a pure function of its inputs, the log owns no thread,
//! and the admin surface is polled rather than served. Storage maintenance is
//! the exception: it runs in engine workers so an SSTable merge never blocks a
//! consensus turn. The simulator retains manual maintenance for deterministic
//! replay.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use keel_api::Peer;
use keel_log::{LogOptions, StdFs, StdLog, SyncMode};
use keel_net::TcpTransport;
use keel_node::{Incoming as SnapshotIncoming, Node, Outgoing, SnapshotEvent, checkpoint_is_due};
use keel_raft::{
    ChangeKind, ConfChangeSingle, ConfChangeV2, ConfState, Config, NodeId, Role, SnapshotMeta,
};
use keel_sm::{Accepted, LsmStore, StateMachine};

use crate::clients::{ClientProgress, Clients, Incoming as ClientIncoming};
use crate::{
    Admin, Histogram, Kind, Metric, Observable, Request, ServerError, Status, write_ready_file,
};

/// Whether a turn found anything to do.
///
/// A bool would do and would read as a bool: `if turn()? { sleep() }` says the
/// opposite of what it means. This says it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Busy {
    Yes,
    No,
}

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
    /// Applied entries between storage-engine checkpoints.
    pub checkpoint_entries: u64,
    pub max_inflight_msgs: usize,
    pub max_inflight_bytes: usize,
    pub max_batch_entries: usize,
}

struct Checkpoint {
    meta: SnapshotMeta,
    path: PathBuf,
    digest: u64,
}

struct Transfer {
    ready_number: u64,
    incoming: SnapshotIncoming,
    last_request: Instant,
}

#[derive(Clone, Copy, Default)]
struct SnapshotProgress {
    checkpoints: u64,
    checkpoint_nanos: u64,
    installed: u64,
    bytes_sent: u64,
    bytes_received: u64,
}

/// A receiver may pause while its storage engine flushes a large snapshot
/// chunk. This is a liveness deadline, not the request cadence: the receiver
/// retries every 250 ms when healthy.
const SNAPSHOT_SENDER_IDLE: Duration = Duration::from_secs(300);

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
    checkpoint: Option<Checkpoint>,
    /// Hashing an immutable checkpoint is proportional to state size and does
    /// not belong on the consensus turn that serves writes.
    checkpoint_digest: Option<std::thread::JoinHandle<Result<Checkpoint, String>>>,
    checkpoint_digest_meta: Option<(u64, u64)>,
    transfer: Option<Transfer>,
    snapshot_activity: BTreeMap<NodeId, Instant>,
    force_checkpoint: bool,
    snapshot_progress: SnapshotProgress,
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
                    SyncMode::Durable | SyncMode::Full => lsm_kv::SyncMode::Durable,
                    SyncMode::Barrier => lsm_kv::SyncMode::Barrier,
                    SyncMode::None => lsm_kv::SyncMode::None,
                },
                maintenance: lsm_kv::Maintenance::Threads,
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

        let mut raft_cfg = Config::new(cfg.id);
        raft_cfg.max_inflight_msgs = cfg.max_inflight_msgs.max(1);
        raft_cfg.max_inflight_bytes = cfg.max_inflight_bytes.max(1);
        let mut node = Node::new(
            raft_cfg,
            ConfState {
                voters: cfg.voters.clone(),
                ..ConfState::default()
            },
            log,
            recovered,
            StateMachine::new(store),
            transport,
        );
        node.set_max_proposals_per_turn(cfg.max_batch_entries);
        let admin = Admin::bind(cfg.admin_addr)?;
        let clients = Clients::bind(cfg.client_addr)?;

        let checkpoint = Self::recover_checkpoint(&cfg, &node)?;
        let mut server = Self {
            node,
            admin,
            clients,
            was_leader: false,
            cfg,
            last_tick: Instant::now(),
            announced: false,
            checkpoint,
            checkpoint_digest: None,
            checkpoint_digest_meta: None,
            transfer: None,
            snapshot_activity: BTreeMap::new(),
            force_checkpoint: false,
            snapshot_progress: SnapshotProgress::default(),
        };
        server.resume_incoming_offer()?;
        Ok(server)
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
    ///
    /// Reports whether the turn did anything, because the caller's decision to
    /// sleep depends on it. A node with a backlog that sleeps between turns is
    /// not idling politely, it is rationing itself: the loop is the only thread
    /// that proposes, replicates, applies and answers, so a millisecond of
    /// sleep is a millisecond none of that happens in. Under load that put a
    /// hard ceiling of about one thousand operations a second on this daemon
    /// whatever the disk or the network could do, and the ceiling did not move
    /// when clients were given more requests to have in flight — which is how
    /// it was found (ADR-034).
    pub fn turn(&mut self) -> Result<Busy, ServerError> {
        if self.last_tick.elapsed() >= self.cfg.tick {
            self.last_tick = Instant::now();
            self.node.tick();
        }
        // Clients first, so anything that arrived joins this turn's `Ready`
        // and shares its fsync. Accepting after the pump would make every
        // request wait a turn for no reason.
        let status = self.node.status();
        let is_leader = status.role == Role::Leader;
        let mut busy = false;
        let incoming = self.clients.poll(is_leader, status.leader)?;
        busy |= !incoming.is_empty();
        for item in incoming {
            match item {
                ClientIncoming::Propose { request, .. }
                | ClientIncoming::Register { request, .. } => {
                    if let Some(proposal) = proposal_of(request, Self::now_ms()) {
                        self.node.propose(proposal);
                    }
                }
                ClientIncoming::Read { ctx } => self.node.read_index(ctx),
            }
        }

        let turn = self
            .node
            .turn()
            .map_err(|e| ServerError::Io(std::io::Error::other(e.to_string())))?;
        busy |= turn.did_something();

        busy |= self.handle_snapshot_events()?;

        let answers = self.node.take_answers();
        busy |= !answers.is_empty();
        for answer in answers {
            self.clients
                .answer_write(answer.session, answer.registration, &answer.response);
        }
        let reads = self.node.take_reads();
        busy |= !reads.is_empty();
        for (ctx, index) in reads {
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

        busy |= self.maybe_checkpoint()?;
        busy |= self.retry_snapshot_request()?;
        busy |= self.expire_snapshot_senders();

        let reported = self.status();
        let core_report = self.node.status();
        let (_, commands) = self.admin.poll_with_commands(&Reported(
            reported.clone(),
            self.node.progress(),
            self.snapshot_progress,
            self.clients.progress(),
            core_report,
        ))?;
        busy |= !commands.is_empty();
        for command in commands {
            self.execute_admin(command);
        }
        let status = reported;

        if !self.announced {
            write_ready_file(&self.cfg.dir.join("keel.ready"), &status)?;
            self.announced = true;
        }
        Ok(if busy { Busy::Yes } else { Busy::No })
    }

    /// How long an idle node waits before turning again.
    ///
    /// Short enough that a tick is never late by more than itself, which
    /// matters because a late tick is an election timeout that fires late.
    pub const IDLE_PAUSE: Duration = Duration::from_millis(1);

    /// Turn until `deadline`, pausing only when there is nothing to do.
    pub fn run_until(&mut self, deadline: Instant) -> Result<(), ServerError> {
        while Instant::now() < deadline {
            if self.turn()? == Busy::No {
                std::thread::sleep(Self::IDLE_PAUSE);
            }
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

    fn snapshot_root(cfg: &NodeConfig) -> PathBuf {
        cfg.dir.join("snapshots")
    }

    fn checkpoint_path(cfg: &NodeConfig, meta: &SnapshotMeta) -> PathBuf {
        Self::snapshot_root(cfg).join(format!("{}-{}", meta.index, meta.term))
    }

    fn staging_path(cfg: &NodeConfig, meta: &SnapshotMeta) -> PathBuf {
        Self::snapshot_root(cfg).join(format!("incoming-{}-{}", meta.index, meta.term))
    }

    fn transfer_manifest(cfg: &NodeConfig) -> PathBuf {
        Self::snapshot_root(cfg).join("incoming.meta")
    }

    fn persist_incoming_offer(&self, from: NodeId, meta: &SnapshotMeta) -> Result<(), ServerError> {
        let root = Self::snapshot_root(&self.cfg);
        std::fs::create_dir_all(&root)?;
        let path = Self::transfer_manifest(&self.cfg);
        let pending = path.with_extension("pending");
        let mut bytes = from.to_le_bytes().to_vec();
        bytes.extend(
            keel_api::encode(meta)
                .map_err(|error| ServerError::Io(std::io::Error::other(error.to_string())))?,
        );
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&pending)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        std::fs::rename(pending, path)?;
        std::fs::File::open(root)?.sync_all()?;
        Ok(())
    }

    fn resume_incoming_offer(&mut self) -> Result<(), ServerError> {
        let path = Self::transfer_manifest(&self.cfg);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if bytes.len() < 8 {
            return Err(ServerError::Recovery {
                what: "an incoming snapshot manifest",
                why: "the file is shorter than its sender id".into(),
            });
        }
        let mut sender = [0u8; 8];
        sender.copy_from_slice(&bytes[..8]);
        let from = NodeId::from_le_bytes(sender);
        let meta = keel_api::decode::<SnapshotMeta>(&bytes[8..]).map_err(|error| {
            ServerError::Recovery {
                what: "an incoming snapshot manifest",
                why: error.to_string(),
            }
        })?;
        if Self::staging_path(&self.cfg, &meta).exists() {
            self.node.resume_snapshot_offer(from, meta);
        } else {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    fn recover_checkpoint(
        cfg: &NodeConfig,
        node: &Node<StdFs, LsmStore, TcpTransport>,
    ) -> Result<Option<Checkpoint>, ServerError> {
        let Some(meta) = node.log().snapshot().cloned() else {
            return Ok(None);
        };
        if node.applied() < meta.index {
            return Ok(None);
        }
        let path = Self::checkpoint_path(cfg, &meta);
        if !path.exists() && node.applied() == meta.index {
            std::fs::create_dir_all(Self::snapshot_root(cfg))?;
            node.state_machine()
                .store()
                .checkpoint(&path)
                .map_err(sm_error)?;
        }
        if !path.exists() {
            return Ok(None);
        }
        let digest = digest_at(&path).map_err(sm_error)?;
        Ok(Some(Checkpoint { meta, path, digest }))
    }

    fn handle_snapshot_events(&mut self) -> Result<bool, ServerError> {
        let events = self.node.take_snapshot_events();
        let busy = !events.is_empty();
        for event in events {
            match event {
                SnapshotEvent::InstallOffered {
                    ready_number,
                    from,
                    meta,
                } => {
                    self.persist_incoming_offer(from, &meta)?;
                    let staging = Self::staging_path(&self.cfg, &meta);
                    let incoming = if staging.exists() {
                        SnapshotIncoming::resume(from, meta, &staging)
                    } else {
                        SnapshotIncoming::new(from, meta, &staging)
                    }
                    .map_err(sm_error)?;
                    self.transfer = Some(Transfer {
                        ready_number,
                        incoming,
                        last_request: Instant::now(),
                    });
                    self.send_snapshot_request()?;
                }
                SnapshotEvent::Request {
                    from,
                    index,
                    term,
                    position,
                } => self.serve_snapshot_request(from, index, term, position)?,
                SnapshotEvent::Chunk {
                    from,
                    index,
                    term,
                    chunk,
                } => {
                    let Some(transfer) = self.transfer.as_mut() else {
                        continue;
                    };
                    if transfer.incoming.from != from
                        || transfer.incoming.meta.index != index
                        || transfer.incoming.meta.term != term
                    {
                        continue;
                    }
                    let bytes = chunk.bytes.len() as u64;
                    let accepted = transfer.incoming.accept(&chunk).map_err(sm_error)?;
                    if matches!(accepted, Accepted::Written | Accepted::Complete) {
                        self.snapshot_progress.bytes_received += bytes;
                    }
                    self.send_snapshot_request()?;
                }
                SnapshotEvent::Complete {
                    from,
                    index,
                    term,
                    digest,
                } => self.finish_snapshot(from, index, term, digest)?,
                SnapshotEvent::Status {
                    from,
                    index,
                    term,
                    ok,
                } => {
                    if self.checkpoint.as_ref().is_some_and(|checkpoint| {
                        checkpoint.meta.index == index && checkpoint.meta.term == term
                    }) {
                        self.node.report_snapshot(from, ok);
                        self.snapshot_activity.remove(&from);
                    }
                }
            }
        }
        Ok(busy)
    }

    fn send_snapshot_request(&mut self) -> Result<(), ServerError> {
        let Some(transfer) = self.transfer.as_mut() else {
            return Ok(());
        };
        let position = transfer.incoming.position().into_iter().collect();
        let peer = transfer.incoming.from;
        let meta = &transfer.incoming.meta;
        self.node
            .send_peer(
                peer,
                Peer::SnapshotRequest {
                    index: meta.index,
                    term: meta.term,
                    position,
                },
            )
            .map_err(node_error)?;
        transfer.last_request = Instant::now();
        Ok(())
    }

    fn retry_snapshot_request(&mut self) -> Result<bool, ServerError> {
        let due = self
            .transfer
            .as_ref()
            .is_some_and(|transfer| transfer.last_request.elapsed() >= Duration::from_millis(250));
        if due {
            self.send_snapshot_request()?;
        }
        Ok(due)
    }

    fn serve_snapshot_request(
        &mut self,
        peer: NodeId,
        index: u64,
        term: u64,
        position: BTreeMap<String, u64>,
    ) -> Result<(), ServerError> {
        self.snapshot_activity.insert(peer, Instant::now());
        let Some(checkpoint) = self
            .checkpoint
            .as_ref()
            .filter(|checkpoint| checkpoint.meta.index == index && checkpoint.meta.term == term)
        else {
            // The checkpoint is durable and the core has compacted to it, but
            // its whole-state verification digest may still be scanning. Keep
            // the follower in snapshot state; its 250 ms retry will be served
            // once the digest is ready.
            if self.checkpoint_digest_meta == Some((index, term)) {
                return Ok(());
            }
            self.node.report_snapshot(peer, false);
            return Ok(());
        };
        let mut outgoing =
            Outgoing::new(peer, checkpoint.meta.clone(), &checkpoint.path).map_err(sm_error)?;
        outgoing.resume_from(position);
        match outgoing.next_chunk().map_err(sm_error)? {
            Some(chunk) => {
                self.snapshot_progress.bytes_sent += chunk.bytes.len() as u64;
                self.node
                    .send_peer(
                        peer,
                        Peer::SnapshotChunk {
                            index,
                            term,
                            file: chunk.file,
                            offset: chunk.offset,
                            crc: chunk.crc,
                            last: chunk.last,
                            data: Bytes::from(chunk.bytes),
                        },
                    )
                    .map_err(node_error)
            }
            None => self
                .node
                .send_peer(
                    peer,
                    Peer::SnapshotComplete {
                        index,
                        term,
                        digest: checkpoint.digest,
                    },
                )
                .map_err(node_error),
        }
    }

    fn expire_snapshot_senders(&mut self) -> bool {
        let expired: Vec<NodeId> = self
            .snapshot_activity
            .iter()
            .filter(|(_, seen)| seen.elapsed() >= SNAPSHOT_SENDER_IDLE)
            .map(|(peer, _)| *peer)
            .collect();
        for peer in &expired {
            self.snapshot_activity.remove(peer);
            self.node.report_snapshot(*peer, false);
        }
        !expired.is_empty()
    }

    fn finish_snapshot(
        &mut self,
        from: NodeId,
        index: u64,
        term: u64,
        digest: u64,
    ) -> Result<(), ServerError> {
        let Some(mut transfer) = self.transfer.take() else {
            return Ok(());
        };
        if transfer.incoming.from != from
            || transfer.incoming.meta.index != index
            || transfer.incoming.meta.term != term
        {
            self.transfer = Some(transfer);
            return Ok(());
        }
        transfer.incoming.finish();
        let meta = transfer.incoming.meta.clone();

        // KEEL-12's ordering: floor durable first, state adoption second.
        self.node
            .persist_snapshot_floor(&meta)
            .map_err(node_error)?;
        self.node
            .state_machine_mut()
            .store_mut()
            .replace_from_checkpoint(|destination| {
                transfer.incoming.publish(destination, digest, digest_at)
            })
            .map_err(sm_error)?;
        if self.node.applied() != meta.index {
            return Err(ServerError::Recovery {
                what: "an installed snapshot",
                why: format!(
                    "checkpoint says applied={} but the offer says {}",
                    self.node.applied(),
                    meta.index
                ),
            });
        }
        let path = self.write_checkpoint(&meta)?;
        let checkpoint = Checkpoint {
            meta: meta.clone(),
            path,
            digest,
        };
        self.node
            .snapshot_installed(transfer.ready_number, meta.clone());
        self.snapshot_progress.installed += 1;
        self.node
            .send_peer(
                from,
                Peer::SnapshotStatus {
                    index,
                    term,
                    ok: true,
                },
            )
            .map_err(node_error)?;
        self.checkpoint = Some(checkpoint);
        let manifest = Self::transfer_manifest(&self.cfg);
        if manifest.exists() {
            std::fs::remove_file(manifest)?;
        }
        Ok(())
    }

    fn maybe_checkpoint(&mut self) -> Result<bool, ServerError> {
        let finished_digest = self.finish_checkpoint_digest()?;
        if self.checkpoint_digest.is_some() {
            return Ok(finished_digest);
        }
        if self.node.role() != Role::Leader {
            return Ok(finished_digest);
        }
        let last = self.checkpoint.as_ref().map_or_else(
            || self.node.log().snapshot().map_or(0, |meta| meta.index),
            |c| c.meta.index,
        );
        let applied = self.node.applied();
        let due = if self.cfg.checkpoint_entries == keel_node::ENTRIES_BETWEEN_CHECKPOINTS {
            checkpoint_is_due(applied, last)
        } else {
            applied.saturating_sub(last) >= self.cfg.checkpoint_entries.max(1)
        };
        if !due && !self.force_checkpoint {
            return Ok(finished_digest);
        }
        self.force_checkpoint = false;
        let Some(meta) = self.node.checkpoint_meta() else {
            return Ok(false);
        };
        let checkpoint_index = meta.index;
        let checkpoint_term = meta.term;
        let checkpoint_conf = meta.conf.clone();
        let snapshot = self
            .node
            .state_machine()
            .store()
            .snapshot()
            .map_err(sm_error)?;
        let root = Self::snapshot_root(&self.cfg);
        let path = Self::checkpoint_path(&self.cfg, &meta);
        self.checkpoint_digest = Some(std::thread::spawn(move || {
            std::fs::create_dir_all(&root).map_err(|e| e.to_string())?;
            let pending = path.with_extension("pending");
            if pending.exists() {
                std::fs::remove_dir_all(&pending).map_err(|e| e.to_string())?;
            }
            if path.exists() {
                std::fs::remove_dir_all(&path).map_err(|e| e.to_string())?;
            }
            snapshot
                .materialize_to(&pending)
                .map_err(|e| e.to_string())?;
            std::fs::rename(&pending, &path).map_err(|e| e.to_string())?;
            std::fs::File::open(&root)
                .and_then(|f| f.sync_all())
                .map_err(|e| e.to_string())?;
            let digest = digest_at(&path).map_err(|error| error.to_string())?;
            Ok(Checkpoint {
                meta: SnapshotMeta {
                    index: checkpoint_index,
                    term: checkpoint_term,
                    conf: checkpoint_conf,
                },
                path,
                digest,
            })
        }));
        self.checkpoint_digest_meta = Some((checkpoint_index, checkpoint_term));
        Ok(true)
    }

    fn finish_checkpoint_digest(&mut self) -> Result<bool, ServerError> {
        if !self
            .checkpoint_digest
            .as_ref()
            .is_some_and(|worker| worker.is_finished())
        {
            return Ok(false);
        }
        let Some(worker) = self.checkpoint_digest.take() else {
            return Ok(false);
        };
        self.checkpoint_digest_meta = None;
        let checkpoint = worker
            .join()
            .map_err(|_| ServerError::Io(std::io::Error::other("checkpoint digest panicked")))?
            .map_err(|why| ServerError::Io(std::io::Error::other(why)))?;
        tracing::info!(
            node_id = self.cfg.id,
            index = checkpoint.meta.index,
            "checkpoint digest ready for transfer"
        );
        self.node
            .checkpoint_taken(checkpoint.meta.clone())
            .map_err(node_error)?;
        self.snapshot_progress.checkpoints += 1;
        self.checkpoint = Some(checkpoint);
        self.remove_old_checkpoints()?;
        Ok(true)
    }

    fn write_checkpoint(&self, meta: &SnapshotMeta) -> Result<PathBuf, ServerError> {
        let root = Self::snapshot_root(&self.cfg);
        std::fs::create_dir_all(&root)?;
        let path = Self::checkpoint_path(&self.cfg, meta);
        let pending = path.with_extension("pending");
        if pending.exists() {
            std::fs::remove_dir_all(&pending)?;
        }
        if path.exists() {
            std::fs::remove_dir_all(&path)?;
        }
        self.node
            .state_machine()
            .store()
            .checkpoint(&pending)
            .map_err(sm_error)?;
        std::fs::rename(&pending, &path)?;
        std::fs::File::open(&root)?.sync_all()?;
        Ok(path)
    }

    fn remove_old_checkpoints(&self) -> Result<(), ServerError> {
        let Some(current) = &self.checkpoint else {
            return Ok(());
        };
        for entry in std::fs::read_dir(Self::snapshot_root(&self.cfg))? {
            let path = entry?.path();
            if path != current.path
                && path.is_dir()
                && !path
                    .file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with("incoming-"))
            {
                std::fs::remove_dir_all(path)?;
            }
        }
        Ok(())
    }

    fn execute_admin(&mut self, request: Request) {
        let change = |kind, node| ConfChangeV2 {
            changes: vec![ConfChangeSingle { kind, node }],
        };
        match request {
            Request::TransferLeader { to } => {
                tracing::info!(
                    node_id = self.cfg.id,
                    target = to,
                    "leader transfer requested"
                );
                self.node.transfer_leader(to);
            }
            Request::AddLearner { node } if self.cfg.peers.contains_key(&node) => {
                tracing::info!(
                    node_id = self.cfg.id,
                    member = node,
                    "learner addition requested"
                );
                let _ = self
                    .node
                    .propose_conf_change(change(ChangeKind::AddLearner, node));
            }
            Request::Promote { node }
                if self.cfg.peers.contains_key(&node) && self.node.learner_caught_up(node) =>
            {
                tracing::info!(
                    node_id = self.cfg.id,
                    member = node,
                    "learner promotion requested"
                );
                let _ = self
                    .node
                    .propose_conf_change(change(ChangeKind::AddVoter, node));
            }
            Request::Remove { node } => {
                tracing::info!(
                    node_id = self.cfg.id,
                    member = node,
                    "member removal requested"
                );
                let _ = self
                    .node
                    .propose_conf_change(change(ChangeKind::RemoveNode, node));
            }
            Request::Snapshot => {
                tracing::info!(node_id = self.cfg.id, "manual checkpoint requested");
                self.force_checkpoint = true;
            }
            Request::Status
            | Request::Metrics
            | Request::Unknown
            | Request::AddLearner { .. }
            | Request::Promote { .. } => {}
        }
    }

    pub fn ready_file(dir: &Path) -> PathBuf {
        dir.join("keel.ready")
    }
}

fn sm_error(error: keel_sm::StateMachineError) -> ServerError {
    ServerError::Io(std::io::Error::other(error.to_string()))
}

fn node_error(error: keel_node::NodeError) -> ServerError {
    ServerError::Io(std::io::Error::other(error.to_string()))
}

fn digest_at(path: &Path) -> Result<u64, keel_sm::StateMachineError> {
    StateMachine::new(LsmStore::open(path)?).state_digest()
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
/// mutably by the turn that is answering. A snapshot of the status and the
/// counters is enough: a scrape is a point in time by definition.
struct Reported(
    Status,
    keel_node::Progress,
    SnapshotProgress,
    ClientProgress,
    keel_raft::Status,
);

impl Observable for Reported {
    fn status(&self) -> Status {
        self.0.clone()
    }

    fn metrics(&self) -> Vec<Metric> {
        let s = &self.0;
        let p = &self.1;
        let snapshots = &self.2;
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
            // The loop's own counters. Entries divided by readies is the batch
            // size, and the batch size is what says whether group commit is
            // doing anything: a node appending one entry per `Ready` is paying
            // a whole round of persist, replicate and apply per operation,
            // whatever it was told about batching.
            Metric {
                name: "keel_turns_total",
                help: "Turns of the node loop",
                kind: Kind::Counter,
                value: p.turns as f64,
            },
            Metric {
                name: "keel_readies_total",
                help: "Ready batches handled",
                kind: Kind::Counter,
                value: p.readies as f64,
            },
            Metric {
                name: "keel_entries_appended_total",
                help: "Log entries written by this node",
                kind: Kind::Counter,
                value: p.entries_appended as f64,
            },
            Metric {
                name: "keel_entries_applied_total",
                help: "Log entries applied to the state machine",
                kind: Kind::Counter,
                value: p.entries_applied as f64,
            },
            Metric {
                name: "keel_messages_sent_total",
                help: "Consensus messages sent to peers",
                kind: Kind::Counter,
                value: p.messages_sent as f64,
            },
            Metric {
                name: "keel_messages_received_total",
                help: "Consensus messages received from peers",
                kind: Kind::Counter,
                value: p.messages_received as f64,
            },
            Metric {
                name: "keel_proposals_dropped_total",
                help: "Proposals refused before they reached the log",
                kind: Kind::Counter,
                value: p.proposals_dropped as f64,
            },
            Metric {
                name: "keel_elections_total",
                help: "Real elections started by this node",
                kind: Kind::Counter,
                value: self.4.elections as f64,
            },
            // Where the time in a round goes. Divided by `keel_readies_total`
            // these are the mean cost of one round's persist, send and apply;
            // divided by `keel_entries_applied_total` they are what one
            // operation's share of each came to, which is the number the batch
            // is there to shrink.
            Metric {
                name: "keel_persist_seconds_total",
                help: "Time spent truncating, appending, writing hard state and fsyncing",
                kind: Kind::Counter,
                value: p.persist_nanos as f64 / 1e9,
            },
            Metric {
                name: "keel_send_seconds_total",
                help: "Time spent encoding consensus messages and handing them to the transport",
                kind: Kind::Counter,
                value: p.send_nanos as f64 / 1e9,
            },
            Metric {
                name: "keel_apply_seconds_total",
                help: "Time spent applying committed entries to the state machine",
                kind: Kind::Counter,
                value: p.apply_nanos as f64 / 1e9,
            },
            Metric {
                name: "keel_snapshots_taken_total",
                help: "Storage-engine checkpoints published by this node",
                kind: Kind::Counter,
                value: snapshots.checkpoints as f64,
            },
            Metric {
                name: "keel_snapshot_checkpoint_seconds_total",
                help: "Time spent creating and publishing local checkpoints",
                kind: Kind::Counter,
                value: snapshots.checkpoint_nanos as f64 / 1e9,
            },
            Metric {
                name: "keel_snapshots_installed_total",
                help: "Received snapshots durably installed by this node",
                kind: Kind::Counter,
                value: snapshots.installed as f64,
            },
            Metric {
                name: "keel_snapshot_bytes_sent_total",
                help: "Checkpoint payload bytes sent to followers",
                kind: Kind::Counter,
                value: snapshots.bytes_sent as f64,
            },
            Metric {
                name: "keel_snapshot_bytes_received_total",
                help: "Checkpoint payload bytes verified from leaders",
                kind: Kind::Counter,
                value: snapshots.bytes_received as f64,
            },
        ]
    }

    fn histograms(&self) -> Vec<Histogram> {
        let progress = &self.1;
        let clients = &self.3;
        vec![
            histogram_count(
                "keel_ready_batch_entries",
                "Entries appended in one Ready batch",
                &progress.batch_sizes,
                &keel_node::BATCH_SIZE_BUCKETS,
            ),
            histogram_seconds(
                "keel_persist_seconds",
                "Time spent persisting one Ready",
                &progress.persist_latencies,
            ),
            histogram_seconds(
                "keel_fsync_seconds",
                "Time spent in one durable log sync",
                &progress.fsync_latencies,
            ),
            histogram_seconds(
                "keel_apply_seconds",
                "Time spent applying one Ready batch",
                &progress.apply_latencies,
            ),
            histogram_count(
                "keel_follower_inflight_messages",
                "Replication messages in flight per follower observation",
                &progress.follower_inflight,
                &keel_node::INFLIGHT_MESSAGE_BUCKETS,
            ),
            histogram_seconds(
                "keel_commit_seconds",
                "Command latency from parking through committed response",
                &clients.commit_latencies,
            ),
        ]
    }
}

fn histogram_seconds(
    name: &'static str,
    help: &'static str,
    buckets: &keel_node::Buckets<12>,
) -> Histogram {
    Histogram {
        name,
        help,
        buckets: keel_node::LATENCY_NANOS_BUCKETS
            .iter()
            .zip(buckets.counts)
            .map(|(upper, count)| (*upper as f64 / 1e9, count))
            .collect(),
        sum: buckets.sum as f64 / 1e9,
        count: buckets.count,
    }
}

fn histogram_count<const N: usize>(
    name: &'static str,
    help: &'static str,
    buckets: &keel_node::Buckets<N>,
    upper_bounds: &[u64; N],
) -> Histogram {
    Histogram {
        name,
        help,
        buckets: upper_bounds
            .iter()
            .zip(buckets.counts)
            .map(|(upper, count)| (*upper as f64, count))
            .collect(),
        sum: buckets.sum as f64,
        count: buckets.count,
    }
}
