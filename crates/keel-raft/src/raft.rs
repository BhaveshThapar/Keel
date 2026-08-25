use bytes::Bytes;

use crate::confchange;
use crate::log::{AppendOutcome, RaftLog};
use crate::message::{Message, MessageBody};
use crate::quorum::VoteResult;
use crate::read_only::{ReadOnly, ReadRequest};
use crate::rng::SplitMix64;
use crate::tracker::{ProgressState, ProgressTracker};
use crate::types::{
    ConfChangeV2, ConfState, DropReason, Entry, EntryPayload, HardState, Index, NodeId,
    ReadOnlyOption, ReadState, Role, SnapshotMeta, SoftState, Term,
};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StepError {
    #[error("not the leader")]
    NotLeader,
    #[error("configuration change rejected: {0}")]
    ConfChange(#[from] confchange::ConfChangeError),
}

#[derive(Debug, Clone)]
pub struct Config {
    pub id: NodeId,
    /// Election timeout in ticks. The actual timeout is drawn uniformly from
    /// `[election_tick, 2 * election_tick)` to break symmetry between candidates.
    pub election_tick: u32,
    pub heartbeat_tick: u32,
    /// The core's only entropy. Two cores with the same seed and the same input
    /// sequence produce byte-identical output (FR-1).
    pub rng_seed: u64,
    pub pre_vote: bool,
    pub check_quorum: bool,
    pub read_only: ReadOnlyOption,
    pub max_entries_per_msg: usize,
    pub max_bytes_per_msg: usize,
    pub max_inflight_msgs: usize,
    pub max_inflight_bytes: usize,
    pub max_uncommitted_bytes: usize,
    pub max_pending_reads: usize,
    /// Removes the Figure 8 current-term commit rule so the simulator can be
    /// shown catching the resulting State Machine Safety violation (TR-8c).
    /// Only honoured under the `negative-demos` feature.
    pub unsafe_disable_fig8_guard: bool,
}

impl Config {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            election_tick: 10,
            heartbeat_tick: 2,
            rng_seed: id,
            pre_vote: true,
            check_quorum: true,
            read_only: ReadOnlyOption::ReadIndex,
            max_entries_per_msg: 256,
            max_bytes_per_msg: 1 << 20,
            max_inflight_msgs: 16,
            max_inflight_bytes: 8 << 20,
            max_uncommitted_bytes: 64 << 20,
            max_pending_reads: 1024,
            unsafe_disable_fig8_guard: false,
        }
    }
}

/// Everything the core wants to be told about.
#[derive(Debug, Clone)]
pub enum Input {
    Tick,
    Message(Message),
    Propose {
        ctx: u64,
        data: Bytes,
    },
    ProposeConfChange {
        ctx: u64,
        cc: ConfChangeV2,
    },
    ReadIndex {
        ctx: u64,
    },
    TransferLeader {
        to: NodeId,
    },
    ReportUnreachable {
        peer: NodeId,
    },
    ReportSnapshotStatus {
        peer: NodeId,
        ok: bool,
    },
    Campaign,
    /// The host has taken a checkpoint through `meta.index`.
    ///
    /// The core drops every in-memory entry at or below it, which is what
    /// bounds memory on a long-running node: without this the log grows for as
    /// long as the process does. It also becomes the snapshot a lagging follower
    /// is offered, which is why `meta` carries the *checkpointed* configuration
    /// rather than the current one — a follower that installed a snapshot and
    /// then adopted today's membership would have skipped every configuration
    /// change between the two.
    ///
    /// Refused, with no effect, if the index is above what has been applied or
    /// at or below a snapshot already held. Both are the host telling the core
    /// something that is not true: it cannot have checkpointed state it has not
    /// applied, and a checkpoint that goes backwards would discard entries a
    /// follower may still need while claiming coverage it does not have.
    SnapshotTaken {
        meta: SnapshotMeta,
    },
    /// Give up leadership without nominating a successor.
    ///
    /// Distinct from [`Input::TransferLeader`], which hands leadership to a
    /// named node and keeps the cluster serving through the change. This one is
    /// for the cases where there is nobody to hand it to and staying leader is
    /// the wrong thing: a node whose storage has latched a fatal error and can
    /// no longer make an entry durable, an operator draining a machine, a
    /// process that is about to exit. It is a no-op on a node that is not the
    /// leader.
    ///
    /// The term does not move. Stepping down is not a failure of the term, and
    /// bumping it would force an election on a cluster that is about to hold one
    /// anyway — while also making every follower's term jump for a reason none
    /// of them can see.
    StepDown,
}

/// Everything the host must act on, in this order:
///
/// 1. persist `hard_state` and `entries`, and fsync;
/// 2. send `messages`;
/// 3. apply `committed_entries` to the state machine, in order;
/// 4. call [`RaftCore::advance`] with the watermarks reached.
///
/// The order is the safety contract, not a suggestion. A vote response never
/// appears in a `Ready` without the `HardState` recording that vote, so a node
/// that crashes after sending a grant cannot come back and grant a second one
/// in the same term (FR-4, PRD question 5).
#[derive(Debug, Clone, Default)]
pub struct Ready {
    pub number: u64,
    pub soft_state: Option<SoftState>,
    pub hard_state: Option<HardState>,
    pub entries: Vec<Entry>,
    pub messages: Vec<Message>,
    pub committed_entries: Vec<Entry>,
    pub snapshot_to_install: Option<SnapshotMeta>,
    pub read_states: Vec<ReadState>,
    pub proposals_dropped: Vec<(u64, DropReason)>,
}

impl Ready {
    pub fn is_empty(&self) -> bool {
        self.soft_state.is_none()
            && self.hard_state.is_none()
            && self.entries.is_empty()
            && self.messages.is_empty()
            && self.committed_entries.is_empty()
            && self.snapshot_to_install.is_none()
            && self.read_states.is_empty()
            && self.proposals_dropped.is_empty()
    }
}

/// What a restart recovered from disk.
///
/// A struct rather than five positional arguments: four of the five are
/// `Option`s, `Vec`s and structs a caller could transpose without the compiler
/// objecting, and getting `applied` and `commit` the wrong way round is exactly
/// the kind of mistake that produces a cluster which is subtly wrong rather than
/// obviously broken.
#[derive(Debug, Clone, Default)]
pub struct Restored {
    /// The configuration as of the last snapshot, or the bootstrap one.
    pub conf: ConfState,
    /// Term, vote and commit index, as they were fsynced.
    pub hard_state: HardState,
    /// The snapshot the log was compacted to, if any.
    pub snapshot: Option<SnapshotMeta>,
    /// Every entry above the snapshot that survived recovery.
    pub entries: Vec<Entry>,
    /// What the state machine says it has applied — read from the same atomic
    /// batch as the data it describes, never inferred from the log.
    pub applied: Index,
}

/// What the host got done. Watermarks, not deltas, so a host that batches or
/// reorders its own work cannot desynchronise the core.
#[derive(Debug, Clone, Default)]
pub struct Advance {
    pub ready_number: u64,
    pub persisted: Option<(Index, Term)>,
    pub applied: Option<Index>,
    pub snapshot_installed: Option<SnapshotMeta>,
}

#[derive(Debug, Clone)]
pub struct Status {
    pub id: NodeId,
    pub term: Term,
    pub role: Role,
    pub leader: Option<NodeId>,
    pub commit: Index,
    pub applied: Index,
    pub persisted: Index,
    pub last_index: Index,
    pub conf: ConfState,
    pub progress: Vec<(NodeId, Index, ProgressState)>,
    /// How many times this node committed an entry from an earlier term purely
    /// on replica count. Always zero in a correct build: the Figure 8 rule
    /// forbids it. Non-zero means the guard was compiled out, which the
    /// negative demonstrations do deliberately.
    pub fig8_bypasses: u64,
    /// Entries the core is holding in memory above its snapshot floor. What a
    /// checkpoint bounds, and the number that grows without limit if nothing
    /// ever takes one.
    pub in_memory_entries: usize,
    /// `SnapshotTaken` inputs refused as impossible or stale.
    pub snapshots_refused: u64,
}

pub struct RaftCore {
    cfg: Config,
    role: Role,
    term: Term,
    voted_for: Option<NodeId>,
    leader: Option<NodeId>,
    log: RaftLog,
    tracker: ProgressTracker,
    rng: SplitMix64,

    ticks: u64,
    election_elapsed: u32,
    heartbeat_elapsed: u32,
    randomized_election_timeout: u32,

    read_only: ReadOnly,
    heartbeat_round_start: u64,
    heartbeat_acks: std::collections::BTreeMap<NodeId, bool>,
    lease_until: u64,

    lead_transferee: Option<NodeId>,
    /// No new configuration change may be proposed until this index is applied.
    pending_conf_index: Index,
    uncommitted_bytes: usize,
    fig8_bypasses: u64,
    /// `SnapshotTaken` inputs the core refused. Non-zero means a host claimed a
    /// checkpoint it could not have taken, which is worth surfacing rather than
    /// swallowing.
    snapshots_refused: u64,
    /// The configuration as of the last checkpoint, offered to a follower that
    /// has fallen behind the log's floor.
    checkpoint_conf: Option<ConfState>,

    // Staged for the next Ready.
    msgs: Vec<Message>,
    read_states: Vec<ReadState>,
    dropped: Vec<(u64, DropReason)>,
    pending_snapshot: Option<SnapshotMeta>,
    hard_state_dirty: bool,
    last_soft_state: Option<SoftState>,
    /// Highest index already handed to the host for application.
    emitted_applied: Index,
    ready_number: u64,
}

impl RaftCore {
    /// Start a node in a brand-new cluster.
    pub fn new(cfg: Config, initial: ConfState) -> Self {
        Self::build(cfg, initial, HardState::default(), None, Vec::new())
    }

    /// Rebuild after a restart from what the host recovered off disk.
    ///
    /// One struct rather than five positional arguments, because four of the
    /// five are `Option`s, `Vec`s and structs that a caller can transpose
    /// without the compiler noticing.
    pub fn restore(cfg: Config, restored: Restored) -> Self {
        let Restored {
            conf,
            hard_state,
            snapshot,
            entries,
            applied,
        } = restored;
        let mut core = Self::build(cfg, conf, hard_state, snapshot, entries);
        // What the state machine says it applied, not what the log infers. The
        // two differ after a restart: the log knows only its snapshot floor,
        // while the state machine wrote its `applied_index` in the same atomic
        // batch as the data (ADR-010) and therefore knows exactly. Taking the
        // log's guess would re-hand every entry between the two to the state
        // machine — harmless while apply is idempotent, and not harmless at all
        // once a session table is deduplicating against sequence numbers those
        // entries carry.
        if applied > 0 {
            core.set_applied_floor(applied);
        }
        core
    }

    /// Everything a restart recovers, in one place.
    fn build(
        cfg: Config,
        conf: ConfState,
        hs: HardState,
        snapshot: Option<SnapshotMeta>,
        entries: Vec<Entry>,
    ) -> Self {
        let conf = match &snapshot {
            // A snapshot carries the configuration as of its index; entries
            // above it may still change that, which apply-time replay handles.
            Some(meta) if meta.index > 0 => meta.conf.clone(),
            _ => conf,
        };
        let log = RaftLog::restore(
            snapshot.as_ref().map(|m| (m.index, m.term)),
            entries,
            hs.commit,
        );
        let mut tracker = ProgressTracker::new(conf, cfg.max_inflight_msgs, cfg.max_inflight_bytes);
        tracker.set_conf(tracker.conf().clone(), log.last_index() + 1);
        let mut rng = SplitMix64::new(cfg.rng_seed);
        let randomized_election_timeout =
            cfg.election_tick + rng.range_u32(0, cfg.election_tick.max(1));
        let read_only = ReadOnly::new(cfg.max_pending_reads);
        let emitted_applied = log.applied();
        let mut core = Self {
            cfg,
            role: Role::Follower,
            term: hs.term,
            voted_for: hs.voted_for,
            leader: None,
            log,
            tracker,
            rng,
            ticks: 0,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            randomized_election_timeout,
            read_only,
            heartbeat_round_start: 0,
            heartbeat_acks: std::collections::BTreeMap::new(),
            lease_until: 0,
            lead_transferee: None,
            pending_conf_index: 0,
            uncommitted_bytes: 0,
            fig8_bypasses: 0,
            snapshots_refused: 0,
            checkpoint_conf: None,
            msgs: Vec::new(),
            read_states: Vec::new(),
            dropped: Vec::new(),
            pending_snapshot: None,
            hard_state_dirty: false,
            last_soft_state: None,
            emitted_applied,
            ready_number: 0,
        };
        core.replay_conf_changes();
        core
    }

    /// Tell the core the state machine is already past `applied`.
    ///
    /// Clamped to `commit`, because an `applied` above the commit index means
    /// the state machine has applied something this node cannot prove was
    /// committed — which is either a corrupt `applied_index` or a log that lost
    /// its tail. Trusting it would be trusting the one number that is now known
    /// to disagree with the log; re-applying a few entries is the recoverable
    /// half of the two.
    fn set_applied_floor(&mut self, applied: Index) {
        let floor = applied.min(self.log.committed());
        self.log.set_applied(floor);
        self.emitted_applied = self.emitted_applied.max(floor);
        self.replay_conf_changes();
    }

    /// Re-derive the configuration from conf-change entries already applied at
    /// restart time. The log below `applied` was applied before the crash, so
    /// the configuration must reflect it.
    fn replay_conf_changes(&mut self) {
        let applied = self.log.applied();
        let changes: Vec<ConfChangeV2> = self
            .log
            .all_entries()
            .filter(|e| e.index <= applied)
            .filter_map(|e| match &e.payload {
                EntryPayload::ConfChange(cc) => Some(cc.clone()),
                _ => None,
            })
            .collect();
        for cc in changes {
            if let Ok(next) = confchange::apply(self.tracker.conf(), &cc) {
                self.tracker.set_conf(next, self.log.last_index() + 1);
            }
        }
    }

    // ---------------------------------------------------------------- accessors

    pub fn id(&self) -> NodeId {
        self.cfg.id
    }

    pub fn term(&self) -> Term {
        self.term
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    pub fn log(&self) -> &RaftLog {
        &self.log
    }

    pub fn conf(&self) -> &ConfState {
        self.tracker.conf()
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    pub fn status(&self) -> Status {
        Status {
            id: self.cfg.id,
            term: self.term,
            role: self.role,
            leader: self.leader,
            commit: self.log.committed(),
            applied: self.log.applied(),
            persisted: self.log.persisted(),
            last_index: self.log.last_index(),
            conf: self.tracker.conf().clone(),
            progress: self
                .tracker
                .progress
                .iter()
                .map(|(id, pr)| (*id, pr.matched, pr.state))
                .collect(),
            fig8_bypasses: self.fig8_bypasses,
            in_memory_entries: self.log.len(),
            snapshots_refused: self.snapshots_refused,
        }
    }

    fn hard_state(&self) -> HardState {
        HardState {
            term: self.term,
            voted_for: self.voted_for,
            commit: self.log.committed(),
        }
    }

    fn soft_state(&self) -> SoftState {
        SoftState {
            leader: self.leader,
            role: self.role,
        }
    }

    /// Whether this node may stand for election: it must be a voter in the
    /// configuration it knows about.
    fn promotable(&self) -> bool {
        self.tracker.conf().voters.contains(&self.cfg.id)
    }

    fn peers(&self) -> Vec<NodeId> {
        self.tracker
            .conf()
            .all_members()
            .into_iter()
            .filter(|id| *id != self.cfg.id)
            .collect()
    }

    fn election_timeout_reached(&self) -> bool {
        self.election_elapsed >= self.randomized_election_timeout
    }

    /// Check-quorum's disruption guard: while this node has heard from a leader
    /// within the last election timeout, it refuses to even consider a vote
    /// request. Without it, a node that was partitioned away and bumped its term
    /// deposes a perfectly healthy leader the moment it reconnects (TR-8a).
    fn leader_lease_held(&self) -> bool {
        self.cfg.check_quorum && self.leader.is_some() && !self.election_timeout_reached()
    }

    fn lease_ticks(&self) -> u64 {
        match self.cfg.read_only {
            ReadOnlyOption::ReadIndex => 0,
            ReadOnlyOption::LeaseBased { drift_bound_pct } => {
                let pct = u64::from(100u8.saturating_sub(drift_bound_pct.min(100)));
                u64::from(self.cfg.election_tick) * pct / 100
            }
        }
    }

    /// Whether a lease read may be served locally right now.
    pub fn lease_valid(&self) -> bool {
        matches!(self.cfg.read_only, ReadOnlyOption::LeaseBased { .. })
            && self.role == Role::Leader
            && self.ticks < self.lease_until
    }

    // ------------------------------------------------------------------- step

    pub fn step(&mut self, input: Input) -> Result<(), StepError> {
        match input {
            Input::Tick => {
                self.tick();
                Ok(())
            }
            Input::Message(m) => {
                self.step_message(m);
                Ok(())
            }
            Input::Propose { ctx, data } => self.propose(ctx, data),
            Input::ProposeConfChange { ctx, cc } => self.propose_conf_change(ctx, cc),
            Input::ReadIndex { ctx } => {
                self.request_read_index(ctx, None);
                Ok(())
            }
            Input::TransferLeader { to } => {
                self.transfer_leader(to);
                Ok(())
            }
            Input::ReportUnreachable { peer } => {
                if let Some(pr) = self.tracker.progress.get_mut(&peer)
                    && pr.state == ProgressState::Replicate
                {
                    pr.become_probe();
                }
                Ok(())
            }
            Input::ReportSnapshotStatus { peer, ok } => {
                self.report_snapshot(peer, ok);
                Ok(())
            }
            Input::Campaign => {
                self.campaign(false);
                Ok(())
            }
            Input::SnapshotTaken { meta } => {
                self.on_snapshot_taken(meta);
                Ok(())
            }
            Input::StepDown => {
                if self.role == Role::Leader {
                    // No leader hint: this node is not nominating anyone, and a
                    // hint pointing at itself would send clients straight back.
                    self.become_follower(self.term, None);
                }
                Ok(())
            }
        }
    }

    fn tick(&mut self) {
        self.ticks += 1;
        self.election_elapsed += 1;

        if self.role == Role::Leader {
            self.heartbeat_elapsed += 1;

            if self.election_elapsed >= self.cfg.election_tick {
                self.election_elapsed = 0;
                if self.cfg.check_quorum && !self.tracker.quorum_active() {
                    // We have lost contact with a majority. Step down instead of
                    // continuing to accept writes we can never commit.
                    self.become_follower(self.term, None);
                    return;
                }
                self.tracker.clear_recent_active(self.cfg.id);
                if self.lead_transferee.is_some() {
                    // The target never caught up; stop waiting so proposals flow again.
                    self.lead_transferee = None;
                }
            }

            if self.heartbeat_elapsed >= self.cfg.heartbeat_tick {
                self.heartbeat_elapsed = 0;
                self.bcast_heartbeat();
            }
            return;
        }

        if self.promotable() && self.election_timeout_reached() {
            self.election_elapsed = 0;
            self.campaign(false);
        }
    }

    fn step_message(&mut self, m: Message) {
        // Pre-vote requests carry a speculative term, so they bypass the normal
        // term reconciliation entirely: answering one never changes our term.
        if let MessageBody::PreVoteReq {
            last_log_index,
            last_log_term,
        } = m.body
        {
            let granted = m.term > self.term
                && self.log.is_up_to_date(last_log_index, last_log_term)
                && !self.leader_lease_held();
            let resp_term = if granted { m.term } else { self.term };
            self.send_with_term(m.from, resp_term, MessageBody::PreVoteResp { granted });
            return;
        }

        if m.carries_real_term() && m.term > self.term {
            let force = matches!(
                m.body,
                MessageBody::VoteReq {
                    is_transfer: true,
                    ..
                }
            );
            if matches!(m.body, MessageBody::VoteReq { .. }) && !force && self.leader_lease_held() {
                // Check-quorum: ignore the request outright, keeping our leader.
                return;
            }
            let lead = match m.body {
                MessageBody::AppendReq { .. }
                | MessageBody::Heartbeat { .. }
                | MessageBody::SnapshotOffer { .. } => Some(m.from),
                _ => None,
            };
            self.become_follower(m.term, lead);
        } else if m.carries_real_term() && m.term < self.term {
            if matches!(
                m.body,
                MessageBody::AppendReq { .. } | MessageBody::Heartbeat { .. }
            ) {
                // Reply at our own term so a stale leader learns it is stale.
                self.send(
                    m.from,
                    MessageBody::AppendRejected {
                        rejected_index: 0,
                        conflict_index: 0,
                        conflict_term: None,
                    },
                );
            }
            return;
        }

        match m.body {
            MessageBody::VoteReq {
                last_log_index,
                last_log_term,
                ..
            } => {
                let can_vote = self.voted_for == Some(m.from)
                    || (self.voted_for.is_none() && self.leader.is_none());
                let granted = can_vote && self.log.is_up_to_date(last_log_index, last_log_term);
                if granted {
                    self.voted_for = Some(m.from);
                    // Forces the vote into this same Ready's HardState, so the
                    // host fsyncs it before the grant leaves the node.
                    self.hard_state_dirty = true;
                    self.election_elapsed = 0;
                }
                self.send(m.from, MessageBody::VoteResp { granted });
            }
            MessageBody::PreVoteResp { granted } => self.handle_pre_vote_resp(m.from, granted),
            MessageBody::VoteResp { granted } => self.handle_vote_resp(m.from, granted),
            MessageBody::AppendReq { .. } => self.handle_append(m),
            MessageBody::Heartbeat {
                leader_commit,
                read_batch,
            } => self.handle_heartbeat(m.from, leader_commit, read_batch),
            MessageBody::AppendAccepted { last_index } => {
                self.handle_append_accepted(m.from, last_index)
            }
            MessageBody::AppendRejected {
                rejected_index,
                conflict_index,
                conflict_term,
            } => self.handle_append_rejected(m.from, rejected_index, conflict_index, conflict_term),
            MessageBody::HeartbeatResp { read_batch } => {
                self.handle_heartbeat_resp(m.from, read_batch)
            }
            MessageBody::ReadIndexReq { ctx } => self.request_read_index(ctx, Some(m.from)),
            MessageBody::ReadIndexResp { ctx, index } => {
                self.read_states.push(ReadState { ctx, index });
            }
            MessageBody::SnapshotOffer { meta } => self.handle_snapshot_offer(m.from, meta),
            MessageBody::TimeoutNow => {
                if self.promotable() {
                    // The leader asked us to take over: skip pre-vote, since we
                    // know the leadership is being handed over deliberately.
                    self.campaign(true);
                }
            }
            MessageBody::PreVoteReq { .. } => unreachable!("handled above"),
        }
    }

    // -------------------------------------------------------------- elections

    fn campaign(&mut self, transfer: bool) {
        if !self.promotable() {
            return;
        }
        self.read_only.drain_all();

        if self.cfg.pre_vote && !transfer {
            self.become_pre_candidate();
            let speculative = self.term + 1;
            self.tracker.record_vote(self.cfg.id, true);
            if self.tracker.tally_votes() == VoteResult::Won {
                self.start_real_election(transfer);
                return;
            }
            for peer in self.peers() {
                if !self.tracker.conf().is_voter(peer) {
                    continue;
                }
                self.send_with_term(
                    peer,
                    speculative,
                    MessageBody::PreVoteReq {
                        last_log_index: self.log.last_index(),
                        last_log_term: self.log.last_term(),
                    },
                );
            }
        } else {
            self.start_real_election(transfer);
        }
    }

    fn start_real_election(&mut self, transfer: bool) {
        self.become_candidate();
        self.tracker.record_vote(self.cfg.id, true);
        if self.tracker.tally_votes() == VoteResult::Won {
            self.become_leader();
            return;
        }
        for peer in self.peers() {
            if !self.tracker.conf().is_voter(peer) {
                continue;
            }
            self.send(
                peer,
                MessageBody::VoteReq {
                    last_log_index: self.log.last_index(),
                    last_log_term: self.log.last_term(),
                    is_transfer: transfer,
                },
            );
        }
    }

    fn handle_pre_vote_resp(&mut self, from: NodeId, granted: bool) {
        if self.role != Role::PreCandidate {
            return;
        }
        self.tracker.record_vote(from, granted);
        match self.tracker.tally_votes() {
            VoteResult::Won => self.start_real_election(false),
            VoteResult::Lost => self.become_follower(self.term, None),
            VoteResult::Pending => {}
        }
    }

    fn handle_vote_resp(&mut self, from: NodeId, granted: bool) {
        if self.role != Role::Candidate {
            return;
        }
        self.tracker.record_vote(from, granted);
        match self.tracker.tally_votes() {
            VoteResult::Won => self.become_leader(),
            VoteResult::Lost => self.become_follower(self.term, None),
            VoteResult::Pending => {}
        }
    }

    fn reset(&mut self, term: Term) {
        if self.term != term {
            self.term = term;
            self.voted_for = None;
            self.hard_state_dirty = true;
        }
        self.leader = None;
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        self.lead_transferee = None;
        self.lease_until = 0;
        self.heartbeat_acks.clear();
        self.randomized_election_timeout =
            self.cfg.election_tick + self.rng.range_u32(0, self.cfg.election_tick.max(1));
        self.tracker.reset_votes();
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        let stale_reads = self.read_only.drain_all();
        self.fail_reads(stale_reads);
        self.reset(term);
        self.role = Role::Follower;
        self.leader = leader;
        self.uncommitted_bytes = 0;
    }

    fn become_pre_candidate(&mut self) {
        // The term does not move: that is the whole point of pre-vote.
        self.role = Role::PreCandidate;
        self.leader = None;
        self.election_elapsed = 0;
        self.tracker.reset_votes();
        self.randomized_election_timeout =
            self.cfg.election_tick + self.rng.range_u32(0, self.cfg.election_tick.max(1));
    }

    fn become_candidate(&mut self) {
        let term = self.term + 1;
        self.reset(term);
        self.role = Role::Candidate;
        self.voted_for = Some(self.cfg.id);
        self.hard_state_dirty = true;
    }

    fn become_leader(&mut self) {
        self.reset(self.term);
        self.role = Role::Leader;
        self.leader = Some(self.cfg.id);
        self.tracker
            .reset_progress(self.log.last_index() + 1, self.cfg.id, self.log.persisted());

        // A no-op in the new term. Until it commits, the Figure 8 rule blocks
        // any commit at all, so this is what unblocks the term — and it is why
        // ReadIndex must wait for it before serving (PRD question 3).
        let index = self.log.last_index() + 1;
        self.log
            .append([Entry::new(self.term, index, EntryPayload::Noop)]);
        self.pending_conf_index = index;
        self.uncommitted_bytes = 0;

        if let Some(pr) = self.tracker.progress.get_mut(&self.cfg.id) {
            pr.maybe_update(self.log.persisted());
        }
        self.maybe_commit();
        self.bcast_append();
    }

    // ------------------------------------------------------------ replication

    fn propose(&mut self, ctx: u64, data: Bytes) -> Result<(), StepError> {
        if self.role != Role::Leader {
            self.dropped
                .push((ctx, DropReason::NotLeader { hint: self.leader }));
            return Err(StepError::NotLeader);
        }
        if self.lead_transferee.is_some() {
            self.dropped
                .push((ctx, DropReason::LeaderTransferInProgress));
            return Ok(());
        }
        if self.uncommitted_bytes + data.len() > self.cfg.max_uncommitted_bytes {
            self.dropped.push((ctx, DropReason::Overloaded));
            return Ok(());
        }
        self.uncommitted_bytes += data.len();
        let index = self.log.last_index() + 1;
        self.log
            .append([Entry::new(self.term, index, EntryPayload::Normal(data))]);
        if let Some(pr) = self.tracker.progress.get_mut(&self.cfg.id) {
            pr.next = self.log.last_index() + 1;
        }
        self.bcast_append();
        Ok(())
    }

    fn propose_conf_change(&mut self, ctx: u64, cc: ConfChangeV2) -> Result<(), StepError> {
        if self.role != Role::Leader {
            self.dropped
                .push((ctx, DropReason::NotLeader { hint: self.leader }));
            return Err(StepError::NotLeader);
        }
        if self.log.applied() < self.pending_conf_index {
            self.dropped.push((ctx, DropReason::ConfChangeInFlight));
            return Ok(());
        }
        // Validate now so a bad change is rejected synchronously instead of
        // wedging the cluster at apply time.
        confchange::apply(self.tracker.conf(), &cc)?;
        let index = self.log.last_index() + 1;
        self.log
            .append([Entry::new(self.term, index, EntryPayload::ConfChange(cc))]);
        self.pending_conf_index = index;
        if let Some(pr) = self.tracker.progress.get_mut(&self.cfg.id) {
            pr.next = self.log.last_index() + 1;
        }
        self.bcast_append();
        Ok(())
    }

    fn bcast_append(&mut self) {
        for peer in self.peers() {
            self.maybe_send_append(peer, false);
        }
    }

    fn maybe_send_append(&mut self, to: NodeId, force: bool) {
        let Some(pr) = self.tracker.progress.get(&to) else {
            return;
        };
        if !force && pr.is_paused() {
            return;
        }
        let prev_index = pr.next.saturating_sub(1);
        let Some(prev_term) = self.log.term(prev_index) else {
            // The follower needs entries we have already compacted away.
            // The configuration as of the checkpoint, not today's. A follower
            // that installed this snapshot and then adopted the current
            // membership would have skipped every configuration change between
            // the two, and would be counting quorums against a set the rest of
            // the cluster has moved on from.
            let meta = SnapshotMeta {
                index: self.log.snapshot_index(),
                term: self.log.snapshot_term(),
                conf: self
                    .checkpoint_conf
                    .clone()
                    .unwrap_or_else(|| self.tracker.conf().clone()),
            };
            let index = meta.index;
            self.send(to, MessageBody::SnapshotOffer { meta });
            if let Some(pr) = self.tracker.progress.get_mut(&to) {
                pr.become_snapshot(index);
            }
            return;
        };

        let entries = self.log.slice(
            pr.next,
            self.log.last_index(),
            self.cfg.max_entries_per_msg,
            self.cfg.max_bytes_per_msg,
        );
        if entries.is_empty() && !force {
            return;
        }
        let bytes: usize = entries.iter().map(Entry::payload_len).sum();
        let last = prev_index + entries.len() as Index;

        self.send(
            to,
            MessageBody::AppendReq {
                prev_log_index: prev_index,
                prev_log_term: prev_term,
                entries,
                leader_commit: self.log.committed(),
            },
        );

        if let Some(pr) = self.tracker.progress.get_mut(&to) {
            match pr.state {
                ProgressState::Replicate => {
                    pr.next = last + 1;
                    pr.inflight.add(last, bytes);
                }
                ProgressState::Probe => pr.probe_sent = true,
                ProgressState::Snapshot => {}
            }
        }
    }

    fn bcast_heartbeat(&mut self) {
        self.heartbeat_acks.clear();
        self.heartbeat_acks.insert(self.cfg.id, true);
        self.heartbeat_round_start = self.ticks;

        let batch = self
            .read_only
            .start_batch(self.log.committed(), self.cfg.id);
        let commit = self.log.committed();

        for peer in self.peers() {
            let commit_for_peer = self
                .tracker
                .progress
                .get(&peer)
                .map_or(commit, |pr| commit.min(pr.matched));
            self.send(
                peer,
                MessageBody::Heartbeat {
                    leader_commit: commit_for_peer,
                    read_batch: batch,
                },
            );
        }

        // A single-voter cluster reaches quorum on its own heartbeat.
        self.check_heartbeat_quorum();
        if let Some(id) = batch {
            self.resolve_read_batch(id, self.cfg.id);
        }
    }

    fn handle_append(&mut self, m: Message) {
        let MessageBody::AppendReq {
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        } = m.body
        else {
            return;
        };
        self.election_elapsed = 0;
        self.leader = Some(m.from);
        if self.role != Role::Follower {
            self.role = Role::Follower;
        }

        if prev_log_index < self.log.committed() {
            // A stale probe for history we have already committed. Point the
            // leader at where we actually are instead of rejecting.
            self.send(
                m.from,
                MessageBody::AppendAccepted {
                    last_index: self.log.committed(),
                },
            );
            return;
        }

        match self.log.try_append(prev_log_index, prev_log_term, entries) {
            AppendOutcome::Ok { last_index } => {
                if self.log.commit_to(leader_commit.min(last_index)) {
                    self.hard_state_dirty = true;
                }
                self.send(m.from, MessageBody::AppendAccepted { last_index });
            }
            AppendOutcome::Conflict {
                conflict_index,
                conflict_term,
            } => {
                self.send(
                    m.from,
                    MessageBody::AppendRejected {
                        rejected_index: prev_log_index,
                        conflict_index,
                        conflict_term,
                    },
                );
            }
        }
    }

    fn handle_heartbeat(&mut self, from: NodeId, leader_commit: Index, read_batch: Option<u64>) {
        self.election_elapsed = 0;
        self.leader = Some(from);
        if self.role != Role::Follower {
            self.role = Role::Follower;
        }
        if self.log.commit_to(leader_commit) {
            self.hard_state_dirty = true;
        }
        self.send(from, MessageBody::HeartbeatResp { read_batch });
    }

    fn handle_append_accepted(&mut self, from: NodeId, last_index: Index) {
        if self.role != Role::Leader {
            return;
        }
        let Some(pr) = self.tracker.progress.get_mut(&from) else {
            return;
        };
        pr.recent_active = true;
        let advanced = pr.maybe_update(last_index);
        match pr.state {
            ProgressState::Probe if advanced => pr.become_replicate(),
            ProgressState::Snapshot if pr.matched >= pr.pending_snapshot => pr.become_probe(),
            ProgressState::Replicate => pr.inflight.free_le(last_index),
            _ => {}
        }
        let matched = pr.matched;

        if advanced && self.maybe_commit() {
            self.bcast_append();
        } else {
            self.maybe_send_append(from, false);
        }

        if self.lead_transferee == Some(from) && matched == self.log.last_index() {
            // The target is fully caught up, so it can win an election
            // immediately rather than losing to a more up-to-date peer.
            self.send(from, MessageBody::TimeoutNow);
        }
    }

    fn handle_append_rejected(
        &mut self,
        from: NodeId,
        rejected_index: Index,
        conflict_index: Index,
        conflict_term: Option<Term>,
    ) {
        if self.role != Role::Leader {
            return;
        }
        // Skip the whole conflicting term in one round trip when we can place it
        // in our own log; otherwise trust the follower's first-index-of-term hint.
        let next_probe = match conflict_term {
            Some(t) => match self.log.last_index_of_term(t) {
                Some(i) => i + 1,
                None => conflict_index,
            },
            None => conflict_index,
        };
        let Some(pr) = self.tracker.progress.get_mut(&from) else {
            return;
        };
        pr.recent_active = true;
        if !pr.maybe_decr_to(rejected_index, next_probe) {
            return;
        }
        if pr.state == ProgressState::Replicate {
            pr.become_probe();
        }
        self.maybe_send_append(from, true);
    }

    fn handle_heartbeat_resp(&mut self, from: NodeId, read_batch: Option<u64>) {
        if self.role != Role::Leader {
            return;
        }
        let behind = match self.tracker.progress.get_mut(&from) {
            Some(pr) => {
                pr.recent_active = true;
                // A heartbeat response proves the follower is reachable, so any
                // throttle we are holding is based on stale evidence. Both the
                // probe latch and a window filled with appends that a partition
                // swallowed would otherwise wedge this follower permanently.
                pr.probe_sent = false;
                if pr.state == ProgressState::Replicate && pr.inflight.full() {
                    pr.inflight.free_first_one();
                }
                pr.matched < self.log.last_index()
            }
            None => false,
        };
        if behind {
            self.maybe_send_append(from, false);
        }
        self.heartbeat_acks.insert(from, true);
        self.check_heartbeat_quorum();
        if let Some(id) = read_batch {
            self.resolve_read_batch(id, from);
        }
    }

    fn check_heartbeat_quorum(&mut self) {
        if self.tracker.joint().vote_result(&self.heartbeat_acks) != VoteResult::Won {
            return;
        }
        // The lease runs from when the round was *sent*, never from when the
        // acks came back: a slow ack cannot buy extra lease time it did not earn.
        let until = self.heartbeat_round_start + self.lease_ticks();
        self.lease_until = self.lease_until.max(until);
    }

    fn resolve_read_batch(&mut self, id: u64, from: NodeId) {
        let joint = self.tracker.joint().clone();
        if let Some((index, reqs)) = self.read_only.ack(id, from, &joint) {
            for req in reqs {
                self.resolve_read(req.ctx, req.from, index);
            }
        }
    }

    /// Advance the commit index to the highest index replicated on a quorum —
    /// but only if that index is from the current term.
    ///
    /// This is the Figure 8 rule. Counting replicas of an *older* term's entry
    /// is not enough: a later leader could still overwrite it, and committing it
    /// early makes two state machines diverge (PRD question 1).
    fn maybe_commit(&mut self) -> bool {
        let mci = self.tracker.committed_index();
        if mci <= self.log.committed() {
            return false;
        }
        let current_term = self.log.term(mci) == Some(self.term);
        #[cfg(feature = "negative-demos")]
        let current_term = current_term || self.cfg.unsafe_disable_fig8_guard;
        if !current_term {
            return false;
        }
        if self.log.term(mci) != Some(self.term) {
            self.fig8_bypasses += 1;
        }
        if self.log.commit_to(mci) {
            self.hard_state_dirty = true;
            self.read_only.release_parked();
            true
        } else {
            false
        }
    }

    // ------------------------------------------------------------------ reads

    fn request_read_index(&mut self, ctx: u64, from: Option<NodeId>) {
        if self.role != Role::Leader {
            match (from, self.leader) {
                // A follower forwards to the leader on the client's behalf.
                (None, Some(leader)) => self.send(leader, MessageBody::ReadIndexReq { ctx }),
                _ => self
                    .dropped
                    .push((ctx, DropReason::NotLeader { hint: self.leader })),
            }
            return;
        }
        if self.read_only.is_full() {
            self.dropped.push((ctx, DropReason::Overloaded));
            return;
        }
        let req = ReadRequest { ctx, from };
        // Until this term's no-op commits, the leader cannot know its commit
        // index reflects everything a previous leader committed. This guard is
        // *upstream* of the lease decision below on purpose: holding a lease
        // proves nobody else is leader, which is a different claim, and a lease
        // path with its own copy of this check is a lease path that can lose it.
        if self.log.term(self.log.committed()) != Some(self.term) {
            self.read_only.park(req);
            return;
        }
        // The read is eligible. Under a lease the leader may answer it itself,
        // with no round trip and no batch — which is the whole reason a lease
        // read is cheaper, and the whole reason it is only correct inside a
        // clock assumption (ADR-005).
        //
        // This lives here rather than in the host. Before, a host asked
        // `lease_valid()` and decided for itself, which put the one read path
        // whose correctness depends on a clock assumption outside the component
        // that owns the assumption, and left every host free to check it
        // differently or to skip the guard above.
        if self.lease_valid() {
            self.resolve_read(ctx, from, self.log.committed());
            return;
        }
        self.read_only.enqueue(req);
    }

    /// Hand one read its index — to the client here, or back to the follower
    /// that forwarded it.
    fn resolve_read(&mut self, ctx: u64, from: Option<NodeId>, index: Index) {
        match from {
            Some(peer) => self.send(peer, MessageBody::ReadIndexResp { ctx, index }),
            None => self.read_states.push(ReadState { ctx, index }),
        }
    }

    /// Adopt a checkpoint the host says it has taken.
    fn on_snapshot_taken(&mut self, meta: SnapshotMeta) {
        // Above what has been applied: the host cannot have checkpointed state
        // it has not applied, so this is a bug in the host rather than a race.
        if meta.index > self.log.applied() {
            self.snapshots_refused += 1;
            return;
        }
        // At or below one already held: stale, and adopting it would replace a
        // configuration that is newer with one that is older.
        if meta.index <= self.log.snapshot_index() {
            self.snapshots_refused += 1;
            return;
        }
        self.log.compact_prefix(meta.index);
        self.checkpoint_conf = Some(meta.conf);
    }

    fn fail_reads(&mut self, reqs: Vec<ReadRequest>) {
        for req in reqs {
            if req.from.is_none() {
                self.dropped
                    .push((req.ctx, DropReason::NotLeader { hint: self.leader }));
            }
        }
    }

    // ------------------------------------------------ transfer and snapshots

    fn transfer_leader(&mut self, to: NodeId) {
        if self.role != Role::Leader || to == self.cfg.id {
            return;
        }
        if !self.tracker.conf().is_voter(to) {
            return;
        }
        self.lead_transferee = Some(to);
        self.election_elapsed = 0;
        let matched = self.tracker.progress.get(&to).map_or(0, |pr| pr.matched);
        if matched == self.log.last_index() {
            self.send(to, MessageBody::TimeoutNow);
        } else {
            // Catch the target up first; the TimeoutNow goes out when its
            // AppendAccepted shows it has the full log.
            self.maybe_send_append(to, true);
        }
    }

    fn handle_snapshot_offer(&mut self, from: NodeId, meta: SnapshotMeta) {
        self.election_elapsed = 0;
        self.leader = Some(from);
        if meta.index <= self.log.committed() {
            self.send(
                from,
                MessageBody::AppendAccepted {
                    last_index: self.log.committed(),
                },
            );
            return;
        }
        // The host fetches and installs the bytes out of band, then tells us via
        // Advance::snapshot_installed.
        self.pending_snapshot = Some(meta);
    }

    fn report_snapshot(&mut self, peer: NodeId, ok: bool) {
        if self.role != Role::Leader {
            return;
        }
        let Some(pr) = self.tracker.progress.get_mut(&peer) else {
            return;
        };
        if pr.state != ProgressState::Snapshot {
            return;
        }
        if ok {
            pr.matched = pr.matched.max(pr.pending_snapshot);
        }
        pr.become_probe();
    }

    // ------------------------------------------------------------------ ready

    fn send(&mut self, to: NodeId, body: MessageBody) {
        let term = self.term;
        self.send_with_term(to, term, body);
    }

    fn send_with_term(&mut self, to: NodeId, term: Term, body: MessageBody) {
        self.msgs.push(Message::new(self.cfg.id, to, term, body));
    }

    pub fn has_ready(&self) -> bool {
        self.hard_state_dirty
            || !self.msgs.is_empty()
            || !self.read_states.is_empty()
            || !self.dropped.is_empty()
            || self.pending_snapshot.is_some()
            || self.log.unstable_from() <= self.log.last_index()
            || self.next_apply_high() > self.emitted_applied
            || self.last_soft_state != Some(self.soft_state())
    }

    fn next_apply_high(&self) -> Index {
        self.log.committed().min(self.log.persisted())
    }

    pub fn ready(&mut self) -> Ready {
        self.ready_number += 1;

        let entries = self.log.slice(
            self.log.unstable_from(),
            self.log.last_index(),
            usize::MAX,
            usize::MAX,
        );
        let last_index = self.log.last_index();
        self.log.mark_handed_off(last_index);

        // Bounded so a node that falls far behind and then catches up does not
        // hand the host a single unbounded apply batch.
        let high = self.next_apply_high();
        let mut committed_entries = Vec::new();
        if high > self.emitted_applied {
            let cap = self.cfg.max_entries_per_msg.max(1) as Index;
            let end = high.min(self.emitted_applied + cap);
            committed_entries = (self.emitted_applied + 1..=end)
                .filter_map(|i| self.log.entry(i).cloned())
                .collect();
            self.emitted_applied = end;
        }

        let soft = self.soft_state();
        let soft_state = if self.last_soft_state == Some(soft) {
            None
        } else {
            Some(soft)
        };
        self.last_soft_state = Some(soft);

        let hard_state = if self.hard_state_dirty {
            Some(self.hard_state())
        } else {
            None
        };
        self.hard_state_dirty = false;

        Ready {
            number: self.ready_number,
            soft_state,
            hard_state,
            entries,
            messages: std::mem::take(&mut self.msgs),
            committed_entries,
            snapshot_to_install: self.pending_snapshot.take(),
            read_states: std::mem::take(&mut self.read_states),
            proposals_dropped: std::mem::take(&mut self.dropped),
        }
    }

    pub fn advance(&mut self, ack: Advance) {
        // A host may only acknowledge a `Ready` this core actually emitted.
        // Acks may arrive out of order — fsync latencies vary and several
        // `Ready`s can be in flight at once — so this is a bound, not a
        // sequence. Every watermark below is a max, which is what makes
        // out-of-order acks safe.
        assert!(
            ack.ready_number > 0 && ack.ready_number <= self.ready_number,
            "advance acknowledged Ready {} but only {} have been emitted",
            ack.ready_number,
            self.ready_number
        );

        if let Some(meta) = ack.snapshot_installed {
            // Stale, and refused for the same reason `on_snapshot_taken`
            // refuses a stale checkpoint: a snapshot at or below what this log
            // has already committed carries nothing the log does not have, and
            // adopting it pulls the floor backwards — and, because
            // `restore_snapshot` sets all three watermarks from the snapshot's
            // index, the commit and applied indices with it. A node that
            // rewinds its applied index has un-applied entries it has already
            // acknowledged ([KEEL-17](../../../BUGS.md)).
            //
            // A leader can offer one: it sends a snapshot at its own floor, and
            // by the time the transfer completes the follower may have caught
            // up past it by ordinary replication.
            if meta.index <= self.log.committed().max(self.log.snapshot_index()) {
                self.snapshots_refused += 1;
            } else {
                self.log.restore_snapshot(meta.index, meta.term);
                self.tracker.set_conf(meta.conf, self.log.last_index() + 1);
                self.emitted_applied = self.log.applied();
                self.hard_state_dirty = true;
            }
        }

        // The term is what makes a late ack safe. A `Ready` can be in flight to
        // the disk while a new leader truncates the log underneath it; the ack
        // that lands afterwards names entries that no longer exist. Taking it at
        // face value would mark the *replacement* entries durable on the
        // strength of an fsync that covered the ones they replaced — and on a
        // leader, that is a quorum count against bytes that were never written
        // (ADR-003). Matching the term rejects exactly that ack. Nothing stalls:
        // a truncation pulls `unstable_from` back, so the replacements are
        // handed out again and acked on their own fsync.
        if let Some((index, term)) = ack.persisted
            && self.log.term(index) == Some(term)
        {
            self.log.set_persisted(index);
            if self.role == Role::Leader
                && let Some(pr) = self.tracker.progress.get_mut(&self.cfg.id)
            {
                pr.maybe_update(self.log.persisted());
                if self.maybe_commit() {
                    self.bcast_append();
                }
            }
        }

        if let Some(applied) = ack.applied {
            self.apply_to(applied);
        }
    }

    fn apply_to(&mut self, applied: Index) {
        let from = self.log.applied() + 1;
        let to = applied.min(self.log.committed());
        if to < from {
            return;
        }
        let changes: Vec<(Index, ConfChangeV2)> = (from..=to)
            .filter_map(|i| self.log.entry(i))
            .filter_map(|e| match &e.payload {
                EntryPayload::ConfChange(cc) => Some((e.index, cc.clone())),
                _ => None,
            })
            .collect();

        let released: usize = (from..=to)
            .filter_map(|i| self.log.entry(i))
            .map(Entry::payload_len)
            .sum();
        self.uncommitted_bytes = self.uncommitted_bytes.saturating_sub(released);
        self.log.set_applied(to);

        for (_index, cc) in changes {
            let Ok(next) = confchange::apply(self.tracker.conf(), &cc) else {
                continue;
            };
            let leaving_joint = cc.is_leave_joint();
            self.tracker.set_conf(next, self.log.last_index() + 1);

            if self.role != Role::Leader {
                continue;
            }
            let conf = self.tracker.conf().clone();
            if conf.auto_leave && !leaving_joint {
                // Finish the change without waiting for an operator: the joint
                // configuration is a transition, not a resting state.
                let index = self.log.last_index() + 1;
                self.log.append([Entry::new(
                    self.term,
                    index,
                    EntryPayload::ConfChange(ConfChangeV2::leave_joint()),
                )]);
                self.pending_conf_index = index;
                self.bcast_append();
            } else if !conf.voters.contains(&self.cfg.id) {
                // We removed ourselves. Step down now that C_new is committed;
                // staying on would mean leading a cluster we are not part of.
                self.become_follower(self.term, None);
                return;
            }
        }
    }
}
