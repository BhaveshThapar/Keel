use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::path::PathBuf;

use bytes::Bytes;
use keel_log::{Log, LogOptions, SyncMode};
use keel_raft::{
    Advance, ConfState, Config, Entry, Index, Input, Message, NodeId, RaftCore, Ready, Restored,
    Role, SnapshotMeta, Term,
};

use crate::digest::LogDigest;
use crate::faultfs::{FaultFs, FaultStats, TearPolicy};
use crate::invariants::{Oracle, Violation};
use crate::network::{Delivery, NetConfig, Network};
use keel_rand::Rng;

const TRACE_DEPTH: usize = 64;

#[derive(Debug, Clone)]
pub struct SimConfig {
    pub nodes: usize,
    pub clients: usize,
    pub tick_ns: u64,
    /// Per-node tick period is drawn from `tick_ns * (100 ± clock_skew_pct)/100`,
    /// so nodes disagree about how fast time passes.
    pub clock_skew_pct: u64,
    pub fsync_min_ns: u64,
    pub fsync_max_ns: u64,
    pub client_period_ns: u64,
    pub nemesis_period_ns: u64,
    pub restart_delay_ns: u64,
    pub net: NetConfig,
    pub election_tick: u32,
    pub heartbeat_tick: u32,
    /// Entries per AppendEntries. Small values are not a performance setting —
    /// they split a leader's log across many messages, which is what makes a
    /// partially-replicated term boundary reachable, and that boundary is
    /// precisely where the Figure 8 hazard lives.
    pub max_entries_per_msg: usize,
    pub max_inflight_msgs: usize,
    pub pre_vote: bool,
    pub check_quorum: bool,
    pub disable_fig8_guard: bool,
    /// Leave a torn tail on disk instead of zeroing it. The rule [KEEL-7]
    /// corrected; removing it is how the harness is shown to catch that class.
    ///
    /// [KEEL-7]: https://github.com/BhaveshThapar/Keel/blob/main/BUGS.md
    pub skip_tail_erase: bool,
    /// Accept a record whose checksum does not match.
    pub skip_record_crc: bool,
    /// What a crash does to bytes no fsync covered.
    ///
    /// The default lands no sectors, so a crash takes every staged write back
    /// whole. Turning tearing on is what the `disk-*` profiles are for, and it
    /// only bites when the unsynced region spans a sector boundary — see
    /// [`SimConfig::segment_bytes`].
    pub tear: TearPolicy,
    /// Size each log segment is preallocated to.
    ///
    /// Small, so rollover and multi-segment recovery are reached in a run
    /// rather than only in principle. It also has to exceed `tear.sector_bytes`
    /// by enough for a write to straddle a boundary: a segment inside one
    /// sector has every offset in that sector, so exactly one draw is made and
    /// tearing is impossible rather than merely unlikely.
    pub segment_bytes: u64,
    pub max_record_bytes: u32,
    /// Pad each client proposal out to at least this many bytes.
    ///
    /// A write only tears when it straddles a sector boundary, and the chance
    /// of that is the record's length over the sector size — so the size of a
    /// proposal is the most direct lever the tear model has. Zero, the default,
    /// leaves the payload exactly as it was.
    pub proposal_bytes: usize,
    /// Chance that a crash or isolation targets the current leader rather than
    /// a random node. Real chaos tooling aims at the leader for a reason: the
    /// windows worth testing are the ones around a leadership change, and
    /// uniform random faults reach them only by luck.
    pub target_leader_pct: u32,
    /// Aim crashes and isolations at a node that has bytes on its disk no fsync
    /// has covered yet.
    pub aim_at_writes_in_flight: bool,
    /// Crash a leader the moment it commits an earlier term's entry on replica
    /// count alone. Uniform random faults reach that one-message-wide window
    /// only by luck; aiming at it directly is how a negative demonstration
    /// shows what the Figure 8 rule is actually preventing.
    pub kill_leader_on_fig8_bypass: bool,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            nodes: 5,
            clients: 4,
            tick_ns: 20_000_000,
            clock_skew_pct: 5,
            fsync_min_ns: 300_000,
            fsync_max_ns: 3_000_000,
            client_period_ns: 5_000_000,
            nemesis_period_ns: 400_000_000,
            restart_delay_ns: 300_000_000,
            net: NetConfig::default(),
            election_tick: 10,
            heartbeat_tick: 2,
            max_entries_per_msg: 4,
            max_inflight_msgs: 8,
            pre_vote: true,
            check_quorum: true,
            disable_fig8_guard: false,
            skip_tail_erase: false,
            skip_record_crc: false,
            tear: TearPolicy::default(),
            segment_bytes: 8 << 10,
            max_record_bytes: 4 << 10,
            proposal_bytes: 0,
            aim_at_writes_in_flight: false,
            target_leader_pct: 60,
            kill_leader_on_fig8_bypass: false,
        }
    }
}

impl SimConfig {
    /// How each node opens its log.
    ///
    /// `SyncMode::Durable` is not cosmetic: [`FaultFs`] retires a write only on
    /// a durable sync, so a log configured for `None` would never make anything
    /// durable and every crash would take everything.
    pub fn log_options(&self) -> LogOptions {
        LogOptions {
            segment_bytes: self.segment_bytes,
            max_record_bytes: self.max_record_bytes,
            sync_mode: SyncMode::Durable,
            preallocate: true,
            unsafe_skip_tail_erase: self.skip_tail_erase,
            unsafe_skip_record_crc: self.skip_record_crc,
        }
    }

    /// Heavier faults and smaller messages. Progress is slower per event, but
    /// the states that only appear under partial replication and rapid
    /// leadership change are reachable, and those are the ones worth checking.
    pub fn chaos(nodes: usize) -> Self {
        Self {
            nodes,
            clients: 6,
            client_period_ns: 2_000_000,
            nemesis_period_ns: 60_000_000,
            restart_delay_ns: 80_000_000,
            max_entries_per_msg: 1,
            max_inflight_msgs: 2,
            clock_skew_pct: 15,
            target_leader_pct: 85,
            net: NetConfig {
                drop_pct: 12,
                duplicate_pct: 5,
                straggle_pct: 8,
                ..NetConfig::default()
            },
            ..Self::default()
        }
    }

    /// Tuned for the window the Figure 8 rule guards: a leader that has just
    /// committed an earlier term's entry and then dies before its own term's
    /// entry commits. That window is one message round wide, so faults have to
    /// arrive at message frequency rather than at election frequency.
    pub fn fig8_hunt(nodes: usize) -> Self {
        Self {
            nodes,
            clients: 4,
            client_period_ns: 1_000_000,
            nemesis_period_ns: 25_000_000,
            restart_delay_ns: 40_000_000,
            max_entries_per_msg: 1,
            max_inflight_msgs: 1,
            election_tick: 6,
            heartbeat_tick: 1,
            target_leader_pct: 90,
            clock_skew_pct: 20,
            kill_leader_on_fig8_bypass: true,
            net: NetConfig {
                drop_pct: 6,
                duplicate_pct: 4,
                straggle_pct: 10,
                ..NetConfig::default()
            },
            ..Self::default()
        }
    }

    /// Faults aimed at the disk rather than at the network.
    ///
    /// The sector size is the one modern hardware has, which makes this the
    /// faithful axis — and the harder one to tear on, since a write only tears
    /// when it straddles a boundary. What makes it reachable is the size of the
    /// record: the chance a write of `L` bytes crosses a boundary of `S` is
    /// `(L - 1) / S`, so a default 23-byte record against a 4 KiB sector tears
    /// about once in two hundred, and a 1 KiB one about once in four.
    pub fn disk_chaos(nodes: usize) -> Self {
        Self {
            tear: TearPolicy {
                sector_bytes: 4096,
                sector_lands_pct: 50,
            },
            segment_bytes: 64 << 10,
            max_record_bytes: 8 << 10,
            // The lever is the record, not the schedule. A 1 KiB proposal makes
            // a frame of about 1050 bytes, which straddles a 4 KiB boundary
            // roughly a quarter of the time; shortening the client period
            // instead would only fill the event budget with proposals and starve
            // the nemesis that has to crash a node for any of it to matter.
            proposal_bytes: 1 << 10,
            aim_at_writes_in_flight: true,
            // Slow fsyncs against unchanged client traffic, so the unsynced
            // window is open most of the time rather than a fraction of it. A
            // slower fsync costs no extra events — there is one writer pass
            // however long it takes — whereas speeding the clients up would
            // fill the event budget with proposals and starve the nemesis that
            // has to crash a node for any of it to matter.
            fsync_min_ns: 2_000_000,
            fsync_max_ns: 12_000_000,
            ..Self::chaos(nodes)
        }
    }

    /// The same, at the sector size a write is most likely to straddle.
    ///
    /// 512 bytes is eight times likelier to cut a given write than 4096, so
    /// this is where the sub-record shapes live and where a tear is cheap to
    /// reach. Smaller segments too, so rollover and multi-segment recovery are
    /// crossed often rather than occasionally.
    pub fn disk_hunt(nodes: usize) -> Self {
        Self {
            tear: TearPolicy {
                sector_bytes: 512,
                sector_lands_pct: 40,
            },
            segment_bytes: 8 << 10,
            max_record_bytes: 4 << 10,
            // A 256-byte proposal against a 512-byte sector straddles a
            // boundary about half the time.
            proposal_bytes: 256,
            restart_delay_ns: 40_000_000,
            ..Self::disk_chaos(nodes)
        }
    }

    /// Every profile `named` accepts. Kept next to it so an error message
    /// listing the choices cannot drift from the choices themselves, and a
    /// slice rather than a fixed-size array so adding one is a single edit
    /// that cannot leave the length behind.
    pub const PROFILES: &'static [&'static str] =
        &["default", "chaos", "fig8-hunt", "disk-chaos", "disk-hunt"];

    pub fn named(name: &str, nodes: usize) -> Option<Self> {
        match name {
            "default" => Some(Self {
                nodes,
                ..Self::default()
            }),
            "chaos" => Some(Self::chaos(nodes)),
            "fig8-hunt" => Some(Self::fig8_hunt(nodes)),
            "disk-chaos" => Some(Self::disk_chaos(nodes)),
            "disk-hunt" => Some(Self::disk_hunt(nodes)),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
enum Event {
    Tick(NodeId),
    Deliver(Message),
    /// One `Ready`'s fsync completed. Only now may its messages go out and its
    /// committed entries be applied — the persist-before-send contract,
    /// enforced by the simulation itself rather than trusted.
    ///
    /// Carries the node's crash generation, so an fsync issued before a crash
    /// cannot be believed by the incarnation that replaced it.
    Fsync {
        node: NodeId,
        epoch: u64,
        batch: Box<FsyncBatch>,
    },
    Client(usize),
    Nemesis,
    Restart(NodeId),
}

/// What one `Ready` still owes once its writes are on the disk but not yet
/// durable: the watermark to report, the messages to release, and the entries to
/// apply. All three wait for the fsync.
#[derive(Debug, Clone)]
struct FsyncBatch {
    ready_number: u64,
    persisted: Option<(Index, Term)>,
    snapshot: Option<SnapshotMeta>,
    messages: Vec<Message>,
    committed: Vec<Entry>,
}

#[derive(Debug)]
struct Scheduled {
    at: u64,
    seq: u64,
    event: Event,
}

impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}
impl Eq for Scheduled {}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Ordering is total and depends only on (time, insertion order), never
        // on the event's contents. Two runs of the same seed therefore process
        // events in exactly the same order.
        (self.at, self.seq).cmp(&(other.at, other.seq))
    }
}

struct SimNode {
    id: NodeId,
    core: RaftCore,
    /// The node's disk. Held here as well as by the `Log`, because it has to
    /// outlive the crash that drops one: cloning gives another handle on the
    /// same bytes.
    fs: FaultFs,
    dir: PathBuf,
    /// `None` while the node is dead, so the lock file is released the way a
    /// killed process releases its flock.
    log: Option<Log<FaultFs>>,
    digest: LogDigest,
    alive: bool,
    /// Bumped on every crash so that fsyncs scheduled before it are discarded.
    epoch: u64,
    tick_period: u64,
    applied_count: usize,
    was_leader: bool,
    fsync_rng: Rng,
}

#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub events: u64,
    pub messages_sent: u64,
    pub messages_dropped: u64,
    pub elections: u64,
    pub crashes: u64,
    pub partitions: u64,
    pub proposals: u64,
    pub committed: Index,
    pub applied: Index,

    // Coverage. A simulator that never reaches the states the safety rules
    // guard is a simulator that proves nothing, so the run reports how often it
    // got there.
    /// Times a node discarded log entries a leader had overwritten.
    pub entries_rewritten: u64,
    /// Times a leader's commit index landed on an entry from an earlier term —
    /// the exact window the Figure 8 rule exists to police.
    pub old_term_commit_windows: u64,
    /// Distinct terms that produced a leader.
    pub terms_with_leaders: u64,
    /// Times a leader committed an earlier term's entry on replica count alone.
    /// Zero unless the Figure 8 guard was compiled out.
    pub fig8_bypasses: u64,

    // The disk. A fault model that never fired proves nothing, and the sizing
    // arithmetic says a badly configured one can be provably inert, so what it
    // did is reported rather than assumed.
    /// Restarts where the real parser found bytes written above the recovery
    /// cursor and discarded them. Zero means no crash ever caught a write in
    /// flight, whatever the tear policy says.
    pub torn_tails: u64,
    /// How much those tears actually cost.
    pub bytes_discarded_by_tears: u64,
    /// Restarts where a durable commit index had to be clamped because the
    /// entries it named went with the tail.
    pub commits_clamped: u64,
    /// Segments seen by a recovery, summed. More than one restart's worth means
    /// the multi-segment recovery path was actually reached.
    pub segments_recovered: u64,
    /// Crashes that tore a node's log while that node was inside a partition.
    ///
    /// The counter the durability claim turns on. It is not enough that tears
    /// happen and partitions happen; what has to be shown is that they met.
    pub tears_during_partition: u64,
}

pub struct World {
    cfg: SimConfig,
    pub seed: u64,
    now: u64,
    seq: u64,
    queue: BinaryHeap<Reverse<Scheduled>>,
    nodes: BTreeMap<NodeId, SimNode>,
    net: Network,
    nemesis_rng: Rng,
    workload_rng: Rng,
    oracle: Oracle,
    trace: VecDeque<String>,
    next_ctx: u64,
    pub stats: Stats,
    pub violations: Vec<Violation>,
}

/// Write one `Ready` through the real log and make it durable, in the order the
/// host loop documents: truncate what history replaced, append, then the hard
/// state, then exactly one sync covering all three.
///
/// A `Ready` carries no `first_new`, so the truncation is decided by comparing
/// the batch's first index against what the log already holds. Without that,
/// `Log::append` refuses with `Discontiguous` the first time a leader overwrites
/// a divergent tail.
fn stage(node: &mut SimNode, rd: &Ready) -> Result<(), String> {
    let Some(log) = node.log.as_mut() else {
        return Err("a live node has no open log".into());
    };
    let step = |r: keel_log::Result<keel_log::SyncToken>| r.map(|_| ()).map_err(|e| e.to_string());

    if let Some(meta) = &rd.snapshot_to_install {
        step(log.install_snapshot(meta))?;
    }
    if let Some(first) = rd.entries.first().map(|e| e.index)
        && first <= log.last_index()
    {
        step(log.truncate(first))?;
    }
    step(log.append(&rd.entries))?;
    if let Some(hs) = rd.hard_state {
        step(log.set_hard_state(hs))?;
    }
    Ok(())
}

/// Make everything written so far durable.
///
/// Not just this `Ready`'s writes: an fsync makes durable what had been written
/// when it was issued, and saying otherwise is the model error KEEL-5 was. So a
/// `Ready` staged behind this one and ahead of this fsync is covered by it, and
/// the watermark reported to the core is still only this batch's — conservative
/// in the safe direction.
fn sync(node: &mut SimNode) -> Result<(), String> {
    let Some(log) = node.log.as_mut() else {
        return Err("a live node has no open log".into());
    };
    log.sync().map(|_| ()).map_err(|e| e.to_string())
}

impl World {
    pub fn new(seed: u64, cfg: SimConfig) -> Self {
        let mut root = Rng::new(seed);
        // Streams are derived once, up front, and named. A component added later
        // takes a new name and cannot shift the draws an existing one sees, so
        // old seeds keep reproducing the same run.
        let net_rng = root.split("network");
        let nemesis_rng = root.split("nemesis");
        let workload_rng = root.split("workload");
        let mut node_rng = root.split("nodes");
        // Derived after `nodes`, never as a fourth draw inside the loop below:
        // `node_rng` is one generator consumed sequentially across every node,
        // so a draw added in the loop body would shift node 2's seed, its skew,
        // its fsync stream, and everything after.
        let mut disk_rng = root.split("disks");

        let ids: Vec<NodeId> = (1..=cfg.nodes as NodeId).collect();
        let conf = ConfState::single(ids.iter().copied());

        let mut nodes = BTreeMap::new();
        let mut violations = Vec::new();
        for id in &ids {
            let core_cfg = Config {
                election_tick: cfg.election_tick,
                heartbeat_tick: cfg.heartbeat_tick,
                rng_seed: node_rng.next_u64(),
                pre_vote: cfg.pre_vote,
                check_quorum: cfg.check_quorum,
                unsafe_disable_fig8_guard: cfg.disable_fig8_guard,
                max_entries_per_msg: cfg.max_entries_per_msg,
                max_inflight_msgs: cfg.max_inflight_msgs,
                ..Config::new(*id)
            };
            let skew = cfg.clock_skew_pct.min(50);
            let tick_period = if skew == 0 {
                cfg.tick_ns
            } else {
                let low = cfg.tick_ns * (100 - skew) / 100;
                let high = cfg.tick_ns * (100 + skew) / 100;
                node_rng.range(low, high + 1)
            };
            let fs = FaultFs::tearing(cfg.tear, disk_rng.split("node"));
            let dir = PathBuf::from(format!("/node-{id}"));
            let log = match Log::open(fs.clone(), &dir, cfg.log_options()) {
                Ok((log, _)) => Some(log),
                Err(e) => {
                    violations.push(Violation {
                        property: "a node can open its log",
                        detail: format!("node {id} could not open its log at boot: {e}"),
                    });
                    None
                }
            };
            nodes.insert(
                *id,
                SimNode {
                    id: *id,
                    core: RaftCore::new(core_cfg, conf.clone()),
                    fs,
                    dir,
                    log,
                    digest: LogDigest::new(),
                    alive: true,
                    epoch: 0,
                    tick_period,
                    applied_count: 0,
                    was_leader: false,
                    fsync_rng: node_rng.split("fsync"),
                },
            );
        }

        let mut world = Self {
            net: Network::new(cfg.net.clone(), net_rng),
            cfg,
            seed,
            now: 0,
            seq: 0,
            queue: BinaryHeap::new(),
            nodes,
            nemesis_rng,
            workload_rng,
            oracle: Oracle::new(),
            trace: VecDeque::new(),
            next_ctx: 1,
            stats: Stats::default(),
            violations,
        };

        for id in ids {
            let at = world.nodes[&id].tick_period;
            world.schedule(at, Event::Tick(id));
        }
        for c in 0..world.cfg.clients {
            let at = world.cfg.client_period_ns * (c as u64 + 1);
            world.schedule(at, Event::Client(c));
        }
        world.schedule(world.cfg.nemesis_period_ns, Event::Nemesis);
        world
    }

    fn schedule(&mut self, at: u64, event: Event) {
        self.seq += 1;
        self.queue.push(Reverse(Scheduled {
            at,
            seq: self.seq,
            event,
        }));
    }

    fn schedule_in(&mut self, delay: u64, event: Event) {
        let at = self.now + delay;
        self.schedule(at, event);
    }

    fn trace(&mut self, line: String) {
        if self.trace.len() == TRACE_DEPTH {
            self.trace.pop_front();
        }
        self.trace.push_back(format!("[{:>12}ns] {line}", self.now));
    }

    pub fn now(&self) -> u64 {
        self.now
    }

    pub fn is_broken(&self) -> bool {
        !self.violations.is_empty()
    }

    /// Process one event. Returns false when the schedule runs dry.
    pub fn step(&mut self) -> bool {
        let Some(Reverse(next)) = self.queue.pop() else {
            return false;
        };
        self.now = next.at;
        self.stats.events += 1;

        match next.event {
            Event::Tick(id) => self.on_tick(id),
            Event::Deliver(msg) => self.on_deliver(msg),
            Event::Fsync { node, epoch, batch } => self.on_fsync(node, epoch, *batch),
            Event::Client(c) => self.on_client(c),
            Event::Nemesis => self.on_nemesis(),
            Event::Restart(id) => self.on_restart(id),
        }
        true
    }

    pub fn run(&mut self, steps: u64) -> Option<&Violation> {
        for _ in 0..steps {
            if !self.step() {
                break;
            }
            if self.is_broken() {
                break;
            }
        }
        self.violations.first()
    }

    // ------------------------------------------------------------- handlers

    fn on_tick(&mut self, id: NodeId) {
        let period = self
            .nodes
            .get(&id)
            .map_or(self.cfg.tick_ns, |n| n.tick_period);
        self.schedule_in(period, Event::Tick(id));
        let alive = self.nodes.get(&id).is_some_and(|n| n.alive);
        if !alive {
            return;
        }
        if let Some(node) = self.nodes.get_mut(&id) {
            let _ = node.core.step(Input::Tick);
        }
        self.pump(id);
        self.check(id);
    }

    fn on_deliver(&mut self, msg: Message) {
        let to = msg.to;
        // The partition is applied here, so a partition that forms while a
        // message is in flight still swallows it.
        if self.net.is_cut(msg.from, to) || !self.nodes.get(&to).is_some_and(|n| n.alive) {
            self.stats.messages_dropped += 1;
            return;
        }
        if let Some(node) = self.nodes.get_mut(&to) {
            let _ = node.core.step(Input::Message(msg));
        }
        self.pump(to);
        self.check(to);
    }

    fn on_fsync(&mut self, id: NodeId, epoch: u64, batch: FsyncBatch) {
        let stale = self
            .nodes
            .get(&id)
            .is_none_or(|n| !n.alive || n.epoch != epoch);
        if stale {
            // The node crashed after this write was issued. Its messages never
            // go out and its entries never became durable, which is the whole
            // reason the send is gated on the fsync.
            return;
        }

        // 1. The write is now durable.
        if let Some(node) = self.nodes.get_mut(&id) {
            if let Err(detail) = sync(node) {
                self.violations.push(Violation {
                    property: "a live node can sync its log",
                    detail: format!("node {id}: {detail}"),
                });
                return;
            }
            node.core.advance(Advance {
                ready_number: batch.ready_number,
                persisted: batch.persisted,
                applied: None,
                snapshot_installed: batch.snapshot,
            });
        }

        // 2. Only now may the messages go out.
        for msg in batch.messages {
            self.send(msg);
        }

        // 3. Apply, then report how far.
        if let Some(node) = self.nodes.get_mut(&id) {
            let applied = batch.committed.last().map(|e| e.index);
            node.applied_count += batch.committed.len();
            if applied.is_some() {
                node.core.advance(Advance {
                    ready_number: batch.ready_number,
                    persisted: None,
                    applied,
                    snapshot_installed: None,
                });
            }
        }

        self.pump(id);
        self.check(id);
    }

    fn on_client(&mut self, client: usize) {
        self.schedule_in(self.cfg.client_period_ns, Event::Client(client));

        // A client sends to whichever node it believes is the leader, which is
        // what a real client with redirect hints does.
        let leaders: Vec<NodeId> = self
            .nodes
            .values()
            .filter(|n| n.alive && n.core.role() == Role::Leader)
            .map(|n| n.id)
            .collect();
        let target = match self.workload_rng.pick(&leaders) {
            Some(id) => *id,
            None => return,
        };

        let ctx = self.next_ctx;
        self.next_ctx += 1;
        let mut payload = format!("c{client}:{ctx}").into_bytes();
        // Padded, not replaced, so the identifying prefix a trace shows is
        // still the first thing in the record.
        payload.resize(payload.len().max(self.cfg.proposal_bytes), b'.');
        let payload = Bytes::from(payload);
        if let Some(node) = self.nodes.get_mut(&target) {
            let _ = node.core.step(Input::Propose { ctx, data: payload });
            self.stats.proposals += 1;
        }
        self.pump(target);
        self.check(target);
    }

    fn on_restart(&mut self, id: NodeId) {
        let opts = self.cfg.log_options();
        // Everything the reopen needs, taken before the borrow is dropped, so
        // that recording a violation below does not fight the borrow checker.
        let Some((cfg, conf, fs, dir)) = self.nodes.get(&id).filter(|n| !n.alive).map(|n| {
            (
                n.core.config().clone(),
                n.core.conf().clone(),
                n.fs.clone(),
                n.dir.clone(),
            )
        }) else {
            return;
        };
        // Through the real recovery parser, over whatever the crash left: the
        // torn tail, the erase, the clamped commit, all of it.
        let (log, recovered) = match Log::open(fs, &dir, opts) {
            Ok(pair) => pair,
            Err(e) => {
                // A node that cannot reopen its own log never rejoins, which is
                // worse than losing the tail — so it is a violation and not a
                // panic.
                self.violations.push(Violation {
                    property: "a crash never leaves a log that will not open",
                    detail: format!("node {id} could not reopen its log: {e}"),
                });
                return;
            }
        };
        if recovered.discarded_tail_bytes > 0 {
            self.stats.torn_tails += 1;
            self.stats.bytes_discarded_by_tears += recovered.discarded_tail_bytes;
        }
        if recovered.clamped_commit {
            self.stats.commits_clamped += 1;
        }
        self.stats.segments_recovered += u64::from(recovered.segments);

        let node = match self.nodes.get_mut(&id) {
            Some(node) => node,
            None => return,
        };
        node.core = RaftCore::restore(
            cfg,
            Restored {
                conf,
                hard_state: recovered.hard_state,
                snapshot: recovered.snapshot,
                entries: recovered.entries,
                // Zero because there is no state machine here yet, so nothing
                // has applied anything the log does not already know about.
                // P8 replaces this with what the real state machine reports.
                applied: 0,
            },
        );
        node.log = Some(log);
        node.digest = LogDigest::new();
        node.alive = true;
        node.was_leader = false;
        self.trace(format!("node {id} restarted"));
        self.pump(id);
        self.check(id);
    }

    fn kill(&mut self, id: NodeId) {
        // Read before the crash, while there is still something in flight to
        // count and the cut set still describes the moment.
        let partitioned = self.net.is_partitioned(id);
        if let Some(node) = self.nodes.get_mut(&id)
            && node.alive
        {
            node.alive = false;
            node.epoch += 1;
            // Drop the `Log` before tearing the image. A killed process has its
            // descriptors closed by the kernel, which releases its flock — and
            // holding this handle any longer would let its `Drop` clear the
            // lock the *restarted* node had since taken.
            node.log = None;
            // Whether this crash actually tore is only knowable afterwards, so
            // the counter is a difference rather than a prediction.
            let before = node.fs.fault_stats();
            node.fs.crash();
            let after = node.fs.fault_stats();
            let tore = after.writes_that_landed_head_first > before.writes_that_landed_head_first
                || after.writes_that_landed_tail_first > before.writes_that_landed_tail_first
                || after.writes_that_landed_in_pieces > before.writes_that_landed_in_pieces;
            self.stats.crashes += 1;
            if tore && partitioned {
                self.stats.tears_during_partition += 1;
            }
            self.schedule_in(self.cfg.restart_delay_ns, Event::Restart(id));
        }
    }

    /// Pick a node to disrupt, usually the leader.
    fn victim(&mut self, ids: &[NodeId]) -> Option<NodeId> {
        if self.cfg.aim_at_writes_in_flight {
            let dirty: Vec<NodeId> = self
                .nodes
                .values()
                .filter(|n| n.alive && n.fs.pending_bytes() > 0)
                .map(|n| n.id)
                .collect();
            // A node that is *both* writing and inside a partition first. It is
            // not enough that tears and partitions both occur; the claim is
            // that they met, and a uniform schedule reaches that intersection
            // rarely enough to be luck.
            let cut: Vec<NodeId> = dirty
                .iter()
                .copied()
                .filter(|id| self.net.is_partitioned(*id))
                .collect();
            // `pick` draws nothing from an empty slice, so a moment with no
            // such node falls through without shifting the stream.
            if let Some(id) = self.nemesis_rng.pick(&cut).copied() {
                return Some(id);
            }
            if let Some(id) = self.nemesis_rng.pick(&dirty).copied() {
                return Some(id);
            }
        }
        if self.nemesis_rng.chance(self.cfg.target_leader_pct)
            && let Some(leader) = self
                .nodes
                .values()
                .find(|n| n.alive && n.core.role() == Role::Leader)
                .map(|n| n.id)
        {
            return Some(leader);
        }
        self.nemesis_rng.pick(ids).copied()
    }

    fn on_nemesis(&mut self) {
        self.schedule_in(self.cfg.nemesis_period_ns, Event::Nemesis);
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        if ids.len() < 2 {
            return;
        }
        let quorum = ids.len() / 2 + 1;

        match self.nemesis_rng.range(0, 100) {
            0..=24 => {
                // Split into two groups at a random point.
                let cut = self.nemesis_rng.range(1, ids.len() as u64) as usize;
                let (a, b) = ids.split_at(cut);
                for x in a {
                    for y in b {
                        self.net.cut_both(*x, *y);
                    }
                }
                self.stats.partitions += 1;
                self.trace(format!("partition {a:?} | {b:?}"));
            }
            25..=39 => {
                // One-way link failure: the hardest kind to reason about,
                // because each side has a different view of who is reachable.
                let Some(from) = self.nemesis_rng.pick(&ids).copied() else {
                    return;
                };
                let Some(to) = self.nemesis_rng.pick(&ids).copied() else {
                    return;
                };
                if from != to {
                    self.net.cut_link(from, to);
                    self.stats.partitions += 1;
                    self.trace(format!("one-way cut {from} -> {to}"));
                }
            }
            40..=49 => {
                // Isolate one node entirely.
                let Some(victim) = self.victim(&ids) else {
                    return;
                };
                for other in &ids {
                    if *other != victim {
                        self.net.cut_both(victim, *other);
                    }
                }
                self.stats.partitions += 1;
                self.trace(format!("isolated node {victim}"));
            }
            50..=74 => {
                self.net.heal();
                self.trace("network healed".to_string());
            }
            75..=89 => {
                // Crash a node, but never so many that no quorum can survive:
                // a cluster with no quorum makes no progress, and a run that
                // makes no progress checks nothing.
                let live = self.nodes.values().filter(|n| n.alive).count();
                if live > quorum {
                    let Some(victim) = self.victim(&ids) else {
                        return;
                    };
                    self.kill(victim);
                    self.trace(format!("crashed node {victim}"));
                }
            }
            _ => {
                let dead: Vec<NodeId> = self
                    .nodes
                    .values()
                    .filter(|n| !n.alive)
                    .map(|n| n.id)
                    .collect();
                if let Some(id) = self.nemesis_rng.pick(&dead).copied() {
                    self.on_restart(id);
                }
            }
        }
    }

    // --------------------------------------------------------------- engine

    /// Run one node's host loop: write the `Ready`, then schedule the fsync that
    /// has to precede its messages.
    ///
    /// The writes happen here and the sync happens in the scheduled event, and
    /// the gap between them is the whole point. It is the only interval in which
    /// bytes exist on the disk that no fsync has covered, so it is the only
    /// interval in which a crash has anything to lose or to tear. Doing both in
    /// one event would make the pair atomic in virtual time, and a fault model
    /// with no window is a fault model that never fires.
    ///
    /// Several `Ready`s can be written before the first fsync fires, and the
    /// fsync that does fire retires all of them. That is group commit, and it is
    /// also what makes the unsynced region wide enough to straddle a sector.
    fn pump(&mut self, id: NodeId) {
        loop {
            let Some(node) = self.nodes.get_mut(&id) else {
                return;
            };
            if !node.alive || !node.core.has_ready() {
                return;
            }
            let rd: Ready = node.core.ready();

            let persisted = rd.entries.last().map(|e| (e.index, e.term));
            let needs_sync = !rd.entries.is_empty()
                || rd.hard_state.is_some()
                || rd.snapshot_to_install.is_some();
            if let Err(detail) = stage(node, &rd) {
                self.violations.push(Violation {
                    property: "the log accepts what the core hands it",
                    detail: format!("node {id}: {detail}"),
                });
                return;
            }
            let delay = if needs_sync {
                node.fsync_rng
                    .range(self.cfg.fsync_min_ns, self.cfg.fsync_max_ns + 1)
            } else {
                0
            };
            let epoch = node.epoch;
            self.schedule_in(
                delay,
                Event::Fsync {
                    node: id,
                    epoch,
                    batch: Box::new(FsyncBatch {
                        ready_number: rd.number,
                        persisted,
                        snapshot: rd.snapshot_to_install,
                        messages: rd.messages,
                        committed: rd.committed_entries,
                    }),
                },
            );
        }
    }

    fn send(&mut self, msg: Message) {
        self.stats.messages_sent += 1;
        match self.net.schedule(&msg) {
            Delivery::Dropped => self.stats.messages_dropped += 1,
            Delivery::Once(d) => self.schedule_in(d, Event::Deliver(msg)),
            Delivery::Twice(a, b) => {
                self.schedule_in(a, Event::Deliver(msg.clone()));
                self.schedule_in(b, Event::Deliver(msg));
            }
        }
    }

    /// Check every safety property against this node's new state. Runs after
    /// every event, which is affordable only because the cumulative digests
    /// reduce each property to one comparison.
    fn check(&mut self, id: NodeId) {
        let Some(node) = self.nodes.get_mut(&id) else {
            return;
        };
        if !node.alive {
            return;
        }
        let (changed, discarded) = node.digest.sync(node.core.log());
        let role = node.core.role();
        let term = node.core.term();
        let last_index = node.core.log().last_index();
        let commit = node.core.log().committed();
        let applied = node.core.log().applied();
        let became_leader = role == Role::Leader && !node.was_leader;
        node.was_leader = role == Role::Leader;
        let old_term_window = role == Role::Leader
            && commit > 0
            && node.core.log().term(commit).is_some_and(|t| t < term);
        if !discarded.is_empty() {
            self.stats.entries_rewritten += discarded.len() as u64;
            if let Some(v) = self.oracle.check_rewrite(id, &discarded) {
                let lowest = discarded.iter().map(|(i, _)| *i).min().unwrap_or(0);
                self.trace(format!("node {id} rewrote its log from index {lowest}"));
                self.violations.push(v);
            }
        }
        if old_term_window {
            self.stats.old_term_commit_windows += 1;
        }

        let mut found: Vec<Violation> = Vec::new();

        if became_leader {
            self.stats.elections += 1;
            let digest = &self.nodes[&id].digest;
            if let Some(v) = self.oracle.check_leader_completeness(id, term, digest) {
                found.push(v);
            }
        }
        if let Some(v) = self.oracle.observe_entries(id, &changed) {
            found.push(v);
        }
        if role == Role::Leader {
            if let Some(v) = self.oracle.observe_leader(id, term) {
                found.push(v);
            }
            if let Some(v) = self.oracle.observe_leader_log(id, term, last_index) {
                found.push(v);
            }
        }
        let digest = self.nodes[&id].digest.clone();
        if let Some(v) = self.oracle.observe_commit(id, commit, &digest) {
            found.push(v);
        }
        if let Some(v) = self.oracle.observe_applied(id, applied, &digest) {
            found.push(v);
        }

        self.stats.committed = self.oracle.max_committed;
        self.stats.applied = self.oracle.max_applied;
        self.stats.terms_with_leaders = self.oracle.terms_with_leaders();
        let bypasses: u64 = self
            .nodes
            .values()
            .map(|n| n.core.status().fig8_bypasses)
            .sum();
        if self.cfg.kill_leader_on_fig8_bypass
            && bypasses > self.stats.fig8_bypasses
            && role == Role::Leader
        {
            self.kill(id);
            self.trace(format!("struck leader {id} inside the Figure 8 window"));
        }
        self.stats.fig8_bypasses = bypasses;
        if !found.is_empty() {
            self.trace(format!(
                "node {id} role={role:?} term={term} last={last_index} commit={commit} applied={applied}"
            ));
            self.violations.extend(found);
        }
    }

    // ----------------------------------------------------------- reporting

    /// A failure report the reader can act on: the seed to replay, what broke,
    /// and the events immediately before it.
    pub fn failure_report(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "seed {} failed after {} events\n",
            self.seed, self.stats.events
        ));
        for v in &self.violations {
            out.push_str(&format!("  violation: {v}\n"));
        }
        out.push_str(&format!(
            "  reproduce with: keel-sim repro --seed {} --nodes {}\n",
            self.seed, self.cfg.nodes
        ));
        out.push_str("  recent events:\n");
        for line in &self.trace {
            out.push_str(&format!("    {line}\n"));
        }
        out.push_str("  node state:\n");
        for node in self.nodes.values() {
            let s = node.core.status();
            out.push_str(&format!(
                "    node {} alive={} role={:?} term={} last={} commit={} applied={}\n",
                s.id, node.alive, s.role, s.term, s.last_index, s.commit, s.applied
            ));
        }
        out
    }

    /// A digest of the whole world, used to prove two runs of one seed are
    /// identical.
    pub fn fingerprint(&self) -> u64 {
        let mut h: u64 = 0xCBF2_9CE4_8422_2325;
        let mut mix = |v: u64| {
            h ^= v;
            h = h.wrapping_mul(0x0000_0100_0000_01B3);
        };
        mix(self.now);
        mix(self.seq);
        mix(self.stats.events);
        mix(self.stats.messages_sent);
        mix(self.stats.messages_dropped);
        for node in self.nodes.values() {
            let s = node.core.status();
            mix(s.id);
            mix(s.term);
            mix(s.last_index);
            mix(s.commit);
            mix(s.applied);
            mix(node.applied_count as u64);
            mix(u64::from(node.alive));
            // The disk is inside the fingerprint, or the determinism gate
            // cannot see it and a nondeterministic tear replays as identical.
            mix(node.fs.durable_digest());
            if let Some((_, d)) = node.digest.at(node.digest.last_index()) {
                mix(d);
            }
        }
        h
    }

    /// What every node's disk did, summed. The tear model's own coverage, as
    /// distinct from what recovery then made of it.
    pub fn disk_stats(&self) -> FaultStats {
        let mut total = FaultStats::default();
        for node in self.nodes.values() {
            let s = node.fs.fault_stats();
            total.crashes += s.crashes;
            total.crashes_with_writes_in_flight += s.crashes_with_writes_in_flight;
            total.bytes_in_flight_at_crash += s.bytes_in_flight_at_crash;
            total.sectors_that_reached_the_device += s.sectors_that_reached_the_device;
            total.sectors_the_crash_took_back += s.sectors_the_crash_took_back;
            total.writes_lost_whole += s.writes_lost_whole;
            total.writes_that_landed_whole += s.writes_that_landed_whole;
            total.writes_that_landed_head_first += s.writes_that_landed_head_first;
            total.writes_that_landed_tail_first += s.writes_that_landed_tail_first;
            total.writes_that_landed_in_pieces += s.writes_that_landed_in_pieces;
            total.files_a_crash_left_a_hole_in += s.files_a_crash_left_a_hole_in;
            total.allocations_a_crash_took_back += s.allocations_a_crash_took_back;
        }
        total
    }

    pub fn live_nodes(&self) -> usize {
        self.nodes.values().filter(|n| n.alive).count()
    }
}
