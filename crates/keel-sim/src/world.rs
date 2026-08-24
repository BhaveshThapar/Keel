use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::path::PathBuf;

use bytes::Bytes;
use keel_log::{Log, LogOptions, SyncMode};
use keel_raft::{
    Advance, ChangeKind, ConfChangeSingle, ConfChangeV2, ConfState, Config, Entry, EntryPayload,
    Index, Input, Message, NodeId, RaftCore, ReadOnlyOption, Ready, Restored, Role, SnapshotMeta,
    Term,
};

use keel_api::{Command, Proposal, ProposalBody, Response, decode, encode};
use keel_sm::{MemStore, StateMachine};

use crate::digest::LogDigest;
use crate::faultfs::{FaultFs, FaultStats, TearPolicy};
use crate::invariants::state_digest;
use crate::invariants::{Oracle, Violation};
use crate::network::{Delivery, NetConfig, Network};
use keel_rand::Rng;

const TRACE_DEPTH: usize = 64;

/// How often the nemesis reaches for each kind of fault.
///
/// A weight table rather than the literal `0..=24 => split, 25..=39 => cut`
/// ranges this replaced, because those ranges were a decision nobody could
/// change per profile without rewriting the match — and a profile that wants to
/// hunt a particular interleaving usually wants a different mix, not a different
/// set of faults.
///
/// The order of the fields is the order they are drawn in, and it is
/// load-bearing: the roll is a single draw compared against running totals, so
/// reordering the fields changes which action a given roll selects and moves
/// every seed's fingerprint. The defaults reproduce the ranges they replaced
/// exactly, which is why the six committed profiles' fingerprints are unmoved by
/// this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NemesisWeights {
    /// Cut the cluster in two at a random point.
    pub split: u32,
    /// A one-way link failure — the hardest kind to reason about, because each
    /// side has a different view of who is reachable.
    pub one_way: u32,
    /// Cut one node off from everybody.
    pub isolate: u32,
    /// Repair everything.
    pub heal: u32,
    /// Crash a node.
    pub crash: u32,
    /// Restart one that is down.
    pub restart: u32,
}

impl Default for NemesisWeights {
    fn default() -> Self {
        // Exactly the ranges this replaced: 0..=24, 25..=39, 40..=49, 50..=74,
        // 75..=89, and everything else. Changing any of these moves every
        // pinned fingerprint, which is the point of writing them down.
        Self {
            split: 25,
            one_way: 15,
            isolate: 10,
            heal: 25,
            crash: 15,
            restart: 10,
        }
    }
}

/// One kind of fault, chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NemesisAction {
    Split,
    OneWay,
    Isolate,
    Heal,
    Crash,
    Restart,
}

impl NemesisWeights {
    pub fn total(&self) -> u32 {
        self.split + self.one_way + self.isolate + self.heal + self.crash + self.restart
    }

    /// Which action a roll in `0..total()` selects.
    ///
    /// Separated from the drawing so it can be tested exhaustively without a
    /// simulator: the property that matters is that every roll below the total
    /// maps to some action and that the boundaries fall where the weights say.
    pub fn action_for(&self, roll: u32) -> NemesisAction {
        let mut bound = self.split;
        if roll < bound {
            return NemesisAction::Split;
        }
        bound += self.one_way;
        if roll < bound {
            return NemesisAction::OneWay;
        }
        bound += self.isolate;
        if roll < bound {
            return NemesisAction::Isolate;
        }
        bound += self.heal;
        if roll < bound {
            return NemesisAction::Heal;
        }
        bound += self.crash;
        if roll < bound {
            return NemesisAction::Crash;
        }
        // Everything above the last boundary, so a roll can never fall off the
        // end even if the weights do not sum to the roll's range.
        NemesisAction::Restart
    }
}

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
    /// Apply committed entries in the order their fsyncs completed rather than
    /// in index order. That is ADR-016's ordering removed, and it is what the
    /// model oracle exists to catch: watermarks are maxima and do not notice,
    /// but an entry handed to a state machine below its watermark is skipped
    /// and its effect is lost. Requires `--features negative-demos`.
    pub skip_apply_ordering: bool,
    /// Take a checkpoint every this many applied entries. Zero never
    /// checkpoints, which is what every profile did before P16.
    pub entries_between_checkpoints: Index,
    /// How much of a snapshot goes in one chunk.
    pub snapshot_chunk_bytes: usize,
    /// Chance that a snapshot chunk is dropped, so a stream is interrupted and
    /// has to resume. A transfer that never breaks tests the happy path and
    /// reports on the rest.
    pub snapshot_chunk_loss_pct: u32,
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
    /// How often the nemesis reaches for each kind of fault.
    pub nemesis_weights: NemesisWeights,
    /// How far a node's tick period may wander *during* a run, as a percentage
    /// of its own period, redrawn on every tick.
    ///
    /// Distinct from `clock_skew_pct`, which draws one period per node at
    /// construction and then never moves it. A fixed skew models machines whose
    /// crystals differ; drift models a machine whose clock is being corrected,
    /// descheduled, or run inside a hypervisor that stopped it — and it is drift
    /// rather than skew that makes an election timeout fire early once and late
    /// the next time, which is the interleaving a fixed offset can never
    /// produce.
    ///
    /// Zero draws nothing at all, which is what keeps the committed profiles'
    /// fingerprints where they are.
    pub clock_drift_pct: u64,
    /// How many of the cluster's nodes start as voters. The rest start as
    /// learners, and a membership change is what promotes them.
    ///
    /// Zero means every node is a voter, which is what every profile before
    /// P23 did. A membership change needs somewhere to change *to*, and the
    /// simulator has no way to conjure a process that was not there at boot —
    /// so the spare capacity has to exist from the start, sitting as learners.
    pub initial_voters: usize,
    /// How often a client event proposes a membership change instead of a
    /// command. Zero draws nothing.
    pub conf_change_pct: u32,
    /// How often a client event asks the leader to hand leadership to somebody
    /// else. Zero draws nothing.
    pub transfer_pct: u32,
    /// Give node 1 the slowest clock in the cluster and everybody else the
    /// fastest, instead of drawing each node's skew independently.
    ///
    /// [ADR-007]'s reasoning applied to clocks. The lease hazard needs a
    /// conjunction — the leader's clock slower than its followers', the leader
    /// cut off, a follower's election timeout expiring while the leader's lease
    /// still holds, and a client reading from the old leader inside that gap —
    /// and independent draws reach it about as often as they reach anything
    /// that needs four coincidences. Measured, a uniform schedule served
    /// **zero** lease reads with a rival leader in twenty seeds, so a build
    /// with leases and a build without were indistinguishable.
    ///
    /// Node 1 is chosen because it wins the first election in most runs, so the
    /// slow clock lands on the leader without anything having to aim at it
    /// afterwards.
    ///
    /// [ADR-007]: https://github.com/BhaveshThapar/Keel/blob/main/DESIGN.md
    pub slowest_node_first: bool,
    /// Serve reads from the leader's lease instead of confirming each one with
    /// a heartbeat round, assuming clock drift between nodes stays under this
    /// percentage.
    ///
    /// `None` is `ReadOnlyOption::ReadIndex`, which is safe under any clock
    /// behaviour. `Some(bound)` is the faster path and is correct *only while
    /// the assumption holds* — which is a statement about the deployment, not
    /// about the algorithm, and is therefore the one thing in the read path a
    /// demonstration can falsify by breaking the deployment instead of the
    /// code.
    pub lease_read_drift_bound: Option<u8>,
    /// What fraction of client operations are linearizable reads rather than
    /// writes.
    ///
    /// Zero draws nothing, for the same reason. A run with no reads cannot
    /// violate read recency and cannot demonstrate that it holds, so this is
    /// the switch that makes the recency oracle mean anything.
    pub read_pct: u32,
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
            skip_apply_ordering: false,
            entries_between_checkpoints: 0,
            snapshot_chunk_bytes: 4096,
            snapshot_chunk_loss_pct: 0,
            tear: TearPolicy::default(),
            segment_bytes: 8 << 10,
            max_record_bytes: 4 << 10,
            proposal_bytes: 0,
            aim_at_writes_in_flight: false,
            target_leader_pct: 60,
            kill_leader_on_fig8_bypass: false,
            nemesis_weights: NemesisWeights::default(),
            clock_drift_pct: 0,
            read_pct: 0,
            lease_read_drift_bound: None,
            slowest_node_first: false,
            initial_voters: 0,
            conf_change_pct: 0,
            transfer_pct: 0,
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

    /// Snapshots, and a stream that keeps breaking.
    ///
    /// Checkpoints often enough that the log floor really moves, chunks small
    /// enough that a transfer is many of them, and enough chunk loss that a
    /// stream is interrupted and has to resume. A profile where every transfer
    /// completes first time tests the happy path and reports on the rest —
    /// which is [KEEL-4](BUGS.md)'s lesson applied to snapshots.
    pub fn snapshot_hunt(nodes: usize) -> Self {
        Self {
            entries_between_checkpoints: 40,
            snapshot_chunk_bytes: 512,
            snapshot_chunk_loss_pct: 30,
            // A follower that is cut off long enough falls behind the floor,
            // which is the only way to be offered a snapshot at all.
            restart_delay_ns: 200_000_000,
            ..Self::chaos(nodes)
        }
    }

    /// Reads under a wandering clock, and a nemesis aimed at leadership.
    ///
    /// Three things at once, because the read hazard needs all three. Reads
    /// have to be issued at all; the cluster has to keep changing leader, since
    /// a read is only interesting when the node answering it might not be
    /// leader any more; and the clock has to drift, because a lease or an
    /// election timeout that is uniformly wrong is wrong in one direction and a
    /// wandering one is wrong in both.
    ///
    /// The weight table is why this is a profile and not a flag. Healing is
    /// worth more here than in `chaos` — a permanently broken cluster commits
    /// nothing, and a read against a cluster with nothing committed cannot be
    /// stale — and crashes are worth more than partitions, because a crash is
    /// what makes a node's applied index fall behind the index a read was
    /// confirmed at.
    pub fn read_hunt(nodes: usize) -> Self {
        Self {
            read_pct: 40,
            // Wander by a fifth of a period, on top of the fixed skew `chaos`
            // already gives each node.
            clock_drift_pct: 20,
            nemesis_weights: NemesisWeights {
                split: 15,
                one_way: 10,
                isolate: 10,
                heal: 35,
                crash: 20,
                restart: 10,
            },
            ..Self::chaos(nodes)
        }
    }

    /// A calm cluster whose leader has the slowest clock, cut off and restored.
    ///
    /// Built for one window and nothing else: a leader that still holds a
    /// lease while somebody else has already replaced it. Every setting here is
    /// aimed at the conjunction that window needs.
    ///
    /// *Calm*, because a lease is only issued to a leader whose own term no-op
    /// has committed, and under `chaos` that almost never happens — measured,
    /// 35 of 3,633 reads were confirmed at all, so the lease path was barely
    /// reached and a run over it said nothing about leases.
    ///
    /// *Slowest node first*, because the hazard is the leader's clock running
    /// slower than its followers': its lease is counted in its own ticks and
    /// their election timeout in theirs.
    ///
    /// *Isolate and heal, aimed at the leader*, because the rival has to be
    /// elected while the old leader is still counting down a lease it can no
    /// longer refresh — which means cutting the leader off and leaving the
    /// others able to talk.
    ///
    /// With `ReadOnlyOption::ReadIndex` this profile is safe and sweeps clean,
    /// which is what makes it usable as the control arm of the lease
    /// demonstration.
    pub fn lease_drift(nodes: usize) -> Self {
        Self {
            read_pct: 40,
            clock_skew_pct: 50,
            slowest_node_first: true,
            target_leader_pct: 100,
            nemesis_period_ns: 150_000_000,
            nemesis_weights: NemesisWeights {
                split: 5,
                one_way: 0,
                isolate: 45,
                heal: 50,
                crash: 0,
                restart: 0,
            },
            nodes,
            ..Self::default()
        }
    }

    /// Membership changes and leader transfers under a fault schedule.
    ///
    /// Closes a gap that was real and stated: `Input::ProposeConfChange` and
    /// `Input::TransferLeader` existed in the core and appeared nowhere in the
    /// simulator, so every membership property rested on an in-process cluster
    /// whose own doc comment admits FIFO messages, instantaneous persistence
    /// and no clock. Joint consensus is the one place where getting a quorum
    /// wrong elects two leaders, and it was the one place the simulator had
    /// never been.
    ///
    /// Two of the three voters start as learners so there is somewhere to
    /// change *to*. A simulated cluster cannot start a process that was not in
    /// the seed, so the spare capacity has to exist at boot and sit unpromoted.
    ///
    /// Calmer than `chaos` on purpose. A membership change needs to commit for
    /// the configuration to move, and a cluster that never commits stays in the
    /// configuration it booted with however many changes are proposed at it.
    ///
    /// **This profile does nothing at three nodes, and that is not a bug to
    /// fix.** A change needs somewhere to change to, the voter floor is three
    /// because a cluster of two stops on a single crash, and a simulated
    /// cluster cannot start a process that was not in the seed — so three nodes
    /// means three voters, no learners, and nothing that can legally move. The
    /// sweep runs it at five, and a test asserts the inertness at three so it
    /// reads as a stated fact rather than as coverage that quietly is not
    /// there.
    pub fn membership_hunt(nodes: usize) -> Self {
        Self {
            initial_voters: 3,
            conf_change_pct: 12,
            transfer_pct: 4,
            nemesis_period_ns: 250_000_000,
            nemesis_weights: NemesisWeights {
                split: 10,
                one_way: 5,
                isolate: 15,
                heal: 45,
                crash: 15,
                restart: 10,
            },
            nodes,
            ..Self::default()
        }
    }

    /// Every profile `named` accepts. Kept next to it so an error message
    /// listing the choices cannot drift from the choices themselves, and a
    /// slice rather than a fixed-size array so adding one is a single edit
    /// that cannot leave the length behind.
    pub const PROFILES: &'static [&'static str] = &[
        "default",
        "chaos",
        "fig8-hunt",
        "disk-chaos",
        "disk-hunt",
        "read-hunt",
        "lease-drift",
        "membership-hunt",
        "snapshot-hunt",
    ];

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
            "snapshot-hunt" => Some(Self::snapshot_hunt(nodes)),
            "read-hunt" => Some(Self::read_hunt(nodes)),
            "lease-drift" => Some(Self::lease_drift(nodes)),
            "membership-hunt" => Some(Self::membership_hunt(nodes)),
            _ => None,
        }
    }
}

/// The proposal an entry carries, or a bookkeeping stand-in for the entries
/// that carry none.
///
/// Only used by the negative-demonstration path, which does not care why an
/// entry would not decode: the ordinary path reports that as a violation.
#[cfg(feature = "negative-demos")]
fn decoded_or_bookkeeping(entry: &Entry) -> Proposal {
    let bookkeeping = Proposal {
        stamped_ms: 0,
        session: None,
        body: ProposalBody::KeepAlive,
    };
    match &entry.payload {
        EntryPayload::Normal(data) => decode::<Proposal>(data).unwrap_or(bookkeeping),
        _ => bookkeeping,
    }
}

#[cfg(not(feature = "negative-demos"))]
fn decoded_or_bookkeeping(_entry: &Entry) -> Proposal {
    Proposal {
        stamped_ms: 0,
        session: None,
        body: ProposalBody::KeepAlive,
    }
}

/// A snapshot arriving at a node, chunk by chunk.
///
/// Modelled with the same shape a real transfer has — verified position, a
/// checksum per chunk, a resume that continues rather than restarts — because a
/// transfer modelled as instantaneous tests nothing about the transfer.
struct Receiving {
    from: NodeId,
    meta: SnapshotMeta,
    /// The `Ready` the offer arrived in.
    ///
    /// Acknowledged when the stream finishes, which may be many events later.
    /// A core may only be told about a `Ready` it emitted, and out-of-order
    /// acknowledgements are already expected — fsync latencies vary and several
    /// can be in flight — so an old number is safe and a made-up one is not.
    ready_number: u64,
    /// The digest of the sender's log at the snapshot's index, carried with it.
    log_digest: u64,
    /// Bytes verified so far. The resume position, and the only record of
    /// progress: a chunk that fails its checksum never reaches it.
    have: Vec<u8>,
    total: usize,
    /// The offset a chunk was last lost at, if the stream is currently stalled
    /// there. A chunk that lands here afterwards is a *resume*: the transfer
    /// picked up where it broke rather than starting again.
    stalled_at: Option<usize>,
}

/// Apply one committed entry to a node's state machine.
///
/// A `Noop` or a configuration change moves the applied index and changes
/// nothing else — but it still has to be committed to the store, or the log
/// would hand it back on every restart.
///
/// An entry whose payload does not decode is a violation rather than something
/// to skip. Every payload in this simulation was encoded by this build, so one
/// that will not decode means the log handed back bytes that are not the bytes
/// that went in — which is exactly the class of failure the disk model exists to
/// produce, and skipping it would let the simulator report a clean run over a
/// corrupted log.
fn apply_entry(node: &mut SimNode, entry: &Entry) -> Result<AppliedKind, String> {
    let proposal = match &entry.payload {
        EntryPayload::Noop | EntryPayload::ConfChange(_) => Proposal {
            stamped_ms: 0,
            session: None,
            body: ProposalBody::KeepAlive,
        },
        EntryPayload::Normal(data) => decode::<Proposal>(data)
            .map_err(|e| format!("entry {} did not decode as a proposal: {e}", entry.index))?,
    };
    let before = node.sm.applied();
    let response = node
        .sm
        .apply(entry.index, &proposal)
        .map_err(|e| format!("entry {} would not apply: {e}", entry.index))?;
    if node.sm.applied() != entry.index {
        return Err(format!(
            "entry {} was handed to a state machine already at {before}; it moved to {} \
             instead. An entry handed back below the watermark is skipped, and its effect \
             is lost",
            entry.index,
            node.sm.applied()
        ));
    }
    Ok(AppliedKind::of(&proposal, &response))
}

/// What applying an entry turned out to be, so a run can report whether it
/// reached the state machine's interesting paths at all.
///
/// A sweep in which nothing ever opened a session, or in which no command was
/// ever refused for having none, is a sweep that tested the apply path and not
/// the session table — and it would look exactly like a clean run.
#[derive(Debug, Clone, Copy)]
enum AppliedKind {
    /// A no-op, a configuration change, or a keep-alive.
    Bookkeeping,
    SessionOpened,
    Committed,
    /// Refused because the session had expired or never existed.
    NoSession,
    /// A sequence number at or below the session's floor.
    Stale,
}

impl AppliedKind {
    fn of(proposal: &Proposal, response: &Response) -> Self {
        match (&proposal.body, response) {
            (_, Response::Registered { .. }) => AppliedKind::SessionOpened,
            (_, Response::Error(keel_api::ApiError::SessionExpired)) => AppliedKind::NoSession,
            (_, Response::Error(keel_api::ApiError::SequenceTooOld { .. })) => AppliedKind::Stale,
            (ProposalBody::Command(_), _) => AppliedKind::Committed,
            _ => AppliedKind::Bookkeeping,
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
    /// The applied index at this node's last checkpoint, so the next is due a
    /// fixed distance later rather than at a fixed time.
    last_checkpoint: Index,
    /// The bytes of this node's last checkpoint, and the digest at its index.
    ///
    /// A real node's checkpoint is a directory of hard links; there is nothing
    /// to link here, so the equivalent is the serialised store. Held so a
    /// follower behind the floor can be streamed it.
    checkpoint: Option<(SnapshotMeta, Vec<u8>, u64)>,
    /// A snapshot this node is receiving, and how far it has verified.
    receiving: Option<Receiving>,
    /// The digest at this node's snapshot floor.
    ///
    /// Held on the node rather than in the `LogDigest`, because it has to
    /// outlive the crash that drops one — the entries below a compacted floor
    /// are gone, so their cumulative hash cannot be recomputed and has to be
    /// carried. A real node carries the same thing in its snapshot metadata.
    snapshot_digest: (Index, u64),
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
    /// The real state machine, on its in-memory store.
    ///
    /// Not a counter and not a model. The apply path is where a session table
    /// deduplicates, where an index is written with the data it describes, and
    /// where two nodes either agree about what a log means or do not — and a
    /// model of it can only be wrong in ways somebody thought of.
    sm: StateMachine<MemStore>,
    /// Committed entries waiting for their predecessors.
    ///
    /// A `Ready` is written when it is pumped and made durable when its fsync
    /// fires (ADR-016), and fsyncs have independent latencies — so a later
    /// batch's fsync can complete before an earlier one's, and the committed
    /// entries arrive out of order. Watermarks do not care, because they are
    /// maxima. Applying does: an entry handed to the state machine below its
    /// watermark is skipped and its effect is lost.
    ///
    /// A real host applies in `Ready` order on one thread and never sees this.
    /// The buffer reassembles the order the model's fsync scheduling scrambled,
    /// rather than pretending the scrambling does not happen.
    pending_apply: BTreeMap<Index, Entry>,
    applied_count: usize,
    was_leader: bool,
    fsync_rng: Rng,
    /// This node's own clock stream. Drift is per node, because a cluster whose
    /// clocks all wandered together would be a cluster with no relative skew at
    /// all — which is the one case consensus does not have to survive.
    clock_rng: Rng,
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
    /// Sessions opened by an applied registration. Zero means no seed ever
    /// reached the session table, and every command was refused for having no
    /// session — a clean run that tested nothing about exactly-once delivery.
    pub sessions_opened: u64,
    /// Commands that a session accepted and that changed the store.
    pub commands_applied: u64,
    /// Commands refused because the session had expired or never existed.
    pub commands_without_a_session: u64,
    /// Commands whose sequence number was at or below the session's floor.
    pub commands_with_a_stale_sequence: u64,
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
    /// The highest term any node reached.
    ///
    /// Read next to `terms_with_leaders`, the two say what pre-vote is worth. A
    /// term that was entered and produced no leader is a term somebody burned:
    /// a node campaigning where nobody could hear it, raising its term each
    /// time, and carrying the total back into a healthy cluster when it
    /// reconnects. With pre-vote the two numbers track each other; without it
    /// the gap is the disruption.
    pub highest_term: Term,
    /// The smallest voter set any node was ever observed in.
    ///
    /// A harness invariant rather than a property of the cluster, and it is
    /// here because breaking it is how [KEEL-10](BUGS.md) was reached: a
    /// membership profile that lets the voter set fall to two produces a
    /// cluster where a single crash stops progress, and a run that stops making
    /// progress is a run whose findings are hard to attribute. Asserted in the
    /// tests, so the harness cannot drift back to it quietly.
    pub smallest_voter_set: usize,
    /// Observations of a node sitting in a joint configuration.
    ///
    /// The window joint consensus exists to make safe, counted rather than
    /// assumed. A membership profile whose changes all committed instantly
    /// never had `C_old,new` open while anything else was happening, so it has
    /// exercised the code path and not the hazard — which is
    /// [KEEL-4](BUGS.md)'s lesson for the third time.
    pub joint_config_windows: u64,
    /// Membership changes proposed, and the ones the core refused because one
    /// was already in flight.
    pub conf_changes_proposed: u64,
    pub conf_changes_refused: u64,
    /// Configurations the cluster actually finished moving to, counted by the
    /// distinct voter sets any node has been observed in.
    pub distinct_configurations: u64,
    /// Leader transfers asked for.
    pub transfers_requested: u64,
    /// Leaders that stopped leading while they could still reach a majority.
    ///
    /// What pre-vote exists to prevent (TR-8a). A node that was partitioned
    /// away campaigns, raises its own term with nobody to hear it, and on
    /// rejoining carries that inflated term into the first message it sends —
    /// at which point a healthy leader with a full quorum steps down for a node
    /// that has no more log than it does. Check-quorum does not stop this: it
    /// filters vote *requests*, and the term arrives in an ordinary response.
    ///
    /// A leader that steps down while genuinely cut off is not counted, because
    /// that is correct behaviour and counting it would make the two arms of the
    /// demonstration look alike.
    pub leaders_deposed_with_a_quorum: u64,
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
    /// Checkpoints taken. Zero on a profile with snapshots on means the log
    /// never grew far enough, and every snapshot path went untested.
    pub checkpoints_taken: u64,
    /// Snapshot streams begun.
    pub streams_started: u64,
    /// Chunks lost mid-stream. Zero means every transfer ran to completion
    /// uninterrupted, so the resume path was never reached.
    pub streams_interrupted: u64,
    /// Chunks appended to a stream that had already made progress — a resume
    /// rather than a restart.
    pub streams_resumed: u64,
    /// Streams that decoded and installed.
    pub streams_completed: u64,
    // Reads. A read is three things — asked for, confirmed at an index, and
    // answered out of a store that has reached it — and each of them can fail
    // to happen. Counting all three is how a profile that turned reads on and
    // never got one answered is distinguishable from one where they worked.
    /// Linearizable reads asked for.
    pub reads_issued: u64,
    /// Reads a leader was entitled to answer out of its own lease, without a
    /// heartbeat round.
    ///
    /// The coverage counter for the lease demonstration. A run configured for
    /// lease reads whose leaders never actually held a lease has taken the fast
    /// path zero times, and whatever it found or did not find says nothing
    /// about leases.
    pub lease_reads_served: u64,
    /// Lease reads served by a node that some other live node had already
    /// overtaken.
    ///
    /// The window the lease hazard lives in, counted rather than assumed —
    /// [KEEL-4](BUGS.md)'s lesson applied to reads. A demonstration that
    /// reports "no violation" with this at zero has not tested leases; it has
    /// tested a cluster that never got into the state where leases are
    /// dangerous.
    ///
    /// The condition is *behind the cluster*, not *a second leader exists*, and
    /// the difference is the thing this counter taught. The first version
    /// counted rival leaders and read zero on runs that were producing stale
    /// reads by the hundred: the hazard does not need two nodes claiming
    /// leadership at the same instant, only one lease-holder answering out of a
    /// commit index the cluster has already moved past.
    pub lease_reads_behind_the_cluster: u64,
    /// Reads the core confirmed an index for. A read issued to a node that then
    /// lost its leadership is never confirmed, which is correct and not a
    /// violation — but a run where none were confirmed checked nothing.
    pub reads_confirmed: u64,
    /// Reads answered out of a store that had applied the confirmed index.
    pub reads_answered: u64,
    /// Reads confirmed at an index at or above what was already committed when
    /// they were asked for, with something actually committed at the time.
    ///
    /// The coverage counter for the recency oracle. A read confirmed when
    /// nothing had been committed yet cannot be stale and cannot demonstrate
    /// that it is not, so a run whose reads all landed on an empty cluster has
    /// not exercised the property however many it answered.
    pub read_recency_windows: u64,
    /// Crashes that tore a node's log while that node was inside a partition.
    ///
    /// The counter the durability claim turns on. It is not enough that tears
    /// happen and partitions happen; what has to be shown is that they met.
    pub tears_during_partition: u64,
}

/// A linearizable read that has been asked for and not yet answered.
///
/// The two indexes are the whole of the recency property. `commit_at_issue` is
/// what the cluster had already committed at the moment the read was asked for
/// — every one of those entries is a write that had completed, so a
/// linearizable read is obliged to see all of them. `confirmed` is the index
/// the core came back with. A read whose confirmed index is *below* what was
/// already committed when it was issued is a read that may legally return stale
/// data, and that is precisely the bug ReadIndex exists to prevent.
#[derive(Debug, Clone)]
struct PendingRead {
    node: NodeId,
    key: Vec<u8>,
    commit_at_issue: Index,
    confirmed: Option<Index>,
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
    read_rng: Rng,
    membership_rng: Rng,
    /// The configuration every node booted with, which is what a node that has
    /// never taken a snapshot recovers to before it replays its log.
    boot_conf: ConfState,
    /// Every voter set any node has been seen in, so a run can say whether the
    /// membership actually moved rather than only that a change was proposed.
    configurations_seen: std::collections::BTreeSet<(Vec<NodeId>, Vec<NodeId>)>,
    /// Reads issued and not yet answered, by context.
    reads: BTreeMap<u64, PendingRead>,
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
fn stage(node: &mut SimNode, rd: &Ready, streaming: bool) -> Result<(), String> {
    let Some(log) = node.log.as_mut() else {
        return Err("a live node has no open log".into());
    };
    let step = |r: keel_log::Result<keel_log::SyncToken>| r.map(|_| ()).map_err(|e| e.to_string());

    // Only when the transfer is instantaneous. With snapshots on, the bytes
    // have to arrive before the floor moves — the log is installed by
    // `install_snapshot` once the stream completes.
    if let Some(meta) = &rd.snapshot_to_install
        && !streaming
    {
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
        // Two more, and they come last for the reason the comment above gives:
        // a split is decided by the root's seed, the label, and how many splits
        // preceded it, so a stream added anywhere but the end moves every
        // stream after it and every old seed becomes a different run. These two
        // draw nothing at all unless the profile turns their feature on, which
        // is why the six committed profiles' fingerprints are unmoved.
        let mut clock_rng = root.split("clocks");
        let read_rng = root.split("reads");
        // Last again, and for the same reason. Membership changes and leader
        // transfers draw nothing on any profile that does not ask for them.
        let membership_rng = root.split("membership");

        let ids: Vec<NodeId> = (1..=cfg.nodes as NodeId).collect();
        // Learners are just voters that have not been promoted yet, and they
        // have to exist at boot: nothing in a simulated cluster can start a
        // process that was not in the seed.
        let conf = if cfg.initial_voters == 0 || cfg.initial_voters >= ids.len() {
            ConfState::single(ids.iter().copied())
        } else {
            let (voters, learners) = ids.split_at(cfg.initial_voters);
            ConfState {
                voters: voters.to_vec(),
                learners: learners.to_vec(),
                ..Default::default()
            }
        };

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
                read_only: match cfg.lease_read_drift_bound {
                    None => ReadOnlyOption::ReadIndex,
                    Some(drift_bound_pct) => ReadOnlyOption::LeaseBased { drift_bound_pct },
                },
                ..Config::new(*id)
            };
            let skew = cfg.clock_skew_pct.min(50);
            let tick_period = if cfg.slowest_node_first {
                // Assigned rather than drawn, so the conjunction the lease
                // hazard needs is reached by construction instead of by luck.
                // No draw at all, which is why this cannot be switched on for a
                // profile that already has a pinned fingerprint.
                if *id == 1 {
                    cfg.tick_ns * (100 + skew) / 100
                } else {
                    cfg.tick_ns * (100 - skew) / 100
                }
            } else if skew == 0 {
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
                    last_checkpoint: 0,
                    checkpoint: None,
                    receiving: None,
                    snapshot_digest: (0, 0),
                    alive: true,
                    epoch: 0,
                    tick_period,
                    sm: StateMachine::new(MemStore::new()),
                    pending_apply: BTreeMap::new(),
                    applied_count: 0,
                    was_leader: false,
                    fsync_rng: node_rng.split("fsync"),
                    // From the clock stream, never from `node_rng`: a draw
                    // added to that generator would shift node 2's core seed
                    // and everything after it.
                    clock_rng: clock_rng.split("node"),
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
            read_rng,
            membership_rng,
            boot_conf: conf.clone(),
            configurations_seen: std::collections::BTreeSet::new(),
            reads: BTreeMap::new(),
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

    /// How many entries the reference state machine has applied.
    ///
    /// Exposed so a test can refuse a run in which the model saw nothing and
    /// every comparison against it was vacuous.
    pub fn oracle_model_applied(&self) -> u64 {
        self.oracle.model_applied
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
        // One chunk per tick, so a snapshot moves without any one event
        // carrying the whole of it.
        if self.nodes.get(&id).is_some_and(|n| n.receiving.is_some()) {
            self.stream_snapshot(id);
        }
        // Drift is applied here rather than at construction, because the point
        // of it is that the period is not the same twice. A node whose ticks
        // are uniformly 5% slow never fires an election timeout early; a node
        // whose ticks wander fires early once and late the next time, and the
        // second of those is a state a fixed offset cannot reach.
        let drift = self.cfg.clock_drift_pct;
        let period = match self.nodes.get_mut(&id) {
            None => self.cfg.tick_ns,
            Some(node) if drift == 0 => node.tick_period,
            Some(node) => {
                let base = node.tick_period;
                let low = base * (100 - drift.min(90)) / 100;
                let high = base * (100 + drift) / 100;
                // `range` draws nothing when the bounds collapse, so a base
                // period small enough that the two bounds meet costs no draw
                // and the stream stays where it is.
                node.clock_rng.range(low, high + 1)
            }
        };
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

        // 3. Apply, in index order, then report how far.
        let mut apply_failure = None;
        let mut applied_kinds = Vec::new();
        let mut committed_for_model = Vec::new();
        let skip_apply_ordering = self.cfg.skip_apply_ordering;
        if let Some(node) = self.nodes.get_mut(&id) {
            node.applied_count += batch.committed.len();
            for entry in &batch.committed {
                committed_for_model.push(entry.clone());
            }
            for entry in batch.committed {
                node.pending_apply.insert(entry.index, entry);
            }
            // Drain only the contiguous run. Anything above a gap waits for the
            // fsync that is still in flight beneath it.
            //
            // With the ordering removed, everything buffered is applied in
            // whatever order it arrived — which is the map's key order, so an
            // entry below the watermark is silently skipped.
            if skip_apply_ordering {
                let arrived: Vec<Entry> = node.pending_apply.values().cloned().collect();
                node.pending_apply.clear();
                for entry in arrived {
                    let before = node.sm.applied();
                    if node
                        .sm
                        .apply(entry.index, &decoded_or_bookkeeping(&entry))
                        .is_ok()
                    {
                        let _ = before;
                        applied_kinds.push(AppliedKind::Bookkeeping);
                    }
                }
            }
            while let Some(entry) = node.pending_apply.remove(&(node.sm.applied() + 1)) {
                match apply_entry(node, &entry) {
                    Ok(kind) => applied_kinds.push(kind),
                    Err(why) => {
                        apply_failure = Some(why);
                        break;
                    }
                }
            }
            let applied = node.sm.applied();
            if applied > 0 {
                node.core.advance(Advance {
                    ready_number: batch.ready_number,
                    persisted: None,
                    applied: Some(applied),
                    snapshot_installed: None,
                });
            }
        }
        for entry in &committed_for_model {
            self.oracle.observe_committed_entry(entry);
        }
        for kind in applied_kinds {
            match kind {
                AppliedKind::SessionOpened => self.stats.sessions_opened += 1,
                AppliedKind::Committed => self.stats.commands_applied += 1,
                AppliedKind::NoSession => self.stats.commands_without_a_session += 1,
                AppliedKind::Stale => self.stats.commands_with_a_stale_sequence += 1,
                AppliedKind::Bookkeeping => {}
            }
        }
        if let Some(detail) = apply_failure {
            self.violations.push(Violation {
                property: "a committed entry applies",
                detail: format!("node {id}: {detail}"),
            });
            return;
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

        // A read, sometimes. `chance` draws nothing at all when the percentage
        // is zero, which is what lets this be added without moving a single
        // committed fingerprint — the six profiles that predate it never reach
        // the read stream.
        if self.cfg.read_pct > 0 && self.read_rng.chance(self.cfg.read_pct) {
            self.issue_read(target, ctx, client);
            return;
        }
        // A membership change or a leader transfer, sometimes. Same rule: a
        // profile that asks for neither draws from neither stream.
        if self.cfg.conf_change_pct > 0 && self.membership_rng.chance(self.cfg.conf_change_pct) {
            self.propose_conf_change(target, ctx);
            return;
        }
        if self.cfg.transfer_pct > 0 && self.membership_rng.chance(self.cfg.transfer_pct) {
            self.request_transfer(target);
            return;
        }

        // A real proposal, encoded the way a real client's would be, because
        // the state machine on the other end is the real one. The sequence
        // number is the context, which is monotonic across the whole run, so a
        // client's stream never goes backwards even across a leadership change.
        //
        // The session is opened lazily by the state machine: `client` here is a
        // small integer and every node's `StateMachine` allocates ids in the
        // same order from the same log, so a client that has not registered
        // simply has its command refused — which is a response like any other
        // and not a violation.
        let mut value = format!("c{client}:{ctx}").into_bytes();
        // Padded, not replaced, so the identifying prefix a trace shows is still
        // the first thing in the record, and so the record is the size the
        // profile chose — a write only tears when it straddles a sector.
        value.resize(value.len().max(self.cfg.proposal_bytes), b'.');

        let proposal = if ctx % 32 == 1 {
            // Every so often, open a session. A run in which nothing ever
            // registers exercises the refusal path and nothing else.
            Proposal {
                stamped_ms: self.now / 1_000_000,
                session: None,
                body: ProposalBody::Register {
                    nonce: client as u64,
                },
            }
        } else {
            Proposal {
                stamped_ms: self.now / 1_000_000,
                session: Some((client as u64 + 1, ctx)),
                body: ProposalBody::Command(Command::Put {
                    key: Bytes::from(format!("c{client}")),
                    value: Bytes::from(value),
                }),
            }
        };
        let Ok(encoded) = encode(&proposal) else {
            return;
        };
        if let Some(node) = self.nodes.get_mut(&target) {
            let _ = node.core.step(Input::Propose {
                ctx,
                data: Bytes::from(encoded),
            });
            self.stats.proposals += 1;
        }
        self.pump(target);
        self.check(target);
    }

    /// Propose one membership change: promote a learner, or demote a voter.
    ///
    /// Promote and demote rather than add and remove, because a simulated
    /// cluster cannot start a process that was not in the seed — every node
    /// exists from boot and membership decides which of them vote. That is a
    /// real restriction and it is the *interesting* half either way: adding a
    /// node that nobody has heard of is a catch-up problem, while moving a node
    /// in or out of the voter set is what opens a joint configuration and what
    /// can split a quorum.
    ///
    /// The voter set is never allowed below three. A cluster of two has a
    /// quorum of two, so a single crash stops it, and a run that stops making
    /// progress checks nothing — the same argument the crash nemesis makes.
    fn propose_conf_change(&mut self, target: NodeId, ctx: u64) {
        let Some(node) = self.nodes.get(&target) else {
            return;
        };
        let conf = node.core.conf().clone();
        // Only from a leader.
        //
        // The guard below reads this configuration to decide what is safe to
        // propose, and a follower's configuration can be arbitrarily stale — it
        // takes effect at apply time, so a node that is behind is looking at
        // the membership of some earlier moment. Proposing against that view
        // let two changes drawn from two stale readings take the voter set
        // somewhere neither of them intended, which is a fault in the harness
        // rather than in the cluster and made a real finding hard to attribute.
        //
        // A leader that is *already* mid-change is deliberately not filtered
        // out here. The core refuses that itself, and the refusal path is a
        // safety property — only one change in flight — that a harness which
        // never triggers it has never checked.
        if node.core.role() != Role::Leader {
            return;
        }
        // A change proposed while one is in flight is refused by the core, which
        // is correct and is counted rather than avoided: the refusal path is
        // part of what P23 is here to exercise.
        let promote =
            conf.voters.len() < 3 || (!conf.learners.is_empty() && self.membership_rng.chance(50));
        let change = if promote {
            let learners = conf.learners.clone();
            let Some(node) = self.membership_rng.pick(&learners).copied() else {
                return;
            };
            ConfChangeSingle {
                kind: ChangeKind::AddVoter,
                node,
            }
        } else {
            // Never the leader itself: a leader that removes itself steps down,
            // which is correct behaviour and well covered by the in-process
            // tests, and doing it here would spend most of the run in elections
            // rather than in joint configurations.
            let demotable: Vec<NodeId> = conf
                .voters
                .iter()
                .copied()
                .filter(|id| *id != target)
                .collect();
            if conf.voters.len() <= 3 {
                return;
            }
            let Some(node) = self.membership_rng.pick(&demotable).copied() else {
                return;
            };
            ConfChangeSingle {
                kind: ChangeKind::AddLearner,
                node,
            }
        };

        if let Some(node) = self.nodes.get_mut(&target) {
            let cc = ConfChangeV2 {
                changes: vec![change],
            };
            match node.core.step(Input::ProposeConfChange { ctx, cc }) {
                Ok(()) => self.stats.conf_changes_proposed += 1,
                Err(_) => self.stats.conf_changes_refused += 1,
            }
        }
        self.pump(target);
        self.check(target);
    }

    /// Ask the leader to hand leadership to one of its voters.
    fn request_transfer(&mut self, target: NodeId) {
        let Some(conf) = self.nodes.get(&target).map(|n| n.core.conf().clone()) else {
            return;
        };
        let candidates: Vec<NodeId> = conf
            .voters
            .iter()
            .copied()
            .filter(|id| *id != target && self.nodes.get(id).is_some_and(|n| n.alive))
            .collect();
        let Some(to) = self.membership_rng.pick(&candidates).copied() else {
            return;
        };
        if let Some(node) = self.nodes.get_mut(&target) {
            let _ = node.core.step(Input::TransferLeader { to });
            self.stats.transfers_requested += 1;
        }
        self.pump(target);
        self.check(target);
    }

    /// Ask a leader for a linearizable read, and remember what the cluster had
    /// already committed when it was asked.
    ///
    /// That second half is the whole of the oracle. A read is only checkable
    /// against something that was true before it started: every entry committed
    /// at this moment is a write that had already completed, so a linearizable
    /// read is obliged to observe all of them. Recording the commit index *at
    /// issue* is what turns "the read returned something" into "the read
    /// returned something new enough".
    fn issue_read(&mut self, target: NodeId, ctx: u64, client: usize) {
        // The highest index committed anywhere. Taken across the cluster rather
        // than from the node being asked, because a write acknowledged by a
        // leader that has since been deposed still happened, and a read that
        // missed it would still be stale.
        let commit_at_issue = self
            .nodes
            .values()
            .filter(|n| n.alive)
            .map(|n| n.core.log().committed())
            .max()
            .unwrap_or(0);

        let key = format!("c{client}").into_bytes();
        // Counted before the step, because `lease_valid()` is the state that
        // decides whether the core may answer locally, and the step is what
        // consumes it.
        if self
            .nodes
            .get(&target)
            .is_some_and(|n| n.core.lease_valid())
        {
            self.stats.lease_reads_served += 1;
            let mine = self
                .nodes
                .get(&target)
                .map(|n| n.core.log().committed())
                .unwrap_or(0);
            if self
                .nodes
                .values()
                .any(|n| n.alive && n.id != target && n.core.log().committed() > mine)
            {
                self.stats.lease_reads_behind_the_cluster += 1;
            }
        }
        match self.nodes.get_mut(&target) {
            // A read the core refuses — no leader, or a leader whose no-op has
            // not committed — is a refusal like any other and not a violation.
            // It is simply never confirmed, and `reads_confirmed` says so.
            Some(node) => {
                let _ = node.core.step(Input::ReadIndex { ctx });
            }
            None => return,
        }
        self.reads.insert(
            ctx,
            PendingRead {
                node: target,
                key,
                commit_at_issue,
                confirmed: None,
            },
        );
        self.stats.reads_issued += 1;
        self.pump(target);
        self.check(target);
    }

    /// The core has confirmed a read index. Check it is not older than what was
    /// already committed when the read was asked for.
    fn on_read_confirmed(&mut self, ctx: u64, index: Index) {
        let Some(pending) = self.reads.get_mut(&ctx) else {
            // A confirmation for a read nobody is waiting on. Not a violation:
            // a leader that was asked twice, or a duplicate delivery, produces
            // one.
            return;
        };
        pending.confirmed = Some(index);
        let issued_at = pending.commit_at_issue;
        self.stats.reads_confirmed += 1;
        if index >= issued_at && issued_at > 0 {
            // The window this profile exists to reach: a read confirmed at an
            // index the cluster had genuinely already committed, so the check
            // below had something to be wrong about.
            self.stats.read_recency_windows += 1;
        }
        if index < issued_at {
            self.violations.push(Violation {
                property: "Read Recency",
                detail: format!(
                    "a read asked for when index {issued_at} was already committed was \
                     confirmed at index {index}, so it may return a value older than a \
                     write that had already completed"
                ),
            });
        }
    }

    /// Answer every read whose confirmed index its node has now applied.
    ///
    /// A read is not answered when it is confirmed — it is answered when the
    /// state machine has caught up to the index the core named. Answering
    /// earlier is exactly the bug: the index says which version of the world
    /// the read is entitled to see, and a store that has not reached it holds
    /// an older one.
    fn answer_reads(&mut self, id: NodeId) {
        let applied = match self.nodes.get(&id) {
            Some(node) if node.alive => node.sm.applied(),
            _ => return,
        };
        let ready: Vec<u64> = self
            .reads
            .iter()
            .filter(|(_, r)| r.node == id && r.confirmed.is_some_and(|i| i <= applied))
            .map(|(ctx, _)| *ctx)
            .collect();
        for ctx in ready {
            let Some(pending) = self.reads.remove(&ctx) else {
                continue;
            };
            let Some(confirmed) = pending.confirmed else {
                continue;
            };
            debug_assert!(
                confirmed <= applied,
                "a read was answered before its confirmed index was applied"
            );
            let observed = self
                .nodes
                .get(&id)
                .and_then(|n| n.sm.get(&pending.key).ok().flatten());
            self.stats.reads_answered += 1;
            // The node's applied index, not the read's confirmed index. The
            // read is entitled to everything through `confirmed`; the store it
            // was answered from has reached `applied`, which may be further on.
            // Judging it against `confirmed` would call a *newer* answer wrong,
            // and a newer answer is exactly what linearizability permits.
            if let Some(v) = self.oracle.check_read(
                id,
                applied,
                &pending.key,
                observed.as_ref().map(|b| b.as_ref()),
            ) {
                self.violations.push(v);
            }
        }
    }

    /// Take a checkpoint if enough has been applied since the last one.
    ///
    /// The checkpoint is the serialised store plus the digest at its index —
    /// the pair a real node keeps in its snapshot metadata, and the pair that
    /// makes a restarted node's digest comparable with its peers'.
    fn checkpoint_if_due(&mut self, id: NodeId) {
        let every = self.cfg.entries_between_checkpoints;
        if every == 0 {
            return;
        }
        let Some(node) = self.nodes.get_mut(&id) else {
            return;
        };
        // The *log's* applied index, not the state machine's. They track each
        // other, but the digest and the log are indexed by the first and mixing
        // the two attaches a cumulative hash to an index the digest never
        // described.
        let applied = node.core.log().applied().min(node.sm.applied());
        if applied == 0 || applied.saturating_sub(node.last_checkpoint) < every {
            return;
        }
        // The digest at the index being checkpointed. Without it the floor is
        // uncarryable and a restart from this checkpoint would compare an
        // invented number against its peers'.
        let Some((_, log_digest)) = node.digest.at(applied) else {
            return;
        };
        let Some(term) = node.core.log().term(applied) else {
            return;
        };

        let meta = SnapshotMeta {
            index: applied,
            term,
            conf: node.core.conf().clone(),
        };
        node.checkpoint = Some((meta.clone(), node.sm.store().to_bytes(), log_digest));
        node.last_checkpoint = applied;
        node.snapshot_digest = (applied, log_digest);
        let _ = node.core.step(Input::SnapshotTaken { meta });
        self.stats.checkpoints_taken += 1;
    }

    /// A follower was offered a snapshot: start receiving one.
    ///
    /// The offer names an index; the bytes come from the leader's checkpoint.
    /// A leader that no longer has one — because it was replaced, or has moved
    /// on — simply sends nothing, and the follower's request expires like any
    /// other.
    fn begin_receiving(
        &mut self,
        follower: NodeId,
        leader: NodeId,
        offered_meta: SnapshotMeta,
        ready_number: u64,
    ) {
        // The leader's *checkpoint* metadata, not the offer's.
        //
        // They can differ: the offer names the core's log floor, and the stored
        // checkpoint names the index its bytes were taken at. Adopting the
        // offer's index with the checkpoint's digest attaches a cumulative hash
        // to the wrong index, and every entry above it then chains from a base
        // nobody else agrees with — which surfaces as a Log Matching violation
        // on correct code, several hundred events later.
        let Some((meta, bytes, log_digest)) =
            self.nodes.get(&leader).and_then(|n| n.checkpoint.clone())
        else {
            return;
        };
        let _ = offered_meta;
        let total = bytes.len();
        if let Some(node) = self.nodes.get_mut(&follower) {
            // A transfer already in flight from the same leader at the same
            // index continues; anything else starts again, because a staging
            // area holding part of a different snapshot is two snapshots
            // spliced together.
            let resume = node
                .receiving
                .as_ref()
                .is_some_and(|r| r.from == leader && r.meta.index == meta.index);
            if !resume {
                node.receiving = Some(Receiving {
                    from: leader,
                    meta,
                    ready_number,
                    log_digest,
                    have: Vec::new(),
                    total,
                    stalled_at: None,
                });
                self.stats.streams_started += 1;
            }
        }
    }

    /// Move one snapshot stream forward by a chunk.
    ///
    /// One chunk per turn rather than a loop, for the same reason a real leader
    /// does it that way: streaming a whole snapshot inside one event would stop
    /// the node answering anything else for the duration.
    fn stream_snapshot(&mut self, follower: NodeId) {
        let chunk_bytes = self.cfg.snapshot_chunk_bytes.max(1);
        let loss_pct = self.cfg.snapshot_chunk_loss_pct;
        let Some(node) = self.nodes.get_mut(&follower) else {
            return;
        };
        let Some(receiving) = node.receiving.as_mut() else {
            return;
        };
        if receiving.have.len() >= receiving.total {
            return;
        }
        let leader = receiving.from;
        let offset = receiving.have.len();
        let take = chunk_bytes.min(receiving.total - offset);

        // The chunk is drawn from the leader's checkpoint, which is where it
        // would come from. A leader that has replaced its checkpoint since the
        // stream began cannot serve it, and the stream stalls until the
        // follower is offered a new one.
        let dropped = node.fsync_rng.chance(loss_pct);
        let Some((_, bytes, _)) = self.nodes.get(&leader).and_then(|n| n.checkpoint.as_ref())
        else {
            return;
        };
        if bytes.len() != {
            let Some(node) = self.nodes.get(&follower) else {
                return;
            };
            node.receiving.as_ref().map_or(0, |r| r.total)
        } {
            return;
        }
        let chunk: Vec<u8> = bytes[offset..offset + take].to_vec();

        let Some(node) = self.nodes.get_mut(&follower) else {
            return;
        };
        let Some(receiving) = node.receiving.as_mut() else {
            return;
        };
        if dropped {
            // Not appended, so the position does not move and the next attempt
            // sends this same chunk again.
            receiving.stalled_at = Some(offset);
            self.stats.streams_interrupted += 1;
            return;
        }
        // A chunk landing exactly where one was lost is the resume: the
        // transfer continued from the break rather than starting over.
        if receiving.stalled_at == Some(offset) {
            receiving.stalled_at = None;
            self.stats.streams_resumed += 1;
        }
        receiving.have.extend_from_slice(&chunk);

        if receiving.have.len() >= receiving.total {
            self.install_snapshot(follower);
        }
    }

    /// A stream finished. Decode it, adopt it, and tell the core.
    fn install_snapshot(&mut self, follower: NodeId) {
        let Some(node) = self.nodes.get_mut(&follower) else {
            return;
        };
        let Some(receiving) = node.receiving.take() else {
            return;
        };
        let store = match MemStore::from_bytes(&receiving.have) {
            Ok(store) => store,
            Err(why) => {
                // Every chunk arrived and the whole does not decode: the set was
                // wrong rather than the bytes. A violation, because the transfer
                // said it was complete.
                self.violations.push(Violation {
                    property: "an installed snapshot decodes",
                    detail: format!("node {follower}: {why}"),
                });
                return;
            }
        };

        // The installed snapshot *is* this node's checkpoint from now on. A
        // node that treated it as transient would restart into an empty store
        // with a compacted log, and the entries that built the state below the
        // floor are gone — so there would be nothing to replay it back from.
        node.checkpoint = Some((
            receiving.meta.clone(),
            receiving.have.clone(),
            receiving.log_digest,
        ));
        node.sm = StateMachine::new(store);
        node.pending_apply.clear();
        // The floor and its digest together, which is the whole of P16's first
        // half seen from the install side.
        // Adopting is a rewrite: the prefix beneath any retained tail becomes
        // the snapshot's, so those entries' digests change and the old ones
        // have to be retired through the check that knows the difference
        // between discarding a divergent entry and discarding a committed one.
        let discarded = node
            .digest
            .adopt_snapshot(receiving.meta.index, receiving.log_digest);
        node.snapshot_digest = (receiving.meta.index, receiving.log_digest);
        node.last_checkpoint = receiving.meta.index;
        if !discarded.is_empty()
            && let Some(v) = self.oracle.check_rewrite(follower, &discarded)
        {
            self.violations.push(v);
        }

        if let Some(log) = node.log.as_mut() {
            let _ = log.install_snapshot(&receiving.meta);
        }
        node.core.advance(Advance {
            ready_number: receiving.ready_number,
            persisted: None,
            applied: None,
            snapshot_installed: Some(receiving.meta),
        });
        self.stats.streams_completed += 1;
    }

    fn on_restart(&mut self, id: NodeId) {
        let opts = self.cfg.log_options();
        // Everything the reopen needs, taken before the borrow is dropped, so
        // that recording a violation below does not fight the borrow checker.
        let Some((cfg, fs, dir, checkpoint)) = self.nodes.get(&id).filter(|n| !n.alive).map(|n| {
            (
                n.core.config().clone(),
                n.fs.clone(),
                n.dir.clone(),
                n.checkpoint.clone(),
            )
        }) else {
            return;
        };
        // The configuration a real node recovers, which is not the one it was
        // holding in memory when it died.
        //
        // Membership is a function of the applied log, so a restarting node
        // starts from whatever its snapshot records — the boot configuration if
        // it has never taken one — and replays the log forward to rebuild the
        // rest. Handing the core the live in-memory configuration *and* the log
        // to replay gives it a membership that has already advanced past the
        // entries about to be re-applied, so the two disagree about where the
        // replay starts. That produced configurations no sequence of proposals
        // could have produced, including voter sets of two on a profile whose
        // floor is three, and it is how [KEEL-10](BUGS.md) was reached.
        let conf = checkpoint
            .as_ref()
            .map_or_else(|| self.boot_conf.clone(), |(meta, _, _)| meta.conf.clone());
        let checkpoint_index = checkpoint.as_ref().map_or(0, |(meta, _, _)| meta.index);
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
                // What the *checkpoint* says, which is what survives a crash.
                //
                // Nothing else does: the store is memory and everything written
                // since the last checkpoint went with it. So a restart restores
                // the checkpoint and replays the log from its index, which is
                // exactly what a real node does — and, once the log is being
                // compacted, is the only thing that can work. Replaying from
                // zero into a fresh store would lose everything below the
                // floor, because the entries that built it are gone.
                //
                // Carrying the *live* store across a crash was tried and is
                // unsound: it survives a crash the log does not, so its applied
                // index can outrun what the recovered log proves was committed,
                // and the entries between the two are handed back, skipped as
                // already-applied, and lost. A checkpoint has no such problem —
                // it is durable by construction and its index is one the log
                // agrees about.
                applied: checkpoint_index,
            },
        );
        node.log = Some(log);
        // Rebased at the floor this node had, not at zero. A digest that
        // started at `(floor, 0)` would compare as different from every peer
        // that still holds the entries below it — a State Machine Safety
        // violation reported on correct code, which is the failure this whole
        // arrangement exists to avoid.
        let (floor, floor_digest) = node.snapshot_digest;
        node.digest = LogDigest::rebased(floor, floor_digest);
        // Restored from the checkpoint, which is what survived. Everything
        // written since is gone with the memory that held it, and the log
        // replays it back from the checkpoint's index.
        node.sm = match &checkpoint {
            Some((_, bytes, _)) => match MemStore::from_bytes(bytes) {
                Ok(store) => StateMachine::new(store),
                Err(why) => {
                    self.violations.push(Violation {
                        property: "a checkpoint decodes",
                        detail: format!("node {id}: {why}"),
                    });
                    return;
                }
            },
            None => StateMachine::new(MemStore::new()),
        };
        node.last_checkpoint = checkpoint_index;
        // Entries buffered behind an fsync that never fired are gone with it.
        node.pending_apply.clear();
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
        // A majority of the *voters*, not of every node that exists. Under a
        // joint configuration it is a majority of both halves, and the smaller
        // half is the one that binds — a crash budget computed over the whole
        // node map would happily kill enough of `C_old` to stop the cluster
        // while the arithmetic still said a quorum survived. Learners never
        // count: they do not vote, so killing one cannot cost a quorum.
        let quorum = self
            .nodes
            .values()
            .map(|n| n.core.conf())
            .map(|conf| {
                let incoming = conf.voters.len() / 2 + 1;
                if conf.is_joint() {
                    incoming.max(conf.voters_outgoing.len() / 2 + 1)
                } else {
                    incoming
                }
            })
            .max()
            .unwrap_or(ids.len() / 2 + 1);

        // One draw, compared against the table's running totals. It is one draw
        // rather than several because the number of draws a decision costs is
        // part of the seed's meaning: two draws here would shift every stream
        // position after it and move every pinned fingerprint.
        let weights = self.cfg.nemesis_weights;
        let roll = self.nemesis_rng.range(0, u64::from(weights.total())) as u32;
        match weights.action_for(roll) {
            NemesisAction::Split => {
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
            NemesisAction::OneWay => {
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
            NemesisAction::Isolate => {
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
            NemesisAction::Heal => {
                self.net.heal();
                self.trace("network healed".to_string());
            }
            NemesisAction::Crash => {
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
            NemesisAction::Restart => {
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

            // With snapshots on, an offer starts a stream and the floor does
            // not move until the bytes have arrived. Without them, the transfer
            // is instantaneous and the log takes the snapshot here — which is
            // what every profile did before P16, and is why their fingerprints
            // are unmoved by it.
            let streaming = self.cfg.entries_between_checkpoints > 0;
            let rd_number = rd.number;
            let offered = if streaming {
                rd.snapshot_to_install
                    .clone()
                    .map(|meta| (node.core.status().leader, meta))
            } else {
                None
            };

            // Read confirmations, taken before the Ready is consumed. They are
            // not I/O and do not wait on an fsync: the round trip that makes
            // the read linearizable already happened, in heartbeats, inside the
            // core.
            let confirmed: Vec<(u64, Index)> =
                rd.read_states.iter().map(|r| (r.ctx, r.index)).collect();

            let persisted = rd.entries.last().map(|e| (e.index, e.term));
            let needs_sync = !rd.entries.is_empty()
                || rd.hard_state.is_some()
                || (rd.snapshot_to_install.is_some() && !streaming);
            if let Err(detail) = stage(node, &rd, streaming) {
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
                        snapshot: if streaming {
                            None
                        } else {
                            rd.snapshot_to_install
                        },
                        messages: rd.messages,
                        committed: rd.committed_entries,
                    }),
                },
            );
            for (ctx, index) in confirmed {
                self.on_read_confirmed(ctx, index);
            }
            self.answer_reads(id);
            if let Some((leader, meta)) = offered
                && let Some(leader) = leader
            {
                self.begin_receiving(id, leader, meta, rd_number);
            }
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
        // A floor whose digest nobody carried. Reported rather than papered
        // over: continuing would compare a made-up number against real ones,
        // and the comparison would fail on correct code.
        let orphaned_floor = node.digest.floor_without_a_digest();
        // Kept in step, so a later restart rebases at the floor this node
        // actually has rather than at the one it had when it started.
        node.snapshot_digest = node.digest.base();
        // A checkpoint is taken here, after the digest has been brought in line
        // with the log, and nowhere else. Taking one earlier reads a digest that
        // may still describe entries a truncation has since replaced — and the
        // checkpoint would then carry a floor digest nobody else agrees with,
        // which surfaces later as a Log Matching violation on correct code.
        let take_checkpoint = self.cfg.entries_between_checkpoints > 0;
        let role = node.core.role();
        let term = node.core.term();
        let last_index = node.core.log().last_index();
        let commit = node.core.log().committed();
        let applied = node.core.log().applied();
        // A hash of everything the state machine holds. Cheap enough to take on
        // every check because the store is a BTreeMap and the run is small; the
        // alternative — comparing whole stores between nodes — is the same
        // information at far greater cost.
        let state_digest = state_digest(&node.sm);
        let became_leader = role == Role::Leader && !node.was_leader;
        let stopped_leading = role != Role::Leader && node.was_leader;
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

        if take_checkpoint {
            self.checkpoint_if_due(id);
        }

        let mut found: Vec<Violation> = Vec::new();

        if let Some(index) = orphaned_floor {
            found.push(Violation {
                property: "a compacted floor carries its digest",
                detail: format!(
                    "node {id}'s log floor moved to index {index} and nothing carried the \
                     cumulative digest there. Comparing this node against its peers from \
                     here would compare an invented number against real ones"
                ),
            });
        }

        // A leader that stepped down while it could still reach a majority.
        // Counted here rather than reported as a violation: it costs
        // availability, not safety, and a rule that costs only availability is
        // demonstrated by comparing two arms rather than by failing one.
        if stopped_leading {
            let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
            let quorum = ids.len() / 2 + 1;
            let reachable = ids
                .iter()
                .filter(|other| {
                    **other == id
                        || (self.nodes.get(*other).is_some_and(|n| n.alive)
                            && !self.net.is_cut(id, **other)
                            && !self.net.is_cut(**other, id))
                })
                .count();
            if reachable >= quorum {
                self.stats.leaders_deposed_with_a_quorum += 1;
                self.trace(format!(
                    "node {id} lost leadership while it still had a quorum"
                ));
            }
        }
        self.stats.highest_term = self.stats.highest_term.max(term);
        // Membership, observed on the node rather than tracked by the harness:
        // the configuration is a function of the applied log, so the node is
        // the only thing that knows what it currently is.
        if let Some(node) = self.nodes.get(&id) {
            let conf = node.core.conf();
            if conf.is_joint() {
                self.stats.joint_config_windows += 1;
            }
            if !conf.voters.is_empty() {
                self.stats.smallest_voter_set = if self.stats.smallest_voter_set == 0 {
                    conf.voters.len()
                } else {
                    self.stats.smallest_voter_set.min(conf.voters.len())
                };
            }
            let shape = (conf.voters.clone(), conf.voters_outgoing.clone());
            if self.configurations_seen.insert(shape) {
                self.stats.distinct_configurations = self.configurations_seen.len() as u64;
            }
        }
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
        // Against the model first: it is the stronger check, and naming it in
        // the report is more use than "two nodes disagree" when all of them are
        // wrong in the same way.
        if let Some(v) = self.oracle.check_against_model(id, applied, state_digest) {
            found.push(v);
        }
        if let Some(v) = self.oracle.observe_applied_state(id, applied, state_digest) {
            self.violations.push(v);
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
            // The configuration is part of the state, and on a membership
            // profile it is the *first* thing a reader needs: a commit index
            // means nothing without knowing which nodes were entitled to vote
            // for it, and two nodes disagreeing about the configuration is the
            // normal middle of a change rather than a fault.
            let conf = if s.conf.is_joint() {
                format!(
                    " voters={:?}+{:?}(joint)",
                    s.conf.voters, s.conf.voters_outgoing
                )
            } else {
                format!(" voters={:?}", s.conf.voters)
            };
            let learners = if s.conf.learners.is_empty() {
                String::new()
            } else {
                format!(" learners={:?}", s.conf.learners)
            };
            out.push_str(&format!(
                "    node {} alive={} role={:?} term={} last={} commit={} applied={}{conf}{learners}\n",
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
