//! The Ready loop.

use std::collections::VecDeque;

use bytes::Bytes;
use keel_api::{ClientId, Peer, Proposal, Response, Seq, decode, encode};
use keel_log::{Fs, Log};
use keel_net::Transport;
use keel_raft::{
    Advance, ConfState, Config, DropReason, Entry, EntryPayload, Index, Input, NodeId, RaftCore,
    Restored, Role, Status,
};
use keel_sm::{StateMachine, Store};

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("the log failed: {0}")]
    Log(#[from] keel_log::Error),
    #[error("the state machine failed: {0}")]
    StateMachine(#[from] keel_sm::StateMachineError),
    #[error("the transport failed: {0}")]
    Transport(String),
    /// A committed entry this node itself proposed does not decode. Not
    /// recoverable: the log holds something no build can apply, and skipping it
    /// would put this node's state machine out of step with its peers'.
    #[error("a committed entry is malformed at index {index}: {why}")]
    MalformedEntry { index: Index, why: String },
}

/// What one turn of the loop did.
///
/// Counters rather than a boolean, because the interesting questions about a
/// host loop are all quantitative: did a hundred proposals cost one append or a
/// hundred, did a heartbeat round go out, how much was applied.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Turn {
    /// `Ready` structs handled. At most one per turn, so this is 0 or 1.
    pub readies: u64,
    pub entries_appended: u64,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub entries_applied: u64,
    /// Proposals refused before they reached the log, with the reason the core
    /// gave — not the leader, overloaded, a conf change already in flight.
    pub proposals_dropped: u64,
}

impl Turn {
    fn did_something(&self) -> bool {
        *self != Turn::default()
    }
}

/// Cumulative counters for the whole node.
#[derive(Debug, Clone, Copy, Default)]
pub struct Progress {
    pub turns: u64,
    pub readies: u64,
    pub entries_appended: u64,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub entries_applied: u64,
    pub proposals_dropped: u64,
}

impl Progress {
    fn add(&mut self, turn: Turn) {
        self.turns += 1;
        self.readies += turn.readies;
        self.entries_appended += turn.entries_appended;
        self.messages_sent += turn.messages_sent;
        self.messages_received += turn.messages_received;
        self.entries_applied += turn.entries_applied;
        self.proposals_dropped += turn.proposals_dropped;
    }
}

/// A response, and enough to say whose it is.
#[derive(Debug, Clone)]
pub struct Answer {
    /// The log index that produced it.
    pub index: Index,
    /// The `(client, seq)` the proposal carried, when it carried one.
    pub session: Option<(ClientId, Seq)>,
    /// The nonce, when the proposal was a registration.
    ///
    /// A registration is the one request with no session pair — it is asking
    /// for one — so it has no identity in `session` and a host that matched it
    /// on anything else would be matching it on nothing. Two clients
    /// registering at the same moment would then be handed each other's
    /// identities, and a client that later retried its registration would end
    /// up *sharing* a `ClientId` with the other one: its next request would hit
    /// the other's dedup cache, be acknowledged, and never apply. That is
    /// [KEEL-9](../../../BUGS.md), and this field is why it cannot happen
    /// again.
    pub registration: Option<u64>,
    pub response: Response,
}

/// A proposal waiting to be stepped into the core.
struct Queued {
    ctx: u64,
    proposal: Proposal,
}

/// One node: a core, a log, a state machine, and a transport.
pub struct Node<F: Fs, S: Store, T: Transport> {
    core: RaftCore,
    log: Log<F>,
    sm: StateMachine<S>,
    transport: T,
    /// Proposals accepted from clients and not yet stepped into the core.
    ///
    /// The queue is the group-commit mechanism, and it is deliberately not a
    /// batching *policy*: there is no size threshold and no timer. Everything
    /// queued between two turns goes into one `Ready`, so the batch size is
    /// however much arrived while the last one was being made durable — which
    /// is the size that makes the fsync pay for itself, arrived at without a
    /// tuning knob.
    queue: VecDeque<Queued>,
    /// Responses produced by applying. The host hands these back; this crate
    /// does not know what a client connection is.
    answers: Vec<Answer>,
    /// Proposals the core refused, with why.
    refusals: Vec<(u64, DropReason)>,
    /// Reads the core has confirmed, waiting for the host to notice.
    reads: Vec<(u64, Index)>,
    /// Next context number for a proposal that came in without one.
    next_ctx: u64,
    progress: Progress,
}

impl<F: Fs, S: Store, T: Transport> Node<F, S, T> {
    /// Build a node from a recovered log and a store.
    ///
    /// The core is told what the *state machine* has applied, not what the log
    /// infers. They differ after a restart, and the state machine's number is
    /// the true one because it was written in the same atomic batch as the data
    /// (ADR-010).
    pub fn new(
        cfg: Config,
        conf: ConfState,
        log: Log<F>,
        recovered: keel_log::Recovered,
        sm: StateMachine<S>,
        transport: T,
    ) -> Self {
        let core = RaftCore::restore(
            cfg,
            Restored {
                conf,
                hard_state: recovered.hard_state,
                snapshot: recovered.snapshot,
                entries: recovered.entries,
                applied: sm.applied(),
            },
        );
        Self {
            core,
            log,
            sm,
            transport,
            queue: VecDeque::new(),
            answers: Vec::new(),
            refusals: Vec::new(),
            reads: Vec::new(),
            next_ctx: 1,
            progress: Progress::default(),
        }
    }

    pub fn id(&self) -> NodeId {
        self.core.status().id
    }

    pub fn status(&self) -> Status {
        self.core.status()
    }

    pub fn role(&self) -> Role {
        self.core.status().role
    }

    pub fn progress(&self) -> Progress {
        self.progress
    }

    pub fn log(&self) -> &Log<F> {
        &self.log
    }

    pub fn state_machine(&self) -> &StateMachine<S> {
        &self.sm
    }

    /// Accept a client proposal. Returns the context it will be answered under.
    ///
    /// Queued, not stepped. Everything queued between two turns becomes one
    /// `Ready`, which is one append and one fsync however many there are.
    pub fn propose(&mut self, proposal: Proposal) -> u64 {
        let ctx = self.next_ctx;
        self.next_ctx += 1;
        self.queue.push_back(Queued { ctx, proposal });
        ctx
    }

    /// How many proposals are waiting for the next turn.
    pub fn queued(&self) -> usize {
        self.queue.len()
    }

    /// Take the responses produced since the last call.
    pub fn take_answers(&mut self) -> Vec<Answer> {
        std::mem::take(&mut self.answers)
    }

    /// Ask for a linearizable read.
    ///
    /// The answer does not come back here. The core confirms it is still the
    /// leader by heartbeat and then reports the index the read must see through
    /// [`Node::take_reads`]; the host answers the client once it has applied
    /// that far. That round trip is what makes the read linearizable, and it is
    /// why a read is not simply a lookup.
    pub fn read_index(&mut self, ctx: u64) {
        let _ = self.core.step(Input::ReadIndex { ctx });
    }

    /// Reads the core has confirmed since the last call: the context the caller
    /// gave, and the index its answer must reflect.
    pub fn take_reads(&mut self) -> Vec<(u64, Index)> {
        std::mem::take(&mut self.reads)
    }

    /// How far the state machine has applied.
    pub fn applied(&self) -> Index {
        self.sm.applied()
    }

    /// Take the refusals produced since the last call.
    pub fn take_refusals(&mut self) -> Vec<(u64, DropReason)> {
        std::mem::take(&mut self.refusals)
    }

    /// Advance the node's own clock by one tick.
    pub fn tick(&mut self) {
        // The core's `step` cannot fail on a tick; it has nowhere to refuse to.
        let _ = self.core.step(Input::Tick);
    }

    /// One turn: receive, drain the queue, pump the `Ready`, flush.
    ///
    /// Returns what it did, so a caller can tell an idle turn from a busy one
    /// without guessing.
    pub fn turn(&mut self) -> Result<Turn, NodeError> {
        let mut turn = Turn::default();
        self.receive(&mut turn)?;
        self.drain_queue();
        self.pump(&mut turn)?;
        self.transport
            .flush()
            .map_err(|e| NodeError::Transport(e.to_string()))?;
        self.progress.add(turn);
        Ok(turn)
    }

    /// Turn until nothing happens, or `limit` turns have passed.
    ///
    /// The limit is not a nicety: a node whose peer is answering every message
    /// with another message has no fixed point, and a loop without a bound would
    /// stop servicing its own timer.
    pub fn run_until_idle(&mut self, limit: usize) -> Result<Progress, NodeError> {
        for _ in 0..limit {
            if !self.turn()?.did_something() {
                break;
            }
        }
        Ok(self.progress)
    }

    fn receive(&mut self, turn: &mut Turn) -> Result<(), NodeError> {
        loop {
            let received = self
                .transport
                .recv()
                .map_err(|e| NodeError::Transport(e.to_string()))?;
            let Some(received) = received else {
                return Ok(());
            };
            turn.messages_received += 1;

            // A frame that does not decode is a peer speaking a protocol this
            // build does not. Dropping it is the whole response: there is no
            // reply that would help, and refusing to run would let one bad peer
            // stop a healthy node.
            match decode::<Peer>(&received.frame) {
                Ok(Peer::Raft(message)) => {
                    let _ = self.core.step(Input::Message(message));
                }
                // Snapshot chunks are M2's; a node that receives one before then
                // is talking to a build from the future.
                Ok(Peer::SnapshotChunk { .. }) | Err(_) => {}
            }
        }
    }

    /// Step every queued proposal into the core, so they share one `Ready`.
    fn drain_queue(&mut self) {
        while let Some(queued) = self.queue.pop_front() {
            // An encoding failure is this node's own doing, not a peer's, and
            // it cannot be proposed. Refusing it as overloaded is the closest
            // honest answer: the entry never reaches the log.
            let Ok(bytes) = encode(&queued.proposal) else {
                self.refusals.push((queued.ctx, DropReason::Overloaded));
                continue;
            };
            let _ = self.core.step(Input::Propose {
                ctx: queued.ctx,
                data: Bytes::from(bytes),
            });
        }
    }

    /// The four steps, in the order that is the safety contract.
    fn pump(&mut self, turn: &mut Turn) -> Result<(), NodeError> {
        if !self.core.has_ready() {
            return Ok(());
        }
        let ready = self.core.ready();
        turn.readies += 1;

        // 1. Persist. Entries, then hard state, then one fsync covering both.
        let first_new = ready.entries.first().map(|e| e.index);
        if let Some(from) = first_new.filter(|i| *i <= self.log.last_index()) {
            self.log.truncate(from)?;
        }
        if !ready.entries.is_empty() {
            self.log.append(&ready.entries)?;
            turn.entries_appended += ready.entries.len() as u64;
        }
        if let Some(hs) = ready.hard_state {
            self.log.set_hard_state(hs)?;
        }
        let persisted = ready.entries.last().map(|e| (e.index, e.term));
        self.log.sync()?;

        // 2. Send. Not before the fsync above: a vote response that goes out
        //    first lets a crashed node grant a second vote in the same term.
        for message in &ready.messages {
            let frame = encode(&Peer::Raft(message.clone()))
                .map_err(|e| NodeError::Transport(e.to_string()))?;
            match self.transport.send(message.to, &frame) {
                Ok(()) => turn.messages_sent += 1,
                // An unreachable peer is a network condition, not an error. The
                // core is told, and consensus is built for exactly this.
                Err(_) => {
                    let _ = self
                        .core
                        .step(Input::ReportUnreachable { peer: message.to });
                }
            }
        }

        // 3. Apply.
        let mut applied = None;
        for entry in &ready.committed_entries {
            self.apply(entry)?;
            applied = Some(entry.index);
            turn.entries_applied += 1;
        }

        for read in &ready.read_states {
            self.reads.push((read.ctx, read.index));
        }
        for (ctx, reason) in ready.proposals_dropped {
            self.refusals.push((ctx, reason));
            turn.proposals_dropped += 1;
        }

        // 4. Report what got done. Watermarks, so a host that reordered its own
        //    work cannot desynchronise the core.
        self.core.advance(Advance {
            ready_number: ready.number,
            persisted,
            applied,
            snapshot_installed: ready.snapshot_to_install,
        });
        Ok(())
    }

    fn apply(&mut self, entry: &Entry) -> Result<(), NodeError> {
        match &entry.payload {
            // A no-op moves the applied index and nothing else. It still has to
            // be committed to the store, or the log would hand it back forever.
            EntryPayload::Noop | EntryPayload::ConfChange(_) => {
                self.sm.apply(
                    entry.index,
                    &Proposal {
                        stamped_ms: 0,
                        session: None,
                        body: keel_api::ProposalBody::KeepAlive,
                    },
                )?;
            }
            EntryPayload::Normal(data) => {
                let proposal = decode::<Proposal>(data).map_err(|e| NodeError::MalformedEntry {
                    index: entry.index,
                    why: e.to_string(),
                })?;
                let response = self.sm.apply(entry.index, &proposal)?;
                // Matched on the session pair rather than on the order entries
                // were proposed. A leadership change can drop a proposal and
                // renumber what follows it, so position is not an identity;
                // `(client, seq)` is one, and it is already in the entry
                // because the state machine deduplicates on it.
                //
                // A registration has no session pair yet, so its nonce travels
                // alongside for the same reason.
                let registration = match &proposal.body {
                    keel_api::ProposalBody::Register { nonce } => Some(*nonce),
                    _ => None,
                };
                self.answers.push(Answer {
                    index: entry.index,
                    session: proposal.session,
                    registration,
                    response,
                });
            }
        }
        Ok(())
    }
}
