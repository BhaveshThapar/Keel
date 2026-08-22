//! A minimal in-memory cluster for driving the core in tests.
//!
//! Everything here is deterministic: messages travel in FIFO order, persistence
//! is instantaneous, and nothing consults a clock. The full fault-injecting
//! version of this lives in `keel-sim`; this one exists so the paper-scenario
//! tests read like the scenarios they encode.

#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use bytes::Bytes;
use keel_raft::{
    Advance, ConfState, Config, Entry, EntryPayload, Index, Input, Message, NodeId, RaftCore, Role,
    Term,
};

pub struct Node {
    pub core: RaftCore,
    /// Entries the state machine has applied, in order.
    pub applied: Vec<Entry>,
    pub down: bool,
    pub reads: Vec<(u64, Index)>,
}

pub struct Cluster {
    pub nodes: BTreeMap<NodeId, Node>,
    queue: VecDeque<Message>,
    /// Directed links that drop everything.
    cut: BTreeSet<(NodeId, NodeId)>,
    pub dropped_messages: usize,
}

impl Cluster {
    pub fn new(ids: &[NodeId]) -> Self {
        Self::with_config(ids, |cfg| cfg)
    }

    pub fn with_config(ids: &[NodeId], f: impl Fn(Config) -> Config) -> Self {
        let conf = ConfState::single(ids.iter().copied());
        let nodes = ids
            .iter()
            .map(|id| {
                let cfg = f(Config::new(*id));
                (
                    *id,
                    Node {
                        core: RaftCore::new(cfg, conf.clone()),
                        applied: Vec::new(),
                        down: false,
                        reads: Vec::new(),
                    },
                )
            })
            .collect();
        Self {
            nodes,
            queue: VecDeque::new(),
            cut: BTreeSet::new(),
            dropped_messages: 0,
        }
    }

    /// Build a cluster whose nodes start with hand-written logs, as if they had
    /// just recovered from disk. `terms[i]` is the term of the entry at index
    /// `i + 1`. Used to set up the paper's log-state figures exactly.
    pub fn from_logs(specs: &[(NodeId, Vec<Term>, Term)]) -> Self {
        let ids: Vec<NodeId> = specs.iter().map(|(id, _, _)| *id).collect();
        let conf = ConfState::single(ids.iter().copied());
        let nodes = specs
            .iter()
            .map(|(id, terms, term)| {
                let entries: Vec<Entry> = terms
                    .iter()
                    .enumerate()
                    .map(|(i, t)| Entry::new(*t, i as Index + 1, EntryPayload::Noop))
                    .collect();
                let hs = keel_raft::HardState {
                    term: *term,
                    voted_for: None,
                    commit: 0,
                };
                let core = RaftCore::restore(Config::new(*id), conf.clone(), hs, None, entries);
                (
                    *id,
                    Node {
                        core,
                        applied: Vec::new(),
                        down: false,
                        reads: Vec::new(),
                    },
                )
            })
            .collect();
        Self {
            nodes,
            queue: VecDeque::new(),
            cut: BTreeSet::new(),
            dropped_messages: 0,
        }
    }

    /// The terms of a node's log entries, lowest index first.
    pub fn log_terms(&self, id: NodeId) -> Vec<Term> {
        self.nodes[&id]
            .core
            .log()
            .all_entries()
            .map(|e| e.term)
            .collect()
    }

    // ------------------------------------------------------------- faults

    /// Cut every link between the two groups, in both directions.
    pub fn partition(&mut self, a: &[NodeId], b: &[NodeId]) {
        for x in a {
            for y in b {
                self.cut.insert((*x, *y));
                self.cut.insert((*y, *x));
            }
        }
    }

    /// Cut only `from -> to`, leaving the reverse direction working.
    pub fn cut_one_way(&mut self, from: NodeId, to: NodeId) {
        self.cut.insert((from, to));
    }

    pub fn heal(&mut self) {
        self.cut.clear();
    }

    pub fn isolate(&mut self, id: NodeId) {
        let others: Vec<NodeId> = self.nodes.keys().copied().filter(|n| *n != id).collect();
        self.partition(&[id], &others);
    }

    pub fn stop(&mut self, id: NodeId) {
        if let Some(n) = self.nodes.get_mut(&id) {
            n.down = true;
        }
    }

    pub fn start(&mut self, id: NodeId) {
        if let Some(n) = self.nodes.get_mut(&id) {
            n.down = false;
        }
    }

    // -------------------------------------------------------------- driving

    pub fn tick_all(&mut self) {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in ids {
            if self.nodes[&id].down {
                continue;
            }
            let _ = self.nodes.get_mut(&id).map(|n| n.core.step(Input::Tick));
            self.pump(id);
        }
    }

    /// Deliver every message currently in flight, pumping each receiver.
    /// Repeats until the network goes quiet or `max_rounds` is reached.
    pub fn deliver_all(&mut self, max_rounds: usize) {
        for _ in 0..max_rounds {
            if self.queue.is_empty() {
                return;
            }
            let batch: Vec<Message> = self.queue.drain(..).collect();
            for m in batch {
                self.deliver(m);
            }
        }
    }

    fn deliver(&mut self, m: Message) {
        if self.cut.contains(&(m.from, m.to)) || self.nodes.get(&m.to).is_none_or(|n| n.down) {
            self.dropped_messages += 1;
            return;
        }
        let to = m.to;
        if let Some(n) = self.nodes.get_mut(&to) {
            let _ = n.core.step(Input::Message(m));
        }
        self.pump(to);
    }

    /// Run the host loop for one node: persist, send, apply, acknowledge.
    pub fn pump(&mut self, id: NodeId) {
        loop {
            let Some(node) = self.nodes.get_mut(&id) else {
                return;
            };
            if node.down || !node.core.has_ready() {
                return;
            }
            let rd = node.core.ready();

            // Persist. This harness has no disk, so an append is durable at once.
            let persisted = rd.entries.last().map(|e| (e.index, e.term));

            let applied = rd.committed_entries.last().map(|e| e.index);
            for e in &rd.committed_entries {
                node.applied.push(e.clone());
            }
            for rs in &rd.read_states {
                node.reads.push((rs.ctx, rs.index));
            }
            let snapshot_installed = rd.snapshot_to_install.clone();

            node.core.advance(Advance {
                ready_number: rd.number,
                persisted,
                applied,
                snapshot_installed,
            });

            for m in rd.messages {
                self.queue.push_back(m);
            }
        }
    }

    /// Tick every node once and settle the network. One "round" of cluster time.
    pub fn run(&mut self, rounds: usize) {
        for _ in 0..rounds {
            self.tick_all();
            self.deliver_all(64);
        }
    }

    /// Tick until a leader emerges, or panic with the state that failed to
    /// converge. Keeps tests from hanging silently.
    pub fn elect_leader(&mut self) -> NodeId {
        for _ in 0..200 {
            self.run(1);
            if let Some(id) = self.leader() {
                // Let the no-op commit so the leader can actually serve.
                self.run(3);
                return id;
            }
        }
        panic!("no leader after 200 rounds: {:?}", self.roles());
    }

    // ------------------------------------------------------------ inspection

    pub fn leader(&self) -> Option<NodeId> {
        let leaders: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|(_, n)| !n.down && n.core.role() == Role::Leader)
            .map(|(id, _)| *id)
            .collect();
        match leaders.len() {
            1 => Some(leaders[0]),
            _ => None,
        }
    }

    pub fn roles(&self) -> Vec<(NodeId, Role, Term)> {
        self.nodes
            .iter()
            .map(|(id, n)| (*id, n.core.role(), n.core.term()))
            .collect()
    }

    pub fn node(&self, id: NodeId) -> &RaftCore {
        &self.nodes[&id].core
    }

    pub fn node_mut(&mut self, id: NodeId) -> &mut RaftCore {
        &mut self.nodes.get_mut(&id).expect("unknown node").core
    }

    pub fn propose(&mut self, id: NodeId, ctx: u64, value: &str) {
        let _ = self
            .nodes
            .get_mut(&id)
            .expect("unknown node")
            .core
            .step(Input::Propose {
                ctx,
                data: Bytes::copy_from_slice(value.as_bytes()),
            });
        self.pump(id);
    }

    /// Values applied by a node, in order, ignoring no-ops and conf changes.
    pub fn applied_values(&self, id: NodeId) -> Vec<String> {
        self.nodes[&id]
            .applied
            .iter()
            .filter_map(|e| match &e.payload {
                EntryPayload::Normal(b) => Some(String::from_utf8_lossy(b).into_owned()),
                _ => None,
            })
            .collect()
    }

    /// Panics unless every live node has applied an identical prefix. This is
    /// State Machine Safety, checked directly.
    pub fn assert_applied_prefixes_agree(&self) {
        let live: Vec<&Node> = self.nodes.values().filter(|n| !n.down).collect();
        for a in &live {
            for b in &live {
                let n = a.applied.len().min(b.applied.len());
                for i in 0..n {
                    assert_eq!(
                        (a.applied[i].index, a.applied[i].term, &a.applied[i].payload),
                        (b.applied[i].index, b.applied[i].term, &b.applied[i].payload),
                        "applied prefixes diverge at position {i}"
                    );
                }
            }
        }
    }
}
