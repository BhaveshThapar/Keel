use std::collections::BTreeSet;

use keel_raft::{Message, NodeId};

use crate::rng::Rng;

#[derive(Debug, Clone)]
pub struct NetConfig {
    pub min_latency_ns: u64,
    pub max_latency_ns: u64,
    pub drop_pct: u32,
    pub duplicate_pct: u32,
    /// Chance that a message is held back far longer than usual, which is what
    /// actually produces reordering: a delayed message arrives after ones sent
    /// later, and the protocol has to cope with the stale contents.
    pub straggle_pct: u32,
    pub straggle_ns: u64,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            min_latency_ns: 200_000,
            max_latency_ns: 2_000_000,
            drop_pct: 2,
            duplicate_pct: 2,
            straggle_pct: 2,
            straggle_ns: 40_000_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// Deliver once after this delay.
    Once(u64),
    /// Deliver twice, the second copy after the later delay.
    Twice(u64, u64),
    Dropped,
}

#[derive(Debug)]
pub struct Network {
    pub cfg: NetConfig,
    rng: Rng,
    /// Directed links that are down. Asymmetric partitions are the interesting
    /// ones, so links are cut in one direction at a time.
    cut: BTreeSet<(NodeId, NodeId)>,
}

impl Network {
    pub fn new(cfg: NetConfig, rng: Rng) -> Self {
        Self {
            cfg,
            rng,
            cut: BTreeSet::new(),
        }
    }

    pub fn cut_link(&mut self, from: NodeId, to: NodeId) {
        self.cut.insert((from, to));
    }

    pub fn cut_both(&mut self, a: NodeId, b: NodeId) {
        self.cut.insert((a, b));
        self.cut.insert((b, a));
    }

    pub fn heal(&mut self) {
        self.cut.clear();
    }

    pub fn is_cut(&self, from: NodeId, to: NodeId) -> bool {
        self.cut.contains(&(from, to))
    }

    /// Whether any link into or out of `id` is currently down.
    ///
    /// Deliberately "inside a partition" rather than "cut off from a quorum".
    /// The stronger reading is empty by construction, not by luck: a node that
    /// cannot reach a quorum commits nothing, so its uncommitted byte budget
    /// fills and its proposals are refused, and within a few entries it has
    /// nothing in flight left to tear. Measured over five seeds, every crash
    /// that caught a write in flight was on a node that still had a quorum.
    pub fn is_partitioned(&self, id: NodeId) -> bool {
        self.cut.iter().any(|(a, b)| *a == id || *b == id)
    }

    /// Decide what happens to a message being sent. The partition itself is
    /// checked at delivery time, not here, so that a partition forming while a
    /// message is in flight still swallows it.
    pub fn schedule(&mut self, _msg: &Message) -> Delivery {
        if self.rng.chance(self.cfg.drop_pct) {
            return Delivery::Dropped;
        }
        let base = self
            .rng
            .range(self.cfg.min_latency_ns, self.cfg.max_latency_ns + 1);
        let delay = if self.rng.chance(self.cfg.straggle_pct) {
            base + self.rng.range(0, self.cfg.straggle_ns)
        } else {
            base
        };
        if self.rng.chance(self.cfg.duplicate_pct) {
            let second = delay + self.rng.range(0, self.cfg.max_latency_ns + 1);
            Delivery::Twice(delay, second)
        } else {
            Delivery::Once(delay)
        }
    }
}
