//! Three real servers, wired so their links can be cut one at a time.
//!
//! The wiring is the interesting part. `keel-server` takes one peer map, so if
//! every node were given the same one, every node would reach node 3 at the same
//! address and a proxy in front of it could only cut *everybody's* traffic to 3.
//! That is a node failure wearing a partition's clothes, and it is the reason so
//! many chaos harnesses only ever produce symmetric partitions.
//!
//! Each node is therefore given its **own** peer map, pointing at a proxy
//! dedicated to that ordered pair. There are `n * (n - 1)` proxies for `n`
//! nodes, named `n0->n1` and so on, and cutting one cuts exactly one direction
//! between exactly two nodes. Everything else — isolating a node inbound but not
//! outbound, splitting the cluster, a one-way link that makes a follower
//! campaign forever while the leader still counts its acknowledgement — is built
//! out of that.
//!
//! Clients are *not* proxied. A partition that also hid the cluster from its
//! clients would make every fault look like a timeout, and the interesting
//! answer — a node that is up, reachable, and unable to serve because it lost
//! contact with the majority — would never be observed.

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::nemesis::Process;
use crate::proxy::{Cut, Link, Mesh};
use crate::{Action, ChaosError};

/// An address nothing is listening on yet.
///
/// Bind, read the port, drop. There is a window between the drop and the
/// server's own bind, which is why nodes are started immediately and why a
/// failure to bind is reported as a startup failure rather than retried into a
/// different port that the peer maps no longer name.
fn free_port() -> Result<SocketAddr, ChaosError> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    drop(listener);
    Ok(addr)
}

pub struct ClusterConfig {
    pub nodes: usize,
    pub dir: PathBuf,
    /// The `keel-server` binary.
    pub server_bin: PathBuf,
    /// `durable`, `barrier`, or `none`. A chaos run that claims to survive
    /// crashes must be `durable`; the others are here so the claim can be shown
    /// to depend on it.
    pub sync: String,
    pub tick_ms: u64,
    /// Environment every node is started with — where the clock nemesis's
    /// preload goes.
    pub env: Vec<(String, String)>,
}

impl ClusterConfig {
    pub fn new(nodes: usize, dir: impl Into<PathBuf>, server_bin: impl Into<PathBuf>) -> Self {
        Self {
            nodes,
            dir: dir.into(),
            server_bin: server_bin.into(),
            sync: "durable".into(),
            tick_ms: 10,
            env: Vec::new(),
        }
    }
}

pub struct Cluster {
    cfg: ClusterConfig,
    /// Where each node really listens for peers.
    peer_addrs: Vec<SocketAddr>,
    /// Where each node listens for clients. Never proxied.
    pub client_addrs: Vec<SocketAddr>,
    pub admin_addrs: Vec<SocketAddr>,
    mesh: Mesh,
    processes: Vec<Process>,
}

fn link_name(from: usize, to: usize) -> String {
    format!("n{from}->n{to}")
}

impl Cluster {
    /// Allocate addresses, stand up the proxy mesh, and start every node.
    ///
    /// Returns only once every node has published its ready file, so a caller
    /// that immediately injects a fault is injecting it into a cluster that has
    /// finished recovering rather than into one that is still replaying its log.
    pub fn start(cfg: ClusterConfig) -> Result<Self, ChaosError> {
        let n = cfg.nodes;
        let mut peer_addrs = Vec::with_capacity(n);
        let mut client_addrs = Vec::with_capacity(n);
        let mut admin_addrs = Vec::with_capacity(n);
        for _ in 0..n {
            peer_addrs.push(free_port()?);
            client_addrs.push(free_port()?);
            admin_addrs.push(free_port()?);
        }

        // One proxy per ordered pair. `from` dials it; it forwards to `to`.
        let mut mesh = Mesh::new();
        let mut route: BTreeMap<(usize, usize), SocketAddr> = BTreeMap::new();
        for from in 0..n {
            for (to, target) in peer_addrs.iter().enumerate() {
                if from == to {
                    continue;
                }
                let link = Link::start(
                    &link_name(from, to),
                    "127.0.0.1:0".parse().map_err(|_| {
                        ChaosError::Io(std::io::Error::other("127.0.0.1:0 did not parse"))
                    })?,
                    *target,
                )?;
                route.insert((from, to), link.listen);
                mesh.add(link);
            }
        }

        let mut processes = Vec::with_capacity(n);
        for i in 0..n {
            let dir = cfg.dir.join(format!("n{i}"));
            std::fs::create_dir_all(&dir)?;
            let mut args = vec![
                "--id".into(),
                (i as u64 + 1).to_string(),
                "--dir".into(),
                dir.to_string_lossy().into_owned(),
                "--listen".into(),
                peer_addrs[i].to_string(),
                "--admin".into(),
                admin_addrs[i].to_string(),
                "--client".into(),
                client_addrs[i].to_string(),
                "--sync".into(),
                cfg.sync.clone(),
                "--tick-ms".into(),
                cfg.tick_ms.to_string(),
            ];
            for j in 0..n {
                // Its own entry is its real address: a node that reached itself
                // through a proxy could be partitioned from itself, which is not
                // a fault any machine produces.
                let addr = if i == j {
                    peer_addrs[i]
                } else {
                    route
                        .get(&(i, j))
                        .copied()
                        .ok_or_else(|| ChaosError::NoBinary(link_name(i, j)))?
                };
                args.push("--peer".into());
                args.push(format!("{}={}", j as u64 + 1, addr));
            }
            processes.push(Process::new(
                &format!("n{i}"),
                &cfg.server_bin,
                args,
                cfg.env.clone(),
                dir.join("node.log"),
            ));
        }

        let mut cluster = Self {
            cfg,
            peer_addrs,
            client_addrs,
            admin_addrs,
            mesh,
            processes,
        };
        for i in 0..n {
            cluster.start_node(i)?;
        }
        Ok(cluster)
    }

    pub fn nodes(&self) -> usize {
        self.cfg.nodes
    }

    pub fn dir(&self) -> &Path {
        &self.cfg.dir
    }

    fn ready_file(&self, i: usize) -> PathBuf {
        self.cfg.dir.join(format!("n{i}")).join("keel.ready")
    }

    pub fn start_node(&mut self, i: usize) -> Result<(), ChaosError> {
        // A stale ready file from the node's previous life would make the wait
        // below return before the restarted node had recovered anything.
        let ready = self.ready_file(i);
        let _ = std::fs::remove_file(&ready);
        let p = self
            .processes
            .get_mut(i)
            .ok_or_else(|| ChaosError::NotRunning(format!("n{i}")))?;
        p.start()?;
        p.wait_ready(&ready, Duration::from_secs(30))
    }

    pub fn process(&mut self, i: usize) -> Result<&mut Process, ChaosError> {
        self.processes
            .get_mut(i)
            .ok_or_else(|| ChaosError::NotRunning(format!("n{i}")))
    }

    /// Cut one node off. `Cut::Forward` means it can receive but not send,
    /// `Cut::Backward` that it can send but not receive.
    ///
    /// The second is the one worth having. A node that can send but not receive
    /// keeps its term climbing and never learns it lost; when the partition
    /// heals it arrives with a term nobody has seen and deposes a leader that
    /// was doing fine. A harness that only cuts both directions never produces
    /// it.
    pub fn isolate(&self, node: usize, cut: Cut) {
        let outbound = matches!(cut, Cut::Forward | Cut::Both);
        let inbound = matches!(cut, Cut::Backward | Cut::Both);
        for other in 0..self.cfg.nodes {
            if other == node {
                continue;
            }
            if outbound && let Some(link) = self.mesh.get(&link_name(node, other)) {
                link.set(Cut::Both);
            }
            if inbound && let Some(link) = self.mesh.get(&link_name(other, node)) {
                link.set(Cut::Both);
            }
        }
    }

    /// Cut the cluster in two. Traffic within each side still flows.
    pub fn split(&self, minority: &[usize]) {
        for from in 0..self.cfg.nodes {
            for to in 0..self.cfg.nodes {
                if from == to {
                    continue;
                }
                let across = minority.contains(&from) != minority.contains(&to);
                if across && let Some(link) = self.mesh.get(&link_name(from, to)) {
                    link.set(Cut::Both);
                }
            }
        }
    }

    pub fn heal(&self) {
        self.mesh.heal();
    }

    /// How much the mesh carried and how much it refused. A partition that
    /// refused nothing did not happen.
    pub fn traffic(&self) -> (u64, u64, u64) {
        let mut carried = 0;
        let mut refused = 0;
        let mut severed = 0;
        for name in self.mesh.names() {
            if let Some(link) = self.mesh.get(&name) {
                let (_, fwd, back, r, s) = link.counters.snapshot();
                carried += fwd + back;
                refused += r;
                severed += s;
            }
        }
        (carried, refused, severed)
    }

    /// Apply one scheduled action. Clock jumps are the caller's business — the
    /// offset file is shared by every node, so it does not belong to any one
    /// cluster member.
    pub fn apply(&mut self, action: &Action) -> Result<(), ChaosError> {
        match action {
            Action::Isolate { node, cut } => {
                self.isolate(*node, *cut);
                Ok(())
            }
            Action::Split { minority } => {
                self.split(minority);
                Ok(())
            }
            Action::Heal => {
                self.heal();
                Ok(())
            }
            Action::Pause { node } => self.process(*node)?.stop(),
            Action::Resume { node } => self.process(*node)?.resume(),
            Action::Kill { node } => self.process(*node)?.kill(),
            Action::Restart { node } => self.start_node(*node),
            Action::ClockJump { .. } => Ok(()),
        }
    }

    /// Every node's peer address. For a test that wants to check the proxies
    /// really are in the middle.
    pub fn peer_addrs(&self) -> &[SocketAddr] {
        &self.peer_addrs
    }

    /// The names of every link, for a test that wants to assert the mesh is
    /// complete.
    pub fn link_names(&self) -> Vec<String> {
        self.mesh.names()
    }

    pub fn link_is_cut(&self, from: usize, to: usize) -> bool {
        self.mesh
            .get(&link_name(from, to))
            .map(|l| l.cut() != Cut::None)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cluster with no processes, to test the wiring alone. Standing up three
    /// servers is an integration test's job; the routing arithmetic is worth
    /// checking without them.
    fn mesh_only(n: usize) -> Cluster {
        let dir = std::env::temp_dir().join(format!("keel-chaos-mesh-{n}"));
        let mut mesh = Mesh::new();
        for from in 0..n {
            for to in 0..n {
                if from == to {
                    continue;
                }
                let target: SocketAddr = "127.0.0.1:1".parse().expect("literal");
                let link = Link::start(
                    &link_name(from, to),
                    "127.0.0.1:0".parse().expect("literal"),
                    target,
                )
                .expect("bind a loopback listener");
                mesh.add(link);
            }
        }
        Cluster {
            cfg: ClusterConfig::new(n, dir, "/nonexistent"),
            peer_addrs: Vec::new(),
            client_addrs: Vec::new(),
            admin_addrs: Vec::new(),
            mesh,
            processes: Vec::new(),
        }
    }

    #[test]
    fn every_ordered_pair_gets_its_own_link() {
        let c = mesh_only(3);
        let mut names = c.link_names();
        names.sort();
        assert_eq!(
            names,
            vec!["n0->n1", "n0->n2", "n1->n0", "n1->n2", "n2->n0", "n2->n1"]
        );
    }

    /// The property the per-node peer map exists for: cutting one node's
    /// outbound traffic must leave every other pair alone.
    #[test]
    fn isolating_one_node_leaves_the_rest_of_the_cluster_connected() {
        let c = mesh_only(3);
        c.isolate(1, Cut::Both);
        assert!(c.link_is_cut(1, 0) && c.link_is_cut(1, 2));
        assert!(c.link_is_cut(0, 1) && c.link_is_cut(2, 1));
        assert!(
            !c.link_is_cut(0, 2) && !c.link_is_cut(2, 0),
            "isolating n1 also cut the link between n0 and n2"
        );
    }

    /// The asymmetric case, which is the one that produces a node whose term
    /// climbs while it is deposed by nobody.
    #[test]
    fn a_one_way_isolation_cuts_one_direction_only() {
        let c = mesh_only(3);
        c.isolate(0, Cut::Backward); // can send, cannot receive
        assert!(
            c.link_is_cut(1, 0) && c.link_is_cut(2, 0),
            "inbound survived"
        );
        assert!(
            !c.link_is_cut(0, 1) && !c.link_is_cut(0, 2),
            "outbound was cut too, which makes it symmetric"
        );
    }

    #[test]
    fn a_split_cuts_across_and_not_within() {
        let c = mesh_only(5);
        c.split(&[3, 4]);
        // Across, both ways.
        assert!(c.link_is_cut(0, 3) && c.link_is_cut(3, 0));
        // Within the majority.
        assert!(!c.link_is_cut(0, 1) && !c.link_is_cut(1, 2));
        // Within the minority: they can still talk to each other, which is what
        // lets them elect nobody and prove they cannot.
        assert!(!c.link_is_cut(3, 4) && !c.link_is_cut(4, 3));
    }

    #[test]
    fn healing_restores_every_link() {
        let c = mesh_only(3);
        c.split(&[2]);
        assert!(c.link_is_cut(0, 2));
        c.heal();
        for from in 0..3 {
            for to in 0..3 {
                if from != to {
                    assert!(!c.link_is_cut(from, to), "n{from}->n{to} stayed cut");
                }
            }
        }
    }
}
