#![allow(clippy::unwrap_used, clippy::expect_used)]

//! M1's exit criterion, as a test: a three-node cluster of real processes
//! serving real traffic.
//!
//! Real processes, not three `Node`s in one test binary. An in-process cluster
//! shares an allocator, a scheduler and a filesystem view, and its own doc
//! comment in `keel-raft` admits its messages are FIFO and its persistence is
//! instantaneous. This one talks over sockets, fsyncs to real files, and can be
//! killed with a signal — which is what the phases after this one need.

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use keel_client::Client;

/// A port nobody is using, released before the server binds it.
///
/// A race in principle and not in practice: the window is microseconds and the
/// alternative is a fixed port, which fails whenever two runs overlap — which
/// on a CI runner is every time.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn addr(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().unwrap()
}

/// Where `cargo` put the server binary.
///
/// Derived from this test's own path rather than assumed, so it works under
/// `--release` and under a target directory somebody moved.
fn server_binary() -> PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop(); // the test binary's hash directory
    if path.ends_with("deps") {
        path.pop();
    }
    path.join("keel-server")
}

struct Cluster {
    processes: Vec<Child>,
    client_addrs: Vec<SocketAddr>,
    _dir: tempfile::TempDir,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in &mut self.processes {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Cluster {
    /// Start `n` nodes and wait until each has written its ready file.
    fn start(n: u64) -> Cluster {
        let binary = server_binary();
        assert!(
            binary.exists(),
            "keel-server was not built at {binary:?}; the test needs it as a \
             dev-dependency so cargo builds it first"
        );

        let dir = tempfile::tempdir().unwrap();
        let peers: Vec<(u64, u16)> = (1..=n).map(|id| (id, free_port())).collect();
        let client_ports: Vec<u16> = (0..n).map(|_| free_port()).collect();
        let admin_ports: Vec<u16> = (0..n).map(|_| free_port()).collect();

        let mut processes = Vec::new();
        for (index, (id, peer_port)) in peers.iter().enumerate() {
            let node_dir = dir.path().join(format!("node{id}"));
            let mut command = Command::new(&binary);
            command
                .arg("--id")
                .arg(id.to_string())
                .arg("--dir")
                .arg(&node_dir)
                .arg("--listen")
                .arg(addr(*peer_port).to_string())
                .arg("--client")
                .arg(addr(client_ports[index]).to_string())
                .arg("--admin")
                .arg(addr(admin_ports[index]).to_string())
                // Nothing here is a durability measurement, and F_FULLFSYNC on
                // a laptop would make the test a measurement of the laptop.
                .arg("--sync")
                .arg("none")
                .arg("--tick-ms")
                .arg("5");
            for (peer, port) in &peers {
                command.arg("--peer").arg(format!("{peer}={}", addr(*port)));
            }
            processes.push(
                command
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped())
                    .spawn()
                    .expect("spawn keel-server"),
            );
        }

        let cluster = Cluster {
            processes,
            client_addrs: client_ports.iter().map(|p| addr(*p)).collect(),
            _dir: dir,
        };

        // The ready file is the point of the ready file: a supervisor that
        // waited for the port would learn only that a socket was bound.
        for id in 1..=n {
            let ready = dir_of(&cluster, id).join("keel.ready");
            wait_for(Duration::from_secs(30), || ready.exists())
                .unwrap_or_else(|| panic!("node {id} never wrote its ready file"));
        }
        cluster
    }

    fn client(&self, nonce: u64) -> Client {
        Client::new(&self.client_addrs, nonce)
    }

    /// Kill node `index` outright. No shutdown, no flush.
    fn kill(&mut self, index: usize) {
        let _ = self.processes[index].kill();
        let _ = self.processes[index].wait();
    }
}

fn dir_of(cluster: &Cluster, id: u64) -> PathBuf {
    cluster._dir.path().join(format!("node{id}"))
}

fn wait_for(limit: Duration, mut done: impl FnMut() -> bool) -> Option<()> {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if done() {
            return Some(());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    None
}

fn ready_file_says_durable(path: &Path) -> bool {
    std::fs::read_to_string(path)
        .map(|s| s.contains("\"sync_mode\":"))
        .unwrap_or(false)
}

/// The M1 criterion. Three real nodes, a client that finds the leader by
/// itself, and writes that read back.
#[test]
fn a_three_node_cluster_serves_traffic() {
    let cluster = Cluster::start(3);

    for id in 1..=3 {
        assert!(
            ready_file_says_durable(&dir_of(&cluster, id).join("keel.ready")),
            "node {id}'s ready file does not describe its sync mode"
        );
    }

    let mut client = cluster.client(1);
    let session = client.register().expect("register");
    assert!(session > 0, "the cluster handed out client id {session}");

    for i in 0..25u32 {
        client
            .put(format!("k{i:03}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap_or_else(|e| panic!("put {i} failed: {e}"));
    }
    for i in 0..25u32 {
        assert_eq!(
            client.get(format!("k{i:03}").as_bytes()).unwrap(),
            Some(format!("v{i}").into_bytes()),
            "key {i} did not read back"
        );
    }

    assert_eq!(client.get(b"never-written").unwrap(), None);

    let rows = client.scan(Some(b"k00"), Some(b"k01"), 100).unwrap();
    assert_eq!(
        rows.len(),
        10,
        "a scan of k000..k010 returned {}",
        rows.len()
    );
    assert_eq!(rows[0].0, b"k000");

    client.delete(b"k000").unwrap();
    assert_eq!(client.get(b"k000").unwrap(), None);

    assert_eq!(client.incr(b"counter", 5).unwrap(), 5);
    assert_eq!(client.incr(b"counter", -2).unwrap(), 3);

    assert_eq!(client.cas(b"fresh", None, Some(b"first")).unwrap(), None);
    assert_eq!(
        client.cas(b"fresh", None, Some(b"second")).unwrap(),
        Some(Some(b"first".to_vec())),
        "a compare-and-swap against the wrong expectation took effect"
    );
}

/// A client keeps working across a leader failure, and what it wrote before is
/// still there afterwards.
#[test]
fn writes_survive_a_leader_being_killed() {
    let mut cluster = Cluster::start(3);
    let mut client = cluster.client(2);
    client.register().expect("register");

    for i in 0..10u32 {
        client
            .put(format!("before{i}").as_bytes(), b"value")
            .unwrap();
    }

    // Find the leader by asking each node, then kill it outright.
    let leader = (0..3)
        .find(|index| {
            std::fs::read_to_string(dir_of(&cluster, *index as u64 + 1).join("keel.ready"))
                .map(|s| s.contains("\"role\":\"leader\""))
                .unwrap_or(false)
        })
        // The ready file is written once, at startup, so it names the role the
        // node had then. Killing node 1 is enough either way: a cluster of
        // three survives one loss whichever one it was.
        .unwrap_or(0);
    cluster.kill(leader);

    // The survivors elect somebody. The client finds them by redirect.
    for i in 0..10u32 {
        client
            .put(format!("after{i}").as_bytes(), b"value")
            .unwrap_or_else(|e| panic!("write {i} after the kill failed: {e}"));
    }

    for i in 0..10u32 {
        assert_eq!(
            client.get(format!("before{i}").as_bytes()).unwrap(),
            Some(b"value".to_vec()),
            "a write acknowledged before the kill was lost"
        );
        assert_eq!(
            client.get(format!("after{i}").as_bytes()).unwrap(),
            Some(b"value".to_vec())
        );
    }
}

/// A history is recorded in the shape an external checker wants: an invocation
/// and a completion per operation, with a third outcome for the ones whose
/// answer never arrived.
#[test]
fn a_client_records_a_history_a_checker_can_read() {
    let cluster = Cluster::start(3);
    let mut client = cluster.client(3).recording();
    client.register().expect("register");

    for i in 0..10u32 {
        client.put(format!("h{i}").as_bytes(), b"v").unwrap();
        client.get(format!("h{i}").as_bytes()).unwrap();
    }

    let history = client.take_history().expect("recording");
    assert_eq!(history.len(), 20, "twenty operations were not recorded");
    assert_eq!(history.pending(), 0, "an operation was left in flight");

    let jsonl = history.to_jsonl();
    assert_eq!(jsonl.lines().count(), 20);
    for entry in history.entries() {
        assert!(
            entry.completed.is_some_and(|c| c >= entry.invoked),
            "an operation completed before it was invoked"
        );
    }
}

/// A node told to run with no peers in its own list refuses to start rather
/// than running as a cluster of one that nobody can reach.
#[test]
fn a_misconfigured_node_refuses_to_start() {
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(server_binary())
        .arg("--id")
        .arg("7")
        .arg("--dir")
        .arg(dir.path())
        .arg("--listen")
        .arg(addr(free_port()).to_string())
        .arg("--client")
        .arg(addr(free_port()).to_string())
        .arg("--admin")
        .arg(addr(free_port()).to_string())
        .arg("--peer")
        .arg(format!("1={}", addr(free_port())))
        .output()
        .expect("run keel-server");

    assert!(
        !output.status.success(),
        "a node not in its own peer list started"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not in its own peer list"),
        "the refusal did not say why: {stderr}"
    );
}
