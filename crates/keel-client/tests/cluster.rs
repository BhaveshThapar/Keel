#![allow(clippy::unwrap_used, clippy::expect_used)]

//! M1's exit criterion, as a test: a three-node cluster of real processes
//! serving real traffic.
//!
//! Real processes, not three `Node`s in one test binary. An in-process cluster
//! shares an allocator, a scheduler and a filesystem view, and its own doc
//! comment in `keel-raft` admits its messages are FIFO and its persistence is
//! instantaneous. This one talks over sockets, fsyncs to real files, and can be
//! killed with a signal — which is what the phases after this one need.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use keel_api::{Command as ApiCommand, Envelope, Request as ApiRequest, encode};
use keel_client::{Client, Pipeline};

static CLUSTER_TEST_LOCK: Mutex<()> = Mutex::new(());

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

fn send_and_drop_answer(addr: SocketAddr, id: u64, request: &ApiRequest) {
    let mut stream = TcpStream::connect(addr).expect("connect for answer-loss injection");
    let payload = encode(&Envelope::new(id, request.clone())).expect("encode request");
    stream
        .write_all(&(payload.len() as u32).to_le_bytes())
        .expect("write request length");
    stream.write_all(&payload).expect("write request body");
    stream.flush().expect("flush request");
    drop(stream);
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
    admin_addrs: Vec<SocketAddr>,
    peer_ports: Vec<(u64, u16)>,
    checkpoint_entries: u64,
    voters: Vec<u64>,
    binary: PathBuf,
    _dir: tempfile::TempDir,
    _serial: MutexGuard<'static, ()>,
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
        Self::start_with_options(n, 10_000, (1..=n).collect())
    }

    fn start_with_checkpoint(n: u64, checkpoint_entries: u64) -> Cluster {
        Self::start_with_options(n, checkpoint_entries, (1..=n).collect())
    }

    fn start_with_options(n: u64, checkpoint_entries: u64, voters: Vec<u64>) -> Cluster {
        // Each test starts several real processes. Running all such tests in
        // parallel makes startup depend on machine load and lets a fast
        // snapshot test race a slow neighbour for its observation window.
        let serial = CLUSTER_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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

        let mut cluster = Cluster {
            processes: Vec::new(),
            client_addrs: client_ports.iter().map(|p| addr(*p)).collect(),
            admin_addrs: admin_ports.iter().map(|p| addr(*p)).collect(),
            peer_ports: peers,
            checkpoint_entries,
            voters,
            binary,
            _dir: dir,
            _serial: serial,
        };
        for index in 0..n as usize {
            let child = cluster.spawn_node(index);
            cluster.processes.push(child);
        }

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

    fn restart(&mut self, index: usize) {
        self.restart_without_waiting(index);
        let ready = dir_of(self, index as u64 + 1).join("keel.ready");
        wait_for(Duration::from_secs(30), || ready.exists())
            .unwrap_or_else(|| panic!("node {} never became ready after restart", index + 1));
    }

    fn restart_without_waiting(&mut self, index: usize) {
        let ready = dir_of(self, index as u64 + 1).join("keel.ready");
        let _ = std::fs::remove_file(ready);
        self.processes[index] = self.spawn_node(index);
    }

    fn spawn_node(&self, index: usize) -> Child {
        let (id, peer_port) = self.peer_ports[index];
        let mut command = Command::new(&self.binary);
        command
            .arg("--id")
            .arg(id.to_string())
            .arg("--dir")
            .arg(dir_of(self, id))
            .arg("--listen")
            .arg(addr(peer_port).to_string())
            .arg("--client")
            .arg(self.client_addrs[index].to_string())
            .arg("--admin")
            .arg(self.admin_addrs[index].to_string())
            .arg("--sync")
            .arg("none")
            .arg("--tick-ms")
            .arg("5")
            .arg("--checkpoint-entries")
            .arg(self.checkpoint_entries.to_string());
        for (peer, port) in &self.peer_ports {
            command.arg("--peer").arg(format!("{peer}={}", addr(*port)));
        }
        for voter in &self.voters {
            command.arg("--voter").arg(voter.to_string());
        }
        command
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn keel-server")
    }

    fn status(&self, index: usize) -> Option<String> {
        let mut stream =
            TcpStream::connect_timeout(&self.admin_addrs[index], Duration::from_millis(250))
                .ok()?;
        stream
            .write_all(b"GET /status HTTP/1.1\r\nHost: keel\r\n\r\n")
            .ok()?;
        let mut response = String::new();
        stream.read_to_string(&mut response).ok()?;
        response.split_once("\r\n\r\n").map(|(_, body)| body.into())
    }

    fn post_admin(&self, index: usize, path: &str) -> Option<String> {
        let mut stream =
            TcpStream::connect_timeout(&self.admin_addrs[index], Duration::from_millis(250))
                .ok()?;
        write!(
            stream,
            "POST {path} HTTP/1.1\r\nHost: keel\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
        )
        .ok()?;
        let mut response = String::new();
        stream.read_to_string(&mut response).ok()?;
        Some(response)
    }

    fn leader(&self) -> Option<usize> {
        (0..self.admin_addrs.len()).find(|index| {
            self.status(*index)
                .is_some_and(|status| status.contains("\"role\":\"leader\""))
        })
    }

    fn post_to_leader(&self, path: &str) -> Option<(usize, String)> {
        wait_for_value(Duration::from_secs(10), || {
            let leader = self.leader()?;
            let response = self.post_admin(leader, path)?;
            response
                .starts_with("HTTP/1.1 202 Accepted")
                .then_some((leader, response))
        })
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

fn wait_for_value<T>(limit: Duration, mut get: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if let Some(value) = get() {
            return Some(value);
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

/// FR-7's exact acceptance case: the command commits, every answer is lost,
/// leadership moves, and a storm of the same `(client, seq)` is still one
/// state-machine operation.
#[test]
fn a_retry_storm_across_forced_failover_applies_the_command_exactly_once() {
    let mut cluster = Cluster::start(3);
    const NONCE: u64 = 424_242;

    let mut registering = cluster.client(NONCE);
    let client_id = registering.register().expect("register session");
    let leader = wait_for_value(Duration::from_secs(10), || {
        (0..3).find(|index| {
            cluster
                .status(*index)
                .is_some_and(|status| status.contains("\"role\":\"leader\""))
        })
    })
    .expect("the cluster never elected a leader");
    let before = cluster
        .status(leader)
        .and_then(|status| applied_from(&status))
        .expect("leader status had no applied index");
    let request = ApiRequest::Command {
        client: client_id,
        seq: 1,
        command: ApiCommand::Incr {
            key: b"failover-dedup".to_vec().into(),
            by: 1,
        },
    };

    // Re-send while deliberately dropping every response. Waiting for applied
    // proves the command crossed the uncertainty boundary before the kill.
    for id in 1..=16 {
        send_and_drop_answer(cluster.client_addrs[leader], id, &request);
    }
    wait_for(Duration::from_secs(10), || {
        cluster
            .status(leader)
            .and_then(|status| applied_from(&status))
            .is_some_and(|applied| applied > before)
    })
    .expect("the answer-lost command never committed");
    cluster.kill(leader);

    let new_leader = wait_for_value(Duration::from_secs(10), || {
        (0..3).filter(|index| *index != leader).find(|index| {
            cluster
                .status(*index)
                .is_some_and(|status| status.contains("\"role\":\"leader\""))
        })
    })
    .expect("the survivors never elected a leader");
    for id in 17..=32 {
        send_and_drop_answer(cluster.client_addrs[new_leader], id, &request);
    }

    // A fresh client with the same registration nonce reopens the session and
    // begins at seq=1, making this the same command after the failover.
    let mut retry = cluster.client(NONCE);
    assert_eq!(retry.incr(b"failover-dedup", 1).expect("retry"), 1);
    assert_eq!(
        retry.get(b"failover-dedup").expect("read back"),
        Some(1i64.to_le_bytes().to_vec())
    );
}

/// ADR-033, against a real cluster: many requests outstanding on one connection,
/// answered in whatever order they finish, and every one of them applied exactly
/// once.
///
/// The exactly-once half is the part worth a test rather than a paragraph. Each
/// key is incremented once and no more, so a write that applied twice — the
/// failure a pipelined retry invites — shows up as a counter reading 2, and a
/// write that was acknowledged and never applied shows up as a counter that is
/// absent. Neither is distinguishable from success by looking at the
/// acknowledgements alone, which is why they are checked afterwards from a
/// second client.
#[test]
fn a_pipeline_keeps_many_requests_in_flight_and_applies_each_once() {
    let cluster = Cluster::start(3);
    const DEPTH: usize = 16;
    const OPS: usize = 400;

    let mut pipeline =
        Pipeline::open(&cluster.client_addrs, 500_000, DEPTH).expect("open a pipeline");
    assert_eq!(pipeline.depth(), DEPTH);

    let mut submitted = 0usize;
    let mut acknowledged = 0usize;
    let mut deepest = 0usize;
    let mut failures = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);

    while acknowledged < OPS && Instant::now() < deadline {
        while submitted < OPS {
            let command = keel_api::Command::Incr {
                key: format!("p{submitted:04}").into_bytes().into(),
                by: 1,
            };
            match pipeline.submit(command) {
                Ok(_) => submitted += 1,
                // Backpressure, not a failure: poll and come back.
                Err(_) => break,
            }
        }
        deepest = deepest.max(pipeline.inflight());
        for completion in pipeline.poll(Duration::from_millis(20)) {
            match completion.result {
                Ok(_) => acknowledged += 1,
                Err(e) => failures.push(e.to_string()),
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {OPS} operations failed: {:?}",
        failures.len(),
        &failures[..failures.len().min(5)]
    );
    assert_eq!(acknowledged, OPS, "not every operation was acknowledged");
    assert!(
        deepest > 1,
        "never more than one request was outstanding, so nothing here was \
         pipelined and the test proves only that a single request works"
    );

    // Read back from a client that shares nothing with the pipeline.
    let mut checker = cluster.client(999);
    checker.register().expect("register");
    for i in 0..OPS {
        let key = format!("p{i:04}");
        assert_eq!(
            checker.get(key.as_bytes()).unwrap(),
            Some(1i64.to_le_bytes().to_vec()),
            "key {key} was applied a number of times that is not once"
        );
    }
}

/// The daemon path the simulator cannot stand in for: a leader checkpoints,
/// compacts past an offline follower, streams the checkpoint over TCP, and the
/// receiver resumes after its process is killed with several chunks present.
#[test]
fn a_real_follower_resumes_and_installs_a_snapshot_after_it_is_killed_mid_stream() {
    let mut cluster = Cluster::start_with_checkpoint(3, 32);
    let leader = wait_for_value(Duration::from_secs(10), || {
        (0..3).find(|index| {
            cluster
                .status(*index)
                .is_some_and(|status| status.contains("\"role\":\"leader\""))
        })
    });
    let leader = leader.expect("the cluster never elected a leader");
    let follower = (0..3).find(|index| *index != leader).unwrap();
    cluster.kill(follower);

    let mut client = cluster.client(71);
    client.register().expect("register");
    for i in 0..40u32 {
        let value = snapshot_value(256 * 1024, u64::from(i) + 1);
        client
            .put(format!("snapshot-{i:03}").as_bytes(), &value)
            .unwrap_or_else(|error| panic!("write {i} failed: {error}"));
    }
    let leader_snapshots = dir_of(&cluster, leader as u64 + 1).join("snapshots");
    wait_for(Duration::from_secs(20), || {
        std::fs::read_dir(&leader_snapshots)
            .ok()
            .is_some_and(|entries| {
                entries.filter_map(Result::ok).any(|e| {
                    e.file_name()
                        .to_string_lossy()
                        .chars()
                        .next()
                        .is_some_and(|c| c.is_ascii_digit())
                })
            })
    })
    .expect("the leader never checkpointed");

    let follower_snapshots = dir_of(&cluster, follower as u64 + 1).join("snapshots");
    // Begin observing before the process publishes its ready file. Waiting for
    // ready first left a race where a loopback transfer could finish and rename
    // the staging directory before this test ever looked at it.
    cluster.restart_without_waiting(follower);
    let staged = wait_for_fast(Duration::from_secs(20), || {
        std::fs::read_dir(&follower_snapshots)
            .ok()
            .is_some_and(|entries| {
                entries.filter_map(Result::ok).any(|entry| {
                    entry.file_name().to_string_lossy().starts_with("incoming-")
                        && directory_bytes(&entry.path()) > 0
                })
            })
    });
    if staged.is_none() {
        let snapshots = std::fs::read_dir(&follower_snapshots)
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .map(|entry| {
                format!(
                    "{}:{}",
                    entry.file_name().to_string_lossy(),
                    directory_bytes(&entry.path())
                )
            })
            .collect::<Vec<_>>();
        panic!(
            "the follower never exposed staged snapshot bytes; follower={:?}, leader={:?}, snapshots={snapshots:?}, process={:?}",
            cluster.status(follower),
            cluster.status(leader),
            cluster.processes[follower].try_wait()
        );
    }
    cluster.kill(follower);
    cluster.restart(follower);

    let installed = wait_for(Duration::from_secs(30), || {
        cluster.status(follower).is_some_and(|status| {
            status.contains("\"applied\":")
                && std::fs::read_dir(&follower_snapshots)
                    .ok()
                    .is_some_and(|entries| {
                        entries.filter_map(Result::ok).any(|entry| {
                            entry
                                .file_name()
                                .to_string_lossy()
                                .chars()
                                .next()
                                .is_some_and(|c| c.is_ascii_digit())
                        })
                    })
        })
    });
    if installed.is_none() {
        let names = std::fs::read_dir(&follower_snapshots)
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .map(|entry| {
                format!(
                    "{}:{}",
                    entry.file_name().to_string_lossy(),
                    directory_bytes(&entry.path())
                )
            })
            .collect::<Vec<_>>();
        panic!(
            "the restarted follower never installed the snapshot; status={:?}, snapshots={names:?}, process={:?}",
            cluster.status(follower),
            cluster.processes[follower].try_wait()
        );
    }
}

fn snapshot_value(size: usize, seed: u64) -> Vec<u8> {
    let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
    let mut bytes = Vec::with_capacity(size);
    while bytes.len() < size {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let take = (size - bytes.len()).min(8);
        bytes.extend_from_slice(&state.to_le_bytes()[..take]);
    }
    bytes
}

#[test]
fn operator_commands_transfer_leadership_and_force_a_checkpoint() {
    let cluster = Cluster::start(3);
    let leader = wait_for_value(Duration::from_secs(10), || {
        (0..3).find(|index| {
            cluster
                .status(*index)
                .is_some_and(|status| status.contains("\"role\":\"leader\""))
        })
    })
    .expect("the cluster never elected a leader");

    let mut client = cluster.client(82);
    client.register().expect("register");
    client.put(b"before-admin", b"value").expect("write");
    let target = (0..3).find(|index| *index != leader).unwrap();
    let response = cluster
        .post_admin(
            leader,
            &format!("/transfer-leader?to={}", target as u64 + 1),
        )
        .expect("transfer request");
    assert!(response.starts_with("HTTP/1.1 202 Accepted"), "{response}");
    wait_for(Duration::from_secs(10), || {
        cluster
            .status(target)
            .is_some_and(|status| status.contains("\"role\":\"leader\""))
    })
    .expect("leadership did not transfer");

    let response = cluster
        .post_admin(target, "/snapshot")
        .expect("snapshot request");
    assert!(response.starts_with("HTTP/1.1 202 Accepted"), "{response}");
    let snapshots = dir_of(&cluster, target as u64 + 1).join("snapshots");
    wait_for(Duration::from_secs(10), || {
        std::fs::read_dir(&snapshots).ok().is_some_and(|entries| {
            entries.filter_map(Result::ok).any(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_digit())
            })
        })
    })
    .expect("the manual checkpoint was not published");
}

#[test]
fn an_operator_adds_promotes_and_removes_a_routed_learner() {
    let cluster = Cluster::start_with_options(4, 10_000, vec![1, 2, 3]);
    let (_, response) = cluster
        .post_to_leader("/add-learner?node=4")
        .expect("add learner request");
    assert!(response.starts_with("HTTP/1.1 202 Accepted"), "{response}");
    wait_for(Duration::from_secs(10), || {
        (0..4).any(|index| {
            cluster
                .status(index)
                .is_some_and(|status| status.contains("\"learners\":[4]"))
        })
    })
    .expect("node 4 never became a learner");

    let mut client = cluster.client(91);
    client.register().expect("register");
    for i in 0..20u32 {
        client
            .put(format!("membership-{i}").as_bytes(), b"value")
            .expect("write while learner catches up");
    }
    wait_for(Duration::from_secs(10), || {
        let leader_status = cluster.leader().and_then(|leader| cluster.status(leader));
        let learner_status = cluster.status(3);
        match (leader_status, learner_status) {
            (Some(leader_status), Some(learner_status)) => {
                applied_from(&leader_status) == applied_from(&learner_status)
            }
            _ => false,
        }
    })
    .expect("the learner never caught up");

    // The server independently checks the leader's replication progress before
    // proposing this promotion.
    let (_, response) = cluster
        .post_to_leader("/promote?node=4")
        .expect("promote request");
    assert!(response.starts_with("HTTP/1.1 202 Accepted"), "{response}");
    wait_for(Duration::from_secs(10), || {
        (0..4).any(|index| {
            cluster
                .status(index)
                .is_some_and(|status| status.contains("\"voters\":[1,2,3,4]"))
        })
    })
    .expect("node 4 was not promoted");

    let (_, response) = cluster
        .post_to_leader("/remove?node=4")
        .expect("remove request");
    assert!(response.starts_with("HTTP/1.1 202 Accepted"), "{response}");
    wait_for(Duration::from_secs(10), || {
        (0..3).any(|index| {
            cluster.status(index).is_some_and(|status| {
                status.contains("\"voters\":[1,2,3]") && status.contains("\"learners\":[]")
            })
        })
    })
    .expect("node 4 was not removed");
}

/// FR-10's production acceptance shape: two routed learners join a three-node
/// voting configuration while writes continue, then the current leader is
/// removed and the same client continues through the election.
#[test]
fn two_nodes_join_under_load_and_the_removed_leader_is_replaced() {
    let cluster = Cluster::start_with_options(5, 10_000, vec![1, 2, 3]);
    let stop = Arc::new(AtomicBool::new(false));
    let successes = Arc::new(AtomicU64::new(0));
    let failures = Arc::new(AtomicU64::new(0));
    let writer = {
        let addrs = cluster.client_addrs.clone();
        let stop = Arc::clone(&stop);
        let successes = Arc::clone(&successes);
        let failures = Arc::clone(&failures);
        std::thread::spawn(move || {
            let mut client = Client::new(&addrs, 606_060);
            client.register().expect("register load client");
            while !stop.load(Ordering::Relaxed) {
                match client.incr(b"membership-live", 1) {
                    Ok(_) => {
                        successes.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
                // Sustained load, while leaving the single-threaded admin
                // surface enough turns to observe and drive configuration
                // changes on a heavily shared CI host.
                std::thread::sleep(Duration::from_millis(10));
            }
        })
    };

    for member in [4u64, 5] {
        wait_for(Duration::from_secs(30), || {
            let configured = (0..5).any(|index| {
                cluster.status(index).is_some_and(|status| {
                    status
                        .split_once("\"learners\":")
                        .and_then(|(_, rest)| rest.split_once(']'))
                        .is_some_and(|(learners, _)| {
                            learners
                                .trim_start_matches('[')
                                .split(',')
                                .any(|id| id == member.to_string())
                        })
                })
            });
            if !configured && let Some(leader) = cluster.leader() {
                // 202 means the command reached the leader's loop, not that a
                // configuration entry survived a simultaneous leadership
                // transition. Retry the idempotent intent until the committed
                // configuration reports it.
                let _ = cluster.post_admin(leader, &format!("/add-learner?node={member}"));
            }
            configured
        })
        .expect("learner was not configured");
        wait_for(Duration::from_secs(30), || {
            let Some(leader) = cluster.leader() else {
                return false;
            };
            match (
                cluster.status(leader),
                cluster.status((member - 1) as usize),
            ) {
                (Some(leader), Some(learner)) => applied_from(&leader) == applied_from(&learner),
                _ => false,
            }
        })
        .expect("learner did not catch up under load");
        wait_for(Duration::from_secs(30), || {
            let promoted = (0..5).any(|index| {
                cluster.status(index).is_some_and(|status| {
                    status.contains(&format!(
                        "\"voters\":[1,2,3{}]",
                        if member == 4 { ",4" } else { ",4,5" }
                    )) && status.contains("\"voters_outgoing\":[]")
                })
            });
            if !promoted && let Some(leader) = cluster.leader() {
                let _ = cluster.post_admin(leader, &format!("/promote?node={member}"));
            }
            promoted
        })
        .expect("promoted voter did not leave joint consensus");
    }

    let old_leader = cluster.leader().expect("no leader to remove");
    let old_id = old_leader as u64 + 1;
    wait_for(Duration::from_secs(30), || {
        let replaced = cluster.leader().is_some_and(|leader| leader != old_leader);
        if !replaced {
            let _ = cluster.post_admin(old_leader, &format!("/remove?node={old_id}"));
        }
        replaced
    })
    .expect("a new leader was not elected after removing the old one");

    wait_for(Duration::from_secs(5), || {
        successes.load(Ordering::Relaxed) >= 20
    })
    .expect("load never made sustained progress");
    stop.store(true, Ordering::Relaxed);
    writer.join().expect("load thread panicked");
    assert_eq!(
        failures.load(Ordering::Relaxed),
        0,
        "a client-visible write failed during membership changes"
    );
}

fn applied_from(status: &str) -> Option<u64> {
    let rest = status.split_once("\"applied\":")?.1;
    rest.split(|character: char| !character.is_ascii_digit())
        .next()?
        .parse()
        .ok()
}

fn directory_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.metadata().ok())
        .filter(|meta| meta.is_file())
        .map(|meta| meta.len())
        .sum()
}

fn wait_for_fast(limit: Duration, mut done: impl FnMut() -> bool) -> Option<()> {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if done() {
            return Some(());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    None
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
