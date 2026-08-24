#![allow(clippy::unwrap_used, clippy::expect_used)]

//! P7's exit criterion, over a real socket: a node starts, writes its ready
//! file, reports `sync_mode: "durable"`, and `/metrics` parses as Prometheus
//! text exposition.

use std::io::{Read, Write};
use std::net::TcpStream;

use keel_log::SyncMode;
use keel_raft::Role;
use keel_server::{Admin, Kind, Metric, Observable, Status, write_ready_file};

/// A node that is not a node. The admin surface's job is to render what it is
/// given, and a cluster would only make the test slower and less specific.
struct Fixture {
    status: Status,
}

impl Fixture {
    fn durable() -> Self {
        Self {
            status: Status {
                id: 1,
                term: 4,
                role: Role::Leader,
                leader: Some(1),
                commit: 900,
                applied: 900,
                persisted: 901,
                last_index: 901,
                voters: vec![1, 2, 3],
                learners: vec![7],
                voters_outgoing: vec![],
                sync_mode: SyncMode::Durable,
                segments: 5,
                failure: None,
            },
        }
    }
}

impl Observable for Fixture {
    fn status(&self) -> Status {
        self.status.clone()
    }

    fn metrics(&self) -> Vec<Metric> {
        vec![
            Metric {
                name: "keel_term",
                help: "Current Raft term",
                kind: Kind::Gauge,
                value: self.status.term as f64,
            },
            Metric {
                name: "keel_commit_index",
                help: "Highest committed log index",
                kind: Kind::Gauge,
                value: self.status.commit as f64,
            },
            Metric {
                name: "keel_applied_index",
                help: "Highest log index applied to the state machine",
                kind: Kind::Gauge,
                value: self.status.applied as f64,
            },
            Metric {
                name: "keel_log_segments",
                help: "Segment files in the durable log",
                kind: Kind::Gauge,
                value: self.status.segments as f64,
            },
            Metric {
                name: "keel_sync_durable",
                help: "1 if this node's fsyncs survive power loss, 0 otherwise",
                kind: Kind::Gauge,
                value: if self.status.sync_mode.is_durable() {
                    1.0
                } else {
                    0.0
                },
            },
        ]
    }
}

fn get(addr: std::net::SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    write!(stream, "GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap();
    stream.flush().unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

fn split(response: &str) -> (&str, &str) {
    response
        .split_once("\r\n\r\n")
        .expect("a response with no header/body separator")
}

/// A scraper's-eye view: every sample line is `name value`, every name has a
/// preceding TYPE, no name appears twice, and the body ends with a newline.
fn parse_exposition(body: &str) -> Vec<(String, String, f64)> {
    assert!(
        body.ends_with('\n'),
        "an exposition body that does not end with a newline is a truncated record"
    );
    let mut types = std::collections::BTreeMap::new();
    let mut seen = std::collections::BTreeSet::new();
    let mut samples = Vec::new();

    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, kind) = rest.split_once(' ').expect("a TYPE line without a kind");
            assert!(
                matches!(
                    kind,
                    "counter" | "gauge" | "histogram" | "summary" | "untyped"
                ),
                "unknown metric type {kind:?}"
            );
            types.insert(name.to_string(), kind.to_string());
            continue;
        }
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let (name, value) = line.split_once(' ').expect("a sample line without a value");
        assert!(
            seen.insert(name.to_string()),
            "{name} appeared in more than one block"
        );
        let kind = types
            .get(name)
            .unwrap_or_else(|| panic!("{name} has samples and no TYPE"))
            .clone();
        let value: f64 = value
            .parse()
            .unwrap_or_else(|_| panic!("{name} has a value that is not a number: {value:?}"));
        samples.push((name.to_string(), kind, value));
    }
    samples
}

#[test]
fn a_node_reports_itself_over_a_real_socket() {
    let node = Fixture::durable();
    let admin = Admin::bind("127.0.0.1:0").unwrap();
    let addr = admin.local_addr().unwrap();

    // The request has to be in flight before `poll` runs: the listener is
    // non-blocking and answers what has arrived rather than waiting.
    let handle = std::thread::spawn(move || {
        (
            get(addr, "/status"),
            get(addr, "/metrics"),
            get(addr, "/nope"),
        )
    });

    let mut served = 0;
    for _ in 0..10_000 {
        served += admin.poll(&node).unwrap();
        if served >= 3 {
            break;
        }
        std::thread::yield_now();
    }
    let (status, metrics, unknown) = handle.join().unwrap();
    assert_eq!(
        served, 3,
        "the listener answered {served} of three requests"
    );

    // --- status
    let (head, body) = split(&status);
    assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
    assert!(head.contains("Content-Type: application/json"));
    assert!(
        body.contains("\"sync_mode\":\"durable\""),
        "a node must say whether its fsyncs survive power loss: {body}"
    );
    assert!(body.contains("\"role\":\"leader\""));
    assert!(body.contains("\"commit\":900"));
    assert!(body.contains("\"learners\":[7]"));

    // --- metrics
    let (head, body) = split(&metrics);
    assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
    assert!(
        head.contains("version=0.0.4"),
        "a scraper decides how to parse from the content type: {head}"
    );
    let samples = parse_exposition(body);
    assert_eq!(samples.len(), 5, "expected five metrics, got {samples:?}");
    let durable = samples
        .iter()
        .find(|(name, _, _)| name == "keel_sync_durable")
        .expect("keel_sync_durable was not exported");
    assert_eq!(
        durable.2, 1.0,
        "a durable node reported itself as not durable"
    );

    // --- anything else
    let (head, _) = split(&unknown);
    assert!(
        head.starts_with("HTTP/1.1 404 Not Found"),
        "an unknown path was answered with something other than 404: {head}"
    );
}

/// The ready file says the node has finished recovering, and it is published by
/// rename so a supervisor can never read a half-written one.
#[test]
fn the_ready_file_is_written_whole_or_not_at_all() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("keel.ready");
    let node = Fixture::durable();

    write_ready_file(&path, &node.status()).unwrap();

    let contents = std::fs::read_to_string(&path).unwrap();
    assert!(contents.ends_with('\n'));
    assert!(contents.contains("\"sync_mode\":\"durable\""));
    assert!(contents.contains("\"id\":1"));

    // No scratch file left behind: a supervisor globbing the directory would
    // otherwise find two files and have to guess.
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| name != "keel.ready")
        .collect();
    assert!(leftovers.is_empty(), "left behind {leftovers:?}");
}

/// A node running without power-loss durability must say so, because it looks
/// identical to one that has it right up until the machine loses power.
#[test]
fn a_node_that_is_not_durable_says_so() {
    let mut node = Fixture::durable();
    node.status.sync_mode = SyncMode::Barrier;

    let json = node.status().to_json();
    assert!(json.contains("\"sync_mode\":\"barrier\""), "{json}");

    let durable = node
        .metrics()
        .into_iter()
        .find(|m| m.name == "keel_sync_durable")
        .expect("keel_sync_durable was not exported");
    assert_eq!(
        durable.value, 0.0,
        "a node in barrier mode reported itself as durable"
    );
}
