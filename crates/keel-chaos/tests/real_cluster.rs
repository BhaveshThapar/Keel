#![allow(clippy::unwrap_used, clippy::expect_used)]

//! P17's exit criterion, as a test: a three-node cluster of real processes,
//! partitioned, healed, `SIGSTOP`ped, `SIGKILL`ed, and asked afterwards whether
//! it lost anything it had acknowledged.
//!
//! The clock jump is not here. It cannot run on this host — macOS strips the
//! preload under System Integrity Protection — so it lives in
//! `keel-chaos clock-check`, which the container script runs and whose output
//! is committed. A test that silently skipped it would be worse than no test:
//! it would report a pass for a fault nobody injected.
//!
//! Two things every test in this file asserts beyond its own subject, because
//! they are the ways a chaos test can pass while doing nothing:
//!
//! - the proxy mesh carried bytes, so the nodes really are talking through it
//!   and a cut really would cut something;
//! - the cluster still commits afterwards, so a "no violations" result is not
//!   the result of a cluster that had stopped answering.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use keel_chaos::cluster::{Cluster, ClusterConfig};
use keel_chaos::proxy::Cut;
use keel_client::Client;

/// Where cargo put the server binary. Derived from this test's own path so it
/// works under `--release` and under a moved target directory.
fn server_binary() -> PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.join("keel-server")
}

fn cluster(dir: &tempfile::TempDir) -> Cluster {
    let binary = server_binary();
    assert!(
        binary.exists(),
        "keel-server was not built at {binary:?}; it is a dev-dependency so that \
         cargo builds it before this test runs"
    );
    let mut cfg = ClusterConfig::new(3, dir.path(), binary);
    // Nothing here is a durability measurement, and F_FULLFSYNC on a laptop
    // would make these tests a measurement of the laptop.
    cfg.sync = "none".into();
    cfg.tick_ms = 5;
    Cluster::start(cfg).expect("start a three-node cluster")
}

/// A nonce nothing else in this process will use.
///
/// This is not hygiene, it is correctness, and getting it wrong the first time
/// cost an afternoon. The same nonce reopens the *same session*, and a fresh
/// `Client` starts its sequence numbers at one — so a second `Client` built
/// with a nonce already used replays sequence number one, hits the exactly-once
/// dedup cache, and is handed the *first* request's cached response. The write
/// is acknowledged and never applied. Every subsequent assertion then measures
/// the test's own session reuse and calls it lost data.
fn fresh_nonce() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::SeqCst)
}

/// Wait until the proxy mesh reports it has actually dropped something.
///
/// A partition is not a fact about the harness's intent, it is a fact about
/// traffic, and the proxy counts traffic. Asserting the counter *before* doing
/// anything under the partition is what stops a test from passing because its
/// write completed in the window between asking for a cut and the cut landing.
fn wait_for_partition(c: &Cluster, within: Duration) -> bool {
    let (_, refused_before, severed_before) = c.traffic();
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        let (_, refused, severed) = c.traffic();
        if refused > refused_before || severed > severed_before {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

/// Retry until the cluster answers or the budget runs out.
///
/// Every write in these tests goes through this. A single attempt that failed
/// during the election after a fault would be reported as a lost write, and the
/// bug would be in the test.
fn put_until(c: &Cluster, key: &str, value: &str, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        let mut client = Client::new(&c.client_addrs, fresh_nonce());
        if client.put(key.as_bytes(), value.as_bytes()).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

fn get_until(c: &Cluster, key: &str, within: Duration) -> Option<Vec<u8>> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        let mut client = Client::new(&c.client_addrs, fresh_nonce());
        if let Ok(Some(value)) = client.get(key.as_bytes()) {
            return Some(value);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    None
}

/// The whole exit criterion in one run, in the order the roadmap names it.
#[test]
fn a_cluster_survives_a_partition_a_pause_and_a_kill() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = cluster(&dir);

    assert!(
        put_until(&c, "before", "1", Duration::from_secs(20)),
        "the cluster never committed anything before any fault was injected"
    );
    let (carried, _, _) = c.traffic();
    assert!(
        carried > 0,
        "the proxy mesh carried no bytes, so the nodes are not talking through it \
         and no partition this harness injects can cut anything"
    );

    // 1. Partition: a minority of one, so the majority can still commit.
    c.split(&[2]);
    // Wait for it to actually bite before claiming anything happened during it.
    // The proxy notices a cut on its next read poll, which is 50 ms away at
    // worst, and a write that completed inside that window would have completed
    // on an unpartitioned cluster — a green test for a fault that had not
    // landed yet.
    assert!(
        wait_for_partition(&c, Duration::from_secs(10)),
        "the partition dropped no connection within ten seconds, so it did not happen"
    );
    assert!(
        put_until(&c, "during-partition", "2", Duration::from_secs(20)),
        "a majority could not commit while one node was partitioned off"
    );

    // 2. Heal.
    c.heal();

    // 3. Pause. Its sockets stay open and unanswered — the fault a crash does
    //    not produce.
    c.process(1).unwrap().stop().unwrap();
    assert!(
        c.process(1).unwrap().is_running(),
        "a paused node is paused, not dead"
    );
    assert!(
        put_until(&c, "during-pause", "3", Duration::from_secs(20)),
        "the cluster could not commit with one node stopped"
    );
    c.process(1).unwrap().resume().unwrap();

    // 4. Kill, and bring it back.
    c.process(0).unwrap().kill().unwrap();
    assert!(
        put_until(&c, "during-kill", "4", Duration::from_secs(20)),
        "the cluster could not commit with one node killed"
    );
    c.start_node(0)
        .expect("the killed node restarts and recovers");

    // Everything acknowledged is still there. Written before, during and after
    // each fault, so a value lost at any point in the sequence shows up.
    for (key, expected) in [
        ("before", "1"),
        ("during-partition", "2"),
        ("during-pause", "3"),
        ("during-kill", "4"),
    ] {
        let got = get_until(&c, key, Duration::from_secs(20));
        assert_eq!(
            got.as_deref(),
            Some(expected.as_bytes()),
            "{key} was acknowledged and is now {got:?}"
        );
    }
}

/// The asymmetric partition, which is the shape that produces a node whose term
/// climbs while nobody deposes it.
///
/// What is asserted is modest on purpose: that the cluster keeps committing
/// through it and comes back. Asserting anything about the isolated node's term
/// would need to read its state, and reading it means reaching it.
#[test]
fn a_one_way_partition_does_not_stop_the_majority() {
    let dir = tempfile::tempdir().unwrap();
    let c = cluster(&dir);
    assert!(put_until(&c, "k", "before", Duration::from_secs(20)));

    // n2 can send but not receive: it will campaign into a void.
    c.isolate(2, Cut::Backward);
    assert!(
        wait_for_partition(&c, Duration::from_secs(10)),
        "the one-way cut dropped no connection, so it did not happen"
    );
    assert!(
        !c.link_is_cut(2, 0),
        "outbound was cut, making it symmetric"
    );
    assert!(
        c.link_is_cut(0, 2),
        "inbound was not cut, so nothing happened"
    );
    assert!(
        put_until(&c, "k", "during", Duration::from_secs(20)),
        "a one-way partition of one node stopped the majority"
    );

    c.heal();
    assert!(put_until(&c, "k", "after", Duration::from_secs(20)));
    assert_eq!(
        get_until(&c, "k", Duration::from_secs(20)).as_deref(),
        Some(b"after".as_slice())
    );
}

/// A node killed and restarted comes back with what it had acknowledged.
///
/// The state machine is volatile by the choice recorded in ADR-010's
/// neighbourhood, so what is asserted is what the *cluster* still knows, not
/// what one node's disk still holds.
#[test]
fn a_killed_node_rejoins_and_the_cluster_keeps_its_acknowledged_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = cluster(&dir);
    for i in 0..5 {
        assert!(put_until(
            &c,
            &format!("k{i}"),
            &i.to_string(),
            Duration::from_secs(20)
        ));
    }

    c.process(2).unwrap().kill().unwrap();
    assert!(!c.process(2).unwrap().is_running());
    c.start_node(2).expect("restart");
    assert!(c.process(2).unwrap().is_running());
    assert_eq!(
        c.process(2).unwrap().starts,
        2,
        "a restart is a second lifetime"
    );

    for i in 0..5 {
        assert_eq!(
            get_until(&c, &format!("k{i}"), Duration::from_secs(20)).as_deref(),
            Some(i.to_string().as_bytes()),
            "k{i} was acknowledged before the kill and is gone after it"
        );
    }
}
