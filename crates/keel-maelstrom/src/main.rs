//! Keel under Jepsen's Maelstrom.
//!
//! Maelstrom is a third transport, and that is the whole point of it being
//! worth doing. The consensus core here is the same `RaftCore` the simulator
//! sweeps and the server runs — no conditional compilation, no adapter-specific
//! branch inside it (FR-12). What differs is everything outside: messages are
//! line-delimited JSON on stdin and stdout, peers are named `n1`, `n2`, and the
//! clock is a thread that writes a tick.
//!
//! The workload is `lin-kv`: `read`, `write`, `cas` against integer keys, with
//! Knossos checking the resulting history for linearizability. What that buys
//! over the simulator's own oracles is independence — a checker nobody here
//! wrote, applying a definition of linearizability nobody here chose, to a
//! history it recorded itself.
//!
//! Two things this adapter deliberately does not do.
//!
//! **It does not persist.** Maelstrom's node processes are not restarted with
//! their storage intact, so a durable log would be written and never read. The
//! log is in memory, which means this run tests replication, election and apply
//! — and says nothing about crash recovery, which is what the simulator's disk
//! profiles and the kill loop are for.
//!
//! **It does not deduplicate on a session.** Maelstrom's clients do not
//! register, and every request carries its own message id. A retry from
//! Maelstrom is a new operation as far as this adapter is concerned, which is
//! exactly what `lin-kv` expects — the checker's job is to decide whether the
//! resulting history is linearizable, not whether the client was deduplicated.

use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::sync::mpsc::{Sender, channel};

use bytes::Bytes;
use keel_raft::{
    Advance, ConfState, Config, Entry, EntryPayload, Index, Input, Message, NodeId, RaftCore, Role,
};
use serde_json::{Value, json};

/// One thing that arrived: a message, or the clock.
enum Event {
    Line(String),
    Tick,
}

fn main() {
    let (tx, rx) = channel();
    spawn_reader(tx.clone());
    spawn_clock(tx);

    let mut node = MaelstromNode::new();
    for event in rx {
        match event {
            Event::Line(line) => node.on_line(&line),
            Event::Tick => node.on_tick(),
        }
    }
}

/// stdin, a line at a time.
fn spawn_reader(tx: Sender<Event>) {
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            let Ok(line) = line else { return };
            if tx.send(Event::Line(line)).is_err() {
                return;
            }
        }
    });
}

/// The clock, which is a thread because Maelstrom gives no other one.
///
/// The core still reads no clock: it is handed `Input::Tick` like any other
/// event, and this thread does nothing but decide when. Twenty milliseconds
/// against Maelstrom's default latencies gives an election timeout of a few
/// hundred, which is short enough to recover quickly and long enough not to
/// campaign over ordinary jitter.
fn spawn_clock(tx: Sender<Event>) {
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(std::time::Duration::from_millis(20));
            if tx.send(Event::Tick).is_err() {
                return;
            }
        }
    });
}

/// A client request waiting for the entry that answers it.
struct Pending {
    /// Who to reply to, and about which message.
    src: String,
    in_reply_to: u64,
    /// The key and what was asked, so a `cas` can be answered with the right
    /// error and a `read` with the value it saw.
    op: Op,
}

#[derive(Clone)]
enum Op {
    Read { key: String },
    Write { key: String, value: Value },
    Cas { key: String, from: Value, to: Value },
}

struct MaelstromNode {
    id: String,
    /// Maelstrom names nodes `n1`, `n2`; the core wants integers. The mapping
    /// is the numeric suffix, which Maelstrom guarantees.
    numeric: NodeId,
    peers: Vec<String>,
    core: Option<RaftCore>,
    /// The log, in memory. See the module docs for why there is no disk here.
    entries: Vec<Entry>,
    /// Applied state: the `lin-kv` keyspace.
    store: BTreeMap<String, Value>,
    applied: Index,
    /// Requests waiting for the entry that answers them, keyed by a token
    /// carried *inside* the entry.
    ///
    /// Not by the index the entry was expected to land at. Two requests that
    /// arrive in one batch predict the same index, and a leadership change
    /// renumbers what follows a dropped proposal — so a predicted index is not
    /// an identity. A token in the payload is one, and it survives both.
    pending: BTreeMap<u64, Pending>,
    next_msg_id: u64,
    next_ctx: u64,
    /// Contexts handed to the core for reads, and who is waiting.
    reads: BTreeMap<u64, Pending>,
    confirmed_reads: Vec<(u64, Index)>,
    /// Requests this node relayed to the leader, waiting for the answer.
    proxied: BTreeMap<u64, Pending>,
    /// Requests relayed *to* this node, and which follower to answer.
    proxy_origin: BTreeMap<u64, String>,
}

impl MaelstromNode {
    fn new() -> Self {
        Self {
            id: String::new(),
            numeric: 0,
            peers: Vec::new(),
            core: None,
            entries: Vec::new(),
            store: BTreeMap::new(),
            applied: 0,
            pending: BTreeMap::new(),
            next_msg_id: 1,
            next_ctx: 1,
            reads: BTreeMap::new(),
            confirmed_reads: Vec::new(),
            proxied: BTreeMap::new(),
            proxy_origin: BTreeMap::new(),
        }
    }

    fn on_tick(&mut self) {
        if let Some(core) = self.core.as_mut() {
            let _ = core.step(Input::Tick);
        }
        self.pump();
    }

    fn on_line(&mut self, line: &str) {
        let Ok(msg) = serde_json::from_str::<Value>(line) else {
            return;
        };
        let src = msg["src"].as_str().unwrap_or_default().to_string();
        let body = &msg["body"];
        let msg_id = body["msg_id"].as_u64().unwrap_or(0);

        match body["type"].as_str().unwrap_or_default() {
            "init" => self.on_init(&src, msg_id, body),
            // Peer traffic: a Raft message wrapped in Maelstrom's envelope.
            "raft" => self.on_raft(body),
            "read" | "write" | "cas" => self.on_client(&src, msg_id, body),
            // A request a follower relayed here, and the answer coming back.
            "proxy" => self.on_proxy(&src, body),
            "proxy_reply" => self.on_proxy_reply(body),
            _ => {}
        }
        self.pump();
    }

    fn on_init(&mut self, src: &str, msg_id: u64, body: &Value) {
        self.id = body["node_id"].as_str().unwrap_or_default().to_string();
        self.numeric = numeric_id(&self.id);
        self.peers = body["node_ids"]
            .as_array()
            .map(|ids| {
                ids.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();

        let voters: Vec<NodeId> = self.peers.iter().map(|p| numeric_id(p)).collect();
        self.core = Some(RaftCore::new(
            Config::new(self.numeric),
            ConfState {
                voters,
                ..ConfState::default()
            },
        ));
        self.reply(src, msg_id, json!({"type": "init_ok"}));
    }

    /// A follower relayed a client request here.
    ///
    /// Answered exactly as if the client had sent it directly, except that the
    /// reply goes back to the follower with the token it used, so it can find
    /// the connection that is waiting.
    fn on_proxy(&mut self, src: &str, body: &Value) {
        let Some(token) = body["token"].as_u64() else {
            return;
        };
        let inner = body["inner"].clone();
        // The follower is standing in for the client. `msg_id` here is the
        // token, so the answer comes back addressed to the right waiter.
        self.proxy_origin.insert(token, src.to_string());
        self.on_client(&format!("__proxy__{token}"), token, &inner);
    }

    /// The leader answered something this node relayed.
    fn on_proxy_reply(&mut self, body: &Value) {
        let Some(token) = body["token"].as_u64() else {
            return;
        };
        let Some(pending) = self.proxied.remove(&token) else {
            return;
        };
        let inner = body["inner"].clone();
        self.reply(&pending.src.clone(), pending.in_reply_to, inner);
    }

    fn on_raft(&mut self, body: &Value) {
        let Some(core) = self.core.as_mut() else {
            return;
        };
        let Some(raw) = body["payload"].as_str() else {
            return;
        };
        // The Raft message travels as its own encoding inside Maelstrom's
        // envelope rather than being re-expressed as JSON. Re-expressing it
        // would mean a second wire format to keep in step with the first, and a
        // bug in the adapter's translation would look exactly like a bug in
        // consensus.
        let Ok(bytes) = hex_decode(raw) else { return };
        let Ok(message) = keel_api::decode::<Message>(&bytes) else {
            return;
        };
        let _ = core.step(Input::Message(message));
    }

    fn on_client(&mut self, src: &str, msg_id: u64, body: &Value) {
        let Some(core) = self.core.as_mut() else {
            return;
        };
        let kind = body["type"].as_str().unwrap_or_default();
        let key = body["key"].to_string();

        // Not the leader: relay to whoever is, and relay the answer back.
        //
        // The real client follows a redirect instead, which is the better
        // design — it costs one hop rather than two and needs no state on the
        // follower. Maelstrom's `lin-kv` client does not follow redirects: it
        // records an error as a definite failure and moves on, so a cluster
        // that redirected would report a linearizable history in which
        // two-thirds of the operations failed. That would be a true statement
        // about nothing.
        //
        // Proxying is a real node behaviour rather than a testing convenience,
        // and the thing being checked is unaffected: the leader still decides,
        // still replicates, still applies.
        if core.status().role != Role::Leader {
            match core.status().leader {
                Some(leader) => {
                    let token = self.next_ctx;
                    self.next_ctx += 1;
                    self.proxied.insert(
                        token,
                        Pending {
                            src: src.to_string(),
                            in_reply_to: msg_id,
                            op: Op::Read { key: key.clone() },
                        },
                    );
                    let dest = format!("n{leader}");
                    let mut relayed = body.clone();
                    relayed["type"] = json!("proxy");
                    self.send(
                        &dest,
                        json!({"type": "proxy", "token": token, "inner": relayed}),
                    );
                }
                // Nobody is leading yet. A definite failure is the honest
                // answer: the request did not happen, and the checker may treat
                // it as such.
                None => self.reply(
                    src,
                    msg_id,
                    json!({
                        "type": "error",
                        "code": 11,
                        "text": "no leader yet",
                    }),
                ),
            }
            return;
        }

        let op = match kind {
            "read" => Op::Read { key: key.clone() },
            "write" => Op::Write {
                key: key.clone(),
                value: body["value"].clone(),
            },
            "cas" => Op::Cas {
                key: key.clone(),
                from: body["from"].clone(),
                to: body["to"].clone(),
            },
            _ => return,
        };

        let pending = Pending {
            src: src.to_string(),
            in_reply_to: msg_id,
            op: op.clone(),
        };

        match op {
            // A read still goes through the core. Answering it from local state
            // would be answering from whatever this node has applied, which on a
            // leader that has been deposed and does not know it is a stale read
            // — and Knossos would find it.
            Op::Read { .. } => {
                let ctx = self.next_ctx;
                self.next_ctx += 1;
                self.reads.insert(ctx, pending);
                let _ = core.step(Input::ReadIndex { ctx });
            }
            Op::Write { .. } | Op::Cas { .. } => {
                let ctx = self.next_ctx;
                self.next_ctx += 1;
                let encoded = serde_json::to_vec(&encode_op(&pending.op, ctx)).unwrap_or_default();
                self.pending.insert(ctx, pending);
                let _ = core.step(Input::Propose {
                    ctx,
                    data: Bytes::from(encoded),
                });
            }
        }
    }

    /// One turn of the host loop: persist (in memory), send, apply, advance.
    fn pump(&mut self) {
        loop {
            let Some(core) = self.core.as_mut() else {
                return;
            };
            if !core.has_ready() {
                return;
            }
            let ready = core.ready();

            if let Some(first) = ready.entries.first() {
                // A truncation: history changed below what is held.
                self.entries.retain(|e| e.index < first.index);
            }
            self.entries.extend(ready.entries.iter().cloned());
            let persisted = ready.entries.last().map(|e| (e.index, e.term));

            for message in &ready.messages {
                self.send_raft(message);
            }

            let mut applied = None;
            for entry in &ready.committed_entries {
                self.apply(entry);
                applied = Some(entry.index);
            }

            for read in &ready.read_states {
                self.confirmed_reads.push((read.ctx, read.index));
            }
            // A proposal the core refused never becomes an entry, so nothing
            // will ever answer it. Saying so is a definite failure, which a
            // checker can use; silence is an indeterminate operation, which it
            // cannot.
            let dropped: Vec<u64> = ready
                .proposals_dropped
                .iter()
                .map(|(ctx, _)| *ctx)
                .collect();
            for ctx in dropped {
                self.fail_pending(ctx, "the proposal was refused before it reached the log");
            }

            let number = ready.number;
            let snapshot = ready.snapshot_to_install.clone();
            if let Some(core) = self.core.as_mut() {
                core.advance(Advance {
                    ready_number: number,
                    persisted,
                    applied,
                    snapshot_installed: snapshot,
                });
            }
            self.answer_reads();
        }
    }

    fn apply(&mut self, entry: &Entry) {
        self.applied = entry.index;
        // A no-op or a configuration change moves the index and nothing else.
        let EntryPayload::Normal(data) = &entry.payload else {
            return;
        };
        let Ok(op) = serde_json::from_slice::<Value>(data) else {
            return;
        };
        let token = op["token"].as_u64().unwrap_or(0);

        let key = op["key"].as_str().unwrap_or_default().to_string();
        let response = match op["op"].as_str().unwrap_or_default() {
            "write" => {
                self.store.insert(key, op["value"].clone());
                Some(json!({"type": "write_ok"}))
            }
            "cas" => {
                let current = self.store.get(&key).cloned().unwrap_or(Value::Null);
                if current == Value::Null {
                    Some(json!({
                        "type": "error",
                        "code": 20,
                        "text": "key does not exist",
                    }))
                } else if current != op["from"] {
                    Some(json!({
                        "type": "error",
                        "code": 22,
                        "text": "current value is not what was expected",
                    }))
                } else {
                    self.store.insert(key, op["to"].clone());
                    Some(json!({"type": "cas_ok"}))
                }
            }
            _ => None,
        };

        // The entry may have been proposed by a different node, in which case
        // nothing here is waiting for it and applying it is all there is to do.
        if let (Some(pending), Some(body)) = (self.pending.remove(&token), response) {
            self.reply(&pending.src.clone(), pending.in_reply_to, body);
        }
    }

    /// Answer every read whose index has been applied.
    fn answer_reads(&mut self) {
        let ready: Vec<(u64, Index)> = self
            .confirmed_reads
            .iter()
            .filter(|(_, index)| *index <= self.applied)
            .copied()
            .collect();
        self.confirmed_reads
            .retain(|(_, index)| *index > self.applied);

        for (ctx, _) in ready {
            let Some(pending) = self.reads.remove(&ctx) else {
                continue;
            };
            let Op::Read { key } = &pending.op else {
                continue;
            };
            let key = key.as_str().trim_matches('"').to_string();
            let body = match self.store.get(&key) {
                Some(value) => json!({"type": "read_ok", "value": value}),
                None => json!({
                    "type": "error",
                    "code": 20,
                    "text": "key does not exist",
                }),
            };
            self.reply(&pending.src.clone(), pending.in_reply_to, body);
        }
    }

    fn fail_pending(&mut self, token: u64, why: &str) {
        if let Some(pending) = self.pending.remove(&token) {
            self.reply(
                &pending.src.clone(),
                pending.in_reply_to,
                json!({"type": "error", "code": 11, "text": why}),
            );
        }
    }

    fn send_raft(&mut self, message: &Message) {
        let Ok(bytes) = keel_api::encode(message) else {
            return;
        };
        let dest = format!("n{}", message.to);
        self.send(
            &dest,
            json!({"type": "raft", "payload": hex_encode(&bytes)}),
        );
    }

    fn reply(&mut self, dest: &str, in_reply_to: u64, mut body: Value) {
        // A reply to a request a follower relayed goes back to that follower
        // rather than to the client, which this node cannot address.
        if let Some(token) = dest
            .strip_prefix("__proxy__")
            .and_then(|t| t.parse::<u64>().ok())
            && let Some(origin) = self.proxy_origin.remove(&token)
        {
            self.send(
                &origin,
                json!({"type": "proxy_reply", "token": token, "inner": body}),
            );
            return;
        }
        body["in_reply_to"] = json!(in_reply_to);
        self.send(dest, body);
    }

    fn send(&mut self, dest: &str, mut body: Value) {
        body["msg_id"] = json!(self.next_msg_id);
        self.next_msg_id += 1;
        let envelope = json!({"src": self.id, "dest": dest, "body": body});
        let mut stdout = std::io::stdout().lock();
        let _ = writeln!(stdout, "{envelope}");
        let _ = stdout.flush();
    }
}

/// What goes in a log entry for a write or a compare-and-swap.
fn encode_op(op: &Op, token: u64) -> Value {
    match op {
        Op::Write { key, value } => json!({
            "op": "write",
            "key": key.trim_matches('"'),
            "value": value,
            "token": token,
        }),
        Op::Cas { key, from, to } => json!({
            "op": "cas",
            "key": key.trim_matches('"'),
            "from": from,
            "to": to,
            "token": token,
        }),
        Op::Read { key } => json!({
            "op": "read",
            "key": key.trim_matches('"'),
            "token": token,
        }),
    }
}

/// `n1` becomes 1.
///
/// Maelstrom's node ids are always `n` followed by a number. A malformed one
/// becomes zero, which the core will refuse to campaign as, so the failure is
/// visible rather than silent.
fn numeric_id(name: &str) -> NodeId {
    name.trim_start_matches('n').parse().unwrap_or(0)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_decode(text: &str) -> Result<Vec<u8>, ()> {
    if text.len() % 2 != 0 {
        return Err(());
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).map_err(|_| ()))
        .collect()
}
