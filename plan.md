# PRD — Raft-replicated LSM-KV

**Working name:** pick something short and searchable (`Keel`, `Quorum-KV`, `RaftKV`). Recruiters and engineers will search your profile for whatever the resume says.

**Language / base:** Rust, reusing LSM-KV as the replicated state machine. Sync core, thin async edge.

---

## 1. Objective

Produce a third project that matches LazerBook and LSM-KV in verification rigor and measured numbers, in the one domain the resume is missing: **distributed systems**. The page becomes latency / storage / distribution — one stack.

The audience is storage and infra teams (Spanner/Bigtable, DynamoDB/Aurora/EBS, RocksDB/ZippyDB, FoundationDB, Azure Storage/Cosmos, Cockroach, MongoDB, Snowflake, Cloudflare, Databricks Lakebase, Stripe DB Infra). Those engineers have all seen a 6.824 Raft. What they have not usually seen from a student, and what this PRD exists to force, is:

1. A **sans-IO Raft core** run under a **deterministic simulator** that checks every Raft safety invariant after every step, across millions of seeded steps.
2. **External linearizability checking** (Jepsen's Maelstrom/Knossos + Porcupine) under partitions, crashes, and clock skew — not "my tests pass."
3. **Production features**, not the paper's minimum: pre-vote, check-quorum, leader transfer, learners + joint consensus, ReadIndex + lease reads, streaming snapshots from SSTable checkpoints, exactly-once client sessions.
4. **Numbers with methodology**: coordinated-omission-aware latency, throughput-vs-p99 curves, an etcd baseline on the same hardware, failover time.
5. **Negative demonstrations and a bug log**: the harness is shown catching real violations when a safety rule is removed.

### Headline to earn (resume bullets, placeholders to be measured)

- Built `<Name>`, a Raft-replicated key-value store in Rust on LSM-KV — pre-vote/check-quorum, joint-consensus membership with learners, ReadIndex and lease reads, streaming snapshots from SSTable checkpoints, exactly-once client sessions; sustained **X writes/s at p99 Y ms** on a 5-node cluster with fsync on, **Z× etcd** on the same hardware and workload.
- Verified linearizability over **M operations** with Maelstrom (`lin-kv`) and Porcupine under partition, crash, and clock-skew nemeses; deterministic simulator replayed **K seeded runs / S steps** with torn-write and fsync-loss injection, checking all five Raft safety properties at every step — zero violations; **B bugs** found by the harness during development, all logged with reproducing seeds.
- Leader failover in **F ms median**; membership changes under load with no write unavailability beyond leader election.

Nothing above goes on the resume until it is measured, with hardware stated.

---

## 2. Scope and non-goals

**In scope:** single Raft group, linearizable KV API (`put`, `get`, `delete`, `cas`, `scan`), durable Raft log, LSM-KV state machine, snapshots/compaction, membership changes, client library + CLI, simulator, nemesis tooling, benchmarks, Prometheus metrics, Docker Compose.

**Non-goals:** SQL, cross-shard transactions, geo-replication, Byzantine fault tolerance, TLS/auth, multi-tenancy. Multi-Raft sharding is a stretch goal only (§9).

---

## 3. Architecture

```
crates/
  raft-core/    sans-IO Raft state machine: (event) -> Ready{msgs, hard_state, entries, committed, snapshot}
  raft-log/     segmented durable log + hard state, CRC32C records, group-commit fsync
  kv-sm/        LSM-KV adapter: apply(), checkpoint(), restore(), applied_index, session table
  net/          length-prefixed TCP framing (postcard), pluggable Transport trait
  server/       node binary: wires core + log + sm + net; admin API; metrics
  client/       library + CLI: leader discovery, sessions, retries
  sim/          deterministic simulator: virtual clock, network/disk/process fault models, invariants
  maelstrom/    stdin/stdout adapter so Maelstrom drives the same core
  bench/        load generator (closed- and open-loop, HdrHistogram), etcd comparison scripts
  chaos/        real-cluster nemesis: partition proxy, SIGSTOP/SIGKILL, clock jumps
```

The core design rule, non-negotiable from day one: **`raft-core` performs no I/O, owns no threads, reads no clock.** It consumes `Tick`, `Message`, `Propose`, `ReadIndex`, `ConfChange` and emits a `Ready` struct the host must act on in a fixed order (persist hard state and entries → send messages → apply committed). This is the etcd/raft-rs "Ready" pattern. It is what makes the simulator, the fuzzer, and Maelstrom all drive the identical code. Retrofitting it later is not realistic; it has to be the first thing built.

---

## 4. Functional requirements

| ID | Requirement | Acceptance criteria |
|---|---|---|
| FR-1 | Sans-IO core with `Ready` output contract | Core compiles without `std::time`, threads, or I/O deps; identical input sequence → identical output (test) |
| FR-2 | Leader election: randomized timeouts, **pre-vote**, **check-quorum**, **leader transfer** (`TimeoutNow`, bypasses pre-vote for forced campaigns) | A node partitioned for 10 election timeouts rejoins without deposing a healthy leader; leader transfer completes in < 1 election timeout under load |
| FR-3 | Log replication: proposal batching (size + time bound), per-follower **pipelining** with bounded in-flight window, fast conflict backtracking via conflict-term/index hints, heartbeat coalescing, flow control on lagging followers | Batch-size and in-flight histograms exported; follower 10k entries behind converges without snapshot |
| FR-4 | Durable log: segmented append-only files, CRC32C per record, **group-commit `fdatasync`** (one per batch), directory fsync on segment creation, truncation on conflict, torn-tail discard on recovery; term/vote persisted **before** any vote is granted or any message in a new term is sent | Recovery after `SIGKILL` mid-write loses nothing committed; simulated torn write always recovers to a valid prefix |
| FR-5 | Commit rule per Raft Figure 8: leader commits only current-term entries by counting replicas; appends a no-op on election | Dedicated test reproducing Figure 8; simulator negative demo (§6, TR-8c) |
| FR-6 | State machine = LSM-KV: in-order apply; **`applied_index` written in the same LSM write batch as the data** so apply is idempotent on replay; ops `put/get/delete/cas/scan` | Kill-during-apply loop (1,000 iterations) never double-applies or regresses `applied_index` |
| FR-7 | **Exactly-once client sessions** (Raft thesis §6.3): `register` → client id; `(client_id, seq)` dedup with cached response; session table lives in the state machine and in snapshots; deterministic expiry via leader-stamped time in entries | Retry storm across a forced leader failover applies each op exactly once (counter test) |
| FR-8 | Reads: **ReadIndex** (default; refused until the new leader's no-op commits), **lease reads** (opt-in; lease = election timeout × (1 − drift bound)), follower reads via ReadIndex forwarding, explicit `stale` reads | ReadIndex and lease modes pass the linearizability checker within the stated clock assumption; lease mode is shown to fail outside it (TR-8b) |
| FR-9 | Snapshots: triggered by log size/entries; snapshot = **LSM checkpoint** (flush memtable, hard-link immutable SSTables, copy manifest) tagged with `(index, term, config)`; **`InstallSnapshot` streamed in chunks with resume**; atomic swap-in on receiver; log compaction below snapshot index | Snapshot of a 1 GB state stalls writes < 50 ms; transfer resumes after the receiver is killed mid-stream |
| FR-10 | Membership: **learners** (non-voting, promoted after catch-up), **joint consensus** (`C_old,new` → `C_new`), leader not in `C_new` steps down after it commits; config persisted in log and snapshot | Add 2 nodes to a 3-node cluster under load with no write unavailability; remove the leader; the single-server-change race from Ongaro's 2015 note is documented and covered by a test |
| FR-11 | Client library: `NotLeader` redirect hints, backoff/retry, timeouts, session lifecycle; CLI | `kv put/get/cas/scan`, `kv bench`, works against a cluster mid-failover |
| FR-12 | Transport: length-prefixed TCP with `postcard`; `Transport` trait implemented by TCP, simulator, and Maelstrom adapter | All three backends run the same core with zero `cfg` branches in `raft-core` |
| FR-13 | Admin/ops: status endpoint (term, leader, commit/applied, config), admin CLI (`transfer-leader`, `add-learner`, `promote`, `remove`, `snapshot`), **Prometheus metrics** (commit latency histogram, fsync latency, batch size, in-flight per follower, elections, snapshot bytes), `tracing` structured logs, Docker Compose for 3/5 nodes | Grafana dashboard JSON committed; this is also what makes "Prometheus/Grafana" on the resume honest |

---

## 5. Correctness and testing requirements

This section is the product. Build it in parallel with the features, not after.

| ID | Requirement | Done when |
|---|---|---|
| TR-1 | Unit tests for the paper's scenarios: Figure 7 log states, Figure 8 commit rule, vote-granting rules, conflict backtracking, joint-consensus quorum math | Each scenario is a named test; listed in CORRECTNESS.md |
| TR-2 | **Invariants checked after every simulated event**: Election Safety, Leader Append-Only, Log Matching, Leader Completeness, State Machine Safety, plus "no committed entry lost", "applied prefixes identical across nodes", "every quorum of `C_old` intersects every quorum of `C_new` during joint config" | Any violation prints the seed and a minimal event trace |
| TR-3 | **Deterministic simulator**: seeded PRNG, virtual clock, priority-queue scheduler; network model (drop, duplicate, reorder, latency distribution, symmetric/asymmetric/one-way partitions, bridge and ring partitions); disk model (fsync latency, crash drops unfsynced writes, torn last record); process model (kill, restart, pause with clock jump); per-node clock skew; randomized client workload with in-sim history recording | CI: ≥ 2,000 seeds × 50k steps per PR; nightly ≥ 1M steps; `sim --seed N` reproduces any failure byte-for-byte |
| TR-4 | **External linearizability**: Maelstrom `lin-kv` (Knossos) with `partition` nemesis, ≥ 60 s runs at a stated rate; real-cluster history export → **Porcupine** KV model, under the chaos tooling | Both pass; commands and outputs committed under `results/` |
| TR-5 | Crash consistency: `SIGKILL` loops during heavy writes with restart + verification (same discipline as LSM-KV), plus simulated torn writes and fsync loss | ≥ 1,000 kill/restart cycles, zero committed-entry loss |
| TR-6 | Fuzzing: `cargo-fuzz` on message decoding and on the core with arbitrary event sequences, under ASan | Execution count and zero-crash result reported |
| TR-7 | Real-cluster chaos: userland partition proxy between nodes, `SIGSTOP`/`SIGKILL`, `libfaketime` clock jumps; throughput timeline recorded during nemesis | Availability plot with nemesis events annotated |
| TR-8 | **Negative demonstrations** (the harness must be shown to have teeth): (a) disable pre-vote → rejoining node disrupts the leader, measured; (b) lease reads with skew beyond the bound → checker flags a stale read; (c) remove the Figure 8 current-term commit rule → simulator finds a State Machine Safety violation and prints the seed | All three committed as runnable scripts with captured output |
| TR-9 | **Bug log**: every bug the harness finds during development recorded in BUGS.md with seed, symptom, root cause, fix commit | Nonzero count is expected and is a selling point |

---

## 6. Performance requirements and evaluation protocol

All headline numbers: **fsync on**, batching and pipelining on, hardware fully stated (CPU, disk model and filesystem, kernel, NIC/topology). Anything measured with fsync off is labeled as such and never headlined.

| ID | Requirement |
|---|---|
| PR-1 | Workloads: YCSB-style A/B/C mixes, value sizes 128 B and 1 KB, key space 1M; closed-loop with N clients and **open-loop at fixed rate with coordinated-omission-aware latency** (HdrHistogram) — same methodology as LazerBook |
| PR-2 | Report **throughput-vs-p99 curves**, not single points; 3-node and 5-node; localhost and cross-node (separate cluster nodes or VMs); three independent runs, medians, spread stated |
| PR-3 | Ablations: batching off/on, pipelining off/on, ReadIndex vs lease reads vs stale reads, fsync vs fdatasync |
| PR-4 | **etcd baseline** (current v3.x) on identical hardware using etcd's own `tools/benchmark` with matching value size and client count; report the ratio and explain the mechanism (bbolt B+tree with per-txn fsync vs LSM group commit; gRPC vs framed TCP) |
| PR-5 | Failover: leader killed at steady state; measure time to first post-failover commit across ≥ 100 trials at 150–300 ms election timeouts; report median/p99 |
| PR-6 | Snapshot: creation stall time and transfer throughput for 1 GB state; lagging-follower catch-up time |
| PR-7 | Latency breakdown (fsync / RTT / apply) and flame graphs for the write path |

**Targets** (to validate, not to claim): ≥ 100K writes/s at 1 KB on 3 nodes localhost NVMe with p99 < 5 ms at half of peak; ReadIndex reads ≥ 300K/s; lease reads bounded by local LSM read speed; failover median < 500 ms at 150 ms timeouts; no write unavailability during membership change beyond election.

---

## 7. Documentation deliverables

- `README.md` — results first (tables, curves, checker outputs), then architecture, then **"Not claimed"** section in the RotomAI style.
- `DESIGN.md` — ADRs: sans-IO core; pre-vote + check-quorum; ReadIndex vs lease trade-off; joint consensus vs single-server changes; snapshot via checkpoint; `applied_index` atomicity; session expiry.
- `CORRECTNESS.md` — matrix of property → test/invariant/checker that enforces it.
- `BUGS.md` — harness-found bugs with seeds.
- `BENCH.md` — full methodology, hardware, raw data paths, how to reproduce.
- `OPERATIONS.md` — running a cluster, admin CLI, metrics, chaos scripts.

---

## 8. Milestones (6 weeks part-time, ~12 h/week)

| Week | Exit criteria |
|---|---|
| 1 — M0 | Sans-IO core with election + replication (in-memory log); simulator skeleton with partitions; five safety invariants; 1,000 seeds pass |
| 2–3 — M1 | Durable log + hard state; LSM-KV adapter with atomic `applied_index`; TCP transport; client sessions; ReadIndex; 3-node cluster serves traffic; Maelstrom `lin-kv` passes without nemesis |
| 4 — M2 | Checkpoint snapshots, streaming `InstallSnapshot`, compaction; Maelstrom passes with `partition` nemesis; Porcupine on real cluster; kill-loop crash tests |
| 5 — M3 | Learners + joint consensus, pre-vote/check-quorum/leader transfer, lease reads; TR-8 negative demos; fuzzing; simulator at CI scale |
| 6 — M4 | Benchmarks + etcd baseline + failover + availability plots; metrics and dashboard; Docker Compose; docs complete; `v1.0.0` tagged |

---

## 9. Stretch goals (only after v1.0.0)

- **TLA+ trace validation**: record implementation traces and check them against the Raft TLA+ spec with TLC. Directly speaks the AWS/MongoDB formal-methods culture.
- **In-process linearizability checker** (Porcupine's algorithm in Rust) so the simulator checks histories without exporting.
- **Multi-Raft with static range sharding** and a trivial placement table — no cross-range transactions. Shows the Cockroach/TiKV shape.
- Kubernetes StatefulSet manifests (makes the "Kubernetes" skill line honest).
- Witness/read-only replicas; `fsync` on a separate thread with completion ordering.

---

## 10. Questions the project must answer cold

These are what a Spanner, DynamoDB, or Cockroach engineer will ask. Each should map to a design decision and a test in the repo.

1. Why can a leader only commit current-term entries by counting? Show the Figure 8 scenario.
2. What does pre-vote prevent, and why does leader transfer need to bypass it?
3. Why can't a fresh leader serve ReadIndex before its no-op commits?
4. What breaks lease reads, and how big can the clock drift be before it does?
5. Where exactly is the fsync before granting a vote, and what happens if it's after?
6. How is `applied_index` kept consistent with the LSM state across a crash mid-apply?
7. Why joint consensus instead of one-at-a-time changes? What was Ongaro's 2015 bug?
8. How does a snapshot avoid blocking writes, and what is in the checkpoint?
9. What makes apply idempotent under client retries across a leader change?
10. What did the simulator find that unit tests didn't? (BUGS.md)

---

## 11. Risks and claim discipline

- **Determinism must be designed in at week 1.** Any I/O, clock, or thread in `raft-core` kills the simulator and the fuzzer.
- **Shared-cluster noise.** Use dedicated allocations, three runs, medians, spread stated — the same discipline RotomAI's README already uses.
- **Say "Jepsen-style via Maelstrom/Knossos and Porcupine," not "Jepsen-tested."** A full Jepsen run is a different artifact; the distinction is exactly the kind of thing a storage engineer checks.
- **No number on the resume without a committed `results/` artifact and stated hardware.** This is the lesson from the RotomAI line.
- **Scope:** multi-Raft and TLA+ are stretch. A finished, verified single group beats a half-built sharded one.

---

## 12. References to build against

Raft paper (Ongaro & Ousterhout, 2014) and Ongaro's thesis (2014) — especially §4 (membership), §6.3 (sessions), §6.4 (reads); etcd `raft` and TiKV `raft-rs` for the Ready/sans-IO pattern; Maelstrom (`jepsen-io/maelstrom`) for `lin-kv`; Porcupine (`anishathalye/porcupine`); FoundationDB's and TigerBeetle's simulation-testing write-ups for the simulator design; etcd `tools/benchmark` for the baseline; HdrHistogram for latency.