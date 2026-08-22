# Design decisions

Each record states the decision, why, and what it costs. Decisions that have not
been implemented yet are marked *planned*.

---

## ADR-001 — The consensus core does no I/O

`keel-raft` takes `Input` events and returns a `Ready`. It never touches a
socket, a file, a thread, or a clock. Time enters only as `Input::Tick`, and
randomness only as a seed in `Config`.

**Why.** This is what makes deterministic simulation possible, and deterministic
simulation is the only practical way to test a consensus implementation against
the fault schedules that actually break it. A core that reads `Instant::now()`
cannot be replayed, so a failure found at 3 a.m. under a rare interleaving is
gone forever. Retrofitting this property is not realistic; it has to be the first
constraint.

**Cost.** The host must run a correct `Ready` loop, and that loop's ordering is a
safety requirement rather than an implementation detail. Getting it wrong is
possible, so the contract is documented on `Ready` itself and every host in this
repository — tests, simulator, server — drives it the same way.

**Enforced by** `determinism::the_core_has_no_io_dependencies`, which fails the
build if a dependency that could reach a clock or a socket appears.

---

## ADR-002 — The core holds the whole log in memory, and the host reports watermarks

The core owns every entry above the snapshot. It never reads back from host
storage. The host says how far it has persisted and applied via `Advance`.

**Why.** The alternative, which etcd's `raft` and TiKV's `raft-rs` both use, is a
`Storage` trait the core queries for older entries. That lets stable entries
leave memory, but it makes determinism contingent on mocking storage, adds a
fallible read to the middle of `step`, and is the single largest source of
complexity in both of those implementations. Watermarks also mean a host that
batches or reorders its own work cannot desynchronise the core: it reports where
it got to, not what it did.

**Cost.** Memory is bounded by the snapshot policy rather than by anything
adaptive. At a 32k-entry snapshot threshold with 1 KB values that is roughly
64 MB resident per node. This is a real cost and it is the reason to revisit the
decision if the entry-size distribution ever changes.

---

## ADR-003 — Persist before send, enforced by what goes in each `Ready`

The host must persist a `Ready`'s `hard_state` and `entries` before sending its
`messages`. The core guarantees the two always travel together: a vote grant
never appears in a `Ready` whose `hard_state` does not already record that vote.

**Why.** This is where the Raft paper's "persist before responding" requirement
actually lives (PRD question 5). If a node grants a vote, crashes, recovers
without the vote on disk, and grants a second vote in the same term, two leaders
can be elected in one term and Election Safety is gone. Making the ordering a
property of the data structure rather than a rule in a comment means a host
cannot get it subtly wrong.

The same argument applies to a leader counting itself: it advances its own
`matched` from the `persisted` watermark, never from `last_index`. A leader that
counted an unfsynced entry could commit something it then lost.

**Cost.** All messages in a `Ready` wait for the fsync, including heartbeats that
depend on nothing. Splitting the two is a known optimisation and is deliberately
deferred: it trades a latency win for a chance to get the ordering wrong.

---

## ADR-004 — Membership changes use joint consensus, applied at apply time

A change to the voter set enters `C_old,new`, where a decision needs a majority
of *both* configurations, and the leader proposes leaving it automatically once
the joint entry commits. Learner-only changes skip the joint phase because they
cannot split a quorum. The configuration takes effect when the entry is applied,
not when it is appended, and only one change may be in flight.

**Why.** One-at-a-time changes look simpler and were the paper's original
proposal, but Ongaro's 2015 correction showed that two overlapping single-server
changes can produce two disjoint quorums and elect two leaders (PRD question 7).
Joint consensus removes the whole class of race rather than patching the
instances of it. Apply-time activation matches etcd and TiKV, and combined with
the one-in-flight rule it keeps the configuration a function of the applied log,
which is exactly what a restarting node can reconstruct.

**Cost.** Two log entries and two commit rounds per voter change instead of one.
A snapshot must carry the configuration, since a node restoring from one has no
log left to replay it from.

---

## ADR-005 — ReadIndex by default, leases opt-in

A linearizable read gets the leader's commit index, confirmed by one heartbeat
round, and is served once the state machine has applied that far. Reads submitted
before the leader's own term no-op commits are *parked*, not refused. Followers
forward reads to the leader and serve them the same way.

**Why.** A fresh leader knows its log contains everything committed, but not
which of those entries *are* committed — the previous leader may have committed
an entry this one has not yet heard about. Serving a read before the term's no-op
commits could therefore miss a completed write (PRD question 3). Parking rather
than refusing avoids a client retry storm after every election, at no cost to
safety.

Lease reads skip the heartbeat round and are strictly faster, but they are only
correct while clock drift between nodes stays inside the assumed bound. That is
an assumption about the deployment, not a property of the algorithm, so it is
opt-in and the bound is explicit in the configuration.

**Cost.** One network round trip per read batch in the default mode. Batching
amortises it: all reads outstanding at a heartbeat share one round.

---

## ADR-006 — Flow control throttles must never outlive their evidence

The leader pipelines appends within a bounded in-flight window and probes a
lagging follower once per heartbeat. Both throttles are released when a heartbeat
response proves the follower is reachable.

**Why.** Learned from [KEEL-1](BUGS.md). Both throttles are set by the leader and
cleared by the follower's response. If the responses are lost — which is exactly
what a partition does — the throttle is held forever on evidence that is
arbitrarily stale, and replication to that follower never restarts even after the
network heals. The failure is silent: the leader's own status shows the follower
in a healthy `Replicate` state.

**Cost.** A heartbeat response can release a slot for a message that is still
genuinely in flight, so the window can briefly exceed its nominal size. That is
the correct trade: the window exists to bound memory, not to be exact.

---

## Planned

These are decided but not yet built. They are recorded here so the shape is
fixed before the code exists.

- **ADR-007 — Durable log layout.** One logical stream of segments, records of
  `[len][crc32c][type][postcard]`, hard state sharing the group-commit fsync
  path, logical truncation with later-record-wins on recovery, and a torn tail
  discarded rather than repaired.
- **ADR-008 — `applied_index` in the same write batch as the data.** Apply must
  be idempotent across a crash mid-apply, which means the index and the data it
  describes have to become durable together or not at all.
- **ADR-009 — Snapshots are storage-engine checkpoints.** Flush the memtable,
  hard-link the immutable SSTables, write a fresh manifest. The consensus layer
  sees only a handshake; the bytes move over the transport with resumable chunks.
- **ADR-010 — Exactly-once sessions.** `(client_id, seq)` dedup with a cached
  response, held in the state machine so it survives failover and appears in
  snapshots, expired deterministically by leader-stamped time in the log rather
  than by any node's local clock.
- **ADR-011 — fsync portability.** `fdatasync` on Linux, `F_FULLFSYNC` on macOS.
  These are not the same operation and the difference is large enough that a
  benchmark which does not say which one it used is uninterpretable.
