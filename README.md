# Keel

A Raft-replicated key-value store in Rust, built on an LSM storage engine, and
verified by a deterministic simulator that replays any failure from a seed.

> **Status: in development (M0).** The consensus core is built and tested; the
> durable log, storage adapter, and networking are not. No performance number is
> claimed yet, because none has been measured. See [Not claimed](#not-claimed).

## What is here today

The [`keel-raft`](crates/keel-raft/) consensus core: a Raft implementation that
does no I/O, owns no threads, and reads no clock. You feed it events and it hands
back a `Ready` describing what to persist, what to send, and what to apply.

That constraint is the whole design. Because the core is a pure function of its
inputs, the same event sequence always produces the same output — so a simulator
can drive thousands of seeded clusters through partitions and crashes and replay
any failure exactly, and the identical code runs under a real network, under
Maelstrom, and inside that simulator with no conditional compilation anywhere.

Implemented and tested: leader election with **pre-vote** and **check-quorum**,
log replication with pipelining and conflict-term backtracking, the Figure 8
commit rule, **ReadIndex** and **lease** reads with follower forwarding,
**leader transfer**, and **learners with joint-consensus** membership changes.

## Correctness

45 tests, including the Raft paper's own scenarios encoded directly:

| Scenario | Test |
|---|---|
| Figure 7 — six divergent follower logs converge on the leader | `figure_7_all_divergent_followers_converge_on_the_leader` |
| Figure 8 — an old-term entry on a majority still may not commit | `figure_8_leader_does_not_commit_an_old_term_entry_by_counting` |
| §5.4.1 — a vote goes only to an up-to-date candidate | `vote_is_granted_only_to_an_up_to_date_candidate` |
| Pre-vote stops a rejoining node from deposing a healthy leader | `pre_vote_stops_a_rejoining_node_from_deposing_the_leader` |
| A fresh leader parks reads until its no-op commits | `a_fresh_leader_parks_reads_until_its_no_op_commits` |

Two of those tests run the same scenario twice, once with the safety rule
disabled, to show the harness actually catches the violation rather than merely
passing. The Figure 8 guard can be compiled out with `--features negative-demos`,
and the old-term entry then commits, which is the bug the rule exists to prevent.

[CORRECTNESS.md](CORRECTNESS.md) maps every claimed property to the test that
enforces it, and lists the ones that are not enforced yet.
[BUGS.md](BUGS.md) records what the harness has caught so far.

```
cargo test --workspace
cargo test -p keel-raft --features negative-demos
```

## Not claimed

- **No performance number.** Nothing has been benchmarked. The throughput,
  latency, and etcd-comparison figures this project intends to publish will be
  measured on stated Linux hardware with fsync on, or they will not be published.
- **Not Jepsen-tested.** The plan is Jepsen-*style* checking via Maelstrom and
  Porcupine. A real Jepsen run is a different artifact, and the distinction
  matters.
- **No durability yet.** The core is exercised against an in-memory harness where
  an append is durable immediately. The real disk path, and everything that can
  go wrong in it, is M1 work.
- **Single Raft group.** No sharding, no cross-shard transactions, no
  geo-replication, no Byzantine fault tolerance, no TLS or authentication.
- **The simulator does not exist yet.** Everything above was found by
  hand-written tests. The seeded, fault-injecting simulator that this design
  exists to enable is the next milestone.

## Design

[DESIGN.md](DESIGN.md) records the decisions and what each one costs. The
load-bearing one is the sans-IO core: `keel-raft` depends on `bytes`,
`thiserror`, and optionally `serde`, and a test fails the build if anything that
could reach a clock or a socket is added to it.

## License

MIT or Apache-2.0, at your option.
