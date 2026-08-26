# Operations

Running a Keel cluster, watching it, and breaking it on purpose.

## Starting a node

```
keel-server \
  --id 1 \
  --dir /var/lib/keel \
  --listen 0.0.0.0:7001 \
  --client 0.0.0.0:7101 \
  --admin  0.0.0.0:7201 \
  --peer 1=node1:7001 --peer 2=node2:7001 --peer 3=node3:7001 \
  --sync durable
```

Every node is given the whole peer list, **including itself**. A node not in its
own peer list refuses to start rather than serving alone, because a cluster of
one that believes it is a cluster of three is the failure mode that loses data
quietly.

To route a node before it votes, keep it in every `--peer` list and name the
initial voters explicitly on every process:

```
--voter 1 --voter 2 --voter 3
```

Node 4 may then start with the same routes as a non-member and be added as a
learner. Omitting every `--voter` keeps the backward-compatible behaviour in
which every routed peer is an initial voter.

### Wait for the ready file, not the port

`--dir/keel.ready` appears once recovery is finished. The listener is bound
*before* the log is replayed, so a supervisor that waits for the socket starts
sending traffic to a node that is still recovering. The file is published by
rename, so it never exists half-written.

```
until [ -f /var/lib/keel/keel.ready ]; do sleep 0.2; done
```

### `--sync`

| Mode | What it means |
|---|---|
| `durable` | `F_FULLFSYNC` on macOS, `fdatasync` on Linux. The only mode under which a durability claim may be made. |
| `barrier` | Writes are ordered but not made to survive power loss. |
| `none` | Neither ordered nor durable. |

The mode is reported in `/status` and as the `keel_sync_durable` metric, and a
benchmark taken in anything but `durable` is refused by the gate in BENCH.md.
It is the first field of `/status` because it is the first thing anyone
diagnosing a data-loss report needs to know.

## Looking at a running cluster

```
curl -s localhost:7201/status
curl -s localhost:7201/metrics
```

`/status` is JSON for a person. `/metrics` is Prometheus text exposition,
parsed back by a test the way a scraper parses it, so a malformed body is a
failing build rather than a silently empty dashboard.

| Metric | What a bad value looks like |
|---|---|
| `keel_is_leader` | two nodes at 1 in the *same* term would be an Election Safety violation; in different terms it is a deposed leader that has not found out yet, which is normal and brief |
| `keel_term` | climbing while nothing else moves means a cluster that cannot elect — a partitioned node campaigning, or a quorum that is not there |
| `keel_commit_index`, `keel_applied_index`, `keel_persisted_index` | applied far behind commit is a slow state machine; *durable* far behind commit is a slow disk, and only that one costs a leader its ability to count itself |
| `keel_log_segments` | climbing and never falling means compaction is not happening |
| `keel_sync_durable` | 0 means no durability claim may be made about anything this node did |
| `keel_failed` | 1 means the node hit a storage error it cannot continue past. It stops serving rather than carrying on, because a node that cannot make writes durable and keeps acknowledging them is worse than a node that is down |
| `keel_entries_appended_total` ÷ `keel_readies_total` | **the batch size**, and the number to look at first when writes are slow. One entry per `Ready` means every operation is paying a whole round of persist, replicate and apply on its own, whatever it was told about batching — that is what a hundred writes a second looked like before ADR-035, while the same cluster with a batch of thirty did four thousand |
| `keel_readies_total` ÷ `keel_turns_total` | how often a turn had anything to do. Near zero on a busy node means the loop is spinning without progress; near one under no load means something is waking it |
| `keel_messages_sent_total` | flat on a leader means replication has stopped; climbing with `keel_commit_index` flat means it is being rejected |
| `keel_proposals_dropped_total` | climbing means the core is refusing proposals before they reach the log — not the leader, overloaded, or a configuration change already in flight. A client sees these as retries and a dashboard sees nothing else |
| `keel_snapshot_bytes_sent_total`, `keel_snapshot_bytes_received_total` | transfer progress; after a receiver restart the counter continues from the recovered per-file positions rather than retransmitting verified bytes |
| `keel_snapshot_checkpoint_seconds_total` ÷ `keel_snapshots_taken_total` | mean synchronous checkpoint stall |

## Operating membership and snapshots

Send commands to the current leader's admin address:

```
keel-admin --admin 127.0.0.1:7201 add-learner  --node 4
# Wait until node 4's applied index reaches the leader's commit index.
keel-admin --admin 127.0.0.1:7201 promote      --node 4
keel-admin --admin 127.0.0.1:7201 transfer-leader --to 4
keel-admin --admin 127.0.0.1:7204 remove       --node 1
keel-admin --admin 127.0.0.1:7204 snapshot
```

`promote` is ignored until the leader's replication tracker says the learner is
caught up. A `202 Accepted` means the leader accepted the operator request for
processing; confirm the resulting configuration through `/status`. Checkpoints
are otherwise automatic after 10,000 newly applied entries and can be tuned
with `--checkpoint-entries`.

## A cluster on one machine

```
docker compose -f deploy/docker-compose.yml up --build
open http://localhost:3000     # Grafana, anonymous, the Keel dashboard
open http://localhost:9090     # Prometheus
```

Three nodes, Prometheus scraping every five seconds — not the default fifteen,
because an election takes hundreds of milliseconds and at fifteen seconds the
whole event falls between two samples.

This is for watching, not for running anything: no TLS, no authentication, no
resource limits, and all three nodes on one host, so a machine failure takes the
cluster.

## The client

```
kv --node 127.0.0.1:7101 --node 127.0.0.1:7102 --node 127.0.0.1:7103 put k v
kv --node 127.0.0.1:7101 get k
kv --node 127.0.0.1:7101 incr counter --by 1
kv --node 127.0.0.1:7101 scan --start a --end z
```

Give it every node's client address; it finds the leader itself and follows
redirects.

### The one thing to get right: nonces

`--nonce` identifies a *session*. The same nonce reopens the same session, which
is what makes a retried command apply exactly once. It is also, for the same
reason, the thing that breaks a script that reuses one across independent
clients:

> Two clients sharing a nonce share a session. The second one's sequence numbers
> replay the first one's, so its writes are answered from the exactly-once cache
> and never applied — acknowledged, and gone.

Give each concurrent client its own nonce. [KEEL-9](BUGS.md) is what this looks
like when a *server* gets it wrong; the same shape is available to a caller.

## Breaking it on purpose

```
# What a seed would do, injecting nothing.
keel-chaos plan --seed 7 --nodes 3 --secs 60

# Do it: partitions, pauses, kills, and clock jumps where the host allows them.
scripts/chaos.sh 45 1 2 3 4

# A thousand kill cycles, and the question of whether anything acknowledged was lost.
scripts/kill-loop.sh 1000 durable 250

# The clock nemesis, which cannot run on macOS at all — see ADR-026.
scripts/chaos-clock.sh
```

Every schedule is drawn from a seed and printed before anything is injected, and
a run that injects no fault, or gets no acknowledgement, **fails** rather than
reporting a pass.

## When something is wrong

**A node will not start.** Its log is in `--dir`; the error names which of the
two subdirectories it could not open. A node that cannot recover its log is not
a node that starts empty — it refuses, because starting empty is how a cluster
silently loses a member's history.

**A node is up and not serving.** Check `keel_is_leader` across all three and
`keel_term`. A follower that cannot reach the leader parks client requests and
answers `Unavailable` after five seconds rather than holding the connection.

**Writes are being refused with `SessionExpired`.** Sessions expire on
leader-stamped time; a client that has been idle longer than the timeout
re-registers. This is the client library's job and `kv` does it.

**`keel_failed` is 1.** The node latched a storage error. It will not serve
again in that process. Look at its log output for the error, fix the disk, and
restart it — its Raft log is intact and it will catch up.
