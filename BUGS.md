# Bugs the harness found

Every defect found by a test, the simulator, or the fuzzer — not by reading the
code — is recorded here with its symptom, root cause, and fix. A nonzero count
is the point: a verification harness that has never caught anything has not been
shown to work.

Format: what broke, how it was found, why it happened, what fixed it.

---

## KEEL-1 — a leader never resumed replication to a follower that came back

**Found by** `cluster_behaviour::a_follower_that_missed_thousands_of_entries_catches_up_without_a_snapshot`

**Symptom.** A follower partitioned away during 2,000 writes stayed permanently
at `matched = 1` after the partition healed, applying zero entries. Heartbeats
flowed and the follower answered them, but no `AppendEntries` ever followed. The
leader's own view showed the follower in `Replicate` state, apparently healthy.

**Root cause.** Two independent latches, both set while the follower was
unreachable and neither cleared by anything the follower could do:

1. In `Probe` state the leader sets `probe_sent` after each probe and clears it
   only when a response arrives. Probes lost to the partition left it set.
2. In `Replicate` state the leader had pipelined 16 `AppendEntries` into the
   in-flight window before the partition took effect. `Inflights::free_le` only
   runs on an `AppendAccepted`, so a window filled with messages that the
   network swallowed stays full forever, and `is_paused()` returns true for good.

The second is the one that actually fired here, and it is the nastier of the
two: the leader's flow control was throttling on evidence that had been stale
for thousands of entries.

**Fix.** A heartbeat response is proof the follower is reachable, which makes any
throttle derived from lost messages invalid. `handle_heartbeat_resp` now clears
`probe_sent` and, if the window is full in `Replicate`, releases one slot via
`Inflights::free_first_one`. One append gets through; its acknowledgement frees
the rest through the normal `free_le` path.

This is the same remedy etcd applies, which is worth knowing: pipelining plus
flow control has a liveness hazard that the Raft paper does not discuss, because
the paper does not pipeline.

---

## KEEL-2 — a truncated log tail would not have been re-persisted

**Found by** review while writing the `Ready` contract, prompted by the Figure 7
convergence test exercising follower truncation.

**Symptom.** No test failure yet — caught before the durable log existed, which
is why it is recorded here rather than as a near miss. The core tracked how far
it had handed entries to the host with a monotonic `stable_offset` watermark. A
follower that truncated at index 5 and appended fresh entries 5..12 would have
reported `last_index = 12` against a `stable_offset` of 10, so only indices 11
and 12 would have gone into `Ready.entries`. Indices 5..10 would have kept their
old, overwritten contents on disk.

**Root cause.** A single high-water mark cannot describe "the log changed below
where you have already written". Truncation moves history backwards; a monotonic
watermark cannot represent that.

**Fix.** The watermark moved into `RaftLog` as `unstable_from`, the lowest index
the host has not been handed. `append`, `try_append`, and `truncate_suffix` all
pull it back to the lowest index they touched, and `mark_handed_off` pushes it
forward only after a `Ready` carries those entries out. The core no longer tracks
persistence position at all.
