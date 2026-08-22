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

---

## KEEL-3 — the simulator's committed-entry check was both wrong and blind

**Found by** running the simulator against a deliberately broken build.

**Symptom.** Two failures in opposite directions from the same check, which is
why this is one entry and not two.

First, with every safety rule intact, 37 of 300 chaos seeds reported "a committed
entry was lost". Second, with the Figure 8 commit rule *compiled out* — a build
that is definitely wrong — 100 seeds reported nothing at all. A checker that
fails on correct code and passes on broken code is worse than no checker,
because it launders both results into noise.

**Root cause.** Two separate mistakes.

The false positives came from checking the wrong thing. The check fired whenever
a node discarded log entries at or above any index that had ever been committed,
without asking whether the *discarded content* was the committed content. But a
leader overwriting a follower's divergent tail is not a bug — it is the entire
mechanism by which Raft converges. The check was flagging Raft working.

The blindness came from checking in the wrong place. The original comparison ran
against the highest recorded committed index at or below a node's own commit
index. A node that overwrote a low index and then committed a high one was
compared only at the high index, where nothing disagreed, and the loss at the low
index was never looked at.

**Fix.** `LogDigest::sync` now returns every `(index, digest)` pair it discards,
and the loss is checked at the moment of the discard: a violation is reported
only when the discarded digest *equals* what was recorded as committed at that
index. Discarding divergent content is silent; discarding committed content is
not.

**What made it visible.** Only the negative demonstration. Both halves of the
check looked reasonable, and 500 clean seeds looked like evidence. It took
running the checker against a build known to be broken to learn that the clean
runs meant nothing. That is the argument for TR-8 in one paragraph.

---

## KEEL-4 — the fault schedule could not reach the state it was meant to test

**Found by** instrumenting the simulator after KEEL-3, once it was clear a clean
run needed independent evidence.

**Symptom.** With the Figure 8 rule compiled out, a five-node cluster under heavy
faults recorded **zero** occurrences of the bug the rule prevents, across every
seed. The broken build and the correct build behaved identically, so no checker
of any quality could have told them apart.

**Root cause.** Two compounding coverage gaps.

*Cluster size.* Commit needs the k-th highest match index, where k is the quorum
size. At five nodes that is the third highest, so two followers must sit on an
earlier term's entry simultaneously. At three nodes it is the second highest, so
one follower suffices. Moving to three nodes took the count from 0 to 36 per
seed.

*Fault timing.* Reaching the window is not enough; the leader has to *die* inside
it, and the window is one message round wide while the nemesis fired every few
hundred rounds. Random faults hit it essentially never.

**Fix.** A `fig8-hunt` profile that aims rather than sprays: it strikes the
leader the moment it commits an earlier term's entry. With it, the correct build
survives 150 seeds and the broken one fails 5 of 40 with Leader Completeness
violations.

The simulator now also reports coverage — partitions, crashes, leadership
changes, entries overwritten, and how often a leader's commit index rested on an
earlier term's entry — and a test fails if a heavy-fault run does not reach those
states. A pass is only worth as much as the states the run actually visited.
