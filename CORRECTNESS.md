# Correctness

Every property this system claims, and the specific thing that enforces it. If a
row has no enforcement, the property is not claimed.

Status legend: **enforced** — a test or checker fails when the property breaks.
**planned** — the mechanism is designed but not yet built.

## Raft safety properties

| Property | Enforced by | Status |
|---|---|---|
| **Election Safety** — at most one leader per term | `Oracle::observe_leader`, after every simulated event | enforced |
| **Leader Append-Only** — a leader never overwrites its own log | `Oracle::observe_leader_log`; `RaftLog::truncate_suffix` also asserts it never cuts committed history | enforced |
| **Log Matching** — same index and term implies identical prefixes | `Oracle::observe_entries`, comparing cumulative prefix digests | enforced |
| **Leader Completeness** — a new leader holds every committed entry | `Oracle::check_leader_completeness` on every leadership assertion, plus `paper_scenarios::vote_is_granted_only_to_an_up_to_date_candidate` | enforced |
| **State Machine Safety** — no two nodes apply different entries at the same index | `Oracle::observe_applied`; `Cluster::assert_applied_prefixes_agree` in the unit tests | enforced |
| **No committed entry lost** — a committed entry is never discarded | `Oracle::check_rewrite`, at the moment a node discards log entries | enforced |

## Rules the implementation has to get right

| Rule | Enforced by | Status |
|---|---|---|
| Figure 8: a leader commits only current-term entries by counting replicas | `paper_scenarios::figure_8_leader_does_not_commit_an_old_term_entry_by_counting` | enforced |
| …and the guard is load-bearing | `figure_8_without_the_guard_the_old_term_entry_commits` (`--features negative-demos`) | enforced |
| Figure 7: divergent follower logs converge on the leader's | `paper_scenarios::figure_7_all_divergent_followers_converge_on_the_leader` | enforced |
| Conflict hints skip a whole term per round trip | `paper_scenarios::figure_7_conflict_hints_beat_one_index_at_a_time` | enforced |
| A vote goes only to an up-to-date candidate (§5.4.1) | `paper_scenarios::vote_is_granted_only_to_an_up_to_date_candidate` | enforced |
| One vote per term | `paper_scenarios::a_node_votes_at_most_once_per_term` | enforced |
| A vote is durable before the grant is sent | Asserted inside `send_vote_req`: a grant must carry a `HardState` naming the voter | enforced |
| A leader counts itself only up to what it has fsynced | `log::tests::next_committed_stops_at_the_persist_watermark`; `RaftLog::set_persisted` clamps to the log | enforced |
| A persist acknowledgement that a truncation invalidated is rejected | `RaftCore::advance` matches the acknowledged term against the log ([KEEL-6](BUGS.md)); `log::tests::a_truncation_pulls_both_watermarks_back` | enforced |
| Pre-vote stops a rejoining node from deposing a healthy leader | `election::pre_vote_stops_a_rejoining_node_from_deposing_the_leader` (runs both with and without) | enforced |
| Check-quorum makes a leader without a majority step down | `election::leader_without_a_quorum_steps_down` | enforced |
| Leader transfer completes inside one election timeout | `election::leader_transfer_moves_leadership_within_one_election_timeout` | enforced |
| A fresh leader will not serve a read before its no-op commits | `cluster_behaviour::a_fresh_leader_parks_reads_until_its_no_op_commits` | enforced |
| A lease lapses when the leader loses contact | `cluster_behaviour::lease_reads_are_only_valid_while_the_lease_holds` | enforced |
| Joint consensus quorums always intersect | `quorum::tests::quorums_of_both_halves_always_intersect` (exhaustive over subsets) | enforced |
| Voter changes pass through `C_old,new` and leave it automatically | `cluster_behaviour::adding_voters_goes_through_a_joint_configuration_and_leaves_it_automatically` | enforced |
| Only one configuration change in flight | `cluster_behaviour::a_second_configuration_change_is_refused_while_one_is_in_flight` | enforced |
| A leader removed from the configuration steps down | `cluster_behaviour::a_leader_that_removes_itself_steps_down` | enforced |
| Membership changes do not stall writes | `cluster_behaviour::writes_keep_flowing_during_a_membership_change` | enforced |
| A far-behind follower catches up from the log, not a snapshot | `cluster_behaviour::a_follower_that_missed_thousands_of_entries_catches_up_without_a_snapshot` | enforced |

## The simulator

The properties above are checked after **every event** in every simulated run,
against a global oracle no individual node can see. Each check is a single digest
comparison, which is what makes per-event checking affordable.

| Property | Enforced by | Status |
|---|---|---|
| A seed replays byte-for-byte | `simulation::a_seed_replays_exactly`; `keel-sim determinism` in CI | enforced |
| Different seeds explore different schedules | `simulation::different_seeds_produce_different_runs` | enforced |
| The cluster actually makes progress | `simulation::the_cluster_makes_progress` | enforced |
| A leader never commits an earlier term's entry by counting | `simulation::no_leader_ever_commits_an_old_term_entry_by_counting` (must be exactly zero) | enforced |

### Coverage

A clean run over a fault schedule that never partitioned anything would prove
nothing, so the simulator reports which states it reached and a test fails if a
heavy-fault run does not reach them: partitions, crashes, dropped messages,
leadership changes, followers having a divergent tail overwritten, and leaders
holding an earlier term's entry at their commit index.

`simulation::heavy_faults_actually_reach_the_interesting_states` enforces this.
It exists because of [KEEL-4](BUGS.md): the original five-node schedule reached
the Figure 8 window exactly zero times, so a correct build and a deliberately
broken one were indistinguishable. CI sweeps both cluster sizes across all three
profiles for the same reason.

### Does the checker catch anything?

`scripts/negative-demos/figure-8.sh` removes the Figure 8 current-term commit
rule and requires the simulator to find the resulting violation, with a control
run proving the same fault schedule is survivable when the rule is present.

Recorded run (`results/negative-demos/figure-8.txt`), fig8-hunt profile, 3 nodes,
80,000 steps:

| Build | Result |
|---|---|
| Rule present (control) | 40 of 40 seeds pass |
| Rule compiled out | 5 of 40 seeds fail, all Leader Completeness |

The control is the half that makes the experiment mean anything. A failure
without it would only show the schedule was too harsh.

## Determinism

| Property | Enforced by | Status |
|---|---|---|
| Identical inputs produce identical outputs | `determinism::identical_inputs_produce_identical_outputs` | enforced |
| The seed is the only source of variation | `determinism::the_seed_is_the_only_source_of_variation` | enforced |
| A whole cluster replays identically | `determinism::a_whole_cluster_replays_identically` | enforced |
| The core cannot reach a clock, thread, or file descriptor | `determinism::the_core_has_no_io_dependencies` (dependency assertion) | enforced |

## Not yet enforced

Named here so the gaps are visible rather than discovered:

- **Byte-level** torn writes. The simulator models durability at record
  granularity: a crash loses everything not yet fsynced. Whether a half-written
  record is discarded correctly is a question about a parser, and belongs to
  `keel-log`'s own tests (M1).
- **What is at an index a durable write covers.** `SimDisk` stages a `Ready`'s
  entries as an opaque batch, so it models *when* a write becomes durable but not
  *what* it contains. A stale acknowledgement and a current one are
  indistinguishable to it, which is why it could not find [KEEL-6](BUGS.md).
  Closed in M1 by running the real `keel-log` underneath the simulator.
- Durable log recovery and group commit — `keel-log` does not exist yet (M1).
- Crash consistency under `SIGKILL`, and atomic `applied_index` with state machine data (M1).
- Exactly-once client sessions across a failover (M1).
- Snapshots, streaming `InstallSnapshot`, and log compaction (M2).
- External linearizability checking via Maelstrom and Porcupine (M1/M2).
- Fuzzing of message decoding and arbitrary event sequences (M3).
- Clock-skew nemesis proving lease reads fail outside their drift bound (M3).
