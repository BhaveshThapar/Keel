# Correctness

Every property this system claims, and the specific thing that enforces it. If a
row has no enforcement, the property is not claimed.

Status legend: **enforced** — a test or checker fails when the property breaks.
**planned** — the mechanism is designed but not yet built.

## Raft safety properties

| Property | Enforced by | Status |
|---|---|---|
| **Election Safety** — at most one leader per term | Simulator invariant, checked after every event | planned (M0) |
| **Leader Append-Only** — a leader never overwrites its own log | Simulator invariant; `RaftLog::truncate_suffix` asserts it never cuts committed history | planned (M0) |
| **Log Matching** — same index and term implies identical prefixes | Simulator invariant (incremental digest per `(index, term)`) | planned (M0) |
| **Leader Completeness** — a new leader holds every committed entry | `paper_scenarios::vote_is_granted_only_to_an_up_to_date_candidate`; simulator invariant on every leadership assertion | partial |
| **State Machine Safety** — no two nodes apply different entries at the same index | `Cluster::assert_applied_prefixes_agree` in every cluster test; simulator invariant | partial |

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

## Determinism

| Property | Enforced by | Status |
|---|---|---|
| Identical inputs produce identical outputs | `determinism::identical_inputs_produce_identical_outputs` | enforced |
| The seed is the only source of variation | `determinism::the_seed_is_the_only_source_of_variation` | enforced |
| A whole cluster replays identically | `determinism::a_whole_cluster_replays_identically` | enforced |
| The core cannot reach a clock, thread, or file descriptor | `determinism::the_core_has_no_io_dependencies` (dependency assertion) | enforced |

## Not yet enforced

Named here so the gaps are visible rather than discovered:

- Durable log recovery, torn-tail discard, and group commit — `keel-log` does not exist yet (M1).
- Crash consistency under `SIGKILL`, and atomic `applied_index` with state machine data (M1).
- Exactly-once client sessions across a failover (M1).
- Snapshots, streaming `InstallSnapshot`, and log compaction (M2).
- External linearizability checking via Maelstrom and Porcupine (M1/M2).
- Fuzzing of message decoding and arbitrary event sequences (M3).
- Clock-skew nemesis proving lease reads fail outside their drift bound (M3).
