# Correctness

Every property this system claims, and the specific thing that enforces it. If a
row has no enforcement, the property is not claimed.

Status legend: **enforced** — a test or checker fails when the property breaks.
**planned** — the mechanism is designed but not yet built.

The rows themselves are checked. `scripts/check-docs.sh` fails the build if a
test named in this file does not exist in the workspace, because a row naming a
renamed test reads as enforcement and enforces nothing — which is worse than a
row that was never written.

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
| …and turning leases on does not move that guard | `cluster_behaviour::lease_configuration_does_not_bypass_the_no_op_park` | enforced |
| A lease lapses when the leader loses contact | `cluster_behaviour::lease_reads_are_only_valid_while_the_lease_holds` | enforced |
| A lease read costs no round trip | `cluster_behaviour::a_lease_read_is_answered_without_a_round_trip` | enforced |
| A step-down ends leadership without moving the term | `cluster_behaviour::a_leader_told_to_step_down_stops_leading_without_moving_the_term`; `cluster_behaviour::stepping_down_a_follower_does_nothing` | enforced |
| A step-down fails the reads it can no longer confirm | `cluster_behaviour::a_step_down_fails_the_reads_it_can_no_longer_confirm` | enforced |
| A restart applies from what the state machine reports, not what the log infers | `paper_scenarios::a_restart_does_not_re_apply_what_the_state_machine_already_applied`; `paper_scenarios::an_applied_index_above_the_commit_index_is_clamped` | enforced |
| A checkpoint bounds what the core holds in memory | `cluster_behaviour::a_checkpoint_bounds_what_the_core_holds_in_memory` | enforced |
| A checkpoint above the applied index is refused, and counted | `cluster_behaviour::a_checkpoint_above_what_was_applied_is_refused` | enforced |
| A checkpoint that goes backwards is refused | `cluster_behaviour::a_stale_checkpoint_is_refused` | enforced |
| A snapshot offer carries the checkpointed configuration, not the current one | `cluster_behaviour::a_snapshot_offer_carries_the_checkpointed_configuration` | enforced |
| Joint consensus quorums always intersect | `quorum::tests::quorums_of_both_halves_always_intersect` (exhaustive over subsets) | enforced |
| Voter changes pass through `C_old,new` and leave it automatically | `cluster_behaviour::adding_voters_goes_through_a_joint_configuration_and_leaves_it_automatically` | enforced |
| Only one configuration change in flight | `cluster_behaviour::a_second_configuration_change_is_refused_while_one_is_in_flight` | enforced |
| A leader removed from the configuration steps down | `cluster_behaviour::a_leader_that_removes_itself_steps_down` | enforced |
| Membership changes do not stall writes | `cluster_behaviour::writes_keep_flowing_during_a_membership_change` | enforced |
| A far-behind follower catches up from the log, not a snapshot | `cluster_behaviour::a_follower_that_missed_thousands_of_entries_catches_up_without_a_snapshot` | enforced |

## Checked by somebody else's checker

| Property | Enforced by | Status |
|---|---|---|
| A three-node cluster's history is linearizable, without a nemesis | `scripts/maelstrom.sh` — Jepsen's Maelstrom driving `lin-kv`, checked by Knossos; committed output in [`results/maelstrom/`](results/maelstrom/) | enforced |
| The same core runs under a third transport with no conditional compilation | the adapter in `keel-maelstrom` constructs the same `RaftCore` (FR-12) | enforced |
| The same, with the cluster cut into halves every ten seconds | `scripts/maelstrom.sh 60 30 partition`; committed output in [`results/maelstrom/`](results/maelstrom/) | enforced |
| A real cluster's history, recorded while it is partitioned, paused and killed, is linearizable | `scripts/porcupine.sh` — [Porcupine](https://github.com/anishathalye/porcupine) v1.3.0 over a history `kv workload` recorded; committed output in [`results/porcupine/`](results/porcupine/) | enforced |
| …and that history has concurrency *within* each client, not only between them | `kv workload --depth 8`, which is what `keel-chaos` starts. A client with one request outstanding never overlaps itself, so a checker handed a depth-1 history has almost nothing to reorder and accepts almost anything | enforced |
| …and the same checker rejects that history with one read's result replaced | the control arm of the same script, which is what makes the experiment arm evidence | enforced |
| A read records what it returned, so a model has something to contradict | `history::tests::a_read_records_what_it_returned` | enforced |
| An unanswered read carries no result, so it is not read as "the key was absent" | `history::tests::an_unanswered_operation_carries_no_result` | enforced |
| Several clients' histories merge into one timeline with one origin | `history::tests::merged_histories_share_an_origin_and_come_out_in_order` | enforced |
| A thousand kill cycles against a real cluster lose no acknowledged write | `scripts/kill-loop.sh`; committed output in [`results/chaos/`](results/chaos/) | enforced |
| Clock skew, under an external checker | — | not planned: Maelstrom's clock nemesis moves the wall clock, and Keel reads `CLOCK_MONOTONIC`. The clock fault is [`keel-chaos`](crates/keel-chaos/)'s, with its own probe |

The distinction that makes this worth having: every other check in this file is
one we wrote, against a property we chose. Knossos applies a definition of
linearizability nobody here chose to a history it recorded itself, and it does
not care what anyone here believes about the code.

What the Maelstrom runs do not establish, stated because a passing external
checker invites more confidence than it has earned: the adapter **does not
persist**, because Maelstrom does not restart nodes with their storage intact.
Crash recovery is what the simulator's disk profiles, the kill loop and the
Porcupine run are for — the last of these records its history from real
`keel-server` processes that are being killed while the clients write.

And a note on the control arm, because it is easy to skip and it is the half
that matters. A checker that accepted everything would also accept a correct
history, so an acceptance on its own says nothing about the checker. The control
takes the same history, replaces one completed read's returned value with a
value nothing in the run ever wrote, and requires a rejection. Both arms are in
the committed output, side by side.

## A client that keeps several requests in flight

| Property | Enforced by | Status |
|---|---|---|
| Many requests on one connection are answered by label, in any order | `clients::tests::several_requests_on_one_connection_are_answered_by_label_in_any_order` | enforced |
| The outstanding count returns to zero however a request is answered | `clients::tests::the_outstanding_count_returns_to_zero_however_a_request_is_answered` — a drift here silently stops a healthy connection being read from | enforced |
| A dropped connection leaves nothing behind in the tables that find requests | `clients::tests::a_dropped_connection_leaves_nothing_behind_in_the_tables` | enforced |
| A stale request does not unhook the live one that displaced it | `clients::tests::sweeping_a_displaced_request_leaves_the_one_that_displaced_it_findable` — two parks can share a session pair when a client retries on a second connection, and removing by key alone loses the newer | enforced |
| Answering leaves the connection readable without blocking | `clients::tests::a_connection_that_has_been_answered_is_still_read_without_blocking` — a socket left blocking stalls the loop that also ticks the election clock, and the symptom is a cluster that never elects a leader | enforced |
| A read is answered under its own label once its index is applied | `clients::tests::a_read_is_answered_under_its_label_once_its_index_is_applied` | enforced |
| No two in-flight requests share a session | `pipeline::tests::no_two_outstanding_requests_share_a_session` — the property the whole design turns on, because a retry must always be of its session's highest sequence number | enforced |
| A resend carries the same session pair under the same label | `pipeline::tests::a_redirect_resends_the_same_session_pair_under_the_same_label` | enforced |
| A finished request frees its slot, and the sequence number moves on | `pipeline::tests::finishing_frees_the_slot_and_the_sequence_number_moves_on` | enforced |
| A full pipeline refuses rather than queueing without bound | `pipeline::tests::a_full_pipeline_refuses_the_next_submission` | enforced |
| A request past its deadline is reported as a timeout, never as a refusal | `pipeline::tests::a_request_past_its_deadline_is_reported_as_a_timeout` — a checker must read it as "may or may not have happened" | enforced |
| **Against a real cluster: many in flight, each applied exactly once** | `cluster::a_pipeline_keeps_many_requests_in_flight_and_applies_each_once` — 400 increments at depth 16, read back from a second client, and a double-apply reads as 2 | enforced |

## A cluster of real processes

| Property | Enforced by | Status |
|---|---|---|
| Three real nodes serve put, get, delete, scan, cas and incr | `cluster::a_three_node_cluster_serves_traffic` — separate processes, real sockets, real files | enforced |
| A client finds the leader by itself and follows redirects | the same test: it is given all three addresses and never told which is the leader | enforced |
| Acknowledged writes survive the leader being killed | `cluster::writes_survive_a_leader_being_killed` | enforced |
| A history is recorded in the shape a checker wants | `cluster::a_client_records_a_history_a_checker_can_read`; `history::tests::a_lost_answer_is_unknown_rather_than_refused` | enforced |
| A misconfigured node refuses to start rather than serving alone | `cluster::a_misconfigured_node_refuses_to_start` | enforced |
| A peer may be named rather than addressed, and a name that is not up yet is waited for | `--peer id=host:port` resolves at startup with a 30-second budget; every node in a cluster starts at once, so the first one up finds its peers unresolvable | enforced |
| A node says whether its fsyncs survive power loss, in its ready file | `cluster::a_three_node_cluster_serves_traffic` checks every node's | enforced |

## What a client observes

Every other section in this file is about what the *nodes* agree on. These are
about what a client is handed, which is a different claim: a cluster whose nodes
agree perfectly can still serve a stale read, because the staleness is in which
node answered and when.

| Property | Enforced by | Status |
|---|---|---|
| A linearizable read is confirmed at an index no older than what was already committed when it was asked for | the `Read Recency` oracle in `keel-sim`, checked at every confirmation under the `read-hunt` profile | enforced |
| A read returns what a reference state machine fed the same committed log holds at the answering node's applied index | `Oracle::check_read`, reported as `Read Correctness` | enforced |
| Reads are actually issued, confirmed *and* answered, and land on a cluster with something committed | `reads_are_actually_issued_confirmed_and_answered` — four counters, because a read can fail to happen at four separate points | enforced |
| The profiles that predate reads still issue none | `the_profiles_that_predate_reads_still_issue_none` — which is why their pinned fingerprints are still meaningful | enforced |
| Turning clock drift on changes the run; leaving it off draws nothing | `clock_drift_is_off_unless_a_profile_asks_for_it` | enforced |
| The default nemesis weights select exactly the ranges they replaced, for every roll | `the_default_weights_reproduce_the_ranges_they_replaced` | enforced |
| A roll never falls off the end of the weight table, whatever the weights | `every_roll_selects_an_action_whatever_the_weights_are` | enforced |
| A lease read is safe only inside its clock assumption, and unsafe outside it | `leases_are_only_safe_inside_their_clock_assumption`; `scripts/negative-demos/lease-drift.sh` — control clean, experiment dirty on the same seeds | enforced |
| Pre-vote stops a partitioned node burning terms nobody can hear | `pre_vote_stops_a_partitioned_node_from_burning_terms`; `scripts/negative-demos/pre-vote.sh` — a margin, because what pre-vote costs is availability rather than safety | enforced |
| A client's read observed under a real cluster's faults | `scripts/porcupine.sh` — see [Checked by somebody else's checker](#checked-by-somebody-elses-checker) | enforced |

## Membership, under the fault schedule

Until P23 these rested entirely on an in-process cluster whose own doc comment
admits FIFO messages, instantaneous persistence and no clock. `keel-sim` issued
neither `Input::ProposeConfChange` nor `Input::TransferLeader`, so joint
consensus — the one place where getting a quorum wrong elects two leaders — was
the one place the simulator had never been.

| Property | Enforced by | Status |
|---|---|---|
| Membership changes commit under partitions, crashes and leadership churn | the `membership-hunt` profile in the sweep; every Raft safety oracle applies unchanged | enforced |
| The joint configuration is genuinely open while other things happen | `membership_actually_changes_and_the_joint_configuration_is_actually_open` — `joint_config_windows` non-zero | enforced |
| The cluster actually reaches configurations it did not boot with | the same test: `distinct_configurations > 1` | enforced |
| A second change while one is in flight is refused, and that refusal happens | the same test: `conf_changes_refused > 0` | enforced |
| Leader transfers are requested under faults | the same test: `transfers_requested > 0` | enforced |
| The crash nemesis's quorum is a majority of the *voters*, and of both halves when joint | `World::on_nemesis` — a budget computed over every node that exists would kill enough of `C_old` to stop the cluster while the arithmetic still said a quorum survived | enforced |
| The profiles that predate membership changes propose none | `the_profiles_that_predate_membership_changes_propose_none` | enforced |
| A restarting node recovers its configuration from its snapshot and rebuilds the rest by replay, rather than keeping the one it held in memory | `the_membership_profile_never_takes_the_voter_set_below_three` — [KEEL-10](BUGS.md) | enforced |
| The voter set never falls to two, where a single crash stops the cluster | the same test, across eighty runs | enforced |
| …and at three nodes the profile is inert, which is stated rather than left to look like coverage | `the_membership_profile_is_inert_at_three_nodes` | enforced |

## A cluster under chaos

Faults injected into real processes, from a seed. The simulator cannot reach any
of these, because it replaces the parts they live in: a scheduler, a TCP stack, a
`SIGSTOP` that lands between two instructions.

| Property | Enforced by | Status |
|---|---|---|
| A cluster survives a partition, a pause and a kill without losing an acknowledged write | `real_cluster::a_cluster_survives_a_partition_a_pause_and_a_kill` | enforced |
| A one-way partition of one node does not stop the majority | `real_cluster::a_one_way_partition_does_not_stop_the_majority` | enforced |
| A killed node rejoins and the cluster still has what it acknowledged | `real_cluster::a_killed_node_rejoins_and_the_cluster_keeps_its_acknowledged_writes` | enforced |
| Isolating one node leaves every other pair connected | `cluster::tests::isolating_one_node_leaves_the_rest_of_the_cluster_connected` | enforced |
| A one-way isolation cuts one direction only | `cluster::tests::a_one_way_isolation_cuts_one_direction_only` | enforced |
| A split cuts across the two sides and not within either | `cluster::tests::a_split_cuts_across_and_not_within` | enforced |
| A stopped process is still alive, and can still be killed | `nemesis::tests::a_stopped_process_stays_alive_until_it_is_resumed`; `nemesis::tests::a_stopped_process_can_still_be_killed` | enforced |
| A signal at something that is not running is an error, not a silent no-op | `nemesis::tests::signalling_something_that_is_not_running_is_an_error_rather_than_a_silent_no_op` | enforced |
| A node that dies during startup is noticed rather than waited out | `nemesis::tests::a_process_that_dies_during_startup_is_noticed_immediately` | enforced |
| A seed determines the whole fault schedule | `schedule::tests::a_seed_determines_the_whole_schedule` | enforced |
| Every fault is paired with its repair, and the cluster ends healthy | `schedule::tests::every_fault_is_repaired_and_the_cluster_ends_healthy` | enforced |
| A split never puts a majority on the minority side | `schedule::tests::a_split_never_puts_a_majority_on_the_wrong_side` | enforced |
| Every kind of fault is actually reachable | `schedule::tests::a_hundred_seeds_reach_every_kind_of_fault` | enforced |
| A host that cannot move clocks draws a schedule with no clock jumps in it | `schedule::tests::a_host_without_clock_control_gets_a_schedule_without_clock_jumps` | enforced |
| …and a host that can, does | `schedule::tests::clock_jumps_do_occur_when_the_host_allows_them` | enforced |
| Elapsed time is not accepted as evidence of a clock jump | `clock::tests::a_probe_that_merely_waited_does_not_count_as_a_jump` | enforced |
| A run that injected no fault, or got no acknowledgement, is refused rather than reported as a pass | `keel-chaos run`, checked in `results/chaos/real-cluster.txt` | enforced |
| A jump reaches `CLOCK_MONOTONIC`, not just the wall clock | `keel-chaos clock-check`, recorded in `results/chaos/clock-jump.txt` — Linux only, see [ADR-026](DESIGN.md) | recorded, not in CI |

## What a number is allowed to mean

A benchmark is a claim, and these are the checks that stop an unsupportable one
being written down. Built at P24, before any code existed that could produce a
number: a gate added afterwards is a gate that has already been bypassed once.

| Property | Enforced by | Status |
|---|---|---|
| A run on a memory filesystem is refused | `publishable::tests::a_run_on_memory_is_refused` — an fsync on tmpfs returns without doing anything, so the run measures a memcpy | enforced |
| A run with fsync off is refused | `publishable::tests::a_run_without_fsync_is_refused`, for both `barrier` and `none` | enforced |
| A run on hardware nobody stated is refused | `publishable::tests::a_host_nobody_described_is_refused` | enforced |
| Fewer than three repetitions is refused | `publishable::tests::one_run_is_refused_and_three_is_the_floor` | enforced |
| A run from a modified tree, or one whose commit is unknown, is refused | `publishable::tests::a_modified_tree_and_an_unknown_commit_are_both_refused` — a number that cannot name the code that produced it is not reproducible, which is the same failure as an unstated CPU | enforced |
| …and the ablations those refusals name are still recordable, with the reason stamped in | `publishable::tests::an_admitted_run_carries_the_reason_it_cannot_be_published`; `gate::tmpfs_and_zero_fsync_are_refused_and_their_controls_are_admitted` | enforced |
| Nothing reaches `results/bench/` without evidence | `Evidence` is sealed to two types, so a third way to satisfy it will not compile; `gate::a_result_cannot_be_written_outside_the_bench_directory` covers the part the type system does not express | enforced |
| An Exploratory result says so above its numbers, not only in its header | `record::tests::an_unheadlineable_result_repeats_the_qualifier_above_the_numbers` | enforced |
| An open-loop stall is charged to every request it delayed | `workload::tests::an_open_loop_charges_a_stall_to_every_request_it_delayed` — the coordinated-omission correction, as a test rather than as a paragraph | enforced |
| A run that could not offer the rate it claims says so | `workload::tests::a_run_that_fell_behind_its_schedule_says_so` | enforced |
| A quoted percentile is never optimistic | `histogram::tests::a_percentile_is_never_optimistic` | enforced |
| …and the relative error stays bounded at every magnitude | `histogram::tests::the_relative_error_is_bounded_at_every_magnitude` | enforced |
| A campaign's plot regenerates byte for byte | `plot::tests::the_same_data_renders_the_same_bytes`; `plot::tests::a_series_recorded_out_of_order_draws_the_same_picture` | enforced |
| A single point is refused, because it is not a curve | `plot::tests::a_single_point_is_refused_because_it_is_not_a_curve` | enforced |
| A failover report with too few trials says its percentiles describe the draw | `failover::tests::a_report_with_too_few_trials_says_its_percentiles_describe_the_draw` | enforced |
| A failover trial that killed nothing is discarded, not counted as a fast recovery | `failover::tests::a_trial_that_killed_nothing_is_discarded_rather_than_timed` | enforced |

## Parsers, against bytes they did not write

Six targets, one per place a byte string arrives from somewhere this process
does not control. The contract each is held to is that it does not panic:
returning an error is correct, refusing to decode is correct, and producing
nonsense from nonsense is correct — but every one of these is reached from a
network or a disk, so a panic is a node a stranger can stop with one bad byte.

| Property | Enforced by | Status |
|---|---|---|
| Six targets compile and survive a smoke run | `every_target_survives_a_smoke_run` | enforced |
| …over inputs that have structure, rather than noise no parser gets past | the same test: more than a quarter of inputs must be structured | enforced |
| A smoke run replays exactly from its seed | `a_smoke_run_is_a_pure_function_of_its_seed` | enforced |
| A target that is written and never wired up is a failure, not an omission | `every_target_is_named_once` | enforced |
| A corrupted record is rejected | `a_corrupted_record_is_rejected` — sixty corruptions, none accepted | enforced |
| …and is accepted once the checksum is compiled out | `the_corruption_is_accepted_without_the_checksum` (`--features negative-demos`) — the half that makes the other half a test of the checksum | enforced |

## The host loop

| Property | Enforced by | Status |
|---|---|---|
| A hundred queued proposals cost one append and one fsync | `group_commit::a_hundred_queued_proposals_cost_one_append_and_one_sync` | enforced |
| …and the same hundred driven singly cost a hundred of each | `group_commit::the_same_hundred_driven_one_at_a_time_cost_a_hundred_of_each` — the half that makes the other half a measurement | enforced |
| Batching changes the cost and not the outcome | `group_commit::batching_changes_the_cost_and_not_the_outcome` | enforced |
| A lone proposal is not held waiting for company | `group_commit::a_single_proposal_is_not_held_waiting_for_company` | enforced |
| A write replicates and both nodes apply the same prefix | `group_commit::a_write_replicates_to_a_peer_before_it_commits` | enforced |
| Messages are never sent before the entries and hard state they depend on are durable | `audit::tests::sending_before_persisting_is_caught`, and `ReadyAudit` wired into the in-process cluster | enforced |
| A `Ready` is not acknowledged before its messages have gone out | `audit::tests::acknowledging_before_sending_is_caught` — the inversion the repository's own test harness was making until P22 | enforced |
| Committed entries are not applied before the same `Ready`'s messages are sent | `audit::tests::applying_before_sending_is_caught` | enforced |
| A `Ready` with nothing to persist may send without an fsync | `audit::tests::a_ready_with_nothing_to_persist_may_send_immediately` — or the audit would fail correct hosts on every heartbeat | enforced |
| Several `Ready`s outstanding at once is permitted, because that is group commit | `audit::tests::several_readys_may_be_outstanding_at_once` | enforced |
| One `Ready` may be acknowledged twice, by a host whose fsync and apply land separately | `audit::tests::one_ready_may_be_acknowledged_twice` | enforced |
| The applied watermark never goes backwards | `audit::tests::an_applied_watermark_that_goes_backwards_is_caught` | enforced |
| A `Ready`'s committed entries reach the state machine as one batch | `Node::apply` calls `StateMachine::apply_batch` once per `Ready`; the equivalence is `state_machine::a_batch_of_entries_means_what_applying_them_one_at_a_time_means` | enforced |
| The simulator cannot reach a socket or a storage engine | `simulation::the_simulator_cannot_reach_a_socket_or_a_storage_engine` (dependency assertion) | enforced |

## What a node says about itself

| Property | Enforced by | Status |
|---|---|---|
| A node reports over a real socket, and answers only `GET` on two paths | `admin_surface::a_node_reports_itself_over_a_real_socket`; `tests::only_get_is_answered` | enforced |
| `/metrics` parses as Prometheus text exposition | `admin_surface::a_node_reports_itself_over_a_real_socket` parses it the way a scraper does; `metrics::tests::the_output_parses_as_exposition` | enforced |
| A node that is not power-loss durable says so | `admin_surface::a_node_that_is_not_durable_says_so` — in `/status` and as a metric | enforced |
| The status is well-formed JSON whatever an operating system put in a failure message | `status::tests::a_failure_message_with_quotes_and_newlines_is_escaped` | enforced |
| The ready file is whole or absent, never half-written | `admin_surface::the_ready_file_is_written_whole_or_not_at_all` | enforced |

## The wire

| Property | Enforced by | Status |
|---|---|---|
| Every request, response and peer message round-trips | `tests::every_request_round_trips`, `tests::every_response_round_trips`, `tests::every_peer_message_round_trips` | enforced |
| A payload with bytes appended is refused, not accepted | `tests::a_payload_with_bytes_appended_is_refused` | enforced |
| A prefix of a message is never decoded as the message | `tests::a_truncated_payload_is_refused_rather_than_guessed` | enforced |
| Arbitrary bytes do not panic the decoder | `tests::arbitrary_bytes_do_not_panic_the_decoder` (generalised by a fuzz target at M3) | enforced |
| A length is checked against the limit before anything is reserved | `frame::tests::an_oversized_length_is_refused_before_anything_is_reserved` | enforced |
| A frame arriving one byte at a time is still one frame | `frame::tests::a_frame_split_across_arbitrary_reads_is_still_one_frame` (exhaustive over every chunk size) | enforced |
| Both transports behave identically | `keel_net::conformance::check`, run against `LoopbackPair` and against `TcpTransport` | enforced |
| A `Message` survives either transport unchanged | `transport_conformance::a_message_round_trips_identically_through_both_transports` | enforced |
| A request's label survives the encoding, and an envelope is not its body | `tests::an_envelope_round_trips_and_is_distinct_from_its_body` | enforced |
| Two answers under different labels are distinct even with identical bodies | `tests::envelopes_with_the_same_body_and_different_labels_are_distinct` | enforced |
| A frame split across reads is not lost by the poll that found half of it | `transport::tests::a_frame_split_across_reads_survives_the_poll_that_found_half_of_it` | enforced |
| Several answers in one read come out one at a time | `transport::tests::several_answers_in_one_read_come_out_one_at_a_time` | enforced |
| A single-request caller ignores answers that are not its own | `transport::tests::a_round_trip_skips_answers_that_are_not_its_own` | enforced |

## The state machine

| Property | Enforced by | Status |
|---|---|---|
| The applied index moves with the data it describes, never separately | `conformance::check`'s `the_applied_index_moves_with_the_batch`, run against both stores | enforced |
| An entry that changes nothing still moves the index | `conformance::check`'s `an_empty_batch_still_moves_the_index` | enforced |
| The applied index never goes backwards | `conformance::check`'s `the_applied_index_never_goes_backwards` | enforced |
| Replaying the log below the watermark applies nothing | `state_machine::replaying_the_log_below_the_watermark_applies_nothing` | enforced |
| A restart recovers the index, the data and the session table together | `state_machine::a_restart_recovers_the_applied_index_from_the_store` | enforced |
| **A retried command applies exactly once** | `state_machine::a_retry_storm_applies_each_command_exactly_once` — a hundred increments retried once each leave the counter at a hundred | enforced |
| …and the retry gets the response it missed, with no write | `state_machine::a_duplicate_returns_the_cached_response_and_writes_nothing` | enforced |
| A sequence below the floor is refused rather than guessed at | `state_machine::a_sequence_below_the_floor_is_refused_rather_than_guessed_at` | enforced |
| A retried registration returns the same identity | `state_machine::re_registering_with_the_same_nonce_returns_the_same_identity` | enforced |
| Client identities are a function of the log, not of a node | `state_machine::identities_are_a_function_of_the_log` | enforced |
| Sessions expire on the leader's stamp and on nothing else | `state_machine::a_session_expires_on_the_leaders_clock_and_only_on_it` | enforced |
| A command from an expired session is refused, not applied undeduplicated | `state_machine::a_command_from_an_expired_session_is_refused` | enforced |
| A client key can never collide with the state machine's own | `conformance::check`'s `the_namespaces_do_not_see_each_other` | enforced |
| A kill mid-apply never double-applies or regresses the index | `kill_during_apply::a_kill_mid_apply_never_double_applies_or_regresses`, 1,000 cycles in CI | enforced |
| …and the atomicity is what makes that true | `kill_during_apply::without_the_atomic_index_a_kill_leaves_an_entry_that_will_apply_twice` (`--features negative-demos`); `scripts/negative-demos/split-batch.sh` | enforced |
| A checkpoint carries the sessions as well as the data | `state_machine::a_checkpoint_carries_the_sessions_as_well_as_the_data` | enforced |
| A retry against a restored checkpoint still applies once | `state_machine::a_retry_against_a_restored_checkpoint_still_applies_once` | enforced |
| The state digest notices a session table two machines disagree about | `state_machine::the_state_digest_covers_the_session_table` | enforced |
| A checkpoint shares its bytes with the source rather than copying them | `checkpoint::a_checkpoint_shares_its_bytes_with_the_source` (same inode, link count ≥ 2) | enforced |
| A checkpoint survives the source losing the names it linked | `checkpoint::a_checkpoint_survives_the_source_losing_the_names_it_linked` | enforced |
| A transfer cut at **any** chunk resumes and completes | `snapshot_transfer::a_transfer_cut_at_any_chunk_resumes_and_completes` — every chunk boundary in turn, not one that worked | enforced |
| A corrupt chunk is rejected and the position does not advance past it | `snapshot_transfer::a_corrupt_chunk_is_rejected_and_the_position_does_not_advance` | enforced |
| An installed snapshot whose digest disagrees is thrown away, not published | `snapshot_transfer::a_snapshot_whose_digest_disagrees_is_thrown_away_rather_than_published` | enforced |
| A partial transfer cannot be published | `snapshot_transfer::an_incomplete_transfer_refuses_to_publish` | enforced |
| Publishing replaces an existing snapshot whole | `snapshot_transfer::publishing_over_an_existing_snapshot_replaces_it_whole` | enforced |
| A chunk cannot name a path outside the snapshot | `snapshot_transfer::a_chunk_that_names_an_escape_is_refused`; `transfer::tests::a_name_that_could_escape_the_directory_is_refused` | enforced |
| A fresh node is brought up past a compacted floor, killed mid-stream, and **resumes** | `snapshot_end_to_end::a_fresh_node_is_brought_up_by_a_snapshot_that_is_killed_mid_stream` — asserts the second attempt sent fewer chunks than the whole snapshot, and that the two attempts together cover it exactly once | enforced |
| A transfer interrupted repeatedly still converges, re-sending nothing verified | `snapshot_end_to_end::a_transfer_interrupted_repeatedly_still_converges` | enforced |
| A checkpoint is due by entries applied, not by time passed | `snapshots::tests::a_checkpoint_is_due_by_entries_applied_rather_than_by_time` | enforced |
| A host that cannot fetch snapshot bytes refuses to acknowledge an install | `NodeError::SnapshotUnsupported`, returned by `Node::pump` rather than echoing `snapshot_to_install` back. The core answers such an acknowledgement by moving the applied index over entries the state machine never saw | enforced |
| The daemon takes or streams a snapshot between real processes | — | **not claimed**: the transfer types and the simulator cover the path; `keel-node`'s loop never calls for a checkpoint, so its log is never compacted and no leader it runs offers one |
| **A batch of entries means what applying them one at a time means** | `state_machine::a_batch_of_entries_means_what_applying_them_one_at_a_time_means` — an increment reading what the increment before it wrote, a compare-and-swap against a value set earlier in the batch, a duplicate sequence number, and an expiry, all in one | enforced |
| A client registered in a batch can be used later in the same batch | `state_machine::a_client_registered_in_a_batch_can_be_used_later_in_the_same_batch` | enforced |
| A batch leaves the applied index at its highest entry, and a replay changes nothing | `state_machine::a_batch_leaves_the_applied_index_at_its_highest_entry` | enforced |
| Every idle session is collected however wide the table is | `state_machine::every_idle_session_is_collected_even_when_the_table_is_far_wider_than_one_sweep` — 200 sessions against a 16-wide rolling window | enforced |
| The expiry sweep is a function of the log, cursor and all | `state_machine::two_machines_fed_the_same_entries_expire_the_same_sessions` | enforced |
| Both stores apply the same log to the same state | `state_machine::both_stores_apply_the_same_log_to_the_same_state`; `keel_sm::conformance::check` run against `MemStore` and `LsmStore` | enforced |

## The durable log

| Property | Enforced by | Status |
|---|---|---|
| Every record kind survives a round trip | `record::tests::every_record_kind_round_trips` | enforced |
| A flipped byte anywhere in a frame is caught | `record::tests::a_flipped_byte_anywhere_in_the_frame_is_caught` (exhaustive over every byte position) | enforced |
| A torn trailing record is discarded and its prefix kept | `recovery::a_torn_trailing_record_is_discarded_and_the_prefix_survives` | enforced |
| A half-written frame header degrades correctly whichever half landed | `record::tests::a_half_written_header_degrades_either_way_round` | enforced |
| Unused space at the end of a full segment is not read as a tear | `record::tests::slack_at_the_end_of_a_full_segment_is_not_a_tear` | enforced |
| A torn record cannot be resurrected by a shorter one written over it | `recovery::a_torn_record_cannot_be_resurrected_by_a_shorter_one_written_over_it` | enforced |
| Damage below the end of the log is refused, not guessed past | `recovery::damage_below_the_end_of_the_log_is_refused_rather_than_guessed_past` | enforced |
| A segment whose header never landed is discarded | `recovery::a_segment_whose_header_never_landed_is_discarded` | enforced |
| An `Entries` record that does not continue the log is refused | `fold::tests::entries_that_do_not_continue_the_log_are_refused`; `recovery::entries_that_do_not_continue_the_log_are_refused_at_the_door` | enforced |
| A commit index a torn tail invalidated is clamped, and reported | `recovery::a_commit_index_the_tear_took_with_it_is_clamped_and_reported` | enforced |
| A truncation survives a restart, replacement and all | `recovery::a_truncation_survives_a_restart` | enforced |
| A compacted log recovers from its floor, not from index 1 | `fold::tests::a_compacted_log_starts_at_its_floor_rather_than_at_index_one`; `recovery::compaction_drops_only_segments_the_snapshot_covers` | enforced |
| **A conflicting installed snapshot discards the log on disk, not only in memory** | `fold::tests::a_snapshot_that_conflicts_with_the_log_discards_all_of_it` — a crash between an install and the next append otherwise recovers two histories spliced ([KEEL-15](BUGS.md)) | enforced |
| …and a snapshot the log agrees with still only compacts | `fold::tests::a_snapshot_the_log_agrees_with_only_compacts`; `fold::tests::a_snapshot_at_the_existing_floor_is_not_a_conflict` | enforced |
| An append never changes a segment's size | `recovery::preallocation_leaves_the_segment_at_its_full_size_from_the_start` | enforced |
| A sync covers what was written before it and nothing later | `recovery::a_sync_covers_exactly_what_was_written_before_it` | enforced |
| Two handles cannot open one log directory | `recovery::a_second_handle_on_a_live_directory_is_refused` | enforced |
| Both filesystem implementations behave identically | `keel_log::conformance::check`, run against `StdFs` and against the simulator's fault-injecting filesystem, tearing and not | enforced |
| Bytes written above the recovery cursor are erased however the scan stopped | `recovery::a_hole_a_crash_left_is_not_read_as_the_clean_end_it_looks_like` ([KEEL-7](BUGS.md)) | enforced |
| A record left above a hole is never read back as one | `recovery::a_record_above_a_hole_is_erased_rather_than_read_on_the_next_open`; `log_over_faultfs::a_log_whose_crash_left_a_hole_never_reads_the_leftover_as_a_record` | enforced |
| The checksum is load-bearing, not decorative | `scripts/negative-demos/torn-record.sh` (control passes, experiment fails) | enforced |

## The simulator

The properties above are checked after **every event** in every simulated run,
against a global oracle no individual node can see. Each check is a single digest
comparison, which is what makes per-event checking affordable.

| Property | Enforced by | Status |
|---|---|---|
| A seed replays byte-for-byte | `simulation::a_seed_replays_exactly`; `keel-sim determinism` in CI | enforced |
| …and across builds, not just within one | `simulation::the_committed_profiles_still_replay_to_their_pinned_fingerprints`, every profile pinned | enforced |
| Different seeds explore different schedules | `simulation::different_seeds_produce_different_runs` | enforced |
| The cluster actually makes progress | `simulation::the_cluster_makes_progress` | enforced |
| A leader never commits an earlier term's entry by counting | `simulation::no_leader_ever_commits_an_old_term_entry_by_counting` (must be exactly zero) | enforced |
| Every node drives the real log over a disk that can tear | `dependencies::the_simulated_disk_is_the_only_place_the_log_writes`; the `disk-*` profiles sweeping clean | enforced |
| Every node drives the real state machine | `simulation::the_state_machine_is_actually_exercised` — sessions opened, commands applied, and commands refused all non-zero | enforced |
| Two nodes at the same applied index hold the same state | `Oracle::observe_applied_state`, after every event. Not the same *entries* — the same result of applying them | enforced |
| …and both agree with a model that applied the same entries once, in order | `Oracle::check_against_model`; `simulation::the_model_oracle_actually_applies_the_log` refuses a vacuous comparison | enforced |
| Applying in index order is load-bearing | `scripts/negative-demos/apply-ordering.sh` (control clean, experiment dirty on the same seeds) | enforced |
| An entry is never handed to a state machine below its watermark | asserted inside `apply_entry`: an entry handed back below the watermark is skipped and its effect lost | enforced |
| A crash never leaves a log that will not open | `log_over_faultfs::a_log_that_crashed_can_always_be_reopened`; a failed reopen is a violation in the sweep, not a panic | enforced |
| A cut is always at a sector boundary from the start of the file | `fault_fs::the_cut_falls_at_a_multiple_of_the_sector_size_from_the_start_of_the_file` | enforced |
| A sector a write only partly covers keeps the bytes it did not touch | `fault_fs::a_sector_a_write_only_partly_covers_keeps_the_bytes_it_did_not_touch` | enforced |
| A pending allocation is all or nothing | `fault_fs::a_pending_allocation_is_all_or_nothing` | enforced |
| The disk is inside the replay fingerprint | `fault_fs::two_disks_with_the_same_seed_tear_identically`; `keel-sim determinism --profile disk-hunt` in CI | enforced |
| A quiet file does not shift the tear stream | `fault_fs::a_file_with_nothing_staged_does_not_shift_the_tear_stream` | enforced |
| Snapshots are taken, streamed, interrupted and resumed | `simulation::snapshots_are_actually_taken_streamed_and_resumed` — `checkpoints_taken`, `streams_started`, `streams_interrupted`, `streams_resumed` and `streams_completed` all non-zero | enforced |
| A compacted floor carries its digest rather than inventing one | `digest::rebase_tests::a_rebased_digest_agrees_with_one_that_was_never_compacted`; a floor with no digest is a violation in its own right | enforced |
| An install that only compacts discards nothing, and moves no digest above the floor | `digest::rebase_tests::adopting_a_snapshot_the_log_already_agrees_with_discards_nothing` | enforced |
| …and one that replaces history still reports what it replaced | `digest::rebase_tests::adopting_a_snapshot_that_replaces_history_reports_what_it_replaced` | enforced |
| **The `snapshot-hunt` profile sweeps clean** | `scripts/sweep.sh` at 500 seeds and 60,000 steps for three and five nodes, and CI's matrix; [KEEL-8](BUGS.md) is closed | enforced |
| The profile list cannot drift from the constructor | `simulation::the_profile_list_and_the_named_constructor_cannot_drift` | enforced |

### Coverage

A clean run over a fault schedule that never partitioned anything would prove
nothing, so the simulator reports which states it reached and a test fails if a
heavy-fault run does not reach them: partitions, crashes, dropped messages,
leadership changes, followers having a divergent tail overwritten, and leaders
holding an earlier term's entry at their commit index.

`simulation::heavy_faults_actually_reach_the_interesting_states` enforces this.
It exists because of [KEEL-4](BUGS.md): the original five-node schedule reached
the Figure 8 window exactly zero times, so a correct build and a deliberately
broken one were indistinguishable. CI sweeps both cluster sizes across every
profile for the same reason.

The disk is held to the same standard, and it has a sharper version of the same
hazard. A write tears only if it straddles a sector boundary, so a profile whose
segments are smaller than its sector cannot tear **at all** — every offset lies
in the same sector, one draw is made, and the only outcomes are lost and whole.
A badly sized profile is therefore not a weaker fault model but an absent one,
sweeping clean and proving nothing.

| Test | What a zero would mean |
|---|---|
| `simulation::heavy_disk_faults_actually_tear_the_log` | no crash caught a write in flight, or the sector model never cut one, or no crash left bytes above a gap, or the real parser never met a torn tail |
| `simulation::a_tear_meets_a_partition` | tears and partitions both happened and never met — the claim the durability bullet in the README turns on |
| `simulation::restarts_recover_across_more_than_one_segment` | recovery never saw more than one segment, so the multi-segment path went untested |
| `fault_fs::a_four_kilobyte_sector_over_a_one_kilobyte_segment_can_never_tear` | the arithmetic itself, pinned so the inert configuration cannot be reached by accident |

### Does the checker catch anything?

Five demonstrations, each control-then-experiment, each with committed output
under `results/negative-demos/`. Three remove a rule and require the harness to
find the violation: the Figure 8 current-term commit rule, the record checksum,
and applying committed entries in index order. A fourth removes the atomicity
between an applied index and the data it describes and requires a kill loop to
catch the double-apply. The fifth holds a bug fixed and varies the *fault model*
instead — `tearing-is-load-bearing.sh` runs the same checksum-removed build with
tears on and with writes lost whole, and requires it to be caught under the
first and invisible under the second. Three of the seven bugs so far were in the
harness, so the harness is checked the same way the code is.

`apply-ordering.sh` is the one worth reading twice. Its failure is invisible to
everything else in this file: the watermarks are maxima and do not notice, and
the log digests agree because the nodes really did apply the same entries. Only
a check on what applying them *produced* can tell — which is why the model
oracle exists, and why removing the ordering is how it is kept honest.

One rule has a test and no demonstration, and that is worth stating rather than
leaving as a gap in the pattern: removing the torn-tail erase produces no
violation in eighty seeds at sixty thousand steps, because resurrection needs
the replacement record to end exactly where the leftover begins and the
simulator reaches that alignment rarely. It keeps its deterministic regression
in `keel-log`'s own tests, where the alignment is constructed rather than hunted.

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

- **Misdirected writes.** The right bytes at the wrong address. The CRC covers a
  frame's contents, not where it sits, so nothing in the format could detect
  one. Adding a self-identifying `(seq, offset)` to the frame would — at eight
  bytes against a thirteen-byte minimal entry frame, and a format change after
  the tag. The decision taken is to keep the format at v1 and record the gap
  here instead, which makes this the one entry in this list with no milestone
  that closes it. Revisited when injected I/O errors land, since a `write_at`
  that fails without rewinding the cursor reaches the same class through the
  error path rather than through a crash.
- **Bit rot in bytes that are already durable.** The tear model decides what
  reached the device; nothing decays afterwards.
- **A crash *during* recovery.** The simulator runs an event to completion in
  virtual time, so `Log::open` is atomic and a torn `Log::erase` is unreachable
  there. The erase converges under tearing by construction — each open erases
  whatever non-zero bytes remain above the cursor — but that is an argument, not
  a test.
- **Injected I/O errors under a fault schedule.** `ENOSPC` on `allocate` and
  `EIO` on `sync` or `read` are not modelled. A node that cannot fsync must halt,
  and there is no server to halt yet (M1 Phase 5).
- **fsync loss.** A `Durable` sync that reports success and does not stick.
  Modelled by nothing, and the other half of TR-5.
- **The state machine's own store under the disk fault model.** The simulator
  drives the real state machine, and its store is `MemStore` — memory, which a
  simulated crash takes, so every restart replays the whole log into a fresh
  one. That is a sound pairing and a strong exercise of the apply path, but it
  is not the pairing a real node has: `LsmStore` is durable and can lose
  unsynced writes of its own. Putting the engine over `FaultFs` is now possible
  — the upstream filesystem seam and `Db::open_manual` exist for exactly this —
  and it is not done.
- **A lease read served by a leader whose own no-op has not committed.** The
  guard exists and sits upstream of the lease branch in `request_read_index`, so
  there is one check rather than two. The window itself is not reachable from the
  in-process cluster: acquiring a lease takes a round of heartbeat
  acknowledgements, and that harness delivers in order, so the no-op is
  acknowledged first. Closed by P9's recency oracle, which reorders.
- Exactly-once sessions across a *failover*. The session table survives a
  restart and deduplicates retries, both tested, and a client now keeps working
  across a leader being killed. What is untested is a *retry storm* crossing that
  change — a client whose answers are lost repeatedly while leadership moves,
  which is what the linearizability checkers are for (M1 Phase 11, M2 Phase 18).
- Membership changes from an operator. `Input::ProposeConfChange` and
  `Input::TransferLeader` exist in the core and are exercised only by an
  in-process cluster; the simulator issues neither, and the admin verbs that
  would drive them are deferred to M3 for that reason (ADR-024).
- Snapshots, streaming `InstallSnapshot`, and log compaction (M2).
- External linearizability checking via Maelstrom and Porcupine (M1/M2).
- Fuzzing of message decoding and arbitrary event sequences (M3).
- Clock-skew nemesis proving lease reads fail outside their drift bound (M3).
