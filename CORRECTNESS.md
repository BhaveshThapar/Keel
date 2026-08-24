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
| Under partition, crash and clock skew | — | planned (M2 Phase 18) |
| A real cluster's history, checked by Porcupine | — | planned (M2 Phase 18) |

The distinction that makes this worth having: every other check in this file is
one we wrote, against a property we chose. Knossos applies a definition of
linearizability nobody here chose to a history it recorded itself, and it does
not care what anyone here believes about the code.

Two things the run does not establish, stated because a passing external checker
invites more confidence than it has earned. There is **no nemesis** — no
partitions, no crashes, no clock skew — so it is a floor rather than a result: a
system that cannot pass without faults will not pass with them. And the adapter
**does not persist**, because Maelstrom does not restart nodes with their storage
intact; crash recovery is what the simulator's disk profiles and the kill loop
are for.

## A cluster of real processes

| Property | Enforced by | Status |
|---|---|---|
| Three real nodes serve put, get, delete, scan, cas and incr | `cluster::a_three_node_cluster_serves_traffic` — separate processes, real sockets, real files | enforced |
| A client finds the leader by itself and follows redirects | the same test: it is given all three addresses and never told which is the leader | enforced |
| Acknowledged writes survive the leader being killed | `cluster::writes_survive_a_leader_being_killed` | enforced |
| A history is recorded in the shape a checker wants | `cluster::a_client_records_a_history_a_checker_can_read`; `history::tests::a_lost_answer_is_unknown_rather_than_refused` | enforced |
| A misconfigured node refuses to start rather than serving alone | `cluster::a_misconfigured_node_refuses_to_start` | enforced |
| A node says whether its fsyncs survive power loss, in its ready file | `cluster::a_three_node_cluster_serves_traffic` checks every node's | enforced |

## The host loop

| Property | Enforced by | Status |
|---|---|---|
| A hundred queued proposals cost one append and one fsync | `group_commit::a_hundred_queued_proposals_cost_one_append_and_one_sync` | enforced |
| …and the same hundred driven singly cost a hundred of each | `group_commit::the_same_hundred_driven_one_at_a_time_cost_a_hundred_of_each` — the half that makes the other half a measurement | enforced |
| Batching changes the cost and not the outcome | `group_commit::batching_changes_the_cost_and_not_the_outcome` | enforced |
| A lone proposal is not held waiting for company | `group_commit::a_single_proposal_is_not_held_waiting_for_company` | enforced |
| A write replicates and both nodes apply the same prefix | `group_commit::a_write_replicates_to_a_peer_before_it_commits` | enforced |
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
