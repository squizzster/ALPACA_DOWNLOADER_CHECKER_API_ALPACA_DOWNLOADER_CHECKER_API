# CLI reports

[CLI `main`](../libs/alpaca_cli/__init__.py) owns results and exit codes. Final results go to stdout as JSON
unless the command selects CSV or another reader format. Every parsed `alpaca-cli-eod` and `alpaca-lineage-lab`
command reports its lifecycle on stderr.

## Completion and status

`update` reports effective cutoff, stages, request counts, failures, pending work and completion. Before
discovery, `maintenance` handles existing eligible summary members. Afterwards, `maintenance_after_discovery`
handles newly eligible or invalidated members and late bars. Reused checks make no comparison requests. Final
`complete` includes both stages and serving. Pricing/lineage invalidations affecting published components appear
in serving's `pending_export_inputs`. Applicable unfinished invalidations prevent completion even with an empty
queue. An unresolved published identity can be complete.
[Plan estimates are conditional](operations.md#daily-update).

A finished daily attempt reports `outcome: complete` or `outcome: completed_with_warnings` and exits 0.
Outstanding required work yields `complete: false`, `warnings`, a warning diagnostic and pending/failure
details. Registry Git bookkeeping requires full data completion. Malformed input, interrupted publication and
inability to finish the attempt retain their error paths.

Each unfinished item at completion records `completion` in the report and durable outcome. The acknowledgement
selection supplies required/published date bounds, obligation/correction IDs, published/approved pricing
generations and group labels awaiting a lineage check. Null means unobserved.

Offline, read-only `status` lists blockers and reconstruction commands for missing/incomplete boundaries.
`ready_for_update` requires all boundaries. `processing_complete` additionally requires no outstanding local
work or publications. Either false exits nonzero. Each boundary is independently coherent. Counts distinguish
empty/nonempty histories, unresolved rows and complete assignments. Exclusive coverage ends differ from last
bars. Status establishes neither current provider freshness nor future coverage.
[`inspect_status`](../libs/alpaca_lineage_build/status.py)

| Canonical exit | Meaning |
|---|---|
| 0 | Success, including finished daily attempts with warnings |
| 1 | Execution failure or other incomplete work |
| 2 | Invalid arguments |
| 3 | Busy store, reader or export lock |
| 130 | Executable keyboard interruption |

Store writers emit an immediate diagnostic and hold `WAIT` until acquiring the gate. Ctrl-C cancels that wait
before store/provider work. Standalone reads support `--wait`. Busy CSV exports fail immediately.
[Policy-lab exit codes](policy-lab.md#all-lineage-audit) differ.

## Optional update debug trace

Daily `update` (including `--plan`), `pricing acquire`, focused `lineage update` and `lineage refresh` accept
`--debug FILE.jsonl`. Choose a new file in an existing directory. An existing or unavailable destination fails
before update work.

```bash
./alpaca-cli-eod update --debug /tmp/alpaca-update.jsonl
./alpaca-cli-eod lineage update --symbol AAPL --no-pricing-acquisition --debug /tmp/alpaca-AAPL.jsonl
```

Ordinary DEBUG updates capture compact native stage summaries and scalar producer effects outside the store at
`FILE.jsonl.handoffs/<operation-id>/`. Held prices, receipts, assignments and caches stay in memory. Explicit
standalone JSON stages capture exact arguments/results. Plans and focused commands retain compact tracing only.
[Pipeline contract](development.md#pipeline-phase-3-complete-pipeline)

The separate [JSONL trace](../libs/common/debug_trace.py) records events as they happen, including phases
between heartbeats. It consumes held arguments, results and decisions without additional reads, validation,
digests or provider calls.

- Every record carries `alpaca.update.debug` v1, run ID, sequence, monotonic elapsed seconds, thread, event and
  stage. Start records UTC time, end records status/exit code.
- Paired `enter`/`exit` records carry span and parent IDs. Exits include duration and bounded result counters.
  Durations include children. Lock-acquired events separate wait from hold time.
- `progress` retains existing observations. `decision` gives prefix eligibility/fallback reasons. Lists carry
  counts and up to eight examples. Nested values and strings are bounded. Lines ≤8 KiB, serialized across
  workers, line-buffered without `fsync`.

Coverage runs from command/store-lock entry through universe-lock acquisition, durable work loading, owner
recovery, accepted selectors, catalogue observation, selection/estimates, serving control-map loading,
registration/work acceptance, initial pricing, live checks, corrections, appends and worker publication. It
includes dependency catch-up/prefetch, complete lineage groups, receipt/boundary/payload reads,
correlation/publication/acknowledgement, serving components/evaluation/publication, output and automatic
registry commit/push.

| Observation | Interpretation |
|---|---|
| HTTP spans | Include limiter wait, request and response handling. Join ordered `http_target` chunks by request ID and part number using `url_part` for full GET targets/parameters, including long rosters and pagination tokens. Omit headers, credentials and response bodies. Exits carry observed request-count changes and available failure status, including wrapped causes. |
| Correction conflicts | Selected/acquired row values and acquired session row count. Correction completion consumption and transactional acceptance emit separate events. |
| `late_bar_action_positive`, `late_bar_action_failed` | Each completed positive action and whether persisted. Failure gives selected symbols, mapping, bounds and classified cause. |
| `updated_boundaries`, `pricing_publication` | Actual changed partition references or explicit repairs. Distinguish effective price change, previous/new source generation and files written. An equal correction may write a completion receipt and preserve every pricing file. |
| Prefix decisions and exits | Explain incompatible/unresolved histories, changed ancestry/evidence, incomplete session seams and changed suffix identity/source priority. Group exits count processed/reused rows. Serving exits count reused prefixes/full-history evaluations. |
| `pricing_storage_read` | Each actual pricing-owner/shared gzip loader read, with path, symbol, artifact kind, purpose, available selector digest and byte count. Distinguishes selected payload/validator consumption, staged/installed validation and explicit authentication. Cache hits emit none. Exact-copy views share physical reads. Staged reads name the output path. Failed spans close with the error type. |
| `pricing_source_selected` | Links the computed generation to validator and boundary digests. |
| `endpoint_selection_binding`, `pricing_generation_check`, `append_approval_binding` | Held selected/current generations and applicable request scope, including rejected stale bindings. |
| `history_comparison`, `history_endpoint_difference` | Each completed symbol's match/difference, source generation, summary token, endpoint sessions and mapping. Differences identify endpoint and changed fields. |
| `metadata_exact_lookup` | Actual uncached lookup attempt, kind and selection reason. Cache hits emit none. |

These owner observations neither enforce a filesystem sandbox nor prove every operation necessary. Normal
returns (including unchanged, blocked or incomplete), exceptions and keyboard interruption close started spans.
Killing the process leaves its last active spans open. Trace write failure disables tracing and warns on stderr
without changing the update outcome. Missing records then cannot establish completion. Parser failures precede
tracing. Keep traces outside Git. Synthetic tests cover complete updates, append workers, failures, early exits
and interruption. Production throughput requires separate evidence.

## Shared lifecycle

Every successfully parsed command, including plans and short reads, flushes command-specific `INIT` before
contract checks, locks or work. Help/argument errors retain argparse behavior. One
[LifecycleReporter](../libs/alpaca_heartbeat/__init__.py) per executable covers nested pricing, lineage and
catalogue work. Library calls stay silent unless a reporter/progress callback is attached. Reporting observes
held memory only, with no provider calls, discovery, payload reads, locks, validation or work selection. A short
command can emit only `INIT` and `FINAL_EXIT`.

Every command/library reporter uses fixed intervals of 10, 60, 120, 180, 240, then repeating 300 seconds
(elapsed 10, 70, 190, 370, 610, 910, 1210…). There is no CLI/reporter override. Tests advance a controlled
clock. A separate timer samples one held status even during waits. Updates replace status/context without
printing, queuing, leaking prior scope or resetting the timer. Invalid/oversized updates retain the last valid
status. Unchanged work repeats with updated elapsed time. Brief phases may never appear.

Progress uses compact stderr JSONL, schema `alpaca.lifecycle.progress` v4:

| Field | Meaning |
|---|---|
| `schema`, `schema_version`, `phase`, `message` | Protocol and reported phase |
| `elapsed_seconds` | Whole monotonic seconds since activation |
| `next_heart_beat_in_seconds` | Remaining scheduled seconds, rounded up. Terminal error/`FINAL_EXIT` carry `null`. |
| Optional `current`/`total` | Paired counts, `0 <= current <= total`, in stage units. CSV counts selected public identities. |
| Optional `scope` | Command or current work scope |
| `context` | Registered scalar phase fields only. Large symbol lists/nested producer reports stay in final results. |

Only initial `INIT` includes `ai_agent_read_this`, even if later samples still hold `INIT`. Monitor heartbeats,
derive reports from their messages, investigate elsewhere only when >10 seconds late, and update the user each
heartbeat unless directed otherwise. Delivery establishes reporter liveness only. Unchanged counts can indicate
waits or stalls. Estimates do not guarantee completion.

`context.estimated_remaining_seconds` is whole seconds or `"TBD"`. CSV estimates from completed identities. The
initial 10-second sample always displays `"TBD"` in every phase, preserving any underlying estimate for later
samples.

Daily pricing reports `eta_scope: "known_pricing_requests"` and `eta_status: "conditional"`. After eight
completed page calls, midpoint and `estimated_remaining_lower_seconds`/`estimated_remaining_upper_seconds`
combine accepted plan assumptions with recent and invocation-wide wall-time throughput, bounded by the shared
rate limit. Calls include retries and rate waits. Never sum worker durations. Failures withdraw the numeric
estimate with `eta_status: "unknown"`, `eta_reason: "request_failed"`. Counters update held progress without
changing phase/cadence. Reporting is identical with or without debug.

Escaped pricing exceptions emit immediate warning `pricing_request_failure` with bounded type/message, available
HTTP status and allowlisted scope, excluding headers/bodies. The pricing boundary bounds and credential-redacts
provider messages/transport reasons before scalar handoff. Heartbeats retain `eta_request_failures`,
`eta_unresolved_request_failures`, `eta_last_error_type` and available `eta_last_http_status`.

- Successful retries within a call count as one success. A completed zero-failure producer scope retires
  exceptions inside that scope, including supported batch fallback. Unrelated successes cannot retire unfinished
  failures.
- A completed group's unavailable auxiliary retires its matching initial raw SIP rejection from the unresolved
  count. The cumulative count and original diagnostic retain it. Successful overall completion retires remaining
  observations. Reporting never retries or reopens work.

The exact late-bar plan supersedes its provisional estimate. Newly selected plans extend exhausted estimates.
Unknown provider work and unmeasured local work have no estimate. Later metadata, corrections, dependencies and
publication can extend runtime. Stalls retain the last observation until the next callback incorporates the
delay. After observed progress, completed/selected counters supply `estimated_phase_remaining_seconds` and
`eta_phase`. New phases/reset counters start new measurements. Component completion cannot zero the command
estimate. Exhausted scope returns `TBD`. Only successful data-update completion yields zero with
`eta_scope: "data_update"`. Registry Git then has its own lifecycle. The estimator reads no store/debug
file/provider endpoint and persists no timing cache.

[Canonical phases and adapters](../libs/alpaca_cli/command_progress.py) cover `WAIT`, `UPDATE`, `RECOVERY`,
`CATALOGUE`, `PLAN`, `PRICING`, `LINEAGE`, `MIGRATE`, `SUMMARY`, `UNIVERSE`, `STATUS`, `READ`, `RESOLVE`,
`EXPORT`, `COMMIT`, `PUSH`. Callbacks replace held state with stage, supported scalars and available counts.
Coherent producer `completed`/`selected` become `current`/`total`. Adapters count supplied
symbol/processed/pending-work collections and retain reported request totals. Before selected mutation, an
immediate diagnostic reports accepted `expected_io`. Later heartbeat context retains numeric estimates as
`expected_` fields. No callback means repeated operation and elapsed time. The lab reports `POLICY` then
`COMPARE` or `AUDIT`, without extra group inspection or invented counts.

Final update `COMMIT`/`PUSH` phases carry `path`, `status` and available selected `commit`. Immediate
diagnostics preserve outcomes between samples. A `status: failed` Git warning leaves the completed data update's
terminal status intact. [Automatic registry Git operations](operations.md#daily-update)

| Record | Emission and limits, including newline |
|---|---|
| Progress/final | ≤4 KiB. Success stops reporting and flushes one `FINAL_EXIT` (`status: success`, code 0). Commands keep their exit codes, including nonzero incomplete work/policy differences. |
| `alpaca.lifecycle.error` v4 | On caught failure flush type/message, latest accepted phase, elapsed seconds, available causes/traceback, then `FINAL_EXIT` (`status: error`). Keyboard interruption emits `status: aborted`, code 130. Errors ≤16 KiB. Oversized diagnostics are shortened with `truncated: true`. |
| `alpaca.lifecycle.diagnostic` v4 | Immediate nonfatal records, e.g. cutoffs, Parquet rejections, cache/RSS summaries. Fields: `schema`, `schema_version`, `elapsed_seconds`, `phase`, `level` (`info`, `warning`, `error`), `message`. ≤16 KiB. Oversized messages shortened with `truncated: true`. An inner-adapter error may precede outer error/final records. Diagnostics leave held status, timer and command result intact. |

Writes serialize and flush. Finalization never waits for a heartbeat and emits no later records. Reporting
failures preserve command outcomes. Abrupt termination or failed stderr writes can prevent delivery.

## CSV lifecycle

CSV and its plan retain the established startup message and ordered phases.
[`csv_progress.PHASES`](../libs/alpaca_cli/csv_progress.py)

| Phase | Context |
|---|---|
| `INIT` | Activate, no context |
| `CATALOGUE` | Open online catalogue, no context |
| `PLAN` | Catalogue generation, identity/group/source counts, workers/maximum active groups, output memory budget, metadata/payload/output bounds |
| `EXPORT` | Plan fields, read/write I/O phase, prepared identities, buffered bytes, batches/completed counts, final-write estimate |
| `PUBLISH` | Final manifest inside run directory, completed identity/group/row counts, CSV bytes and column counts |
| `FINAL_EXIT` | Immediate terminal `status`, `exit_code`, `last_phase` |

## Library use and consumption

Importing internal `libs.alpaca_heartbeat` starts no timer or output. Register `Phase` definitions, create
`LifecycleReporter`, call `start()` once, replace held status with `update()`, emit immediate events with
`diagnostic()` and call `finish()` on every exit. Adapters own messages/fields, serialization and exit
selection. Heartbeat runtime, v4 wire schema and data/publication versions are independent. Reporting
integration does not invalidate/relabel stored artifacts. [Version authorities](../VERSION_TRACKER.md)

Repository consumers call I/O-free `parse_record(line)` on each stderr line to check supported schema/version,
size, envelope and progress counts/deadlines. Dispatch by `schema`, then `phase`. Retain the last progress
observation. Use `FINAL_EXIT.status`/`exit_code` for the terminal report and diagnostic `level` only for
severity. Optional context is producer-specific. External monitors use their own JSON parser (`libs/` is
internal). Next heartbeat due = receipt time + `next_heart_beat_in_seconds`. Allow 10 seconds' delivery slack
before investigation. Terminal records have no deadline.

Count failed workflow symbol obligations once across maintenance and lineage reports. Unscoped command and
independent serving failures have separate owners. Stage counts remain diagnostic and add nothing to the
aggregate.
