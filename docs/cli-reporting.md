# CLI reports

[CLI `main`](../libs/alpaca_cli/__init__.py) owns result/exit behavior. Diagnostics go to
stderr; final reports use JSON stdout, except commands explicitly producing CSV or selected reader formats.
Every command in `alpaca-cli-eod` and `alpaca-lineage-lab` uses the shared lifecycle below.

## Completion and status

`update` reports completion, failures, pending work, effective cutoff, stage results and request counts.
The `maintenance` stage handles existing eligible summary members before discovery; `maintenance_after_discovery`
handles newly eligible or invalidated members and late-bar work. Reused checks issue no additional comparison
requests. The final `complete` value includes both stages and serving completion.
Work-plan fields are conditional, not a guarantee of total requests; see
[preview scope](operations.md#daily-update). Published unresolved identity may be completed processing.

`status` is offline/read-only; missing/incomplete boundaries report blockers and reconstruction commands:

- `ready_for_update`: required boundaries exist.
- `processing_complete`: also no outstanding local work/publications.

Either false exits nonzero. Neither asserts a fresh provider check or future coverage. Counts separate
empty/nonempty history, unresolved rows and complete assignment; exclusive coverage ends differ from last bars.
Each inspected boundary is coherent separately, not one cross-producer transaction.
[Status authority](../libs/alpaca_lineage_build/status.py): `inspect_status`.

Canonical exit codes: 0 success; 1 failure/incomplete work; 2 invalid arguments; 3 busy store/reader/export lock.
Standalone reads may wait with `--wait`; CSV exports always fail immediately when busy. Executable keyboard
interruption exits 130. [Policy-lab exit codes](policy-lab.md#all-lineage-audit) differ.

## Optional update debug trace

Daily `update` (including `--plan`), `pricing acquire` and focused `lineage update` / `lineage refresh` accept
`--debug FILE.jsonl`. The destination must be a new file in an existing directory; opening it fails before
update work if it is unavailable or already exists. For example:

```bash
./alpaca-cli-eod update --debug /tmp/alpaca-update.jsonl
./alpaca-cli-eod lineage update --symbol AAPL --no-pricing-acquisition --debug /tmp/alpaca-AAPL.jsonl
```

This separate compact JSONL stream records events as they occur, including phases too brief for a heartbeat.
It uses only arguments, results and decisions already held by the producer. It does not inspect storage to
improve diagnostics or repeat a completed validation. Implementation: [debug_trace.py](../libs/common/debug_trace.py).

Each record carries schema `alpaca.update.debug`, schema version 1, run ID, sequence number, monotonic elapsed
seconds, thread, event and stage. The run starts with UTC time and ends with status/exit code. Paired `enter` /
`exit` records carry span and parent IDs; exits include duration and bounded result counters. Durations include
child work: do not sum nested spans as independent elapsed time. Lock spans emit an acquired event to separate
waiting from held work. `progress` records retain bounded existing observations; `decision` records identify
prefix eligibility and fallback reasons. Lists carry counts and up to eight examples; nested values and strings
are bounded and lines are at most 8 KiB. Records are serialized across workers and line-buffered without `fsync`.

Coverage includes command/store-lock entry, universe-lock acquisition, durable work-state loading, owner recovery,
accepted selectors, catalogue observation, selection and estimates, serving control-map loading,
registration/work acceptance, initial pricing, live history checks, corrections, appends and
worker publication, dependency catch-up/prefetch, complete lineage groups, receipt/boundary/payload reads,
correlation/publication/acknowledgement, serving components/evaluation/publication, result output and automatic
registry commit/push. CLI HTTP spans include limiter waits, request time and response handling, not exclusively network latency.
Full GET targets and parameters are recorded in ordered `http_target` chunks (join `url_part` by request ID and
part number), including long symbol rosters and pagination tokens. Request headers, credentials and response
bodies are not serialized. HTTP exits retain observed request-count changes and available failure status.
Correction conflict events retain selected/acquired row values and the acquired session row count.
`late_bar_action_positive` identifies each completed positive action and whether it was persisted;
`late_bar_action_failed` includes the selected symbols, mapping, bounds and classified cause.
`updated_boundaries` reports changed partition references or explicit repairs, not candidates.
Effective price changes, generation changes and files written are distinct; an equal correction can
write a completion receipt while preserving every pricing file.
Prefix decisions explain incompatible or
unresolved histories, changed ancestry/evidence, incomplete session seams and changed suffix identity/source priority. Group
exits report rows processed versus reused; serving exits report reused prefixes and full-history evaluations.

`pricing_storage_read` brackets each actual read through the pricing owner and the shared reader's gzip
loader. It identifies the path, symbol, artifact kind, purpose, available selector digest and byte count.
Purposes distinguish selected payload/validator consumption from staged or installed producer validation
and explicit authentication. Cache hits emit no physical-read span; exact-copy views share their existing
physical reads. Staged reads identify their output path. Failed reads close with the error type.
`pricing_source_selected` links an already computed source generation to its validator and boundary digests.
`endpoint_selection_binding`, `pricing_generation_check` and `append_approval_binding` record the held
selected/current generations and applicable request scope, including rejected stale bindings.
`history_comparison` records each completed symbol's matching/differing result, source generation, summary
token, endpoint sessions and mapping; `history_endpoint_difference` names the endpoint and changed fields.
No response bodies, extra file reads, digests or provider requests are produced for these diagnostics.
These are owning-path observations, not a filesystem sandbox or proof that every recorded operation is necessary.

Normal returns (including unchanged, blocked and incomplete results), exceptions and keyboard interruption
close their started spans. A killed process leaves open spans identifying its last active work. A trace write
failure disables tracing and emits a stderr warning while preserving the update outcome; missing records then
must not be interpreted as successful completion. Argument-parser failures occur before tracing starts.
Keep trace files outside Git. The synthetic trace tests exercise complete updates, append workers, failures,
early exits and interruption; they do not establish production throughput.

## Shared lifecycle

Both executables emit bounded, versioned compact JSONL on stderr for every successfully parsed command,
including plans and short reads. Stdout retains each command's existing JSON/CSV result format. Help and
argument errors retain argparse behavior and do not start a lifecycle. Reporting activates before contract
checks, store locks or command work, and immediately flushes `INIT` with a command-specific startup message.
Authority: [LifecycleReporter](../libs/alpaca_heartbeat/__init__.py). A short command can emit just `INIT` and
`FINAL_EXIT`; the timer does not delay completion.

One executable owns one reporter, including nested pricing/lineage/catalogue work. Library calls stay silent
unless a caller explicitly attaches a reporter or progress callback. The reporter reads held memory only:
no provider requests, store discovery, payload reads, locks, validation or work-selection decisions.

Fixed heartbeat intervals: 10, 60, 120, 180, 240, then repeating 300 seconds—elapsed times
10, 70, 190, 370, 610, 910, 1210… . This cadence applies to every command and library reporter;
there is no CLI or reporter interval override. Tests advance a controlled clock instead of changing the cadence.
Whole `elapsed_seconds` use the monotonic clock from activation.

One held status is sampled by a separate timer, including while work waits. Updates replace status/context
without printing, queuing or resetting the timer. Heartbeats retain it; unchanged work repeats with new elapsed
time. Context replacement prevents previous-scope leakage. Invalid/oversized updates preserve the last valid
status; brief phases may fall between samples and never appear.

Progress schema `alpaca.lifecycle.progress`, version 4: `schema`, `schema_version`, `elapsed_seconds`,
`next_heart_beat_in_seconds`, `phase`, `message`. The next-heartbeat value is remaining scheduled seconds rounded
up, or `null` in terminal error/`FINAL_EXIT` records. Optional `current`/`total` occur together with
`0 <= current <= total`; their meaning follows the reported stage/unit. CSV counts selected public identities.
`context` permits only registered scalar phase fields; large symbol lists and nested producer reports stay in
the final result rather than entering heartbeat records.
An optional `scope` identifies the command or current work scope.

Only initial `INIT` includes `ai_agent_read_this`: monitor heartbeats; use other monitoring only when a heartbeat
is over 10 seconds late; derive reports from heartbeat messages; update the user at each heartbeat unless directed
otherwise. Later records omit it even if the held phase remains `INIT`.

`context.estimated_remaining_seconds` is `"TBD"` when there is no estimate. CSV export supplies an
approximate estimate from completed identities; numeric estimates are whole seconds. Initial 10-second heartbeat always displays
`"TBD"`, regardless of phase or an available estimate. This overrides display only; later heartbeats retain the estimate.

Daily execution also estimates the remaining **known pricing requests**, identified by
`eta_scope: "known_pricing_requests"` and `eta_status: "conditional"`. After eight completed page calls,
the midpoint and `estimated_remaining_lower_seconds` / `estimated_remaining_upper_seconds` combine the
accepted work-plan assumptions with recent and invocation-wide wall-time throughput, bounded by the shared
request-rate limit. Worker durations are not added together. Completed calls include their own retries and
rate waits; a failed call withdraws the numeric estimate. Request counters update held progress without
changing the phase or heartbeat cadence, and work without debug receives the same reporting.

The exact late-bar request plan replaces its provisional selection estimate. Newly selected request plans
extend an exhausted estimate. These are conditional estimates, not duration limits: later metadata,
corrections, dependencies and publication work may extend the command. They do not estimate unknown provider
work or unmeasured local work. Stalls hold the last observation until another callback arrives; subsequent
measurements incorporate the delay. Completed/selected counters additionally supply
`estimated_phase_remaining_seconds` and `eta_phase` after observed progress in that phase; a different phase
or reset counter starts a new measurement. A completed component does not set the command estimate to zero.
Exhausted request estimates return `TBD` until more scope is known; only successful data-update completion
reports zero with `eta_scope: "data_update"`. Subsequent registry Git work has its own lifecycle.
The estimator reads no store, debug file or provider endpoint and persists no timing cache.

Canonical phase definitions and adapters live in
[command_progress.py](../libs/alpaca_cli/command_progress.py): `UPDATE`, `RECOVERY`, `CATALOGUE`, `PLAN`,
`PRICING`, `LINEAGE`, `MIGRATE`, `SUMMARY`, `UNIVERSE`, `STATUS`, `READ`, `RESOLVE`, `EXPORT`, `COMMIT`, `PUSH`.
Existing callbacks replace held status with their stage, supported scalar counters and available counts.
The adapter carries producer `completed`/`selected` counts into `current`/`total` when coherent, counts supplied
symbol/processed/pending-work collections, and preserves request totals from producer reports. Component
completion describes that component, not command completion. Selection emits an immediate diagnostic containing
the accepted `expected_io` before selected mutation; its numeric estimates remain in later heartbeat context
under `expected_` names. These are retained observations, with no reporting-driven storage reads.
Commands without callbacks repeat their operation and elapsed time. The policy lab reports `POLICY` followed by
`COMPARE` or `AUDIT`, without additional group inspection or invented progress counts.
The final update `COMMIT` and `PUSH` phases hold `path`, `status` and the selected `commit` when available;
immediate diagnostics also preserve fast outcomes between timer samples. A `status: failed` warning describes
Git only; the terminal record still reflects the completed data update. See
[automatic registry commits and pushes](operations.md#daily-update).

Success stops reporting and flushes one `FINAL_EXIT` (`status: success`, code 0). Caught failure first flushes
`alpaca.lifecycle.error`, version 4: error type/message, latest accepted phase, elapsed seconds, available
causes/traceback; then `FINAL_EXIT` (`status: error`). Existing command exit codes determine terminal
success/error; incomplete work and policy differences remain nonzero. Keyboard interruption emits
`status: aborted`, code 130. Finalization never waits for the next heartbeat; none follows it.

Writes are serialized/flushed. Progress/final records are at most 4 KiB; errors 16 KiB, including newline.
Oversized errors shorten diagnostics and set `truncated: true`. Reporting failures do not change command outcomes;
abrupt termination or failed stderr writes can prevent delivery.

Existing nonfatal diagnostics, such as pricing cutoffs, Parquet rejections and cache/RSS summaries, use
`alpaca.lifecycle.diagnostic`, version 4. These immediate records have `schema`, `schema_version`,
`elapsed_seconds`, `phase`, `level` (`info`, `warning`, `error`) and `message`; they do not replace held status,
reset the timer or imply command failure. They are at most 16 KiB, including newline; oversized messages are
shortened with `truncated: true`. A caught inner-adapter error can appear as a diagnostic before the outer
error/final records. There are no diagnostic records after finalization.

Heartbeat delivery confirms the reporting thread is alive, not that the operation has advanced. Unchanged
counts can indicate a wait or stalled work; approximate estimates are not a completion guarantee.

## CSV lifecycle

CSV keeps its established startup message, `CATALOGUE` → `PLAN` → `EXPORT` → `PUBLISH` phases and phase-specific
context. Planning uses the same lifecycle. Authority:
[csv_progress.py](../libs/alpaca_cli/csv_progress.py), `PHASES`.

| Phase | Context scope |
|---|---|
| `INIT` | Reporting activation; no context. |
| `CATALOGUE` | Open online catalogue; no context. |
| `PLAN` | Catalogue generation, identity/group/source counts, workers/maximum active groups, output memory budget, metadata/payload/output bounds. |
| `EXPORT` | Plan fields, read/write I/O phase, prepared identities, buffered bytes, batch count, completed counts and final-write estimate. |
| `PUBLISH` | Final manifest inside run directory; completed identity/group/row counts, CSV bytes, column counts. |
| `FINAL_EXIT` | Immediate terminal `status`, `exit_code`, `last_phase`. |

## Library use and consumption

The shared internal library is `libs.alpaca_heartbeat`; importing it starts no timer and prints nothing.
Callers register `Phase` definitions, create `LifecycleReporter`, call `start()` once, replace held status
with `update()`, and call `finish()` on every exit. Use `diagnostic()` for immediate diagnostic events.
Command adapters own messages/fields, result serialization and exit selection. Heartbeat runtime releases
are separate from the version-4 wire schema and data/publication versions; reporting-only integration does
not invalidate or relabel stored artifacts. [Version authorities](../VERSION_TRACKER.md).

Repository consumers read stderr line by line and call `parse_record(line)` to decode and check supported schema/version,
record limits, envelope fields and progress counts/deadlines. It performs no I/O. Dispatch by `schema`, then
`phase`; retain the last progress observation and use `FINAL_EXIT.status`/`exit_code` for the terminal report.
Treat diagnostic `level` as severity, not the process exit status. Optional context is producer-specific.
External monitors can consume the JSONL using their own JSON parser; `libs/` remains internal to this repository.
For monitoring, use each progress record's remaining `next_heart_beat_in_seconds` from its receipt time; allow
10 seconds of delivery slack before investigating a late heartbeat. Terminal records carry no next deadline.

Failure aggregation counts failed workflow symbol obligations once, regardless of how many maintenance or
lineage reports describe them. Unscoped command failures and independent serving failures have separate owners.
Stage-specific counts remain diagnostic and are not added again to the aggregate. DEBUG `metadata_exact_lookup`
events identify actual uncached lookup attempts, lookup kind and selection reason; cache hits emit no lookup
event. `pricing_publication` distinguishes effective price change from previous/new source generation and writes.
Correction completion consumption and transactional acceptance have separate events. These events use existing
producer values and perform no additional storage checks.
