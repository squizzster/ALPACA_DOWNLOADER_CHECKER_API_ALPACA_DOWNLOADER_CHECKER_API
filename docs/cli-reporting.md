# CLI reports

[CLI `main`](../libs/alpaca_cli/__init__.py) owns result/exit behavior. Diagnostics go to
stderr; final reports use JSON stdout, except commands explicitly producing CSV or selected reader formats.
Every command in `alpaca-cli-eod` and `alpaca-lineage-lab` uses the shared lifecycle below.

## Completion and status

`update` reports completion, failures, pending work, effective cutoff, stage results and request counts.
Estimate fields are conditional, not an ETA or a guarantee of total requests; see
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

`context.estimated_remaining_seconds` is `"TBD"` when there is no producer estimate. CSV export supplies an
approximate estimate from completed identities; numeric estimates are whole seconds. Other commands do not
recalculate estimates or infer completion from elapsed time. Initial 10-second heartbeat always displays
`"TBD"`, regardless of phase or an available estimate. This overrides display only; later heartbeats retain the estimate.

Canonical phase definitions and adapters live in
[command_progress.py](../libs/alpaca_cli/command_progress.py): `UPDATE`, `RECOVERY`, `CATALOGUE`, `PLAN`,
`PRICING`, `LINEAGE`, `MIGRATE`, `SUMMARY`, `UNIVERSE`, `STATUS`, `READ`, `RESOLVE`, `EXPORT`, `COMMIT`, `PUSH`.
Existing callbacks replace held status with their stage, supported scalar counters and available counts.
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
| `PLAN` | Catalogue generation, identity/group/source counts, workers/maximum active groups, metadata/payload/output bounds. |
| `EXPORT` | Plan fields, workers, completed counts, CSV bytes, column counts, estimated seconds remaining. |
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
