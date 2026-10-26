# CLI reports

[CLI `main` / `progress_reporter`](../libs/alpaca_cli/__init__.py) own result/exit behavior. Diagnostics go to
stderr; final reports use JSON stdout, except commands explicitly producing CSV or selected reader formats.
CSV export has the lifecycle below; other commands retain their existing progress formats.

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
Standalone reads may wait with `--wait`; CSV exports always fail immediately when busy. CSV keyboard interruption
exits 130. [Policy-lab exit codes](policy-lab.md#all-lineage-audit) differ.

## CSV lifecycle

`export csv`, including `--plan`, emits bounded, versioned compact JSONL on stderr; stdout remains the final
JSON result. Reporting activates after parsing, before CLI contract checks/catalogue access, and immediately
flushes `INIT`. Authority: [LifecycleReporter](../libs/alpaca_cli/lifecycle.py); phase messages/typed fields:
[csv_progress.py](../libs/alpaca_cli/csv_progress.py), `PHASES`.

Default heartbeat intervals: 10, 60, 120, 180, 240, then repeating 300 seconds—elapsed times
10, 70, 190, 370, 610, 910, 1210… . `--heartbeat-seconds N` instead sets a fixed integer interval, 1–86400.
Whole `elapsed_seconds` use the monotonic clock from activation.

One held status is sampled by a separate timer, including while work waits. Updates replace status/context
without printing, queuing or resetting the timer. Heartbeats retain it; unchanged work repeats with new elapsed
time. Context replacement prevents previous-scope leakage. Invalid/oversized updates preserve the last valid
status; brief phases may fall between samples and never appear.

Progress schema `alpaca.lifecycle.progress`, version 4: `schema`, `schema_version`, `elapsed_seconds`,
`next_heart_beat_in_seconds`, `phase`, `message`. The next-heartbeat value is remaining scheduled seconds rounded
up, or `null` in terminal error/`FINAL_EXIT` records. Optional `current`/`total` occur together with
`0 <= current <= total`; CSV counts selected public identities. `context` permits only registered phase fields.

Only initial `INIT` includes `ai_agent_read_this`: monitor heartbeats; use other monitoring only when a heartbeat
is over 10 seconds late; derive reports from heartbeat messages; update the user at each heartbeat unless directed
otherwise. Later records omit it even if the held phase remains `INIT`.

During `EXPORT`, `context.estimated_remaining_seconds` is `"TBD"` until progress supplies a numeric estimate;
later estimates are whole seconds. Initial 10-second heartbeat always displays `"TBD"`, regardless of phase or
an available estimate. This overrides display only; later heartbeats retain the estimate. Fixed intervals other
than 10 seconds are unaffected.

| Phase | Context scope |
|---|---|
| `INIT` | Reporting activation; no context. |
| `CATALOGUE` | Open online catalogue; no context. |
| `PLAN` | Catalogue generation, identity/group/source counts, workers/maximum active groups, metadata/payload/output bounds. |
| `EXPORT` | Plan fields, workers, completed counts, CSV bytes, column counts, estimated seconds remaining. |
| `PUBLISH` | Final manifest inside run directory; completed identity/group/row counts, CSV bytes, column counts. |
| `FINAL_EXIT` | Immediate terminal `status`, `exit_code`, `last_phase`. |

Success stops reporting and flushes one `FINAL_EXIT` (`status: success`, code 0). Caught failure first flushes
`alpaca.lifecycle.error`, version 4: error type/message, latest accepted phase, elapsed seconds, available
causes/traceback; then `FINAL_EXIT` (`status: error`). Busy exports exit 3, other CSV failures 1; keyboard
interruption emits `status: aborted`, code 130. Finalization never waits for the next heartbeat; none follows it.

Writes are serialized/flushed. Progress/final records are at most 4 KiB; errors 16 KiB, including newline.
Oversized errors shorten diagnostics and set `truncated: true`. Reporting failures do not change export outcome;
abrupt termination or failed stderr writes can prevent delivery.
