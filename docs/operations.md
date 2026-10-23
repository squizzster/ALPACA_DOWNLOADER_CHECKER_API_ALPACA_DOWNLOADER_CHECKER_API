# Operations

Run from repository root with the [requirements](../README.md) installed. Default standalone store:
`data_pricing/`; select another with `--store PATH`. Options and dispatch:
[canonical CLI](../libs/alpaca_cli/__init__.py), `./alpaca-eod --help`, then command `--help`.
Progress goes to stderr; final reports are JSON stdout. No argument abbreviation.

## Daily update

```bash
./alpaca-eod update --plan
./alpaca-eod update
./alpaca-eod status
```

Update observes one complete all-status US-equity catalogue. New discovery accepts active
OTC/NYSE/NASDAQ/AMEX/TXSE labels regardless of tradability; other-exchange histories/dependencies remain.
Catalogue omission does not remove history or prove inactivity. Known active empty/partial histories also
advance outside the live-check summary. SIP acquisition does not promise a separate OTC feed.

Execution: [daily owner](ARCHITECTURE.md#daily-update). Failed history checks block append, while independent
successful publications remain recoverable. Every invocation checks live; saved experiments are not inputs.
Published unresolved identity is completed processing, distinct from acquisition failure or public eligibility.

`--plan` may fetch metadata but never registers, initializes, recovers or publishes. It reports selected work,
reasons, additions, actions/pages, metadata reads and conditional dependencies. Sparse histories, pagination
and producer checks alter actual requests. Missing boundaries/pending publications are explicit blockers;
execution independently reselects, so a plan is not an execution token.

### Session cutoff

`--through YYYY-MM-DD` is inclusive and rounds back to the last exchange session on/before it. Default:
last session strictly before today's New York date, frozen once per invocation. Friday is eligible after
Saturday midnight New York, not UTC midnight. Explicit cutoffs must also precede today's New York date;
unsupported calendar ranges fail before selection/registration/requests.

For example, October 16–18, 2026 select October 16, with exclusive boundary `2026-10-17T00:00:00Z`.
Closed days alone cause no advancement/publication. `pricing_cutoff` reports requested/effective dates.
Recovery retains accepted exclusive bounds; initial/dependency acquisitions share the target. Newer sources
are preserved and reported separately without checking/rewinding; reconciliation needing a later target stays
incomplete. Coverage is not a historical knowledge snapshot: observation time, request coverage, last bar
and mapping `asof` differ, and a final-session bar need not exist. Reader/export dates remain inclusive
calendar filters. Authority: [pricing_session_end](../libs/alpaca_pricing_download/download.py#L107).

## Inspect local progress

`./alpaca-eod status`: offline, read-only boundary/work inspection. Missing/incomplete boundaries name blockers
and reconstruction commands; status never performs recovery.

- `ready_for_update`: required boundaries exist.
- `processing_complete`: also no outstanding local work/publications.

Either false produces a nonzero exit. Neither claims a fresh provider check or future coverage. Counts
separate empty/nonempty history, unresolved rows and complete assignment; exclusive coverage ends differ from
last observed bars. [Read scope and consistency](technical-contracts.md#universe-observation-and-work-state).

## Adopt or reconstruct an existing store

Finish pending publications through their owner first. Then establish missing boundaries explicitly:

```bash
./alpaca-eod lineage migrate-index --store data_pricing
./alpaca-eod universe rebuild --store data_pricing
./alpaca-eod summary rebuild --store data_pricing
./alpaca-eod catalogue bootstrap --store data_pricing --plan
./alpaca-eod catalogue bootstrap --store data_pricing
./alpaca-eod status --store data_pricing
```

`migrate-index` projects used lookup evidence and complete membership from indexed receipts in resumable
batches; no price reads/recalculation. Schema upgrade alone does not establish coverage.
`universe rebuild` initializes or preserves compatible registration/work state, including an empty store;
it neither finishes work nor establishes lineage/summary. Incompatible state requires examination, not erasure.
`summary rebuild` uses indexed status/validators offline, without histories/provider requests.
Missing scheduling coverage requires explicit `lineage update`, which may census source generations once;
daily update never silently reconstructs it. New stores first need [pricing and lineage](#focused-standalone-work).

## Serving catalogue maintenance

```bash
./alpaca-eod catalogue status --store data_pricing
./alpaca-eod catalogue update --store data_pricing --plan
./alpaca-eod catalogue update --store data_pricing
```

Bootstrap after lineage publication; rerun after interruption. Its offline preview reads compact boundaries
and reports selected symbols, legacy membership-projection reads, receipt/payload bounds and derived
publications. Execution establishes eligibility from full selected histories, possibly reread per identity
within the bounded cache; narrow date filters cannot avoid coverage-dependent source selection. No provider
requests, identity-rule execution, redownloads, pricing copies or rewrites.

Daily execution and preview require a complete serving selector even with an empty queue. Focused lineage
commands maintain only selected complete groups in an existing catalogue; they never bootstrap implicitly.
Direct pricing writes need lineage next. Ordinary application reads never maintain the store.

`catalogue update` consumes obligations and reuses compatible decisions; reports include
`reused_history_prefixes`, `full_history_evaluations`, `checked_bars`. [Reuse and recovery rules](technical-contracts.md#serving-catalogue)
distinguish ineligibility from unfinished work. Resume pending pricing/lineage owners before serving maintenance.
Changed policy requires explicit `catalogue rebuild --plan`, then `catalogue rebuild`, with the same store;
rebuild preserves completed upstream identity decisions.

## Recovery registry and retries

[resources/ticker.csv](../resources/ticker.csv) is one sorted unique CSV record, atomically unioned under a
stable shared lock. Accepted labels only accumulate; omission, inactivity, exchange changes, emptiness and
failure never remove them. Retention preserves opportunities, not guaranteed provider history; today's
catalogue cannot reconstruct every historical priceable label.

`universe reconcile` observes/registers without price acquisition. Daily and standalone producers share
registration, durable work and a store update lock. Initialize work state first; recover registration before
requests. `--symbols-file` accepts headerless CSV or one label per line; selection is frozen for that command.
`--recovery-universe` is explicitly additive and may select full acquisition.

Accepted targets and explicit `pricing acquire --from` starts survive interruption, omission, inactivity and
narrower retries. Keep the earliest unfinished start and required end; completion needs both, even for empty
history. Later-than-invocation extensions defer. Identical completed empty requests reuse coverage; later targets
acquire only increments. Successful empty acquisition is neither rejection nor failure.
Identity changes or explicit refresh can invalidate coverage. Disabling acquisition
leaves obligations pending. [Exact work contracts](technical-contracts.md#universe-observation-and-work-state).

Transport failure stays pending with a five-minute delay. Access denial/exact rejection stays blocked until
`update --retry-blocked`, which also bypasses transport delays, never accepted cutoff restrictions. These
restrictions span complete groups and daily/standalone phases; switching commands grants no retry. Independent
groups may finish. Standalone lineage reports unsatisfied coverage/check work; daily update finishes it.
Correction approval remains generation-bound even after summary removal. [Rejected auxiliary exception](technical-contracts.md#lineage-reuse).

After interruption rerun the owning producer; never delete selected artifacts, locks or journals manually.
Older targets cannot restart larger unfinished acquisition. Only after authorized out-of-band restore/import,
with producers stopped, may disposable hints be discarded and explicitly reconstructed. Retain receipts,
locks, journals and `universe_updates.sqlite3`; rebuild summary separately. Never test restoration over a live store.

## Focused standalone work

```bash
./alpaca-eod universe rebuild --store data_pricing
./alpaca-eod pricing acquire --store data_pricing --symbol DELL --symbol HPE
./alpaca-eod lineage update --store data_pricing
./alpaca-eod lineage inspect --store data_pricing --symbol DELL
```

Initialize once. Acquisition requires an explicit compatible/empty store and publishes prices only.
Initial start is 2016 unless later `--from`; before registration/requests, reject starts before 2016, after
the resolved final session or excluding held observations. Finish missing boundaries using [setup](#adopt-or-reconstruct-an-existing-store).

| Command | Scope |
|---|---|
| `pricing check` | Live summary last-session comparison; no publication. |
| `pricing refresh` | Check eligible histories, replace differences, finish lineage. |
| `pricing refresh --symbol TICKER` | Explicit retained-span replacement; follow with lineage. |
| `lineage update --symbol TICKER` | Complete selected group; may acquire dependency prices. |
| `lineage update` | Pending/incompatible work; explicit hint reconstruction if needed. |
| `lineage refresh` | Full selected-group evidence, including older corrections. |
| `lineage inspect --symbol TICKER` | Offline evidence, no rule replay. |
| `lineage update/refresh --no-pricing-acquisition` | Metadata requests remain possible. |

Pricing has up to three workers sharing one 199-request/minute invocation budget, not an account-wide limit.
Lineage has one writer/store; others wait. Failed batches retain independent successes. Approved append is
internal; there is no public append shortcut. [Lock/publication contract](technical-contracts.md#publication-and-recovery).

## Read and export

```bash
./alpaca-eod resolve --store data_pricing --symbol DELL
./alpaca-eod read --store data_pricing --symbol DELL --adjustment raw all
./alpaca-eod catalogue search --store data_pricing DELL
./alpaca-eod catalogue export --store data_pricing --output ./online.csv
./alpaca-eod export csv --store data_pricing --plan
./alpaca-eod export csv --store data_pricing
./alpaca-eod export csv --store data_pricing --workers 2
./alpaca-eod export csv --store data_pricing --workers 4
./alpaca-eod export bloodhound --output ./alpaca.parquet --symbol DELL
```

[Public API](public-api.md) owns identity/date/output semantics. Exit codes: 0 success; 1 failure/incomplete
work; 2 invalid arguments; standalone reader 3 busy (`--wait` waits).

`export csv` defaults to `exports/csv/` and all online public IDs. Repeat `--public-id ID` for a selected
trial; `--output DIR` selects another export root. `--from`/`--through` are inclusive date filters.
`--plan` reads the opened catalogue only and reports selected work without opening pricing payloads or
creating output. See the [per-identity CSV contract](public-api.md#per-identity-csv-export) for file layout,
column mappings and publication guarantees.

`--workers` accepts 1 through 4 concurrent complete groups, defaulting to 2. CSV export has no `--wait` option;
busy source or destination locks exit immediately with code 3. The library uses the same nonblocking
CSV behavior. Planning reports the configured workers and maximum active groups without opening payloads.

CSV execution and `--plan` emit compact, versioned JSONL on stderr; stdout remains the final JSON result.
Reporting begins after argument parsing, before CLI contract checks and catalogue access. An `INIT` record
is flushed immediately. Heartbeat intervals are 60, 120, 180, 240 and then 300 seconds repeatedly: records
fall at elapsed **1, 3, 6, 10, 15, 20… minutes**. `--heartbeat-seconds N` selects a fixed interval instead
(integer seconds, 1–86400). Whole `elapsed_seconds` use the monotonic clock from reporting activation.

The reporter holds one status in memory. Work updates replace it without printing, queuing or resetting
the timer; a separate timer samples it even while work is waiting. A heartbeat retains the status, so
unchanged work repeats with a new elapsed time. Every update replaces its context as well, preventing
values from an earlier phase or work scope from leaking into the next. Invalid or oversized updates leave
the last valid status unchanged. Brief phases may occur between heartbeats and never appear in the output.

Progress schema `alpaca.lifecycle.progress`, version 1, contains `schema`, `schema_version`,
`elapsed_seconds`, `phase` and `message`. Optional `current` and `total` always occur together and satisfy
`0 <= current <= total`; `context` contains only fields registered for the phase. CSV `current`/`total`
count selected public identities. Phase messages and typed field registrations live in
[csv_progress.py](../libs/alpaca_cli/csv_progress.py):

| Phase | Work and permitted context |
|---|---|
| `INIT` | Reporting activation; no context. |
| `CATALOGUE` | Open the published online catalogue; no context. |
| `PLAN` | Select work; catalogue generation, identity/group/source counts, workers/maximum active groups and planned metadata/payload/output bounds. |
| `EXPORT` | Write CSVs and digests; plan fields, configured workers, completed counts, CSV bytes, column counts and estimated seconds remaining. |
| `PUBLISH` | Publish the final manifest inside the run directory; completed identity/group/row counts, CSV bytes and column counts. |
| `FINAL_EXIT` | Immediate termination record with `status`, `exit_code` and `last_phase`. |

On success, reporting stops and flushes one `FINAL_EXIT` with `status: success`, exit code 0. A caught
failure flushes a separate `alpaca.lifecycle.error` schema, version 1, with `error_type`, `message`, latest
accepted `phase`, elapsed seconds and available causes/traceback, then `FINAL_EXIT` with `status: error`.
Busy destinations exit 3; other CSV failures exit 1. Keyboard interruption emits `status: aborted` and
exits 130. Finalization never waits for the next heartbeat, and no heartbeat follows the final record.
Writes are serialized and flushed; progress/final records are limited to 4 KiB and errors to 16 KiB,
including the newline. Oversized error diagnostics are shortened with `truncated: true`.
Reporting failures do not change the export outcome; abrupt termination or failed stderr writes can
prevent delivery. This lifecycle currently applies to `export csv`; other commands keep their existing
progress formats. The reusable reporter is owned by [alpaca_cli](ARCHITECTURE.md#code-ownership).

Parquet needs [PyArrow](../requirements-bloodhound.txt) and an existing output parent outside the source store.
Default selection is all indexed lineage groups; any rejected identity aborts by default.
`--include-all-passed` allows output despite rejected identities;
diagnostics remain stderr. `--replace` permits replacement after successful conversion. Parquet is the sole
persistent output. [Conversion guarantees and failure modes](technical-contracts.md#parquet-conversion).
