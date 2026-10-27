# Operations

## Prerequisites

Run from repository root with Python 3.11, Unix `flock` and `America/New_York` timezone data.
Install [production dependencies](../requirements.txt):

```bash
python3.11 -m pip install -r requirements.txt
./alpaca-cli-eod --help
./alpaca-cli-eod update --help
```

Provider acquisition requires `ALPACA_API_KEY_ID`, `ALPACA_API_SECRET_KEY` and Alpaca SIP access. Local readers
and offline catalogue maintenance require compatible publications, not credentials/network. Parquet additionally
requires [PyArrow](../requirements-bloodhound.txt). Default store: repository `data_pricing/`; override with
`--store PATH`. [build_parser / dispatch](../libs/alpaca_cli/__init__.py) own commands/options; no abbreviation.
See [CLI reports](cli-reporting.md) for streams, exit codes and monitoring.

## Daily update

For an initialized store:

```bash
./alpaca-cli-eod update
./alpaca-cli-eod status
```

Update observes one complete all-status US-equity catalogue. New discovery accepts active
OTC/NYSE/NASDAQ/AMEX/TXSE labels regardless of tradability; other-exchange histories/dependencies remain.
Omission proves neither inactivity nor removable history. Known active empty/partial histories also advance
outside the live-check summary. SIP acquisition does not promise a separate OTC feed.

Every invocation checks live; saved experiments are not inputs. Failed history checks block append;
independent successes remain recoverable. Published unresolved identity is completed processing, distinct
from acquisition failure or public eligibility. [Execution ownership](ARCHITECTURE.md#daily-update).

After a successful non-plan update, the CLI automatically commits a changed `resources/ticker.csv` to the
current Git branch with message `Update recovery ticker registry`. This includes outstanding registry changes
from earlier runs. The registry lock pins the file during the check/commit; Git selects only that path and
preserves unrelated staged/unstaged work. An unchanged registry produces no commit. Other commands, plans
and unsuccessful updates do not auto-commit. No push or branch switch occurs.

The `COMMIT` phase and immediate diagnostics report checking, committing, the resulting commit ID, unchanged
registry or Git failure through the [heartbeat protocol](cli-reporting.md#shared-lifecycle). Git output is
captured, so stdout remains the data update report. A Git failure leaves the successful data result and exit
code intact; its warning means the registry still needs a commit. Git identity, hooks and signing settings
remain those of the checkout. Automatic commits require the tracked registry in this exact Git checkout.

Optional `update --plan` may fetch metadata but never registers, initializes, recovers or publishes. It reports
work/reasons, additions, actions/pages, metadata reads and conditional dependencies. Sparse histories,
pagination and producer checks change actual requests. Missing boundaries/pending publications are blockers;
execution reselects independently, so a plan is not an execution token.

### Session cutoff

Producer `--through YYYY-MM-DD` is inclusive, rounded back to the last exchange session on/before it.
Default: last session strictly before today's New York date, frozen per invocation. Friday becomes eligible
after Saturday midnight New York, not UTC. Explicit cutoffs must also precede today's New York date;
unsupported calendar ranges fail before selection/registration/requests.

A permitted explicit cutoff of October 16–18, 2026 selects October 16, exclusive end `2026-10-17T00:00:00Z`.
Closed days alone cause no advancement/publication. `pricing_cutoff` reports requested/effective dates.
Recovery retains accepted exclusive bounds; initial/dependency acquisitions share the target. Newer sources
are reported/preserved without checking or rewinding; reconciliation needing a later target stays incomplete.
Observation time, request coverage, last bar and mapping `asof` differ: coverage is not a historical knowledge
snapshot, and a final-session bar need not exist. Reader/export bounds remain inclusive calendar filters.
Authority: [download.py](../libs/alpaca_pricing_download/download.py), `pricing_session_end`.

## Inspect local progress

`./alpaca-cli-eod status` reads local boundaries/work without recovery. Missing/incomplete boundaries report
blockers and reconstruction commands. [Status fields and limits](cli-reporting.md#completion-and-status);
[read scope](technical-contracts.md#universe-observation-and-work-state).

## Adopt or reconstruct an existing store

Finish pending publications through their owner first. Establish only the missing/incompatible boundaries
named by status; these commands have distinct scopes:

| Command | Scope |
|---|---|
| `universe rebuild` | Initialize/preserve compatible registration/work state, including an empty store; does not finish work or establish lineage/summary. Examine incompatible state; do not erase it. |
| `lineage migrate-index` | Project used lookup evidence and complete membership from indexed receipts in resumable batches; no price reads/recalculation. Schema upgrade alone does not establish coverage. |
| `lineage migrate-policy` | Exact offline policy promotion; [conditions and serving follow-up](lineage.md#policy-migration). |
| `lineage update` | Restore missing scheduling coverage explicitly; may census source generations once. Daily update never reconstructs it implicitly. |
| `summary rebuild` | Rebuild from indexed status/validators offline; no history/provider reads. |
| `catalogue bootstrap --plan`, then `catalogue bootstrap` | Establish serving coverage after lineage publication. |

Append `--store PATH` to select a store. New stores first need [pricing and lineage](#focused-standalone-work).
Recheck `status` after owner maintenance; success at one boundary does not establish the others.

## Serving catalogue maintenance

```bash
./alpaca-cli-eod catalogue status
./alpaca-cli-eod catalogue update --plan
./alpaca-cli-eod catalogue update
```

Bootstrap follows lineage; rerun after interruption. Offline preview reports compact selection, legacy
membership-projection reads, receipt/payload bounds and derived publications. Execution evaluates full selected
histories, possibly reread per identity within bounded cache; narrow date filters cannot avoid coverage checks.
No provider requests, identity-rule replay, redownloads, pricing copies or rewrites.

Daily execution/preview require a complete serving selector even with an empty queue. Focused lineage maintains
only selected complete groups in an existing catalogue; it never bootstraps implicitly. Direct pricing writes
need lineage next; readers never maintain stores.

`catalogue update` consumes obligations/reuses compatible decisions; reports include `reused_history_prefixes`,
`full_history_evaluations`, `checked_bars`. Finish pending pricing/lineage owners first. Policy changes require
`catalogue rebuild --plan`, then `catalogue rebuild` on the same store, preserving upstream identity decisions.
[Eligibility, reuse and recovery](technical-contracts.md#serving-catalogue).

## Recovery registry and retries

[resources/ticker.csv](../resources/ticker.csv) is one sorted unique CSV record, atomically unioned under a
stable shared lock. Accepted labels accumulate: omission, inactivity, exchange changes, emptiness and failure
never remove them. Retention preserves opportunities, not guaranteed history; today's catalogue cannot
reconstruct every historically priceable label.

`universe reconcile` observes/registers without price acquisition. Daily/standalone producers share durable work,
registration and a store update lock. Initialize work state first; recover registration before requests.
`--symbols-file` accepts headerless CSV or one label per line, frozen for the invocation.
`--recovery-universe` is explicitly additive and may select full acquisition.

Accepted targets and explicit `pricing acquire --from` starts survive interruption, omission, inactivity and
narrower retries. Completion requires the earliest unfinished start and required end, even for empty history.
Later-than-invocation extensions defer. Identical completed empty requests reuse coverage; later targets acquire
increments. Successful emptiness is neither rejection nor failure. Identity changes/explicit refresh can
invalidate coverage; disabling acquisition leaves obligations pending. [Work contracts](technical-contracts.md#universe-observation-and-work-state).

Transport failures stay pending with a five-minute delay. Access denial/exact rejection stays blocked until
`update --retry-blocked`, which also bypasses transport delays, never cutoff restrictions. Restrictions span
complete groups and daily/standalone phases; changing commands grants no retry. Independent groups may finish.
Standalone lineage reports unsatisfied coverage/check work; daily update finishes it. Correction approval stays
generation-bound after summary removal. [Rejected auxiliary exception](lineage.md#missing-auxiliary-prices).

After interruption, rerun the owning producer; never manually delete selected artifacts, locks or journals.
Older targets cannot restart larger unfinished acquisition. Only after authorized out-of-band restore/import,
with producers stopped, may disposable hints be discarded/reconstructed. Retain receipts, locks, journals and
`universe_updates.sqlite3`; rebuild summary separately. Never test restoration over a live store.

## Focused standalone work

```bash
./alpaca-cli-eod universe rebuild --store data_pricing
./alpaca-cli-eod pricing acquire --store data_pricing --symbol DELL --symbol HPE
./alpaca-cli-eod lineage update --store data_pricing
./alpaca-cli-eod lineage inspect --store data_pricing --symbol DELL
```

Initialize once. Acquisition requires an explicit compatible/empty store and publishes prices only. Initial
start is 2016 unless later `--from`; reject starts before 2016, after the resolved final session or excluding
held observations before registration/requests. Finish missing boundaries through [setup](#adopt-or-reconstruct-an-existing-store).

| Command | Scope |
|---|---|
| `pricing check` | Live summary last-session comparison; no publication. |
| `pricing refresh` | Check eligible histories, replace differences, finish lineage. |
| `pricing refresh --symbol TICKER` | Explicit retained-span replacement; follow with lineage. |
| `lineage update --symbol TICKER` | Complete selected group; may acquire dependency prices. |
| `lineage refresh` | Full selected-group evidence, including older corrections. |
| `lineage inspect --symbol TICKER` | Offline published evidence; no rule replay. |
| `lineage update/refresh --no-pricing-acquisition` | Metadata requests remain possible. |

Pricing uses up to three workers sharing a 199-request/minute invocation budget, not an account-wide limit.
Lineage has one writer/store; others wait at its owner lock. The CLI's outer operation gate fails immediately
when busy. Failed batches retain independent successes. Approved append is internal, with no public shortcut.
[Lock order and recovery](technical-contracts.md#publication-and-recovery).

## Read and export

```bash
./alpaca-cli-eod resolve --symbol DELL
./alpaca-cli-eod read --symbol DELL --adjustment raw all
./alpaca-cli-eod catalogue search DELL
```

[Public API](public-api.md) owns identity/date semantics. [Exports](exports.md) distinguishes single-history CSV,
flat online CSV, per-identity CSV runs and standalone Parquet; these selections and guarantees differ.
