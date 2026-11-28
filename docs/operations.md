# Operations

Start an empty store with the [new installation guide](../NEW_SETUP_VIRGIN_INSTALL_GUIDE.md). For an
existing store, [inspect status](#inspect-local-progress), run the named owner's recovery, then use
[daily update](#daily-update) once the required boundaries exist.

## Prerequisites

Run from the repository root with Python 3.11, Unix `flock` and `America/New_York` timezone data.
Python's SQLite build must support `json_each` and `->>` for dated public ticker selection. Install the
[production dependencies](../requirements.txt).

```bash
python3.11 -m pip install -r requirements.txt
./alpaca-cli-eod --help
./alpaca-cli-eod update --help
```

The canonical `./alpaca-cli-eod` launcher preserves an active virtual environment. Otherwise, it uses
this checkout's executable `.venv/bin/python3.11` when present, falling back to the current Python only
when that checkout environment is unavailable. Install optional dependency groups, including Bloodhound,
in the environment the launcher selects.

- Acquisition requires `ALPACA_API_KEY_ID`, `ALPACA_API_SECRET_KEY` and Alpaca SIP access. Local reads
  and offline catalogue maintenance require only compatible publications. Parquet and EOD event exports
  need the [Bloodhound dependencies](../requirements-bloodhound.txt).
- The default store is the repository's `data_pricing/`. Select another with `--store PATH`. Use an empty
  or compatible directory, a symlink-free path and a writable parent for the sibling operation lock.
  Spell out option names. [CLI command definitions](../libs/alpaca_cli/__init__.py)
- See [CLI reports](cli-reporting.md) for streams, exit codes and monitoring. Daily and focused lineage
  updates accept `--debug FILE.jsonl` for a [paired trace](cli-reporting.md#optional-update-debug-trace)
  with reuse and fallback reasons.

## Daily update

Run on an initialized store.

```bash
./alpaca-cli-eod update
./alpaca-cli-eod status
```

Each update observes one complete all-status US-equity catalogue. Discovery accepts active
OTC/NYSE/NASDAQ/AMEX/TXSE labels regardless of tradability and retains other-exchange histories and
dependencies. Omission establishes neither inactivity nor grounds for removal. Known active empty or
partial histories advance even outside the live-check summary. SIP acquisition gives no separate OTC-feed
guarantee.

The schema-2 [lineage YAML](../resources/alpaca_security_correlation_policy.yaml) selects registered
generic rules and parameters under `policy` and optional generic producers under `producers`. It admits
no factual applications or supplement registry. Authored assertions fail before new store or provider
work. Applicable decisions and frozen journals retain accepted provenance and scope. A pending legacy
fact obligation without an admissible completed handoff needs generic recovery by its owner.
Acknowledgement or replay as a research assertion cannot clear it.
[Retained provenance](lineage.md#sourced-event-facts) and the [nonpublishing rule tool](lineage-debug.md)

Shared stages pass native prepared/history records under one continuous operation lock without bulk
encoding or decoding. Standalone JSON APIs support separate processes. Each invocation checks live and
excludes saved experiments. A failed history check blocks its append. Independent successes remain
recoverable. An unresolved identity can complete processing. Its publication certifies neither public
eligibility nor an acquisition failure. [Execution ownership](ARCHITECTURE.md#daily-update)

**Registry commit.** Only a fully successful non-plan update automatically commits or pushes. It checks
and commits only `resources/ticker.csv` under the registry lock, including outstanding changes from
earlier runs. A changed file yields `Update recovery ticker registry` on the current branch. An unchanged
file yields no commit. Other staged and unstaged work stays intact.

**Registry push.** After releasing the lock, the CLI checks the current branch's configured upstream and
single push destination. It pushes exactly one registry-only commit whose sole parent matches the
advertised remote tip. Explicit source commit and destination ref exclude later local work, other
branches, tags and submodules. It never forces, merges, rebases or creates an upstream branch. Multiple
or unrelated pending commits and divergent history need ordinary Git synchronization. An up-to-date
remote needs no push. The next successful update retries an eligible pending commit even with an
unchanged registry.

Automatic commit requires the registry tracked in this checkout and uses its Git identity, hooks and
signing settings. Automatic push also requires one configured upstream remote branch. `COMMIT`/`PUSH`
[heartbeats](cli-reporting.md#shared-lifecycle) and immediate diagnostics report checks, commits and IDs,
unchanged/already-current outcomes, pushes and failures. Captured Git output leaves stdout for the data
report. A Git failure warns of outstanding work and preserves the successful data result and exit code.

`update --plan` accepts compact local boundaries before fetching metadata. Missing boundaries or pending
publications block requests. The preview registers, initializes, recovers and publishes nothing. It
reports work and reasons, additions, actions/pages, metadata reads and conditional dependencies. Actual
requests depend on sparse histories, pagination and producer checks. Execution selects independently.

### Session cutoff

`--through YYYY-MM-DD` includes the last exchange session on or before that date. Update and `--plan`
coalesce consecutive selected late-bar sessions sharing a mapping. `--coalesce-late-bars` selects this
default. `--exact-late-bars` uses individual New York sessions for provider-window diagnostics. Both
preserve session selection and freshness.

Each invocation freezes the last session strictly before today's New York date. Friday becomes eligible
after Saturday midnight in New York, regardless of UTC midnight. Explicit cutoffs must also precede today
in New York. Unsupported calendar ranges fail before selection, registration or requests.

An allowed October 16–18, 2026 cutoff selects October 16, exclusive end `2026-10-17T00:00:00Z`. Closed
days alone cause no advancement or publication. `pricing_cutoff` reports requested and effective dates.
Recovery retains accepted exclusive bounds. Initial and dependency acquisitions share the target. Newer
sources are reported and preserved without checks or rewinds. Reconciliation needing a later target stays
incomplete.

Observation time, request coverage, last bar and mapping `asof` have separate meanings. Coverage cannot
establish historical knowledge or guarantee a final-session bar. Reader/export dates filter inclusive
calendar bounds. [`pricing_session_end` in download.py](../libs/alpaca_pricing_download/download.py)

## Inspect local progress

`./alpaca-cli-eod status` reads local boundaries and work without recovery. Missing or incomplete
boundaries report blockers and reconstruction commands.
[Status fields](cli-reporting.md#completion-and-status) and
[read scope](technical-contracts.md#universe-observation-and-work-state)

## Adopt or reconstruct an existing store

Finish pending publications through their owners. Restore only the missing or incompatible boundaries
named by status.

| Command | Scope |
|---|---|
| `universe rebuild` | Initialize an empty workflow database or migrate version 1 offline, retaining registration and work before compaction. Rerun after interruption. Inspect unknown formats. |
| `lineage migrate-index` | Project used lookup evidence and complete membership from indexed receipts in resumable batches, without price reads or recalculation. A schema upgrade alone leaves coverage unestablished. |
| `lineage migrate-policy` | Apply an exact offline policy promotion. [Conditions and serving follow-up](lineage.md#policy-migration) |
| `lineage migrate-assignments --symbol TICKER` | Assemble missing compact assignment projections for the complete group without price reads or rule replay. Widen explicitly with `--symbols-file FILE` or `--all-published`. `--limit` selects 1–10,000 groups per run (default 128). Repeat until `remaining_groups` is zero. Journal recovery skips completed groups. Run `catalogue update` to consume the preserved decision. |
| `lineage update` | Restore missing scheduling coverage, with at most one source-generation census. Daily update requires explicit restoration. |
| `summary rebuild` | Rebuild offline from indexed status and validators without history or provider reads. |
| `catalogue bootstrap --plan`, then `catalogue bootstrap` | Establish serving coverage after lineage publishes. |

Append `--store PATH` to select a store. New stores first need
[pricing and lineage](#focused-standalone-work). Recheck `status` after maintenance. Each owner
establishes only its own boundary.

## Rebuild published lineage after a computation repair

```bash
./alpaca-cli-eod lineage rebuild --all-published --plan
./alpaca-cli-eod lineage rebuild --all-published --debug /tmp/alpaca-lineage-rebuild.jsonl
```

This explicit universe repair recomputes obsolete lineage at computation revision 2. Each complete group
supplies its retained prices, accepted metadata, individual ranges and mappings. It needs no credentials,
provider requests or pricing writes. Missing, pending or incompatible inputs block the repair without
wider acquisition or silent evidence refresh. After full evaluation, the owner journals the revision.
Retries skip current, completed groups before reading receipts or payloads. The subsequent explicit
serving rebuild retains applicable numerical checkpoints and establishes legacy export applicability.
Daily live-price freshness and normal-update performance have separate acceptance criteria.

## Backfill historical listings

Recover dated exchange evidence predating regular asset observations on a published store. Selected
online securities require an applicable serving metadata projection.

```bash
./alpaca-cli-eod lineage backfill-listings --symbol SYMBOL --plan
./alpaca-cli-eod lineage backfill-listings --symbol SYMBOL --dry-run
./alpaca-cli-eod lineage backfill-listings --symbol SYMBOL --apply
./alpaca-cli-eod export metadata --symbol SYMBOL
```

A ticker selects complete online groups matching a dated label or source symbol. Repeated `--public-id`
limits new acquisitions to named owners. `--all-published` selects every online trading range.
`--limit N` selects the first N groups awaiting publication in stable order.

| Mode | Result |
|---|---|
| `--plan` | Reads completed selectors and reports outstanding scope without credentials. |
| `--stage-auctions` | Retains missing native auction screens for the selection. |
| `--dry-run` | Acquires missing observations and saves completed assessments locally for later publication. |
| `--apply` | Consumes applicable staged assessments, finishes missing work and publishes lineage and serving metadata for completed groups. |
| `--publish-staged` | Publishes whole ready groups offline. Reports incomplete if any selected group still awaits assessment. |

Workers share the Alpaca metadata request budget. Restarts use the observation database and publication
journal. If pricing extends before application, reuse applicable completed intervals and acquire only
uncovered sessions. Changed ownership, competition, rules or retained evidence reselect the affected
scope. Applying a bounded batch advances the next selection.

Repeat the plan after publication. A completed range may have unresolved exchange evidence. Scoped runs
report per ID. Universe runs report counts.
[Evidence and unresolved outcomes](lineage.md#historical-listing-assessments)

## Recover selected current-revision lineage

For defective current-revision decisions, derive an explicit
[generation-bound selection](technical-contracts.md#targeted-retained-lineage-recovery) from the accepted
comparison. Omit groups with disputed membership changes for separate review.

```bash
./alpaca-cli-eod lineage recover --store data_pricing --selection /tmp/lineage-recovery.selection.json --plan
./alpaca-cli-eod lineage recover --store data_pricing --selection /tmp/lineage-recovery.selection.json --debug /tmp/lineage-recovery.debug.jsonl
```

The plan uses compact local controls without receipt or pricing-payload reads. Execution reconstructs
only the selected complete groups from accepted retained prices and metadata, then maintains serving
output. It makes no provider requests or pricing writes. Changed pricing, receipt generations, membership
or policy block recovery. Preserve the frozen selection on rejection. Journaled completion markers skip
completed groups before receipt or price reads on retry. Daily update never implicitly selects this
recovery. The all-published computation rebuild skips current decisions.

When a serving group reports `accepted_ownership_recovery_required`, its checkpoint can support a single
source's former assignment, disputed trading around a cash distribution, or a processing-date discrepancy
between successive ticker names. Select the entire group with `mode` set to `restore_accepted_owner`. The
three cases share a selection format and journal. The
[recovery contract](technical-contracts.md#targeted-retained-lineage-recovery) specifies each case's
observations and generation bindings.

If comparison already produced complete candidate artifacts, pass the reviewed
`alpaca_retained_candidate_handoff` as `--selection`. Recovery finalizes bookkeeping and journals the
decision without reconstructing prices, rerunning correlation or numerical selection, or rebuilding
assignment projections. Required `review` records an approved conclusion after classification of every
semantic difference. Missing review or prepared outputs block publication. The same generation and
mapping checks apply. After selected serving maintenance, generate a new selected
[CSV export](exports.md#per-identity-csv-export).

Prepare a newly authorized assessment against one complete group's current publication.

```bash
./alpaca-cli-eod lineage prepare-recovery --store data_pricing --symbol LABEL --output /tmp/recovery.pending.json
```

Preparation writes an unapproved candidate outside the store using accepted group prices and metadata.
Add `--observe-market-evidence` for missing, rule-selected SIP/OTC trades and literal raw bars at
selected ownership boundaries. Successful and empty responses and entitlement denials retain their actual
request scopes. A temporary authenticated checkpoint beside the output preserves completed requests
across interruption. Retrying completed preparation with the same path, scope and mode checks selector
applicability before returning the prior output.

When an identity's old and new selections cover the same complete eight-view session grid, the candidate
dates each change of winning pricing source. The original numerical selector's complete-bundle equality
can carry serving's prior result across those dates. The packet supplies selected native values for
serving to validate on remaining changed dates. The complete-group comparison also shows identity and
label changes outside that numerical scope. If labels change while multiple sources retain every selected
date, serving uses a dated market projection and retains its prior numerical result.

Compare `comparison.public_change_intervals`, all group dimensions in `comparison.comparison` and
candidate receipts against the selected baseline. For a supported result, save a copy with
`review.version` set to `1`, `review.status` set to `approved` and the reviewer's conclusion in
`review.conclusion`. Pass the copy to `lineage recover --selection`. Pricing, lineage and serving retain
their publications until reviewed recovery runs.

For a diagnosed numerical-source conflict with accepted ownership, use the same commands with
schema-version-1 `alpaca_lineage_numerical_recovery` in `numerical_only` mode. The
[numerical-only contract](technical-contracts.md#numerical-only-completion) binds the exact group,
generation and boundary-local all-view row scopes. The owner publishes completed numerical choices,
preserving accepted ownership, membership and unaffected history. It makes no provider requests or
pricing writes. A required mapping correction remains the owner's explicit work. Serving receives
completed equality and held selected bundles without comparison or payload rereads. Daily update can
finish explicitly enrolled selections from existing work state. Unreviewed legacy conflicts have no
automatic discovery or enrollment. Unchanged retries skip completed work before reading source bodies.

## Serving catalogue maintenance

```bash
./alpaca-cli-eod catalogue status
./alpaca-cli-eod catalogue update --plan
./alpaca-cli-eod catalogue update
```

Bootstrap after lineage publishes. Rerun after interruption. Offline preview reports compact selection,
legacy membership-projection reads, receipt/payload bounds and derived publications. Ordinary maintenance
skips matching groups, reuses completed history prefixes and finishes stale lineage export
acknowledgements from matching compact producer selections.

Ownership uncertainty puts affected identities offline with a reason in `catalogue status` under
`offline_reviews`. Independent supported identities stay online. An offline review can be a completed
publication. Bootstrap, rebuild or invalidation may require full selected histories, with per-ID rereads
if the bounded cache requires them. Narrow date filters cannot establish full-history eligibility.
Maintenance makes no provider requests, replays no identity rules, and never redownloads, copies or
rewrites prices.

For a version-1 raw-gap selector, `catalogue rebuild` converts projections before history maintenance,
preserving completed decisions and pending work. Selected ineligible groups may need bounded raw-source
reads. [Source-specific scheduling](technical-contracts.md#pricing-acquisition-and-continuation) and
[exact policy migration](lineage.md#policy-migration) give the upgrade order.

Even with an empty queue, daily execution and preview require a complete serving selector. Focused
lineage maintains only selected complete groups in an existing catalogue and cannot bootstrap implicitly.
Follow direct pricing writes with lineage. Readers never maintain stores.

Focused recovery can finish selected groups in an incomplete catalogue. Its `complete` covers only that
work. Until explicit bootstrap/rebuild finishes, `serving_catalogue.catalogue_complete` stays false and
daily planning and public reads reject the catalogue.

Finish pending pricing and lineage work before `catalogue update` consumes obligations and reuses
compatible decisions. Its report includes `reused_history_prefixes`, `full_history_evaluations` and
`checked_bars`. For a policy change, run `catalogue rebuild --plan` then `catalogue rebuild` on the same
store, preserving upstream identity decisions.
[Eligibility, reuse and recovery](technical-contracts.md#serving-catalogue)

## Recovery registry and retries

[resources/ticker.csv](../resources/ticker.csv) unions labels atomically into one sorted, unique CSV
record under a stable exclusive lock. Accepted labels survive omission, inactivity, exchange changes,
empty results and failures. The registry preserves request opportunities. Today's catalogue cannot
recover every historically priceable label or guarantee history.

`universe reconcile` observes and registers without acquiring prices. Daily and standalone producers
share durable work, registration and the store update lock. Initialize work state first and recover
registration before requests. `--symbols-file` freezes headerless CSV or one label per line for the
invocation. `--recovery-universe` adds labels explicitly and can select full acquisition.

Accepted targets and explicit `pricing acquire --from` starts survive interruption, omission, inactivity
and narrower retries. Completion covers the earliest unfinished start through the required end, even for
empty history. Extensions beyond the invocation defer. Identical successful empty requests reuse
coverage. Later targets acquire increments. Emptiness counts as success. An identity change or explicit
refresh may invalidate coverage. Disabling acquisition leaves obligations pending.
[Work contracts](technical-contracts.md#universe-observation-and-work-state)

Transport failures stay pending with a five-minute retry delay. `update --retry-blocked` releases access
denials and exact rejections and bypasses transport delays. Cutoffs still apply. Restrictions follow
complete groups across commands and daily/standalone phases. Independent groups can finish. Standalone
lineage reports unsatisfied coverage/check work for daily update to finish. Correction approval stays
generation-bound after summary removal.
[Rejected auxiliary exception](lineage.md#missing-auxiliary-prices)

After interruption, rerun the owning producer and retain selected artifacts, locks and journals. An older
target cannot restart a larger unfinished acquisition. Discard or reconstruct disposable hints only after
an authorized out-of-band restore/import with producers stopped. Keep receipts, locks, journals and
`universe_updates.sqlite3`. Rebuild the summary separately. Test restoration away from the live store.

## Focused standalone work

```bash
./alpaca-cli-eod universe rebuild --store data_pricing
./alpaca-cli-eod pricing acquire --store data_pricing --symbol DELL --symbol HPE
./alpaca-cli-eod lineage update --store data_pricing
./alpaca-cli-eod lineage inspect --store data_pricing --symbol DELL
```

Initialize once. Acquisition requires an explicitly selected compatible or empty store and publishes only
prices. History starts in 2016 unless `--from` selects a later date. Before registration or requests,
acquisition rejects starts before 2016, after the resolved final session, or excluding held observations.
Follow [setup](#adopt-or-reconstruct-an-existing-store) for missing boundaries.

Selected lineage updates use provider observations and applicable completed decisions. They reject
`--event-supplements` and fact-backed API inputs. Research documents supply neither runtime owners nor
event interpretations. Use external references in the
[generic-rule research loop](development.md#lineage-research-and-rule-improvement-loop).
`--no-pricing-acquisition` preserves normal selection. Metadata requests remain possible and missing
required observations remain explicit owning work.

| Command | Scope |
|---|---|
| `pricing check` | Live first/latest session comparison in the fully adjusted view; no publication. |
| `pricing refresh` | Check eligible histories, replace differences, finish lineage. |
| `pricing refresh --symbol TICKER` | Explicit retained-span replacement; follow with lineage. |
| `lineage update --symbol TICKER` | Complete selected group; may acquire dependency prices. |
| `lineage refresh` | Full selected-group evidence, including older corrections. |
| `lineage inspect --symbol TICKER` | Offline published evidence; no rule replay. |
| `lineage update/refresh --no-pricing-acquisition` | Metadata requests remain possible. |

Up to three pricing workers share a 199-request/minute budget scoped to one invocation. Account-wide
limits apply separately. Lineage allows one writer per store. Others wait at its owner lock. CLI writers
wait at the outer operation gate before store or provider work. Failed batches retain independent
successes. Approved append is internal with no public shortcut.
[Lock reporting and cancellation](cli-reporting.md#completion-and-status) and
[lock order and recovery](technical-contracts.md#publication-and-recovery)

## Read and export

```bash
./alpaca-cli-eod resolve --symbol DELL
./alpaca-cli-eod read --symbol DELL --adjustment raw all
./alpaca-cli-eod catalogue search DELL
```

See [Public API](public-api.md) for identity and date semantics and [Exports](exports.md) for the
selection and guarantees of single-history CSV, flat online CSV, per-identity CSV runs and standalone
Parquet.
