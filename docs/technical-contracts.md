# Technical contracts

## Trust and ownership

Owners calculate/validate outputs, publish selectors last. Completed receipts are authoritative on first
read: consume selected data only; no existence/symlink inventories, stat-based cache keys, payload rehashing
or repeated upstream rules. Reacquired locks require current-selector generation checks, not file audits.
Publication compares new outputs against recorded digests; validates new destinations/outputs. Recovery
owns its journal; `authenticate_*` explicitly audits existing data; forced repair replaces supplied payloads.
Matching complete input generations permit reuse; changes invalidate dependent closure. Readers reject
incomplete publications, never repair.

Operations retain accepted package/store compatibility, lineage membership and summary snapshots in RAM.
Compact revisions guard reuse; publisher-returned index changes advance the view; changed summary revision
requires a new snapshot. Standalone entry points establish their own context. No persistent context cache
or alternative publication authority.

## Standalone artifacts

Paths relative to standalone store:

| Artifact | Authority |
|---|---|
| `store.manifest.json` | Store format. |
| `end_of_day/pricing/<TICKER>/source.manifest.json` | Pricing boundaries/validator selection. |
| `validator_first_last_sessions.json` beside source | Exact first/last rows per view, bound to source receipt. |
| Pricing `completed_years`, current-year/current-month boundaries | CSV/gzip payloads + selecting manifests. |
| `end_of_day/manifests/<TICKER>/update.json` | Unconsumed inclusive changed range: `from`, `to`. |
| `end_of_day/lineage/<TICKER>/lineage.manifest.json` | Evidence, input generations, chronological assignments. |
| `end_of_day/lineage/index.sqlite3` | Published groups/generations, lookup-evidence projection. |
| `universe_updates.sqlite3` | Accepted observations, registration intentions, targets, attempts, outcomes. |
| `pricing_check_summary.sqlite3` | Minimal live-check baseline. |
| `lineage_update_check.sqlite3`, `download_completion.json` | Disposable scheduling hints, never data authority. |

Effective pricing/assignments = completed years + year excluding active month + authoritative month.
Rollover consolidates; ordinary month updates preserve cold payloads. Manifests map eight logical views
onto distinct payloads; exact-copy aliases share files.

Columns: `timestamp,open,high,low,close,volume,trade_count,vwap`. JSON numbers → exact strings, no
binary-float arithmetic. Wrong symbols, malformed values, duplicate/out-of-range timestamps or incomplete
pagination fail. Downloader validates source structure, not market validity/identity.

Ordinary updates compare every view's first/last endpoints; mismatch → full replacement. Retained dependency
stages: compatible endpoint probes → selected acquisition ranges → resulting first-session probes, batched
by the existing solver. Apply row budgets to endpoints first, actual incremental/replacement ranges later.
Buffered responses retain downloader comparisons, overlap checks, bounded fresh retry and locked source
reselection. Failed requests exclude affected tickers from later stages; multi-symbol HTTP 400 may use
exact fallback. Wider request failure may defer more members. Progress reports actions, minimum/observed
pages, acquisition reuse and publication. Minima exclude conditional corrections/dependency expansion;
never an ETA.

Completed responses bind normalized rows to symbol, adjustment, dates, asof. HTTP owner exhausts pagination
and normalizes once; consumers slice without renormalizing. Fresh first-session probes remain independent.
Integrated live check compares only summary last `all` rows; any differing field requires all-view retained-
span replacement before append. Replacement notices include removed sessions; append notices cover changes.

Append approval is an in-memory handoff of checked source generations for selected missing intervals;
the summary stays minimal. Publication rechecks generations under ticker locks, even if concurrent edits
preserve the last row. New summary membership cannot expand prior approval. Empty append requires all eight
retained views empty; partial views use ordinary history checks.

Buffered initial/invalidated acquisitions bind selected generation, including explicit absence. Under ticker
lock: recover journal → compare current source with selection → publish. Conflict rejects buffered replacement
and requires reselection, never overwrites concurrent publication. Retained-span refresh preserves original
start/end/mapping date. Exact schemas: [contract](../libs/alpaca_pricing_download/contract.py),
[publication](../libs/alpaca_pricing_download/storage.py).

## Check summary

Entry: only `symbol`, saved `asof`, exact eight-field `last_all`. Eligible: unique stored same-symbol active
US-equity lookup + nonempty `all` endpoint. Stored active status ≠ publicly online.
Timestamp supplies session date; date/asof pairs group compatible checks. Additional state only controls
publication: format, epoch/revision/coverage, pending owner markers. No first rows, other views, identities,
histories or prior comparison results.

Pricing maintains eligible baselines; lineage owns membership. Separate markers prevent cross-owner
completion. Missing/incomplete/pending summary fails closed. Verify snapshot token before accepting a
comparison; normal reads never enumerate ticker files. Owners: [storage](../libs/alpaca_pricing_download/check_summary.py),
[eligibility/rebuild](../libs/alpaca_lineage_build/check_summary.py).

## Universe observation and work state

Registry = accepted labels, not identity/acquisition success. Stable shared lock + sorted atomic union
preserves additions across stores. Clean-room builds own immutable input snapshots; pinning preserves
original run identity/contracts, never upgrades old work. Labels, hashes, snapshot bytes derive from one
captured byte sequence. Combined publication lock serializes initialization/pinning. Before snapshot
publication, `clean_room_run/initialization.pending.json` records original manifest/input bytes. New run
manifest publishes last; pinning leaves existing manifests unchanged; journal clears afterward. Recovery
finishes original inputs before invocation target/contract checks. Registry growth cannot alter interrupted input.

Catalogue observation retains every record, including duplicate labels. Active discovery-exchange records
introduce work. Unique symbol records may supply metadata; duplicate/missing evidence uses exact lookup;
CUSIPs remain exact. Absence proves neither negative identity nor unavailable pricing.

Lineage index projects receipt-used symbol lookups, dependency evidence, coverage, row outcomes and selected
receipt/source generations. Receipts retain last positive own-label lookup for maintenance; later missing
lookup does not prove inactivity. Current identity/summary decisions still use current evidence.
Projection has its own schema/revision/coverage. Explicit migration reads indexed receipts in resumable
batches; absent historical facts remain unknown. Normal comparison uses projection; changed evidence
refreshes complete affected groups, invalidating evidence/decision reuse.

Transactions persist observation, registration intention and work → publish registry union → acknowledge
registration. Only pricing/lineage selectors establish publication; the work database cannot. Completed
publication satisfies interrupted acknowledgement without reacquisition. Accepted work retains stable
obligation ID, refresh/replacement requirements, evidence and target. Lineage projects the ID only after
locked pricing inputs satisfy it; coverage alone cannot satisfy unperformed refresh. Maintenance persists
additional coverage before requests.

Unfinished pricing targets survive acquisition/check flags, inactivity, omission and older invocations.
New inactivity may require metadata only; cannot cancel accepted pricing. Persist explicit inclusive
`pricing_start` before requests. Shared pricing validation requires start ≥ 2016-01-01 and < exclusive end;
CLI, work producer and batch acquisition reject impossible ranges before mutation/requests. Merge unfinished
work using earliest start. Selection, dependency acquisition, stamping and acknowledgement require both
endpoints; unchanged last session cannot satisfy missing extension. Reuse compatible completed extensions
after interrupted lineage/acknowledgement. Without explicit start, retain source range or initial 2016 default.

Failed checks create obligations requiring a subsequent check of the selected generation. Sources record
identity-replacement acquisition-evidence fingerprints even when bars are unchanged. Stable store update
locks serialize daily/standalone orchestration; publisher locks/generation checks still apply.
Standalone registration loads registry once, persists intentions and acknowledges union before existing-group
catch-up or new dependency acquisition. Acquisition freezes first-read label selection through registration,
execution and batch memo; never rereads mutable selection files/registry. Narrower memos cannot bypass
unfinished starts. Persist dependency coverage/complete groups before registration/acquisition; links survive
later evidence omission until satisfying publication. Registry membership cannot satisfy work.

Recover pending registration first; known membership needs no republication. Explicit work-state initialization
is independent of lineage/projection/summary, avoiding circular prerequisites. Retained-span correction:
durable obligation before acquisition → correction ID in pricing publication → lineage projection for
completion/reuse. Later matching endpoints cannot erase unperformed correction; completed correction needs
no full redownload after interrupted lineage/acknowledgement.

Access failures, retry delays and accepted cutoffs constrain metadata, acquisition, correction, append and
expansion across daily/standalone commands. Required-member failure blocks sibling group publication.
Interrupted local registry publication stays pending for immediate owner recovery, without provider retry
delay. Durable outcomes preserve exact-request metadata restrictions. Failure transactions return committed
state to in-memory policy; dependency/correction extensions retain status, attempts and retry deadline.

Every lineage entry point reconciles used symbol evidence against unfinished work and selected prior receipts.
Changed identity mapping records complete-group replacement requirements atomically before pricing requests;
discovery also records available dependency identity evidence before initial acquisition. Pricing binds
evidence fingerprint/source generation; lineage cannot stamp different lookup facts. Catalogue/exact provenance
alone cannot invalidate identical evidence. Compatible completed replacements survive interruption; disabled
acquisition leaves changed requirements pending. Pricing replaced during history-refresh reconciliation
supplies the subsequent approval generation.

Identical completed requests, including empty, reuse coverage. Compatible empty advancement requests only
the increment; relevant identity changes/explicit retained-span refresh may invalidate it. Outcomes distinguish
empty, published, unresolved, access denial, exact rejection and retryable failure. Missing/incompatible work
state or incomplete projection requires explicit maintenance, never an empty queue or automatic receipt crawl.

Preview consumes compact boundaries without writes/recovery; pages/expansion depend on responses and estimates
say so. Execution independently reselects boundaries/current catalogue. Inclusive targets → exclusive UTC;
newer spans remain preserved, excluded from older-target checking/correction.

Offline status reads index/projection, summary, scheduling and work state read-only. Scheduling/summary pending
symbols select the only per-ticker pricing-journal existence checks. Check lineage pending marker under shared
publication lock. No payload/receipt reads, ticker scans or recovery. Each boundary is coherent separately;
report is not a cross-producer transaction. [Status semantics](operations.md#inspect-local-progress).

## API plan

Pure [build_plan](../libs/alpaca_data_api_solver/__init__.py):
`{"steps":[{"needs":[...]}]}` → versioned ordered action steps. Each need: `kind`, nonempty `why`, plus:

| Kind | Fields |
|---|---|
| `bars` | `symbols`, `adjustments`, `asof`, `ranges` with `start`/`end_exclusive`. |
| `asset_catalogue` | None; one complete all-status US-equity observation, filtered locally. |
| `asset_lookup` | `identifiers`; optional `lookup_kind`: symbol, cusip, asset_id, exact. |
| `corporate_actions` | `symbols`, inclusive `start`/`end`. |

Actions: `strategy`, `why`, `request`, `deliver`, pagination where needed. Delivery selects requested slices.
Strategies: `bars_single_range`, `bars_multi_range`, `bars_boundary_batch`, `asset_lookup`, `asset_catalogue`,
`corporate_actions_batch`. Within-step needs are independent; steps preserve prerequisites. Exhaust tokens
before advancing; construct dependent needs after responses.

Bars: 1Day/SIP/USD; separate request per adjustment. `asof` selects mapping, not adjustment vintage.
Date-only solver ranges: New York midnight; timestamps require timezone. Batching respects URL/page limits
and delivery slices; bounded optimization, no global HTTP minimum guarantee. CUSIPs stay exact.

## Lineage reuse

Reuse complete groups only with matching inputs, dependency/recorded-absence sets, compatible versions,
no pending pricing work or explicit metadata refresh. Receipts retain evidence requirements independently
of approval via existing fingerprints. Matching evidence is not renewed by coverage/approval changes.
Missing history approval republishes the completed decision through the journal without pricing decode or
correlation; until then, no acknowledgement. Already projected approval requires no group reopening.

Completed work hints are authoritative. Lost coverage permits one source-generation census; matches need
no lineage receipts. Metadata prefetch bounds dependency members to 1,000. Rule-only changes may reuse
approved metadata, but recalculate affected assignments.

Ordinary actions resume from prior active month; older corrections require explicit metadata refresh.
Known dependency catch-up preserves continuation and accepted refresh obligations; new dependencies require
full evidence. Per-invocation, per-symbol containing completed action intervals satisfy filtered subsets.
Failed/incomplete responses establish no coverage; wider ranges/new invocations acquire evidence. Exact CUSIP
semantics remain independent. Group expansion reselects under enlarged locks; decoded inputs reuse only
matching generation/scope. Closed assignments require stricter continuity/single-identity gates:
[resume.py](../libs/alpaca_lineage_build/resume.py).

Exact rejected auxiliary pricing ≠ empty history. A missing auxiliary may remain metadata-only while supported
histories publish; arbitrary failure, seeds and retained series cannot use this outcome. Owning group records
rejected request, metadata and obligation ID before acknowledgement; projection permits interrupted reuse
without acquisition. [Absence owner](../libs/alpaca_lineage_build/acquisition.py).

## Publication and recovery

Stable per-ticker `.download.lock` flock inodes: exclusive producers/shared readers. One lineage worker holds
`.worker.lock`; sorted pricing locks + brief publication gate protect groups. Readers select → lock → recheck
before consuming; unrelated groups may proceed.

Close temporary files → atomic replacement under journal. Payloads precede receipts/index; summary refresh
precedes acknowledgement; journal clears last. Pricing hands new source/endpoints directly to summary/caller;
retain unchanged historical references, inventory only replaced boundaries, reuse already selected documents.
Recovery reconstructs durable inputs without in-memory handoff. Acknowledgement cannot consume newer notices/
hints. Interrupted multi-file work blocks readers until recovery. Owner cleans abandoned unpublished staging
under ticker lock.

[SQLite](../libs/common/sqlite_connections.py): transactions, disk rollback journals, write `synchronous=OFF`.
Producers permit native transaction recovery; consumers read-only. No forced publication synchronization.
Contract covers process interruption, not machine/power failure; SQLite owns internal recovery.

## Identity and catalogue

Row ownership ≠ UUID membership. Every row correlated or explicitly uncorrelated; dependency/successor alone
cannot merge securities. Public IDs derive from sorted unique provider UUIDs, not tickers. Ambiguity remains
unresolved; independently supported portions may publish.

Combined directories: `end_of_day/<SYMBOL>/`, `validation/`, `lineage/`, `correlation/`, `warnings/`,
`availability/`, `catalogue/`. `.publication.lock`: whole write transaction; `.publication.pending.json`:
blocks interrupted reads. [Monthly owners](../libs/alpaca_end_of_day_v6/) define receipts/generations.
Availability checks advertised reads/explained continuity; warnings advisory. `DataLibrary` exposes only online IDs.

## Parquet conversion

[Converter](../libs/alpaca_bloodhound_export/): complete resolved eight-view histories → fully adjusted OHLC,
raw close/volume, jointly inferred split factor. Assumptions/full public-ID mappings: Parquet footer
`alpaca_bloodhound`. Schema compatibility ≠ independent corporate-action certification.

Convert full histories before filtering. Ambiguous labels, incompatible factors or ID collisions fail.
Retain output within RAM allowance → serialize once → check source freshness → atomic install. No spill or
diagnostic sidecar. Exact schema/model/arithmetic:
[command_line.py](../libs/alpaca_bloodhound_export/command_line.py),
[factors.py](../libs/alpaca_bloodhound_export/factors.py),
[projection.py](../libs/alpaca_bloodhound_export/projection.py).
