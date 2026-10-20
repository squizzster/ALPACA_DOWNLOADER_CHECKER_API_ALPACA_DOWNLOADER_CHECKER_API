# Technical contracts

## Trust and ownership

Each boundary calculates and validates its own output, then publishes its selecting receipt last.
Completed receipts are authoritative from first consumption. Readers open only the data needed for the
work, without existence/symlink inventories, file-stat cache keys, payload hashing or repeated upstream rules.
After reacquiring locks, read the current selector to detect a newer generation; do not audit its files.
Publication compares new outputs with recorded digests and validates new destinations and outputs.
Recovery handles its journal; `authenticate_*` explicitly audits existing data. Forced repair replaces supplied payloads.
Matching complete input generations permit reuse; changed inputs invalidate their dependent closure.
Readers refuse incomplete publications and never repair them.

## Standalone artifacts

Paths below are relative to the standalone store.

| Artifact | Meaning |
|---|---|
| `store.manifest.json` | Store format. |
| `end_of_day/pricing/<TICKER>/source.manifest.json` | Selects compact pricing boundaries and validator. |
| `validator_first_last_sessions.json` beside the source | Exact first/last rows for each view, bound to the source receipt. |
| Pricing `completed_years`, current-year and current-month boundaries | CSV/gzip payloads and selecting manifests. |
| `end_of_day/manifests/<TICKER>/update.json` | Unconsumed inclusive changed range: `from`, `to`. |
| `end_of_day/lineage/<TICKER>/lineage.manifest.json` | Evidence, input generations and chronological assignment selection. |
| `end_of_day/lineage/index.sqlite3` | Published groups/generations and lookup-evidence projection. |
| `universe_updates.sqlite3` | Accepted observations, registration intentions, work targets, attempts and outcomes. |
| `pricing_check_summary.sqlite3` | Minimal live-check baseline. |
| `lineage_update_check.sqlite3`, `download_completion.json` | Disposable scheduling hints, not data authority. |

Effective pricing and assignments use **completed years + year excluding active month + authoritative
month**. Rollover consolidates; ordinary month updates leave cold payloads unchanged.
Manifests map eight logical views to distinct physical payloads; exact-copy aliases share files.

Pricing columns are `timestamp,open,high,low,close,volume,trade_count,vwap`.
JSON decimals/integers become exact strings without binary-float arithmetic.
Wrong symbols, malformed values, duplicate/out-of-range timestamps and incomplete pagination fail.
The downloader checks source structure, not market validity or identity.

Ordinary updates compare first/last endpoints in all views; a mismatch selects full replacement.
The integrated live check compares only the summary's last `all` row.
Any differing field requires replacement of all views over the retained span before append.
Full replacement notices include removed sessions; append notices cover changed dates.
Append approval carries the checked source generations for the selected missing intervals. Publication
rechecks those generations under ticker locks, including when a concurrent edit preserves the last row.
The summary remains minimal; approval is an in-memory handoff. New summary membership never expands an
existing approval. Empty append baselines require all eight retained views to be empty; partial views use
the ordinary downloader's history checks.
Buffered initial and invalidated acquisitions carry the selected source generation, including explicit
absence, into publication. Under the ticker lock, the pricing owner first finishes any journal and then
compares the current source with that selection. A conflict rejects the buffered replacement and requires
reselection; it cannot overwrite a concurrent publication. Retained-span refresh also preserves its
original start, end and symbol-mapping date.
See [pricing contracts](../libs/alpaca_pricing_download/contract.py) and
[publication](../libs/alpaca_pricing_download/storage.py) for exact schemas.

## Check summary

Each entry contains only `symbol`, saved `asof` and `last_all`: the exact eight-field last row.
Eligibility means a unique stored same-symbol, active US-equity lookup and a nonempty `all` endpoint.
This trusts stored status; active does not mean publicly online.

Session date derives from the timestamp. Date/asof pairs define compatible live-check groups.
The only additional state is publication control: format, epoch/revision/coverage and pending owner markers.
No first rows, other views, identities, histories or previous comparison results belong here.

Pricing maintains eligible baselines; lineage maintains membership. Separate owner markers prevent
one publisher from clearing another's incomplete work.
Missing/incomplete/pending summaries fail closed. A comparison must verify its snapshot token before
accepting results. Normal summary reads do not enumerate ticker files.
Owners: [storage](../libs/alpaca_pricing_download/check_summary.py) and
[eligibility/rebuild](../libs/alpaca_lineage_build/check_summary.py).

## Universe observation and work state

The recovery registry remembers accepted labels, not security identity or successful acquisition.
A stable shared lock protects sorted union publication; different stores cannot lose each other's additions.
Each clean-room build owns its input snapshot. Pinning an older input preserves its original run identity
and producer contracts rather than upgrading past work.
Labels, hashes and snapshot bytes come from one captured byte sequence. The combined store publication
lock serializes run initialization and pinning. Before publishing a new snapshot, initialization records
its original manifest and input bytes in `clean_room_run/initialization.pending.json`. New run manifests
publish last; pinning leaves existing manifests unchanged. The journal then clears. Recovery finishes
those original inputs before checking the invocation's target and contract compatibility. Registry growth
cannot change an interrupted run's input.

One catalogue observation retains all records, including multiple records for a label. Active records on
the discovery exchanges introduce work. Unique symbol records may supply current metadata; duplicate or
missing catalogue evidence uses exact lookups. CUSIP needs remain exact. Catalogue absence supplies no
negative identity or pricing conclusion.

The lineage publisher writes the exact symbol-lookup facts used by its receipts into its existing index,
including relevant dependency evidence, coverage, row outcomes and selected receipt/source generations.
New receipts also retain the last positive own-label asset lookup for maintenance selection. A later
missing lookup does not establish inactivity. Current identity decisions and summary eligibility continue
to use the current evidence, independently of that retained observation.
Projection completeness has its own schema/revision/coverage state. Explicit migration reads indexed
receipts only in resumable batches; absent historical facts stay unknown. Normal comparison uses the
projection and refreshes affected complete groups, invalidating both evidence and decision reuse.

Orchestration transactions persist observation, registration intention and work before publishing the
registry union, then acknowledge registration. Pricing and lineage selectors remain the authorities for
publication; the work database cannot independently declare them published. A completed publication can
satisfy interrupted acknowledgement without repeating acquisition. Accepted work carries a stable
obligation identifier, refresh/replacement requirements, evidence and target. A lineage publication projects
that identifier only after its locked pricing inputs satisfy the obligation. Coverage alone cannot satisfy
an unperformed refresh. Maintenance persists any additional coverage before requests begin. An unfinished
pricing target survives independently of acquisition/check flags, including later inactivity, catalogue
omission or an older invocation. A newly observed inactive label can require metadata reconciliation alone;
that does not cancel a previously accepted pricing target. Failed checks also create obligations, and a
subsequent history check must cover the selected generation before completion. Pricing source selectors record the
acquisition-evidence fingerprint for identity-driven replacement, including when the returned bars do not
change. Stable store update locks serialize orchestration; existing publisher locks and generation checks
still apply.
Standalone producers hold that same update lock. Their registration context loads the registry once,
persists new intentions and acknowledges the atomic union before either existing-group catch-up or newly
discovered dependency acquisition. Dependency work records acquisition coverage and the complete required
group before registration or acquisition. Those links remain in work selection until satisfying publication,
even when later provider evidence omits the relationship; registry membership cannot satisfy them.
Pending registration intentions recover first; known membership
does not need republication. Explicit work-state initialization is independent of lineage/projection and
summary construction, so those producers can establish their boundaries without a circular prerequisite.
Accepted retained-span corrections also receive a durable obligation before acquisition. Their pricing
publication records the correction identifier; lineage projects it for completion and interrupted reuse.
A later matching endpoint cannot erase an unperformed correction. A completed correction does not need
another full download merely because lineage or acknowledgement was interrupted.
Access failures, retry delays and accepted cutoffs constrain selection metadata, acquisition, correction,
append and group expansion through daily and canonical standalone commands. Failure of a required member
prevents its sibling from publishing the affected identity group. Local interrupted registry publication
remains pending for immediate owner recovery; it does not invent a provider retry delay.
Exact-request restrictions discovered during metadata acquisition also survive through the durable outcome.

An identical completed pricing request is reusable even when empty. Advancing compatible empty coverage
requests only the additional interval. Relevant identity changes or explicit retained-span refresh may
invalidate coverage. Durable outcomes distinguish empty, published, unresolved, access denial, exact
rejection and retryable failure. Missing/incompatible durable state or incomplete projection fails closed
into explicit maintenance, never an empty queue or automatic receipt crawl.

Preview uses accepted compact boundaries without writes or recovery. Exact pages and dependency expansion
are response-dependent; estimates disclose those conditions. Execution reselects boundaries and observes
current catalogue evidence independently. Inclusive pricing targets become exclusive UTC timestamps;
newer retained spans are preserved and excluded from older-target validation/correction.

Offline status reads the lineage index/projection, pricing summary, scheduling controls and durable work
state through read-only connections. Pending symbols from the scheduling and summary boundaries select
the only per-ticker pricing-journal existence checks. The lineage publication marker is checked under its
existing shared publication lock. Status never opens pricing payloads or lineage receipts, scans ticker
directories or invokes recovery. Each boundary supplies its own coherent observation; the combined report
is not a transaction spanning all producers. See [status semantics](operations.md#inspect-local-progress).

## API plan

[build_plan](../libs/alpaca_data_api_solver/__init__.py) is pure:
`{"steps":[{"needs":[...]}]}` becomes versioned ordered steps containing actions.
Each need has `kind`, nonempty `why` and:

| Kind | Fields |
|---|---|
| `bars` | `symbols`, `adjustments`, `asof`, `ranges` with `start`/`end_exclusive`. |
| `asset_catalogue` | No extra fields; one complete all-status US-equity observation, filtered locally. |
| `asset_lookup` | `identifiers`; optional `lookup_kind`: symbol, cusip, asset_id or exact. |
| `corporate_actions` | `symbols`, inclusive `start`/`end` dates. |

Actions carry `strategy`, `why`, `request`, `deliver` and pagination instructions where needed.
Delivery routes results to the requested slices. Strategies are `bars_single_range`, `bars_multi_range`,
`bars_boundary_batch`, `asset_lookup`, `asset_catalogue` and `corporate_actions_batch`.

Needs within a step are independent; steps preserve prerequisites. Executors exhaust page tokens before
advancing. Data-dependent needs are constructed after receiving their prerequisite.
Bar requests fix 1Day/SIP/USD. Each adjustment requires a separate request; `asof` selects symbol mapping,
not historical adjustment vintage. Date-only solver ranges mean New York midnight; timestamp ranges
require a timezone. Batching respects URL/page limits and delivery slices; it is a bounded optimization,
not a promise of globally minimal HTTP calls. CUSIPs stay exact.

## Lineage reuse

Complete groups can be reused only when inputs, dependency/recorded-absence sets and compatible versions
match, with no pending pricing work or explicit metadata refresh.
Completed work hints are authoritative. If their coverage is lost, one source-generation census restores it;
matching entries do not open lineage receipts, and metadata prefetch is bounded to 1,000 dependency members.
Rule-only changes may reuse approved metadata; they still recalculate affected assignments.
Ordinary action acquisition resumes from the previously active month; older provider corrections need
explicit metadata refresh. Closed-assignment reuse has stricter continuity and single-identity gates in
[resume.py](../libs/alpaca_lineage_build/resume.py).

A recorded exact rejection of missing auxiliary pricing is distinct from a successful empty history.
It may remain metadata-only while independently supported pricing publishes; arbitrary failures do not
qualify. [acquisition.py](../libs/alpaca_lineage_build/acquisition.py) owns that decision.

## Publication and recovery

Per-ticker `.download.lock` files are stable flock inodes: exclusive producers, shared readers.
A single lineage worker holds `.worker.lock`; sorted pricing locks and a brief publication gate protect
each group. Readers select, lock and recheck the group before consumption. Unrelated groups can proceed.

Writers close temporary files and atomically replace selected files under a recovery journal.
Payloads precede receipts/index selection; summary refresh precedes acknowledgement; the journal clears last.
Acknowledgement cannot consume a newer notice or hint. Interrupted multi-file work blocks readers until
producer recovery completes. Unpublished abandoned staging is cleaned by its owner under the ticker lock.

[SQLite connections](../libs/common/sqlite_connections.py) retain transactions and disk rollback journals,
with `synchronous=OFF` for writes. Producers allow native interrupted-transaction recovery;
consumers open read-only. Ordinary publication performs no forced disk synchronization.
The contract covers process interruption, not machine or power failure; SQLite owns its internal recovery.

## Identity and catalogue

Row ownership and UUID membership are separate decisions. Every source row is correlated or explicitly
uncorrelated; a shared dependency or successor alone does not merge securities.
Public IDs derive from sorted unique provider-UUID membership, not ticker spelling.
Ambiguity remains unresolved; independently supported portions can still publish.

The combined store separates `end_of_day/<SYMBOL>/`, `validation/`, `lineage/`, `correlation/`,
`warnings/`, `availability/` and `catalogue/`. Its `.publication.lock` covers the whole write
transaction; `.publication.pending.json` blocks reads after interruption.
[Monthly boundary owners](../libs/alpaca_end_of_day_v6/) define their receipts and generations.
Availability checks advertised reads and explained continuity; warnings are advisory.
Only online identities cross the `DataLibrary` boundary.

## Parquet conversion

The [converter](../libs/alpaca_bloodhound_export/) consumes complete resolved eight-view histories.
It exports all-adjusted OHLC, raw close/volume, and a jointly inferred split factor.
Inference assumptions and full public-ID mappings stay in the Parquet footer under `alpaca_bloodhound`;
schema compatibility is not independent corporate-action certification.

Full histories pass conversion before date filtering. Ambiguous labels, incompatible factors and ID
collisions fail. Output is retained within the configured RAM allowance, serialized once, checked for
source freshness, then atomically installed. No disk spill or diagnostic sidecar is created.
The exact schema, factor model and arithmetic live in
[command_line.py](../libs/alpaca_bloodhound_export/command_line.py),
[factors.py](../libs/alpaca_bloodhound_export/factors.py) and
[projection.py](../libs/alpaca_bloodhound_export/projection.py).
