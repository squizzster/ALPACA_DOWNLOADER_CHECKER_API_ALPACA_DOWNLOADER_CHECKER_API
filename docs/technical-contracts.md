# Technical contracts

## Trust and ownership

Each boundary calculates and validates its own output, then publishes its selecting receipt last.
Downstream `load_*` paths check selection and input identity; they consume approved results without
re-hashing every payload or repeating upstream rules. `authenticate_*` is explicit audit work.
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
| `end_of_day/lineage/index.sqlite3` | Published dependency/group index. |
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

## API plan

[build_plan](../libs/alpaca_data_api_solver/__init__.py) is pure:
`{"steps":[{"needs":[...]}]}` becomes versioned ordered steps containing actions.
Each need has `kind`, nonempty `why` and:

| Kind | Fields |
|---|---|
| `bars` | `symbols`, `adjustments`, `asof`, `ranges` with `start`/`end_exclusive`. |
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
