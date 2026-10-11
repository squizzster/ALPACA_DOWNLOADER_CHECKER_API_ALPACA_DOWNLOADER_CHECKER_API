# Technical contracts

This document owns the durable data, identity, publication, and authentication contracts. Consumer types and methods
belong in [Public API](public-api.md); commands and mutation hazards belong in [Operations](operations.md).

## Authoritative implementation

| Contract | Source |
|---|---|
| Request defaults and adjustment labels | [`settings.py`](../libs/download_alpaca_end_of_day_bars/settings.py) |
| HTTP, retry, pagination, and rate limiting | [`alpaca_http.py`](../libs/download_alpaca_end_of_day_bars/alpaca_http.py) |
| Frozen columns and request identity | [`contracts.py`](../libs/alpaca_end_of_day_pipeline/contracts.py) |
| Hard row and dataset validation | [`daily_bar_validation.py`](../libs/alpaca_end_of_day_pipeline/daily_bar_validation.py) |
| Cache and frozen-file authentication | [`cached_downloads.py`](../libs/alpaca_end_of_day_pipeline/cached_downloads.py) |
| Update orchestration and symbol authority | [`orchestration.py`](../libs/alpaca_end_of_day_ticker_pipeline/orchestration.py) |
| Asset and corporate-action evidence | [`lineage.py`](../libs/alpaca_security_lineage/lineage.py) |
| Symbol disposition | [`symbol_disposition.py`](../libs/alpaca_security_lineage/symbol_disposition.py) |
| Ownership and ordered row rules | [`ownership_interval_rules.py`](../libs/alpaca_end_of_day_security_correlation/ownership_interval_rules.py), [`row_correlation.py`](../libs/alpaca_end_of_day_security_correlation/row_correlation.py) |
| Long-gap suffix rules | [`ticker_reuse_gap.py`](../libs/alpaca_end_of_day_security_correlation/ticker_reuse_gap.py) |
| Reused current-symbol rule | [`current_symbol_reuse.py`](../libs/alpaca_end_of_day_security_correlation/current_symbol_reuse.py) |
| One-session transition and episode partition | [`identity_partition.py`](../libs/alpaca_end_of_day_security_correlation/identity_partition.py), [`episode_partition.py`](../libs/alpaca_end_of_day_security_correlation/episode_partition.py) |
| Verified market-session exclusions | [`session_exclusions.py`](../libs/alpaca_end_of_day_security_correlation/session_exclusions.py) |
| Master-ID derivation and terminal fallback | [`security_identity.py`](../libs/alpaca_end_of_day_security_correlation/security_identity.py), [`terminal_fallback.py`](../libs/alpaca_end_of_day_security_correlation/terminal_fallback.py) |
| Sharded master catalogue | [`security_master.py`](../libs/alpaca_end_of_day_security_correlation/security_master.py) |
| Final online evaluation | [`online_status.py`](../libs/alpaca_end_of_day_ticker_pipeline/online_status.py) |
| Warning rules | [`cross_adjustment_warnings.py`](../libs/alpaca_end_of_day_warning_pipeline/cross_adjustment_warnings.py), [`security_segment_statistics.py`](../libs/alpaca_end_of_day_warning_pipeline/security_segment_statistics.py) |
| Public consumer projection | [`alpaca_end_of_day/__init__.py`](../alpaca_end_of_day/__init__.py) |
| Detailed authenticated read | [`history.py`](../libs/alpaca_end_of_day_reader/history.py) |
| Tracked US market sessions | [`us_market_hours.json`](../libs/common/us_market_hours.json) |

Code and authenticated schemas win if prose becomes stale. A behavioral or serialized change must update the owning
version and changelog described in [Development](development.md#versioned-boundaries).

## Request and symbol authority

Daily-bar requests use SIP, `1Day`, ascending order, a 10,000-row limit, and the eight exact adjustment labels defined
by `ALPACA_ADJUSTMENT_MODES`. The historical start is `2016-01-01T00:00:00Z`; the end is an exclusive UTC boundary.
The HTTP client rejects a next-page token instead of silently returning a partial history.

The complete ticker pipeline performs an exact Trading API asset lookup before choosing the requested symbol. A
successful US-equity lookup makes Alpaca's returned symbol authoritative for downloads, directories, lineage,
correlation, warnings, and master publication. This preserves punctuation such as `BFA -> BF.A`. The submitted alias
remains provenance and never becomes a second data identity.

An exact asset HTTP 404 remains explicit `not_found` evidence. It does not fabricate an asset with a missing UUID.
Later disposition rules decide whether the symbol is historical-only, a justified recurring skip, quarantined, or
unresolved.

## Frozen-source boundary

Request identity includes symbol, adjustment, start, exclusive end, `asof`, feed, and timeframe. A coherent cached pair
must match that identity and authenticate before reuse.

- Validated rows are serialized through `pigz -5`; no plain CSV is published.
- The gzip sidecar binds request identity, coverage, file stat, and SHA-256.
- A direct response uses the `alpaca_download` boundary. An inferred adaptive view uses the distinct
  `empirical_adjustment_alias` boundary and binds its policy, direct observations, source-manifest digest, and shared
  inode.
- Byte-identical direct responses may share an inode after every file and sidecar authenticates. Their independent
  direct-response manifests remain intact.
- Outputs are written before digest-binding receipts. Missing, stale, or changed inputs fail closed.
- Integrity failures preserve every source row and record exact findings in `integrity_failure.manifest.json`; they do
  not rewrite prices.

A valid zero-activity bar has one positive price repeated exactly across OHLC and numerical zeros for volume, trade
count, and VWAP. Partial or contradictory states are hard failures.

Raw and price-only-adjusted views require positive integer volume and `trade_count <= volume`. Split-containing views
allow nonnegative integer volume and do not compare it with the unscaled trade count because Alpaca scales and rounds
split volume. Every active bar still requires a positive integer trade count.

A missing expected session is not a pre-identity corruption finding. Identity and venue context are required before the
system can distinguish a data gap, a ticker boundary, or a verified non-trading period. Duplicate, out-of-order, or
closed-session observations remain hard dataset failures.

## Identity model

The system keeps three distinct concepts:

- **Corporate lineage** describes company or transaction relationships.
- **Security identity** describes the actual shares, anchored by authenticated Alpaca asset UUIDs and dated evidence.
- **Pricing episode** describes a continuous period of comparable, verified daily observations.

A corporate action may relate two securities without making their prices one continuous history. A ticker may be
reused by unrelated securities. CUSIP, names, exchanges, corporate actions, and dates provide evidence; none alone is a
permanent identity.

The master/public ID is deterministic for resolved membership:

```text
master_id = SHA-256("\n".join(sorted(unique_alpaca_asset_ids)))
```

Ticker, name, CUSIP, FIGI, date, calendar, and action evidence determine or describe membership but do not alter the
digest for a fixed Alpaca-ID set. A UUID-less retained symbol series receives a deterministic
`market_data_series_id` with `is_security_identity: false`; it never enters the master catalogue and must never be used
to join ticker reuse.

## Correlation and episode rules

Every authenticated source row must appear exactly once in the correlated or uncorrelated output. `ROW_CORRELATION_RULES`
in [`row_correlation.py`](../libs/alpaca_end_of_day_security_correlation/row_correlation.py) is the execution-order
authority. Earlier rules may deliberately prevent a later generic fallback from joining an unresolved boundary.

Rules describe evidence shapes, never named tickers. The current consequential shapes are:

- A gap of at least **60 expected US market sessions** is a ticker-reuse candidate. The guard keeps both sides
  uncorrelated unless a preceding episode rule proves the applicable side's identity.
- After at least **100 missing sessions**, a complete rightmost run can use the exact current-symbol UUID, non-empty
  name, and exchange when authenticated adjustment grids agree and the same UUID has no dated evidence on the left.
  Missing CUSIP alone does not veto this rule; conflicting CUSIPs or UUID evidence do.
- The same rightmost shape may end before the request boundary when its exact asset status is `inactive` and its
  observed grid is internally complete. Exact `active` status flags the incomplete suffix and leaves it offline.
- A dated terminal rightmost run can use a different UUID when an exact CUSIP lookup and unambiguous ownership interval
  identify it, the run is complete to the terminal boundary, and that UUID has no evidence on the left.
- One absent expected session can separate an action-linked old ticker and new ticker when each resolves to exactly one
  different UUID, no third UUID competes, the action date matches the boundary, and all eight adjustment grids agree.
- Action-linked UUIDs remain separate pricing identities when each UUID has one complete, non-overlapping continuous
  run in all eight views. A recurring UUID, overlap, missing view, undated assignment, or unresolved edge rejects the
  partition.
- A current symbol reused after its old occupant renamed away can publish its complete new run after at least **100
  vacant sessions**. This requires exact different old/new UUIDs and CUSIPs, exact current name and exchange, complete
  corporate-action coverage, all eight matching adjustment grids, dated in-run evidence for the new identity, and no
  in-run structural action or unresolved evidence.

Long-gap suffix rules allocate pricing rows to a proven UUID; they do not partition that UUID from authenticated
predecessor or successor UUID lineage. UUID-lineage edges are suppressed only when the row carries explicit
`identity_partition` evidence from a one-session transition, distinct continuous UUID episodes, or authenticated
current-symbol reuse. Retained edges can add a predecessor or successor UUID to membership, or preserve another ticker
carried by the same UUID. This keeps ticker-occupancy allocation separate from security-membership partitioning.

The current-symbol suffix rules leave the earlier episode uncorrelated. If the same Alpaca UUID has evidence on both
sides of a long gap, the suffix is not promoted; both sides remain unresolved until a different rule can prove the
boundary.

The terminal continuous-ticker fallback runs only after earlier rules leave the complete source uncorrelated. It can
promote a stable, non-empty set of same-ticker candidates only when exactly one current symbol lookup anchors that set,
all candidate names agree, ambiguous identity is the sole reason, the market calendar evaluates successfully, and no
expected or unexpected session remains. It records the candidate set, calendar proof, and an unresolved-boundary
warning.

## Session exclusions and continuity

A missing session is resolved only by evidence bound to the same identity and data generation.

A verified full-session suspension records exact UUID, CUSIP, exchange, halt/resume timestamps, market-calendar digest,
session digest, and SHA-256-authenticated primary-source material. Every missing session must fall inside the verified
window, and no positive-activity raw bar may occur in it.

Final online evaluation also recognizes a gap of at most **five expected sessions** at a same-UUID, same-CUSIP ticker
name change when all advertised adjustment grids agree, the exact action and symbol lookups agree, raw sources contain
no bar in the gap, and no competing structural event or identity exists. This is a continuity exclusion at the
already-resolved master identity, not a generic license to fill missing data.

## Symbol disposition

Each correlation generation binds one evidence-derived disposition:

| Classification | Data policy | Recurring file-batch action |
|---|---|---|
| `alpaca_asset_identity_available` | process | run |
| `historical_market_data_only` | retain the symbol series without inventing identity | run |
| `just_skip` | retain exact historical-alias evidence | skip |
| `inactive_empty_asset_out_of_scope` | out of scope | skip |
| `otc_asset_out_of_scope` | out of scope | skip |
| `unsupported_empty_symbol` | quarantine | skip |
| `exchange_test_symbol` | exclude | skip |
| `unresolved_symbol_disposition` | investigate | run |

Recurring skips authenticate the complete prior correlation partition and frozen sources. Explicit `--symbol` runs
always re-evaluate. Classifications depend on exact HTTP outcomes, asset fields, dated directory evidence, and complete
all-eight zero-row evidence where required; no ticker spelling pattern establishes one.

## Availability gate

The authenticated per-master-ID shard is the availability authority. Final evaluation exercises every advertised
adjustment through the detailed reader, then persists one `online` boolean and `availability_status`.

```text
online = published master ID
       + at least one returned bar
       + current authenticated dependencies
       + every advertised adjustment reads successfully
       + zero integrity errors or unresolved source conflicts
       + valid market sessions
       + zero unexplained internal session or identity boundaries
```

A reader-ready history with an unexplained expected session strictly between its first and last bar is offline with
`availability_status: "lineage_gap"`. Other failures use `availability_status: "offline"`. Provider `active`,
`inactive`, or `tradable` metadata is evidence for specific identity rules and never the public availability result.

The public library filters on `online is True` and exposes neither classification nor offline metadata. The internal
reader can still open published offline histories for operator investigation.

## Tracked resources

[`resources/`](../resources/) belongs to the source checkout and contains two kinds of machine-read input:

- six JSON changelog arrays, each validated against its owning version constant; and
- [`ticker.csv`](../resources/ticker.csv), a one-record recovery snapshot of sorted source symbols.

The changelogs are exact version histories, not generated logs. Their array positions are their versions, and a file
missing from its configured path makes the corresponding validator fail. `ticker.csv` contains no bars, identity
membership, availability, or per-ticker validation state; it can seed a new acquisition but cannot restore an
authenticated generation. Version workflow belongs in [Development](development.md#versioned-boundaries), and
recovery belongs in [Operations](operations.md#backup-and-recovery).

## Runtime artifact layout

Per source symbol under `data/end_of_day/<SYMBOL>/`:

| Artifact | Contract |
|---|---|
| `<adjustment>.csv.gz` | Frozen direct rows or byte-identical hard-linked empirical view. |
| `<adjustment>.csv.gz.manifest.json` | Direct-source receipt or explicit empirical-alias evidence. |
| `pipeline.manifest.json` | Current validation generation and processing receipts. |
| `integrity_failure.manifest.json` | Current hard failures; present with `failures: []` when clean. |
| `correlated.pipeline.json` | Master-ID-assigned row ranges with exact provenance. |
| `uncorrelated.pipeline.json` | Unassigned ranges and work items. |
| `security_correlation.manifest.json` | Digest binding both exhaustive branches and lineage evidence. |
| `verified_session_exclusions.json` | Optional authenticated venue-session evidence. |
| `cross_adjustment_warnings.pipeline.json` | Comparable-view findings. |
| `security_segment_statistical_warnings.pipeline.json` | Master-ID history findings. |
| `warning_pipeline.manifest.json` | Digest binding warning outputs and upstream generations. |

Global catalogue under `data/end_of_day/json_manifests/`:

| Artifact | Contract |
|---|---|
| `security_master.index.json` | Small generation root binding source and security inventory roots and independent counts. |
| `security_master.manifest.json` | Index stat, SHA-256, version, generation, source count, and master-ID count. |
| `objects/sources/<bucket>/<symbol>.<sha256>.json` | Immutable symbol contribution and availability per contributed ID. |
| `objects/securities/<bucket>/<master-id>.<sha256>.json` | Immutable aggregate identity selected directly by master/public ID. |
| `inventories/{sources,securities}/buckets/<bucket>/<sha256>.json` | Canonical object descriptors in two-hex SHA-256 key buckets. |
| `inventories/{sources,securities}/roots/<sha256>.json` | Bucket descriptors and the applicable object count. |

`source_count` and `master_id_count` measure different object sets and need not match. Updating one symbol writes its
new immutable source object, affected security objects and buckets, then the small roots and receipt. Unchanged shards
are reused. Runtime `data/` is ignored by Git.

### Read-only catalogue audit

[`audit_alpaca_end_of_day_catalog.py`](../audit_alpaca_end_of_day_catalog.py) materializes and authenticates every source
and identity object in one catalogue generation before applying catalogue-wide checks. In addition to the per-object
authentication rules above, the audit requires the source contribution graph to close exactly over the published
identities and treats an Alpaca asset UUID appearing in more than one master-ID membership set as an integrity error.
Master-ID membership represents identity equivalence classes, so those sets must be disjoint even when one set is a
subset of another.

An open-ended active ticker claim shared by multiple online master IDs is reported separately as an investigative
warning. Ticker ambiguity remains explicit rather than being resolved by choosing an ID. The command checks that the
catalogue generation is unchanged at the end of the scan and never publishes, repairs, downloads, or contacts Alpaca.
Its report is an operator diagnostic, not a persisted catalogue artifact or public API schema.

## Internal authenticated reader

The internal reader accepts one published 64-hex master ID and one exact adjustment label. It authenticates the master
root, selected inventory bucket and identity object, bound source objects, correlation branches, warnings, validation
receipts, and frozen downloads. It performs no writes or network requests.

Equal duplicate observations are coalesced with all provenance. For adjusted values, a differing source may be selected
only by the strict-superset coverage rule; an otherwise unresolved disagreement may use exactly one source requested
under the dated historical ticker for that row. Raw disagreement and every remaining ambiguity are hard read errors.

Detailed results preserve stored field strings, source rows, segment provenance, applicable warnings, and integrity
findings. Until a repair owner exists, `repair.overlay` and `repair.repaired_data` remain `null`. The public library then
projects only online identity and exact typed bar fields from this detailed result.
