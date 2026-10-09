# Technical contracts

## Authoritative sources

| Contract | Source |
|---|---|
| Request defaults and adjustment labels | [`download_alpaca_end_of_day_bars/settings.py`](../libs/download_alpaca_end_of_day_bars/settings.py) |
| HTTP, pagination, retries, and rate limits | [`download_alpaca_end_of_day_bars/alpaca_http.py`](../libs/download_alpaca_end_of_day_bars/alpaca_http.py) |
| Frozen columns and request identity | [`alpaca_end_of_day_pipeline/contracts.py`](../libs/alpaca_end_of_day_pipeline/contracts.py) |
| Hard validation | [`alpaca_end_of_day_pipeline/daily_bar_validation.py`](../libs/alpaca_end_of_day_pipeline/daily_bar_validation.py) |
| Cache and frozen-file authentication | [`alpaca_end_of_day_pipeline/cached_downloads.py`](../libs/alpaca_end_of_day_pipeline/cached_downloads.py) |
| Submitted-to-authoritative symbol resolution | [`alpaca_end_of_day_ticker_pipeline/orchestration.py`](../libs/alpaca_end_of_day_ticker_pipeline/orchestration.py) |
| Asset and corporate-action evidence | [`alpaca_security_lineage/lineage.py`](../libs/alpaca_security_lineage/lineage.py) |
| Ownership and row-correlation rules | [`ownership_interval_rules.py`](../libs/alpaca_end_of_day_security_correlation/ownership_interval_rules.py), [`row_correlation.py`](../libs/alpaca_end_of_day_security_correlation/row_correlation.py) |
| Master-ID derivation and terminal fallback | [`security_identity.py`](../libs/alpaca_end_of_day_security_correlation/security_identity.py), [`terminal_fallback.py`](../libs/alpaca_end_of_day_security_correlation/terminal_fallback.py) |
| Master index | [`security_master.py`](../libs/alpaca_end_of_day_security_correlation/security_master.py) |
| Warning rules | [`cross_adjustment_warnings.py`](../libs/alpaca_end_of_day_warning_pipeline/cross_adjustment_warnings.py), [`security_segment_statistics.py`](../libs/alpaca_end_of_day_warning_pipeline/security_segment_statistics.py) |
| Public read result | [`alpaca_end_of_day_reader/history.py`](../libs/alpaca_end_of_day_reader/history.py) |
| Market sessions | [`common/us_market_hours.json`](../libs/common/us_market_hours.json) |

## Symbol authority

The complete ticker pipeline performs an exact Trading API asset lookup before its download stage. A successful lookup
must contain a valid US-equity asset ID and symbol. Alpaca's returned symbol then controls the request identity,
per-ticker directory, lineage query, correlation source, warning outputs, and security-master source. This includes
punctuation that may be absent from the submitted alias, such as `BFA -> BF.A` or `BRKB -> BRK.B`.

The correlation receipt retains a compact copy of that lookup under `lineage_report.symbol_resolution`, including the
submitted symbol, authoritative symbol, HTTP status, change flag, and asset evidence. The correlation generation
digest binds this report. An exact 404 is also explicit evidence: it keeps the submitted symbol and lets downstream
lineage and disposition rules decide whether the symbol is historical, quarantined, or otherwise unresolved.

Only the authoritative symbol is a live per-ticker artifact and security-master source. The submitted alias is input
provenance, not a second market-data identity.

## Frozen source

- Request identity includes symbol, adjustment, start, end, `asof`, feed, and timeframe.
- Alpaca requests use SIP `1Day`, ascending order, a 10,000-bar limit, and reject any next-page token.
- Validated rows are serialized through `pigz -5`; no plain CSV is published.
- The gzip sidecar binds request identity, row coverage, file stat, and SHA-256.
- A direct response uses the `alpaca_download` boundary. A view inferred by the adaptive policy uses the distinct
  `empirical_adjustment_alias` boundary and binds its policy, pattern, direct observations, source-manifest digest, and
  hard-link relationship. Consumers authenticate both boundaries through the same cache interface.
- Byte-identical direct responses may also share an inode after both files and manifests authenticate. Their independent
  direct-response manifests remain in place; storage deduplication does not convert them into inferred views.
- Integrity failures do not alter or remove rows. Exact failures are stored in `integrity_failure.manifest.json`.
- Alpaca's accepted zero-activity representation requires one positive price repeated exactly across open, high, low,
  and close together with `volume = 0`, `trade_count = 0`, and `VWAP = 0`. These rows remain valid source and session
  observations but are omitted from numerical warning analysis without producing an error, warning, or information
  finding. Partial or contradictory matches remain hard integrity failures.
- Raw and price-only-adjusted views require positive integer volume and `trade_count <= volume`. Split-containing views
  instead require nonnegative integer volume and do not compare trade count with split-scaled volume: Alpaca scales and
  rounds volume for splits while retaining the unscaled count of trades, so valid adjusted volume may be zero or below
  trade count. Trade count itself remains a positive integer for every non-zero-activity bar.
- A missing expected session is not a pre-identity integrity failure. It is identity evidence for the terminal fallback
  and an advisory check after correlation.

## Master identity

```text
master_id = SHA-256("\n".join(sorted(unique_alpaca_asset_ids)))
```

Ticker, name, date, CUSIP, FIGI, corporate-action, and calendar evidence determine or describe membership; none changes
the digest for a fixed membership set.

The final continuity rule runs only after all earlier rules leave the complete source uncorrelated. It promotes a
history only when every criterion is exactly `true`:

- the only unresolved reason is ambiguous identity;
- exactly two stable Alpaca-ID candidates exist;
- both candidates use the requested ticker;
- market-calendar evaluation succeeds;
- no expected session is missing or unexpected.

Promotion retains both Alpaca IDs, the calendar proof, any identity-changing action evidence, and an unresolved-boundary
warning. Otherwise the rows remain uncorrelated.

An HTTP-404 asset lookup is preserved as `outcome: not_found` and `http_status: 404`; it is not represented as an
asset with a missing ID. When authenticated local bars exist but no Alpaca identity can be resolved, the rows remain in
the durable uncorrelated branch and receive a deterministic `market_data_series_id`. That ID identifies Alpaca's
symbol-keyed series only: `is_security_identity` is `false`, it never enters the security master, and it must not be
used to join ticker reuse.

Each correlation generation binds one symbol disposition:

| Classification | Data policy | Recurring batch action |
|---|---|---|
| `alpaca_asset_identity_available` | process | run |
| `historical_market_data_only` | retain symbol series without inventing identity | run |
| `just_skip` | just_skip | skip |
| `inactive_empty_asset_out_of_scope` | out_of_scope | skip |
| `otc_asset_out_of_scope` | out_of_scope | skip |
| `unsupported_empty_symbol` | quarantine | skip |
| `exchange_test_symbol` | exclude | skip |
| `unresolved_symbol_disposition` | investigate | run |

Empty-symbol quarantine requires an exact asset HTTP 404 plus complete, stat-consistent zero-row evidence for every
local download. Exchange-test classification uses an exact-symbol, dated Nasdaq directory snapshot; it is never
inferred from a ticker pattern.

`just_skip` applies when the exact input symbol returns asset HTTP 404, corporate-action/CUSIP evidence resolves
exactly one occupant to an Alpaca asset ID, and all eight authenticated SIP/1Day adjustment views are present and
empty. The retained lineage explains the obsolete symbol; recurring batches skip it without deleting its frozen
evidence.

OTC scope exclusion likewise never uses ticker spelling. It requires an exact successful Alpaca input-asset lookup
whose returned exchange is `OTC`, plus complete, stat-consistent zero-row evidence from the configured SIP/1Day
downloads. The frozen empty downloads and their integrity records are retained for audit, while recurring file batches
skip the ticker unless explicitly overridden.

Inactive-asset scope exclusion requires an exact successful Alpaca input-asset lookup with `status: inactive` and
`tradable: false`, plus complete, stat-consistent zero-row SIP/1Day evidence for all eight adjustment views. Inactivity
alone is insufficient: an incomplete view set or an inactive asset with any retained historical bars remains in scope.

## Artifacts

Per ticker: `data/end_of_day/<SYMBOL>/`

| Artifact | Contract |
|---|---|
| `<adjustment>.csv.gz` | Frozen direct rows or a byte-identical hard-linked empirical view |
| `<adjustment>.csv.gz.manifest.json` | Direct-source receipt or explicit empirical-alias evidence |
| `pipeline.manifest.json` | Current validation generation and processing receipts |
| `integrity_failure.manifest.json` | Current hard failures; present with `failures: []` when clean |
| `correlated.pipeline.json` | Master-ID-assigned row ranges with exact Alpaca provenance |
| `uncorrelated.pipeline.json` | Unassigned row ranges and lineage work items |
| `security_correlation.manifest.json` | Digest binding both exhaustive branches |
| `cross_adjustment_warnings.pipeline.json` | Comparable-view findings |
| `security_segment_statistical_warnings.pipeline.json` | Master-ID history findings |
| `warning_pipeline.manifest.json` | Digest binding both warning outputs and upstream generations |

Global: `data/end_of_day/json_manifests/`

| Artifact | Contract |
|---|---|
| `security_master.index.json` | Small generation root binding the source and master-ID inventory roots, their independent counts, and no embedded identity payloads. |
| `security_master.manifest.json` | Index stat, SHA-256, version, generation, source count, and master-ID count. |
| `objects/sources/<bucket>/<symbol>.<sha256>.json` | Immutable per-symbol correlation contribution, including one computed availability state per contributed master ID. |
| `objects/securities/<bucket>/<master-id>.<sha256>.json` | Immutable aggregate identity selected directly by master ID, including its persisted online boolean and compact availability classification. |
| `inventories/{sources,securities}/buckets/<bucket>/<sha256>.json` | Canonical object descriptors in a two-hex-character SHA-256 key bucket. |
| `inventories/{sources,securities}/roots/<sha256>.json` | Canonical bucket descriptors and the applicable source or master-ID count. |

`source_count` counts symbol-source objects; `master_id_count` counts unique aggregate master-ID objects. They are not
expected to be equal. The generation digest commits to both inventory roots. Updating one symbol publishes its new
immutable source object, only affected master-ID objects and buckets, then the small root and receipt; unchanged shards
are reused.

The authenticated per-master-ID object is the availability authority. Its `online` field is true only when the
completed ticker publication established all conditions: the master ID is published, bars exist, current dependencies
authenticate, every advertised adjustment returns through the public reader without integrity errors or cross-source
conflicts, and no expected US market session is absent strictly between the first and last returned bars. Online means
zero unresolved boundaries. A history which otherwise passes but has that internal boundary is offline with
`availability_status: "lineage_gap"`. It remains readable for later investigation. Every other state, including a
well-formed ID absent from the inventory, is offline. This internal state is independent of provider active/inactive
metadata.
There is no parallel availability list or status database; an atomic ticker update changes only its source object,
affected master-ID objects and inventory buckets, and the small roots.

Runtime `data/` is ignored by Git.

## Reader

Input is one published 64-hex master ID and one exact adjustment label. `all` is Alpaca's all-actions-adjusted view,
not a wildcard.

The reader:

1. authenticates the small master root, the selected master-ID inventory bucket, and its immutable identity object;
2. authenticates the identity against its selected per-symbol source objects, then authenticates only the correlation
   sources and digests named by that identity;
3. authenticates applicable warning, validation, and frozen-download boundaries;
4. returns one JSON-compatible structure without writing or contacting Alpaca.

The provider-neutral availability facade supports a cheaper question without reading bars:

```python
from libs.alpaca_end_of_day_reader import (
    get_alpaca_end_of_day_security_availability,
    list_alpaca_end_of_day_master_ids_by_availability_status,
    list_online_alpaca_end_of_day_master_ids,
)

status = get_alpaca_end_of_day_security_availability("<64-hex-master-id>")
online_master_ids = list_online_alpaca_end_of_day_master_ids()
lineage_gap_master_ids = list_alpaca_end_of_day_master_ids_by_availability_status("lineage_gap")
```

`status` contains `master_id`, the stored `online` boolean, and `availability_status`. The list calls authenticate
identity shards and filter persisted fields rather than re-running bar, correlation, calendar, or warning checks for
every consumer.

`bars` preserves frozen CSV field strings. Each bar includes source/row/segment provenance plus referenced integrity
and warning IDs. Equal duplicate observations are coalesced with all provenance; conflicting values at one timestamp
abort. Full diagnostics remain in their top-level sections. `security` is the clean master-index record.

Until a repair owner exists, returned values come from authenticated frozen downloads and both repair fields are
`null`.

## Authentication meanings

- Empty integrity failures: no current hard failure recorded.
- Complete partition: every source row is in one branch.
- Zero uncorrelated rows: every source row has an identity assignment.
- Warning/information findings: advisory only.
- Successful update: stages completed; a read still authenticates its selected dependency chain independently.

Version sources and required changelogs are indexed in [Development](development.md).
