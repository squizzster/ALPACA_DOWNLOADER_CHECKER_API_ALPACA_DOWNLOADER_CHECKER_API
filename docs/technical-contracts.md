# Technical contracts

## Authoritative sources

| Contract | Source |
|---|---|
| Request defaults and adjustment labels | [`download_alpaca_end_of_day_bars/settings.py`](../libs/download_alpaca_end_of_day_bars/settings.py) |
| HTTP, pagination, retries, and rate limits | [`download_alpaca_end_of_day_bars/alpaca_http.py`](../libs/download_alpaca_end_of_day_bars/alpaca_http.py) |
| Frozen columns and request identity | [`alpaca_end_of_day_pipeline/contracts.py`](../libs/alpaca_end_of_day_pipeline/contracts.py) |
| Hard validation | [`alpaca_end_of_day_pipeline/daily_bar_validation.py`](../libs/alpaca_end_of_day_pipeline/daily_bar_validation.py) |
| Cache and frozen-file authentication | [`alpaca_end_of_day_pipeline/cached_downloads.py`](../libs/alpaca_end_of_day_pipeline/cached_downloads.py) |
| Asset and corporate-action evidence | [`alpaca_security_lineage/lineage.py`](../libs/alpaca_security_lineage/lineage.py) |
| Ownership and row-correlation rules | [`ownership_interval_rules.py`](../libs/alpaca_end_of_day_security_correlation/ownership_interval_rules.py), [`row_correlation.py`](../libs/alpaca_end_of_day_security_correlation/row_correlation.py) |
| Master-ID derivation and terminal fallback | [`security_identity.py`](../libs/alpaca_end_of_day_security_correlation/security_identity.py), [`terminal_fallback.py`](../libs/alpaca_end_of_day_security_correlation/terminal_fallback.py) |
| Master index | [`security_master.py`](../libs/alpaca_end_of_day_security_correlation/security_master.py) |
| Warning rules | [`cross_adjustment_warnings.py`](../libs/alpaca_end_of_day_warning_pipeline/cross_adjustment_warnings.py), [`security_segment_statistics.py`](../libs/alpaca_end_of_day_warning_pipeline/security_segment_statistics.py) |
| Public read result | [`alpaca_end_of_day_reader/history.py`](../libs/alpaca_end_of_day_reader/history.py) |
| Market sessions | [`common/us_market_hours.json`](../libs/common/us_market_hours.json) |

## Frozen source

- Request identity includes symbol, adjustment, start, end, `asof`, feed, and timeframe.
- Alpaca requests use SIP `1Day`, ascending order, a 10,000-bar limit, and reject any next-page token.
- Validated rows are serialized through `pigz -5`; no plain CSV is published.
- The gzip sidecar binds request identity, row coverage, file stat, and SHA-256.
- Integrity failures do not alter or remove rows. Exact failures are stored in `integrity_failure.manifest.json`.
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

## Artifacts

Per ticker: `data/end_of_day/<SYMBOL>/`

| Artifact | Contract |
|---|---|
| `<adjustment>.csv.gz` | Frozen Alpaca rows |
| `<adjustment>.csv.gz.manifest.json` | Frozen-source receipt |
| `pipeline.manifest.json` | Current validation generation and processing receipts |
| `integrity_failure.manifest.json` | Current hard failures; present with `failures: []` when clean |
| `correlated.pipeline.json` | Master-ID-assigned row ranges with exact Alpaca provenance |
| `uncorrelated.pipeline.json` | Unassigned row ranges and lineage work items |
| `security_correlation.manifest.json` | Digest binding both exhaustive branches |
| `cross_adjustment_warnings.pipeline.json` | Comparable-view findings |
| `security_segment_statistical_warnings.pipeline.json` | Master-ID history findings |
| `warning_pipeline.manifest.json` | Digest binding both warning outputs and upstream generations |

Global: `data/end_of_day/`

| Artifact | Contract |
|---|---|
| `security_master.index.json` | Master ID → Alpaca IDs, names, tickers, dates, identifiers, membership basis, available views, source symbols, and exact correlation digests. No bars, warnings, errors, or repair state. |
| `security_master.manifest.json` | Index stat, SHA-256, version, generation, source count, and master-ID count |

Runtime `data/` is ignored by Git.

## Reader

Input is one published 64-hex master ID and one exact adjustment label. `all` is Alpaca's all-actions-adjusted view,
not a wildcard.

The reader:

1. authenticates the master index and resolves the ID;
2. authenticates only the correlation sources and digests named by that identity;
3. authenticates applicable warning, validation, and frozen-download boundaries;
4. returns one JSON-compatible structure without writing or contacting Alpaca.

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
