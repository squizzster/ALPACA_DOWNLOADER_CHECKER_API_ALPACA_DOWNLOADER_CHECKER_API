# Data and artifact contracts

## Canonical sources

Use these sources instead of copying constants or schemas into another document:

- Bar request defaults and adjustment modes: `ALPACA_ADJUSTMENT_MODES`, `EARLIEST_REQUEST_TIMESTAMP`,
  `ALPACA_DATA_FEED`, and `ALPACA_BAR_TIMEFRAME` in
  [`libs/download_alpaca_end_of_day_bars/settings.py`](../libs/download_alpaca_end_of_day_bars/settings.py).
- HTTP query construction, retry count, pagination rejection, and rate-limit handling:
  [`request_alpaca_daily_bars`](../libs/download_alpaca_end_of_day_bars/alpaca_http.py).
- Frozen CSV columns and request-identity fields:
  [`libs/alpaca_end_of_day_pipeline/contracts.py`](../libs/alpaca_end_of_day_pipeline/contracts.py).
- Hard row and dataset validation:
  [`validate_end_of_day_bars`](../libs/alpaca_end_of_day_pipeline/daily_bar_validation.py).
- Cache classification and frozen-file authentication:
  [`inspect_cached_download`](../libs/alpaca_end_of_day_pipeline/cached_downloads.py) and
  [`authenticate_frozen_download`](../libs/alpaca_end_of_day_pipeline/cached_downloads.py).
- Identity evidence and API traversal:
  [`build_identity_report`](../libs/alpaca_security_lineage/lineage.py).
- Ordered ownership and row-correlation rules:
  [`OWNERSHIP_INTERVAL_RULES`](../libs/alpaca_end_of_day_security_correlation/ownership_interval_rules.py) and
  [`ROW_CORRELATION_RULES`](../libs/alpaca_end_of_day_security_correlation/row_correlation.py).
- Warning rules and thresholds:
  [`CROSS_ADJUSTMENT_RULES`](../libs/alpaca_end_of_day_warning_pipeline/cross_adjustment_warnings.py),
  [`SECURITY_SEGMENT_STATISTICAL_RULES`](../libs/alpaca_end_of_day_warning_pipeline/security_segment_statistics.py),
  and [`WarningPipelineConfiguration`](../libs/alpaca_end_of_day_warning_pipeline/settings.py).
- Authoritative market sessions:
  [`libs/common/us_market_hours.json`](../libs/common/us_market_hours.json), loaded by
  [`us_equity_market_session_calendar.py`](../libs/common/us_equity_market_session_calendar.py).

## Frozen source contract

The bar request uses ticker symbols, SIP `1Day` data, ascending order, an explicit `asof`, and a 10,000-bar limit.
Any returned next-page token is rejected; the code never accepts a silently truncated dataset. The request identity
also includes the adjustment, start, end, feed, and timeframe.

The complete JSON response remains in memory until deterministic validation finishes. Only then is every row,
including a failed row, serialized without pricing repair through `pigz -5` into a same-directory temporary gzip. No
plain CSV is written. SHA-256 is calculated and independently verified before each boundary file is atomically
replaced.

The CSV preserves bar fields but not Alpaca's outer returned-symbol key. Keep the requested ticker, `asof`, and request
range when interpreting historical symbol mapping; a filename or response label is not a security identity.

## Integrity classification

The base pipeline treats these as action-required integrity failures:

- missing fields or unusable, out-of-range, non-increasing, or non-session-midnight timestamps;
- non-positive or non-finite OHLC/VWAP, invalid high/low ordering, or open/close/VWAP outside the range;
- non-positive or non-integral volume or trade count, or trade count greater than volume;
- empty/unusable datasets, calendar-range errors, unexpected closed-session rows, and duplicated or incorrectly
  ordered sessions.

An absent expected session is not a pre-identity integrity failure. Ticker changes, ticker reuse, acquisitions, and
listing boundaries can create legitimate gaps. After correlation, missing-session checks are advisory and scoped to
one UUID ownership segment; intervals between distinct UUIDs are excluded. The regression contracts are in
[`ValidationTests`](../tests/test_alpaca_end_of_day_pipeline.py) and
[`WarningPipelineTests`](../tests/test_alpaca_end_of_day_warning_pipeline.py).

Integrity failures do not alter or filter the frozen gzip. Exact failed row data and deterministic error text are
recorded in the integrity manifest. Warning findings never enter that manifest.

## On-disk artifacts

All production artifacts for a ticker live in `data/end_of_day/<SYMBOL>/`.

| Artifact | Owner and contract |
|---|---|
| `<adjustment>.csv.gz` | Frozen Alpaca rows for one request identity. Commas in adjustment names become underscores in filenames. |
| `<adjustment>.csv.gz.manifest.json` | Download-boundary sidecar: request identity, coverage, file stat, SHA-256, and publication verification. |
| `pipeline.manifest.json` | Current validation generation and one versionless processing receipt per processed download. |
| `integrity_failure.manifest.json` | Current action-required failures only; it exists with `failures: []` when clean. |
| `correlated.pipeline.json` | Row ranges assigned to canonical Alpaca asset UUIDs with their provenance. |
| `uncorrelated.pipeline.json` | Unassigned row ranges and non-row lineage work items retained without inference. |
| `security_correlation.manifest.json` | Receipt that binds both correlation branches, their inputs, rule order, and generation. |
| `cross_adjustment_warnings.pipeline.json` | Warning/information findings across comparable adjustment views. |
| `security_segment_statistical_warnings.pipeline.json` | Warning/information findings evaluated within UUID histories. |
| `warning_pipeline.manifest.json` | Receipt that binds both warning outputs to current upstream digests and configuration. |

`data/` is ignored by Git. Do not use runtime artifacts as fixtures or commit them. The tracked calendar is application
input, not runtime output.

## Authentication and cache trust

The gzip and sidecar form one frozen boundary. Exactly one present file is an orphaned pair. A malformed sidecar,
changed size or `mtime_ns`, or a SHA mismatch is not an orphan and cannot be repaired by choosing one file as truth.

Normal cache reads trust the publication-verified sidecar while file size and `mtime_ns` still match. Local
reprocessing, forced replacement, and correlation recompute gzip SHA-256. A forced replacement authenticates the
existing pair before contacting Alpaca and cannot overwrite an unexplained mismatch. Warning processing authenticates
the correlation outputs and verifies the frozen file's stat and manifest bindings; it does not rehash the gzip.

Correlation accepts current clean downloads and downloads with recorded integrity failures, authenticates their
SHA-256, and partitions every row exactly once. Warning processing then authenticates the correlation receipt and
both branches, verifies the exhaustive ranges, and binds the current validation manifests by digest. Rows with
recorded row-level integrity failures remain in the frozen source and correlation partition but are excluded from
numeric warning calculations.

Do not confuse these states:

- an empty integrity-failure list means no current hard failure was recorded;
- a complete correlation partition means all source rows were placed in a branch;
- zero uncorrelated rows means all source rows received an identity assignment;
- warnings and information are advisory and do not become integrity errors;
- command exit success is defined by the CLI implementation and is not a future reader-readiness guarantee.

## Versioned generations

Validation, correlation, and warnings have independent version constants and ordered changelogs. Their current values
are authoritative in
[`pipeline_version.py`](../libs/alpaca_end_of_day_pipeline/pipeline_version.py),
[`correlation_version.py`](../libs/alpaca_end_of_day_security_correlation/correlation_version.py), and
[`warning_pipeline_version.py`](../libs/alpaca_end_of_day_warning_pipeline/warning_pipeline_version.py).

A validation-version change reauthenticates and reprocesses frozen downloads locally; it does not rewrite the gzip or
its sidecar. Correlation and warning generation digests include their own versions and upstream identities, so stale
derived outputs rebuild while coherent identical generations remain byte-stable cache hits. Receipt files are
published after the outputs they authenticate.
