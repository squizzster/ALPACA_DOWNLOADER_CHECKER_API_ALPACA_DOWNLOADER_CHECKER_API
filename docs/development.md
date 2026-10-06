# Development

## Code map

| Change | Owner |
|---|---|
| Full-pipeline order and multi-symbol reporting | [`alpaca_end_of_day_ticker_pipeline/`](../libs/alpaca_end_of_day_ticker_pipeline/) |
| Bar CLI, HTTP, request timing, and rate limits | [`download_alpaca_end_of_day_bars/`](../libs/download_alpaca_end_of_day_bars/) |
| Frozen publication, cache, hard validation, and integrity manifests | [`alpaca_end_of_day_pipeline/`](../libs/alpaca_end_of_day_pipeline/) |
| Asset and corporate-action evidence | [`alpaca_security_lineage/`](../libs/alpaca_security_lineage/) |
| Correlation, master IDs, and master index | [`alpaca_end_of_day_security_correlation/`](../libs/alpaca_end_of_day_security_correlation/) |
| Advisory warning overlays | [`alpaca_end_of_day_warning_pipeline/`](../libs/alpaca_end_of_day_warning_pipeline/) |
| Master-ID reads and stdout result | [`alpaca_end_of_day_reader/`](../libs/alpaca_end_of_day_reader/) |
| Atomic writes, hashing, rule helpers, and market calendar | [`common/`](../libs/common/) |

Root scripts are entry points or compatibility facades. Keep domain logic in its owner; `libs/common/` imports no
application package.

## Versioned boundaries

Every behavior or serialized-contract change bumps its version and appends one changelog entry. Array position is the
implicit version; entries contain no `version` key.

| Boundary | Version | Changelog |
|---|---|---|
| Validation/frozen processing | [`PIPELINE_VERSION`](../libs/alpaca_end_of_day_pipeline/pipeline_version.py) | [`alpaca_end_of_day_pipeline_change_log.json`](../alpaca_end_of_day_pipeline_change_log.json) |
| Correlation | [`CORRELATION_VERSION`](../libs/alpaca_end_of_day_security_correlation/correlation_version.py) | [`alpaca_end_of_day_security_correlation_change_log.json`](../alpaca_end_of_day_security_correlation_change_log.json) |
| Master index | [`SECURITY_MASTER_VERSION`](../libs/alpaca_end_of_day_security_correlation/security_master_version.py) | [`alpaca_security_master_change_log.json`](../alpaca_security_master_change_log.json) |
| Warning overlays | [`WARNING_PIPELINE_VERSION`](../libs/alpaca_end_of_day_warning_pipeline/warning_pipeline_version.py) | [`alpaca_end_of_day_warning_pipeline_change_log.json`](../alpaca_end_of_day_warning_pipeline_change_log.json) |
| Public reader | [`READER_VERSION`](../libs/alpaca_end_of_day_reader/reader_version.py) | [`alpaca_end_of_day_reader_change_log.json`](../alpaca_end_of_day_reader_change_log.json) |

Lineage report schema uses `REPORT_VERSION` in
[`alpaca_security_lineage/settings.py`](../libs/alpaca_security_lineage/settings.py).

## Verification

```bash
ruff check .
ruff format --check .
python3 -W error::ResourceWarning -m unittest discover -s tests -v
```

Tests use temporary directories and HTTP doubles. Runtime `data/` is ignored by Git.
