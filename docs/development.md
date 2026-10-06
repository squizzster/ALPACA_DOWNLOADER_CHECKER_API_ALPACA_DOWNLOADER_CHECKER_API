# Development guide

## Find the owning source

| Change | Start here |
|---|---|
| Full-pipeline stage order or multi-symbol reporting | [`libs/alpaca_end_of_day_ticker_pipeline/`](../libs/alpaca_end_of_day_ticker_pipeline/) |
| CLI parsing, bar HTTP, rate limits, or request timing | [`libs/download_alpaca_end_of_day_bars/`](../libs/download_alpaca_end_of_day_bars/) |
| Frozen publication, cache state, hard validation, receipts, or failures | [`libs/alpaca_end_of_day_pipeline/`](../libs/alpaca_end_of_day_pipeline/) |
| Asset/corporate-action queries or read-only lineage reports | [`libs/alpaca_security_lineage/`](../libs/alpaca_security_lineage/) |
| Ownership intervals, UUID assignment, row partition, or correlation publication | [`libs/alpaca_end_of_day_security_correlation/`](../libs/alpaca_end_of_day_security_correlation/) |
| Cross-adjustment or UUID-scoped advisory checks | [`libs/alpaca_end_of_day_warning_pipeline/`](../libs/alpaca_end_of_day_warning_pipeline/) |
| Atomic writes, hashing, shared rule mathematics, or market sessions | [`libs/common/`](../libs/common/) |

Root Python files are stable executables or compatibility facades. Keep domain changes in the owning library and
preserve exported compatibility names unless the caller contract is intentionally changed. `libs/common/` must not
import an application package.

Tests with exact serialized contracts are often the fastest schema guide:

- [`tests/test_alpaca_end_of_day_pipeline.py`](../tests/test_alpaca_end_of_day_pipeline.py)
- [`tests/test_alpaca_security_lineage.py`](../tests/test_alpaca_security_lineage.py)
- [`tests/test_alpaca_end_of_day_security_correlation.py`](../tests/test_alpaca_end_of_day_security_correlation.py)
- [`tests/test_alpaca_end_of_day_warning_pipeline.py`](../tests/test_alpaca_end_of_day_warning_pipeline.py)
- [`tests/test_alpaca_end_of_day_ticker_pipeline.py`](../tests/test_alpaca_end_of_day_ticker_pipeline.py)

## Version changes

Behavior or serialization changes must update the owner, not an unrelated downstream layer:

| Boundary changed | Required version source | Required changelog |
|---|---|---|
| Validation, frozen serialization, or validation-manifest behavior | [`PIPELINE_VERSION`](../libs/alpaca_end_of_day_pipeline/pipeline_version.py) | [`alpaca_end_of_day_pipeline_change_log.json`](../alpaca_end_of_day_pipeline_change_log.json) |
| Ownership rules, row assignment, or correlation serialization | [`CORRELATION_VERSION`](../libs/alpaca_end_of_day_security_correlation/correlation_version.py) | [`alpaca_end_of_day_security_correlation_change_log.json`](../alpaca_end_of_day_security_correlation_change_log.json) |
| Warning rules, thresholds, or warning serialization | [`WARNING_PIPELINE_VERSION`](../libs/alpaca_end_of_day_warning_pipeline/warning_pipeline_version.py) | [`alpaca_end_of_day_warning_pipeline_change_log.json`](../alpaca_end_of_day_warning_pipeline_change_log.json) |

Append exactly one changelog object for each version increment. Array position is the implicit version, and validators
require the array length to equal the constant. Do not add a `version` key to entries. A documentation-only change does
not change an artifact generation.

The lineage report schema marker is `REPORT_VERSION` in
[`libs/alpaca_security_lineage/settings.py`](../libs/alpaca_security_lineage/settings.py); it has no ordered changelog
contract.

## Verification

Ruff configuration is in [`pyproject.toml`](../pyproject.toml). Run:

```bash
ruff check .
ruff format --check .
python3 -W error::ResourceWarning -m unittest discover -s tests -v
```

Unit tests use temporary directories and test doubles for HTTP. Runtime `data/` is ignored and is not required. No CI
workflow or documentation-specific checker is checked in, so these commands are the repository's local verification
surface.
