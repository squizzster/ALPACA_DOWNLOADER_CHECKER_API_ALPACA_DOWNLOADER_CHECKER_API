# Alpaca end-of-day data

Small CLI for Alpaca US-equity daily bars. The design preserves the API response,
records pipeline failures, and never lets one boundary silently repair another.
The naming and ownership rules are in [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Contract

```text
Alpaca JSON
    │
    ▼
RAM: complete response → pipeline checks → rows + failure observations
                                             │
                                             ▼
                          pigz -5 → same-dir .tmp → SHA-256 twice
                                             │
                    ┌────────────────────────┴───────────────────────┐
                    ▼                                                ▼
       frozen gzip + download sidecar                   derived symbol manifests
                                                        (receipt + failures)
```

1. The complete response stays in memory until all row and dataset checks finish.
   No gzip, including a temporary gzip, exists before that point.
2. After checking, every row is written unchanged, including failed rows. No plain
   CSV is written. Publication uses same-directory temporary files and `os.replace`.
3. `<adjustment>.csv.gz` plus its sidecar is the frozen Alpaca download boundary.
   Local pipeline runs never rewrite it. `--force` may replace only an authenticated
   pair; `--repair` applies only when exactly one file of the pair is missing.
4. A present but malformed, stat-mismatched, or SHA-mismatched pair is fatal.
   Neither flag may guess which file is authoritative.
5. The pipeline produces receipts and failures. It never records a success status.

## Symbol artifacts

For each adjustment under `data/end_of_day/<SYMBOL>/`:

| Artifact | Meaning |
|---|---|
| `<adjustment>.csv.gz` | Rows received from Alpaca, compressed with `pigz -5` after pipeline checks. |
| `<adjustment>.csv.gz.manifest.json` | Compact download identity, compressed SHA-256, bytes/stat, rows, dates, request, and one-time hash verification. |

Each symbol directory also contains:

| Artifact | Meaning |
|---|---|
| `pipeline.manifest.json` | Header has the pipeline version. Versionless receipts bind each processed data file to its download SHA and processing metadata. No outcome field. |
| `integrity_failure.manifest.json` | Header and every failure carry the pipeline version. Contains only exact row/data errors and dataset errors. It remains present with `failures: []` when empty. |

The normal cache path trusts a verified sidecar while size and `mtime_ns` still
match. Before local reprocessing or `--force`, the gzip SHA is recomputed and must
match the sidecar.

## State decisions

| Gzip | Sidecar / derived state | Action |
|---|---|---|
| absent | absent | Download normally. |
| exactly one present | no `--repair` | Abort. `--force` is not a substitute. |
| exactly one present | `--repair` | Redownload, then replace/create both halves as one complete pair. |
| present | malformed, stat-inconsistent, or SHA-mismatched sidecar | Fatal; neither flag may repair or replace it. |
| authenticated | current receipt SHA, no failure | Use cache. |
| authenticated | current receipt SHA, matching failure | Replay the failure on stderr; do not rewrite data. |
| authenticated | missing/stale receipt, old manifest version, or new SHA | Authenticate gzip, read it into memory, and run the current pipeline locally. No network or gzip rewrite. |
| authenticated, request identity changed | valid | Require `--force`. |

Pricing/data failures, whether newly found or replayed, are reported on stderr;
the CLI finishes the selected adjustments and exits 1. Download-boundary
corruption is fatal and exits 2 without contacting Alpaca or changing artifacts.

## Pipeline rule

Current `PIPELINE_VERSION`: **14**.

Any pipeline code or behavior change must increment `PIPELINE_VERSION` and append
exactly one object to `alpaca_end_of_day_pipeline_change_log.json`. Its array
length must equal the version or the CLI refuses to run.

A new version invalidates the symbol-level receipt generation. Each selected gzip
is SHA-authenticated and reprocessed locally. The first update starts a generation
containing only adjustments processed at that version; an unselected adjustment
has no current receipt and reprocesses when later requested. `pipeline.manifest.json`
stores the version once because receipts from different generations never mix.
`integrity_failure.manifest.json` stores it at the header and on each failure.

Current checks: timestamp parse/range/order; New York session midnight; positive
finite OHLC/VWAP with high/low bounds; positive integer volume/trade count;
trade count ≤ volume; and complete sessions from fixed
`libs/common/us_market_hours.json`.

## CLI

```bash
# All eight adjustment modes
./download_alpaca_end_of_day_bars.py --symbol DELL

# One or more modes
./download_alpaca_end_of_day_bars.py --symbol DELL --adjustment raw
./download_alpaca_end_of_day_bars.py --symbol DELL --adjustment split --adjustment dividend

# Explicit replacement of an authenticated pair
./download_alpaca_end_of_day_bars.py --symbol DELL --adjustment raw --force

# Redownload both halves of an orphaned pair
./download_alpaca_end_of_day_bars.py --symbol DELL --adjustment raw --repair
```

Credentials are read only when a network request is required:

```bash
export ALPACA_API_KEY_ID=...
export ALPACA_API_SECRET_KEY=...
```

Modes: `raw`, `split`, `dividend`, `spin-off`, `split,dividend`,
`split,spin-off`, `dividend,spin-off`, `all`.

Requests use SIP `1Day` bars from `2016-01-01`, ascending, explicit `asof`, and a
10,000-bar limit. A next-page token is rejected rather than truncated. The limiter
targets 180 requests/minute and honors `429`, `Retry-After`, and
`X-RateLimit-Reset`.

## Tests

Install Ruff once with `uv tool install ruff`. [`pyproject.toml`](pyproject.toml)
sets its line length to 113: Ruff's default 88 plus 25.

```bash
ruff check .
ruff format --check .
python3 -m unittest discover -s tests -v
```

The checked-in fixtures are synthetic 9–10-row datasets: ten valid bars, one high
below low, one trade count above volume, and one missing session. Tests build real
gzip/manifests under temporary directories and cover every cache-boundary state
without network access.

## Code map

| File | Owner |
|---|---|
| `download_alpaca_end_of_day_bars.py` | Stable executable entry point. |
| `alpaca_end_of_day_pipeline.py` | Stable pipeline API facade. |
| `us_equity_market_session_calendar.py` | Stable calendar compatibility facade. |
| `libs/download_alpaca_end_of_day_bars/` | CLI orchestration, Alpaca HTTP, and rate limiting. |
| `libs/alpaca_end_of_day_pipeline/` | Cache classification, validation, publication, and derived manifests. |
| `libs/common/` | Atomic writes, file hashing, and authoritative US-session calendar data. |
| `alpaca_end_of_day_pipeline_change_log.json` | Ordered pipeline changes; array position is the version. |

`data/` is runtime state and is intentionally ignored by Git, including all gzip
files and manifests. `libs/common/us_market_hours.json` is tracked because the
fixed calendar is integral to pipeline validation.

The future repair/decision layer and final reader are not implemented. They must
publish separate output. The reader will authenticate the frozen pair, then match
receipts, failures, and repair decisions by data file, download SHA, and pipeline
version before combining them in memory; a listed failure without a decision cannot
produce repaired output. CLI `--repair` only redownloads an orphaned download pair;
it never repairs pricing/data.
