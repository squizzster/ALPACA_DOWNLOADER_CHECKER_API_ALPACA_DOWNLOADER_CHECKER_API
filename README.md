# Alpaca end-of-day data

Small CLI for Alpaca US-equity daily bars. The design preserves the API response,
records pipeline failures, and never lets one boundary silently repair another.

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

| Gzip | Sidecar | Receipt/failure state | Action |
|---|---|---|---|
| absent | absent | any | Download normally. |
| exactly one present | — | any | Abort; require `--repair`. |
| present | invalid or inconsistent | any | Fatal; no repair or force. |
| authenticated | current receipt SHA | no failure | Use cache. |
| authenticated | current receipt SHA | matching failure | Report failure on stderr; do not rewrite data. |
| authenticated | missing/stale receipt, old manifest version, or new SHA | any | Authenticate gzip, read into memory, and run the current pipeline locally. No network or gzip rewrite. |
| authenticated, request identity changed | valid | any | Require `--force`. |

## Pipeline rule

Current `PIPELINE_VERSION`: **11**.

Any pipeline behavior change must increment `PIPELINE_VERSION` and append exactly
one object to `pipeline_version_change_log.json`. Its array length must equal the
version or the CLI refuses to run.

A new version invalidates the symbol-level receipt generation. Each selected gzip
is SHA-authenticated and reprocessed locally. `pipeline.manifest.json` stores the
version once; receipts do not. `integrity_failure.manifest.json` stores the version
at its header and on each failure, and never mixes failure versions.

Current checks: timestamp parse/range/order; New York session midnight; positive
finite OHLC/VWAP with high/low bounds; positive integer volume/trade count;
trade count ≤ volume; and complete sessions from fixed `us_market_hours.json`.

## CLI

```bash
# All eight adjustment modes
./download_eod.py --symbol DELL

# One or more modes
./download_eod.py --symbol DELL --adjustment raw
./download_eod.py --symbol DELL --adjustment split --adjustment dividend

# Explicit replacement of an authenticated pair
./download_eod.py --symbol DELL --adjustment raw --force

# Rebuild a pair only when gzip or sidecar is missing
./download_eod.py --symbol DELL --adjustment raw --repair
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

```bash
python3 -m unittest discover -s tests -v
```

The checked-in fixtures are synthetic 9–10-row datasets: clean, one bad price, one
bad activity value, and one missing session. Tests build real gzip/manifests under
temporary directories and cover every cache-boundary state without network access.

## Code map

| File | Owner |
|---|---|
| `download_eod.py` | CLI, cache decision, Alpaca request, rate limiting. |
| `eod_pipeline.py` | In-memory checks, compression/publication, receipts and failure manifests. |
| `us_market_calendar.py` / `us_market_hours.json` | Authoritative expected US sessions. |
| `pipeline_version_change_log.json` | Ordered pipeline changes; array position is the version. |

`data/` is runtime state and is intentionally ignored by Git, including all gzip
files and manifests. `us_market_hours.json` is tracked because the fixed calendar
is integral to pipeline validation.

The future repair/decision layer and final reader are not implemented. They must
publish separate output. The reader will combine the frozen download, matching
failure evidence, and repair decisions in memory. CLI `--repair` is only for a
missing half of the download pair.
