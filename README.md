# Alpaca EOD Data Engine

Alpaca EOD Data Engine turns Alpaca US-equity daily bars into an authenticated, identity-aware local data catalogue.
It downloads and freezes source responses, checks pricing and session integrity, reconstructs security lineage across
ticker changes, publishes stable public IDs, and serves exact histories through a read-only Python library and CLI.

The project is useful when downstream research, screening, or trading systems need a dependable handoff instead of
provider tickers and raw files. Consumers can enumerate the online universe, search company names and identifiers,
resolve reused tickers by date, read any of eight adjustment views, or export one globally ordered flat CSV containing
raw and fully adjusted prices and volumes.

This repository owns acquisition, integrity, identity resolution, publication, and serving. It does not decide how a
consumer should interpret or use the data. At the public boundary, an ID either belongs to the authenticated online
snapshot and works, or it produces the same unavailable error without revealing internal or offline state.

Key properties include:

- frozen gzip sources and digest-bound manifests;
- deterministic public IDs built from resolved security identity rather than ticker spelling;
- exact `Decimal` prices, integer activity fields, and inclusive session-date ranges;
- eight logical adjustment histories, including `raw` and Alpaca's fully adjusted `all` view;
- sharded, atomic catalogue updates with generation consistency; and
- local, synchronous, credential-free consumer reads with no network requests.

## Documentation

### Understand and consume

- [Architecture](ARCHITECTURE.md) — responsibility boundary, data flows, component ownership, and system invariants.
- [Public API](docs/public-api.md) — supported `alpaca_end_of_day` imports, search and ticker resolution, histories,
  flat CSV export, types, errors, and snapshot behavior.
- [Technical contracts](docs/technical-contracts.md) — source authentication, validation, identity rules, availability
  gates, artifact schemas, and internal reader behavior.

### Operate and maintain

- [Operations](docs/operations.md) — update, batch, read, export, cache, replacement, backup, and recovery commands.
- [Development](docs/development.md) — code ownership, versioning rules, local verification, documentation ownership,
  and the absence of repository CI.
- [Version tracker](VERSION_TRACKER.md) — current contract versions, unreleased work, verification status, and concise
  historical milestones.
- [Agent instructions](AGENTS.md) — authoritative repository workflow and safety rules for coding agents.

## Requirements

- Python **3.11** is the configured target in [`pyproject.toml`](pyproject.toml), and Python 3.11.15 is verified in the
  current development environment. The repository has no package metadata declaring `requires-python`; earlier
  versions are not supported or verified.
- IANA time-zone data containing `America/New_York`; session dates are derived with `zoneinfo.ZoneInfo`.
- [`pigz`](https://zlib.net/pigz/) when publishing or replacing frozen downloads. No minimum `pigz` version is encoded
  by the repository; version 2.8 is verified in the current development environment.
- `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY` plus the required market-data entitlement for update commands.
  Consumer reads and exports require neither credentials nor network access.

The repository has no package metadata or dependency lock. Run its scripts and import the public module from the
repository root or otherwise place this checkout on `PYTHONPATH`.

## Consumer quick start

```python
from alpaca_end_of_day import DataLibrary

data = DataLibrary()

print(data.online_count())
for security in data.search_securities("Orning"):
    print(security.public_id, security.names)

matches = data.resolve_ticker("META", on="2025-06-30")
history = data.get_history(
    matches[0].public_id,
    "raw",
    start="2025-01-01",
    end="2025-12-31",
)
```

Export every online security as one date-then-public-ID ordered file:

```bash
./read_alpaca_end_of_day_ticker.py \
  --dump-flat-online-csv /path/to/online.csv
```

The output contains separately labelled raw and fully adjusted OHLC, volume, trade-count, and VWAP columns. See the
[Public API](docs/public-api.md#flat-online-csv) for its exact schema and disk-spooling behavior.

## Operator quick start

Clone the repository, provide credentials through the environment, and run the complete pipeline for explicit
symbols:

```bash
git clone https://github.com/squizzster/ALPACA_DOWNLOADER_CHECKER_API_ALPACA_DOWNLOADER_CHECKER_API.git
cd ALPACA_DOWNLOADER_CHECKER_API_ALPACA_DOWNLOADER_CHECKER_API

export ALPACA_API_KEY_ID="<key-id>"
export ALPACA_API_SECRET_KEY="<secret-key>"

./run_alpaca_end_of_day_ticker_pipeline.py \
  --symbol AAPL \
  --adaptive-adjustments \
  --request-end 2026-10-06
```

Use only one multi-worker pipeline process against a repository at a time. Runtime bars, manifests, credentials,
ticker universes, and logs are intentionally ignored by Git. Read [Operations](docs/operations.md) before replacing,
repairing, backing up, or restoring any frozen boundary.

Audit the complete published catalogue without credentials, network access, or writes:

```bash
./audit_alpaca_end_of_day_catalog.py
```

This is a historical end-of-day data engine. It is not an order-routing system, live quote API, brokerage client, or
investment recommendation service, and it is not affiliated with or endorsed by Alpaca.
