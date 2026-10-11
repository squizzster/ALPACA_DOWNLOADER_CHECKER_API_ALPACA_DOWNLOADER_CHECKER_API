# Alpaca EOD Data Engine

Alpaca EOD Data Engine builds a local, authenticated catalogue from Alpaca US-equity daily bars. It freezes provider
responses, validates prices and market sessions, reconstructs security identity across ticker changes, publishes
stable public IDs, and serves exact histories through a read-only Python library and CLI.

The engine gives research, screening, and trading systems a dependable data handoff instead of a collection of raw
files keyed by mutable ticker labels. Consumers can enumerate the online universe, search names and identifiers,
resolve ticker reuse by date, read eight adjustment views, and export one globally ordered raw-plus-adjusted CSV.

The repository owns acquisition, authentication, validation, identity resolution, publication, and serving. It does
not make investment or trading decisions. At the consumer boundary, an ID either belongs to one authenticated online
snapshot and works, or it receives the same unavailable error without disclosing internal or offline state.

Key capabilities are:

- frozen gzip sources and digest-bound manifests;
- deterministic public IDs built from resolved security identity rather than ticker spelling;
- exact `Decimal` prices, integer activity fields, and inclusive session-date ranges;
- eight logical adjustment histories, including `raw` and Alpaca's fully adjusted `all` view;
- sharded, atomic catalogue updates with generation consistency; and
- local, synchronous, credential-free consumer reads with no network requests.

## Documentation

Every maintained project document is linked below. Start with the first two when approaching the repository for the
first time.

### Start here

- [Agent instructions](AGENTS.md) — authoritative workflow, safety rules, interface boundaries, and verification
  expectations for repository agents.
- [Architecture](docs/ARCHITECTURE.md) — responsibility boundary, data flows, repository layout, component ownership,
  and system invariants.
- [Pipeline](PIPELINE.md) — the single production flow, ordered correlation cascade, split-identity rule, and
  non-divergence controls.

### Consume and operate

- [Public API](docs/public-api.md) — supported `alpaca_end_of_day` imports, search and ticker resolution, histories,
  flat CSV export, types, errors, and snapshot behavior.
- [Operations](docs/operations.md) — command map, update and read workflows, operational hazards, replacement, backup,
  and both restore and fresh-rebuild procedures.

### Develop and reference

- [Technical contracts](docs/technical-contracts.md) — request and frozen-source contracts, identity rules, availability
  gates, runtime schemas, tracked resources, and internal reader behavior.
- [Development](docs/development.md) — change routing, versioning rules, contributor verification, documentation
  ownership, and the absence of repository CI.
- [Version tracker](docs/VERSION_TRACKER.md) — current contract versions, `Unreleased` changes, verification evidence,
  known gaps, and concise historical milestones.

## Requirements

- Python **3.11** is the sole configured and verified runtime target: [`pyproject.toml`](pyproject.toml) targets 3.11,
  and Python 3.11.15 is verified in the current development environment. No package metadata declares or enforces a
  `requires-python` range.
- IANA time-zone data containing `America/New_York`; session dates are derived with `zoneinfo.ZoneInfo`.
- [`pigz`](https://zlib.net/pigz/) when publishing or replacing frozen downloads. No minimum `pigz` version is encoded
  by the repository; version 2.8 is verified in the current development environment.
- `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY` plus the required market-data entitlement for update commands.
  Consumer reads and exports require neither credentials nor network access.

Tracked application code uses the Python standard library and declares no third-party Python runtime dependency. The
repository has no package metadata or dependency lock; run scripts and import the public package from the repository
root, or otherwise place the checkout on `PYTHONPATH`.

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

This requires a compatible authenticated catalogue under `data/end_of_day/`. Runtime `data/` is intentionally absent
from Git, so a fresh clone must restore or publish it before `DataLibrary()` can open. See
[Backup and recovery](docs/operations.md#backup-and-recovery).

Export every online security as one date-then-public-ID ordered file:

```bash
./read_alpaca_end_of_day_ticker.py \
  --dump-flat-online-csv /path/to/online.csv
```

The output contains separately labelled raw and fully adjusted OHLC, volume, trade-count, and VWAP columns. See the
[Public API](docs/public-api.md#flat-online-csv) for its exact schema and disk-spooling behavior.

## Operator quick start

From the repository root, provide credentials through the environment and run the complete pipeline for explicit
symbols:

```bash
export ALPACA_API_KEY_ID="<key-id>"
export ALPACA_API_SECRET_KEY="<secret-key>"

./run_alpaca_end_of_day_ticker_pipeline.py \
  --symbol AAPL \
  --adaptive-adjustments \
  --request-end YYYY-MM-DD
```

Choose `YYYY-MM-DD` as the intentional exclusive request boundary; it is not the last included session date. Use only
one multi-worker pipeline process against a repository at a time. Runtime bars, manifests, credentials, ad hoc ticker
lists, and logs are intentionally ignored by Git. The tracked recovery seed is the deliberate exception. Read
[Operations](docs/operations.md) before replaying, advancing, replacing, repairing, backing up, restoring, or rebuilding
any frozen boundary.

Audit the complete published catalogue without credentials, network access, or writes:

```bash
./audit_alpaca_end_of_day_catalog.py
```

This is a historical end-of-day data engine. It is not an order-routing system, live quote API, brokerage client, or
investment recommendation service, and it is not affiliated with or endorsed by Alpaca.
