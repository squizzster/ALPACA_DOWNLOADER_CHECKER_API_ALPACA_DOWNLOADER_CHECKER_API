# Alpaca EOD Data Engine

An authenticated US-equity daily-bar engine built around the
[Alpaca Market Data API](https://docs.alpaca.markets/us/docs/historical-api). It downloads and freezes source data,
checks integrity, reconstructs historical security identity, publishes stable internal master IDs, and exposes a
read-only consumer API.

The central consumer promise is deliberately simple:

```text
online = published master ID
       + bars exist
       + every dependency authenticates
       + every advertised adjustment is readable
       + zero integrity errors or cross-source conflicts
       + zero unresolved lineage boundaries
```

If a master ID is `online: true`, this engine will return its data. Provider trading status, API mechanics, ticker
changes, manifests, and lineage work remain behind that boundary.

> **Phase 1 is complete.** Acquisition, validation, identity correlation, sharded master-ID publication, online
> evaluation, and authenticated reading now operate as one end-to-end boundary.

Runtime market data is intentionally not stored in Git. Each deployment builds and authenticates its own `data/`
tree using its Alpaca credentials and data entitlements.

## What it provides

- Eight logical daily-bar views: `raw`, `split`, `dividend`, `spin-off`, all three two-action combinations, and `all`.
- Immutable compressed source files with SHA-256, request, stat, session, and calendar provenance.
- Hard integrity checks that preserve bad source rows and record the exact failure instead of silently repairing data.
- Historical identity correlation using Alpaca asset UUIDs, corporate actions, CUSIPs, ticker history, and explicit
  unresolved branches.
- A deterministic provider-neutral master ID for each resolved historical pricing identity.
- A compact persisted `online` boolean for applications that only need to know whether data is ready.
- A read-only API and CLI that authenticate the selected dependency chain without contacting Alpaca.
- Small atomic updates: one ticker changes its own shards and inventory buckets, not a giant master file or database.

## How it fits together

```text
Alpaca daily bars
  -> frozen authenticated files
  -> hard integrity results
  -> asset and corporate-action evidence
  -> correlated / uncorrelated row partition
  -> advisory warning overlays
  -> online evaluation
  -> sharded master-ID catalog
  -> read-only API / CLI
```

See [Architecture](ARCHITECTURE.md) for ownership boundaries and invariants.

## Quick start

### Requirements

- Python 3.11 or newer
- [`pigz`](https://zlib.net/pigz/) for publishing compressed downloads
- Alpaca API credentials and the required market-data entitlement for updates
- No credentials or network access for local reads

Clone the repository:

```bash
git clone https://github.com/squizzster/ALPACA_DOWNLOADER_CHECKER_API_ALPACA_DOWNLOADER_CHECKER_API.git
cd ALPACA_DOWNLOADER_CHECKER_API_ALPACA_DOWNLOADER_CHECKER_API
```

Provide credentials as environment variables. Do not put them in the repository:

```bash
export ALPACA_API_KEY_ID="<key-id>"
export ALPACA_API_SECRET_KEY="<secret-key>"
```

Alpaca's official guides cover [authentication](https://docs.alpaca.markets/us/docs/authentication),
[market-data setup](https://docs.alpaca.markets/us/docs/getting-started-with-alpaca-market-data), and the
[historical stock-bars endpoint](https://docs.alpaca.markets/us/v1.4.2/reference/stockbarsingle-1).

Run the complete pipeline for one or more symbols:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py --symbol AAPL -s DELL
```

Run a deduplicated batch with the adaptive adjustment policy:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py \
  --file path/to/symbols.txt \
  --workers 3 \
  --adaptive-adjustments
```

For reproducible batches, pin the exclusive request boundary:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py \
  --file path/to/symbols.txt \
  --workers 3 \
  --adaptive-adjustments \
  --request-end 2026-10-06
```

Use only one multi-worker pipeline process against a repository at a time. See [Operations](docs/operations.md) for
cache, repair, skip, backup, and recovery behavior.

## Read authenticated data

Read one adjustment by the repository's 64-hex master ID:

```bash
./read_alpaca_end_of_day_ticker.py \
  --type raw \
  --master-id <64-hex-master-id>
```

The command emits one JSON document. It performs no update, repair, or network request.

Python consumers can ask the smaller availability question first:

```python
from libs.alpaca_end_of_day_reader import (
    get_alpaca_end_of_day_security_availability,
    list_alpaca_end_of_day_master_ids_by_availability_status,
    list_online_alpaca_end_of_day_master_ids,
    read_alpaca_end_of_day_security_history,
)

online_ids = list_online_alpaca_end_of_day_master_ids()
status = get_alpaca_end_of_day_security_availability(online_ids[0])
history = read_alpaca_end_of_day_security_history(online_ids[0], "raw")

assert status["online"] is True
assert history["bars"]

# Offline but still readable for investigation.
gap_ids = list_alpaca_end_of_day_master_ids_by_availability_status("lineage_gap")
```

`online` is the data engine's provider-neutral availability boundary. It is intentionally independent of whether
Alpaca currently labels an asset active, inactive, or tradable.

Availability is computed from each deployment's own authenticated data and is never a fixed repository-wide number:

| State | Consumer meaning |
|---|---|
| `online: true`, `availability_status: "online"` | Data exists and satisfies the complete read contract. |
| `online: false`, `availability_status: "lineage_gap"` | Data remains readable for investigation, but an expected internal market session is absent. |
| `online: false`, `availability_status: "offline"` | One or more other required boundaries did not resolve. |
| Master ID is unpublished | Offline; the reader will not invent or discover an identity outside the catalog. |

Consumers should call the availability API for the current answer. They do not need to know how many securities were
submitted, skipped, quarantined, or investigated to produce it.

## Identity and provenance

A ticker is a label, not a permanent security identity. The engine keeps Alpaca's authoritative punctuated symbol
(for example, `BFA -> BF.A`) while retaining the submitted alias as provenance. It derives the stable internal ID from
resolved provider identities:

```text
master_id = SHA-256("\n".join(sorted(unique_alpaca_asset_ids)))
```

Rows that cannot be assigned safely remain explicit and do not receive a fabricated security identity. Empty,
obsolete, OTC, inactive, exchange-test, and invalid-symbol cases are classified from authenticated evidence—not from
ticker spelling. The complete rules are in [Technical contracts](docs/technical-contracts.md).

## Storage model

Generated runtime artifacts live under `data/end_of_day/` and are ignored by Git:

```text
data/end_of_day/<SYMBOL>/               frozen bars and per-symbol provenance
data/end_of_day/json_manifests/         sharded source and master-ID catalog
```

The catalog has a tiny authenticated root and immutable per-source/per-master-ID objects. A ticker update rewrites
only the affected objects, SHA-256-prefix buckets, small inventory roots, and final receipt. There is no status
database and no monolithic identity payload.

Frozen bar bytes are never silently changed. A repair layer does not yet exist; reader repair fields therefore remain
`null`.

## Documentation

| Need | Document |
|---|---|
| Pipeline stages, ownership, and invariants | [Architecture](ARCHITECTURE.md) |
| Master-ID, artifact, validation, and reader contracts | [Technical contracts](docs/technical-contracts.md) |
| Update, read, cache, backup, and recovery commands | [Operations](docs/operations.md) |
| Code map, versions, changelogs, and tests | [Development](docs/development.md) |

Every serialized or behavioral boundary has an explicit version and append-only changelog.

## Verification

```bash
ruff check .
ruff format --check .
python3 -W error::ResourceWarning -m unittest discover -s tests -v
```

The tests use temporary repositories and HTTP doubles; they do not require production data or live credentials.

## Scope

This is a historical US-equity end-of-day data engine, not an order-routing system, brokerage client, live quote API,
or investment recommendation service. It is an independent project and is not affiliated with or endorsed by Alpaca.
Users remain responsible for their Alpaca account terms, market-data entitlements, and permitted use of downloaded
data.
