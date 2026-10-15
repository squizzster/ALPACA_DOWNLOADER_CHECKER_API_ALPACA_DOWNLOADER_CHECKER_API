# Alpaca EOD Data Engine

Alpaca EOD Data Engine builds a local end-of-day catalogue from Alpaca US-equity daily bars. It stores all eight
logical adjustment views in symbol/year/month boundaries, validates prices and sessions, reconstructs security
identity across ticker changes, evaluates availability, publishes stable public IDs, and serves exact histories
through a read-only Python library and CLI.

**A `public_id` identifies a security, never a ticker symbol.** Facebook trading first as `FB` and then as `META`
is one security with one public ID. An unrelated security later using `FB` has a different ID. A ticker is only a
dated label: even a familiar symbol such as `GOOG` must not be assumed permanently unique. A ticker-named download
is a pricing source, not proof of a single security. This distinction is the purpose of lineage: follow one security
across ticker changes while keeping unrelated securities separate when symbols are reused, with dated ownership.
See the [identity model](docs/technical-contracts.md#identity-model).

Pricing can also be acquired independently with [`download_alpaca_pricing.py`](download_alpaca_pricing.py). This
standalone command performs no asset lookup, lineage, correlation, public-ID creation, or catalogue publication.
Its compact source store retains combined completed years, a year-to-date snapshot, and an authoritative current
month. First/last-session checks select incremental update or full replacement. Its sibling
[`build_alpaca_lineage.py`](build_alpaca_lineage.py) consumes locked pending dates with one worker, trusts approved
pricing, and publishes dated identity assignments using the same three boundaries. The public catalogue reader is
not yet connected to this standalone pair.

The repository owns acquisition, boundary publication, validation, identity resolution, catalogue publication, and
serving. It does not make research or trading decisions. At the public boundary an ID either belongs to the selected
online snapshot and works, or receives the same unavailable response without disclosing internal or offline state.

The central design is a receipt-defined cascade. One boundary owns each calculation; downstream boundaries trust the
completed result instead of re-hashing it or recalculating its rules. Changed source months and identity closures are
processed narrowly, unchanged catalogue objects are retained, and the reader-visible catalogue receipt is written
last. See [Technical contracts](docs/technical-contracts.md#boundary-laws) for the exact trust model.

Key capabilities are:

- deterministic monthly gzip payloads with direct exact-copy manifest aliases;
- incremental month validation with reusable predecessor seams;
- explicit source, validation, lineage, correlation, warning, candidate, availability, and catalogue generations;
- deterministic public IDs derived from resolved Alpaca UUID membership rather than ticker spelling;
- exhaustive correlated/uncorrelated row partitioning with fail-closed ambiguity;
- content-addressed incremental catalogue objects and bounded superseded-object retention;
- a provider-only, per-label checkpointed clean-room builder with exhaustive boundary and public-reader audit; and
- local, synchronous, credential-free public reads with no network requests.

## Documentation

Every maintained project document is linked below. Start with Architecture and Pipeline, then use the authoritative
guide for the boundary being changed.

### Start here

- [Agent instructions](AGENTS.md) — repository workflow, safety rules, interface boundaries, and verification.
- [Architecture](docs/ARCHITECTURE.md) — responsibility, boundary graph, trust model, isolation, and code ownership.
- [Pipeline](PIPELINE.md) — exact execution order, reuse cascade, correlation registries, and non-divergence controls.

### Consume and operate

- [Public API](docs/public-api.md) — supported imports, snapshot behavior, search, resolution, histories, and export.
- [Operations](docs/operations.md) — current commands, request boundaries, update behavior, recovery, and known hazards.

### Develop and reference

- [Technical contracts](docs/technical-contracts.md) — boundary laws, immediate inputs, trust operations, schemas,
  artifact layout, identity, availability, and publication contracts.
- [Development](docs/development.md) — change routing, boundary versioning, tests, and documentation ownership.
- [Version tracker](docs/VERSION_TRACKER.md) — current versions, verification evidence, runtime state, and known gaps.
- [Storage rework notes](docs/storage-rework-notes.md) — retained store inventory, source-preservation decision, and planned
  reuse of existing downloads during development; cleanup is deferred.

## Runtime and requirements

- Python **3.11** is the configured runtime. The current environment verifies Python 3.11.13; its unqualified
  `python3` is Python 3.9.25, so repository verification must invoke `python3.11` explicitly.
- IANA time-zone data containing `America/New_York`.
- `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY`, plus the required SIP market-data entitlement, for producer runs.
- No credentials or network access for `DataLibrary` reads or flat export.

The current monthly publisher uses Python's deterministic gzip encoder directly; `pigz` is not a requirement for this
path. Tracked application code otherwise uses the standard library and has no dependency lock or package metadata.

The public catalogue uses ignored `data_vtest/`; focused standalone development uses ignored `data_pricing/`. A source
checkout cannot serve public data until a compatible catalogue store is restored or deliberately published. The older
`data/end_of_day/` layout is not a fallback for the combined producer or public reader.

## Consumer quick start

```python
from alpaca_end_of_day import DataLibrary

data = DataLibrary()

print(data.catalog())
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

`DataLibrary` selects one completed catalogue generation and trusts its published boundary receipts. It does not run
the explicit full-digest audit path on every read. A history call refuses to combine data with a newer catalogue
generation; create a new instance after `CatalogChangedError`.

Export every online security as one date-then-public-ID ordered file:

```bash
./read_alpaca_end_of_day_ticker.py \
  --dump-flat-online-csv /path/to/online.csv
```

## Standalone pricing downloader

```bash
./download_alpaca_pricing.py --symbol GOOG --store data_pricing --to 2026-10-13
```

The destination is mandatory and must be a new empty directory or an existing standalone pricing store. Existing
`data_v4/`, `data_vtest/`, and retained experiments are not migrated or copied. Matching first/last sessions permit
month updates; a mismatch automatically replaces the full history. Consolidation happens at month/year rollover.
Use repeated `-s`, `--file PATH`, or `--all` (the tracked ticker universe). `--from` cannot exclude previously stored
observations; a ticker-specific error does not stop the rest of a batch. `--force-download` explicitly requests clean
full replacement. One source-bound first/last validator avoids cold-payload reads during normal endpoint checks. See
[Operations](docs/operations.md#standalone-pricing-download) for credentials, recovery, and refresh hazards, and
[Technical contracts](docs/technical-contracts.md#standalone-pricing-source-contract) for the exact boundaries.
Up to three workers run independent whole-ticker updates, sharing one 199-request/minute budget and 429 cooldown.

## Standalone lineage

```bash
./build_alpaca_lineage.py --store data_pricing --symbol GOOG
```

Omit the selection to process pending notices. Lineage owns identity evidence and the established dated-ownership
rules; missing related pricing goes through the downloader after releasing pricing locks. Approved closed boundaries
are reused where their inputs remain valid. Publication completes before notices are removed. No pricing copies,
legacy-store migration, or public catalogue publication occur. See
[Operations](docs/operations.md#standalone-lineage) for selection, metadata refresh, and recovery.

## Existing combined producer quick start

Provide credentials through the environment and choose an intentional exclusive request boundary:

```bash
export ALPACA_API_KEY_ID="<key-id>"
export ALPACA_API_SECRET_KEY="<secret-key>"

./run_alpaca_end_of_day_ticker_pipeline.py \
  --symbol AAPL \
  --request-end YYYY-MM-DD
```

For a new symbol this requests all history since 2016 for all eight adjustments. For an existing symbol it replaces
the request-end month partition by default. Select a wider month-partition replacement range with `--request-start
YYYY-MM-01`; the final partition contains observations only before the exclusive request end.

The current producer is sequential and holds the global publication lock through provider acquisition and catalogue
publication. Use it for focused updates, not as one enormous cohort.

To construct a generation that never consults another runtime store, use the separate clean-room scheduler. It reads
the tracked one-record universe, freezes one request boundary, checkpoints every submitted-label resolution and
terminal outcome, processes each new canonical symbol in its own bounded transaction, and runs the complete audit
before optional activation:

```bash
./build_alpaca_end_of_day_clean_room.py \
  --request-end YYYY-MM-DD \
  --activate
```

The only admitted inputs are `resources/ticker.csv`, live provider responses, and the repository's versioned
contracts. `data/` and `data_vtest/` are neither queried nor migrated during construction. Exact recovery and audit
semantics are in [Operations](docs/operations.md#build-a-clean-room-generation).

This project is a historical end-of-day data engine. It is not an order-routing system, live quote API, brokerage
client, or investment recommendation service, and it is not affiliated with or endorsed by Alpaca.
