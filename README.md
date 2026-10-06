# Alpaca end-of-day data

Provenance-preserving Alpaca US-equity daily bars with frozen source files, explicit integrity results, historical
security correlation, a repository master-ID index, advisory warnings, and a read-only API/CLI.

## Documentation index

| Need | Document |
|---|---|
| Pipeline stages, ownership, and invariants | [Architecture](ARCHITECTURE.md) |
| Master-ID rules, artifacts, and reader result contract | [Technical contracts](docs/technical-contracts.md) |
| Update, read, cache, and recovery commands | [Operations](docs/operations.md) |
| Code ownership, versions, changelogs, and tests | [Development](docs/development.md) |

## Primary commands

```bash
# Update one or more tickers through every pipeline stage.
./run_alpaca_end_of_day_ticker_pipeline.py --symbol FUN -s DELL

# Read one published adjustment view by repository master ID.
./read_alpaca_end_of_day_ticker.py --type raw --master-id <64-hex-master-id>
```

The reader resolves IDs only through `data/end_of_day/security_master.index.json`. A master ID is SHA-256 over the
newline-joined sorted unique Alpaca asset IDs in one historical pricing identity. The index contains identity metadata,
not bars, warnings, errors, or repairs. Until a repair layer exists, reads return authenticated frozen values with
separate integrity and warning sections and `null` repair fields.
