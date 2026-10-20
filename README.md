# Alpaca EOD Data Engine

Exact US-equity pricing in eight adjustment views; dated security lineage. Tickers are labels, not identities.
Scope ends at the data handoff.

Read [Agent instructions](AGENTS.md) → [Architecture](docs/ARCHITECTURE.md) → the relevant guide:

| Guide | Scope |
|---|---|
| [Operations](docs/operations.md) | Commands, setup, session cutoff, recovery. |
| [Public API](docs/public-api.md) | Pricing reads, identity resolution. |
| [Technical contracts](docs/technical-contracts.md) | Trusted handoffs, artifacts, publication. |
| [Development](docs/development.md) | Changes, lineage research, verification. |

## Daily update

```bash
./alpaca-eod update
```

Discovers active OTC/NYSE/NASDAQ/AMEX/TXSE labels, unions the recovery registry, maintains pricing/lineage
in `data_pricing/`. `update --plan`: read-only scope preview.

Requires Python 3.11, Unix file locking, New York timezone data, Alpaca credentials and SIP access.
Local readers require published data, no credentials/network. Runtime stores stay outside Git.

This is the complete documentation index. Guides describe current behavior; Git holds history.
