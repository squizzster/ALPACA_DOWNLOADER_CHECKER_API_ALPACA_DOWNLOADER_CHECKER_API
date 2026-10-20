# Alpaca EOD Data Engine

Downloads exact US-equity pricing in eight adjustment views and resolves dated security lineage.
A ticker is a label, not an identity. The system ends at the data handoff.

## Run the daily update

```bash
./alpaca-eod update
```

Discovers active OTC/NYSE/NASDAQ/AMEX/TXSE labels, remembers them in the additive recovery registry,
and maintains pricing and lineage in `data_pricing/`. Use `update --plan` for a read-only scope preview.
See [setup and recovery](docs/operations.md).

Requires Python 3.11, Unix file locking, New York timezone data and Alpaca credentials with SIP access.
Runtime stores are not in Git. Local readers need published data but no credentials or network.

## Where to look

| Guide | Purpose |
|---|---|
| [Architecture](docs/ARCHITECTURE.md) | Execution flow, code ownership and store separation. |
| [Operations](docs/operations.md) | Commands, session cutoff and recovery. |
| [Public API](docs/public-api.md) | Read pricing and resolve identities. |
| [Technical contracts](docs/technical-contracts.md) | Trusted handoffs, artifacts and publication rules. |
| [Development](docs/development.md) | Changes, lineage research and verification. |
| [Agent instructions](AGENTS.md) | Working rules for this repository. |

This is the complete documentation index. Git holds history; these guides describe the current system.
