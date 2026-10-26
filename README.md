# Alpaca EOD Data Engine

Maintains exact US-equity prices in eight adjustment views, dated security identities and an online serving
catalogue. Incremental checks reuse completed work; journals recover interrupted publications. Local readers
and CSV/Parquet exports consume published histories and evidence. Research and trading decisions stay outside
the data boundary.

Requires Python 3.11, Unix `flock` and `America/New_York` timezone data. Production policy compilation uses
[PyYAML](requirements.txt); Parquet additionally uses [PyArrow](requirements-bloodhound.txt).
See [setup and credentials](docs/operations.md#prerequisites). Runtime stores stay outside Git.

## Run and consume

- [Operations](docs/operations.md): setup, `./alpaca-cli-eod update`, cutoffs, recovery and focused commands.
- [Public API](docs/public-api.md): identity selection, snapshots, histories and errors.
- [Exports](docs/exports.md): CSV/Parquet selection, formats, memory, locks and publication guarantees.
- [CLI reports](docs/cli-reporting.md): completion, exit codes and the shared stderr heartbeat protocol.

## Understand the system

- [Architecture and ownership](docs/ARCHITECTURE.md): execution paths and responsible packages.
- [Technical contracts](docs/technical-contracts.md): authoritative artifacts, pricing, work state, serving and recovery.
- [Lineage](docs/lineage.md): policy-bound reuse, metadata continuation, ownership evidence and correction limits.
- [Correlation policy lab](docs/policy-lab.md): read-only comparison/audit, candidate constraints and parity meanings.

## Change and verify

- [Agent instructions](AGENTS.md): authoritative repository and storage boundaries.
- [Development](docs/development.md): research loop, tests and documentation verification.
- [Version tracker](VERSION_TRACKER.md): owning constants, runtime changelogs and publication compatibility.

Start with [store setup](docs/operations.md#adopt-or-reconstruct-an-existing-store); update does not bootstrap
missing boundaries. Command definitions and help live in [the CLI](libs/alpaca_cli/__init__.py).
