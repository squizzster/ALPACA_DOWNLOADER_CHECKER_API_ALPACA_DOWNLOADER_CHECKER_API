# Alpaca EOD Data Engine

Maintains exact US-equity prices in eight adjustment views, dated security identities and an online serving
catalogue. Incremental checks reuse completed work; journals recover interrupted publications. Local readers
and CSV/Parquet exports consume published histories and evidence. Research and trading decisions stay outside
the data boundary.

[Ownership allocation](docs/lineage.md#ownership-allocation-criterion) seeks the most likely lineage from the
available evidence. Both incorrect allocation and withholding supported history are errors.

**Before continuing boundary or handoff work, read the [Trust-boundary implementation handoff](docs/trust-boundary-handoff.html).**
It records the audit findings, verified fixes, remaining gaps, owning regressions and live verification limits.
The review covers trust repairs and evidence-gated lineage extensions. [AGENTS.md](AGENTS.md) remains authoritative.

Requires Python 3.11, Unix `flock` and `America/New_York` timezone data. Production policy compilation uses
[PyYAML](requirements.txt); Parquet additionally uses [PyArrow](requirements-bloodhound.txt).
See [setup and credentials](docs/operations.md#prerequisites). Runtime stores stay outside Git.

## Run and consume

- [Operations](docs/operations.md): setup, `./alpaca-cli-eod update`, cutoffs, recovery and focused commands.
- [Public API](docs/public-api.md): identity selection, snapshots, histories and errors.
- [Exports](docs/exports.md): CSV/Parquet selection, formats, memory, locks and publication guarantees.
- [Standalone Bloodhound converter](bloodhound_app/README.md): convert completed CSV exports or legacy histories
  with the imported app, its own dependencies, tests and documented limitations.
- [CLI reports](docs/cli-reporting.md): completion, exit codes and the shared stderr heartbeat protocol.
- [Business-equity baseline](IS_ELIGIBLE_LISTED_BUSINESS_EQUITY/README.md): independent metadata and lineage export
  without pricing, with the definition retained for later assessments.

## Understand the system

- [Architecture and ownership](docs/ARCHITECTURE.md): execution paths and responsible packages.
- [Technical contracts](docs/technical-contracts.md): authoritative artifacts, pricing, work state, serving and recovery.
- [Lineage](docs/lineage.md): policy-bound reuse, metadata continuation, ownership evidence and correction limits.
- [Correlation policy lab](docs/policy-lab.md): read-only comparison/audit, candidate constraints and parity meanings.
- [Lineage rule tool](docs/lineage-debug.md): one YAML research policy, rule index, ticker explanations,
  selected/all-group replay, saved handoffs and exported ticker instances.

## Change and verify

- [Agent instructions](AGENTS.md): authoritative repository and storage boundaries.
- [Development](docs/development.md): research loop, tests and documentation verification;
  [Pipeline Phase 1](docs/development.md#pipeline-phase-1-prepare) and
  [Phase 2](docs/development.md#pipeline-phase-2-history-production) and
  [Phase 3](docs/development.md#pipeline-phase-3-complete-pipeline) own native stage contracts and their standalone JSON adapters.
- [Version tracker](VERSION_TRACKER.md): owning constants, runtime changelogs and publication compatibility.

Start with [store setup](docs/operations.md#adopt-or-reconstruct-an-existing-store); update does not bootstrap
missing boundaries. Command definitions and help live in [the CLI](libs/alpaca_cli/__init__.py).
