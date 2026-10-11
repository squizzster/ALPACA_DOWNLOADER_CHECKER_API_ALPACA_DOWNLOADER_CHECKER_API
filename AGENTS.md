# Agent instructions

These instructions apply to the entire repository. User instructions take precedence for the active task.

## Start with the boundary

Read [README.md](README.md), then [ARCHITECTURE.md](docs/ARCHITECTURE.md). Use the documentation index in the README to
open the one authoritative guide for the area being changed. Confirm claims against current code, schemas, CLI
definitions, and tests; generated runtime data is evidence, not source code.

The repository is a data handoff boundary. It acquires, authenticates, validates, resolves identity, publishes, and
serves data. Downstream research or trading decisions are outside its responsibility.

## Public and internal interfaces

- Consumer code imports only [`alpaca_end_of_day.py`](alpaca_end_of_day.py). Its `DataLibrary` exposes online IDs and
  never reveals whether an unavailable ID is internally known or why it is offline.
- [`read_alpaca_end_of_day_ticker.py`](read_alpaca_end_of_day_ticker.py) is a CLI client of that public library.
- Modules under `libs/`, including `libs/alpaca_end_of_day_reader/`, are implementation and operator-inspection
  interfaces. Do not present them as supported application imports.
- A ticker is a dated label. Preserve one-to-many ticker resolution and never silently choose an identity when the
  evidence is ambiguous.

## Change discipline

- Preserve frozen bar files. Do not redownload, rewrite, repair, or run the full ticker universe unless the task
  explicitly requires it. Derived manifests may change only when the requested pipeline work requires publication.
- Correlation and availability rules must describe a generic, obvious evidence shape. Do not special-case a ticker.
  Put each rule at the correct point in the explicit rule registry, fail closed on ambiguity, and verify the complete
  set of identities affected by the rule.
- Keep offline discovery and diagnostics outside the public API. A separate discovery product can be designed later;
  it is not part of `DataLibrary`.
- Root Python files are executable entrypoints, except for the supported consumer library
  [`alpaca_end_of_day.py`](alpaca_end_of_day.py). Put domain behavior in the owning package identified in
  [docs/development.md](docs/development.md#code-ownership).
- Any serialized or behavioral boundary change must bump its owning version and append exactly one entry to its
  runtime-validated JSON changelog. Do not add explicit `version` keys to those arrays; array position is the version.
- Preserve existing worktree changes unless the task explicitly supersedes them. Keep secrets, runtime `data/`, logs,
  ad hoc ticker lists, and experimental output out of Git. The tracked recovery-universe exception is
  [`artifacts/ticker.csv`](artifacts/ticker.csv).

## Documentation discipline

Give each explanation one home:

- architecture and ownership in [ARCHITECTURE.md](docs/ARCHITECTURE.md);
- consumer behavior in [docs/public-api.md](docs/public-api.md);
- operator commands and hazards in [docs/operations.md](docs/operations.md);
- schemas, identity rules, and publication gates in
  [docs/technical-contracts.md](docs/technical-contracts.md);
- contributor workflow and verification in [docs/development.md](docs/development.md); and
- versions, pending changes, milestones, and current verification gaps in
  [VERSION_TRACKER.md](docs/VERSION_TRACKER.md).

The README is a concise product overview and complete documentation index. Update cross-references whenever a
maintained Markdown file is added, moved, or retired.

## Verification

During focused implementation, run the smallest meaningful unit tests. Before a release or handoff, run the complete
repository verification commands from [docs/development.md](docs/development.md#verification). Do not claim a green
suite when a command fails; record the exact gap in [VERSION_TRACKER.md](docs/VERSION_TRACKER.md#verification-status).

There is currently no repository CI workflow, so local evidence is the only automated verification gate.
