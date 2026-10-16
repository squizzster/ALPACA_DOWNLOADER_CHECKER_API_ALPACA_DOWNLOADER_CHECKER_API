# Agent instructions

These instructions apply to the entire repository. User instructions take precedence for the active task.

## Start with the boundary

Read [README.md](README.md), then [Architecture](docs/ARCHITECTURE.md), then [Pipeline](PIPELINE.md). Use the complete
documentation index in the README to open the authoritative guide for the area being changed. Check `git status`
before editing, preserve work already present, and confirm claims against current code, schemas, CLI definitions, and
tests. Generated runtime data is evidence, not source code.

The repository is a data handoff boundary. It acquires, authenticates, validates, resolves identity, publishes, and
serves data. Downstream research or trading decisions are outside its responsibility.

## Public and internal interfaces

- Consumer code imports only the [`alpaca_end_of_day`](alpaca_end_of_day/__init__.py) package. Its `DataLibrary`
  exposes online IDs and never reveals whether an unavailable ID is internally known or why it is offline.
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
- For lineage-rule development, follow the [research-and-rule-improvement loop](docs/development.md#lineage-research-and-rule-improvement-loop).
  Cases supply concepts, not ticker exceptions. The main agent owns integration; use a fresh research agent per case,
  focused checks during implementation, and one consolidated review at the agreed batch boundary. Trust approved inputs.
- Keep offline discovery and diagnostics outside the public API. A separate discovery product can be designed later;
  it is not part of `DataLibrary`.
- Tracked root Python files are executable entrypoints. The supported consumer library lives in
  [`alpaca_end_of_day/`](alpaca_end_of_day/__init__.py); put all other domain behavior in the owning package identified
  in [Architecture](docs/ARCHITECTURE.md#code-ownership).
- Any serialized or behavioral boundary change must bump its owning version and append exactly one entry to its
  runtime-validated JSON changelog. Do not add explicit `version` keys to those arrays; array position is the version.
- Documentation-only changes do not bump runtime versions or JSON changelogs. Record pending documentation changes
  under `Unreleased` in [VERSION_TRACKER.md](docs/VERSION_TRACKER.md#unreleased).
- Preserve existing worktree changes unless the task explicitly supersedes them. Keep secrets, runtime `data/`, logs,
  ad hoc ticker lists, and experimental output out of Git. The tracked recovery-universe exception is
  [`resources/ticker.csv`](resources/ticker.csv).

## Documentation discipline

Give each explanation one home:

- architecture and ownership in [ARCHITECTURE.md](docs/ARCHITECTURE.md);
- the single execution path and ordered rule cascade in [PIPELINE.md](PIPELINE.md);
- consumer behavior in [docs/public-api.md](docs/public-api.md);
- operator commands and hazards in [docs/operations.md](docs/operations.md);
- schemas, the identity model, artifact contracts, and publication gates in
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
