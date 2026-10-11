# Architecture

## Responsibility boundary

This repository is the acquisition, integrity, identity, publication, and read boundary for its data. It downloads
provider data, preserves and authenticates source artifacts, checks pricing and session integrity, resolves security
identity, publishes the histories that satisfy the read contract, and serves them to downstream consumers. Its
responsibility ends at that handoff.

A publication result is a statement about data readiness. It is not a judgment about a security, its investment value,
or its suitability for any strategy. Trading, research, screening, modelling, and policy decisions belong to consumers
and never feed back into publication eligibility.

## Two read boundaries

The project deliberately separates application consumption from operator inspection:

| Boundary | Supported entrypoint | What it exposes |
|---|---|---|
| Consumer | [`alpaca_end_of_day.DataLibrary`](../alpaca_end_of_day/__init__.py) | Only the authenticated online universe, public identity metadata, exact bars, search, ticker resolution, and flat export. |
| Operator/internal | [`libs/alpaca_end_of_day_reader`](../libs/alpaca_end_of_day_reader/) | Detailed provenance, validation findings, warning overlays, source selection, and offline records for pipeline evaluation or investigation. |

A consumer cannot discover an offline identity through the public boundary. Every well-formed ID outside the opened
online snapshot receives the same `PublicIdUnavailableError`. Internal readers remain necessary because operators must
be able to explain and repair publication failures without exposing that machinery as an application contract.

## Update flow

Update orchestration is owned by
[`libs/alpaca_end_of_day_ticker_pipeline/orchestration.py`](../libs/alpaca_end_of_day_ticker_pipeline/orchestration.py):

```text
Alpaca bars or authenticated cache
  -> frozen source publication and hard integrity validation
  -> asset and corporate-action evidence
  -> symbol disposition
  -> exhaustive correlated / uncorrelated row partition
  -> advisory warning overlays
  -> authenticated internal reads of every advertised adjustment
  -> online and availability evaluation
  -> sharded, digest-bound master catalogue
```

The complete pipeline publishes derived artifacts only after their inputs authenticate. Receipts are written last so a
reader never treats a partially published generation as complete.

[`PIPELINE.md`](../PIPELINE.md) is the authority for the one permitted execution path, the ordered correlation
cascade, and the controls that prevent update or migration work from creating a second rule engine.

## Consumer flow

```text
DataLibrary construction
  -> authenticate one security-master generation
  -> expose only identity shards with online = true

public ID + adjustment + optional dates
  -> authenticate selected identity and source dependencies
  -> read exact frozen values through the internal reader
  -> require the same catalogue generation and online state
  -> project a small typed consumer result
```

`DataLibrary` is bound to the generation opened at construction. A concurrent publication causes a history read to
raise `CatalogChangedError`; the caller creates a new instance to observe the new snapshot.

## Code ownership

| Boundary | Owner | Responsibility |
|---|---|---|
| Consumer library | [`alpaca_end_of_day/`](../alpaca_end_of_day/__init__.py) | Online-only catalogue, search, resolution, typed history, and flat CSV projection. |
| Bar request | [`libs/download_alpaca_end_of_day_bars/`](../libs/download_alpaca_end_of_day_bars/) | CLI planning, credentials, HTTP, pagination, rate limiting, and adjustment acquisition. |
| Frozen source and hard integrity | [`libs/alpaca_end_of_day_pipeline/`](../libs/alpaca_end_of_day_pipeline/) | Gzip/sidecar publication, cache state, validation, authenticated aliases, and immutable-boundary errors. |
| Identity evidence | [`libs/alpaca_security_lineage/`](../libs/alpaca_security_lineage/) | Read-only Alpaca asset and corporate-action traversal and symbol disposition. |
| Correlation and master identity | [`libs/alpaca_end_of_day_security_correlation/`](../libs/alpaca_end_of_day_security_correlation/) | Ownership intervals, row partition, episode partition, master-ID derivation, and sharded catalogue publication. |
| Advisory checks | [`libs/alpaca_end_of_day_warning_pipeline/`](../libs/alpaca_end_of_day_warning_pipeline/) | Cross-adjustment and security-history warning overlays. |
| Internal authenticated read | [`libs/alpaca_end_of_day_reader/`](../libs/alpaca_end_of_day_reader/) | Dependency authentication, source conflict handling, detailed result, and operator status queries. |
| Online evaluation | [`libs/alpaca_end_of_day_ticker_pipeline/online_status.py`](../libs/alpaca_end_of_day_ticker_pipeline/online_status.py) | Exercise every advertised read and classify final availability. |
| Shared primitives | [`libs/common/`](../libs/common/) | Atomic writes, hashing, pure rule evaluation, and the tracked US-market calendar. |

Tracked root Python files are executable entrypoints. The supported consumer implementation lives in
[`alpaca_end_of_day/`](../alpaca_end_of_day/__init__.py); other domain behavior belongs in its owning library, and
`libs/common/` imports no application package.

## Repository layout

| Path | Role |
|---|---|
| [`alpaca_end_of_day/`](../alpaca_end_of_day/__init__.py) | The only supported Python import for consumer applications. |
| [Root command files](../run_alpaca_end_of_day_ticker_pipeline.py) | Thin, executable adapters for the maintained CLIs; the complete inventory is in [Operations](operations.md#command-map). |
| [`libs/`](../libs/) | Domain implementations and operator-inspection interfaces. These are not supported consumer imports. |
| [`tests/`](../tests/) | Unit and contract tests built around temporary repositories and HTTP doubles. |
| `data/` | Ignored runtime state: frozen responses, receipts, correlation outputs, warnings, and the published catalogue. It is not part of a Git clone. |
| [`resources/`](../resources/) | Tracked, machine-read project resources: runtime-validated version changelogs and the deliberate recovery-universe snapshot. It is not runtime market data. |
| [`docs/`](.) | Maintained project guidance and version history; `README.md` and tooling-required `AGENTS.md` remain at the repository root. |
| [`pyproject.toml`](../pyproject.toml) | Ruff configuration and the declared Python syntax target. |

`resources/` travels with source control, while `data/` must be restored or regenerated before the public library can
serve a fresh checkout. Exact runtime paths and schemas live in
[Technical contracts](technical-contracts.md#runtime-artifact-layout); recovery choices live in
[Operations](operations.md#backup-and-recovery).

## Core invariants

- Frozen CSV bytes are never silently repaired or rewritten. An authorized publisher may atomically replace a path;
  authenticated byte-identical views may share an inode while retaining distinct provenance manifests.
- Every source row appears exactly once in the correlated or uncorrelated branch. Ambiguity remains explicit.
- A ticker is a dated label, not permanent identity. Rules operate on generic authenticated evidence and never on a
  hard-coded ticker exception.
- A public/master ID is the SHA-256 digest of sorted unique Alpaca asset-ID membership. Descriptive metadata helps
  establish membership but does not change the digest for a fixed member set.
- A UUID-less market-data series may receive a non-security series ID for retention, but it never enters the master
  catalogue.
- Online means that the published identity has bars, every selected dependency authenticates, every advertised
  adjustment reads successfully, and no integrity error, source conflict, or unresolved session/identity boundary
  remains. Provider `active` or `tradable` metadata does not define online status.
- Verified venue suspensions and tightly evidenced same-security rename boundaries may explain otherwise missing
  sessions. Unexplained internal sessions keep an identity offline.
- Integrity failures preserve and describe the source rows. Advisory warnings do not alter returned values.
- Readers never update, repair, download, or contact Alpaca.

The exact evidence gates and artifact schemas live in [Technical contracts](technical-contracts.md). Consumer
semantics live in [Public API](public-api.md), and operational mutation rules live in [Operations](operations.md).
