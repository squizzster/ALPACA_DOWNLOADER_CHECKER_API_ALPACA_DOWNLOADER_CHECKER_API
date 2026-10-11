# Version tracker

This repository versions independent behavioral and serialized boundaries rather than one package release. There are
currently no Git tags. Runtime validators use the JSON changelog arrays linked below; this file is the human-readable
index for pending work, current compatibility, and important milestones.

## Unreleased

### Added

- Public API v1 in [`alpaca_end_of_day.py`](../alpaca_end_of_day.py): online-only catalogue access, name and identifier
  search, dated ticker resolution, typed exact histories, snapshot generation checks, and disk-backed flat CSV export.
- A standalone read-only catalogue audit authenticates every published source and identity shard, checks global UUID
  membership and source closure, and reports all cross-catalogue findings without changing runtime data.
- A tracked, one-record recovery universe in [`artifacts/ticker.csv`](../artifacts/ticker.csv) captures the authenticated
  source symbols needed to seed a fresh acquisition.
- Correlation v9-v20: generalized stable-candidate fallback, ticker-reuse gap detection, authenticated session
  exclusions, complete and inactive current-suffix rules, dated terminal suffixes, one-session distinct-UUID
  transitions, continuous UUID episode partitioning, authenticated current-symbol reuse, and separation of ticker
  suffix allocation from authenticated UUID-lineage membership.
- Root [agent instructions](../AGENTS.md) and this tracker.

### Modified

- Security master v5 can authenticate a deliberate set of supported correlation generations during incremental
  migration without changing IDs, availability, or frozen bindings.
- Internal reader v6-v7 resolves adjusted overlaps only through strict-superset coverage and resolves a remaining
  disagreement only through one exact dated historical-ticker source.
- Multi-worker ticker orchestration serializes provisional master publication, online evaluation, and final
  availability publication as one in-process catalogue-finalization transaction.
- Correlation v20 preserves authenticated predecessor and successor UUID edges for long-gap suffix assignments while
  reserving edge suppression for explicit identity partitions.
- The README and topic guides now distinguish the online public library from internal offline inspection, document
  company-name search and flat export, and give each architectural or operational explanation one owner.
- Runtime changelogs now live under [`artifacts/`](../artifacts/), while architecture and version-tracking documents
  live under this `docs/` directory.

### Removed

- Removed the internal root compatibility facades `alpaca_end_of_day_pipeline.py` and
  `us_equity_market_session_calendar.py`; repository code and tests use their owning packages under `libs/`.
- Removed consumer guidance that imported `libs.alpaca_end_of_day_reader` or enumerated offline states. Those internal
  functions remain available to operators and pipeline evaluation; they are no longer described as application APIs.
- Removed the obsolete statement that company-name search was outside the public contract. Current search is
  case-insensitive normalized substring matching; it is not typo-tolerant fuzzy matching.

## Verification status

Status recorded **2026-10-11 UTC** for the current working tree:

- `ruff check --exclude figi_test.py .`: passed. The prescribed `ruff check .` is not green solely because the
  deliberately untracked experimental `figi_test.py` has one import-order finding; that file was left untouched.
- `ruff format --check --exclude figi_test.py .`: passed for 103 Python files. The prescribed
  `ruff format --check .` likewise reports only that untracked experimental file as unformatted.
- `git diff --check`: passed.
- A custom documentation check validated all local links, anchors, and paths across the Git-visible Markdown files;
  the README directly indexes every other maintained document.
- All six maintained root command-line entrypoints returned current `--help` output matching the documented commands
  and options.
- Version/changelog validation passed for pipeline v19, correlation v20, master v5, warnings v6, reader v7, and public
  API v1.
- A custom secret-pattern scan found zero matches in the 115 tracked files plus the three new audit files; the
  deliberately untracked experimental `figi_test.py` was outside this handoff scan.
- Correlation v20 was published for the eight reports identified by the read-only comparative evaluation. Three now
  retain their authenticated successor UUID and five retain authenticated ticker aliases carried by the same UUID.
  Every one of their 64 frozen bar views was an authenticated cache hit at the pinned `2026-10-06` request boundary.
  The aggregate content, size, and nanosecond-mtime digest over those bar files and sidecars remained
  `029a245856b6e4b0b57db869dafa466a15fe1a54c78578a84d01fd163be3bd71`.
- The post-publication read-only catalogue audit authenticated all 18,092 source shards and 13,483 identity shards
  against generation `ceb3574445531d66bfc312cdf928d62d90b9ae21843abc637e6b186d7bc8e68b`. Duplicate-UUID errors fell from 14 to 11;
  one concurrent online active-ticker warning remains. Source closure, member projection, history projection, and
  snapshot stability all pass.
- The local public snapshot exposes 13,103 online IDs. The audit itself changed no runtime data.
- `python3 -W error::ResourceWarning -m unittest discover -s tests -v`: passed all 179 tests.

There is no repository CI workflow or configured Markdown/link checker. Local verification is the current gate.

## Current version matrix

| Boundary | Current working tree | Baseline `85ca700` | Status and compatibility |
|---|---:|---:|---|
| Public consumer API | 1 | absent | New, unreleased boundary. |
| Frozen validation pipeline | 19 | 19 | Unchanged from the Phase 1 baseline. |
| Security correlation | 20 | 8 | Versions 8 through 20 are accepted by the current master migration path. |
| Security master | 5 | 4 | v5 adds multi-correlation-version migration support. |
| Warning pipeline | 6 | 6 | Unchanged from the Phase 1 baseline. |
| Detailed internal reader | 7 | 5 | v6-v7 add deterministic source-conflict selection. |
| Lineage report | 3 | 3 | Unchanged; no separate runtime changelog. |
| Security ID scheme | 1 | 1 | SHA-256 of sorted unique Alpaca asset IDs. |
| Verified session exclusions | 1 | absent | Introduced with correlation v12. |

The exact current constants live in their owning modules. See [Development](development.md#versioned-boundaries)
for the version-to-file map.

## Baseline milestone

### 2026-10-09 — Phase 1 complete (`85ca700`)

The committed baseline completed frozen acquisition, hard integrity validation, identity correlation, sharded
master-ID publication, warning overlays, persisted online/availability status, and the detailed authenticated reader.
Its boundary versions were pipeline v19, correlation v8, master v4, warnings v6, reader v5, and lineage report v3.
Runtime data, credentials, ticker universes, logs, and experimental files remained outside Git.

## Contract history

### Public API

- **v1 — Unreleased:** established the online-only `DataLibrary`, typed catalogue and bars, exact and dated lookups,
  generic unavailable-ID behavior, name/identifier search, and raw-plus-all flat CSV export.

Exact history: [`alpaca_end_of_day_api_change_log.json`](../artifacts/alpaca_end_of_day_api_change_log.json).

### Frozen validation pipeline

- **v1-v6:** streaming gzip publication, cache generations, market-calendar coverage, changelog validation, recovery
  modes, and strict row validation.
- **v7-v16:** separated immutable source ownership from integrity findings, authenticated every local use, split
  receipts from failures, modularized ownership, and deferred missing-session interpretation until identity was known.
- **v17:** added adaptive adjustment acquisition, explicit empirical aliases, and authenticated hard-link compaction.
- **v18-v19:** defined accepted zero-activity bars, split-adjusted volume semantics, and typed invalid-symbol failures.

Exact history: [`alpaca_end_of_day_pipeline_change_log.json`](../artifacts/alpaca_end_of_day_pipeline_change_log.json).

### Security correlation

- **v1-v6:** established exhaustive row partitioning, ordered ownership rules, dated UUID attribution, branch manifests,
  and action-supported UUID transitions.
- **v7-v9:** introduced deterministic security IDs, continuous-ticker fallback, evidence-derived symbol disposition, and
  stable same-ticker candidate sets.
- **v10-v16:** evolved long-gap handling from candidate detection into fail-closed complete, inactive, and dated
  terminal suffix rules, with authenticated venue-session exclusions.
- **v17-v19:** added one-session distinct-UUID transitions, separate continuous UUID episodes, and the complete
  current-symbol run after authenticated ticker reuse.
- **v20 — Unreleased:** preserves authenticated UUID lineage for long-gap suffix row assignments and suppresses lineage
  edges only for explicit identity partitions.

Exact history:
[`alpaca_end_of_day_security_correlation_change_log.json`](../artifacts/alpaca_end_of_day_security_correlation_change_log.json).

### Security master

- **v1:** published the global digest-bound master-ID lookup.
- **v2:** replaced its monolithic payload with immutable source/security objects and prefix-bucket inventories.
- **v3-v4:** persisted `online` and compact availability status, including `lineage_gap`.
- **v5 — Unreleased:** permits supported correlation generations during incremental derived-catalogue migration.

Exact history: [`alpaca_security_master_change_log.json`](../artifacts/alpaca_security_master_change_log.json).

### Internal reader

- **v1-v3:** moved from UUID reads to deterministic master IDs and made the authenticated master catalogue the sole
  discovery root.
- **v4-v5:** exposed persisted online/status queries for internal consumers and kept lineage-gap histories available to
  operators.
- **v6-v7 — Unreleased:** added strict-superset adjusted-source selection and dated historical-ticker selection while
  keeping every remaining ambiguity fatal.

Exact history: [`alpaca_end_of_day_reader_change_log.json`](../artifacts/alpaca_end_of_day_reader_change_log.json).

### Warning pipeline

- **v1-v3:** established digest-bound cross-adjustment and security-history warnings scoped to correlated identities.
- **v4-v6:** supported empty correlated sources, excluded accepted zero-activity rows from numerical analysis, and
  retained valid split-volume zeros.

Exact history: [`alpaca_end_of_day_warning_pipeline_change_log.json`](../artifacts/alpaca_end_of_day_warning_pipeline_change_log.json).
