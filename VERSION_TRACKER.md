# Version tracker

This repository versions independent behavioral and serialized boundaries rather than one package release. There are
currently no Git tags. Runtime validators use the JSON changelog arrays linked below; this file is the human-readable
index for pending work, current compatibility, and important milestones.

## Unreleased

### Added

- Public API v1 in [`alpaca_end_of_day.py`](alpaca_end_of_day.py): online-only catalogue access, name and identifier
  search, dated ticker resolution, typed exact histories, snapshot generation checks, and disk-backed flat CSV export.
- Correlation v9-v19: generalized stable-candidate fallback, ticker-reuse gap detection, authenticated session
  exclusions, complete and inactive current-suffix rules, dated terminal suffixes, one-session distinct-UUID
  transitions, continuous UUID episode partitioning, and authenticated current-symbol reuse.
- Root [agent instructions](AGENTS.md) and this tracker.

### Modified

- Security master v5 can authenticate a deliberate set of supported correlation generations during incremental
  migration without changing IDs, availability, or frozen bindings.
- Internal reader v6-v7 resolves adjusted overlaps only through strict-superset coverage and resolves a remaining
  disagreement only through one exact dated historical-ticker source.
- Multi-worker ticker orchestration serializes provisional master publication, online evaluation, and final
  availability publication as one in-process catalogue-finalization transaction.
- The README and topic guides now distinguish the online public library from internal offline inspection, document
  company-name search and flat export, and give each architectural or operational explanation one owner.

### Removed

- Removed consumer guidance that imported `libs.alpaca_end_of_day_reader` or enumerated offline states. Those internal
  functions remain available to operators and pipeline evaluation; they are no longer described as application APIs.
- Removed the obsolete statement that company-name search was outside the public contract. Current search is
  case-insensitive normalized substring matching; it is not typo-tolerant fuzzy matching.

## Verification status

Status recorded **2026-10-10 UTC** for the current working tree:

- `ruff check .`: passed.
- `ruff format --check .`: passed for 102 Python files.
- `git diff --check`: passed.
- A custom documentation check validated all 114 local links, anchors, and paths across eight Git-visible Markdown
  files; the README directly indexes all seven other maintained documents.
- All five root command-line entrypoints returned current `--help` output matching the documented commands and options.
- Version/changelog validation passed for pipeline v19, correlation v19, master v5, warnings v6, reader v7, and public
  API v1.
- A custom secret-pattern scan found zero matches in 115 Git-visible files.
- All 11,040 source shards, 8,809 identity shards, 11,040 correlation outputs, and 11,040 warning outputs authenticated
  against local generation `aaa20b07cc5ee9156d346ecc0c299a5b4a15b4e4aea827a2f2a60faf0134ce89`.
- The local public snapshot exposed 8,782 online IDs; all eight STRC adjustment histories returned 298 rows.
- This documentation refactor left all 107 Git-visible non-Markdown files byte-identical to its starting baseline. The
  preceding frozen verification pass also found no size/mtime change among 351,240 runtime data files.

The complete test command is **not green**:

```text
python3 -W error::ResourceWarning -m unittest discover -s tests -v
Ran 170 tests: 168 passed, 2 failed
```

Both failures occur because `ticker_reuse_gap_guard` currently executes before the dated as-of UUID resolver and treats
sparse unit-fixture rows as pricing episodes:

- [`test_asof_selected_meta_identity_beats_unrelated_historical_meta_ticker`](tests/test_alpaca_end_of_day_security_correlation.py)
  presents two rows separated by 502 expected sessions. The guard returns two uncorrelated episodes before the META
  identity-chain rule runs.
- [`test_asof_uuid_branch_routes_reused_b_ticker_to_exact_barrick_uuids`](tests/test_alpaca_end_of_day_security_correlation.py)
  presents a final gap of 71 expected sessions. The guard likewise preempts the Barrick dated UUID routing.

The implementation order is explicit in `ROW_CORRELATION_RULES` in
[`row_correlation.py`](libs/alpaca_end_of_day_security_correlation/row_correlation.py): the gap guard is rule 5 and the
as-of resolver is rule 8. It remains unresolved whether those sparse fixtures should model complete session grids or
whether dated identity evidence should precede the guard. No behavior or test was changed during the verification pass.

There is no repository CI workflow or configured Markdown/link checker. Local verification is the current gate.

## Current version matrix

| Boundary | Current working tree | Baseline `85ca700` | Status and compatibility |
|---|---:|---:|---|
| Public consumer API | 1 | absent | New, unreleased boundary. |
| Frozen validation pipeline | 19 | 19 | Unchanged from the Phase 1 baseline. |
| Security correlation | 19 | 8 | Versions 8 through 19 are accepted by the current master migration path. |
| Security master | 5 | 4 | v5 adds multi-correlation-version migration support. |
| Warning pipeline | 6 | 6 | Unchanged from the Phase 1 baseline. |
| Detailed internal reader | 7 | 5 | v6-v7 add deterministic source-conflict selection. |
| Lineage report | 3 | 3 | Unchanged; no separate runtime changelog. |
| Security ID scheme | 1 | 1 | SHA-256 of sorted unique Alpaca asset IDs. |
| Verified session exclusions | 1 | absent | Introduced with correlation v12. |

The exact current constants live in their owning modules. See [Development](docs/development.md#versioned-boundaries)
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

Exact history: [`alpaca_end_of_day_api_change_log.json`](alpaca_end_of_day_api_change_log.json).

### Frozen validation pipeline

- **v1-v6:** streaming gzip publication, cache generations, market-calendar coverage, changelog validation, recovery
  modes, and strict row validation.
- **v7-v16:** separated immutable source ownership from integrity findings, authenticated every local use, split
  receipts from failures, modularized ownership, and deferred missing-session interpretation until identity was known.
- **v17:** added adaptive adjustment acquisition, explicit empirical aliases, and authenticated hard-link compaction.
- **v18-v19:** defined accepted zero-activity bars, split-adjusted volume semantics, and typed invalid-symbol failures.

Exact history: [`alpaca_end_of_day_pipeline_change_log.json`](alpaca_end_of_day_pipeline_change_log.json).

### Security correlation

- **v1-v6:** established exhaustive row partitioning, ordered ownership rules, dated UUID attribution, branch manifests,
  and action-supported UUID transitions.
- **v7-v9:** introduced deterministic security IDs, continuous-ticker fallback, evidence-derived symbol disposition, and
  stable same-ticker candidate sets.
- **v10-v16:** evolved long-gap handling from candidate detection into fail-closed complete, inactive, and dated
  terminal suffix rules, with authenticated venue-session exclusions.
- **v17-v19:** added one-session distinct-UUID transitions, separate continuous UUID episodes, and the complete
  current-symbol run after authenticated ticker reuse.

Exact history:
[`alpaca_end_of_day_security_correlation_change_log.json`](alpaca_end_of_day_security_correlation_change_log.json).

### Security master

- **v1:** published the global digest-bound master-ID lookup.
- **v2:** replaced its monolithic payload with immutable source/security objects and prefix-bucket inventories.
- **v3-v4:** persisted `online` and compact availability status, including `lineage_gap`.
- **v5 — Unreleased:** permits supported correlation generations during incremental derived-catalogue migration.

Exact history: [`alpaca_security_master_change_log.json`](alpaca_security_master_change_log.json).

### Internal reader

- **v1-v3:** moved from UUID reads to deterministic master IDs and made the authenticated master catalogue the sole
  discovery root.
- **v4-v5:** exposed persisted online/status queries for internal consumers and kept lineage-gap histories available to
  operators.
- **v6-v7 — Unreleased:** added strict-superset adjusted-source selection and dated historical-ticker selection while
  keeping every remaining ambiguity fatal.

Exact history: [`alpaca_end_of_day_reader_change_log.json`](alpaca_end_of_day_reader_change_log.json).

### Warning pipeline

- **v1-v3:** established digest-bound cross-adjustment and security-history warnings scoped to correlated identities.
- **v4-v6:** supported empty correlated sources, excluded accepted zero-activity rows from numerical analysis, and
  retained valid split-volume zeros.

Exact history: [`alpaca_end_of_day_warning_pipeline_change_log.json`](alpaca_end_of_day_warning_pipeline_change_log.json).
