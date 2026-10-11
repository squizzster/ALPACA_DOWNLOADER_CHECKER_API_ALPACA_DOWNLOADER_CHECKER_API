# Version tracker

This repository versions independent behavioral and serialized boundaries rather than one package release. There are
currently no Git tags. Runtime validators use the JSON changelog arrays linked below; this file is the human-readable
index for pending work, current compatibility, and important milestones.

## Unreleased

### Added

- A root [`PIPELINE.md`](../PIPELINE.md) defines the single production path, the ordered correlation cascade, the
  same-symbol split membership rule, and the controls that prohibit migration-specific or ticker-specific rule engines.
- Public API v1 in [`alpaca_end_of_day`](../alpaca_end_of_day/__init__.py): online-only catalogue access, name and identifier
  search, dated ticker resolution, typed exact histories, snapshot generation checks, and disk-backed flat CSV export.
- A standalone read-only catalogue audit authenticates every published source and identity shard, checks global UUID
  membership and source closure, and reports all cross-catalogue findings without changing runtime data.
- A tracked, one-record recovery universe in [`resources/ticker.csv`](../resources/ticker.csv) captures the authenticated
  source symbols needed to seed a fresh acquisition.
- A repository layout map, six-command operator map, and separate restore-versus-rebuild procedures orient a fresh
  maintainer without duplicating implementation contracts.
- Correlation v9-v23: generalized stable-candidate fallback, ticker-reuse gap detection, authenticated session
  exclusions, complete and inactive current-suffix rules, dated terminal suffixes, one-session distinct-UUID
  transitions, continuous UUID episode partitioning, authenticated current-symbol reuse, and separation of ticker
  suffix allocation from authenticated UUID-lineage membership, including exact same-symbol split continuity.
- Root [agent instructions](../AGENTS.md) and this tracker.

### Modified

- Security master v5 can authenticate a deliberate set of supported correlation generations during incremental
  migration without changing IDs, availability, or frozen bindings.
- Internal reader v6-v7 resolves adjusted overlaps only through strict-superset coverage and resolves a remaining
  disagreement only through one exact dated historical-ticker source.
- Multi-worker ticker orchestration serializes provisional master publication, online evaluation, and final
  availability publication as one in-process catalogue-finalization transaction.
- Correlation v20-v23 preserves authenticated predecessor and successor UUID edges for long-gap suffix assignments,
  reserves edge suppression for explicit identity partitions, joins exact same-symbol split UUIDs without using a
  split to route pricing rows, resolves different-UUID ownership overlaps only when one explicit start is uniquely
  latest, and treats overlapping labels carrying one UUID as one security claim.
- The README now leads with the product boundary, indexes every maintained document, states the verified runtime
  requirements, and warns that a fresh clone has no readable runtime catalogue.
- Topic guides now distinguish the online public library from internal offline inspection, document company-name
  search and flat export, give each architectural or operational explanation one owner, and preserve the focused-replay
  hazards that distinguish cached rule application from an intentional data advance.
- The tracked changelogs and recovery universe now live under [`resources/`](../resources/), renamed from the ambiguous
  `artifacts` directory. All seven files retained identical bytes, and their configured readers now use the new path.
  This source-layout change does not alter a serialized or behavioral boundary, so runtime versions are unchanged.
- The public implementation now lives in [`alpaca_end_of_day/__init__.py`](../alpaca_end_of_day/__init__.py), preserving
  the supported `from alpaca_end_of_day import ...` contract while leaving only executable command files in the root.

### Removed

- Removed the internal root compatibility facades `alpaca_end_of_day_pipeline.py` and
  `us_equity_market_session_calendar.py`; repository code and tests use their owning packages under `libs/`.
- Removed consumer guidance that imported `libs.alpaca_end_of_day_reader` or enumerated offline states. Those internal
  functions remain available to operators and pipeline evaluation; they are no longer described as application APIs.
- Removed the obsolete statement that company-name search was outside the public contract. Current search is
  case-insensitive normalized substring matching; it is not typo-tolerant fuzzy matching.
- Removed the duplicate component-ownership matrix from the development guide and the repeated catalogue-audit rule
  list from operations; each now links to its authoritative architecture or technical-contract section.
- Removed the non-CLI `alpaca_end_of_day.py` file from the repository root; its import-compatible package replacement
  retains the same public names and default repository-root behavior.

## Verification status

Status recorded **2026-10-11 UTC** for this handoff:

- `python3 -W error::ResourceWarning -m unittest discover -s tests -v`: passed all 191 tests.
- All six changelog validators passed at pipeline v19, correlation v23, master v5, warnings v6, reader v7, and public
  API v1 from their new `resources/` paths. The seven renamed resource files are byte-identical to their former paths;
  `ticker.csv` remains one record containing exactly 18,092 sorted, unique authenticated source symbols.
- All six maintained root commands returned current `--help` output matching the documented command map. They are the
  only tracked root Python files; the import-only public library resolves from `alpaca_end_of_day/__init__.py`.
- A custom documentation check validated local links, anchors, and referenced paths across all nine maintained
  Markdown files. The README directly links the other eight documents, and no source or documentation reference to
  the former directory as a live path remains.
- `git diff HEAD --check` passed.
- `ruff check --exclude figi_test.py .` passed, and `ruff format --check --exclude figi_test.py .` passed for 104 files.
  The prescribed unexcluded commands are not green solely because the deliberately untracked experimental
  `figi_test.py` has one import-order finding and is unformatted; that file remains untouched.
- All 664 frozen adjustment views for the 83 split-focused source symbols authenticate. The 71 requested replays retain
  the exclusive `2026-10-06` request boundary; the 12 data-advanced symbols retain `2026-10-10`. Their correlation
  outputs authenticate at supported versions, including SNTI at v22. The seven evidenced split pairs each resolve to
  exactly one public ID, including online ONMD ID
  `693b6a276454c2ed7ac9eb53bf23bc3d1635febc57dd9c6b005f46ef73162361`.
- The v22 focused replay authenticated all 40 frozen views for BUFF, SNTI, SPRB, STRR, and ZIVO as cache hits at the
  exclusive `2026-10-06` boundary. BUFF, SPRB, and the current STRR identity are online through `2026-10-05`; SNTI is
  offline with `lineage_gap`; ZIVO remains online under the existing availability contract although its frozen history
  ends `2025-03-21`.
- The canonical complete pipeline replayed the 200 source symbols named by the previous UUID-overlap audit at their
  existing exclusive `2026-10-06` boundary. All 1,600 frozen adjustment views were cache hits, all 200 source
  contributions now authenticate at correlation v22, and no force or data advance occurred. The read-only catalogue
  audit improved from 130 duplicate-Alpaca-UUID errors and 20 active-ticker warnings to one intermediate error and no
  warnings.
- The v23 impact audit authenticated all 18,092 persisted lineage digests. Of 50 untraversed same-UUID returned-symbol
  occurrences, 46 failed the OTC/missing-exchange gate and two echoed a CUSIP; only the ACY and MTMT occurrences
  qualified, both returning MPU. Nine sources contained overlapping labels for one UUID; only ABVEW changed row
  allocation, while GRP.U and SNMP changed provenance.
- The canonical complete pipeline replayed only ACY, MTMT, MPU, ABVEW, GRP.U, and SNMP at their existing exclusive
  `2026-10-06` boundary. All 48 frozen adjustment views were cache hits; no force, repair, replacement, or data advance
  occurred. ACY, MTMT, and MPU now resolve to one online public ID
  `ab75c2187254f39b83823f5359b3f59c37b56dfd9f454c06fd24976e17319197` with all three evidenced UUIDs.
- The exhaustive catalogue audit passed all seven checks with zero errors and zero warnings. The public library opens
  generation `59503660e45fc756a3319a928faababbdc13f9d3c4547f2f2f49fee9cf68d659`, containing 13,476 public IDs,
  13,121 online IDs, 97 `lineage_gap` identities, and 258 offline identities.

There is no repository CI workflow or configured Markdown/link checker. Local verification is the only automated gate.

## Current version matrix

| Boundary | Current working tree | Baseline `85ca700` | Status and compatibility |
|---|---:|---:|---|
| Public consumer API | 1 | absent | New, unreleased boundary. |
| Frozen validation pipeline | 19 | 19 | Unchanged from the Phase 1 baseline. |
| Security correlation | 23 | 8 | Versions 8 through 23 are accepted during incremental publication. |
| Security master | 5 | 4 | v5 adds multi-correlation-version migration support. |
| Warning pipeline | 6 | 6 | Unchanged from the Phase 1 baseline. |
| Detailed internal reader | 7 | 5 | v6-v7 add deterministic source-conflict selection. |
| Lineage report | 4 | 3 | v4 adds bounded fixed-point discovery of an authoritative same-UUID name-change symbol. |
| Security ID scheme | 1 | 1 | SHA-256 of sorted unique Alpaca asset IDs. |
| Verified session exclusions | 1 | absent | Introduced with correlation v12. |

The exact current constants live in their owning modules. See [Development](development.md#versioned-boundaries)
for the version-to-file map.

## Milestones

### 2026-10-11 — Correlation v23 focused replay (`Unreleased`)

Lineage acquisition now follows the one valid non-OTC US-equity symbol returned by both exact same-UUID CUSIP
endpoints of a complete name change. The existing overlap rule treats intervals carrying that same UUID as
noncompeting and defers row provenance to the exact requested-symbol rule. A catalogue-wide read-only impact audit
selected six source symbols; the canonical pipeline replayed only those six from cache at `2026-10-06`. The exhaustive
catalogue audit then passed with zero errors and zero warnings.

### 2026-10-11 — Catalogue UUID-closure replay (`Unreleased`)

The canonical complete pipeline replayed only the 200 source symbols implicated by the 130 duplicate-UUID findings,
using a symbol file and their existing exclusive `2026-10-06` boundary. All frozen inputs were cache hits. The replay
removed 129 duplicate-UUID errors and all 20 active-ticker warnings; one source-local lineage mismatch across MPU,
ACY, and MTMT remained explicit at that intermediate generation and was resolved by correlation v23.

### 2026-10-11 — Correlation v22 focused replay (`Unreleased`)

One rule in the existing row-correlation registry selects the overlapping authenticated ownership interval whose
explicit start is uniquely latest, after stronger as-of lineage evidence and before the requested-symbol fallback. A
tied or wholly undated overlap fails closed, and row routing does not join identities. The canonical pipeline replayed
only BUFF, SNTI, SPRB, STRR, and ZIVO from a symbol file at their existing exclusive `2026-10-06` boundary; all 40
frozen adjustment views were cache hits.

### 2026-10-11 — Correlation v21 focused replay (`Unreleased`)

The exact same-symbol split rule joined seven authenticated UUID pairs without routing or rewriting price rows. The
canonical complete pipeline replayed 83 affected sources: 71 retained the exclusive `2026-10-06` request boundary and
12 retained their already replaced `2026-10-10` boundary. All 664 frozen views were subsequently authenticated as
cache-valid. At that intermediate generation, the later catalogue-wide UUID-closure cleanup was still pending.

### 2026-10-11 — Correlation v20 published (`Unreleased`)

Eight affected reports were republished from authenticated local inputs at the pinned `2026-10-06` request boundary;
all 64 frozen bar views were cache hits. Three reports retained an authenticated successor UUID and five retained a
ticker alias carried by the same UUID. The aggregate content, size, and nanosecond-mtime digest over those frozen bars
and sidecars remained `029a245856b6e4b0b57db869dafa466a15fe1a54c78578a84d01fd163be3bd71`.

### 2026-10-09 — Phase 1 complete (`85ca700`)

The committed baseline completed frozen acquisition, hard integrity validation, identity correlation, sharded
master-ID publication, warning overlays, persisted online/availability status, and the detailed authenticated reader.
Its boundary versions were pipeline v19, correlation v8, master v4, warnings v6, reader v5, and lineage report v3.
Runtime data, credentials, ticker universes, logs, and experimental files remained outside Git.

## Contract history

### Public API

- **v1 — Unreleased:** established the online-only `DataLibrary`, typed catalogue and bars, exact and dated lookups,
  generic unavailable-ID behavior, name/identifier search, and raw-plus-all flat CSV export.

Exact history: [`alpaca_end_of_day_api_change_log.json`](../resources/alpaca_end_of_day_api_change_log.json).

### Frozen validation pipeline

- **v1-v6:** streaming gzip publication, cache generations, market-calendar coverage, changelog validation, recovery
  modes, and strict row validation.
- **v7-v16:** separated immutable source ownership from integrity findings, authenticated every local use, split
  receipts from failures, modularized ownership, and deferred missing-session interpretation until identity was known.
- **v17:** added adaptive adjustment acquisition, explicit empirical aliases, and authenticated hard-link compaction.
- **v18-v19:** defined accepted zero-activity bars, split-adjusted volume semantics, and typed invalid-symbol failures.

Exact history: [`alpaca_end_of_day_pipeline_change_log.json`](../resources/alpaca_end_of_day_pipeline_change_log.json).

### Security correlation

- **v1-v6:** established exhaustive row partitioning, ordered ownership rules, dated UUID attribution, branch manifests,
  and action-supported UUID transitions.
- **v7-v9:** introduced deterministic security IDs, continuous-ticker fallback, evidence-derived symbol disposition, and
  stable same-ticker candidate sets.
- **v10-v16:** evolved long-gap handling from candidate detection into fail-closed complete, inactive, and dated
  terminal suffix rules, with authenticated venue-session exclusions.
- **v17-v19:** added one-session distinct-UUID transitions, separate continuous UUID episodes, and the complete
  current-symbol run after authenticated ticker reuse.
- **v20-v23 — Unreleased:** preserve authenticated UUID lineage for long-gap suffix row assignments, suppress lineage
  edges only for explicit identity partitions, join exact same-symbol split UUIDs without using splits to route
  pricing rows, resolve a different-UUID ownership overlap only through one uniquely latest explicit start date, and
  treat overlapping labels carrying the same UUID as one security claim.

Exact history:
[`alpaca_end_of_day_security_correlation_change_log.json`](../resources/alpaca_end_of_day_security_correlation_change_log.json).

### Security master

- **v1:** published the global digest-bound master-ID lookup.
- **v2:** replaced its monolithic payload with immutable source/security objects and prefix-bucket inventories.
- **v3-v4:** persisted `online` and compact availability status, including `lineage_gap`.
- **v5 — Unreleased:** permits supported correlation generations during incremental derived-catalogue migration.

Exact history: [`alpaca_security_master_change_log.json`](../resources/alpaca_security_master_change_log.json).

### Internal reader

- **v1-v3:** moved from UUID reads to deterministic master IDs and made the authenticated master catalogue the sole
  discovery root.
- **v4-v5:** exposed persisted online/status queries for internal consumers and kept lineage-gap histories available to
  operators.
- **v6-v7 — Unreleased:** added strict-superset adjusted-source selection and dated historical-ticker selection while
  keeping every remaining ambiguity fatal.

Exact history: [`alpaca_end_of_day_reader_change_log.json`](../resources/alpaca_end_of_day_reader_change_log.json).

### Warning pipeline

- **v1-v3:** established digest-bound cross-adjustment and security-history warnings scoped to correlated identities.
- **v4-v6:** supported empty correlated sources, excluded accepted zero-activity rows from numerical analysis, and
  retained valid split-volume zeros.

Exact history: [`alpaca_end_of_day_warning_pipeline_change_log.json`](../resources/alpaca_end_of_day_warning_pipeline_change_log.json).
