# Version tracker

This repository versions serialized boundaries and reusable transforms independently. There are no Git tags. Runtime
validators use the JSON changelog arrays under `resources/`; array position is the boundary version.

## Unreleased

### Added

- Standalone lineage v1 and `build_alpaca_lineage.py`: one worker; trusted pricing consumption; identical three
  chronological boundaries; approved closed-assignment reuse; established identity rules; missing-pricing delegation;
  affected dependency closure; and recoverable publication before notice acknowledgement. No pricing copies or
  public-catalogue integration. One small SQLite selector updates affected dependency rows, not a global JSON rewrite.
- Standalone pricing v6 separates ordinary trusted handoff from explicit authentication. No pricing layout, limiter,
  lock or notice-schema change; older v1–v5 boundaries remain readable.
- Standalone pricing source v5: at most three whole-ticker workers with one shared paced 199-request/rolling-minute
  budget and shared HTTP 429/5xx cooldown; `--workers 1/2/3`, bounded scheduling, input-ordered results, and compatible
  v4 validator reads. One worker owns its ticker through publication. No new datastore files or layout changes.
- Standalone pricing source v4: one source-bound endpoint validator, post-acquisition first-session check with one
  full restart, removed-date invalidation, and repeated-symbol/file/universe CLI with safe FROM/TO and force options.
  Per-ticker failures continue the batch. The existing store layout/marker remains v3; no additional datastore.
- Standalone pricing source v1–v3 and `download_alpaca_pricing.py`: exact ticker-only acquisition, three compact
  boundaries, first/last-session consistency checks, automatic full replacement, and a locked pending-date handoff.
  No identity/lineage imports or public-reader integration. Retained older stores remain untouched.
- Focused live GOOG rollover, adjustment-vintage, full-refresh, and matching-endpoint checks in ignored `data_pricing/`;
  results and the distinction between live prices and a forced mismatch decision are recorded below.
- Documentation-only [storage rework notes](storage-rework-notes.md) recording the 2026-10-14 decision to retain all
  inventoried stores in place, defer cleanup and further copies, and reuse transformed downloads as effective REST
  source data. The original monthly-retention preference is superseded by the standalone contract.
- A native symbol/year/month source boundary with deterministic gzip output, eight logical adjustment views, direct
  exact-copy aliases, structural source inventories, and selected-month replacement.
- Incremental monthly validation artifacts that reuse unchanged month results and bound predecessor seams.
- Normalized lineage, correlation-envelope, warning-envelope, restartable identity-candidate, availability, and final
  catalogue-publication boundaries.
- A publication transaction with one stable reader/writer lock, a retained recovery marker, and final catalogue
  receipt written last.
- Content-addressed affected-closure catalogue updates that preserve unrelated source/security objects and delay
  reclamation of superseded objects for one additional publication.
- Correlation transform v24, rejecting a name-change UUID edge immediately superseded by a next-day corrective rename
  that preserves the predecessor UUID.
- Clean-room run ledger v1 and `build_alpaca_end_of_day_clean_room.py`: immutable universe/request identity,
  per-label resolution and terminal outcomes, one canonical owner, bounded retries, authenticated recovery without
  reacquisition, mandatory complete audit, and rollback-safe activation retaining the prior public store.
- Documentation of calculation ownership, receipt trust, explicit audit versus ordinary load, boundary isolation,
  reuse keys, and current recomputation granularity.

### Modified

- Documentation-only integration checkpoint records current automated verification and the unfinished live lineage
  cohort below. No correctness verdict, identity-rule change, runtime version bump, or JSON changelog change.
- Documentation-only clarification near the top of the README: public IDs identify securities, not ticker labels;
  FB → META preserves one identity, while unrelated ticker reuse does not. No runtime or JSON changelog change.
- Standalone lineage v2 closes the missing-initial-ticker handoff: use the existing downloader defaults when no
  published range exists, then resume through the normal receipt/notice path. No new datastore or audit; compatible
  v1 receipts, indexes and recovery journals remain readable. Operator commands and contracts now reflect this path.
- Earlier documentation-only reconciliation of the standalone downloader: source representation, compact boundaries,
  actual changed-date notices, shared locking, recovery, and future lineage separation. No runtime version or JSON
  changelog change for that documentation pass. The subsequent v4 implementation has its own single changelog entry.
- Both existing combined-producer entrypoints now invoke the native monthly cascade and select only `data_vtest/`.
- The public `alpaca_end_of_day.DataLibrary` now selects only the monthly store's reader-visible catalogue and reads
  monthly source rows through trusted published boundaries.
- Existing symbols replace only selected month partitions; new symbols acquire all history from 2016.
- Correlation, warnings, identity aggregation, availability, and catalogue publication consume trusted completed
  boundaries rather than re-auditing or recalculating their upstream owners.
- The monthly orchestrator accepts scheduler-owned shared limiters and durable resolutions, can authenticate completed
  acquisition boundaries during clean-room recovery, and can terminate a failed-validation source without mutating
  the public catalogue.
- The human documentation now reflects the actual October 14 code, CLI, runtime layout, trust model, and known scale
  limitations instead of the superseded October 11 layout.

### Removed from the combined-producer contract

- Partial-adjustment selection, adaptive acquisition, `--force`, `--repair`, skip-disposition flags, and worker
  options. Those options remain only in older internal command implementations and are not accepted by the maintained
  combined-producer entrypoints. The independent downloader separately supports `--refresh-all`.
- `data/end_of_day/` fallback or format probing for the public library.
- The claim that the legacy root catalogue-audit command verifies the current application store.

The clean-room ledger is a new serialized operational boundary at version 1 with exactly one changelog entry. Existing
source-through-public boundary versions do not change because their serialized contracts and default focused behavior
are unchanged.

## Verification status

Main integration checkpoint, **2026-10-15 UTC**:

- `ruff check .`, `ruff format --check .` (144 Python files), and `git diff --check`: passed.
- `python3.11 -B -W error::ResourceWarning -m unittest discover -s tests -v`: passed all 375 tests.
- Explicit local documentation check: 188 relative links/anchors across 10 maintained documents passed.
- Two live ten-seed standalone lineage batches have been attempted: 19 processed, with 17 matching retained dated
  assignments and two differences (ASCA and BTN). JTAI reached the three-acquisition-wave limit without publishing
  lineage; its pending pricing notice remains intact. Thirty seeds from the selected 50 remain unattempted.
- These comparisons are evidence, not correctness verdicts. ASCA/BTN differences and JTAI's bounded acquisition
  failure remain open for grouped review; no rules or limits were changed. Batch lists and detailed results remain
  ignored runtime artifacts under `data/lineage_test_candidates/`, not committed datasets.

Standalone lineage v2 missing-seed handoff, **2026-10-15 UTC**:

- `ruff check .`, `ruff format --check .` (144 Python files), and `git diff --check`: passed.
- CLI help, the runtime changelog, and 187 local links/anchors across 10 maintained documents: passed.
- `python3.11 -B -W error::ResourceWarning -m unittest discover -s tests -v`: passed all 375 tests, including
  32 standalone lineage tests. Seven added regressions cover missing seeds, downloader defaults, unlocked delegation,
  no-download waiting, isolated/bounded failure, empty publication, CLI META dependency acquisition and v1 recovery.
- Actual `./build_alpaca_lineage.py --store data_pricing --symbol META`: exit 0, two pricing acquisition waves,
  published and acknowledged FB/META/METV. The same command immediately returned `unchanged` on a second run.
- No identity rules, pricing layout, or audit behavior changed. Live source/lineage additions are described below;
  GOOG and retained older stores were not processed or copied.

Standalone lineage v1 / trusted pricing handoff v6, **2026-10-15 UTC**:

- `ruff check .` and `ruff format --check .`: passed (144 Python files).
- `python3.11 -B -W error::ResourceWarning -m unittest discover -s tests -v`: passed all 368 tests, including 25 new
  standalone lineage tests and the existing 111 standalone pricing tests.
- CLI help, both runtime changelogs, 187 local links/anchors across 10 maintained documents, and `git diff --check`:
  passed.
- Synthetic checks cover trusted handoffs, no cold-price reads or historical rule recalculation on the safe resume
  path, twin rollover, reused ticker separation, full-refresh deletions, metadata-only corrections, dependency
  acquisition/re-entry, bounded failure, empty acquired dependencies, process locking and interruption recovery.
- No live calls, production notices consumed, retained-store changes, or pricing copies during this implementation.
- Documentation and changelog reconciliation follows the working implementation. Public-reader integration and
  universe-scale operational proof remain outside this delivery.

Standalone pricing v5, **2026-10-15 UTC**, against the working tree:

- `ruff check .`: passed.
- `ruff format --check .`: passed for 132 files.
- `python3.11 -B -W error::ResourceWarning -m unittest discover -s tests -v`: passed all 343 tests, including 111
  standalone acquisition, validator, CLI, rate-limit, and handoff tests.
- Standalone CLI `--help`, its runtime-validated v5 changelog, 178 local links/anchors across 10 maintained
  documents, and `git diff --check`: passed.
- Automated tests used synthetic temporary fixtures and HTTP doubles, with separate processes for lock exclusion,
  waiting before provider calls, and process-death release. Both notice recovery and consumer failure are covered.
- New regression tests cover provider changes between probes/acquisition, bounded restart, legacy validator bootstrap,
  zero ordinary cold/year gzip reads, removed-edge notices, force after recovery, damaged owned payload replacement,
  atomic initialization, bounded transport retry, and per-ticker FROM/metadata failures in mixed batches.
- Threaded/virtual-clock regression tests cover worker counts 1/2/3, whole-ticker ownership, aggregate rolling-minute
  pacing, overlapping HTTP I/O, cooldown extension while waiting, numeric/date Retry-After, exhausted 429 cooldown,
  zero-delay retries respecting the rate cap, and compatible v4 validator updates without cold reads.
- Earlier v4 independent review checked source/validator journal binding and identified a malformed-receipt batch-abort case;
  the owning loader now reports it as a ticker error and a regression test verifies continuation.
- No live API calls, runtime dataset mutations, or store copies were made for v4/v5. The earlier GOOG snapshot below
  remains v3; its validator will be created during its next successful update. Universe selection was tested offline.

### Current standalone development store

`data_pricing/end_of_day/pricing/GOOG` has 2,710 effective sessions per view, 2016-01-04 through 2026-10-13:
2,514 completed-years rows, 196 year-snapshot rows, and 9 active-month rows with overlap replaced. Its 12 pricing files
are seven gzip payloads, four receipts, and the stable lock. No staging or publication journal remains.

`data_pricing/end_of_day/manifests/GOOG/update.json` is still pending:
`{"from":"2016-01-04","to":"2026-10-13"}`. The authorized v3 layout move used an in-place directory rename and
preserved payload hashes, inodes, and modification times; no old GOOG copy remains. No lineage consumer has processed
this notice. The original seven-cutoff dataset below was deleted at the user's request before the later live rebuild.

The v2 META handoff test added META, FB and METV in this same store, through 2026-10-14 (exclusive end 2026-10-15).
It acquired META first, discovered related FB/METV, then acquired those inputs and published lineage. All three notices
were consumed only after publication. Pricing has 11 META files, 11 FB files and 10 METV files; lineage has four JSON
files per ticker plus one shared index. No price copies or archives were added.

- META: all 2,711 sessions per view resolved, with FB through 2022-06-08 and META from 2022-06-09 under one ID.
- FB: the reused ticker's 328 sessions from 2025-06-26 resolve to a different ID. Its older 1,620 sessions per view
  remain unassigned under the existing ticker-reuse gap guard (12,960 observations across eight views). This does not
  prevent the META source above from resolving that earlier Facebook history.
- METV: all 1,329 sessions per view resolved to its own ID, with its earlier META label through 2022-01-28 and METV
  from 2022-01-31. A related label is not a shared security ID.

### Live GOOG boundary sequence — 2026-10-14

Historical v1 experiment: the standalone CLI acquired only GOOG into `data_pricing/`, then updated that same directory
in order. Month-only cutoffs meant through month-end. No retained older store was changed and no dataset snapshots
were copied. These counts describe that experiment, not additional retained datasets.

| Through date | CLI exclusive end | Completed-years rows | Year-snapshot rows | Current-month rows | Effective rows | GOOG files |
|---|---|---:|---:|---:|---:|---:|
| 2016-07-31 | 2016-08-01 | 0 | 145 | 20 | 145 | 13 |
| 2016-12-31 | 2017-01-01 | 0 | 252 | 21 | 252 | 13 |
| 2017-01-31 | 2017-02-01 | 252 | 20 | 20 | 272 | 17 |
| 2019-02-28 | 2019-03-01 | 754 | 40 | 19 | 794 | 17 |
| 2024-07-31 | 2024-08-01 | 2,012 | 146 | 22 | 2,158 | 13 |
| 2026-06-30 | 2026-07-01 | 2,514 | 123 | 21 | 2,637 | 13 |
| 2026-10-13 | 2026-10-14 | 2,514 | 196 | 9 | 2,710 | 12 |

Counts are per adjustment, and all eight views had equal row counts at every checkpoint. The year snapshot includes
the active month at consolidation; effective consumption does not count that overlap twice. File counts include
the ticker lock and four manifests, but exclude the store-level manifest. Every checkpoint retained exactly two
subdirectories under GOOG: its active year and active month.

Each checkpoint verified all boundary manifest and payload digests, alias resolution, logical row counts, timestamp
ordering, the effective history, exact file/directory ownership, partition date ranges, and absence of staging or
pending publication. Across updates, all eight views preserved the observations before the previous active month.
Same-year updates retained completed-years file identities, sizes, and modification times. Superseded month/year
directories were retired only after verified consolidation; their observations remained in consolidated boundaries.

The initially attempted inclusive October 14 cutoff (`--request-end 2026-10-15`) received HTTP 403:
`subscription does not permit querying recent SIP data`. The failed request left the published June state unchanged
and no pending/staging files. The user then selected October 13; that update passed. These are live provider queries
with historical date cutoffs, not proof of the exact historical response versions available on those past dates.
October 14 pricing remains unverified; a subsequent same-month live refresh passed as described below.

### Live adjustment and refresh checks — 2026-10-14

- Memory-only GOOG probes compared the same July 1–8, 2022 bars using `asof=2022-07-10` and `asof=2022-07-18`,
  around its July 18 split. Both returned the same five raw bars and the same five split-adjusted bars. July 8 closes
  were raw `2403.37` and split `120.17` under either `asof`. Historical `asof` did not restore pre-split adjustment
  vintage; it is a symbol-mapping parameter.
- The later v2 live store began with 2,709 sessions through October 12. First/last probes matched, then a test-only
  in-memory override forced the mismatch decision. Eight real full-history requests replaced it through October 13
  with 2,710 sessions per view, exactly matching those fresh responses. Prices were never fabricated or altered.
- A subsequent unmodified update matched all 16 endpoint probes and re-requested the active month. Completed-years
  and year payload hashes, inodes, and modification times were unchanged. No prior-price archive was retained.
- v3 added the split paths and pending notice without new API calls. Synthetic tests cover automatic mismatches,
  changed-date accuracy, full replacement, recovery, and locking. A naturally occurring adjustment between two live
  snapshots has not yet been observed.

### Earlier combined-pipeline verification

Status recorded **2026-10-14 UTC** against commit `7788875` with the documentation reconciliation in the working
tree:

- `ruff check .`: passed.
- `ruff format --check .`: passed for 116 files.
- `python3.11 -B -W error::ResourceWarning -m unittest discover -s tests -v`: passed all 227 tests.
- All local paths and heading anchors resolve across the 9 tracked Markdown files.
- `git diff --check`: passed.
- All eight monthly boundary changelogs validated at source v2, lineage source v2, validation v3, correlation envelope
  v2, warning envelope v2, identity candidate v2, availability v1, and catalogue publication v2.
- Clean-room ledger changelog v1 validated. A fresh one-label provider-only integration acquired AAPL, authenticated
  every boundary, and read 21,680 logical observations through all eight public adjustments. Re-running after the
  acquisition checkpoint made no provider request and completed the audit successfully.
- The current public library opened security-master generation
  `8b38acdffa483f7916fd6bd4ef5db3d51deaf96b87a67f6c2dc05af394570c3c`, published
  `2026-10-14T02:18:10Z`, with 8 online identities.
- The reader-visible catalogue receipt reported 11 source contributions and 8 total identities; all 8 were online.
- The current runtime store occupied approximately 29 MiB and contained 11 source directories.
- `git status --short --branch` was clean on `main` and aligned with the local `origin/main` reference before these
  documentation-only edits.

The preceding checks establish unit and focused contract behavior. They do not establish full-universe performance or
complete current-store audit coverage.

## Current version matrix

| Boundary or transform | Version | Role |
|---|---:|---|
| Standalone pricing source | 6 | Whole-ticker workers, shared rate budget, compact storage, endpoint validator, and trusted pending-date handoff. |
| Standalone lineage | 2 | One-worker identity twin, initial/related pricing handoff, chronological reuse and publish-before-acknowledge recovery. |
| Clean-room run ledger | 1 | Immutable scope, checkpoints, exhaustive audit, completion, and activation authority. |
| Public consumer API | 1 | Online-only typed consumer surface. |
| Monthly pricing source | 2 | Month storage, aliases, inventories, and selected-month replacement. |
| Monthly validation | 3 | Reusable month results and predecessor seams. |
| Lineage source | 2 | Normalized evidence bound to source and validation generations. |
| Correlation envelope | 2 | Compact exhaustive partitions and correlation-owned identity evidence. |
| Warning envelope | 2 | Compact advisory outputs bound to immediate input generations. |
| Identity candidate | 2 | Materialized snapshot or affected-closure aggregation. |
| Availability | 1 | Per-candidate online/offline decision. |
| Catalogue publication | 2 | Snapshot and bounded affected-closure catalogue selection. |
| Validation transform | 19 | Established daily-bar and dataset rules. |
| Correlation transform | 24 | Established ownership, row allocation, and UUID-membership rules. |
| Catalogue engine | 5 | Content-addressed source/security objects and inventories. |
| Warning transform | 6 | Established advisory rules. |
| Detailed history transform | 7 | Established source-selection and history assembly semantics. |
| Lineage report | 4 | Bounded name-change evidence discovery. |
| Security ID scheme | 1 | SHA-256 of sorted unique Alpaca asset UUIDs. |
| Verified session exclusions | 1 | Authenticated venue-session evidence format. |

Exact constants and changelog paths are listed in [Development](development.md#versioned-boundaries).

## Known gaps

Standalone pricing and lineage are implemented; public-reader integration is not.
Lineage's no-cold-read continuation currently covers the explicit single-identity reuse gate. Context-dependent
multi-identity cases and rollover calculate the affected chronology using trusted inputs, not an upstream audit.
Older metadata corrections outside the resumed month window require explicit refresh. The lineage twin is verified
with synthetic fixtures and the focused live META/FB/METV handoff above; no universe-scale run has been performed.
Endpoint checks are the chosen refresh policy, not exhaustive historical correction detection. Live verification is
limited to GOOG and the META-related closure; no standalone full-universe performance claim is made.
The batch runner has at most three ticker workers with one shared 199-request/minute limiter. A normal populated-ticker update uses up to 32
requests before pagination/retries: 16 initial probes, eight acquisitions, and eight final first-session probes.
For 18,092 such tickers that implies roughly 48.5 hours at the configured rate, before latency. `--all` is functional,
not yet demonstrated as a daily universe service. Request batching/performance work is separate from this boundary change.
Workers overlap latency but do not multiply the request allowance. The allowance is process-local, not shared across
independent command invocations using the same credentials. The worker implementation has not yet been exercised live.

The existing combined catalogue workflow has these separate gaps:

1. **No full-universe operational proof yet.** The recorded catalogue snapshot contains 11 source contributions, not the
   tracked 18,092-label recovery universe.
2. **Focused cohorts still use one long transaction.** The focused producer holds its publication lock during provider
   requests. The clean-room scheduler avoids universe-wide lock/memory scope by using one canonical transaction in an
   isolated store.
3. **Sequential acquisition.** The scheduler deliberately shares the two 199-request-per-minute limiters and processes
   canonical owners sequentially. It is restartable and bounded, but not fast.
4. **No generic audit-only command.** The exhaustive current-format audit exists as the mandatory final clean-room
   phase and is bound to that run ledger. It does not yet accept an arbitrary restored store without a ledger.
5. **Focused lineage reacquisition precedes reuse.** A focused update contacts Alpaca before semantic equality is
   known. Clean-room recovery authenticates and skips a complete lineage checkpoint.
6. **Coarse changed-input recomputation.** A changed input recalculates correlation and warnings for the complete
   processing-symbol closure; those outputs are not incrementally partitioned by month.
7. **Interpreter mismatch in legacy adapters.** Unqualified `python3` is 3.9.25 in the current environment while three
    legacy root commands use a `python3` shebang. Invoke them with `python3.11` when deliberately inspecting legacy
    state.
8. **Historical release naming remains.** Internal package paths, helper names, maintained command help, and
    serialized boundary strings still expose an older release label. It is retained compatibility naming, not a
    second pipeline or the name of the current architecture.
9. **No monotonic focused request-end guard.** An existing source can be updated with an earlier exclusive end, replacing its
    selected terminal month partition with a shorter history. Operators must pin and review this boundary explicitly.
10. **Activation assumes one producer.** Existing readers are drained through the store lock and a failed public smoke
    check rolls back, but operators must not start another producer or activator during the directory selection swap.

## Milestones

### 2026-10-15 — Whole-ticker workers and shared request allowance (working tree)

v5 adds a maximum of three independent ticker workers, sharing one paced rolling-minute budget and retry cooldown.
Each worker owns its complete ticker boundary. Store layout, payload deduplication, ticker locks, and lineage handoff
are unchanged; v4 validators remain readable. No live acquisition or runtime-store mutation is part of this change.

### 2026-10-15 — Standalone batch CLI and endpoint validator (working tree)

v4 adds one per-ticker validator inside the established layout, safe date/selection options, isolated batch failures,
and bounded post-acquisition consistency checks. Automatic refresh and force remain clean replacements; clipped-edge
retention is deferred in a code comment. Runtime data is unchanged by this implementation.

### 2026-10-14 — Standalone pricing and future lineage handoff (working tree)

The stable v3 contract separates ticker acquisition from identity work, replaces permanent monthly history with
three compact boundaries, checks first/last sessions before updating, and publishes one locked pending date range.
GOOG is the sole live development ticker. The existing combined pipeline remains separate.

### 2026-10-14 — Native monthly cascade published (`7788875`)

The maintained producer and public reader moved to one monthly store. Eight logical adjustment views are represented
per month through deterministic physical payloads and direct manifest aliases. Validation became incrementally
month-scoped, later boundaries became generation-bound silos, candidate and availability became durable boundaries,
and affected catalogue publication preserved unrelated objects. Correlation transform v24 added the superseded-rename
UUID-membership veto.

The admission runtime contained 11 source contributions and 8 online identities. This milestone proved focused
boundary behavior, not universe-scale operation.

### 2026-10-11 — Correlation v23 catalogue handoff (`ab6715a`)

The prior flat frozen-source system completed its focused overlap and lineage replays. Its exhaustive legacy catalogue
audit passed with 13,476 identities, 13,121 online identities, 97 lineage gaps, and 258 other offline identities. That
runtime remains historical evidence under `data/end_of_day/`; it is not selected by the current public library.

### 2026-10-09 — Phase 1 baseline (`85ca700`)

The baseline completed flat frozen acquisition, validation, correlation, warning publication, sharded catalogue
publication, availability, and the detailed internal reader at validation version 19, correlation version 8,
catalogue engine version 4, warning transform version 6, and reader version 5.

## Contract-history summary

### Standalone pricing

- v1: exact ticker REST transformation, three pricing boundaries, rollover, and recoverable publication.
- v2: first/last-session checks across every view and automatic full-history replacement on mismatch.
- v3: separate pricing/manifests paths, actual changed-date notices, pending-range accumulation, and shared locking.
- v4: source-bound first/last validator, bounded first-session recheck, removed-date notices, safe batch/date/force CLI,
  atomic initialization, and transport retry; no store-layout redesign.
- v5: at most three whole-ticker workers with a shared paced 199-RPM budget and shared retry cooldown, bounded
  scheduling, stable batch result ordering, and v4 validator compatibility; no new persistent artifacts.
- v6: ordinary handoff trusts published pricing receipts/aliases; explicit authentication remains separate.

### Monthly boundaries

- Source v1–v2 established monthly placement, deterministic exact-copy aliasing, complete authentication, and
  selected-month replacement.
- Validation v1–v3 established source-bound results, reusable month calculations, and mirrored predecessor seams.
- Lineage source v1–v2 established normalized evidence and source/validation generation bindings.
- Correlation and warning envelopes v1–v2 established compact immediate-input-bound outputs and moved later identity
  evidence into the correlation receipt.
- Identity candidate v1–v2 established a materialized, validated restart boundary supporting snapshot and affected
  closure modes.
- Availability v1 established one read-based decision per candidate identity.
- Catalogue publication v1–v2 established final selection, bounded retention, and affected-closure delta publication.

### Established transforms

- Validation transform v1–v19 defines daily-bar, zero-activity, volume, session, and integrity rules.
- Correlation transform v1–v24 defines exhaustive row partitioning, ticker-reuse and transition rules, UUID membership,
  same-symbol split continuity, overlap selection, fixed-point discovery, and the superseded-rename veto.
- Catalogue engine v1–v5 defines deterministic IDs, content-addressed sharding, persisted availability, and supported
  correlation-generation migration.
- Detailed history transform v1–v7 defines deterministic source selection, provenance, and conflict handling.
- Warning transform versions 1–6 define advisory cross-adjustment and segment-statistics behavior.

The JSON changelog arrays under [`resources/`](../resources/) are the exact machine-validated histories.
