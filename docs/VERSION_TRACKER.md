# Version tracker

This repository versions serialized boundaries and reusable transforms independently. There are no Git tags. Runtime
validators use the JSON changelog arrays under `resources/`; array position is the boundary version.

Start with [current state](#current-state), then the [version matrix](#current-version-matrix),
[verification evidence](#verification-status) and [known gaps](#known-gaps). Dated checkpoints describe those runs,
not current worker status or blanket approval of every runtime receipt.

## Current state

**2026-10-18 update:** standalone pricing **v8** and lineage **v6** are implemented and verified.
Repeated requests reuse completed work; changed inputs follow affected ticker links through a disposable work list.
Rule-only lineage changes reuse approved metadata when the complete group still matches.
The [verification checkpoint](#correlation-only-metadata-reuse--2026-10-18) covers the full **748-test** suite.
Correlation remains **v55**; production catch-up and the research backlog below are unchanged.
The [read-only timing preflight](#read-only-timing-preflight--2026-10-18) identified the coupling of rule-only catch-up
to metadata refresh. Lineage **v6** separates those decisions.

As of **2026-10-17**, the accepted lineage code baseline is **`ada3975` / correlation v55**. It includes the earlier
[434-group comparison](#correlation-v52-consolidated-handoff--2026-10-17),
[auxiliary-acquisition/PANW fix](#auxiliary-acquisition-regression--2026-10-17), and these research iterations:

| Group | Version | Status |
|---|---:|---|
| 1 — Retired-label continuation | 53 | Implemented; scoped comparison and selected publication accepted. |
| 2 — Inactive dividend-backed continuity | 54 | Implemented; scoped comparison and selected publication accepted. |
| 3 — Cash-closing boundaries before unrelated reuse | 55 | Implemented; scoped comparison and selected publication accepted. |
| 4 — Overlapping ownership without source scope | — | Selected only; not implemented or accepted. Start with POLY. |

Groups 1–3 recovered **22,438 source-sessions** with no prior-assignment losses/reassignments or existing membership
changes in their comparisons. Eight adjustment views are consistency checks, not eight independent recoveries.
Completing an iteration does not resolve every shortlisted source. Group 4 has **24 sources / 11,941 unresolved
source-sessions** in the mixed-version stored-state screen; that is not a completed v55 replay.

The [consolidated verification](#consolidated-lineage-release-checkpoint--2026-10-17) passed the full suite and real
CLI/reader checks. These are code and selected-publication results, not whole-universe correctness claims.

- **Runtime store:** `data_pricing/` still contains older-rule publications. Normal lineage catch-up is needed before
  current-version reads of those groups; no whole-store catch-up was performed for this release. Use the
  [restart guide](operations.md#restart-and-catch-up), not price copies or manual receipt edits.
- **Consumers:** selected standalone histories are verified. The separate `DataLibrary` catalogue and Bloodhound
  Parquet were not rebuilt by this lineage checkpoint; standalone resolution does not establish online availability.
- **Next development:** investigate Group 4 through the [research/rule loop](development.md#lineage-research-and-rule-improvement-loop).
  Publish supported intervals; leave only unsupported attribution unresolved. Research informs generic deterministic
  rules, never runtime ticker exceptions. Further operating and coverage limits remain in [known gaps](#known-gaps).

## Unreleased

### Added

- Standalone lineage v6 reuses approved metadata for correlation-only recalculation when all group inputs and the
  metadata contract still match. Changed inputs, pending notices, new dependencies and explicit refresh retain their
  acquisition path. Locked absent inputs remain part of the group. No new cache, artifact or correlation rule.
- Standalone pricing v8 / lineage v5 add disposable store-level update hints and a successful-exit downloader batch
  memo. Identical completed date ranges reuse receipts without live probes; advancing the boundary retains endpoint
  checks. Lineage selects changed inputs, affected ticker links and incompatible versions; missing hints restore a
  receipt scan, whose successful exit establishes coverage. Existing receipts remain authoritative and compatible
  v2–v4 lineage decisions remain reusable. No production catch-up or frozen pricing mutation is part of this change.
- Standalone pricing v7 / lineage v4 / public API v10 / operator evidence v3 distinguish exact initial auxiliary
  rejection from successful emptiness and hard acquisition failure. Metadata discovery and identity candidates are
  retained; absent dependencies join locks, reverse closure, recovery and reader/export freshness checks. No price
  copies, ticker exceptions, new identity rules or blanket reprocessing of completed sources.
- Public API v9 / Bloodhound export v7 retain accepted Arrow output in RAM and serialize once at the end, packing row
  groups across IDs. `--memory-mib` (default/maximum 8192) shares retained capacity with a dynamically limited read
  cache and reserves working headroom. Memory exhaustion aborts without omissions or disk spill. No output file is
  created during acquisition; post-serialization freshness, exact values, source policy and atomic publication remain.
- Public API v8 / Bloodhound export v6 add RAM-only cumulative read/reuse counters, conservative decoded-row
  accounting, uncontested-history fast paths, exact alias reuse without source competition, and unchanged-receipt
  reuse under the existing freshness handshake. Fixed projection constraints are compiled once without changing
  arithmetic. The exporter prints the ledger once to stderr after success; no ledger file or footer field is added.
- Public API v7 / Bloodhound export v5 stream completed groups through one locked context, share decoded payloads
  across their IDs and release working data on close. A compact retained token supports final freshness checks without
  reopening lineage evidence. Deterministic physical order is now source group, series ID, date; global collisions
  abort before publication even when earlier groups have staged rows. Equal-view intervals and verified rational
  witnesses are reused under unchanged numeric checks. Consumer, operational and boundary contracts describe this path.
- Bloodhound export v4 permits explicit `--cache-mib 6144` within an 8 GiB process allowance. The default remains
  2048 MiB; only the accepted CLI budget changes, not calculations, acceptance policy or source data.
- Public API v6 and Bloodhound export v3 add bounded generation-selected RAM reuse, exact-view assembly reuse and
  lightweight locked final freshness checks. Export defaults to a 2048 MiB cache; `--cache-mib 0` disables cross-call
  reuse. Numerical calculations, acceptance policy, source selection and schema are unchanged. No disk cache or
  source copies are created; usage, memory headroom and cache authority are documented in their owning guides.
- Bloodhound export v2 replaces `--allow-partial` with `--include-all-passed`: unchanged conversion checks accept or
  reject whole public-ID histories independently; diagnostics go to stderr only. The Parquet and stdout contain no
  omission ledger or partial marker, and footer mappings/provenance describe only identities with published rows.
- Bloodhound export v1: standalone offline 18-column Parquet conversion, exact source OHLC/raw-unit preservation,
  joint eight-view split-factor inference and embedded public-ID mapping. Public API v5 adds metadata-only source/group
  projections and same-snapshot history evidence. No producer/rule change, source rewrite or worker restart.

- Public API v4 adds one-snapshot `StandaloneReader.get_histories`, multiple/repeated reader `--type` values and
  `--all-types`. Single-view output stays compatible; multiple-view JSON groups by adjustment and CSV labels each
  row. Per-call physical-alias reuse avoids repeated gzip reads. No producer/rule versions, store layout, downloads,
  or background-worker restart change; consumer syntax and output contracts are documented in Public API.
- Public API v2 adds `StandaloneReader` and `read_alpaca_pricing.py`: explicit-store, source-scoped resolved-ID
  histories; exact JSON/CSV stdout; metadata-only resolution; shared producer locks with busy-by-default handling;
  no downloads, datastore writes or new catalogue. `DataLibrary` keeps its online-only combined-store contract.
  Reader usage, handoff, lock hazards and verification ownership are documented in their existing guides.
- Documentation-only lineage research-and-rule-improvement procedure in [Development](development.md#lineage-research-and-rule-improvement-loop),
  linked prominently from the compact README and agent instructions: fresh GPT-6.1-Sol High researcher per sequential
  case, main-agent integration responsibility, generic concepts, focused checks and one consolidated batch review.
  Includes the reusable research brief, success criteria, minimal fact retention and evidence-versus-rule reproducibility.
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

- Read-only full-store timing preflight records shared planning cost, the older-rule backlog and a bounded
  stored-evidence calculation sample. The resulting lineage-v6 change separates rule-only recalculation from metadata
  refresh; its contract, execution path, operation and focused verification are documented in their existing homes.
- Documented repeated-run reuse, disposable-cache deletion, changed-input selection, fallback coverage and the
  supported-writer assumption in the existing architecture, pipeline, operational and contract guides.
- Documentation-only current-state refactor: compact README and direct status pointer; one current group/backlog
  summary here; dated verification kept separate; optional Parquet dependency clarified. Detailed ownership, rule,
  identity, locking, recovery and research-loop guidance remains in its existing authoritative files.
- Documentation-only consolidated lineage checkpoint accepts the implemented baseline through correlation v55,
  reconciles Groups 1–3 with the earlier completed comparison/acquisition work, and explicitly leaves the selected
  Group 4 investigation pending. It separates code acceptance, selected publication, whole-store catch-up and
  downstream catalogue/export work; no new rule or runtime-version change is introduced by this verification pass.
- Correlation transform v55 recognizes a traded cash-closing session at the end of a uniquely anchored initial
  episode before unrelated ticker reuse. Rule 4 retains its placement, exact-identity and competing-owner gates;
  untraded event-day rows do not erase the supported prefix. No global interval, membership or datastore redesign.
- Correlation transform v54 extends the existing inactive dividend-history fallback to nearby stock-only departures
  and fully agreeing overlapping payment epochs. It preserves predecessor-only membership, unknown UUID/CUSIP
  vintages, stronger rule precedence and the existing cash-departure path. The upstream eligibility check and a
  production three-pass regression test cover the same shape; no registry reorder or ticker-specific exception.
- Correlation transform v53 adds bounded retired-label continuation after the stronger row rules. A unique provider
  name-change path, agreeing exact successor identity and an informative observed handoff can recover an existing
  component's history without inventing its historical UUID/CUSIP. Later reuse, inert padding, incompatible occupants
  and competing replacements at intermediate nodes remain excluded. Source descriptors include feed/timeframe, and
  terminal fallback reporting derives its position from the registry. Ticker suffixes are not identity rules.
- The contributor loop now records the first evidence-group lessons: screen before grouping, research representative
  and contrasting evidence shapes, distinguish temporal corroboration from exact-price proof, check intermediate
  relationships, and cache approved comparison inputs in RAM before one consolidated review and selected publication.
- Correlation transform v52 accepts a historical split endpoint whose exact UUID/CUSIP has a unique later accepted
  rename to its current lookup label. Rule 4 also resolves a complete current traded suffix anchored by exact dividend
  evidence after an unrelated former issue's departure, excluding the intervening synthetic carry-forward interval.
  Incoming traversal for independently dated donor certificates is scoped before its handoff so later membership
  does not erase earlier provenance. No ticker-specific rule, pricing rewrite or registry reordering is introduced.
- The contributor research loop treats established public facts as the benchmark, separates unanswered questions
  from internal UUID mapping, and scopes uncertainty to unsupported intervals rather than entire histories.
- Standalone lineage v3 separates the one-worker guard from brief group-publication locking. All standalone readers
  use sorted shared pricing locks and bounded re-selection, so unrelated work no longer blocks completed reads.
  Public API v3 adopts that handshake. The SQLite protocol header upgrades once; compatible v2 receipts and month
  continuations remain reusable without price copies, rule changes or a whole-store rebuild. The existing guides
  now describe lock ordering, upgrade/recovery hazards and concurrent read semantics.
- Documentation-only navigation and restart clarification: the README keeps a compact product overview and complete
  document index; Operations owns restart/catch-up selection and reader-blocking consequences; Technical contracts
  owns the unchanged-group gate, version invalidation and lock/recovery protocol. The contributor guide owns the
  documentation handoff checks. Single-worker lineage, default/explicit acquisition effects and retained-store
  restrictions are preserved. No application behavior, runtime version or JSON changelog changes.
- Correlation transform v32–v51 adds dated occupancy anchors, independently attributed informative historical
  intervals, bounded traded episodes, retired-label continuity and inactive component history. It separates cash
  targets from buyer history and corrects membership when an acquirer adopts its target's ticker. Exact versioned
  changes remain in the runtime changelog; the consolidated extended-batch evaluation is recorded below.
- The consolidated comparison exposed an over-broad retired-label inference across stale carry-forwards and an
  empty-calendar-range error for equal action/boundary dates. V51 narrows the new backward inference while preserving
  informative suffixes, and handles zero discrepancy without bypassing the other ownership gates.
- Documentation reconciles those concepts in the ordered cascade and identity contract, and generalizes the
  research loop to an agreed batch boundary with a diminishing-returns stopping decision. No documentation-only
  runtime version increment is required.
- Documentation-only refactor consolidates operational examples and execution explanations into their owning guides,
  documents the real evidence CLI, reconciles the implemented rule cascade and ID-stability limits, and separates
  historical snapshots from current verification. No application behavior, runtime version or JSON changelog change
  is made by this documentation pass; preceding implementation changes retain their own version entries.
- Correlation transform v27 narrows the requested-label reuse guard for one fully dated, same-symbol successor
  chain. The original predecessor retains its existing inferred-lower-bound warning; unrelated reuse and earlier
  ambiguity/gap vetoes remain unchanged. The exact evidence gates live in [Pipeline](../PIPELINE.md#2-row-allocation).
- Correlation transform v26 preserves an existing positively dated ownership start when the same ticker/CUSIP/UUID
  episode has later overlapping arrival evidence. The existing as-of rule prefers a unique explicitly started
  interval for the exact selected UUID before broader predecessor competition. Registry order, ambiguity vetoes,
  identity-edge rules and the membership-hash contract are retained; no datastore or pricing change.
- Documentation records the full stored-source impact, including additional dependency changes and unresolved
  predecessor conflicts, separately from the earlier retained-versus-current comparison.
- Documentation-only checkpoint records the completed live 50-seed lineage run and separates attribution differences,
  membership-only differences and empty histories; detailed runtime reports remain ignored and no verdict is implied.
- Correlation transform v25 scopes latest-start competition to the requested historical label, after stronger
  as-of/reuse rules. Concurrent as-of predecessor branches and competing intervals without source scope fail closed.
  Requested-label fallback also refuses an undated reuse boundary when another UUID occupied the same label.
  Split membership and the membership-hash ID contract are unchanged; new regressions separate those from attribution.
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

### Correlation-only metadata reuse — 2026-10-18

- Narrow lineage-v6 change to the existing group reuse gate: unchanged approved metadata survives a correlation-only
  version change; the affected group still recalculates with current rules. Other invalidation and explicit-refresh
  behavior remains. Exactly one v6 changelog entry was appended; prior entries are unchanged.
- **13 focused tests passed in 377.304 seconds**, including all nine new regression tests and existing arrival,
  refresh, daily continuation and publication-recovery checks. ResourceWarnings were treated as errors. Ruff lint
  and formatting (**193 Python files**) passed; **299 local links/anchors**, all ten maintained documents and complete
  README coverage passed.
- Fresh full gate: **748 tests passed in 1032.095 seconds**, **zero skips**, with ResourceWarnings treated as errors
  under Python **3.11.13**. The saved local output is `experimental_temp/metadata_reuse_20261018_verified.log`.
  Test temporary stores stayed under `/data2/alpaca/experimental_temp`; no production catch-up or pricing mutation
  was run. `git diff --check` passed. The earlier **739-test** result below records the preceding lineage-v5 checkpoint.

### Repeated-run reuse — 2026-10-18

- Fresh full gate: **739 tests passed in 3067.857 seconds**, **zero skips**, with ResourceWarnings treated as errors
  under Python **3.11.13**. The saved local output is `experimental_temp/update_checks_20261018_verified.log`.
  Test temporary stores stayed under `/data2/alpaca/experimental_temp`; no production download, pricing rewrite or
  lineage catch-up was run.
- Ruff **0.15.21** lint and formatting passed for **192 Python files**. Nine maintained CLI `--help` calls passed.
  Local documentation links/anchors, complete README coverage of all ten maintained documents and `git diff --check`
  passed. These remain local checks; the repository has no CI workflow.
- Regression coverage includes five identical downloads without payload reads or network requests; endpoint probes
  on request advancement; affected reverse links and shared UUIDs; provenance-only changes; deleted/recreated hints;
  interrupted publication recovery; and successful-exit batch memo invalidation before a recoverable publication.
  Synthetic **6,000-entry** selections forbid per-ticker processing on unchanged downloader and lineage repeats.
- Exactly one changelog entry was appended for each of pricing **v8** and lineage **v5**; prior entries are unchanged.
  Existing receipts remain authoritative, compatible v2–v4 lineage decisions remain reusable, and correlation/public
  API versions are unchanged. Missing hints restore receipt discovery; only a successful fallback scan records full
  coverage. Concurrent newer hints survive acknowledgement.
- These results establish behavior, not full-store restart timing. Older-rule production publications still need
  legitimate catch-up, and deleting the disposable caches restores discovery cost. The lineage-v6 follow-up is
  recorded above.

### Read-only timing preflight — 2026-10-18

- The existing **9,383,936-byte** index selects **20,035 sources / 14,803 dependency groups**. **20,017 sources /
  14,795 groups** require correlation-v55 catch-up; all indexed identity-report versions and calendar digests already
  match. Correlation counts are v51: **19,752**, v52: **64**, v53: **190**, v54: **11**, v55: **18**. Both disposable
  scheduling caches are absent, so the first normal run still needs fallback discovery and coverage establishment.
- Shared planning used the production index loader, version gate and dependency graph, opening no individual
  receipts or prices. The first observed pass took **23.397 seconds**; four same-process repeats took
  **0.233–0.301 seconds**. Initial Python imports separately took **75.751 seconds**; the later sample's imports took
  **0.080 seconds**. These distinguish first-access and warm costs, not a timed end-to-end producer invocation.
- A stratified **64-source** receipt sample had **64 matching pricing generations**, no pending dated notices and
  the same exclusive end, **2026-10-15**. Those checks took **253.105 seconds** in this environment. Eight complete
  groups (**22 sources**, one to five per group, covering stored correlation v51–v55) then ran the existing three-pass
  correlation and constructed proposed publications in RAM using approved stored evidence. All completed; correlation
  took **0.209–5.545 seconds per group**, with total local load/calculation/construction **2.838–47.556 seconds**.
  This bounded timing sample is not a whole-store impact audit or a reliable linear estimate of catch-up duration.
- Existing dependency links include **22 labels across 20 groups** with no pricing source or lineage index entry.
  A complete catch-up must handle their acquisition/requestability through the existing boundary; an all-store
  `--no-download` run cannot be assumed to finish successfully.
- At this preflight, the v5 `process_group` path treated a correlation-version mismatch as a full metadata-refresh reason. Successful
  metadata acquisition for the **20,017** incompatible sources therefore requires at least **40,034** identity requests
  (one action query and one symbol lookup each). At the configured 199/minute pace this alone implies about
  **3 hours 21 minutes**, before CUSIP lookups, new dependencies, pagination, retries, price acquisition, calculation
  or publication. This is a conditional request floor, not a completion ETA.
- The identified follow-up was to reuse approved metadata for correlation-only recalculation when its own contract,
  input coverage and dependencies remain valid; keep explicit refresh and actual input changes separate. It is now
  implemented in lineage v6, with verification recorded above. The preflight itself made no runtime change or
  production publication. The sample made **zero provider requests** and opened **430 distinct runtime files**,
  including **66 gzip payloads**, below the 5,000-file concern.
  Existing receipts and prices were preserved.
- Ignored evidence: `experimental_temp/full_store_preflight_20261018.json`,
  `experimental_temp/full_store_bounded_probe_20261018.json` and its matching `.py`/`.log`. Runtime code remains the
  code covered by the **739-test** gate at this preflight; the later v6 implementation has a separate verification entry.

### Current-state documentation refactor — 2026-10-17

- Five documentation files changed; no application, runtime-version, changelog or runtime-data changes. Current
  progress has one home above; historical results and detailed contract/recovery/research gotchas are retained.
- **719 tests passed in 62.428 seconds**, zero skips, with ResourceWarnings treated as errors. Ruff lint/format
  (**188 Python files**) and `git diff --check` passed.
- **292 local links/anchors**, all **10 maintained documents**, complete README coverage and its seven-paragraph
  overview checked. Nine maintained CLI `--help` calls passed under Python **3.11.13**; no acquisition was run.
- No repository CI or dedicated Markdown checker exists; these are local checks. Publication/catch-up limits in
  current state and the known gaps remain unchanged.

### Consolidated lineage release checkpoint — 2026-10-17

This checkpoint accepted the accumulated lineage implementation through correlation **v55**, committed in `ada3975`.
The [current-state summary](#current-state) owns group status and remaining work. The verification below did not
repeat the completed same-input impact comparisons or introduce another rule.

- Fresh full gate: **719 tests passed in 62.813 seconds**, **zero skips**, with ResourceWarnings treated as errors.
  Ruff lint and formatting passed for **188 Python files**. All **281 local documentation links/anchors** and complete
  README coverage passed, as did `git diff --check` and runtime validation of the **55-entry changelog**. Its first
  52 entries are unchanged; there is exactly one new entry for each of v53, v54 and v55.
- The normal production CLI, with acquisition enabled and no forced refresh, returned **eight unchanged groups /
  18 sources** in **0.409 seconds**, with zero failures. Selected group generations remain identical. Seeds were
  AT, CAI, FLY, AAAP, AHL, PLT, IVH and BOAS.WS; no pricing acquisition was required.
- Fresh supported-reader consumption passed for **eight public IDs / 7,511 identity-sessions**, all eight views,
  in **0.542 seconds**. It covers all three newly recovered cash-closing histories, their unrelated current occupants,
  and prior-group IVH/BOAS.WS controls. Recorded recovery ranges and counts match, session grids agree, final freshness
  checks pass, and a bare reused CAI request still raises `AmbiguousTickerError`.
- Pricing is unchanged: no copies, repair, redownload or whole-universe run. This verification adds no runtime
  behavior or version. Earlier checkpoints' uncommitted-work notes describe those earlier checkpoints, not this
  consolidated acceptance.

### Correlation v55 cash-closing / ticker-reuse group — 2026-10-17

- Screened 202 gap-blocked histories and selected 63 with exact dated departure candidates. The v54 baseline has
  54,764 unassigned raw source-sessions in that group. One correction recovers **4,240** (7.7%): **AT 1,351**,
  **CAI 1,484**, **FLY 1,405**. The other 60 sources / 50,524 unassigned sessions retain their decisions; different
  shapes are deferred, not forced into this cash-only rule.
- Fresh independent research for each changed case established a cash closing on the last observed session and
  suspension the following trading day. Exact provider CUSIP lookups establish the internal retired identities;
  backfilled current names do not. Confirmed public facts and their limits are retained in the existing ignored
  research file, never used as production ticker/date overrides.
- The full 7,392-source multi-identity metadata screen found 178 cash-departure candidates, narrowed by necessary
  two-occupant/inactive-former/active-current gates to 64 potential sources. Compared these, all 63 group targets and
  controls through the production three-pass path: **117 complete dependency groups / 356 sources**, using the same
  RAM-cached inputs. Only the three intended reports change. All eight views agree on recoveries, with no lost or
  reassigned prior rows, changed existing public IDs/membership, or changed UUID relationship graph. Final diagnostic
  and duplicate-action handling was rechecked on its 12-group / 44-source scope with unchanged assignments.
- Real normal CLI publication covered **18 sources / eight groups** in **40.27 seconds**, with zero failures and
  zero pricing-acquisition waves. Published chronological assignments match the reviewed candidate; normalized
  evidence and pricing generations are unchanged. All eight seeds returned `unchanged` on a normal repeat.
- `StandaloneReader` consumed **eight public IDs / 7,511 identity-sessions** across all eight views in **0.59 seconds**,
  including all three recovered ranges, their separately identified current occupants, IVH and the prior warrant
  control. Freshness checks pass; a reused ticker without an explicit ID remains ambiguous. This does not publish
  combined-catalogue availability or rebuild Bloodhound. No pricing was copied, repaired or redownloaded.
- **719 tests passed in 62.234 seconds**, with ResourceWarnings treated as errors. Focused tests cover cash-event-day
  trades, flat traded prices, untraded partial-history handling, post-event rejection, unknown competing CUSIPs,
  duplicate actions, exact action/interval support, missing views and preserved stock-transition/occupant vetoes.
  Ruff lint/format (188 Python files), all 280 local documentation links/anchors, complete README coverage,
  `git diff --check` and runtime validation of the 55-entry changelog passed. Changes were uncommitted at that checkpoint.

### Correlation v54 inactive-identity group — 2026-10-17

- Closed this group iteration, not every unresolved history. The 41-source multiple-identity group contained 49,067
  unassigned raw source-sessions. One extension of the existing terminal fallback recovers **5,545** (11.3%):
  **IVH 1,809**, **MINDP 2,070**, **TEAF 1,666**. The remaining 38 sources / 43,522 sessions need other evidence
  shapes or retain their existing exclusions; they are not claimed resolved.
- Compared actual v53/v54 production three-pass reports on identical RAM-cached inputs across **38 complete groups /
  168 sources**, plus a **7-source prior-group control** with the candidate disabled/enabled. Every adjustment view
  has the same recoveries, no lost/reassigned previously attributed rows, and no changed existing membership or IDs.
  The three recovered predecessor components exclude ACP, MIND common and TYG respectively. Historical UUID/CUSIP
  vintages remain null. LSXMA and the reused-note counter-shape remain unchanged.
- One metadata-only necessary-shape screen covered all 7,392 indexed multi-identity sources. Of 26 structural
  candidates, the helper changes only the three above; complete affected groups were already in the comparison.
  Fresh independent researchers established the three continuities and LSXMA's distinct issue/reclassification caveat.
  Confirmed facts were retained separately in the existing ignored research file, not injected into production.
- Real CLI publication covered **22 sources / six groups**, with zero pricing acquisition waves. Selected chronological
  documents exactly match the reviewed candidate; pricing generations and normalized evidence are unchanged.
  The development MINDP publication was rerun with metadata refresh after correcting an upstream cash-only eligibility
  precheck. That integration gap is now covered by a production three-pass test, not only a helper test.
  A normal repeat of all six selected seeds returned `unchanged`.
- `StandaloneReader` read **six IDs / 7,609 identity-sessions** across all eight views, including every recovered
  interval and the prior warrant-group control. This proves standalone consumption, not combined-catalogue availability
  or Bloodhound admission. No prices were copied/repaired, and neither downstream product was rebuilt.
- **711 tests passed in 62.391 seconds**, with ResourceWarnings treated as errors. Ruff lint/format (188 Python files)
  passed. Focused tests cover complete overlap equality, unmatched distributions, qualified names, extra occupants,
  tied/contained epochs, stock endpoint/ratio/date guards, and predecessor/acquirer separation.
  All 279 local documentation links/anchors and complete README coverage passed, as did `git diff --check` and
  runtime validation of the 54-entry correlation changelog. Changes remain uncommitted at this checkpoint.

### Correlation v53 first new evidence group — 2026-10-17

- Reviewed the **78-source retired/rename-linked subgroup** using fresh, independent research for the Graf-derived
  warrants, VIACA's delayed processing boundary and BOA/Selina public warrants. Primary records establish the
  external histories; stored provider evidence establishes the internal mapping. Confirmed facts and narrow research
  limits are retained in the existing ignored research reference, never read by production rules.
- The metadata impact screen considered the 20,035 indexed sources, inspected 7,618 with the necessary dependency
  and known-identity shape, and found 945 metadata candidates, mostly already assigned by stronger rules. The only
  additionally unresolved candidate outside the initial dependency closure was LGL.RT; its complete group was added.
  BCACW's group supplies a negative control where related common/unit identities do not establish warrant identity.
- The final same-input production replay covers **68 dependency groups / 387 sources**: the original 66 groups plus
  those two controls. The original 374 sources compare v52 against v53; the additional 13 control sources compare
  the production cascade with the candidate assessment disabled/enabled. Across all eight views,
  **12,653 source-sessions in 32 sources** gain attribution to **30 existing public IDs**. No previously assigned row
  changes owner, label, UUID membership or ID; no remaining unresolved reason
  changes. This is not 12,653 newly created securities, and the eight views are consistency checks, not independent
  ownership confirmations. Existing pricing is unchanged.
- The generic rule is row rule **13**, after existing ownership and gap decisions and before unresolved output.
  Research review caught a missing intermediate-node replacement check; the final code and regression test include
  it. The motivating Graf chain gains 1,107 source-sessions while its inert bridge and unrelated later reuse stay
  unassigned. VIACA gains 24 sessions without merging share classes; BOAS.WS gains 395 public-warrant sessions.
- Actual normal `build_alpaca_lineage.py` publication completed **31 affected/control groups / 197 sources** in
  **348.84 seconds**, with zero failures and zero pricing-acquisition waves. All selected pricing generations,
  normalized evidence and final boundary assignments match the approved comparison. Acquisition was enabled;
  no forced download, price copy or source repair was used.
- `StandaloneReader` read **38 distinct public IDs / 41,136 deduplicated identity-sessions** across all eight views,
  with equal session grids, every recovered interval present and passing final freshness checks. These are the
  30 extended histories plus control identities, not every inherited identity in the dependency groups.
  A second actual CLI run returned **31 unchanged groups**, zero failures and no stderr output.
- **702 tests passed in 62.126 seconds**, with ResourceWarnings treated as errors. Ruff lint/format (188 Python files),
  `git diff --check`, maintained local documentation links/anchors, README coverage and the downloader, lineage,
  evidence, reader and Bloodhound command help checks passed. The converter's executable has no `.py` suffix.
- **Closed scope, not universal resolution:** the target missing-identity category falls from 25,646 to **12,993**
  unassigned source-sessions. A further 1,467 pre-existing undated-reuse sessions are unchanged: **14,460 unresolved
  source-sessions across 47 of the original 78 sources** remain. This includes intentional padding/reuse exclusions
  and still-to-do evidence shapes, not permission to guess or proof that every remaining observation is unresolvable.
- The selected store now has **197 v53, 64 v52 and 19,774 v51 receipts**. Older groups still require normal lineage
  catch-up before reading with the current rules. No whole-store rerun, separate `DataLibrary` catalogue publication
  or Bloodhound regeneration was performed. This checkpoint accepts the bounded rule and selected publication,
  not every unaffected inherited assignment or every unresolved family.

### Correlation v52 consolidated handoff — 2026-10-17

- **Completed the original 414 comparison groups.** A donor-certificate interaction required 27 follow-up group
  comparisons, including 20 additional unique groups: the final union is **434 groups / 1,305 sources**. Both v51
  and v52 used the same approved inputs and the production three-pass fixed point. All eight adjustment views agree
  on the changes; there are no remaining comparison errors. Of these sources, 596 change and 709 remain identical.
- The accepted effects are distinct, not a single resolved-count score:

  | Effect | Source-sessions | Meaning |
  |---|---:|---|
  | Newly attributed | 4,739 | Supported history previously withheld: GLOW 581, GLBL 521, MCVT 141, LOGC 855, MCHB 2,429 and SATLW 212. |
  | Incorrect assignments removed | 5,338 | Thirteen sources' constant zero-volume/zero-trade carry-forwards; these are not observations of a traded security. |
  | Historical label corrected | 1,756 | NHIQ-source history is attributed to its historical NH label. |
  | Membership-only changes | 756,214 | Accepted continuing split membership changes public-ID fingerprints; not a historical owner change. |
  | Unresolved reason clarified | 1,857 | Already-unassigned synthetic bridges remain unassigned with the specific reason. |

  Counts are source-sessions, not deduplicated public-ID histories, and are not multiplied by eight views. Split
  membership, historical ownership and adjustment values remain separate decisions. No source prices were repaired.
- The consolidated check caught two collateral label regressions before acceptance: 475 PROG and 60 CHG sessions.
  Scoping incoming donor traversal before the certified handoff preserves those histories without rejecting valid
  later membership. Fresh primary-source research supported the bounded MCVT, MCHB and SATLW recoveries. Confirmed
  facts and their internal-mapping limits were retained once in the existing ignored research reference.
- Actual `build_alpaca_lineage.py` publication completed for **24 selected groups / 64 sources** in **185.53 seconds**,
  with no failures. This covers every source with a non-membership-only effect plus BIOR, LUXH, META and PANW controls.
  All selected pricing generations and normalized identity evidence are unchanged from the approved comparison.
  PANW's auxiliary delegation reconfirmed the rejected label without withholding PANW; no pricing generation changed.
  Published assignment counts and changed intervals match the accepted comparison across all eight views.
- The supported `StandaloneReader` successfully read **51 distinct public IDs / 65,113 deduplicated identity-sessions**
  across all eight views, with equal session grids and passing final freshness checks. This includes separate GLOW
  ETF and GLOW/OBLG/TWAV histories, the recovered MCHB and SATLW prefixes, PANW versus CYBR, and distinct reused FB
  identities. A second actual CLI invocation returned **24 unchanged groups**, zero failures.
- **692 repository tests passed in 62.331 seconds**, with ResourceWarnings treated as errors. Ruff lint/format
  (186 Python files), `git diff --check`, all **277 local documentation links/anchors**, complete README coverage,
  and the downloader, lineage, evidence, reader and Bloodhound CLI help checks passed.
- **Release scope:** the rule changes and this comparison batch are accepted; the whole unresolved universe is not
  declared solved. Publication was selected-case, not a full-store rerun: **64 receipts use v52; 19,971 still use v51**
  at handoff. The existing reader correctly requires current-rule receipts; those older groups need a normal lineage
  catch-up before consumption with this code. No freshness bypass was added. Neither the separate `DataLibrary`
  online catalogue nor the completed Bloodhound Parquet was rebuilt. Remaining historical questions include the
  earlier GLBL issuer history and LUXH's narrow November 2022 trading/process-date discrepancy; successful reads do
  not certify every unaffected inherited assignment.

### Auxiliary acquisition regression — 2026-10-17

- Traced the complete latest full-run failure set: **27 starting labels** had pricing but no lineage publication.
  Every failure reached a discovered auxiliary which the bars endpoint explicitly rejected. Provider metadata for
  those labels remained useful. The old independent ownership calculation was not the cause of PANW's absence.
- The first new regression test failed against the prior implementation, then passed with the fix. **677 tests passed
  in 63.990 seconds**, with ResourceWarnings treated as errors. Ten focused tests cover the positive case, hard-error
  counterexamples, partial page/view rejection, metadata failure, range expansion, reuse/refresh, recovery, later price
  arrival, cached readers/export tokens and offline evidence. Ruff lint/format (184 Python files), `git diff --check`,
  **277 local links/anchors** and complete README documentation coverage passed.
- Actual `build_alpaca_lineage.py` publication completed for all **27 previously failed starting labels**, spanning
  **20 dependency groups / 33 actual sources**, with zero failures and zero unresolved rows in these groups. Four
  previously missing dependencies were acquired normally: ITRMF, NBRVF, SPIEF and SPIEQ. No existing pricing copies
  were made. **22 rejected auxiliary labels** retain metadata rather than fabricated prices.
- There are **31 newly published sources**, including the four acquired dependencies. The two already published
  warrant sources, ASPSW/ASPSZ, retain their pricing generations, UUIDs and all boundary contents except the schema
  version. The normal public reader successfully consumed **25 public IDs / 42,942 deduplicated identity-sessions**
  across all eight views, with matching session grids and passing final freshness checks.
- PANW has **2,711 sessions**, 2016-01-04 through 2026-10-14, under public ID
  `315fde4f2b19201b2b2e4ed9d8945379f58289a1461b2dde143b7d6d7a122052`, matching its retained identity.
  CYBR remains separate. The previously requested `/tmp/top_500.csv` now has 331 IDs, 169 blanks and no US-country
  blanks. Neither the Bloodhound Parquet nor the separate `DataLibrary` online catalogue was rebuilt.
- **Remaining review, not accepted as solved:** the metadata-only baseline census covered 20,004 lineage receipts;
  2,217 sources contain 1,087,268 unresolved raw source-sessions. Of those sources, 265 have some retained assignments,
  and 44 have fewer assigned sessions now. Counts alone do not adjudicate them. None of the 1,674 sources with zero
  identity candidates had retained assignments either. This acquisition checkpoint used correlation v51. The v52
  handoff above subsequently closes the reviewed evidence shapes; it does not adjudicate all remaining unresolved
  histories or all 44 count decreases. No full-universe rerun was made.

### Offline Bloodhound conversion — 2026-10-16

- Export v7 / API v9, **2026-10-17**: the actual export function passed the real **100-ID / 163,443-row** comparison
  using a RAM-only output adapter (normal published-source reads). All 18 columns, identity mappings and factor reports
  match exactly; one serialization produced two row groups. **360 unique gzip files required exactly 360 decompressions**,
  with no cache eviction. Retained Arrow buffers used 21,905,407 bytes, or 25,182,207 bytes including conservative
  per-table charges. This is equivalence/I/O-count evidence, not a measured full-export disk speedup.
  **667 repository tests passed in 59.339 seconds**, with ResourceWarnings treated as errors; Ruff lint/format
  (182 Python files), actual CLI help and `git diff --check` passed. Targeted tests cover zero acquisition-time output
  files, one packed serialization, post-write freshness, shared cache limits and fatal no-spill memory exhaustion.
  All 274 local Markdown links/anchors and complete README coverage passed. The full RAM-output run launched at
  **2026-10-17 00:41:45 UTC**, with `--memory-mib 8192 --cache-mib 6144 --include-all-passed --wait`, targeting
  `/tmp/alpaca_blood_hound.parquet`. Completion is pending at this launch checkpoint.
  **Completed 2026-10-17 01:29 UTC** (about 47 minutes): **13,534 identities / 18,782,903 rows** published atomically
  to `/tmp/alpaca_blood_hound.parquet` (627,176,451 bytes; 598.12 MiB), after all 14,785 consumed-group checks passed.
  All 20,004 source locators were considered; 84 source/identity checks were rejected under the all-passed policy.
  Peak RSS was **3,483.21 MiB**. Acquisition samples consistently reported zero disk writes; final serialization used
  one `write_table` call and produced **144 packed row groups**. The RAM-only ledger reported no cache evictions and
  20,004 unchanged source-receipt reuses. Final footer-only verification confirmed the exact 18-column schema,
  mapping uniqueness/counts/row totals, accepted-only metadata and no leftover temporary file. No full source reread,
  benchmark output, disk cache, source copy or diagnostic sidecar was created.
  The user-requested v6 interruption exited cleanly before publication at **11,811 accepted IDs / 16,424,286 rows**;
  its temporary file was removed. No source data was changed.
- Export v6 / API v8: **659 repository tests passed in 58.001 seconds**, with ResourceWarnings treated as errors.
  The retained 100-ID / 163,443-row in-memory production-path workload improved from **16.28–16.39 s** to
  **10.94–10.96 s** (about 33% less elapsed time). All 18 columns, identity mappings and factor reports match exactly.
  A further numeric-cache prototype took **11.08–11.09 s**, slightly slower, and was discarded. This established
  diminishing returns; the retained implementation is frozen for the final full export. These timings describe
  this corpus, not a guaranteed full-universe speedup. Trials created no disk benchmark output or source copies.
  Ruff lint/format (181 Python files) and `git diff --check` also passed. The final full export launched at
  **2026-10-16 23:42:06 UTC**, targeting `/tmp/alpaca_blood_hound.parquet` with
  `--include-all-passed --wait --cache-mib 6144`; completion is pending at this checkpoint.
- Export v5 / API v7: **657 repository tests passed in 60.133 seconds** with ResourceWarnings treated as errors.
  Ruff lint/format (181 Python files), 273 local Markdown links/anchors with complete README coverage, and
  `git diff --check` passed. Eight new group-context tests cover ordinary/batch equality, once-only metadata reads,
  shared decoded payloads across IDs, release on close, caller mutation, consumer exception identity, locks and final
  freshness without reopening lineage. Streamed export tests cover late collision cleanup and moving snapshots.
- The existing **100-ID / 163,443-row corpus** matches **all 18 columns exactly** after sorting by series ID/date;
  every identity mapping and factor report also matches. Physical row order and converter version change deliberately.
  This was an output-equivalence check, not a cache-size benchmark. The temporary derived output was removed;
  retained pricing and lineage data were not copied or modified.
- At the user's request, the v4 run was interrupted after **708 accepted IDs / 978,839 staged rows**, with no final
  publication. The actual v5 streaming CLI restarted at **2026-10-16 23:14 UTC**, targeting the same
  `/tmp/alpaca_blood_hound.parquet` with `--include-all-passed --wait --cache-mib 6144`. Completion remains pending at
  this launch checkpoint; dated launch records do not establish current worker status.
- Export v4: **649 repository tests passed in 57.213 seconds**, including the explicit 6144 MiB CLI option;
  Ruff lint/format (179 Python files) and `git diff --check` passed. At the user's request, the 2 GiB-cache run was
  stopped during source selection and restarted at **2026-10-16 22:46:03 UTC** with `--cache-mib 6144`, the same
  all-passed policy and destination. No cache-size benchmark was run; this changes the allowance, not the calculations.
  Completion is not established by this launch checkpoint.
- Export v3 / API v6: **648 repository tests passed in 58.121 seconds** with ResourceWarnings treated as errors.
  Ruff lint/format (179 Python files), 272 local Markdown links/anchors and complete README coverage, actual CLI help
  and `git diff --check` passed. Thirteen new cache tests cover eviction, caller mutation, changed inputs, live locks,
  missing/symlinked payloads, exact-view assembly reuse and counterexamples, plus cached/uncached Parquet equality.
  A CLI regression also covers disabling the cache and rejecting invalid budgets before opening the store.
- On the same existing **100-public-ID / 163,443-row corpus**, the uncached/full-evidence final-check control took
  **40.15 s**; caching plus lightweight checks took **32.97 s**; adding exact-view assembly reuse took **25.46 s**.
  All three produced the same Parquet SHA-256
  `7bed54131110f91012317671887b2d54897a328dd2ceb9dd6bd3275936a7a7ed` under the same v3 writer contract.
  The final process used **25.36 CPU s**, peaked at **558.7 MiB RSS**, and retained **324 MiB** of accounted cache.
  Requested read bytes fell from **555 MB to 213 MB**; these are process read counters, not physical disk reads.
  These sequential, warm-cache-sensitive measurements establish this corpus's equivalence and improvement, not a
  full-universe ETA or maximum RSS guarantee. Temporary derived benchmark outputs were removed; source files were
  not copied, downloaded or modified.
- The v2 all-passed run was interrupted during source selection to apply this optimization; it published no final
  file. The actual v3 full-roster CLI was restarted at **2026-10-16 22:37 UTC**, targeting
  `/tmp/alpaca_blood_hound.parquet` with `--include-all-passed --wait` and the default 2048 MiB cache. Completion and
  full-universe memory/performance remain unverified at this checkpoint; this dated launch is not live job status.
- Export v2: **634 repository tests passed in 55.612 seconds**, including **28 projection/Parquet/CLI tests**.
  Ruff lint/format (177 Python files), 271 local Markdown links/anchors and complete README coverage, CLI help,
  runtime version/changelog validation and `git diff --check` passed. Coverage includes continuing past a rejected
  identity, rejection of an entire history for one bad date, independent IDs in one group, stderr-only diagnostics,
  clean footer/stdout, empty-window filtering, all-rejected output cleanup and fatal writer/snapshot failures.
- A bounded real CLI run selected BHVN and NVDA groups: three public IDs were considered, BHVN was rejected by the
  unchanged factor checks, and NVDA/MLNX published **3,796 rows across two identities**. The footer contained no
  rejected ID or omission fields; stdout contained only output/rows/identities. The temporary test artifact was
  removed, with no source writes or downloads.
- The first strict full-roster run selected **13,618 public IDs** from **20,004 source locators**, then stopped after
  19 successful conversions on BHVN's incompatible raw/split observations for **2022-09-23**. No final Parquet was
  published; temporary output was removed and source data remained unchanged. This demonstrates a full-universe
  conversion failure under the existing numeric contract, not grounds to relax it or repair upstream data here.
- Earlier v1 implementation checkpoint follows; its bounded Parquet retains the v1 footer contract.

- **627 tests passed** in **54.309 seconds**, with ResourceWarnings treated as errors. New coverage comprises
  22 factor tests, 21 projection/Parquet/CLI tests and 10 public-reader evidence tests. This includes exact rational
  preference only inside the joint bounds, genuine fractional ratios, thin-volume rounding, incompatible eight-view
  evidence, generation changes even for initially unreadable partial selections, no-clobber publication, and identical
  bytes for fixed synthetic inputs/writer versions.
- The analytical joint-factor projection agreed with an independent numerical optimizer on **500 generated cases**
  (319 feasible, 181 infeasible). This tests the mathematics, not provider action semantics.
- The actual standalone CLI exported **100 public IDs with one historical ticker each**, excluding GOOG/GOOGL from
  this test corpus: **163,443 rows**, **zero failures**, **123 constant split regimes**, all with evidence-supported
  exact rational selections. The bounded artifact is `data/bloodhound_export_validation/single_ticker_100.parquet`
  (7,932,665 bytes). No production exclusion of GOOG/GOOGL was introduced.
- Arrow and DuckDB checks found zero duplicate keys, non-midnight timestamps, nonfinite values, invalid prices or
  factors, negative volumes, all-adjusted OHLC geometry failures, or dollar-volume/adjustment-ratio discrepancies.
  Largest conditional selected-factor relative-error bound in this corpus was **8.9068e-6**; that is a bound under
  the declared inference model, not an independently established market-truth error.
- `ruff check .`, `ruff format --check .` (**177 Python files**), executable `--help`, runtime changelog validation,
  local Markdown links/anchors, complete README documentation coverage and `git diff --check` passed.

Reviewed current and archived Bloodhound Yahoo, Massive, merged and Alpaca construction code, contracts, manifests
and Parquet footers; their old outputs were not assumed correct. Source pricing/lineage files were not modified,
no price API requests or source copies were made. At that bounded checkpoint no full-universe export or Bloodhound
backtest/registration was run; the later strict full-roster attempt is recorded above. Only the bounded derived
Parquet was retained; interim test output was replaced, not archived.

Remaining limits: rounding/multiplicativity/constant-regime assumptions are explicit; hidden small share changes and
provider adjustment vintage are not recoverable with certainty from eight rounded views. Dated exchange/type remain
unknown, and output is not independently share-action certified or admitted to Bloodhound's configured catalogue.

### Multiple standalone adjustment views — 2026-10-16

- **574 tests passed** in **51.667 seconds**, with ResourceWarnings treated as errors. The 46 reader tests cover
  multi/repeated type selection, compound names, all eight views, single-view compatibility, one-snapshot reads,
  physical-alias reuse without a persistent cache, distinct adjusted values, and no partial output after a late error.
- `ruff check .`, `ruff format --check .` (**170 files**), `git diff --check`, reader `--help`, and **257 local Markdown
  links/anchors** with complete README coverage passed.
- Live GOOG reads from January 1 returned **197 sessions per view** for both `--type raw all` and repeated `--type`
  flags, selecting the same group generation. The first close was **315.32 raw / 314.70 fully adjusted**.
  `--all-types --format csv` returned **1,576 data rows** (eight views × 197 sessions), plus one labelled header.
  Observed runtimes were **0.23 seconds** for two JSON views and **0.39 seconds** for all-eight CSV output.

This is consumer API v4 only. No pricing publication, producer/rule change, data copy or worker restart was performed
for this feature. The live check is scoped to GOOG, not an all-store correctness or performance claim.

### Concurrent standalone reads — 2026-10-16

- **567 tests passed** in **50.657 seconds**, with ResourceWarnings treated as errors. The 39 standalone-reader
  tests include cross-process unrelated publication, same-group waiting without lock inversion, publication/recovery
  protection, bounded selection retries, typed index failures and exact v2 receipt/month-resume compatibility.
- `ruff check .`, `ruff format --check .` (**170 files**) and `git diff --check` passed. **257 local Markdown links
  and anchors**, complete README coverage of the other nine maintained documents, and reader `--help` passed.
- The authorized old worker was interrupted during calculation, with no pending publication left in its active group.
  The detached replacement started at **20:14 UTC**, PID **1598510**, using `--all --workers 1` with pricing downloads
  enabled. Its store remains `/data2/alpaca/data_pricing`; this dated record is not a live process-status guarantee.
- While that worker ran, the actual GOOG CLI returned resolution in **0.275 seconds** and **10 fully adjusted
  sessions** for October 1–14 in **0.344 seconds**. Both selected the same group generation. GOOG's lineage v2,
  correlation v51 receipt retained its modification time; the index protocol was v3. No price copy or forced refresh
  was used to activate the reader.

The current whole-universe lineage pass is not established as complete. Same-group work, incomplete publications
and stale input generations still prevent reads; these checks do not establish universe-wide identity correctness
or `DataLibrary` catalogue availability. Correlation rules and pricing layout are unchanged.

### Standalone resolved reader — 2026-10-16

- **558 tests passed** in **47.244 seconds**, with ResourceWarnings treated as errors; 30 new standalone-reader
  tests cover real synthetic publisher handoffs, all adjustments, exact values, overlay/rollover, reused tickers and
  aliases, partial unresolved history, source selection, stale inputs, non-mutation and process-independent locks.
- `ruff check .`, `ruff format --check .` (**170 files**) and `git diff --check` passed.
- **256 local Markdown links and anchors** and complete README coverage of the other nine maintained documents
  passed a local check. The new executable's `--help` succeeded; its real subprocess test read synthetic published
  prices without credentials or store changes.
- A live `./read_alpaca_pricing.py --store data_pricing -s GOOG --resolve` returned the expected **busy status 3**
  while the detached lineage invocation held its existing global lock. No lock was bypassed or producer restarted.

That initial API v2 checkpoint could not verify live history while the old writer held its lock. The subsequent v3
coordination checkpoint above addresses that limitation. Neither checkpoint is a universe-wide readability or
availability approval; pricing layout and lineage rules are unchanged.

### Documentation and restart clarification — 2026-10-16

This refactor changes eight Markdown files only; application code, runtime versions and JSON changelogs are unchanged.
The existing topic layout and all ten maintained documents are retained. Detailed research workflow remains in
Development, restart choices in Operations, and reuse/locking gates in Technical contracts.

Verified with Python **3.11.13** and Ruff **0.15.21**:

- **528 tests passed** in **37.715 seconds**, with ResourceWarnings treated as errors.
- `ruff check .`, `ruff format --check .` (**166 files**) and `git diff --check` passed.
- **243 local Markdown links and anchors**, complete README coverage of the other nine documents,
  **24 distinct inline source/resource paths** and **12 referenced lineage symbols** passed local checks.
- All **ten documented entrypoints** returned successful `--help`; the five documented lineage option combinations
  were parsed without executing acquisition or publication.

No production acquisition or lineage restart was performed for this documentation check. Full-store restart timing,
public integration of the standalone pair, and the current background run's completion are not established by these
tests. There is still no repository CI or configured Markdown/link-checking tool; the documentation checks were
explicit local checks, not a new automated repository gate.

### Earlier implementation and evidence scope — 2026-10-16

The implementation committed in `c5176d5` is correlation **v51**, standalone lineage **v2**, identity report **v4**,
and operator evidence schema **v2**. The extended batch closed **39 investigations**: 30 routine cases followed by nine targeted
reviews of material consolidated-comparison findings. Routine selection stopped at diminishing reusable concepts;
case count does not establish universe coverage.

The accepted same-input comparison against correlation **v31**, commit `1c008a3`, covers **3,446 sources**, **1,265
complete dependency groups** and **3,771,492 source-sessions**. It includes the retained evidence-shape screens and
controls, not a full-store audit:

- **97,980 newly attributed source-sessions** across 115 sources; **zero previously attributed sessions lost**.
- **2,304 ownership corrections** across seven sources, separately reviewed against independent primary records and
  directly established donor histories. **2,711 membership-only changes** across CWBC/CVCY preserve the continuing
  acquirer while separating the adopted-ticker target.
- **123 sources changed** in total; **100,939 source-sessions remain unmatched** within this comparison scope.
  All eight adjustment views agree on ownership; counts are not multiplied by eight.
- The running independent downloader changed 25 pricing generations during evaluation. Only their 15 complete
  dependency groups, 76 sources, were replayed in both versions against matching approved inputs. Unaffected results
  were retained; provider-input changes are not counted as rule gains.

The comparison exposed and corrected over-broad backward retired-label inference through synthetic padding, plus
an equal-date calendar error. V51 preserves independently dated ownership while bounding only unsupported backward
inference. Subsequent targeted review supports all changed owners. Backfilled CUSIP vintages, some preexisting
membership gaps, and the authenticity of individual inert prices remain distinct limitations; the batch does not
claim to repair them or validate every historical identifier field.

Final implementation verification: **528 tests passed** in **37.501 seconds**, with ResourceWarnings treated as
errors. `ruff check .`, `ruff format --check .` (166 files), `git diff --check`, and **220 local documentation links**
across ten maintained documents passed. No repository CI exists.

Real evidence-CLI publication completed for **40 selected labels in 37 dependency groups**. The trusted lineage
handoff then verified **151 sources**, all eight adjustment views, against the accepted ownership, public IDs and
membership: no assignment errors or changed-input discrepancies. All 37 publications needed **zero pricing
acquisition waves**. Other affected groups were evaluated. The authorized full local-source lineage run was launched
after this accepted checkpoint and commit; its completion and any remaining failures are separate runtime outcomes,
not implied by these checks. This dated record is not a worker-status monitor.
Standalone lineage publication does not establish public-catalogue integration or consumer availability. Sourced
facts and compact case outcomes remain in the existing ignored research files; they are not production inputs.

### Previous three-case checkpoint — 2026-10-16

The working implementation is correlation **v31**, standalone lineage **v2**, identity report **v4**, and operator
evidence schema **v2**. Runtime receipts select their own versions; this is not a claim that every source was rebuilt.
The agreed three-case development batch completed through the real CLI. Focused publications needed no new pricing
acquisition or copies. Broader affected-source comparisons were evaluated without publishing every affected group.
No universe worker restart or public-catalogue integration is part of this documentation task.

Documentation/integration verification, **2026-10-16 UTC**:

- `ruff check .`, `ruff format --check .` (151 files), and `git diff --check`: passed.
- `python3.11 -B -W error::ResourceWarning -m unittest discover -s tests -v`: **440 tests passed**, 36.201 seconds.
- Checked 219 relative links/anchors across all 10 maintained documents, complete README coverage, and the documented
  row registry against `row_correlation_rule_order`: passed. All 10 documented entrypoints' `--help` commands passed.
- No repository CI or dedicated Markdown checker exists; the local link/anchor/coverage check was read-only. Prior
  non-Markdown working-tree changes and untracked implementation files were unchanged by this documentation pass.
- No live acquisition, lineage publication, runtime-store audit, data copies or new runtime versions were performed
  for this refactor. Existing implementation changes are verified separately from the documentation-only edits.

The evidence CLI and shared cascade are implemented. The research/improvement loop is agent-led, not an automated
scheduler. Full failed-predicate traces are not retained; offline or reused evidence must not be described as a fresh
replay. Existing ignored research facts and compact case outcomes are references, not production dependencies.

### Earlier verification checkpoints

Earlier v28 draft checkpoint (superseded by the revised baseline and v29–v31): the v27 lineage worker was
stopped on 2026-10-15; the downloader was left running at that checkpoint. Draft impact runs were stopped when review exposed
over-broad evidence sharing and an over-strict historical-candidate rejection; those draft behaviors were removed.
The next comparison was directed to start from the original retained cascade, carrying forward established corrections.
That review was outstanding then, not a description of today's implemented version. Passing unit tests alone never
authorized a restart or established universal correctness. Retained examples select v19, v22 and v23, not one uniform version.

Correlation v27 verification, **2026-10-15 UTC**:

- Screened all 5,171 captured published standalone sources against the exact positive gate; 28 qualify. Replayed
  their complete dependency/UUID closures plus explicit controls: 132 sources in 41 groups, 1,278,904 observations
  across eight views. Both v26-baseline and v27-candidate passes reached the three-pass fixed point; no deferrals.
- Four sources recover assignments, sessions per view: ACST 2,205 (2016-01-04 through 2024-10-07), AMAM 583
  (2021-06-18 through 2023-10-11), ANTE 1,499 (2019-06-13 through 2025-05-29), and PWM 145 (2023-07-06 through
  2024-01-31). That is 4,432 ticker-sessions / 35,456 view observations assigned to four existing membership IDs.
  All eight views agree. No previously resolved observation changes owner, becomes unresolved or changes membership.
- All 128 other replayed sources retain their assignments, including AACI/AACIW, ACAC/ACACW, AAN, ACAT, ACEV,
  AENT, ALCE, ASCA, BTN, DOC, INBX, FB/META and JTAI. Each changed identity has an explicit same-symbol dated
  handoff; its previously approved inferred lower boundary and membership calculation are retained, not redefined.
- Eight new synthetic regressions cover two/three-UUID handoffs in all adjustments, alternate ticker spellings,
  exact cutovers, stock mergers, extra undated occupants, shared-successor branches, missing/superseded or mismatched
  links, overlaps, recurrence, and precedence of gap/as-of vetoes. Registry order remains unchanged.
- `ruff check .`, `ruff format --check .` (144 files), `git diff --check`, and 190 local documentation links/anchors:
  passed. Full `python3.11 -B -W error::ResourceWarning -m unittest discover -s tests -v`: **402 tests passed**.
- Replay is read-only, using stored pricing and captured normalized evidence, not external adjudication. Thirty
  control/dependency sources have newer pricing than their selecting lineage receipt; both rule versions consume
  identical captured inputs, and none is one of the four changed sources. No provider calls, pricing copies,
  publication, notice consumption or availability calculation. Online/offline transitions are not evaluated.
  Runtime receipts remain on their published versions until normal reprocessing; the active worker was not restarted.

Correlation v26 verification, **2026-10-15 UTC**:

- Actual implementation replay covered all 256 published standalone sources in 51 dependency groups, including all 50 selected
  seeds: 2,060,656 observations across eight adjustment views. All groups reached the established three-pass fixed
  point, and its impact matched the prior candidate replay exactly. All eight views agree on every changed span.
- `ruff check .`, `ruff format --check .` (144 files), `git diff --check`, and 189 local documentation links/anchors:
  passed. Full `python3.11 -B -W error::ResourceWarning -m unittest discover -s tests -v`: **394 tests passed**,
  including 12 new regressions for positive starts, exact entity selection, vetoes, precedence and fixed-point membership.
- Eight source labels change: BNT, DOC, ECDA, EFHT, JTAI, OXAC, INBX and METC. Counts per view: 1,095 unresolved
  observations resolve, 484 resolved observations change attribution, 2,797 change membership only, and 405 become
  unresolved. Changed spans involve 10 membership IDs; the remaining 248 sources retain their ticker/UUID/membership
  assignments. Four changed labels are selected seeds; BNT, EFHT, METC and OXAC are dependencies.
- DOC resolves 1,086 PEAK sessions and corrects the UUID on 479 later DOC sessions. INBX resolves the one INXB
  session and corrects the UUID on the following two sessions. ECDA resolves six sessions; preserving its first
  dated arrival removes the artificial gap that had split EFHT/ECDA membership in the next cascade pass.
- The same repeated-arrival shape corrects three JTAI UUID assignments, rejoins OXAC/JTAI membership, and resolves
  two METC sessions. BNT's 405 sessions from 2023-01-20 through 2024-08-29 expose concurrent BNRE/BNRE.A predecessor
  claims after preserving BNRE's earlier dated start; they remain unresolved rather than choosing a branch.
- ALTM's 760 disputed sessions remain unresolved. BTN, FB/META and the ASCA/MDGS/PBTS membership-growth controls
  are unchanged. This validates deterministic use of captured evidence, not every provider fact independently.
- The replay uses trusted stored pricing and normalized evidence in memory. No API calls, data copies, pricing or
  lineage publication, notice consumption, or public availability calculation occurs. Online/offline transitions
  are not evaluated: the standalone lineage boundary does not publish the public catalogue. Existing runtime
  receipts remain on their prior versions until normal reprocessing; ignored impact reports are not publications.

Completed live 50-seed lineage run, **2026-10-15 UTC**, correlation v25:

- The actual standalone CLI processed the full selected file in one run: 50 selected tickers, 250 source labels
  including dependencies, exit 0, no failures, 22 minutes 30 seconds. Normal downloader handoffs acquired missing
  pricing and lineage published its receipts; no retained-store copying or public-catalogue publication occurred.
- Compared 573,600 observations across all eight adjustment views within the shared retained/current request ranges:
  41 tickers match, four have attribution differences, three have membership-only differences, and AXICY/DNMRQ have
  empty shared histories. No source-session additions or removals occurred within those comparison ranges.
- Attribution differences, sessions per view: DOC 1,086 (2019-11-06 through 2024-03-01), ALTM 760 (2022-02-23 through
  2025-03-05), ECDA six (2023-12-13 through 2023-12-20), and INBX one (2024-05-30). Previously resolved rows are now
  unresolved. ASCA, MDGS and PBTS retain historical ticker/UUID attribution but have expanded membership and new IDs.
- BTN now matches all 1,758 historical sessions per view; JTAI succeeds and matches. All seven differences agree
  across the eight views. These are comparison results, not independent correctness verdicts; grouped review remains
  outstanding. Lists, exact IDs and detailed reports stay under ignored `data/lineage_test_candidates/`.

Initial correlation v25 implementation verification, **2026-10-15 UTC**, before the live run above:

- `ruff check .`, `ruff format --check .` (144 files), `git diff --check`, and 189 local documentation
  links/anchors: passed. Full `python3.11 -B -W error::ResourceWarning -m unittest discover -s tests -v`:
  **382 tests passed**, including seven new ownership/membership regressions. Rule order remains explicit at 8–10.
- Replayed all 111 already-published standalone sources in their 21 dependency groups, using captured evidence,
  trusted pricing and all three correlation passes. All reached the fixed point; 97 retained their dated assignments.
  Fourteen changed: AMRK, BAM.WI, BAMR, BTN, DOC, DPW, FGH, HCP, HYAC, JWAC, NETE, NUKK, PIH and SCRM.
  The changed spans involve 16 membership IDs; per view, 10,100 source observations resolve, 3,525 become unresolved,
  and 1,471 change resolved attribution. These are correlation results, not public online/offline transitions.
- BTN's 1,758 sessions per view now resolve to its own predecessor UUID. ASCA's 513 historical assignments per view
  remain unchanged; its accepted split extension retains the current membership-derived hash. Synthetic earlier/later
  evidence tests verify that membership expansion changes the hash without changing historical ticker/UUID ownership.
- Deliberate unresolved results include FGH's 26 sessions before the captured February 2023 transition, concurrent
  predecessor claims in DOC, and undated reused-label claims in HCP/HYAC. No external dates or ticker exceptions were
  inserted. Split membership logic, pricing, datastore layout and the ID scheme were not redesigned.
- No provider calls, pricing copies, notice consumption, runtime publication or public availability changes occurred
  during this in-memory replay. At that point lineage receipts remained v24 pending normal reprocessing. The replay verifies
  the complete stored-input cascade, not independent real-world correctness of every wider changed history; those
  evidence conflicts remain review items. No full retained/universe rebuild or remaining 30-seed batch was run.

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

### Standalone development snapshot — 2026-10-15

At this historical checkpoint, `data_pricing/end_of_day/pricing/GOOG` had 2,710 effective sessions per view, 2016-01-04 through 2026-10-13:
2,514 completed-years rows, 196 year-snapshot rows, and 9 active-month rows with overlap replaced. Its 12 pricing files
were seven gzip payloads, four receipts, and the stable lock. No staging or publication journal remained.

`data_pricing/end_of_day/manifests/GOOG/update.json` was pending:
`{"from":"2016-01-04","to":"2026-10-13"}`. The authorized v3 layout move used an in-place directory rename and
preserved payload hashes, inodes, and modification times; no old GOOG copy remained. No lineage consumer had processed
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
| Standalone pricing source | 8 | Identical-boundary reuse, successful-exit batch memo and disposable changed-input hints; live probes on advancement. |
| Standalone lineage | 6 | Changed-input selection, disposable hints and approved metadata reuse for rule-only recalculation; compatible v2–v5 decisions. |
| Clean-room run ledger | 1 | Immutable scope, checkpoints, exhaustive audit, completion, and activation authority. |
| Public consumer API | 10 | Online-only `DataLibrary`; standalone groups with bounded reuse and actual/absent input freshness. |
| Bloodhound export | 7 | RAM-held Arrow output, shared retained-memory allowance, one packed serialization and atomic 18-column publication. |
| Monthly pricing source | 2 | Month storage, aliases, inventories, and selected-month replacement. |
| Monthly validation | 3 | Reusable month results and predecessor seams. |
| Lineage source | 2 | Normalized evidence bound to source and validation generations. |
| Correlation envelope | 2 | Compact exhaustive partitions and correlation-owned identity evidence. |
| Warning envelope | 2 | Compact advisory outputs bound to immediate input generations. |
| Identity candidate | 2 | Materialized snapshot or affected-closure aggregation. |
| Availability | 1 | Per-candidate online/offline decision. |
| Catalogue publication | 2 | Snapshot and bounded affected-closure catalogue selection. |
| Validation transform | 19 | Established daily-bar and dataset rules. |
| Correlation transform | 55 | Traded cash-closing sessions in independently anchored pre-reuse histories; supported prefixes retained, unrelated identities kept separate. |
| Operator lineage evidence | 3 | Actual workflow and paths; expose changed formerly absent inputs during offline inspection. |
| Catalogue engine | 5 | Content-addressed source/security objects and inventories. |
| Warning transform | 6 | Established advisory rules. |
| Detailed history transform | 7 | Established source-selection and history assembly semantics. |
| Lineage report | 4 | Bounded name-change evidence discovery. |
| Security ID scheme | 1 | SHA-256 of sorted unique Alpaca asset UUIDs. |
| Verified session exclusions | 1 | Authenticated venue-session evidence format. |

Exact constants and changelog paths are listed in [Development](development.md#versioned-boundaries).

## Known gaps

Standalone pricing, lineage and source-scoped resolved reading are implemented; integration with `DataLibrary`'s
online catalogue is not. The new reader requires a source symbol even when selecting a public ID; it does not provide
a global live online catalogue. The separate Bloodhound converter can enumerate existing published groups; it cannot
make unresolved/unpublished identities available. Unrelated lineage groups can publish during a read.
Lineage's no-cold-read continuation
covers the explicit single-identity reuse gate. Context-dependent multi-identity cases and rollover calculate the
affected chronology using trusted inputs, not an upstream audit. A full evidence diagnostic can read closed session
grids even when normal processing reuses assignments.

Standalone lineage remains one worker per store. Reads can still wait on their own pricing group or a brief commit;
an incomplete publication prevents reads until recovery. Default/`--all` repeats use shared work hints and the existing
index; deleting hints restores a receipt scan. No full-store restart-time guarantee has been established.
See [restart and reader limits](operations.md#restart-and-catch-up). Multi-worker lineage and priority scheduling were
investigated, not implemented. The v3 change removes whole-invocation reader blocking, not writer serialization.

Older metadata corrections outside the resumed month window require explicit refresh. Endpoint checks are the chosen
pricing-refresh policy, not exhaustive historical correction detection. The focused publications and broader read-only
comparisons do not establish complete current-universe correctness or throughput. Historical run summaries below are
not a live monitor; no whole-universe acceptance or performance claim is made by this documentation pass.
The [timing preflight](#read-only-timing-preflight--2026-10-18) measured shared planning and bounded local calculation,
not complete catch-up. Lineage v6 removes metadata acquisition for eligible rule-only changes; local recalculation,
publication and genuinely changed or missing inputs still incur work.

The research loop is not an implemented automatic scheduler. Some unit/warrant histories remain insufficiently
anchored; adjacent/backfilled action records can leave membership gaps, and synthetic pre-listing carry-forwards need
separate ownership analysis. Exact raw-prefix recovery does not resolve every later reused-symbol interval, certify
all historical legal CUSIPs, or prove adjusted-price equivalence across aliases. Current research facts and selected
case limits are retained locally, not injected as executable ticker rules.

The standalone pricing downloader has at most three ticker workers with one shared 199-request/minute limiter.
A populated-ticker update advancing its request boundary uses up to 32 requests before pagination/retries: 16 initial probes, eight acquisitions,
and eight final first-session probes.
For 18,092 such tickers that implies roughly 48.5 hours at the configured rate, before latency. `--all` is functional,
not yet demonstrated as a daily universe service. Request batching/performance work is separate from this boundary change.
Workers overlap latency but do not multiply the request allowance. The allowance is process-local, not shared across
independent command invocations using the same credentials. The tests establish limiter behavior, not achieved live
universe throughput or a daily completion guarantee.

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

### 2026-10-15 — Whole-ticker workers and shared request allowance (development checkpoint)

v5 adds a maximum of three independent ticker workers, sharing one paced rolling-minute budget and retry cooldown.
Each worker owns its complete ticker boundary. Store layout, payload deduplication, ticker locks, and lineage handoff
are unchanged; v4 validators remain readable. No live acquisition or runtime-store mutation is part of this change.

### 2026-10-15 — Standalone batch CLI and endpoint validator (development checkpoint)

v4 adds one per-ticker validator inside the established layout, safe date/selection options, isolated batch failures,
and bounded post-acquisition consistency checks. Automatic refresh and force remain clean replacements; clipped-edge
retention is deferred in a code comment. Runtime data is unchanged by this implementation.

### 2026-10-14 — Standalone pricing and future lineage handoff (development checkpoint)

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
- Correlation transform v1–v51 defines exhaustive row partitioning, ticker-reuse and transition rules, UUID membership,
  same-symbol split continuity, overlap selection, fixed-point discovery, the superseded-rename veto, shared endpoint
  completion and independent exact-raw predecessor-prefix ownership. V28 preserves accepted continuity membership;
  v29 interprets missing endpoints; v30 adds the prefix rule; v31 separates raw ownership evidence from adjustments.
  V32–v51 extend those concepts to dated occupancy, bounded informative episodes, independent donor intervals,
  inactive observed components and retired labels, and distinguish continuing acquirers from acquired targets.
- Catalogue engine v1–v5 defines deterministic IDs, content-addressed sharding, persisted availability, and supported
  correlation-generation migration.
- Detailed history transform v1–v7 defines deterministic source selection, provenance, and conflict handling.
- Warning transform versions 1–6 define advisory cross-adjustment and segment-statistics behavior.

The JSON changelog arrays under [`resources/`](../resources/) are the exact machine-validated histories.
