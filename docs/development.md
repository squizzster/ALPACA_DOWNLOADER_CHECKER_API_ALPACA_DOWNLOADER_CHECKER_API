# Development

## Find the change owner

The [architecture ownership table](ARCHITECTURE.md#code-ownership) is the authority for component boundaries. Start at
the public package or root command named by the request, then follow its imports to the owning package under `libs/`.
The focused producer enters the monthly boundary package; the clean-room constructor enters its scheduling/audit
package and delegates every domain calculation to the same monthly cascade; the public reader enters
[`alpaca_end_of_day`](../alpaca_end_of_day/__init__.py). Root commands remain thin executable adapters.

Tests mirror the boundary names under [`tests/`](../tests/). Add focused coverage in the owning test module, and check
adjacent integration tests when a change crosses acquisition, correlation, publication, or read boundaries. Tests for
trusted loading must assert that consumers do not call the corresponding authenticator or recalculate a published
digest. Tests for publishers must assert that output validation precedes receipt publication.

The standalone pricing command enters only `libs/alpaca_pricing_download/`. Its focused tests are:

- `tests/test_alpaca_pricing_download.py`: exact values, pagination, overlap, rollover, first/last-session checks,
  automatic full replacement, unchanged cold/year payloads, recovery, and absence of identity/lineage imports.
- `tests/test_alpaca_pricing_handoff.py`: actual changed-date ranges, pending accumulation, full-range replacement,
  acknowledgement/failure/recovery, and separate-process locking in both directions, including process death.
- `tests/test_alpaca_pricing_validator.py`: no cold-payload reads during ordinary checks, legacy bootstrap, source/validator
  crash consistency, bounded provider-change restart, removed-session notices, force repair, and history-start safety.
- `tests/test_alpaca_pricing_cli.py`: symbol/file/universe selection, date semantics, deduplication, and ticker-specific
  failure isolation, bounded worker counts, whole-ticker worker ownership, and ordered results. Universe tests inspect
  selection only; they never download it.
- `tests/test_alpaca_pricing_rate_limit.py`: virtual-clock and threaded checks of the shared rolling-minute budget,
  pacing, overlapping I/O, retry headers, cooldown extensions, and exhaustion without clearing another worker's pause.

These use synthetic fixtures and HTTP doubles. Do not run production downloads or copy retained runtime stores to
test this path. The handoff is an internal producer interface, not a supported application import.

`tests/test_alpaca_lineage_build.py` covers the standalone lineage twin: chronological reuse/rollover, trusted
consumption without pricing authenticators or cold reads, FB → META versus reused FB, missing-price delegation,
reverse dependencies, bounded acquisition, empty inputs, metadata refresh, deletion, partial publication, acknowledgement
recovery, generation changes, one-worker locking and CLI selection. It uses synthetic temporary pricing and identity
doubles; no retained runtime data or live API calls are needed.

`tests/test_alpaca_lineage_evidence.py` checks that operator evidence follows the real pipeline, separates reused
decisions from fresh calculation, and keeps offline inspection read-only. `tests/test_alpaca_predecessor_prefix.py`
checks exact independent prefix attribution, counterexamples and the shared three-pass fixed point. These supplement
the ordered-rule and identity-membership coverage in `tests/test_alpaca_end_of_day_security_correlation.py`.

`tests/test_alpaca_standalone_reader.py` exercises `alpaca_end_of_day.StandaloneReader` and its actual CLI using the
real standalone publishers with synthetic provider doubles. It covers exact values, compact overlays/rollover,
source aliases, selected-adjustment reads, ticker reuse, partial unresolved history, shared history-source policy,
multi-view JSON/CSV selection, single-snapshot alias reuse, single-view compatibility and all-or-error output,
stale and incomplete generations, read-only behavior, and cross-process busy/wait/release semantics. It also verifies
unrelated publication during reads, same-group lock order, bounded re-selection and v2 receipt/month reuse through
the v3 protocol upgrade. No retained pricing is copied and no provider credentials are required. Reader behavior owns
the public API version; shared publication locking owns the standalone lineage version, not the identity rules.

`tests/test_alpaca_standalone_cache.py` checks bounded eviction, cached/uncached value and Parquet equality,
independent mutable results, exact-view assembly reuse and its label/policy counterexamples, changed generations,
missing files, producer locks, conservative Unicode accounting and lightweight freshness without unchanged receipt
rereads. Source data remains read-only.
`tests/test_alpaca_standalone_group.py` covers batch/ordinary history equality, one-handshake document parsing,
decoded reuse across IDs, release on close, consumer exception propagation, mutation isolation and retained-token
freshness without reopening approved lineage data.
Bloodhound export tests also verify no file creation during acquisition, one packed serialization, post-serialization
freshness, shrinking cache allowances, zero-copy Arrow concatenation and fatal memory exhaustion under all-passed mode.

`libs/common/` is dependency-neutral shared infrastructure. Consumer code imports only `alpaca_end_of_day`, never a
module under `libs/`.

## Versioned boundaries

Every behavioral or serialized-contract change bumps its owning version and appends exactly one JSON changelog entry.
The tracked runtime changelogs live in [`resources/`](../resources/). They are arrays whose one-based position is the
version; entries contain `modifications` and no explicit `version` key. Runtime validators require array length to
equal the current constant.

Documentation-only work does not bump a runtime version or modify these JSON arrays. Record pending documentation and
repository-layout changes under `Unreleased` in [VERSION_TRACKER.md](VERSION_TRACKER.md#unreleased).

| Boundary | Version constant | Runtime changelog |
|---|---|---|
| Standalone pricing source | [`VERSION`](../libs/alpaca_pricing_download/contract.py) | [`alpaca_pricing_download_change_log.json`](../resources/alpaca_pricing_download_change_log.json) |
| Standalone lineage | [`VERSION`](../libs/alpaca_lineage_build/contract.py) | [`alpaca_lineage_build_change_log.json`](../resources/alpaca_lineage_build_change_log.json) |
| Operator lineage evidence | [`EVIDENCE_VERSION`](../libs/alpaca_lineage_build/evidence.py) | [`alpaca_lineage_evidence_change_log.json`](../resources/alpaca_lineage_evidence_change_log.json) |
| Clean-room run ledger | [`CLEAN_ROOM_RUN_VERSION`](../libs/alpaca_end_of_day_clean_room/generation.py) | [`alpaca_clean_room_run_change_log.json`](../resources/alpaca_clean_room_run_change_log.json) |
| Public consumer API | [`API_VERSION`](../alpaca_end_of_day/__init__.py) | [`alpaca_end_of_day_api_change_log.json`](../resources/alpaca_end_of_day_api_change_log.json) |
| Bloodhound export | [`VERSION`](../libs/alpaca_bloodhound_export/__init__.py) | [`alpaca_bloodhound_export_change_log.json`](../resources/alpaca_bloodhound_export_change_log.json) |
| Monthly source | [`SOURCE_VERSION`](../libs/alpaca_end_of_day_v6/versions.py) | [Source-boundary changelog](../resources/alpaca_v6_source_change_log.json) |
| Monthly validation | [`VALIDATION_VERSION`](../libs/alpaca_end_of_day_v6/versions.py) | [Validation-boundary changelog](../resources/alpaca_v6_validation_change_log.json) |
| Lineage source | [`LINEAGE_SOURCE_VERSION`](../libs/alpaca_end_of_day_v6/versions.py) | [Lineage-boundary changelog](../resources/alpaca_v6_lineage_source_change_log.json) |
| Correlation envelope | [`CORRELATION_ENVELOPE_VERSION`](../libs/alpaca_end_of_day_v6/versions.py) | [Correlation-envelope changelog](../resources/alpaca_v6_correlation_envelope_change_log.json) |
| Warning envelope | [`WARNING_ENVELOPE_VERSION`](../libs/alpaca_end_of_day_v6/versions.py) | [Warning-envelope changelog](../resources/alpaca_v6_warning_envelope_change_log.json) |
| Identity candidate | [`IDENTITY_CANDIDATE_VERSION`](../libs/alpaca_end_of_day_v6/versions.py) | [Identity-candidate changelog](../resources/alpaca_v6_identity_candidate_change_log.json) |
| Availability | [`AVAILABILITY_VERSION`](../libs/alpaca_end_of_day_v6/versions.py) | [Availability changelog](../resources/alpaca_v6_availability_change_log.json) |
| Catalogue publication | [`CATALOGUE_PUBLICATION_VERSION`](../libs/alpaca_end_of_day_v6/versions.py) | [Catalogue-publication changelog](../resources/alpaca_v6_catalogue_publication_change_log.json) |
| Validation transform | [`PIPELINE_VERSION`](../libs/alpaca_end_of_day_pipeline/pipeline_version.py) | [`alpaca_end_of_day_pipeline_change_log.json`](../resources/alpaca_end_of_day_pipeline_change_log.json) |
| Correlation transform | [`CORRELATION_VERSION`](../libs/alpaca_end_of_day_security_correlation/correlation_version.py) | [`alpaca_end_of_day_security_correlation_change_log.json`](../resources/alpaca_end_of_day_security_correlation_change_log.json) |
| Catalogue engine | [`SECURITY_MASTER_VERSION`](../libs/alpaca_end_of_day_security_correlation/security_master_version.py) | [`alpaca_security_master_change_log.json`](../resources/alpaca_security_master_change_log.json) |
| Warning transform | [`WARNING_PIPELINE_VERSION`](../libs/alpaca_end_of_day_warning_pipeline/warning_pipeline_version.py) | [`alpaca_end_of_day_warning_pipeline_change_log.json`](../resources/alpaca_end_of_day_warning_pipeline_change_log.json) |
| Detailed history transform | [`READER_VERSION`](../libs/alpaca_end_of_day_reader/reader_version.py) | [`alpaca_end_of_day_reader_change_log.json`](../resources/alpaca_end_of_day_reader_change_log.json) |

The normalized lineage report also uses `REPORT_VERSION` in
[`alpaca_security_lineage/settings.py`](../libs/alpaca_security_lineage/settings.py). Supporting manifest and ID-scheme
versions live beside their schemas. [VERSION_TRACKER.md](VERSION_TRACKER.md) records the current matrix and gaps; the
JSON arrays remain the exact machine-validated histories.

When correlation changes, review `SUPPORTED_CORRELATION_VERSIONS` as a migration decision. Supporting an older derived
generation is deliberate compatibility, not permission to bypass authentication.

## Rule changes

Correlation and availability rules must match a generic evidence shape and apply uniformly across the universe. Add a
rule only when its inputs, positive gate, vetoes, and placement in the cascade are simple enough to state precisely.
Publish the intervals that deterministic evidence supports without demanding complete knowledge of every related
history. An unresolved competition or an entirely unlinked observation is not permission to invent an owner.
Use the loop below for lineage changes; availability remains a separate boundary. Never add a ticker-specific condition.

## Lineage research and rule-improvement loop

### Objective and responsibilities

Improve the deterministic production cascade so each `public_id` represents the best-supported continuous pricing
history. Misattribution and withholding attributable history are both errors. The original cascade is a useful
baseline, not ground truth; neither old/new agreement nor a higher resolved count proves correctness. Cases supply
general concepts, not ticker-specific recipes. This is an agent-led development procedure, not an automatic batch
scheduler or a model-dependent runtime resolver.

Choose the most likely correct attribution from the complete evidence pattern, allowing residual uncertainty when
one explanation is better supported. A one-to-four-session processing discrepancy alone neither defeats continuity
nor authorizes a merge. Test the competing explanation, including a genuine change of predecessor. Leave history
unmatched when attribution remains insufficient after the cascade, not merely because an individual rule declines it.

Treat reliable public records as settled facts where they establish the question being investigated. Use those facts
as the benchmark for judging internal results; do not repeatedly downgrade established history to uncertainty.
Separate confirmed facts, unanswered research questions and the internal mapping still needed. Scope uncertainty to
the affected dates, links or issue: publish independently supported portions, and withhold only portions without a
defensible attribution. An unsuccessful search is not itself evidence against otherwise supported history.

- **Main agent:** owns the complete cascade, case selection, internal-evidence interpretation, implementation and
  acceptance. Translate a research concept into the actual pipeline; do not paste an agent's suggested rule verbatim.
- **Research agent:** use a fresh `gpt-6.1-sol`, `reasoning_effort="high"`, `fork_turns="none"` for each case. Give it a
  self-contained brief, not the parent's accumulated conclusions. It researches and challenges; it does not edit
  code, run publishing commands or mutate data. Reuse it for follow-ups within that case; start clean for the next.
  If the requested model/configuration is unavailable, report that rather than silently claiming the same process.
- **Production pipeline:** remains the sole resolver and publisher. Public research establishes external facts;
  deterministic stored/provider inputs must establish the internal UUID mapping. Research facts never become
  executable ticker exceptions or a production dependency on the research agent.

Agree the selection criteria and stopping point before starting. Investigate cases sequentially while they add useful
concepts or material recoveries, then perform one consolidated evaluation and handoff. A case quota does not establish
coverage. Announce each case's ownership question and explain findings and decisions as work proceeds. Starting a
full lineage run or pricing-universe run requires authorization for that distinct operation.

### One case, step by step

1. **Frame one question.** Specify the observations and dates whose ownership or continuity is disputed. Separate
   row ownership, identity membership, transition dates, adjustments and missing evidence. Identify the current
   baseline from code and receipts, preserving existing worktree changes; do not assume all publications use it.
2. **Get actual evidence.** Use `./output_current_lineage_evidence.py TICKER --store data_pricing` through the
   [normal workflow](operations.md#current-lineage-evidence). Missing pricing may be acquired through the existing
   downloader handoff. Trust approved inputs; do not copy bars, re-authenticate the store or write a second resolver.
   Use its versions, input identities, evidence, dated decisions and selected-rule path to locate the question.
   Offline inspection is not a replay, and a reused report is not a newly calculated decision.
3. **Brief the clean researcher.** Supply that question, the evidence output, relevant current rule implementations
   and order, and any reusable confirmed research. Do not supply a preferred verdict or assume either retained or
   current assignments are correct. Ask for primary-source facts, uncertainty, a generic concept, counterexamples
   and a proposed cascade position. Use the brief below.
4. **Bridge facts to our evidence.** Determine which available observation distinguishes the candidate securities,
   what it proves, and over which interval. Public facts cannot identify opaque provider UUIDs or the owner of unseen
   bars. If research knows the answer but our inputs cannot establish it, explain that narrow gap instead of inserting
   a researched date or ticker override. Conversely, an incomplete field need not defeat independent supporting evidence.
5. **Formulate and challenge one concept.** Write: given A and B, conclude C for interval D, unless E contradicts it.
   Locate its owner: evidence interpretation, dated ownership, row allocation or UUID membership. Prefer correcting
   an existing rule over adding another fallback. Check earlier precedence, later fallbacks and cross-source fixed-point
   effects. Ask the case's researcher whether this formulation makes common sense and where it could misassign or
   unnecessarily suppress history. The main agent resolves the challenge and owns the final formulation.
6. **Implement and test narrowly.** Change the owning production rule; cover the positive case, nearest counterexample,
   date boundaries, partial-history behavior and adjacent-rule interactions. Update the registry-order test where
   necessary, bump the owning version and append one runtime changelog entry for a behavioral change. Screen existing
   evidence cheaply for other potentially affected securities; defer the broad replay to the batch end.
7. **Close the case before advancing.** State whether it yields a justified candidate change, supports the existing
   behavior, or exposes insufficient internal evidence. All are valid outcomes; a new rule is not required. Preserve
   focused checks and a compact outcome so the final review can attribute changes. The next case uses the updated
   baseline and a new researcher; candidate changes remain subject to the batch-wide review.

### Reusable research brief

> Research this security history independently using primary records. Establish dated relationships, share classes
> and external identifiers where available; separate confirmed facts, inference and unknowns. Compare those facts
> with the supplied deterministic evidence and current cascade. Explain what our inputs can establish about internal
> identity without claiming public records identify opaque UUIDs. Propose a simple, non-ticker-specific concept, its
> counterexamples and its proper cascade position, or explain why no justified change is available. Consider both
> incorrect attribution and withholding attributable history. Research only: do not edit code or publish data.

### End of the agreed batch

Perform one consolidated impact and regression review, not a whole-store audit after every edit:

1. Compare the starting baseline and final cascade using the **same approved inputs**. Use the union of candidate
   evidence shapes to find affected sources, then evaluate complete dependency/identity groups through the production
   fixed-point path. Include earlier-case controls. A selection screen is not permission to omit affected identities.
2. Inspect every changed identity, separating newly attributed rows, lost/reassigned rows, dated-boundary changes,
   membership-only changes and unresolved intervals. Counts are source-sessions; eight views are consistency checks,
   not eight independent ownership successes. A membership-hash change alone is not an ownership regression.
3. Research any newly exposed, materially different evidence shape or contradiction before accepting the change.
   Reuse confirmed facts instead of researching the same event again. Correct or withdraw an over-broad rule; do not
   excuse collateral damage because the motivating case works.
4. Run the complete [repository verification](#verification), then exercise the real selected-case CLI/publication.
   If final acquisition changes provider evidence, distinguish that input change from a rule effect and investigate
   only the newly affected scope. Never claim an isolated in-memory comparison was a real publication.
5. Report the accepted concepts, their positions and interactions, affected scope, verification and remaining limits.
   Resolved lineage is not the same as public online availability: the standalone pair does not publish the catalogue.
   Claim online/offline movements only if that owning boundary was actually evaluated. Stop at the agreed batch boundary.

Focused tests during implementation and the producer's normal publication checks remain necessary. This cadence
removes repeated broad audits; it does not skip required correctness checks or repeat upstream boundary approval.

### Lessons to apply as concepts

- A source label, a corporate relationship and a continuing pricing identity are different things.
- Row ownership, UUID membership and adjustment representation need separate decisions at their owning layers.
- Exact price agreement needs informative observations and independently established ownership; flat synthetic
  carry-forwards, isolated matching intervals and circular donor claims are not equivalent evidence.
- A gap is evidence to interpret, not a universal verdict. An event date is not automatically a trading cutover.
- Current/backfilled external identifiers need not describe every historical session literally.
- Supported intervals can be published without pretending the remaining history is solved. Neither blanket rejection
  nor wholesale restoration of an earlier output is the objective.
- A rule is accepted for its behavior in the complete cascade, not merely for passing its motivating example.

### Success, repeatability and minimal retention

For a proposed change, **TRUE / accepted** means the generic deterministic evidence supports its scoped conclusion,
counterexamples and full-cascade impact have been addressed, tests pass, and selected real publication succeeds.
Anything short of that is not accepted. Within the row cascade, a rule falling through does not declare a security
false or unavailable; later rules still run. A justified no-change finding can successfully close an investigation.

Same code and same approved inputs should reproduce decisions. Live acquisition can change inputs, so use existing
receipt versions and generation identities to distinguish evidence changes from rule changes. Do not claim exact
historical reproduction if those inputs are no longer retained; do not solve that by copying pricing stores.

Retain confidently established research facts once with source URLs, effective dates and limits, plus a compact case
outcome. The existing ignored `data/lineage_research/confirmed_facts.json` is a reusable research reference, not a
runtime input; `data/lineage_test_candidates/lineage_rule_iterations.json` records case outcomes. These local files
need not exist in a fresh checkout. Reuse them when present rather than creating another journal hierarchy. Keep
price payloads, research files and experimental outputs out of Git. On-demand evidence explains decisions; permanent
per-predicate traces, price copies and repeated audit ledgers are not the product. Document stable process and concepts,
not every tentative theory.

## Verification

The Bloodhound converter's focused tests are `test_alpaca_bloodhound_factors.py`,
`test_alpaca_bloodhound_export.py` and `test_alpaca_standalone_export_reader.py`. Install
[`requirements-bloodhound.txt`](../requirements-bloodhound.txt) to execute the optional Parquet integration tests;
without PyArrow those tests explicitly skip. Tests use synthetic data and do not require Bloodhound's repository,
live services or source copies. Live exportability checks are bounded and separate from the unit suite.

The repository's defined local checks are:

```bash
ruff check .
ruff format --check .
python3.11 -B -W error::ResourceWarning -m unittest discover -s tests -v
git diff --check
```

Tests use temporary repositories and HTTP doubles. They do not require production data or live credentials. `-B`
prevents ignored bytecode caches. Use `python3.11` explicitly in the current environment: unqualified `python3` is
Python 3.9.25 and cannot import the repository's Python 3.11 syntax.

There is no `.github/workflows/` directory and no other repository CI configuration. Local commands are therefore the
only automated gate. Ruff is configured in [`pyproject.toml`](../pyproject.toml), but its executable version is not
pinned. Ruff also scans untracked Python files, so inspect `git status` before interpreting its output. Current tool
versions, results, and unresolved gaps belong in [VERSION_TRACKER.md](VERSION_TRACKER.md#verification-status).

## Documentation ownership

| Topic | Home |
|---|---|
| Product summary and complete document index | [`README.md`](../README.md) |
| Agent workflow and repository safety rules | [`AGENTS.md`](../AGENTS.md) |
| Responsibility, flow, ownership, and invariants | [`ARCHITECTURE.md`](ARCHITECTURE.md) |
| Single execution path, ordered rule cascade, and non-divergence controls | [`PIPELINE.md`](../PIPELINE.md) |
| Consumer-facing Python and CLI contract | [`public-api.md`](public-api.md) |
| Operator commands, mutation, backup, and recovery | [`operations.md`](operations.md) |
| Schemas, evidence gates, identity rules, runtime artifacts, and tracked resources | [`technical-contracts.md`](technical-contracts.md) |
| Contributor workflow, research loop, local checks, and documentation ownership | [`development.md`](development.md) |
| Current versions, pending changes, history, and verification gaps | [`VERSION_TRACKER.md`](VERSION_TRACKER.md) |
| Dated retained-store inventory and reuse restrictions | [`storage-rework-notes.md`](storage-rework-notes.md) |

Keep one explanation authoritative and link to it from other documents. The repository has no configured Markdown
linter or link checker; documentation changes require an explicit local relative-link, anchor, and referenced-path
check.

For a documentation-only handoff:

1. Compare guidance with the owning symbols, CLI parsers, version constants, changelog arrays and relevant tests.
   Use `python3.11 -B <entrypoint.py> --help` for safe option verification; do not run a mutating example merely to
   check its syntax. Keep important restrictions and failure/recovery behavior when consolidating prose.
2. Check every maintained Markdown file's relative links, heading anchors and referenced repository paths. Confirm
   that the README links directly to every other maintained document, including root agent/pipeline instructions.
   Ignored runtime paths are examples or dated evidence, not files a fresh checkout must contain.
3. Review `git diff --name-only` to confirm the refactor changes documentation only, and run the repository checks
   above before handoff. Record actual results and any verification gaps under
   [verification status](VERSION_TRACKER.md#verification-status), with the documentation change under `Unreleased`.
   Leave runtime versions and JSON changelogs unchanged.
