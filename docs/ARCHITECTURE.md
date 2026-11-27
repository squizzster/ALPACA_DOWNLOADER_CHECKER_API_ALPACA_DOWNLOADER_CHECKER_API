# Architecture

[Agent instructions](../AGENTS.md) own repository boundaries. Consumers import
[alpaca_end_of_day](../alpaca_end_of_day/__init__.py); `libs/` is internal. Root executables adapt package-owned
behavior. Tests use synthetic stores/HTTP doubles; resources hold contracts, calendar and recovery labels.

## Producer handoffs

The producer validates new output and publishes its selector last. The handoff binds store, generation, identity mapping and approved scope. After reacquiring a lock, check applicability. A changed binding selects affected work at its owner. A missing boundary requires explicit owning recovery or rebuild.

| Producer | What the next stage can use | Authority |
|---|---|---|
| Pricing | A selected pricing generation and the decision to reuse, correct the latest session or replace a retained span. Lineage consumes the approved prices. | [Comparison and acquisition](technical-contracts.md#pricing-acquisition-and-continuation) |
| Lineage | The selected group, dated assignments, unresolved intervals and numerical decisions. Serving consumes those decisions to assess public eligibility. | [Lineage reuse](lineage.md#lineage-reuse) |
| Serving | Online public IDs, dated ticker ranges and applicable input generations from one catalogue selection. Readers assemble histories and exporters validate their newly constructed output. | [Serving catalogue](technical-contracts.md#serving-catalogue) and [export inputs](exports.md#shared-catalogue-input) |

Pricing compares the first and latest up to ten fully adjusted sessions once. A match approves reuse and acquisition of missing sessions, including reuse of the other seven historical views. A latest-session-only difference selects local correction. An earlier difference selects full retained-span replacement. An incomplete comparison establishes no difference. Acquisition, lineage, serving and recovery inherit the decision's scope.

Serving may read selected prices to assemble a history. Retries and reads cannot reauthenticate approved prices. Owner journals cover process interruption. Machine and power failure fall outside this contract. [Storage instructions](../AGENTS.md#trust-before-touching-storage) · [Open findings](trust-boundary-handoff.html)

## Daily update

`./alpaca-cli-eod update` maintains pricing → lineage → serving. [Invocation](operations.md#daily-update) · [Storage guarantees](technical-contracts.md#trust-and-ownership)

1. Before discovery acquisition advances dependencies, reconcile changed identities or lost eligibility, then process existing eligible summary members through live check → correction → append. Request the first full New York session and the selected tail within completed coverage. Request overlapping dates once.
2. For a lagging retained dependency, acquire the missing suffix first. Empty suffixes leave storage untouched. Nonempty suffixes carry their observations into approved acquisition.
3. After discovery, check newly eligible or invalidated members lacking an applicable acquisition or append approval. Unchanged checks carry their summary entry, generation and original request scope within the operation. Durable corrections retain scope, cause and observations across restart.

Pricing publishes the comparison tail each time. Recognized legacy sources publish it through a bounded metadata upgrade before provider work. Local correction and append share partition assembly. Lineage and serving retain applicable work before the corrected session. [Pricing protocol](technical-contracts.md#pricing-acquisition-and-continuation)

| Responsibility | Authority |
|---|---|
| Accepted boundaries, work selection, estimates | [universe_plan.py](../libs/alpaca_lineage_build/universe_plan.py): `boundaries`, `select_work`, `preview` |
| Prepare native handoff and independent JSON adapter | [prepare.py](../libs/alpaca_pipeline/prepare.py); [contract and Phase 1 scope](development.md#pipeline-phase-1-prepare) |
| Native history completion and serving handoff | [histories.py](../libs/alpaca_pipeline/histories.py); [contract and Phase 2 scope](development.md#pipeline-phase-2-history-production) |
| Native serving stage, JSON adapter and continuous-lock orchestration | [serve.py](../libs/alpaca_pipeline/serve.py), [runner.py](../libs/alpaca_pipeline/runner.py); [Phase 3 contract](development.md#pipeline-phase-3-complete-pipeline) |
| Recovery, observation, registration, orchestration | [universe_update.py](../libs/alpaca_lineage_build/universe_update.py): `run_update` |
| Live check → correction → approved append | [update.py](../libs/alpaca_lineage_build/update.py): `update_everything` |
| Group selection, prefetch and lineage command coordination | [pipeline.py](../libs/alpaca_lineage_build/pipeline.py): `run_lineage` |
| Dependency closure, component evaluation and complete-group completion | [group_engine.py](../libs/alpaca_lineage_build/group_engine.py): `process_group`, `correlate_components`, `publish_completed_group` |
| Offline serving maintenance | [producer.py](../libs/alpaca_serving_catalogue/producer.py): `maintain` |

The pure [API solver](../libs/alpaca_data_api_solver/__init__.py) (`build_plan`) orders requests from stated needs. Responses select dependent requests. Shared HTTP execution owns batching, pagination and attempt order. Endpoint adapters own validation, retries, rate limits and timeouts. [API plan](technical-contracts.md#api-plan)

Pricing's [adaptive acquisition owner](../libs/alpaca_pricing_download/adaptive.py) selects dependent adjustment stages and passes their completed eight-view result to publication. [Acquisition policy](technical-contracts.md#pricing-acquisition-and-continuation)

## Lineage and identity

Dependency links do not merge identities. Dated row ownership and UUID membership have separate rules. Weigh each plausible owner against nonallocation. Genuine ambiguity remains explicit, and independently supported intervals survive. [Allocation criterion](lineage.md#ownership-allocation-criterion)

Ordered authorities: [ownership intervals](../libs/alpaca_end_of_day_security_correlation/ownership_interval_rules.py),
[row allocation](../libs/alpaca_end_of_day_security_correlation/row_correlation.py),
[UUID membership](../libs/alpaca_end_of_day_security_correlation/security_identity.py).

Production and replay share lineage's compiler/evaluator. Correlation owns ordered rule registries and certificate-backed factories. [Lineage](lineage.md) governs policy authorization, evidence and reuse. The [policy lab](policy-lab.md) compares and audits offline.

After frozen-journal recovery, the [ruleset owner](../libs/alpaca_lineage_build/ruleset.py) accepts schema-2 [canonical YAML](../resources/alpaca_security_correlation_policy.yaml) once. It selects registered generic Python rules (`policy`) and optional generic `producers` with independent identities. A container-format change leaves applicable decisions valid. Authored assertions fail before new store/API work. Retained research provenance never supplies fresh inference. The [lineage rule tool](lineage-debug.md) explains recorded results or replays selected/all groups via public JSON. Paired comparison distinguishes stored/control drift from candidate impact on one loaded group, without production publication.

The group engine closes dependencies, selects source work, evaluates current components and requests necessary pricing outside pricing locks. Ordinary correlation and selected source work share one consumer for journal publication, receipt-cache/index updates and exact workflow acknowledgement. JSON restoration consumes completed [source work](../libs/alpaca_lineage_build/source_work_contract.py) and [rulesets](../libs/alpaca_lineage_build/ruleset_contract.py) without importing executors or recompiling.

The [metadata owner](../libs/alpaca_lineage_build/metadata.py) and [provider-evidence boundary](../libs/alpaca_security_lineage/inference_evidence.py) select retained API observations for new inference once. Consumers trust that projection. Original receipts remain comparison/recovery data. Frozen journals install completed outputs under recorded provenance and accepted assignments, membership and numerical choices retain their scope.

Optional [component refinement](../libs/alpaca_end_of_day_security_correlation/component_refinement.py), explicitly enabled in canonical YAML, uses completed components, provider renames and accepted extension edges. Numerical overlap, dated ownership and serving eligibility remain distinct. [Legacy provenance and pending obligations](lineage.md#sourced-event-facts)

## Serving and consumption

[Serving policy](technical-contracts.md#serving-catalogue) adds eligibility to completed assignments without
provider requests or identity-rule replay. Empty/ineligible results complete work; unreadable required
publications leave work pending.

`StandaloneReader` exposes resolved group histories/evidence; `DataLibrary` exposes online identities. Both
use the same locked history assembly. CSV, Bloodhound Parquet, and EOD events share the catalogue export
selection and group reader in [catalogue export](../alpaca_end_of_day/_catalogue_export.py). The EOD event
writer consumes the same completed numerical projection as Parquet. The metadata companion consumes prepared public-ID records directly from the selected catalogue, without opening prices, lineage receipts or provider connections. [Public API](public-api.md) owns reader semantics;
[exports](exports.md) own output contracts. Default standalone store: repository `data_pricing/`.

## Code ownership

| Owner | Responsibility |
|---|---|
| [IS_ELIGIBLE_LISTED_BUSINESS_EQUITY](../IS_ELIGIBLE_LISTED_BUSINESS_EQUITY/) | Independent consumer exporting public-reader identity metadata and group evidence without prices or eligibility assessments. |
| [bloodhound_app](../bloodhound_app/README.md) | Bloodhound conversion from shared public catalogue histories, completed CSV exports or legacy files. Owns the numerical projection, Parquet schema, and [EOD event writer](../bloodhound_app/eod_stream.py). |
| [alpaca_end_of_day](../alpaca_end_of_day/) | Public API, shared history assembly, CSV exports, reader adapters. |
| [alpaca_cli](../libs/alpaca_cli/) | Canonical parser/dispatch, progress adapters and post-update recovery-registry Git commits/pushes. |
| [alpaca_heartbeat](../libs/alpaca_heartbeat/) | Shared opt-in stderr lifecycle timer, protocol encoding and consumption. |
| [alpaca_recovery_registry](../libs/alpaca_recovery_registry/) | Locked, atomic recovery-label union. |
| [alpaca_pricing_download](../libs/alpaca_pricing_download/) | Acquisition, pricing publication, endpoint validation, summary storage. |
| [alpaca_lineage_build](../libs/alpaca_lineage_build/) | Orchestration, lineage, policy compilation/evaluation, summary membership, registration, status. |
| [alpaca_pipeline](../libs/alpaca_pipeline/) | Native stage contracts, three shared implementations, independent JSON adapters and the continuous-lock runner. |
| [alpaca_serving_catalogue](../libs/alpaca_serving_catalogue/) | Eligibility, public projections, catalogue publication/recovery. |
| [alpaca_data_api_solver](../libs/alpaca_data_api_solver/) | Pure request planning. |
| [alpaca_security_lineage](../libs/alpaca_security_lineage/) | Identity/action acquisition and normalization. |
| [alpaca_end_of_day_security_correlation](../libs/alpaca_end_of_day_security_correlation/) | Ownership intervals, row assignments, UUID rules, certified rule factories. |
| [alpaca_lineage_policy_lab](../libs/alpaca_lineage_policy_lab/) | Read-only selection/comparison/audit using the shared policy core. |
| [alpaca_lineage_debug](../libs/alpaca_lineage_debug/) | Public JSON diagnostics, research YAML selection, group replay, recorded explanations, saved handoffs and export-instance reports. |
| [alpaca_end_of_day_pipeline](../libs/alpaca_end_of_day_pipeline/) | Compatibility version emitted by correlation reports. |
| [alpaca_end_of_day_reader](../libs/alpaca_end_of_day_reader/) | Numerical source-choice consumption guards. |
| [alpaca_bloodhound_export](../libs/alpaca_bloodhound_export/) | Canonical catalogue-to-Parquet command adapter and retained standalone conversion primitives. |
| [common](../libs/common/) | Market calendar, SQLite connections, store operation lock, shared provider HTTP execution. |
