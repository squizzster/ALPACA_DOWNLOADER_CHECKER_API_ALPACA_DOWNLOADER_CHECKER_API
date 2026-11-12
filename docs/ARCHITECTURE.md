# Architecture

[Agent instructions](../AGENTS.md) own repository boundaries. Consumers import
[alpaca_end_of_day](../alpaca_end_of_day/__init__.py); `libs/` is internal. Root executables adapt package-owned
behavior. Tests use synthetic stores/HTTP doubles; resources hold contracts, calendar and recovery labels.

## Daily update

`./alpaca-cli-eod update` maintains pricing → lineage → serving catalogue. Producers validate/publish;
downstream stages trust completed handoffs. [Operations](operations.md#daily-update) owns invocation;
[technical contracts](technical-contracts.md#trust-and-ownership) own storage guarantees.

After selection, existing eligible summary members complete live check → correction → append before
discovery acquisition can advance their dependencies. Changed identity or lost eligibility reconciles first.
The live check compares the first and latest up to ten stored fully adjusted sessions from the compact summary.
Only a changed latest session selects localized correction; any earlier selected difference selects full
retained-span replacement. Matching observations permit missing-session acquisition. The first request covers its full New York session; the second spans the selected tail.
Both are capped at completed coverage. Overlapping first/tail dates are requested once.
Selected retained dependencies and ordinary downloads use the same `all` acceptance rule from
their producer-owned comparison handoff. Before that comparison, a lagging dependency's completed missing-suffix
acquisition selects whether there is work: empty suffixes leave retained storage untouched, while nonempty suffixes
cross into the approved acquisition without another request or calculation. They do not check other historical
views or confirm approved history again.
After discovery, maintenance checks newly eligible or invalidated members without an applicable completed
full-acquisition or approved-append handoff; unchanged completed checks carry
their summary entry, pricing generation and original request scope within the operation. Durable corrections carry their selected scope,
cause and observations across restart; an interrupted publication finishes through its existing journal.
Pricing maintains the comparison tail on every publication. Recognized legacy sources publish it through a
bounded metadata upgrade before provider work. Local correction and append share partition assembly; lineage
and serving retain applicable completed work before the corrected session. The
[pricing protocol](technical-contracts.md#pricing-acquisition-and-continuation) defines the exact decisions and recovery boundaries.

| Responsibility | Authority |
|---|---|
| Accepted boundaries, work selection, estimates | [universe_plan.py](../libs/alpaca_lineage_build/universe_plan.py): `boundaries`, `select_work`, `preview` |
| Recovery, observation, registration, orchestration | [universe_update.py](../libs/alpaca_lineage_build/universe_update.py): `run_update` |
| Live check → correction → approved append | [update.py](../libs/alpaca_lineage_build/update.py): `update_everything` |
| Dependency closure and complete-group publication | [pipeline.py](../libs/alpaca_lineage_build/pipeline.py): `run_lineage`, `process_group` |
| Offline serving maintenance | [producer.py](../libs/alpaca_serving_catalogue/producer.py): `maintain` |

The pure [API solver](../libs/alpaca_data_api_solver/__init__.py), `build_plan`, maps reasoned needs to ordered
requests. The shared HTTP executor owns batching, pagination and attempt sequencing; endpoint adapters own
validation and retry/rate/timeout policies. Dependent needs follow responses. See the
[API plan contract](technical-contracts.md#api-plan).
Pricing's [adaptive acquisition owner](../libs/alpaca_pricing_download/adaptive.py) selects each dependent
adjustment stage and carries its completed eight-view handoff into publication; the
[pricing contract](technical-contracts.md#pricing-acquisition-and-continuation) defines its empirical policy.

## Lineage and identity

Dependency membership, dated row ownership and provider-UUID membership are distinct. Dependency links do not
merge identities. The [allocation criterion](lineage.md#ownership-allocation-criterion) compares plausible
owners with nonallocation; genuine ambiguity remains explicit and independently supported intervals survive.

Ordered authorities: [ownership intervals](../libs/alpaca_end_of_day_security_correlation/ownership_interval_rules.py),
[row allocation](../libs/alpaca_end_of_day_security_correlation/row_correlation.py),
[UUID membership](../libs/alpaca_end_of_day_security_correlation/security_identity.py).

Lineage's compiler/evaluator serve production and offline replay; correlation owns ordered rule registries and
certificate-backed rule factories. [Lineage](lineage.md) owns policy authorization, reuse and evidence constraints;
the [policy lab](policy-lab.md) owns read-only candidate comparison/audit.

## Serving and consumption

[Serving policy](technical-contracts.md#serving-catalogue) adds eligibility to completed assignments without
provider requests or identity-rule replay. Empty/ineligible results complete work; unreadable required
publications leave work pending.

`StandaloneReader` exposes resolved group histories/evidence; `DataLibrary` exposes online identities. Both
use the same locked history assembly. [Public API](public-api.md) owns reader semantics;
[exports](exports.md) own output contracts. Default standalone store: repository `data_pricing/`.

## Code ownership

| Owner | Responsibility |
|---|---|
| [IS_ELIGIBLE_LISTED_BUSINESS_EQUITY](../IS_ELIGIBLE_LISTED_BUSINESS_EQUITY/) | Independent consumer exporting public-reader identity metadata and group evidence without prices or eligibility assessments. |
| [bloodhound_app](../bloodhound_app/README.md) | Standalone offline CSV-to-Parquet application with its own CLI, dependencies, tests and publication limitations. |
| [alpaca_end_of_day](../alpaca_end_of_day/) | Public API, shared history assembly, CSV exports, reader adapters. |
| [alpaca_cli](../libs/alpaca_cli/) | Canonical parser/dispatch, progress adapters and post-update recovery-registry Git commits/pushes. |
| [alpaca_heartbeat](../libs/alpaca_heartbeat/) | Shared opt-in stderr lifecycle timer, protocol encoding and consumption. |
| [alpaca_recovery_registry](../libs/alpaca_recovery_registry/) | Locked, atomic recovery-label union. |
| [alpaca_pricing_download](../libs/alpaca_pricing_download/) | Acquisition, pricing publication, endpoint validation, summary storage. |
| [alpaca_lineage_build](../libs/alpaca_lineage_build/) | Orchestration, lineage, policy compilation/evaluation, summary membership, registration, status. |
| [alpaca_serving_catalogue](../libs/alpaca_serving_catalogue/) | Eligibility, public projections, catalogue publication/recovery. |
| [alpaca_data_api_solver](../libs/alpaca_data_api_solver/) | Pure request planning. |
| [alpaca_security_lineage](../libs/alpaca_security_lineage/) | Identity/action acquisition and normalization. |
| [alpaca_end_of_day_security_correlation](../libs/alpaca_end_of_day_security_correlation/) | Ownership intervals, row assignments, UUID rules, certified rule factories. |
| [alpaca_lineage_policy_lab](../libs/alpaca_lineage_policy_lab/) | Read-only selection/comparison/audit using the shared policy core. |
| [alpaca_end_of_day_pipeline](../libs/alpaca_end_of_day_pipeline/) | Compatibility version emitted by correlation reports. |
| [alpaca_end_of_day_reader](../libs/alpaca_end_of_day_reader/) | Numerical source-choice consumption guards. |
| [alpaca_bloodhound_export](../libs/alpaca_bloodhound_export/) | Offline Parquet conversion. |
| [common](../libs/common/) | Market calendar, SQLite connections, store operation lock, shared provider HTTP execution. |
