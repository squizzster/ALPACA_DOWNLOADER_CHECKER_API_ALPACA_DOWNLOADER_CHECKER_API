# Architecture

The supported [public package](../alpaca_end_of_day/__init__.py) reads one standalone store, normally
`data_pricing/`: `DataLibrary` exposes approved identities; `StandaloneReader` exposes resolved histories
and evidence. Both use the same locked history assembly.

## Daily update

`./alpaca-eod update` maintains pricing → lineage → serving catalogue. Producers own validation and
publication; downstream stages trust completed handoffs. See [operating rules](operations.md#daily-update)
and [trust boundaries](technical-contracts.md#trust-and-ownership).

Execution authorities:

| Concern | Source |
|---|---|
| Accepted boundaries, selected work, estimates | [universe_plan.py](../libs/alpaca_lineage_build/universe_plan.py) |
| Registration, acquisition, reconciliation and daily orchestration | [universe_update.py](../libs/alpaca_lineage_build/universe_update.py) |
| Live check → correction → approved append | [update.py](../libs/alpaca_lineage_build/update.py) |
| Dependency expansion and lineage publication | [pipeline.py](../libs/alpaca_lineage_build/pipeline.py) |
| Offline serving maintenance | [producer.py](../libs/alpaca_serving_catalogue/producer.py) |

## API planning

The pure [solver](../libs/alpaca_data_api_solver/__init__.py) maps reasoned needs to ordered request actions;
HTTP clients own pagination/retries. See the [plan contract](technical-contracts.md#api-plan).

## Lineage and identity

Dependency membership, dated row ownership and provider-UUID membership are distinct. Dependency links
alone cannot merge identities. Ambiguous rows remain unassigned; independently supported intervals survive.
Ordered authorities: [ownership intervals](../libs/alpaca_end_of_day_security_correlation/ownership_interval_rules.py),
[row allocation](../libs/alpaca_end_of_day_security_correlation/row_correlation.py),
[UUID membership](../libs/alpaca_end_of_day_security_correlation/security_identity.py).

## Serving catalogue

The [serving policy](technical-contracts.md#serving-catalogue) adds eligibility to completed assignments;
it makes no provider requests or identity decisions. Empty/ineligible results are completed work;
unreadable required publications leave work pending. Reader behavior belongs in the [API guide](public-api.md).

## Code ownership

Consumer applications import only `alpaca_end_of_day`; `libs/` is internal. Root commands are executable
adapters. Tests use synthetic fixtures/HTTP doubles; resources hold runtime contracts, calendar and recovery labels.

| Owner | Responsibility |
|---|---|
| [alpaca_end_of_day](../alpaca_end_of_day/) | Public library, shared standalone reader and reader CLIs. |
| [alpaca_cli](../libs/alpaca_cli/) | Canonical parser/dispatch. |
| [alpaca_recovery_registry](../libs/alpaca_recovery_registry/) | Locked, atomic label union. |
| [alpaca_pricing_download](../libs/alpaca_pricing_download/) | Acquisition, pricing publication, endpoint validation, summary storage. |
| [alpaca_lineage_build](../libs/alpaca_lineage_build/) | Orchestration, lineage, summary eligibility, registration and offline status. |
| [alpaca_serving_catalogue](../libs/alpaca_serving_catalogue/) | Eligibility, public projections, catalogue publication and recovery. |
| [alpaca_data_api_solver](../libs/alpaca_data_api_solver/) | Pure request planning. |
| [alpaca_security_lineage](../libs/alpaca_security_lineage/) | Identity/action acquisition and normalization. |
| [alpaca_end_of_day_security_correlation](../libs/alpaca_end_of_day_security_correlation/) | Ownership intervals, row assignment and UUID rules. |
| [alpaca_end_of_day_pipeline](../libs/alpaca_end_of_day_pipeline/) | Compatibility version emitted by correlation reports. |
| [alpaca_end_of_day_reader](../libs/alpaca_end_of_day_reader/) | Shared history source selection. |
| [alpaca_bloodhound_export](../libs/alpaca_bloodhound_export/) | Offline Parquet conversion. |
| [common](../libs/common/) | Calendar and SQLite connections. |
