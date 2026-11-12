# Version authorities

Owning constants and runtime JSON changelogs are authoritative; this index stores no duplicate version table.
Runtime validators check one-based changelog array position against the owning constant.
[AGENTS.md](AGENTS.md#changes) owns bump/entry requirements and the documentation-only exemption.

## Runtime releases

| Owner and constant | Changelog |
|---|---|
| [Business-equity baseline](IS_ELIGIBLE_LISTED_BUSINESS_EQUITY/__init__.py): `VERSION` | [Baseline history](IS_ELIGIBLE_LISTED_BUSINESS_EQUITY/change_log.json) |
| [Standalone Bloodhound converter](bloodhound_app/alpaca_to_bloodhound.py): `RUNTIME_VERSION` | [Converter history](bloodhound_app/change_log.json) |
| [Public API](alpaca_end_of_day/__init__.py): `API_VERSION` | [API history](resources/alpaca_end_of_day_api_change_log.json) |
| [CLI](libs/alpaca_cli/__init__.py): `VERSION` | [CLI history](resources/alpaca_cli_change_log.json) |
| [Heartbeat reporting](libs/alpaca_heartbeat/__init__.py): `VERSION` | [Heartbeat history](resources/alpaca_heartbeat_change_log.json) |
| [Pricing](libs/alpaca_pricing_download/contract.py): `RUNTIME_VERSION` | [Pricing history](resources/alpaca_pricing_download_change_log.json) |
| [Live check](libs/alpaca_pricing_download/live_check.py): `VERSION` | [Live-check history](resources/alpaca_live_check_experiment_change_log.json) |
| [Lineage](libs/alpaca_lineage_build/contract.py): `RUNTIME_VERSION` | [Lineage history](resources/alpaca_lineage_build_change_log.json) |
| [Operator evidence](libs/alpaca_lineage_build/evidence.py): `EVIDENCE_VERSION` | [Evidence history](resources/alpaca_lineage_evidence_change_log.json) |
| [Correlation](libs/alpaca_end_of_day_security_correlation/correlation_version.py): `RUNTIME_VERSION` | [Correlation history](resources/alpaca_end_of_day_security_correlation_change_log.json) |
| [Pipeline transport runtime](libs/alpaca_pipeline/prepare.py): `VERSION` | [Protocol history](resources/alpaca_pipeline_change_log.json) |
| [Pipeline report](libs/alpaca_end_of_day_pipeline/pipeline_version.py): `PIPELINE_VERSION` | [Pipeline history](resources/alpaca_end_of_day_pipeline_change_log.json) |
| [Serving](libs/alpaca_serving_catalogue/contract.py): `RUNTIME_VERSION` | [Serving history](resources/alpaca_serving_catalogue_change_log.json) |
| [API solver](libs/alpaca_data_api_solver/__init__.py): `VERSION` | [Solver history](resources/alpaca_data_api_solver_change_log.json) |
| [Recovery registry](libs/alpaca_recovery_registry/__init__.py): `VERSION` | [Registry history](resources/alpaca_recovery_registry_change_log.json) |
| [Policy lab](libs/alpaca_lineage_policy_lab/contract.py): `LAB_VERSION` | [Lab history](resources/alpaca_lineage_policy_lab_change_log.json) |
| [Parquet export](libs/alpaca_bloodhound_export/__init__.py): `VERSION` | [Export history](resources/alpaca_bloodhound_export_change_log.json) |

## Compatibility

Pipeline transport `VERSION` tracks implementation compatibility; `SCHEMA_VERSION`
owns the transient request, prepared and histories document schemas independently.
Its runtime binding rejects arguments from an incompatible executable implementation.

Pricing/lineage/serving `RUNTIME_VERSION` tracks releases; their `VERSION` constants retain persisted publication
contracts. A runtime bump alone neither invalidates completed decisions nor authorizes reconstruction.
Correlation likewise separates runtime releases from `CORRELATION_VERSION`, the rule implementation selected
by authorized policy. Reuse optimizations preserve that rule identity and its completed assignments.
An exact retained-policy applicability proof can preserve an earlier completed decision's original provenance;
it authorizes neither executing the earlier policy as current nor promoting potentially affected decisions.
Metadata membership and mapped-source ownership have separate completion handoffs in the selecting lineage
receipt; a pricing change alone does not invalidate an unchanged metadata decision.
Pricing installation journals declare [INSTALLATION_HANDOFF_VERSION](libs/alpaca_pricing_download/storage.py)
independently of readable source schemas; [recovery](docs/technical-contracts.md#publication-and-recovery) consumes
their accepted staging and summary values.
Public-common continuity and distinct-security conversions have independent version-1 fact schemas;
their completed membership, source and relation handoffs belong to the lineage receipt. Serving numerical
checkpoint reuse across accepted membership changes does not change eligibility policy or catalogue schema.
Lineage computation revisions independently select owning reconstruction of earlier derived decisions while preserving accepted pricing and metadata; the lineage owner records the revision on receipts and compact index entries. Readable schemas need not be reusable. Owner compatibility gates decide reuse; do not relabel stored artifacts
to make numbers agree.

- [Pricing representation](docs/technical-contracts.md#standalone-artifacts): source and selected boundary versions may differ.
- [Pricing comparison handoff](docs/technical-contracts.md#pricing-acquisition-and-continuation): recognized legacy sources publish their missing tail without changing prices; the compact summary carries its actual pricing generation.
- [Lineage reuse](docs/lineage.md#lineage-reuse): policy identity, complete-group inputs and exact supported promotion.
- [Serving checkpoints](docs/technical-contracts.md#serving-catalogue): policy/calendar-bound positive and negative decisions.
- [Policy provenance](libs/alpaca_lineage_build/correlation_policy_contract.py): serialized identity of the
  [canonical YAML](resources/alpaca_security_correlation_policy.yaml).
- Independent schemas remain with their owners: [CSV](alpaca_end_of_day/_csv_export.py) `SCHEMA_VERSION`,
  [lifecycle](libs/alpaca_heartbeat/__init__.py) `SCHEMA_VERSION`,
  [identity evidence](libs/alpaca_security_lineage/settings.py) `REPORT_VERSION`,
  [sourced event input](libs/alpaca_security_lineage/event_supplements.py) `SCHEMA_VERSION` (lineage runtime owner).

Dependency pins: [production](requirements.txt), [Parquet](requirements-bloodhound.txt),
[policy lab](requirements-policy-lab.txt). Verification: [development](docs/development.md#verification).
