# Version authorities

Runtime versions come from the owning constant and its JSON changelog. Validators require matching one-based array positions.
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
| [Lineage rule tool](libs/alpaca_lineage_debug/__init__.py): `VERSION` | [Debug history](resources/alpaca_lineage_debug_change_log.json) |
| [Parquet export](libs/alpaca_bloodhound_export/__init__.py): `VERSION` | [Export history](resources/alpaca_bloodhound_export_change_log.json) |

## Compatibility

- Pipeline transport `VERSION` binds the executable implementation and rejects incompatible arguments. `SCHEMA_VERSION` separately owns transient request, prepared and histories documents.
- Pricing, lineage and serving `RUNTIME_VERSION` track releases. Their `VERSION` constants govern persisted publications. A runtime bump alone neither invalidates completed decisions nor authorizes reconstruction.
- Correlation separates runtime releases from policy-selected `CORRELATION_VERSION`. Reuse optimizations preserve rule identity and completed assignments. An exact retained-policy applicability proof preserves earlier provenance only. It cannot execute the old policy as current or promote potentially affected decisions.
- Metadata membership and mapped-source ownership have separate handoffs in the selecting lineage receipt. Pricing changes alone leave unchanged metadata decisions valid.
- Pricing journals declare [INSTALLATION_HANDOFF_VERSION](libs/alpaca_pricing_download/storage.py) independently of readable source schemas. [Recovery](docs/technical-contracts.md#publication-and-recovery) consumes accepted staging and summary values.
- Public-common continuity and distinct-security conversions use independent version-1 fact schemas. The lineage receipt owns their membership, source and relation handoffs. Serving may reuse numerical checkpoints across accepted membership changes without changing eligibility policy or catalogue schema.
- Lineage computation revisions select owning reconstruction of earlier derived decisions, preserving accepted pricing and metadata. Receipts and compact index entries record the revision. Readability alone cannot authorize reuse. Apply the owner's compatibility gate. Never relabel a stored artifact to match a version.

- [Pricing representation](docs/technical-contracts.md#standalone-artifacts): source and selected boundary versions may differ.
- [Pricing comparison handoff](docs/technical-contracts.md#pricing-acquisition-and-continuation): recognized legacy sources publish their missing tail without changing prices; the compact summary carries its actual pricing generation.
- [Lineage reuse](docs/lineage.md#lineage-reuse): policy identity, complete-group inputs and exact supported promotion.
- [Serving checkpoints](docs/technical-contracts.md#serving-catalogue): policy/calendar-bound positive and negative decisions.
- [Policy provenance](libs/alpaca_lineage_build/correlation_policy_contract.py): serialized identity of the
  [canonical YAML](resources/alpaca_security_correlation_policy.yaml).
- Independent schemas remain with their owners: [CSV](alpaca_end_of_day/_csv_export.py) `SCHEMA_VERSION`,
  [security metadata companion](resources/security_metadata.schema.json) manifest `version`,
  [lifecycle](libs/alpaca_heartbeat/__init__.py) `SCHEMA_VERSION`,
  [identity evidence](libs/alpaca_security_lineage/settings.py) `REPORT_VERSION`,
  [sourced event input](libs/alpaca_security_lineage/event_supplements.py) `SCHEMA_VERSION` (lineage runtime owner).

Dependency pins: [production](requirements.txt), [Parquet](requirements-bloodhound.txt),
[policy lab](requirements-policy-lab.txt). Verification: [development](docs/development.md#verification).
