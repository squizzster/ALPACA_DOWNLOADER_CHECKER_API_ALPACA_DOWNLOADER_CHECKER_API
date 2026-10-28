# Version authorities

Owning constants and runtime JSON changelogs are authoritative; this index stores no duplicate version table.
Runtime validators check one-based changelog array position against the owning constant.
[AGENTS.md](AGENTS.md#changes) owns bump/entry requirements and the documentation-only exemption.

## Runtime releases

| Owner and constant | Changelog |
|---|---|
| [Business-equity baseline](IS_ELIGIBLE_LISTED_BUSINESS_EQUITY/__init__.py): `VERSION` | [Baseline history](IS_ELIGIBLE_LISTED_BUSINESS_EQUITY/change_log.json) |
| [Public API](alpaca_end_of_day/__init__.py): `API_VERSION` | [API history](resources/alpaca_end_of_day_api_change_log.json) |
| [CLI](libs/alpaca_cli/__init__.py): `VERSION` | [CLI history](resources/alpaca_cli_change_log.json) |
| [Heartbeat reporting](libs/alpaca_heartbeat/__init__.py): `VERSION` | [Heartbeat history](resources/alpaca_heartbeat_change_log.json) |
| [Pricing](libs/alpaca_pricing_download/contract.py): `RUNTIME_VERSION` | [Pricing history](resources/alpaca_pricing_download_change_log.json) |
| [Live check](libs/alpaca_pricing_download/live_check.py): `VERSION` | [Live-check history](resources/alpaca_live_check_experiment_change_log.json) |
| [Lineage](libs/alpaca_lineage_build/contract.py): `RUNTIME_VERSION` | [Lineage history](resources/alpaca_lineage_build_change_log.json) |
| [Operator evidence](libs/alpaca_lineage_build/evidence.py): `EVIDENCE_VERSION` | [Evidence history](resources/alpaca_lineage_evidence_change_log.json) |
| [Correlation](libs/alpaca_end_of_day_security_correlation/correlation_version.py): `CORRELATION_VERSION` | [Correlation history](resources/alpaca_end_of_day_security_correlation_change_log.json) |
| [Pipeline report](libs/alpaca_end_of_day_pipeline/pipeline_version.py): `PIPELINE_VERSION` | [Pipeline history](resources/alpaca_end_of_day_pipeline_change_log.json) |
| [Serving](libs/alpaca_serving_catalogue/contract.py): `RUNTIME_VERSION` | [Serving history](resources/alpaca_serving_catalogue_change_log.json) |
| [API solver](libs/alpaca_data_api_solver/__init__.py): `VERSION` | [Solver history](resources/alpaca_data_api_solver_change_log.json) |
| [Recovery registry](libs/alpaca_recovery_registry/__init__.py): `VERSION` | [Registry history](resources/alpaca_recovery_registry_change_log.json) |
| [Policy lab](libs/alpaca_lineage_policy_lab/contract.py): `LAB_VERSION` | [Lab history](resources/alpaca_lineage_policy_lab_change_log.json) |
| [Parquet export](libs/alpaca_bloodhound_export/__init__.py): `VERSION` | [Export history](resources/alpaca_bloodhound_export_change_log.json) |

## Compatibility

Pricing/lineage/serving `RUNTIME_VERSION` tracks releases; their `VERSION` constants retain persisted publication
contracts. A runtime bump alone neither invalidates completed decisions nor authorizes reconstruction.
Readable schemas need not be reusable. Owner compatibility gates decide reuse; do not relabel stored artifacts
to make numbers agree.

- [Pricing representation](docs/technical-contracts.md#standalone-artifacts): source and selected boundary versions may differ.
- [Lineage reuse](docs/lineage.md#lineage-reuse): policy identity, complete-group inputs and exact supported promotion.
- [Serving checkpoints](docs/technical-contracts.md#serving-catalogue): policy/calendar-bound positive and negative decisions.
- [Policy provenance](libs/alpaca_lineage_build/correlation_policy_contract.py): serialized identity of the
  [canonical YAML](resources/alpaca_security_correlation_policy.yaml).
- Independent schemas remain with their owners: [CSV](alpaca_end_of_day/_csv_export.py) `SCHEMA_VERSION`,
  [lifecycle](libs/alpaca_heartbeat/__init__.py) `SCHEMA_VERSION`,
  [identity evidence](libs/alpaca_security_lineage/settings.py) `REPORT_VERSION`.

Dependency pins: [production](requirements.txt), [Parquet](requirements-bloodhound.txt),
[policy lab](requirements-policy-lab.txt). Verification: [development](docs/development.md#verification).
