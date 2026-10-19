# Architecture

Two workflows share identity rules but publish different stores.

| Workflow | Store | Consumer |
|---|---|---|
| Standalone pricing and lineage | `data_pricing/` | `alpaca_end_of_day.StandaloneReader` |
| Combined acquisition-to-catalogue pipeline | `data_vtest/` | `alpaca_end_of_day.DataLibrary` |

Standalone lineage does not publish the online catalogue. The legacy `data/end_of_day/` tree is
not a fallback. Stores are runtime data, outside Git.

## Daily update

`download_alpaca_pricing.py --update-everything` enters
[update.py](../libs/alpaca_lineage_build/update.py):

1. Freeze one completed-session target and recover pending pricing/lineage work.
2. Load the minimal summary and live-check its last `all` rows in compatible batches.
3. Replace any differing ticker across its original start/end/asof in all eight views; finish affected lineage.
4. Pass the approved summary directly to append; request only missing sessions.
5. Publish append results, process affected lineage and refresh the summary.

Failed history checking prevents append. Successful independent publications remain recoverable.
Every invocation checks live; saved experiment results are not production inputs.

Ordinary ticker downloads retain their own endpoint-check/update path. Explicit append relies on the
approved history boundary. The [operator guide](operations.md) distinguishes these modes.

## API planning

The application specifies **what is needed and why**. The pure solver returns ordered JSON request
actions; pricing and identity clients execute them, including pagination and retries.
Compatible symbols/ranges share requests. Adjustment and asof boundaries remain distinct.
Symbol metadata can use catalogue batches with exact fallbacks; CUSIPs always use exact lookups.
See the [plan contract](technical-contracts.md#api-plan).

## Lineage and identity

One lineage worker selects changed inputs and outdated rules, expands dependencies, and consumes
published pricing under sorted ticker locks. Missing pricing is delegated with those locks released;
the worker then reloads the complete group.

Metadata requests batch across selected groups. Unchanged groups reuse completed decisions.
Rule-only changes can reuse compatible approved evidence. Changed groups use the shared preliminary,
cross-source and fixed-point correlation path, then publish assignments before acknowledging pricing work.

The ordered rule implementations are the authority:
[ownership intervals](../libs/alpaca_end_of_day_security_correlation/ownership_interval_rules.py),
[row allocation](../libs/alpaca_end_of_day_security_correlation/row_correlation.py) and
[UUID membership](../libs/alpaca_end_of_day_security_correlation/security_identity.py).
Dependency links do not themselves merge identities. Ambiguous rows stay unassigned.

## Combined catalogue workflow

[run_alpaca_end_of_day_ticker_pipeline.py](../run_alpaca_end_of_day_ticker_pipeline.py) runs:

```text
symbol resolution → monthly source → validation → lineage evidence
    → correlation → advisory warnings → identity candidate → availability → catalogue
```

Only submitted canonical symbols reacquire pricing/evidence. Correlation and availability include the
affected identity closure; catalogue publication preserves unrelated objects.
Availability decides whether resolved histories can be served; it cannot change membership or bars.

[build_alpaca_end_of_day_clean_room.py](../build_alpaca_end_of_day_clean_room.py) schedules this same
pipeline per canonical symbol in an isolated store, with checkpoints and a mandatory final audit.
Activation selects the audited store as `data_vtest/`.

## Code ownership

| Owner | Responsibility |
|---|---|
| [alpaca_end_of_day/](../alpaca_end_of_day/) | Supported consumer library and reader CLIs. |
| [alpaca_pricing_download/](../libs/alpaca_pricing_download/) | Acquisition, append, pricing publication, endpoint validators and summary storage. |
| [alpaca_lineage_build/](../libs/alpaca_lineage_build/) | Update orchestration, dependencies, lineage publication and summary eligibility. |
| [alpaca_data_api_solver/](../libs/alpaca_data_api_solver/) | Pure request planning; no network or store access. |
| [alpaca_security_lineage/](../libs/alpaca_security_lineage/) | Provider identity/action acquisition and normalization. |
| [alpaca_end_of_day_security_correlation/](../libs/alpaca_end_of_day_security_correlation/) | Dated ownership, row allocation, identity rules and catalogue engine. |
| [alpaca_end_of_day_v6/](../libs/alpaca_end_of_day_v6/) | Combined monthly cascade and catalogue publication. |
| [alpaca_end_of_day_clean_room/](../libs/alpaca_end_of_day_clean_room/) | Isolated construction, audit and activation. |
| [alpaca_end_of_day_pipeline/](../libs/alpaca_end_of_day_pipeline/) | Shared validation rules. |
| [alpaca_end_of_day_warning_pipeline/](../libs/alpaca_end_of_day_warning_pipeline/) | Advisory warning rules. |
| [alpaca_end_of_day_reader/](../libs/alpaca_end_of_day_reader/) | Internal history assembly and source selection. |
| [alpaca_bloodhound_export/](../libs/alpaca_bloodhound_export/) | Offline Parquet conversion. |
| [common/](../libs/common/) | Calendar, atomic writes and SQLite connections. |

Root commands are adapters; `tests/` contains synthetic fixtures and HTTP doubles.
`resources/` contains runtime contracts/changelogs, calendar data and the recovery ticker list.
