# Architecture

Two workflows share identity rules but publish different stores.

| Workflow | Store | Consumer |
|---|---|---|
| Standalone pricing and lineage | `data_pricing/` | `alpaca_end_of_day.StandaloneReader` |
| Combined acquisition-to-catalogue pipeline | `data_vtest/` | `alpaca_end_of_day.DataLibrary` |

Standalone lineage does not publish the online catalogue. The legacy `data/end_of_day/` tree is
not a fallback. Stores are runtime data, outside Git.

## Daily update

`alpaca-eod update` enters
[universe_update.py](../libs/alpaca_lineage_build/universe_update.py):

1. Freeze a completed-session target; require initialized work state, projection and scheduling coverage.
2. Recover known publication journals, then accept one complete catalogue observation shared across lineage phases.
3. Compare compact boundaries, register accepted additions, and persist reasons/targets before acquisition.
4. Acquire initial or invalidated histories and explicitly refresh changed metadata across complete groups.
5. Live-check eligible retained histories, correct differing spans, then pass checked source generations to append.
6. Finish affected lineage and summary publication before acknowledging durable work.

The exchange allowlist controls new catalogue discovery. Catalogue absence never removes history or proves
unavailability. The additive registry and orchestration queue have separate purposes: registration cannot
make unfinished pricing disappear. The lineage index projects published lookup evidence so daily comparison
does not open every receipt. An explicit migration establishes projection coverage once.

The existing maintenance sequence remains owned by [update.py](../libs/alpaca_lineage_build/update.py).
Failed history checking prevents append. Successful independent publications remain recoverable.
Accepted coverage, refresh, replacement and dependency-group obligations persist until satisfying producer
publications select them. Retry restrictions apply to complete affected groups through daily and standalone
commands, including maintenance and newly discovered dependencies.
Every invocation checks live; saved experiment results are not production inputs. Sources already beyond
an older target remain unchanged. Initial/dependent acquisition cannot silently advance that target.

Ordinary downloads and dependency advancement retain their endpoint checks. Compatible empty coverage can
advance in batches; approved nonempty append relies on the completed history boundary. Newly eligible
summary members receive their own check. Retained active histories outside the summary still advance
through the pricing owner. See [operations](operations.md).

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

`alpaca-eod catalogue update` enters [pipeline.py](../libs/alpaca_end_of_day_v6/pipeline.py):

```text
symbol resolution → monthly source → validation → lineage evidence
    → correlation → advisory warnings → identity candidate → availability → catalogue
```

Only submitted canonical symbols reacquire pricing/evidence. Correlation and availability include the
affected identity closure; catalogue publication preserves unrelated objects.
Availability decides whether resolved histories can be served; it cannot change membership or bars.

`alpaca-eod catalogue build` schedules this same pipeline through
[generation.py](../libs/alpaca_end_of_day_clean_room/generation.py), with checkpoints, an immutable
universe snapshot and a mandatory final audit. Resume consumes the original snapshot and contracts.
Separate activation selects the audited store as `data_vtest/`, retaining its predecessor.

## Code ownership

| Owner | Responsibility |
|---|---|
| [alpaca_end_of_day/](../alpaca_end_of_day/) | Supported consumer library and reader CLIs. |
| [alpaca_cli/](../libs/alpaca_cli/) | One canonical parser and dispatch; no domain publication rules. |
| [alpaca_recovery_registry/](../libs/alpaca_recovery_registry/) | Sorted additive membership, stable union lock and atomic snapshot publication. |
| [alpaca_pricing_download/](../libs/alpaca_pricing_download/) | Acquisition, append, pricing publication, endpoint validators and summary storage. |
| [alpaca_lineage_build/](../libs/alpaca_lineage_build/) | Update orchestration, dependency registration, lineage publication, summary eligibility and offline producer status. |
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
