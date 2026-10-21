# Architecture

The current application boundary uses one runtime store, outside Git:

| Workflow | Store | Consumer |
|---|---|---|
| Pricing/lineage and serving catalogue | `data_pricing/` | `alpaca_end_of_day.DataLibrary` |
| Resolved-history and evidence inspection | Same store | `alpaca_end_of_day.StandaloneReader` |

A separate serving producer consumes completed lineage and pricing decisions. Both readers use the same
locked history assembly. Neither reads the retained monthly workflow's `data_vtest/` or `data/end_of_day/`.

## Daily update

`alpaca-eod update` → [universe_update.py](../libs/alpaca_lineage_build/universe_update.py):

1. Freeze completed-session target; require initialized work state, projection and scheduling coverage.
2. Recover known journals; accept one complete catalogue observation shared across lineage phases.
3. Compare compact boundaries; register additions; persist reasons/targets before acquisition.
4. Acquire initial/invalidated histories; refresh changed metadata across complete groups.
5. Live-check eligible histories → correct differing spans → hand checked source generations to append.
6. Publish affected lineage/summary and durable catalogue obligations before acknowledging upstream work.
7. Publish serving decisions for changed complete groups, then acknowledge those exact obligations.

The exchange allowlist gates new discovery. Catalogue absence neither removes history nor proves
unavailability. Registration cannot satisfy unfinished pricing. The lineage index projects published
lookups; explicit migration establishes coverage without subsequent receipt censuses.

[update.py](../libs/alpaca_lineage_build/update.py) owns maintenance: failed history checking blocks append;
independent successful publications remain recoverable. Coverage, refresh, replacement and complete-group
obligations survive until satisfying publication. Group retry restrictions span daily/standalone commands,
maintenance and discovered dependencies. Every invocation checks live; saved experiments are not inputs.
Older targets preserve newer sources; initial/dependent acquisitions cannot silently extend the target.

Ordinary/dependency downloads retain endpoint checks. Compatible empty coverage advances in batches;
approved nonempty append trusts the completed history boundary. New summary members get their own check.
Active histories outside the summary still advance through pricing. See [Operations](operations.md).

## API planning

Applications specify needs/reasons → pure solver produces ordered JSON actions → pricing/identity clients
execute pagination/retries. Compatible symbols/ranges batch; adjustments/asof remain distinct.
Symbol metadata may use catalogue batches with exact fallbacks; CUSIPs always use exact lookups.
[Plan contract](technical-contracts.md#api-plan).

## Lineage and identity

One worker selects changed inputs/outdated rules, expands dependencies and reads pricing under sorted
ticker locks. Delegated acquisition releases locks; enlarged groups reselect sources and reuse decoded
inputs only when generation/scope match. Metadata batches across selected groups.

Unchanged groups reuse decisions; rule-only changes may reuse compatible approved evidence. Changed groups
run preliminary → cross-source → fixed-point correlation, then publish assignments before acknowledging
pricing. Dependency links do not merge identities; ambiguous rows stay unassigned.

Ordered authorities: [ownership intervals](../libs/alpaca_end_of_day_security_correlation/ownership_interval_rules.py),
[row allocation](../libs/alpaca_end_of_day_security_correlation/row_correlation.py),
[UUID membership](../libs/alpaca_end_of_day_security_correlation/security_identity.py).

## Serving catalogue

[alpaca_serving_catalogue](../libs/alpaca_serving_catalogue/) owns eligibility, public projections and
incremental publication. It accepts resolved IDs and assignments, evaluates the new serving guarantees,
and publishes a compact SQLite catalogue. Negative eligibility is completed work; an unreadable required
publication leaves work pending. No identity rules or provider requests run here.

The lineage selector transaction records catalogue obligations and complete input-group membership.
Maintenance selects those groups plus their previous contributions, replacing obsolete IDs and mappings
atomically. Compatible decisions reuse recorded inputs without opening histories. Published append scopes
reuse completed history checks; only new observations and continuity seams need serving validation.
Downloader replacements and changed historical selection invalidate affected checks. Explicit bootstrap
establishes old stores' membership projection and catalogue coverage over indexed groups.

`DataLibrary(store=...)` materializes public catalogue fields in one short read transaction. Search and
resolution use that snapshot. Histories use the shared engine, checking the selected serving decision,
lineage and pricing generations under group locks before payload consumption. `StandaloneReader` retains
resolved partial-history access independently of serving eligibility.
[Serving contract](technical-contracts.md#serving-catalogue).

## Retained combined workflow

`alpaca-eod legacy catalogue update` → [pipeline.py](../libs/alpaca_end_of_day_v6/pipeline.py):

```text
symbol resolution → monthly source → validation → lineage evidence → correlation
→ advisory warnings → identity candidate → availability → catalogue
```

Only submitted canonical symbols reacquire pricing/evidence. Correlation/availability cover the affected
identity closure; publication preserves unrelated objects. Availability governs serving, never membership/bars.

`legacy catalogue build` schedules the same pipeline via [generation.py](../libs/alpaca_end_of_day_clean_room/generation.py):
checkpoints, immutable universe snapshot, mandatory final audit. Resume uses original inputs/contracts.
Separate activation selects audited `data_vtest/`, retaining its predecessor.
Its audit and activation use the owning monthly reader; neither depends on `DataLibrary`.

## Code ownership

| Owner | Responsibility |
|---|---|
| [alpaca_end_of_day/](../alpaca_end_of_day/) | Supported consumer library/reader CLIs. |
| [alpaca_cli/](../libs/alpaca_cli/) | Canonical parser/dispatch; no domain publication rules. |
| [alpaca_recovery_registry/](../libs/alpaca_recovery_registry/) | Sorted union, stable shared lock, atomic snapshot. |
| [alpaca_pricing_download/](../libs/alpaca_pricing_download/) | Acquisition, append, pricing publication, validators, summary storage. |
| [alpaca_lineage_build/](../libs/alpaca_lineage_build/) | Update orchestration, dependency registration, lineage publication, summary eligibility, offline status. |
| [alpaca_serving_catalogue/](../libs/alpaca_serving_catalogue/) | Serving policy, public metadata, catalogue publication, bootstrap and recovery obligations. |
| [alpaca_data_api_solver/](../libs/alpaca_data_api_solver/) | Pure planning; no network/storage. |
| [alpaca_security_lineage/](../libs/alpaca_security_lineage/) | Provider identity/action acquisition, normalization. |
| [alpaca_end_of_day_security_correlation/](../libs/alpaca_end_of_day_security_correlation/) | Dated ownership, row allocation, identity rules, catalogue engine. |
| [alpaca_end_of_day_v6/](../libs/alpaca_end_of_day_v6/) | Combined monthly cascade/publication. |
| [alpaca_end_of_day_clean_room/](../libs/alpaca_end_of_day_clean_room/) | Isolated construction, audit, activation. |
| [alpaca_end_of_day_pipeline/](../libs/alpaca_end_of_day_pipeline/) | Shared validation rules. |
| [alpaca_end_of_day_warning_pipeline/](../libs/alpaca_end_of_day_warning_pipeline/) | Advisory warnings. |
| [alpaca_end_of_day_reader/](../libs/alpaca_end_of_day_reader/) | Internal history assembly/source selection. |
| [alpaca_bloodhound_export/](../libs/alpaca_bloodhound_export/) | Offline Parquet conversion. |
| [common/](../libs/common/) | Calendar, atomic writes, SQLite connections. |

Root commands: adapters. `tests/`: synthetic fixtures/HTTP doubles. `resources/`: runtime contracts/changelogs,
calendar, recovery ticker list.
