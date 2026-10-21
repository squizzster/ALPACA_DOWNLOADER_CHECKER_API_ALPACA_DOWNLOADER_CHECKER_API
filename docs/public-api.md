# Public API

Import only [alpaca_end_of_day](../alpaca_end_of_day/__init__.py). Both readers are local and read-only;
missing/incomplete/incompatible publications fail without acquisition, repair, discovery or store substitution.

| Reader | Selected data |
|---|---|
| `StandaloneReader(store)` | Resolved histories/evidence within a source group, even if other rows remain unresolved. |
| `DataLibrary(store=...)` | Published online identities; default repository `data_pricing/`. |

`DataLibrary` requires keyword store selection; old positional roots and `path=` raise migration errors.
[Bootstrap](operations.md#serving-catalogue-maintenance) before opening it.

## Common semantics

Tickers are dated labels: resolution preserves multiple matches, and date filters never silently choose an
identity. A 64-hex `public_id` is stable for fixed resolved provider-UUID membership; membership changes can
change it, with no old-ID crosswalk promised.

Dates accept `datetime.date`, ISO date strings or `None`, as inclusive New York session bounds. Valid empty
ranges return empty histories. [ADJUSTMENTS](../libs/alpaca_pricing_download/contract.py) defines the eight names;
`all` means fully adjusted, not every view. Numeric values stay exact.

## Standalone reader

```python
from alpaca_end_of_day import StandaloneReader

reader = StandaloneReader("data_pricing")
for match in reader.resolve_ticker("DELL")["matches"]:
    views = reader.get_histories("DELL", ["raw", "all"], public_id=match["public_id"])
    print(match["public_id"], views["views"]["all"]["bar_count"])
```

Symbol locates the source group and remains required with `public_id`; multiple matches without an ID raise
`AmbiguousTickerError`. [StandaloneReader](../alpaca_end_of_day/standalone.py#L418) defines signatures and dictionary
schemas: `get_history` returns one view with exact numeric strings; `get_histories` returns `views[adjustment]`
(default eight) from one snapshot, all succeeding or failing. `include_evidence=True` includes its evidence.

`resolve_ticker` and `get_generation` need no price reads. `list_published_sources` returns locators that may be
stale/unresolved, not an online catalogue. `get_evidence` exposes published identities, assignments and normalized
evidence. For several IDs, share a [group snapshot](../alpaca_end_of_day/_standalone_group.py):

```python
with reader.open_group("DELL") as group:
    for security in group.summary["securities"]:
        views = group.get_histories(public_id=security["public_id"])
group.check_current()  # Optional freshness check before downstream publication.
```

Separate calls may select different generations. Busy groups raise `ReaderBusyError`, or wait with `wait=True`;
stale/incomplete/incompatible publications raise `StandaloneReaderError`. `cache_bytes` defaults to zero;
`cache_info()` reports bounded RAM retention, and group `set_cache_limit` stays within that cap, not a total-process
memory limit. Caching preserves locks/current selection/pending checks and trusts published digests.

Equal overlaps collapse. Conflicting adjusted bars prefer strictly broader correlated coverage, then a unique
source matching the historical ticker; raw uses only the latter. Unsettled conflicts fail. Assembly precedes
filtering, so narrow queries may read older payloads. Authority: [_history](../alpaca_end_of_day/standalone.py#L239).

Reader CLI defaults to `raw`; `--all-adjustments` selects eight. Ambiguity needs symbol plus `--public-id`.
Compound names are single arguments; `raw,all` is invalid. Multiple-view CSV adds `adjustment`; output is
prepared before printing. [Commands/help](operations.md#read-and-export).

## Online catalogue reader

```python
from alpaca_end_of_day import DataLibrary

data = DataLibrary(store="data_pricing")
for security in data.search_securities("DELL", limit=5):
    history = data.get_history(security.public_id, "all")
    print(security.public_id, len(history.bars))
```

[DataLibrary](../alpaca_end_of_day/__init__.py#L477) defines catalogue/list/get/search/dated-resolution methods,
history signatures and CSV export; [public types](../alpaca_end_of_day/__init__.py#L73) define all fields.
`get_histories` locks once and returns adjustment → typed `History`, default eight views. OHLC/VWAP/volume
are `Decimal`, trade count is integral; `to_dict()` emits exact decimal strings. `wait`/`cache_bytes` match
standalone semantics.

Metadata retains known/conflicting observations and supported effective intervals separately. Undated exact
fallbacks stay undated; catalogue time dates only catalogue-sourced lookups. Ownership evidence can support
historical labels/CUSIPs, including unknown endpoints; observed names/exchanges/classes do not establish
historical periods. Resolution ranges describe labels on stored histories. Undated names remain searchable.
`resolve_ticker` accepts catalogue labels beginning with a letter or digit, followed by letters, digits,
periods or hyphens; it trims surrounding whitespace and ignores case. This includes historical numeric labels.
FIGIs are not currently supplied or fabricated.
Provider IDs, internal status/reasons and diagnostic paths stay hidden. Coverage records approved stored
sessions and explained gaps; [eligibility guarantees](technical-contracts.md#serving-catalogue) define its limits.

Instances retain their opened catalogue snapshot without a long-lived transaction. Search/list/resolve do no
ticker I/O. History checks the selected serving decision and lineage/pricing generations under shared locks.
Changed selected inputs raise `CatalogChangedError`: finish maintenance and open a new instance. Unrelated
updates remain readable; catalogues retain no old pricing files. `PublicIdUnavailableError` does not distinguish
unknown from internally offline IDs. Both errors inherit `DataLibraryError`; malformed arguments raise
`ValueError`, busy groups `ReaderBusyError`.

`write_flat_online_csv` orders date → public ID with separate raw/fully adjusted columns. Mismatched sessions,
duplicates or ambiguous dated metadata fail; undated descriptions yield empty historical cells. Destinations
must be outside the source store. Paths use disk spool + atomic replacement; streams lack replacement guarantees.
Exports declare one catalogue snapshot and match each consumed group's generations, not a store-wide guarantee
that every group remains latest at installation.
