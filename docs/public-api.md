# Public API

Import only [alpaca_end_of_day](../alpaca_end_of_day/__init__.py). Readers are local/read-only;
missing, incomplete, pending or incompatible publications fail without acquisition, repair, discovery or store substitution.

| Reader | Scope |
|---|---|
| `StandaloneReader(store)` | Resolved group histories/evidence, even with other unresolved rows. |
| `DataLibrary(store=...)` | Published online identities; default repository `data_pricing/`. |

`DataLibrary` requires keyword store selection; positional roots and `path=` raise migration errors.
[Bootstrap serving](operations.md#serving-catalogue-maintenance) before opening it.

## Common semantics

Tickers are dated labels. Resolution preserves multiple matches; date filters never silently select an identity.
A 64-hex `public_id` is stable for fixed resolved provider-UUID membership. Membership changes can change the ID;
no old-ID crosswalk is promised.

Discover candidates through ticker resolution/search, then retain `public_id` for finalized-data history,
exports, joins and cache keys. Do not persist ticker identity keys or re-resolve on every read: one label may
name several securities over time; one identity may retain several dated labels. If accepted UUID membership
changes, the old ID becomes unavailable; select explicitly from the new catalogue.

Dates: `datetime.date`, ISO date string or `None`; inclusive New York session bounds. Valid empty ranges return
empty histories. [ADJUSTMENTS](../libs/alpaca_pricing_download/contract.py) defines eight names; `all` means fully
adjusted, not every view. Numbers remain exact.

## Standalone reader

```python
from alpaca_end_of_day import StandaloneReader

reader = StandaloneReader("data_pricing")
for match in reader.resolve_ticker("DELL")["matches"]:
    views = reader.get_histories("DELL", ["raw", "all"], public_id=match["public_id"])
    print(match["public_id"], views["views"]["all"]["bar_count"])
```

Symbol locates the source group and remains required with `public_id`. Multiple matches without an ID raise
`AmbiguousTickerError`. [StandaloneReader](../alpaca_end_of_day/standalone.py) owns signatures/dictionary schemas:
`get_history` returns one view with exact numeric strings; `get_histories` returns `views[adjustment]` (default
all eight) from one snapshot, all succeeding or failing. `include_evidence=True` includes evidence.

`resolve_ticker`/`get_generation` need no price reads. `list_published_sources` returns possibly stale/unresolved
locators, not an online catalogue. `get_evidence` exposes published identities, assignments and normalized
evidence. Share one [GroupSnapshot](../alpaca_end_of_day/_standalone_group.py) across IDs:

```python
with reader.open_group("DELL") as group:
    for security in group.summary["securities"]:
        views = group.get_histories(public_id=security["public_id"])
group.check_current()  # Optional freshness check before downstream publication.
```

Summary, history and evidence are independent nested projections: mutation cannot change later group reads or
captured metadata. Separate calls may select different generations. Busy groups raise `ReaderBusyError`, or wait
with `wait=True`; stale/incomplete/incompatible publications raise `StandaloneReaderError`.

`cache_bytes` defaults to zero. `cache_info()` reports bounded RAM retention; group `set_cache_limit` stays
within that cap, not a total-process limit. Caching retains locks, current-selection/pending checks and trust in
published digests. Readers consume [producer-selected observations](lineage.md#observation-selection), preserving
accepted dated labels; unsettled source authority fails explicitly. Assembly precedes filtering, so narrow
queries may read older payloads. Authority: [standalone.py](../alpaca_end_of_day/standalone.py), `_history`.

Reader CLI defaults to `raw`; `--all-adjustments` selects eight. Ambiguity needs symbol plus `--public-id`.
Compound adjustment names are single arguments; `raw,all` is invalid. Multiple-view CSV adds `adjustment`;
output is prepared before printing. [Commands](operations.md#read-and-export).

## Published lineage metadata

```python
from alpaca_end_of_day import StandaloneReader

reader = StandaloneReader("data_pricing")
with reader.open_published_evidence() as selected:
    roster = selected.roster  # Index-only plan: sources, generations and group count.
    for group in selected.iter_groups():
        print(group["securities"], group["sources"])
```

This metadata-only boundary consumes completed lineage publications, including
historical resolved identities. It selects membership and recorded generations
from the lineage index once, after its own schema/completeness checks. Missing
membership/projection coverage fails into explicit `lineage migrate-index`;
there is no receipt census or fallback reconstruction.

One shared lineage publication gate keeps the accepted view stable throughout
the context; lineage publication waits until it closes. Each selected receipt and
assignment document is read once when its body is consumed. The iterator retains
one group's bodies, not a universe of manifests. It does not consult pricing
manifests, locks, notices or payloads, rehash upstream data, replay rules or perform
a later freshness pass. The context releases its gate on failure/interruption.

`roster` is an independent projection. A closed snapshot cannot be consumed.
Group `generation` uses its first source's recorded receipt generation as a
reference; the roster's generation describes the recorded index revision. Prices
may have advanced since lineage publication: this API returns the accepted
lineage evidence, not a claim of current pricing or online availability. Group
actions remain unassigned evidence; observation dates keep their original meaning.
[_published_evidence.py](../alpaca_end_of_day/_published_evidence.py) owns this path.

## Online catalogue reader

```python
from alpaca_end_of_day import DataLibrary

data = DataLibrary(store="data_pricing")
for security in data.search_securities("DELL", limit=5):
    history = data.get_history(security.public_id, "all")
    print(security.public_id, len(history.bars))
```

[DataLibrary and public types](../alpaca_end_of_day/__init__.py) own catalogue/list/get/search/dated-resolution,
history and export contracts. `get_histories` locks once, returning adjustment → typed `History`, default eight
views. OHLC/VWAP/volume are `Decimal`, trade count integral; `to_dict()` emits exact decimal strings.
`wait`/`cache_bytes` match standalone semantics.

Metadata separates known/conflicting observations from supported effective intervals. Undated exact fallbacks
stay undated; catalogue time dates only catalogue-sourced lookups. Ownership can support historical labels/CUSIPs
with unknown endpoints; observed names/exchanges/classes do not establish historical periods. Resolution ranges
describe labels on stored histories; undated names remain searchable. `resolve_ticker` trims/ignores case and
accepts a leading letter or digit followed by letters, digits, periods or hyphens, including historical numeric
labels. FIGIs are not supplied/fabricated. Provider IDs, internal status/reasons and diagnostic paths stay hidden.
[Eligibility](technical-contracts.md#serving-catalogue) bounds approved-session/explained-gap coverage.

Instances retain an opened catalogue snapshot without a long-lived transaction. Search/list/resolve do no ticker
I/O. History checks the selected serving decision and lineage/pricing generations under shared locks. Changed
selected inputs raise `CatalogChangedError`: finish maintenance, then reopen. Unrelated updates remain readable;
catalogues retain no old pricing files. `PublicIdUnavailableError` conceals whether an ID is unknown or internally
offline. Both inherit `DataLibraryError`; malformed arguments raise `ValueError`, busy groups `ReaderBusyError`.

[Exports](exports.md) owns flat/per-identity CSV contracts, snapshot scope and output publication.
