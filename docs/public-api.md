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
`AmbiguousTickerError`. [StandaloneReader](../alpaca_end_of_day/standalone.py#L445) defines signatures and dictionary
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

Readers consume the producer's numerical source selection and preserve every accepted dated label. Unsettled
source authority fails explicitly. The [selection contract](technical-contracts.md#lineage-reuse) defines the
producer's policy and correction scope. Assembly precedes filtering, so narrow queries may read older payloads.
Authority: [_history](../alpaca_end_of_day/standalone.py#L239).

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

[DataLibrary](../alpaca_end_of_day/__init__.py#L484) defines catalogue/list/get/search/dated-resolution methods,
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

## Per-identity CSV export

`DataLibrary.plan_csv_export(*, public_ids=None, start=None, end=None, workers=2)` reports the selected work from
the opened catalogue. `write_csv_export(destination, *, public_ids=None, start=None, end=None,
progress=None, workers=2)` writes the export and returns its report. Both select every online public ID by default;
an explicit list selects only those online identities. Date bounds are inclusive. The destination must
be outside the source store. [CLI usage](operations.md#read-and-export) defaults to `exports/csv/`.

Each run creates a UTC timestamp directory with microsecond precision:

```text
exports/csv/
  <UTC timestamp>/
    final_manifest.json
    <public_id>/
      <public_id>_eod.csv
      <public_id>_digest.json
```

Each CSV has one `date` column and fully populated pricing columns for its distinct numeric series.
Each of the eight views defines `open`, `high`, `low`, `close`, `volume`, `trade_count`, `vwap`. Column names use
the stable view prefixes `raw`, `split`, `dividend`, `spin_off`, `split_dividend`, `split_spin_off`,
`dividend_spin_off`, `all`, followed by `_` and the field name, for example `raw_close` or
`split_dividend_volume`. Identities may have different headers. Every logical column that matches an
earlier column throughout the exported rows maps to that column in the digest. Canonical columns follow
the listed view order, then field order. For example, `split_close` may map to `raw_close` while
`split_volume` has its own column. Every included column is populated on every row. An unchanged value
on an individual session is still written; rows are full session records.
Equality is exact numeric equality, including differently spelled decimals; the canonical column retains
its original numeric strings. Equal series may also share across fields, such as `raw_high` and `raw_open`.
Identities without rows in the requested date range are omitted; an empty selection produces a complete
manifest with no identity files.

Each identity digest defines its CSV columns, all 56 logical column mappings, row count, actual first/last
session and checksum. The final manifest defines all eight possible views and selects the completed
run with identity file references relative to its own timestamp directory. It is published atomically
inside that directory only after every CSV and digest has been written. Consumers follow the manifest
returned by `write_csv_export` and its referenced digests directly.
The digest's `column_aliases` maps every logical price column directly to its actual CSV column;
`views[adjustment].columns` provides the same map by field. An unfinished run directory has no
`final_manifest.json`; export never discovers or resumes runs by crawling the output directory. A stable
destination lock serializes exporters. Completed runs are self-contained and may be retained or removed
independently.

Export trusts the opened serving catalogue and selected lineage/pricing manifests. It processes each
selected identity group once, follows named payloads, and shares decoded payloads within that group.
It does not discover directories, recalculate upstream digests or replay identity/eligibility decisions.
Output checksums are calculated as CSV bytes are written. The export declares the opened catalogue
snapshot and consumes compatible groups under their reader locks. A store-wide shared operation lock spans
the catalogue-generation check through final-manifest publication. Store writers take the exclusive side,
so neither an update nor another writer can overlap an export. A `DataLibrary` opened before the export must
still match the current compact catalogue generation; this checks only the selector token.

`workers` accepts integers from 1 through 4, defaulting to 2. Parallel workers process complete groups from the same
accepted catalogue. Each group's identities stay together with their decoded payload reuse; outstanding groups
are bounded by the worker count and only bounded, compact completion messages cross threads. No payload prefetch or
full catalogue reread occurs. The coordinator invokes progress callbacks serially on the calling thread
and orders manifest entries by the original sorted group/identity selection, independently of completion order.
It publishes only after every group succeeds and all workers release their resources. Progress counts completed
CSV/digest pairs; selected identities with no rows advance the processed count without creating files.

CSV source and destination locks always fail immediately with `ReaderBusyError` when busy, including when
the library was opened with `wait=True`. Other library reads retain their configured wait behavior. On failure
or interruption, dispatch stops and workers close their active work before the error reaches the caller.
Cancellation takes effect between group/identity operations; an operating-system read or write already in
progress must return. Previous completed exports remain intact and incomplete directories remain unselected.
