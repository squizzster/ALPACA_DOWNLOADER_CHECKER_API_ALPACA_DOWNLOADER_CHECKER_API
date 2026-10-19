# Public API

Import only [alpaca_end_of_day](../alpaca_end_of_day/__init__.py).
Both readers are local and read-only; they do not acquire, repair or authenticate the whole store.

| Reader | Input | Scope |
|---|---|---|
| `StandaloneReader(store)` | Standalone pricing/lineage | Resolved histories within a source group. |
| `DataLibrary(path=None)` | Combined catalogue under `data_vtest/` | Published online identities only. |

For `DataLibrary`, `path` is the repository root, not the store directory.
It defaults to the package's parent. Neither reader substitutes another store format.

## Common semantics

A ticker can identify multiple securities over time. Resolution returns every match; date filters do
not silently choose an identity. `public_id` is an opaque 64-hex identifier, stable for fixed resolved
provider-UUID membership. Membership changes can change it; no old-ID crosswalk is promised.

Dates accept `datetime.date`, ISO date strings or `None`. History bounds are inclusive New York
session dates. Empty valid ranges return empty histories.

The eight exact adjustment names are `raw`, `split`, `dividend`, `spin-off`, `split,dividend`,
`split,spin-off`, `dividend,spin-off` and `all`. Here `all` means fully adjusted, not every view.

## Standalone reader

```python
from alpaca_end_of_day import StandaloneReader

reader = StandaloneReader("data_pricing")
for match in reader.resolve_ticker("DELL")["matches"]:
    views = reader.get_histories("DELL", ["raw", "all"], public_id=match["public_id"])
    print(match["public_id"], views["views"]["all"]["bar_count"])
```

A symbol locates a published source group. It is still required with `public_id`.
Without an explicit ID, multiple matches raise `AmbiguousTickerError`.
Resolved portions remain readable even if other rows are unresolved.

`get_history(symbol, adjustment="raw", ...)` returns a dictionary containing `generation`, `security`,
`range`, `bar_count` and `bars`. Bars include session, timestamp, dated tickers and seven numeric
fields as **exact strings**. `get_histories` returns shared metadata plus `views[adjustment]`;
all requested views share one snapshot and either all succeed or the call fails.
Its default selects all eight views; `include_evidence=True` adds the same snapshot's evidence.

| Method | Purpose |
|---|---|
| `resolve_ticker(symbol)` | Metadata-only matches and observed coverage. |
| `list_published_sources()` | Source locators, possibly stale/unresolved; not an online catalogue. |
| `get_evidence(symbol)` | Published group identities, assignments and normalized evidence. |
| `get_generation(symbol)` | Freshly selected group generation without price reads. |
| `open_group(symbol)` | Hold one group snapshot while consuming several IDs. |

```python
with reader.open_group("DELL") as group:
    for security in group.summary["securities"]:
        views = group.get_histories(public_id=security["public_id"])
group.check_current()  # Optional freshness check before downstream publication.
```

Calls select current receipts under shared group locks. Conflicting work raises `ReaderBusyError`;
`wait=True` waits. Incomplete, stale or incompatible publication raises `StandaloneReaderError`.
Separate calls can select different generations.

`cache_bytes=N` enables bounded cross-call RAM reuse; default zero. `cache_info()` reports usage.
A group can reduce retention with `set_cache_limit(N)` within that cap. This is not a process memory
limit. Caching never bypasses current receipt selection, locks or pending-work checks.
The published digest selects cached content directly; first and cached reads do not revalidate file metadata.

Equal overlapping observations collapse. Conflicting adjusted bars prefer strictly broader correlated
coverage, then a unique source matching the historical ticker; raw bars use only the latter preference.
Unsettled conflicts fail. Assembly precedes date filtering, so narrow queries may read older payloads.

CLI:

```bash
./read_alpaca_pricing.py --store data_pricing -s DELL --resolve
./read_alpaca_pricing.py --store data_pricing -s DELL --type raw all --format csv
```

Use `--public-id` with the symbol when ambiguous. `--all-types` selects eight views.
Compound names remain one argument; `raw,all` is not a selection syntax.
Default is `raw`; multiple-view CSV adds an adjustment column. Output is prepared before printing.

## Online catalogue reader

```python
from alpaca_end_of_day import DataLibrary

data = DataLibrary()
for security in data.search_securities("DELL", limit=5):
    history = data.get_history(security.public_id, "all")
    print(security.public_id, len(history.bars))
```

`catalog()`, `catalog_generation()` and `online_count()` describe the selected snapshot.
`list_securities()` returns online securities; `get_security(public_id)` selects one.
`search_securities(query, limit=None)` searches public ticker/name/identifier fields.
`resolve_ticker(ticker, on=None)` returns every exact dated match, optionally covering one date.

Security metadata includes public ID, tickers, names, CUSIPs, external identifiers, exchanges,
asset classes, coverage and adjustments. Provider IDs, internal status and diagnostic paths are hidden.
`get_history(public_id, adjustment="raw", start=None, end=None)` returns typed bars:
OHLC/VWAP are `Decimal`; volume/trade count are integers. `to_dict()` preserves exact values as JSON.

One instance stays on its opened generation. If publication changes, a history read raises
`CatalogChangedError`; open a new instance. `PublicIdUnavailableError` reveals no distinction between
unknown and internally offline IDs. Both inherit `DataLibraryError`; malformed arguments raise `ValueError`.

`write_flat_online_csv(destination, start=None, end=None)` exports dated identity fields and separate
raw/fully adjusted columns, ordered by date then public ID. Mismatched sessions, duplicate rows or
ambiguous dated metadata fail. Path output uses a disk spool and atomic replacement; streams do not
have that replacement guarantee.

```bash
./read_alpaca_end_of_day_ticker.py --dump-flat-online-csv ./online.csv
```

See each CLI's `--help` and [operations](operations.md#read-and-export) for invocation and exit statuses.
