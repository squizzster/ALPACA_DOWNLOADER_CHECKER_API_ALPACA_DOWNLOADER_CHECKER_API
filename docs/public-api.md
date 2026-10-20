# Public API

Import only [alpaca_end_of_day](../alpaca_end_of_day/__init__.py). Both readers: local, read-only;
no acquisition, repair or whole-store authentication. Neither substitutes another store format.

| Reader | Input | Scope |
|---|---|---|
| `StandaloneReader(store)` | Standalone pricing/lineage | Resolved histories within a source group. |
| `DataLibrary(path=None)` | Repository root containing `data_vtest/`; default package parent | Published online identities only. |

## Common semantics

Tickers may match multiple dated securities; resolution preserves every match. Date filters never silently
choose identity. Opaque 64-hex `public_id` is stable for fixed resolved provider-UUID membership; changed
membership may change IDs. No old-ID crosswalk promised.

Dates: `datetime.date`, ISO date string or `None`; inclusive New York session bounds. Empty valid ranges
return empty histories. Exact adjustments: `raw`, `split`, `dividend`, `spin-off`, `split,dividend`,
`split,spin-off`, `dividend,spin-off`, `all`. `all` means fully adjusted, not every view.

## Standalone reader

```python
from alpaca_end_of_day import StandaloneReader

reader = StandaloneReader("data_pricing")
for match in reader.resolve_ticker("DELL")["matches"]:
    views = reader.get_histories("DELL", ["raw", "all"], public_id=match["public_id"])
    print(match["public_id"], views["views"]["all"]["bar_count"])
```

Symbol locates the source group; required even with `public_id`. Multiple matches without explicit ID:
`AmbiguousTickerError`. Resolved portions remain readable despite other unresolved rows.

`get_history(symbol, adjustment="raw", ...)`: dictionary with `generation`, `security`, `range`, `bar_count`,
`bars`. Bars: session, timestamp, dated tickers, seven numeric fields as exact strings.
`get_histories`: shared metadata + `views[adjustment]`; default eight views, one snapshot, all succeed or fail.
`include_evidence=True` includes that snapshot's evidence.

| Method | Result |
|---|---|
| `resolve_ticker(symbol)` | Metadata-only matches/observed coverage. |
| `list_published_sources()` | Locators, possibly stale/unresolved; not an online catalogue. |
| `get_evidence(symbol)` | Published group identities, assignments, normalized evidence. |
| `get_generation(symbol)` | Fresh group generation; no price reads. |
| `open_group(symbol)` | One snapshot across several IDs. |

```python
with reader.open_group("DELL") as group:
    for security in group.summary["securities"]:
        views = group.get_histories(public_id=security["public_id"])
group.check_current()  # Optional freshness check before downstream publication.
```

Calls select current receipts under shared group locks; separate calls may select different generations.
Conflicting work: `ReaderBusyError`, or wait with `wait=True`. Incomplete/stale/incompatible publication:
`StandaloneReaderError`.

`cache_bytes=N` enables bounded cross-call RAM reuse; default zero. `cache_info()` reports usage;
group `set_cache_limit(N)` reduces retention within that cap, not total process memory.
Caching preserves current receipt selection, locks and pending checks. Published digests select content;
neither first nor cached reads revalidate file metadata.

Equal overlaps collapse. Conflicting adjusted bars prefer strictly broader correlated coverage, then a
unique source matching the historical ticker; raw bars use only the latter. Unsettled conflicts fail.
Assembly precedes filtering: narrow queries may read older payloads.

```bash
./alpaca-eod resolve --store data_pricing --symbol DELL
./alpaca-eod read --store data_pricing --symbol DELL --adjustment raw all --format csv
```

CLI: default `raw`; `--all-adjustments` selects eight views; ambiguity requires `--public-id` with symbol.
Compound names are single arguments; `raw,all` is invalid selection syntax. Multiple-view CSV adds
`adjustment`; output is prepared before printing.

## Online catalogue reader

```python
from alpaca_end_of_day import DataLibrary

data = DataLibrary()
for security in data.search_securities("DELL", limit=5):
    history = data.get_history(security.public_id, "all")
    print(security.public_id, len(history.bars))
```

`catalog()`, `catalog_generation()`, `online_count()`: selected snapshot.
`list_securities()`: online securities; `get_security(public_id)`: one security.
`search_securities(query, limit=None)`: public ticker/name/identifier search.
`resolve_ticker(ticker, on=None)`: every exact dated match, optionally covering one date.

Metadata: public ID, tickers, names, CUSIPs, external identifiers, exchanges, asset classes, coverage,
adjustments. Provider IDs, internal status and diagnostic paths stay hidden.
`get_history(public_id, adjustment="raw", start=None, end=None)`: typed bars; OHLC/VWAP `Decimal`,
volume/trade count integers. `to_dict()` preserves exact values as JSON.

Instances retain their opened generation. Publication change → history read raises `CatalogChangedError`;
open a new instance. `PublicIdUnavailableError` never distinguishes unknown from internally offline IDs.
Both inherit `DataLibraryError`; malformed arguments raise `ValueError`.

`write_flat_online_csv(destination, start=None, end=None)`: dated identity fields, separate raw/fully
adjusted columns, ordered date → public ID. Mismatched sessions, duplicate rows or ambiguous dated metadata
fail. Paths use disk spool + atomic replacement; streams lack that replacement guarantee.

```bash
./alpaca-eod catalogue export --output ./online.csv
```

Invocation/exit statuses: command `--help`, [Operations](operations.md#read-and-export).
