# Public API

[`alpaca_end_of_day.py`](../alpaca_end_of_day.py) is the only supported Python import for consumer applications.
Everything under `libs/` is implementation or operator-inspection machinery.

The public library exposes only the authenticated online snapshot. The absence of an ID, ticker, name, or history does
not disclose whether an internal offline record exists.

A source checkout alone is not a readable data library: Git does not contain runtime `data/`. `DataLibrary` requires a
compatible authenticated catalogue under `data/end_of_day/json_manifests/`; restore or publish that runtime tree first.
See [Backup and recovery](operations.md#backup-and-recovery).

## Open a snapshot

```python
from alpaca_end_of_day import DataLibrary

data = DataLibrary()
catalog = data.catalog()

print(catalog.generation)
print(data.online_count())
```

`DataLibrary(path=None)` authenticates the security-master catalogue at construction. `path` is the repository root;
it defaults to the directory containing `alpaca_end_of_day.py`. One instance stays bound to that generation.

| Method | Result |
|---|---|
| `catalog()` | `Catalog` with generation digest, aware UTC publication time, online count, and supported adjustments; `to_dict()` also identifies the API version. |
| `catalog_generation()` | The opened generation digest. |
| `online_count()` | Number of identities in the public online snapshot. |
| `list_securities()` | Immutable tuple of every online `Security`, ordered by `public_id`. |
| `search_securities(query, limit=None)` | Ranked online identities matching a ticker, name, CUSIP, FIGI-family value, or public ID. |
| `resolve_ticker(ticker, on=None)` | Every exact ticker/public-ID observed range, optionally containing one date. |
| `get_security(public_id)` | Public identity and coverage metadata for exactly one online ID. |
| `get_history(public_id, adjustment="raw", start=None, end=None)` | Exact daily bars for all time or an inclusive date range. |
| `write_flat_online_csv(destination, start=None, end=None)` | Date-ordered raw and fully adjusted rows for the online universe. |

Methods are local, synchronous, read-only, credential-free, and make no network requests.

## Search and resolve

Search is case-insensitive, collapses repeated whitespace, and performs substring matching across public fields:

```python
matches = data.search_securities("Orning")
for security in matches:
    print(security.public_id, security.names)
```

The result is a tuple of `Security` objects. Exact matches rank before prefixes, prefixes before other substrings; field
priority is ticker, name, CUSIP, external identifier, then public ID. `limit` must be a positive integer when supplied.
Search never includes an offline identity.

Ticker resolution serves a different purpose. It uses an exact, case-insensitive ticker and retains dates because one
ticker may identify different securities over time:

```python
all_uses = data.resolve_ticker("META")
uses_on_date = data.resolve_ticker("META", on="2025-06-30")
```

The result is always a tuple. Each `TickerResolution` contains the ticker, public ID, names, inclusive first and last
observed sessions, and adjustments available for that range. The library does not silently choose between overlapping
matches; callers decide what an empty, single, or multiple result means for their application.

## Public identity

`public_id` is the stable, opaque, 64-hex identifier for one resolved historical-pricing identity. It is the internal
master ID exposed under a consumer name. Store, compare, and submit it as an identifier; do not derive it from ticker,
name, CUSIP, or another provider identifier. Input is case-insensitive and returned IDs are lowercase.

Every `Security` contains:

| Field | Meaning |
|---|---|
| `public_id` | Stable exact lookup key. |
| `tickers` | All authenticated ticker labels attached to the identity. No primary ticker is invented. |
| `names` | Authenticated security names. |
| `cusips` | Authenticated CUSIPs when present. Missing CUSIP data is represented by an empty tuple. |
| `external_identifiers` | Typed FIGI-family identifiers when present. |
| `exchanges`, `asset_classes` | Authenticated descriptive identity fields. |
| `first_session`, `last_session` | Available market-session coverage. |
| `adjustments` | Histories advertised for this ID. |

The object deliberately omits provider asset IDs, provider status, source paths, manifests, lineage evidence, warnings,
validation state, and offline status. Existence at this boundary already means online.

## Read history

```python
history = data.get_history(
    public_id,
    "split",
    start="2025-01-01",
    end="2025-12-31",
)

for bar in history.bars:
    print(bar.session, bar.close, bar.volume)
```

Supported adjustments are:

```text
raw
split
dividend
spin-off
split,dividend
split,spin-off
dividend,spin-off
all
```

`all` is Alpaca's fully adjusted view, not a wildcard. `start` and `end` accept `datetime.date`, `YYYY-MM-DD`, or
`None`; both bounds are inclusive US market-session dates. A valid range with no observations returns an empty `bars`
tuple.

Each `DailyBar` contains `session`, OHLC, `volume`, `trade_count`, and `vwap`. Prices and VWAP are `Decimal`; volume
and trade count are integers. `to_dict()` methods produce JSON-compatible ISO dates and decimal strings without binary
floating-point conversion.

## Flat online CSV

Use the library when an application needs a single rectangular extract:

```python
receipt = data.write_flat_online_csv(
    "/path/to/online.csv",
    start="2025-01-01",
    end="2025-12-31",
)
print(receipt.row_count)
```

The output order is global and deterministic:

```text
date ascending, then unique_id ascending within each date
```

`unique_id` is the public ID. Every row carries the dated `ticker`, `name`, `cusip`, `exchange`, and `asset_class`,
followed by separately labelled raw and fully adjusted values:

```text
date, unique_id, ticker, name, cusip, exchange, asset_class,
raw_open, raw_high, raw_low, raw_close, raw_volume, raw_trade_count, raw_vwap,
all_adjusted_open, all_adjusted_high, all_adjusted_low, all_adjusted_close,
all_adjusted_volume, all_adjusted_trade_count, all_adjusted_vwap
```

Raw and adjusted volume and trade count remain separate because adjustments can change volume while retaining trade
count. Export fails closed if an online identity lacks either `raw` or `all`, if those views have different session
sets, if a session appears twice, or if dated identity metadata is ambiguous.

The exporter does not retain the universe's rows in memory. It accumulates approximately 100,000 rows at a time,
sorts each block, and writes block segments to a temporary binary spool on disk. It then streams one final file by date
and public ID. For a path destination, the spool and final temporary file live beside the destination and the completed
file is installed with `os.replace`; interrupted exports do not replace an existing destination. A supplied text
stream receives the completed spool directly and has no filesystem atomicity guarantee.

`FlatCsvExport` reports the generation, online and exported security counts, row count, inclusive bounds, and columns.
The receipt is returned to Python; the CLI does not append it to the CSV.

## Command-line client

Read one history as JSON:

```bash
./read_alpaca_end_of_day_ticker.py \
  --public-id <64-hex-public-id> \
  --type raw \
  --start 2025-01-01 \
  --end 2025-12-31
```

`--master-id` remains an alias for `--public-id`. Export to a path or use `-`/an omitted optional path for stdout:

```bash
./read_alpaca_end_of_day_ticker.py \
  --dump-flat-online-csv /path/to/online.csv \
  --start 2025-01-01 \
  --end 2025-12-31

./read_alpaca_end_of_day_ticker.py --dump-flat-online-csv -
```

`--type` applies only to a single-ID history. The CSV always contains `raw` and `all`. Argument errors use status 2;
authentication, contract, and I/O errors use status 1.

## Snapshot consistency

Identity and catalogue calls use the generation opened by one `DataLibrary`. A history read authenticates its detailed
dependencies and compares their master generation with the opened snapshot. If publication changed meanwhile, the
library raises `CatalogChangedError` rather than combining generations. Create a new `DataLibrary` to read the newer
snapshot.

## Errors

| Error | Meaning |
|---|---|
| `ValueError` | A query, limit, public ID, adjustment, or date argument is malformed. |
| `PublicIdUnavailableError` | A well-formed ID does not resolve in the online snapshot. The response reveals no internal state. |
| `CatalogChangedError` | Publication changed after the library snapshot opened. |
| `DataLibraryError` | Authentication or the public data contract failed. The two public errors above inherit from it. |

The exact exported names are listed in `alpaca_end_of_day.__all__`; public API version history is tracked in
[`alpaca_end_of_day_api_change_log.json`](../resources/alpaca_end_of_day_api_change_log.json) and summarized in
[`VERSION_TRACKER.md`](VERSION_TRACKER.md).
