# Public API

[`alpaca_end_of_day`](../alpaca_end_of_day/__init__.py) is the only supported Python import for consumer applications.
Everything under `libs/` is implementation or operator-inspection machinery.

`DataLibrary` exposes only the producer-validated online snapshot selected by the current publication receipt.
Ordinary reads trust completed boundary receipts; they do not re-hash every catalogue object or monthly payload. The
absence of an ID, ticker, name, or history does not disclose whether an internal offline record exists.

A source checkout alone is not a readable data library: Git does not contain the runtime `data_vtest/` store.
`DataLibrary` requires a compatible completed catalogue there; it does not probe or fall back to the legacy
`data/end_of_day/` tree. See [Backup and recovery](operations.md#backup-and-recovery).
The standalone pricing/lineage twins are not an additional `DataLibrary` store format. Use `StandaloneReader` for
their resolved pricing; internal handoff readers under `libs/` are not supported application imports.

## Standalone resolved-pricing reader

`StandaloneReader` consumes the existing standalone store directly. It does not run availability, produce an online
catalogue, or change `DataLibrary`. A resolved portion remains readable when other observations are unresolved;
unresolved rows are not assigned an ID by the reader.

```python
from alpaca_end_of_day import StandaloneReader

reader = StandaloneReader("data_pricing")
choices = reader.resolve_ticker("GOOG")  # metadata only; no price payloads
history = reader.get_history("GOOG", "all", start="2026-10-01", end="2026-10-14")
views = reader.get_histories("GOOG", ["raw", "all"], start="2026-10-01", end="2026-10-14")
```

The symbol locates an existing published source and its dependency group. Resolution returns every ID assigned to
that source, plus IDs bearing that historical ticker in the group. A provider source alias need not equal a historical
trading label. Dependency membership is not security membership: only the published row assignments select IDs.
`matches` contains public IDs, actual historical tickers, available adjustments, and first/last observed sessions;
those endpoints do not promise uninterrupted coverage.

If several IDs match, `get_history` raises `AmbiguousTickerError`; pass `public_id=...` from the resolution result.
The symbol is still required: there is no new global public-ID catalogue or universe scan. Date bounds select rows
after identity selection; they do not silently choose an identity for a reused ticker. A source alias is never
invented as a historical trading label.

History results are JSON-compatible dictionaries with `security`, `adjustment`, inclusive `range`, `bar_count`, and
`bars`. Bars contain the New York `session`, original `timestamp`, published historical `tickers`, and all seven
numeric pricing fields as **exact strings**, including volume and trade count. All eight adjustment modes below are
supported; `all` means fully adjusted, not every mode. A valid range without observations returns an empty array.

`get_histories` accepts a nonempty list/tuple of exact adjustment names (all eight by default), removes duplicates in
first-requested order, and reads them under **one snapshot and lock set**. Its result has shared `generation`,
`symbol`, `security` and `range` metadata, with `views[adjustment] = {"bar_count": ..., "bars": [...]}`. Each view keeps
its own observed sessions and established source-selection policy; no cross-view session intersection is imposed.
Any unavailable or conflicting requested view fails the entire request. By default, manifest and decoded payload
reuse is in-memory for that call only: exact-copy aliases do not reopen the same gzip. Views also reuse history
assembly when payloads, row selections, dated labels and overlap policy are identical. Returned views remain
independently mutable; raw and adjusted overlap policies remain distinct.

API v5 adds metadata-only `list_published_sources()` and `get_evidence(symbol)`. The first returns existing source
locators and receipt generations, including entries that may be stale or unresolved; it is not an online catalogue.
The second selects a completed group and returns resolved `securities` with `read_symbol`, `latest_tickers` (ties are
explicit), source generations/requests, compact dated assignments and normalized published evidence. Dependency
membership does not attribute every group action to every security. Current asset lookups are not historical listing
facts, and request `asof` is not the adjustment vintage. `get_histories(..., include_evidence=True)` includes this
projection under the **same** locked snapshot as its bars. Compare returned generations when composing several calls.

API v6 adds optional cross-call RAM reuse: `StandaloneReader(store, cache_bytes=2 * 1024**3)` retains selected JSON
and immutable decoded rows up to that accounted cache budget; the default is zero. `cache_info()` reports the budget,
retained bytes, entries, hits and misses. This budget is not a whole-process memory cap: active histories, returned
results and caller work need additional memory. No disk cache is created. Every call still selects current receipts,
checks pending work and acquires the same locks; changed or missing files cannot silently reuse old cache entries.

`get_generation(symbol)` returns the freshly selected group generation using that same locked freshness check,
without loading assignment boundaries or prices. It is useful for checking an already-consumed snapshot before a
downstream publication; it does not authenticate payloads or pin future calls to that generation.

API v7 adds a group context for batch consumers:

```python
with reader.open_group("NVDA") as group:
    for security in group.summary["securities"]:
        history = group.get_histories(public_id=security["public_id"])
        # Consume this complete eight-view history before requesting the next ID.
group.check_current()  # optional freshness gate before downstream publication
```

The context selects one completed group and holds its shared pricing locks through consumption. `summary` contains
all resolved group IDs, latest labels and selecting source references; it omits normalized action evidence and detailed
assignments. Explicit-ID reads use the same history assembly as `get_histories`, returning all eight full histories
with this compact summary as `evidence`. Working documents are parsed once per handshake; the configured RAM budget
bounds decoded data retained between IDs in this group. Closing releases locks and working data. History reads then
fail; `summary` and `check_current()` remain usable through a small retained token. The latter checks current index
selections and locked pricing receipts without reopening approved lineage documents. Caller mutations to returned
summaries or histories cannot change that token or later reads. Exceptions from consumer code retain their type.

API v8 shares an in-memory read/reuse/eviction ledger across group caches; `cache_info()["ledger"]` reports cumulative
work even after group working data is released. Conservative tuple/Unicode accounting bounds retained decoded rows.
Uncontested observations bypass conflict assembly, while overlapping timestamps still use the existing resolver
with full source coverage. Exact raw/adjusted aliases may share assembly only when there is no source competition.
Final freshness checks reuse an unchanged pricing receipt's locked file identity; changed files are loaded and
compared, and current index selections, pending work and locks are always checked. Values and source policy are unchanged.

API v9 adds `group.set_cache_limit(cache_bytes)` between explicit-ID reads. The limit must be a nonnegative integer
within the reader's configured cap. Shrinking evicts retained entries only as needed; zero disables retained reuse.
This lets a batch consumer reserve RAM for its growing output without changing values or the snapshot handshake.
Closing still releases the group cache, and a closed group cannot change its limit.

For overlapping sources, the reader reuses `_resolve_bar_candidates` in
[`history.py`](../libs/alpaca_end_of_day_reader/history.py): equal observations collapse; differing adjusted bars
prefer strictly broader correlated session coverage, then a unique source matching the historical ticker; differing
raw bars use only that historical-ticker preference. An unsettled price conflict raises an error. No price arithmetic
or new identity rule is applied. Each contributing boundary opens only the selected adjustment's physical gzip,
following approved aliases. The selected history is assembled **before date filtering** so a narrower request cannot
change source preference; narrow requests can therefore read cold pricing too.

Each method selects one completed group through the [snapshot handshake](technical-contracts.md#lock-delegate-publish-acknowledge)
and holds its shared pricing locks. Unrelated lineage work can calculate and publish concurrently. Results include a group `generation`; calls
may select newer publications, unlike the pinned `DataLibrary` instance below. There is no on-disk reader cache,
index, price copy, repair, acknowledgement, or network request. Busy producers raise `ReaderBusyError` by default;
`StandaloneReader(store, wait=True)` waits. Missing, stale, incomplete, or incompatible publications raise
`StandaloneReaderError`. Malformed query arguments raise `ValueError`.

The standalone CLI writes JSON or CSV to stdout:

```bash
./read_alpaca_pricing.py --store data_pricing -s GOOG --resolve
./read_alpaca_pricing.py --store data_pricing -s GOOG --type all --from 2026-10-01 --to 2026-10-14
./read_alpaca_pricing.py --store data_pricing -s GOOG --type raw all --from 2026-01-01
./read_alpaca_pricing.py --store data_pricing -s GOOG --all-types --format csv
./read_alpaca_pricing.py --store data_pricing -s FB --public-id <64-hex-id> --format csv
```

`--type` accepts several space-separated names or repeated flags (`--type raw --type all`). Compound names such as
`split,dividend` remain one view; do not use comma-separated selection such as `raw,all`. `--all-types` selects all
eight and cannot be combined with `--type`. With no selection, the default remains `raw`.

One distinct selected type retains the existing JSON/CSV shape. Multiple types return the `views` JSON structure
above; CSV adds an `adjustment` column and one header, followed by each type's chronological rows in requested order.
The command prepares every requested view before printing, so a late view failure produces no partial stdout.

`--start`/`--end` alias `--from`/`--to`. CSV carries session, public ID, historical tickers (pipe-separated when several
agreeing sources supply labels), timestamp and exact numeric fields. `--resolve` is metadata-only JSON and does not
accept history filters. See [reader operation and exit statuses](operations.md#standalone-reader).

The remaining sections describe the combined-catalogue `DataLibrary` interface.

## Open a snapshot

```python
from alpaca_end_of_day import DataLibrary

data = DataLibrary()
catalog = data.catalog()

print(catalog.generation)
print(data.online_count())
```

`DataLibrary(path=None)` acquires the shared publication lock, rejects an incomplete publication marker, and selects
the published security-master catalogue at construction. `path` is the repository root; it defaults to the parent of
the `alpaca_end_of_day` package. One instance stays bound to that generation. This is the trusted consumption path,
not the explicit boundary-audit path described in
[Technical contracts](technical-contracts.md#trust-operations).

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

Methods are local, synchronous and credential-free, and make no network requests or store mutations. Export writes
the explicitly requested destination and a temporary spool as described below.

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

`public_id` is the opaque, 64-hex identifier for one resolved historical-pricing identity. It is the internal
master ID exposed under a consumer name. Store, compare, and submit it as an identifier; do not derive it from ticker,
name, CUSIP, or another provider identifier. Input is case-insensitive and returned IDs are lowercase.

It is stable for fixed resolved UUID membership, not guaranteed immutable across evidence updates. A membership
extension can change the ID without changing historical row ownership. No persistent-ID crosswalk or old-snapshot
archive is promised. The exact derivation belongs to the [identity model](technical-contracts.md#identity-model).

Every `Security` contains:

| Field | Meaning |
|---|---|
| `public_id` | Exact lookup key within the selected catalogue. |
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

Identity and catalogue calls use the security-master generation opened by one `DataLibrary`. A history read reacquires
the shared publication lock, checks that the current catalogue still selects that generation, and consumes trusted
published correlation, warning, and monthly-source boundaries. It does not repeat their publication-time digest
audits or domain calculations. If publication changed meanwhile, the library raises `CatalogChangedError` rather than
combining generations. Create a new `DataLibrary` to read the newer snapshot.

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
