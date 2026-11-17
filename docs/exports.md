# Exports

[Public API](public-api.md) owns identity, dates and reader semantics. Export formats have different selections
and guarantees; destinations must be outside the source store.

The separate [standalone Bloodhound converter](../bloodhound_app/README.md) consumes completed per-identity
CSV exports or legacy histories. Its README owns its invocation, dependencies and known publication limits.

| Command | Selection/output | Authority |
|---|---|---|
| `read --symbol TICKER --format csv` | One resolved source-group identity; multiple views add `adjustment`. | [standalone_cli.py](../alpaca_end_of_day/standalone_cli.py): `run_arguments` |
| `catalogue export --output PATH` | Flat online CSV ordered date → public ID. | [DataLibrary](../alpaca_end_of_day/__init__.py): `write_flat_online_csv` |
| `export csv` | Online identities; one timestamped run with CSV/digest pairs and final manifest. | [_csv_export.py](../alpaca_end_of_day/_csv_export.py): `plan_export`, `write_export` |
| `export bloodhound --output PATH` | Online identities; one Bloodhound Parquet file and conversion report. | [command_line.py](../libs/alpaca_bloodhound_export/command_line.py): `export` |

## Shared catalogue input

`export csv` and `export bloodhound` consume the same online catalogue through
[_catalogue_export.py](../alpaca_end_of_day/_catalogue_export.py). The opened catalogue owns eligibility,
selected groups, source generations and dated ticker ranges. Under the shared operation gate, its token and
selected export-input applicability are accepted once. Dirty selected inputs fail before history reads;
unrelated changed groups do not block a selected export. Missing capabilities require owning catalogue
maintenance, never an implicit store census.

Each selected group is opened once. Its named receipts and pricing manifests are retained, and decoded
payloads are shared across identities and adjustment aliases. Consumers assemble the accepted online span;
they do not reopen the lineage index, authenticate upstream hashes, repeat compatibility checks or replay
ownership and eligibility decisions. The operation gate remains held through output publication.

`DataLibrary.open_history_export(*, public_ids=None, start=None, end=None)` exposes this same context to
format adapters. `iter_histories()` is single-use and yields public security metadata, exact decimal-string
rows for all eight views, and the completed inclusive ticker ranges. Keep the context open through publication
and treat held values as read-only. `on_error="record"` retains completed peers and reports unreadable selected
identities without retrying their group; initial selector and operation-lock failures still stop the export.

## Per-identity CSV export

```bash
./alpaca-cli-eod export csv --plan
./alpaca-cli-eod export csv --output exports/csv --workers 2
```

Default: all online public IDs, output `exports/csv/`. Identities published offline are excluded; operator
`catalogue status` retains their review markers. [Serving contracts](technical-contracts.md#serving-catalogue)
define that publication boundary. Repeat `--public-id ID` for selected online identities;
`--from`/`--through` are inclusive date filters. `--plan` reads only the opened catalogue, reports selected work
and configured concurrency, and opens no payloads or output files. Automatic memory sizing reads the operating
system's available-memory counter once. Library equivalents:
`DataLibrary.plan_csv_export(*, public_ids=None, start=None, end=None, workers=2, memory_bytes=None)` and
`write_csv_export(destination, *, public_ids=None, start=None, end=None, progress=None, workers=2, memory_bytes=None)`.
The latter returns the export report. [CLI lifecycle](cli-reporting.md#csv-lifecycle) also applies to planning.

Each run creates a UTC timestamp directory with microsecond precision:

```text
exports/csv/
  <UTC timestamp>/
    final_manifest.json
    <public_id>/
      <public_id>_eod.csv
      <public_id>_digest.json
```

### Columns and selection

Each CSV contains `date` and fully populated distinct numeric series. Eight views each define
`open, high, low, close, volume, trade_count, vwap`. Stable prefixes, in canonical order:
`raw, split, dividend, spin_off, split_dividend, split_spin_off, dividend_spin_off, all`.
Prefix + `_` + field forms the logical column, e.g. `split_dividend_volume`.

A logical column equal to an earlier column throughout exported rows maps directly to that column in the digest;
canonical order is view, then field. Equality is exact numeric equality despite decimal spelling; canonical
columns retain their original strings. Sharing can cross fields (`raw_high`/`raw_open`); `split_close` may alias
`raw_close` while `split_volume` remains separate. Headers may differ by identity. Every included column is
populated on every row; unchanged individual-session values are still written.

Identities with no rows in the date window produce no files. Empty selection produces a complete empty manifest.
Each digest records actual columns, all 56 logical mappings, row count, actual first/last sessions and checksum.
`column_aliases` maps logical columns directly to CSV columns; `views[adjustment].columns` gives the same map by field.

CSV schema 2 also carries `security.ticker_ranges`: the opened catalogue's completed observed-label ranges,
each with `ticker`, `first_session` and `last_session` (both endpoints inclusive). These ranges preserve labels
selected across all advertised views, including genuine overlapping labels. They are copied intact from the
accepted catalogue without reconstructing row labels, revisiting ownership evidence or additional source reads.
Date-filtered CSVs retain the complete identity's ranges; consumers project them only onto the supplied rows.
`ticker_ownership_intervals` remains broader evidence with potentially unknown endpoints and is not a complete
observed-label map. Schema-1 exports lack the dated-range handoff and remain unchanged; produce a new completed
export when it is required rather than filling an old digest from a different catalogue generation.

### Publication and concurrency

The final manifest defines all eight views and references identity files relative to its timestamp directory.
It is atomically installed only after every CSV/digest succeeds. Consume the returned manifest and its named
digests directly. An unfinished run has no `final_manifest.json`; exports neither discover nor resume runs by
crawling output directories. A stable destination lock serializes exporters. Completed runs are self-contained
and independently retainable/removable.

The [shared catalogue input](#shared-catalogue-input) owns source consumption. Each identity's complete CSV is encoded in a memory buffer,
with its checksum accumulated during encoding, then passed to the destination stream in one write call.
Completed CSV and digest bytes accumulate in a batch. One coordinator owns source and destination I/O:
source iteration pauses while it writes the batch, then resumes using the same selected group and payload memo.
No source reads run in background workers. CSV/digest bytes are released as their files are written; the final
manifest still publishes only after every batch succeeds. The operating system may split or defer physical I/O;
application phases introduce no `fsync` or forced writeback.

`--memory-mib` (1–5120) overrides the pending-output budget; the library accepts positive `memory_bytes`.
The automatic budget is half Linux `MemAvailable`, with a 1 GiB minimum and 5 GiB maximum, sampled once at
entry. It bounds completed CSV/digest bytes held in the batch, not total process RSS. One active group's decoded
sources, up to `workers` in-flight identity histories/CSV buffers, Python object overhead and the catalogue/run
manifest are additional working memory. A single CSV/digest exceeding the budget aborts without a final manifest;
there is no spill, reread or automatic retry. The 1 GiB floor applies even when available memory is below 1 GiB.
Small explicit budgets are useful for constrained runs; the default can retain the whole output when it fits.

Groups are consumed under reader locks. A store-wide shared operation lock spans the current catalogue-token
check through final publication; a previously opened `DataLibrary` must still match that token. Store-writing
CLI commands take the exclusive side, excluding update/writer overlap. [Lock ownership](technical-contracts.md#publication-and-recovery).
Internal producers invoked outside the CLI must also respect this gate when exports can run.

`workers`: integer 1–4, default 2; workers only encode in-memory identities. The coordinator opens one group
at a time and bounds outstanding encoding jobs by worker count. Group identities share decoded payloads even
when a batch flush splits a group. Encoding completion order does not change output or manifest order.
Progress callbacks run on the coordinator and distinguish prepared identities/buffered bytes from completed
CSV/digest pairs. Read/build phases report no time estimate; the final write phase estimates from its own write
rate. Failure/interruption cancels queued encoding, waits for active jobs and releases their buffers before
returning. Publication waits for every group and all worker resources. No-row identities advance processed count
without files.

Source/destination/store locks fail immediately with `ReaderBusyError`, even if `DataLibrary(wait=True)`;
CLI has no CSV `--wait` and exits 3 when busy. Other reads retain configured waiting. Failure/interruption stops
dispatch and closes active work before reaching the caller. Cancellation occurs between group/identity
operations; an OS read/write already running must return. Completed runs remain intact; incomplete directories
remain unselected. Kernel process exit releases locks.

## Flat online CSV

```bash
./alpaca-cli-eod catalogue export --output ./online.csv
```

`DataLibrary.write_flat_online_csv` orders date → public ID, with separate raw/fully adjusted columns.
The generation-bound online reader supplies unique, ordered, matching raw/fully adjusted session grids:
history assembly owns uniqueness and the selected serving decision owns cross-view compatibility. Flat export
consumes those rows without rebuilding session maps, resorting each history or repeating those checks.
It still rejects ambiguous dated metadata and duplicate date/public-ID output keys; undated descriptions yield
empty historical cells. Path destinations use disk spool and atomic replacement; streams (`--output -`) lack replacement
guarantees. One opened catalogue snapshot and matching consumed-group generations are declared, without a
store-wide promise that every group remains latest at installation.

## Parquet conversion

```bash
python3.11 -m pip install -r requirements-bloodhound.txt
./alpaca-cli-eod export bloodhound --output exports/bloodhound.parquet
```

Default selection is all online identities, using the [same catalogue input](#shared-catalogue-input) as CSV.
No intermediate CSV is created, discovered or parsed. `--public-id` selects online identities; `--symbol`
selects every online identity with that dated label. Date filters apply after full accepted online-history
factor inference. Destinations must be outside the source store. `--replace` permits replacing a prior export;
rejected histories abort by default. Explicit `--include-all-passed` publishes a partial result and reports
its omissions.

The existing [Bloodhound converter](../bloodhound_app/README.md) owns the unchanged 18-column schema,
float64 numerical rules, inferred split scales, metadata handling, stable SHA-256-derived series IDs,
precision checks and serialized-output validation. Fully adjusted OHLC is copied exactly into float64;
raw close and raw fractional volume remain separate. Dated ticker labels come from the completed catalogue
ranges. Output is sorted by stable series ID and session. Existing completed-CSV and legacy-file conversion
remain supported inputs to the standalone app.

Completed Arrow tables accumulate within `--memory-mib` (256–8192 MiB, default 8192). The retained-output limit
reserves one quarter, capped at 2 GiB, for working memory; active histories, catalogue and Python overhead are
additional. Sources are read once, then accepted tables serialize in one Parquet write. Memory exhaustion is
fatal even with explicit omission enabled: no spill, history reread or automatic retry occurs.

A JSON conversion report accompanies `FILE.parquet` as `FILE.parquet.report.json`. It records the accepted
catalogue generation, factor evidence, per-identity results, exact schema/equation validation and omissions.
The [standalone publication limits](../bloodhound_app/README.md#8-known-limitations-and-scope) apply to this
converter's Parquet/report pair. A failed conversion preserves an existing pair; an interrupted two-file
publication needs inspection before consumption. The shared source gate does not supply a destination
journal or change those existing limits. No provider requests, pricing writes or Bloodhound registration occur.
