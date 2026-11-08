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
| `export bloodhound --output PATH` | Resolved standalone histories; one Parquet file. | [command_line.py](../libs/alpaca_bloodhound_export/command_line.py): `export` |

## Per-identity CSV export

```bash
./alpaca-cli-eod export csv --plan
./alpaca-cli-eod export csv --output exports/csv --workers 2
```

Default: all online public IDs, output `exports/csv/`. Repeat `--public-id ID` for selected online identities;
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

Export trusts the opened catalogue and selected lineage/pricing manifests, processes each selected group once,
follows named payloads and shares decoding within the group. No directory discovery, upstream digest calculation
or identity/eligibility replay. Catalogue references select the required receipt bodies directly: export does
not reopen the lineage index, reread store compatibility, recheck group decision tokens, probe pending/update
paths or unavailable inputs, or authenticate upstream generations. Each selected receipt and manifest is read
once and retained for its group's consumption. Each identity's complete CSV is encoded in a memory buffer,
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
Internal producers invoked outside the CLI must also respect this gate when exports can run. The serving selector also owns version-1 export-input applicability. Pricing and lineage publishers record exact intended generation changes before replacing mutable selected files; serving accepts only matching generations. Export reads catalogue token and selected applicability together once, rejecting dirty selected inputs without opening them or authenticating upstream generations. Unselected changed groups do not block a selected export. Older catalogues lacking this guarantee require explicit `catalogue rebuild`, which reuses accepted numerical checkpoints and records completed group bindings for interrupted continuation. The accepted catalogue and applicability handoff remain authority for the operation.

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
./alpaca-cli-eod export bloodhound --output ./alpaca.parquet --symbol DELL
```

Requires [PyArrow](../requirements-bloodhound.txt) and an existing output parent outside the source store.
Default selection: all indexed lineage groups, not the online catalogue. Rejected identities abort by default;
`--include-all-passed` permits output despite rejections, reported on stderr. `--replace` allows replacement only
after successful conversion. Output is one Parquet file; no diagnostic sidecar or Bloodhound registration.

Complete resolved eight-view histories produce fully adjusted OHLC, raw close/volume and a jointly inferred
split factor. [factors.py](../libs/alpaca_bloodhound_export/factors.py) and
[projection.py](../libs/alpaca_bloodhound_export/projection.py) own exact conversion; schema compatibility is not
independent corporate-action certification. Assumptions/full public-ID mappings are in the `alpaca_bloodhound` footer.

Convert full histories before filtering. Ambiguous labels/incompatible factors reject an identity; ID collision,
snapshot change, memory exhaustion or writer failure aborts export. Accepted output stays within its RAM
allowance, serializes once, checks source freshness and installs atomically, without spill.

Installation determines success. Later progress, temporary-cleanup or CLI-reporting failures enter
`diagnostic_errors` without changing committed success; failed temporary cleanup can leave the temporary name.
Pre-installation failures abort publication and preserve an existing destination.
