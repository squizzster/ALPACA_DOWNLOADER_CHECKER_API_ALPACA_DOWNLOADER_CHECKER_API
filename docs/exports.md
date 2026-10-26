# Exports

[Public API](public-api.md) owns identity, dates and reader semantics. Export formats have different selections
and guarantees; destinations must be outside the source store.

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
and configured concurrency, and opens no payloads or output files. Library equivalents:
`DataLibrary.plan_csv_export(*, public_ids=None, start=None, end=None, workers=2)` and
`write_csv_export(destination, *, public_ids=None, start=None, end=None, progress=None, workers=2)`.
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

### Publication and concurrency

The final manifest defines all eight views and references identity files relative to its timestamp directory.
It is atomically installed only after every CSV/digest succeeds. Consume the returned manifest and its named
digests directly. An unfinished run has no `final_manifest.json`; exports neither discover nor resume runs by
crawling output directories. A stable destination lock serializes exporters. Completed runs are self-contained
and independently retainable/removable.

Export trusts the opened catalogue and selected lineage/pricing manifests, processes each selected group once,
follows named payloads and shares decoding within the group. No directory discovery, upstream digest calculation
or identity/eligibility replay. Output checksums accumulate while writing CSV bytes.

Groups are consumed under reader locks. A store-wide shared operation lock spans the current catalogue-token
check through final publication; a previously opened `DataLibrary` must still match that token. Store-writing
CLI commands take the exclusive side, excluding update/writer overlap. [Lock ownership](technical-contracts.md#publication-and-recovery).

`workers`: integer 1–4, default 2; each worker processes complete groups from the same accepted catalogue.
Group identities share decoded payloads; outstanding groups are bounded by worker count. Threads exchange only
bounded compact completions, without payload prefetch or full catalogue rereads. The coordinator calls progress
serially on the calling thread and orders manifest entries by original sorted group/identity selection,
independent of completion order. Publication waits for every group and released worker resources. Progress counts
completed CSV/digest pairs; no-row identities advance processed count without files.

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
Mismatched sessions, duplicates or ambiguous dated metadata fail; undated descriptions yield empty historical
cells. Path destinations use disk spool and atomic replacement; streams (`--output -`) lack replacement
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
