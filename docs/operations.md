# Operations

Run from the repository root. Use each command's `--help` as the option authority.

## Requirements

- Python 3.11-compatible runtime.
- `pigz` when publishing or replacing a frozen download.
- `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY` for update, lineage, or correlation commands.
- No credentials for the reader.

## Update

Run every stage for one or more tickers:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py --symbol FUN -s DELL -s FB
```

For a larger batch, provide one ticker per nonblank line. Blank lines are ignored, and duplicate tickers are processed
once in first-seen order:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py --file path/to/symbols.txt
```

`--file` (also spelled `--symbols-file`) and `--symbol` are mutually exclusive.

File-driven recurring batches skip a symbol only when its previous complete correlation partition authenticates and
binds a `skip` disposition. Quarantined empty 404 symbols and exchange test symbols therefore incur no new API calls.
Authenticated empty SIP histories whose exact Alpaca input asset reports exchange `OTC` are also marked
`out_of_scope` and skipped; no ticker-name pattern is used.
An exact inactive, non-tradable Alpaca asset is likewise marked `out_of_scope` only when every authenticated SIP/1Day
adjustment view is present and empty; inactive assets with retained historical bars continue to run.
An exact-404 historical alias is marked `just_skip` only when corporate-action/CUSIP evidence resolves one occupant
to an Alpaca asset ID and all eight authenticated SIP/1Day views are present and empty.
Historical symbol-only series continue to run so later ticker reuse or newly available identity evidence can be
detected. Frozen files and their recorded integrity results remain untouched. To deliberately re-evaluate every
supplied symbol, use:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py \
  --file path/to/symbols.txt \
  --include-skipped-symbols
```

An explicit `--symbol` run always processes the ticker and surfaces its underlying integrity status; authenticated
recurring disposition skips apply only to `--file` batches.

When Alpaca responds to a bar request with exact HTTP 400 `invalid symbol` evidence, the batch records and summarizes
the failure as `invalid_ticker_code`. This is a known error, not a successful skip: the batch continues with other
symbols but exits nonzero after completing them.

The complete pipeline performs an exact Alpaca asset lookup before choosing the symbol used for downloads and local
artifacts. A successful US-equity lookup makes Alpaca's returned `symbol` authoritative, including punctuation. Thus:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py --symbol BFA --request-end 2026-10-06
```

operates on `data/end_of_day/BF.A/` and reports `BFA -> BF.A`. A repeat submission of `BFA` resolves the same way and
can reuse the authenticated `BF.A` cache. The correlation receipt records both values under `symbol_resolution`; do
not maintain a second `BFA` directory or security-master source. An exact asset 404 retains the submitted symbol so
the normal lineage and symbol-disposition rules can classify the evidence.

Use up to three ticker workers for overlapping network and per-symbol processing. Every worker shares one thread-safe
199-request-per-minute limiter across bar, corporate-action, and asset requests. Per-symbol artifacts remain isolated,
and the shared security-master read/modify/write transaction is serialized inside the process:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py \
  --file path/to/symbols.txt \
  --workers 3
```

To minimize bar requests while still materializing all eight logical adjustment filenames, add
`--adaptive-adjustments`:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py \
  --file path/to/symbols.txt \
  --workers 3 \
  --adaptive-adjustments \
  --request-end 2026-10-06
```

The adaptive policy requests `raw`, then `all`, and follows the empirically optimal decision tree. Across the 4,439
complete stocks used to validate policy version `alpaca_adjustment_equivalence_v1`, it covered all nine observed
equivalence patterns with 2.9606 bar downloads per stock on average. If the observations contradict every known branch,
the downloader fetches every remaining adjustment. This is an empirical optimization: a future pattern that is
indistinguishable on the sampled views can only be discovered by downloading all eight.

Every inferred logical view has an `empirical_adjustment_alias` manifest and is a same-directory hard link to an
authenticated direct response. Existing complete direct sets remain direct responses, but byte-identical gzip files are
atomically replaced with hard links as each ticker passes through the adaptive queue. Thus downstream filenames remain
unchanged while physical storage approaches one inode per unique payload. Never edit a linked gzip in place; publishers
replace pathnames atomically.

`--adaptive-adjustments` always represents all eight views and therefore cannot be combined with `--adjustment`,
`--force`, or `--repair`.

Run only one multi-worker pipeline process against a repository at a time. The security-master serialization coordinates
workers within that process; it does not coordinate an older or separate process that does not use the same lock.

The command freezes one exclusive UTC request boundary before processing the first ticker, so a long batch does not
change request identity when UTC midnight passes. Pin or reuse that boundary explicitly when resuming part of a batch:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py \
  --file path/to/symbols.txt \
  --request-end 2026-10-06
```

Omit `--adjustment` to process all eight views. Repeat it to select views. The full pipeline always refreshes identity
evidence, then publishes correlation, the master index, and warnings; coherent generations remain cached.
The master publication is sharded: a ticker update writes its source object, affected identity objects and inventory
buckets, and the small root under `data/end_of_day/json_manifests/`; it does not rewrite the complete identity corpus.
Correlation inspects every stored gzip for that ticker, so after a validation-version change run all views before a
partial-view update.

`download_alpaca_end_of_day_bars.py` is a compatibility entry point for the same full pipeline.

## Narrow commands

```bash
# Read-only lineage evidence to stdout.
./inspect_alpaca_security_lineage.py FB > /tmp/fb-lineage.json

# Authenticate local downloads, publish correlation and master-index updates, print the receipt.
./correlate_alpaca_end_of_day_security_segments.py FB
```

Correlation requires current validation receipts. Warning publication has a library API but no standalone command.

## Read

```bash
./read_alpaca_end_of_day_ticker.py \
  --type raw \
  --master-id <64-hex-master-id>
```

Types: `raw`, `split`, `dividend`, `spin-off`, `split,dividend`, `split,spin-off`, `dividend,spin-off`, `all`.

The command emits one JSON document to stdout. It accepts no Alpaca UUID, performs no update or repair, and makes no
network request. Authentication/read failures go to stderr with status `1`; argument errors use argparse status `2`.

Public API:

```python
from libs.alpaca_end_of_day_reader import read_alpaca_end_of_day_security_history

history = read_alpaca_end_of_day_security_history("<64-hex-master-id>", "raw")
```

For a consuming application that only needs repository availability:

```python
from libs.alpaca_end_of_day_reader import (
    get_alpaca_end_of_day_security_availability,
    list_alpaca_end_of_day_master_ids_by_availability_status,
    list_online_alpaca_end_of_day_master_ids,
)

status = get_alpaca_end_of_day_security_availability("<64-hex-master-id>")
if status["online"]:
    ...

online_master_ids = list_online_alpaca_end_of_day_master_ids()
lineage_gap_master_ids = list_alpaca_end_of_day_master_ids_by_availability_status("lineage_gap")
```

These calls are read-only, require no credentials, and expose no Alpaca request or pipeline state. `online` is the
boolean persisted by the completed ticker pipeline after confirming a published master ID, bars, current authenticated
dependencies, successful public-reader results for every advertised adjustment, and zero unresolved boundaries.
`availability_status: "lineage_gap"` means the reader otherwise succeeds but an expected market session is absent
inside the observed history, so the ID is offline but remains queryable. Repository availability is unrelated to
Alpaca's active/inactive asset status.

## Replacement and recovery

For a directly restorable backup of authenticated manifests, use the POSIX/PAX tar format so fractional mtimes are
preserved:

```bash
tar --format=posix --pax-option=delete=atime,delete=ctime \
  -czf /path/to/security-master-backup.tar.gz \
  data/end_of_day/json_manifests
```

The security-master receipt authenticates the root file's nanosecond mtime as well as its bytes and SHA-256. A basic
tar archive may round that timestamp and therefore fail closed after extraction even when every file byte is intact.
Test important backups by extracting them under a temporary repository root and authenticating the restored master.

```bash
# Replace one coherent frozen pair after authenticating it.
./run_alpaca_end_of_day_ticker_pipeline.py --symbol DELL --adjustment raw --force

# Recreate exactly one orphaned gzip/sidecar pair.
./run_alpaca_end_of_day_ticker_pipeline.py --symbol DELL --adjustment raw --repair
```

| State | Action |
|---|---|
| Both gzip and sidecar absent | Normal download |
| Coherent current pair | Cache hit |
| Coherent pair with stale derived generation | Local revalidation/republication; frozen pair unchanged |
| Changed request identity, including a later default end date | `--force` |
| Exactly one of gzip/sidecar missing | `--repair` on that adjustment |
| Malformed metadata, changed stat, or SHA mismatch | Stop; neither flag chooses an authority |

Do not manually delete one member of a pair to refresh data. `--repair` repairs publication completeness, not prices,
identity, or derived manifests.

## Exit status

| Status | Meaning |
|---|---|
| `0` | Selected stages completed with no actionable pricing/data integrity failure |
| `1` | Recorded/replayed integrity failure or runtime-stage failure |
| `2` | Corrupt frozen boundary or invalid recovery mode |

For multiple tickers, recorded integrity failures and per-ticker runtime failures are aggregated after processing all
selected tickers. A failed ticker does not cancel unrelated queued tickers; corrupted-boundary failures make the final
status `2`, and other per-ticker failures make it `1`. Warnings and uncorrelated work do not change exit status.
In a recurring `--file` batch, integrity failures belonging to a newly established skip disposition remain recorded
for audit but are reported separately and omitted from the active-universe failure total.
