# Operations

Run commands from the repository root. Each command's `--help` output is the option authority.

## Command map

| Command | Purpose | Side effects and access |
|---|---|---|
| [`run_alpaca_end_of_day_ticker_pipeline.py`](../run_alpaca_end_of_day_ticker_pipeline.py) | Canonical complete pipeline for one or more symbols. | Writes runtime data and normally contacts Alpaca; credentials required. |
| [`download_alpaca_end_of_day_bars.py`](../download_alpaca_end_of_day_bars.py) | Compatibility name for the same complete pipeline. | Same pipeline options and data effects; use the canonical command for documented `SIGHUP` handling. |
| [`inspect_alpaca_security_lineage.py`](../inspect_alpaca_security_lineage.py) | Print local and direct-provider identity evidence for one ticker. | Read-only locally, contacts Alpaca, credentials required. |
| [`correlate_alpaca_end_of_day_security_segments.py`](../correlate_alpaca_end_of_day_security_segments.py) | Rebuild correlation and master output for one ticker from authenticated bars and fresh lineage evidence. | Writes derived runtime data, contacts Alpaca, credentials required. |
| [`audit_alpaca_end_of_day_catalog.py`](../audit_alpaca_end_of_day_catalog.py) | Authenticate and audit one complete published catalogue generation. | Strictly read-only; no credentials or network. |
| [`read_alpaca_end_of_day_ticker.py`](../read_alpaca_end_of_day_ticker.py) | Read one public history or export the online universe. | Strictly read-only; no credentials or network. |

## Requirements and safety

- Python 3.11, the configured and locally verified runtime target, plus IANA `America/New_York` time-zone data.
- `pigz` for any command that publishes or replaces a frozen gzip.
- `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY` for update, lineage, or correlation work that contacts Alpaca.
- No credentials or network access for consumer reads and exports.

A fresh checkout contains code and tracked [`resources/`](../resources/), but no runtime `data/`. Restore a compatible
runtime tree or deliberately acquire and publish one before using the public reader.

Use only one multi-worker pipeline process against a repository at a time. Its lock coordinates workers inside that
process; it does not coordinate another process or an older implementation.

Do not edit a gzip, sidecar, derived manifest, or sharded catalogue object by hand. Do not rerun a complete ticker
universe merely to apply an identity-rule change. Work from the current derived state and process the affected symbols
unless the task explicitly requires reacquisition.

## Run the complete pipeline

Process explicit symbols:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py --symbol FUN -s DELL -s FB
```

An explicit `--symbol` run always re-evaluates the symbol. For a recurring batch, supply one symbol per nonblank line:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py --file path/to/symbols.txt
```

`--file` (also `--symbols-file`) and `--symbol` are mutually exclusive. File duplicates are processed once in
first-seen order. A file-driven run skips a symbol only when its prior complete correlation partition and frozen inputs
authenticate and its evidence-derived disposition says to skip. To deliberately re-evaluate those entries:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py \
  --file path/to/symbols.txt \
  --include-skipped-symbols
```

Historical symbol-only series continue to run so later ticker reuse or newly available identity evidence can be
detected. Frozen evidence remains in place for skipped, quarantined, excluded, and out-of-scope symbols.

An exact Alpaca HTTP 400 `invalid symbol: <symbol>` becomes the known `invalid_ticker_code` failure. Other queued
symbols finish, then the batch exits nonzero.

### Authoritative symbols

The pipeline performs an exact Alpaca asset lookup before choosing the local symbol. A successful US-equity result
makes Alpaca's returned punctuation authoritative:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py --symbol BFA --request-end 2026-10-06
```

This operates on `data/end_of_day/BF.A/` and reports `BFA -> BF.A`. Do not create a parallel `BFA` directory or source.
The correlation receipt preserves the submitted alias under `symbol_resolution`. An exact asset 404 retains the input
symbol so lineage and disposition rules can classify it.

### Workers and rate limiting

Use one to three workers:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py \
  --file path/to/symbols.txt \
  --workers 3
```

Workers share one thread-safe 199-request-per-minute limiter across bar, asset, and corporate-action requests.
Per-symbol artifacts stay isolated, and the security-master read/modify/write transaction is serialized within the
process.

`SIGHUP` and `Ctrl-C` request a graceful shutdown: active workers drain before the command exits. Their conventional
statuses are `128 + signal` for `SIGHUP` and `130` for `Ctrl-C`.

### Adaptive adjustment acquisition

Add `--adaptive-adjustments` to materialize all eight logical views with fewer bar requests:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py \
  --file path/to/symbols.txt \
  --workers 3 \
  --adaptive-adjustments \
  --request-end 2026-10-06
```

Policy `alpaca_adjustment_equivalence_v1` requests `raw`, then `all`, and follows the recorded empirical decision tree.
The policy was measured over 4,439 complete stocks, covered nine observed equivalence patterns, and averaged 2.9606
bar requests per stock. An unfamiliar observation downloads every remaining adjustment.

An inferred view has its own `empirical_adjustment_alias` manifest and is a same-directory hard link to an
authenticated direct response. Existing complete direct views retain direct-response manifests; byte-identical files
may be atomically compacted to shared inodes. Never edit a linked gzip in place.

`--adaptive-adjustments` cannot be combined with `--adjustment`, `--force`, or `--repair`.

### Reproducible request boundaries

The command freezes one exclusive UTC request boundary before its first ticker so a batch crossing midnight keeps one
request identity. Pin it when reproducibility or a resumed subset matters:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py \
  --file path/to/symbols.txt \
  --request-end 2026-10-06
```

`--request-end` is exclusive: `2026-10-06` requests completed bars strictly before that instant, so the last ordinary
US session is `2026-10-05`. It is not the label of the last included bar.

Omit `--adjustment` to process all eight views; repeat it to select views. Correlation examines every stored gzip for
that symbol, so after a validation-version change process all views before a partial-view update.

`download_alpaca_end_of_day_bars.py` is a compatibility entrypoint for the same complete pipeline.

### Focused reprocessing

A narrow rule or data correction uses the canonical complete pipeline; it does not require new code or a
full-universe run. First determine the smallest complete set of affected source symbols: start with the identities
whose published result can change, include every source symbol already contributing rows to those identities, and
include any source symbol carrying evidence needed to decide them. Put that closed set in a plain file, one symbol per
line, and run:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py \
  --file path/to/affected-symbols.txt \
  --request-end YYYY-MM-DD
```

That ordinary `--file` invocation is sufficient unless an affected symbol has a prior authenticated disposition whose
recurring action is `skip`. Add `--include-skipped-symbols` only when the closed set deliberately includes one of those
symbols. Repeated `--symbol` arguments always re-evaluate their symbols and do not need that flag.

For a very small set, repeat `--symbol` instead of using `--file`. Omit `--adjustment` so all eight views pass through
the normal acquisition, authentication, correlation, warning, master-publication, and availability stages. The
pipeline authenticates reusable inputs and acquires only what its existing rules require up to the one shared request
boundary.

For a rule-only replay, `YYYY-MM-DD` must be the existing common `request_to` recorded by the authenticated bar
manifests. Do not substitute today's date, and do not use `--force`: unchanged request identities are cache hits, after
which the normal downstream stages apply the new rule. If the selected symbols do not share one authenticated
boundary, stop and split or investigate the set instead of silently advancing it.

A later `--request-end` changes the frozen request identity. Existing files then fail closed with
`refresh_required`; that is a deliberate data advance and only that operation may justify `--force`. When replacing a
dependency-connected set, use `--workers 1` so one worker cannot authenticate a related source while another worker
is replacing its gzip/manifest pair. This serialization warning concerns replacement only; ordinary cache-backed
focused replay may use multiple workers.

Do not create a migration script, alternate builder, direct bulk publisher, or shortened pipeline for this job. Do
not reprocess every source merely because its historical report contains the same kind of evidence. That larger set is
useful for a read-only impact audit; execution expands beyond the affected-symbol closure only when the audit proves
that another source can change the result.

Finish the rule and its focused tests before the read-only impact audit, then freeze the affected-symbol closure before
the replay. Correlation reports and catalogue generations bind deterministic output and provenance; changing rule code
or ordering after a replay means the replay and final catalogue audit must be repeated. Treat the post-audit generation
as final only after the source change itself is final.

## Narrow operator commands

```bash
# Authenticate every catalogue shard and run all catalogue-wide integrity checks.
./audit_alpaca_end_of_day_catalog.py

# Read lineage evidence and print JSON. This may contact Alpaca.
./inspect_alpaca_security_lineage.py FB > /tmp/fb-lineage.json

# Authenticate local downloads, publish correlation and master updates, then print its receipt.
./correlate_alpaca_end_of_day_security_segments.py FB
```

Correlation requires current validation receipts. Warning publication has a library API but no standalone command.
These commands are operator tools and do not define the consumer contract.

The catalogue audit writes its JSON report to standard output. It is exhaustive over the published catalogue, not the
complete frozen-data dependency graph; exact checks and scope are defined in
[Technical contracts](technical-contracts.md#read-only-catalogue-audit). An authentication or integrity error exits
with status `1`. Investigative warnings alone produce `passed_with_warnings` and status `0`; argument errors use status
`2`. Use `--repository-root` to inspect a staged restoration without placing it over the live repository.

## Consume online data

### One history

```bash
./read_alpaca_end_of_day_ticker.py \
  --public-id <64-hex-public-id> \
  --type raw \
  --start 2025-01-01 \
  --end 2025-12-31
```

`--master-id` is an alias for `--public-id`. Adjustment types are `raw`, `split`, `dividend`, `spin-off`,
`split,dividend`, `split,spin-off`, `dividend,spin-off`, and `all`.

The command prints one consumer JSON document. Bounds are inclusive market-session dates; omit them for all available
history. It performs no update, repair, or network request. Argument errors use status 2; authentication, contract, or
I/O errors use status 1.

### Flat online CSV

```bash
./read_alpaca_end_of_day_ticker.py \
  --dump-flat-online-csv /path/to/online.csv \
  --start 2025-01-01 \
  --end 2025-12-31
```

The export contains raw and fully adjusted OHLC, volume, trade count, and VWAP for every online ID with observations in
the range. It is ordered by date, then `unique_id`. The exporter uses disk-backed chunks and installs a path output
atomically, so reserve space beside the destination for both the spool and temporary final file. Use `-` to stream CSV
to stdout.

Python applications use `DataLibrary`; search, resolution, types, exact columns, and failure semantics are documented
in [Public API](public-api.md). Application code must not import the internal reader under `libs/`.

## Replacement and repair

```bash
# Replace one coherent authenticated pair with a new provider response.
./run_alpaca_end_of_day_ticker_pipeline.py \
  --symbol DELL \
  --adjustment raw \
  --force

# Recreate exactly one orphaned gzip/sidecar pair.
./run_alpaca_end_of_day_ticker_pipeline.py \
  --symbol DELL \
  --adjustment raw \
  --repair
```

| Observed state | Required action |
|---|---|
| Both gzip and sidecar absent | Normal download. |
| Coherent current pair | Cache hit. |
| Coherent pair with stale derived generation | Authenticate and reprocess locally; frozen pair stays unchanged. |
| Changed request identity, including a later default end | Use `--force`. |
| Exactly one of gzip or sidecar missing | Use `--repair` for that adjustment. |
| Malformed metadata, changed stat, or SHA mismatch | Stop. Neither flag chooses an authority. |

`--repair` restores publication completeness; it does not repair prices, security identity, correlation, or derived
manifests. Never delete one member of a pair to force a refresh.

## Backup and recovery

Choose between restoring an existing authenticated generation and acquiring a new one. They are different outcomes.

### Restore an existing generation

Frozen bars, correlation outputs, warning outputs, the master catalogue, and primary evidence referenced by a verified
session exclusion form one dependency graph. Back up the complete `data/end_of_day/` tree when the goal is to reproduce
the existing service state; a manifest-only archive cannot recreate missing frozen or evidence files.

The security-master receipt authenticates the root file's nanosecond mtime as well as its bytes and SHA-256. Use a
POSIX/PAX archive so fractional mtimes survive:

```bash
tar --format=posix --pax-option=delete=atime,delete=ctime \
  -czf /path/to/alpaca-end-of-day-backup.tar.gz \
  data/end_of_day
```

Restore into a staging copy of a compatible source revision, then audit that copy before switching consumers to it:

```bash
tar -xzf /path/to/alpaca-end-of-day-backup.tar.gz \
  -C /path/to/restored-repository

./audit_alpaca_end_of_day_catalog.py \
  --repository-root /path/to/restored-repository
```

A basic tar archive may round timestamps and fail authentication even when bytes match. Never test restoration over the
live catalogue.

### Acquire a fresh generation

The source checkout includes [`resources/ticker.csv`](../resources/ticker.csv), a headerless, single-record CSV of the
sorted unique source-symbol universe captured from the authenticated master catalogue. It includes source symbols, not
only currently online public identities. Convert it to the pipeline's one-symbol-per-line format:

```bash
tr ',' '\n' < resources/ticker.csv > /tmp/alpaca-restore-symbols.txt
```

Running that complete list is an explicit, network-intensive rebuild. With credentials, entitlement, `pigz`, and an
intentional exclusive request boundary, the command is:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py \
  --file /tmp/alpaca-restore-symbols.txt \
  --workers 3 \
  --adaptive-adjustments \
  --request-end YYYY-MM-DD
```

This regenerates source files, manifests, correlation outputs, warnings, and the catalogue; none of those runtime files
come from `resources/`. It is not a byte-for-byte restoration: provider data, identity evidence, and the chosen request
boundary can produce a different authenticated generation. Refresh the tracked ticker snapshot deliberately whenever
the authenticated source-symbol universe changes.

## Pipeline exit status

| Status | Meaning |
|---|---|
| `0` | Selected stages completed without an actionable pricing/data integrity failure. |
| `1` | Recorded/replayed integrity failure or another runtime-stage failure. |
| `2` | Corrupt frozen boundary or invalid recovery mode. |

Multi-symbol runs aggregate failures after all selected tickers finish. A failed ticker does not cancel unrelated queued
tickers. Warnings and uncorrelated work do not change exit status. In recurring file batches, integrity failures bound
to a newly authenticated skip disposition remain recorded for audit but are excluded from the active-universe failure
total.
