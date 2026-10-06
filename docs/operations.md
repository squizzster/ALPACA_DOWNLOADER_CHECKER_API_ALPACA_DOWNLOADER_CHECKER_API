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

Omit `--adjustment` to process all eight views. Repeat it to select views. The full pipeline always refreshes identity
evidence, then publishes correlation, the master index, and warnings; coherent generations remain cached.
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

## Replacement and recovery

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
| `0` | Selected stages completed with no pricing/data integrity failure |
| `1` | Recorded/replayed integrity failure or runtime-stage failure |
| `2` | Corrupt frozen boundary or invalid recovery mode |

For multiple tickers, recorded integrity failures are aggregated after processing all selected tickers. Authentication
or publication failure aborts immediately. Warnings and uncorrelated work do not change exit status.
