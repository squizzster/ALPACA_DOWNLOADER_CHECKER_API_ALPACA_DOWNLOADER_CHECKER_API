# Commands and operations

Run commands from the repository root.

## Prerequisites

- `python3`; Ruff targets Python 3.11 as configured in [`pyproject.toml`](../pyproject.toml).
- `pigz` on `PATH` whenever a new, forced, or repaired frozen download must be published. Cache reads and local
  validation migrations do not invoke it.
- `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY` for every full-pipeline, lineage, or correlation command. The full
  pipeline always queries identity APIs even when all bar downloads are cached; bar credentials themselves are read
  lazily by the download stage.

Use built-in help as the option authority:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py --help
./download_alpaca_end_of_day_bars.py --help
./inspect_alpaca_security_lineage.py --help
./correlate_alpaca_end_of_day_security_segments.py --help
```

## Complete ticker pipeline

The primary command accepts repeatable symbols and processes them in order:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py --symbol DELL -s FB -s TWTR
```

Omitting `--adjustment` selects every supported adjustment. Repeating it selects specific modes. The older
`download_alpaca_end_of_day_bars.py` entry point invokes the same complete pipeline and remains a compatibility
command; it is not a download-only CLI.

The stages are download/cache validation, live identity evidence, correlation publication, and warning publication.
The command reports correlation and warning generation summaries on stdout. Identity evidence is fetched afresh but
semantic generation digests exclude its timestamp and API request count, allowing unchanged correlation output to
remain cached.

Correlation inspects every local `*.csv.gz` in the symbol directory, not only adjustments selected on the command
line. Every such file must have a current validation receipt. After a validation version change, omit `--adjustment`
on the first run for a symbol with multiple existing modes so the complete receipt generation is rebuilt before
correlation.

## Narrow commands

The lineage command is read-only and emits one JSON document to stdout:

```bash
./inspect_alpaca_security_lineage.py FB > /tmp/fb-lineage.json
```

It reads local sidecars and derived-manifest summaries without authenticating gzip SHA-256, then queries Alpaca. The
report is evidence, not a row-to-UUID assignment.

The correlation command queries lineage, authenticates every local frozen download, publishes both row-partition
branches and their receipt, then prints that receipt:

```bash
./correlate_alpaca_end_of_day_security_segments.py FB
```

It requires current validation receipts. Use the complete pipeline to migrate stale validation state. Warning
publication currently has a library API but no standalone executable.

## Replacement and recovery

Use the narrowest adjustment selection with a mutation flag:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py --symbol DELL --adjustment raw --force
./run_alpaca_end_of_day_ticker_pipeline.py --symbol DELL --adjustment raw --repair
```

`--force` replaces every selected coherent pair after first recomputing and verifying its existing SHA-256. It is
required when the request identity changes, including a later request end/`asof`. It cannot bypass malformed metadata,
changed file stats, or a SHA mismatch.

The default request end is the current UTC midnight and `asof` is that date. Running on a later UTC date therefore
changes the request identity of an existing pair and requires `--force` to fetch that later range.

`--repair` is only for exactly one missing member of a gzip/sidecar pair. Every selected pair must be orphaned, so
select the specific affected adjustment. It redownloads and republishes the complete pair; it does not repair prices,
identity assignments, or derived manifests.

Do not manually delete only one member to refresh data. Do not use either flag to choose an authority after an
unexplained mismatch.

## Cache and migration behavior

- Both frozen files absent: the bar stage downloads normally.
- Coherent current pair and current derived generation: cache hit; no artifact is rewritten.
- Coherent pair with a stale or missing validation receipt: SHA-authenticate and revalidate locally.
- Current recorded integrity failure: replay it without changing the frozen pair unless `--force` requests a fresh
  authenticated replacement.
- Changed request identity: require `--force`.
- Malformed, stat-inconsistent, or SHA-mismatched boundary: abort without replacement.

Version-driven local migrations may update validation, correlation, and warning manifests. They must leave frozen
gzips and sidecars unchanged.

## Exit behavior

- `0`: all requested stages completed and no selected artifact reported a pricing/data integrity failure.
- `1`: pricing/data failures were recorded or replayed, or a non-boundary runtime stage failed.
- `2`: the frozen download boundary is corrupt, unsafe to replace, or used with an invalid recovery mode.

For multiple symbols, recorded pricing/data failures are aggregated after every selected ticker has completed its
downstream stages. An authentication or runtime exception aborts immediately. Warnings, information findings,
uncorrelated rows, and uncorrelated work items do not by themselves change the CLI exit status; inspect their owning
manifests before treating a ticker as reader-ready.
