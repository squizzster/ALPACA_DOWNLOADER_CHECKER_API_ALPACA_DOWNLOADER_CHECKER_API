# Operations

Run from the repository root using Python 3.11. Each command's `--help` is the option reference.
Producers need `ALPACA_API_KEY_ID`, `ALPACA_API_SECRET_KEY` and SIP access.
Readers need compatible published data, without credentials or network.

## Daily update

```bash
./download_alpaca_pricing.py --update-everything
```

Defaults to the application's `data_pricing/`; `--store PATH` overrides it.
Updates the **existing stored-active universe**, including required lineage dependencies.
It does not discover a new universe. A ready check summary is required.

The [ordered flow](ARCHITECTURE.md#daily-update) recovers work, checks history, replaces changes,
appends missing sessions and finishes lineage. Stage progress goes to stderr; the final result is JSON
on stdout. There is no per-request log or automatically saved run report.
A failed result returns nonzero. Rerun after interruption.

The target is the last exchange session **strictly before today's New York date**, frozen once per run.
Friday becomes eligible after Saturday midnight in New York. Crossing UTC midnight changes nothing.
`--to` is inclusive; `--request-end` is exclusive UTC midnight. Both enforce that cutoff.
`--from`, force and `--append-ready` cannot accompany `--update-everything`.

## Summary bootstrap

For an existing standalone store with published lineage, seed once:

```bash
./build_alpaca_lineage.py --store data_pricing --rebuild-check-summary
./build_alpaca_lineage.py --store data_pricing --check-summary
```

Both are offline. Rebuild reads indexed stored status and endpoint validators, without price-history reads
or provider requests. Finish pending publications first. Normal pricing/lineage finishes then maintain the
summary automatically. Missing or incomplete coverage requires explicit rebuild; it is never treated as
an empty universe. Rebuild after external store changes or older producers that do not maintain it.

## Focused standalone work

```bash
./download_alpaca_pricing.py --store data_pricing -s DELL -s HPE
./build_alpaca_lineage.py --store data_pricing --pending
./output_current_lineage_evidence.py DELL --store data_pricing --offline
```

Ordinary pricing downloads require an explicit compatible or empty store. They publish pricing only;
run lineage afterward. Initial history starts in 2016 unless `--from` specifies a later start.
A start that excludes held observations is rejected, including with force.

| Option | Scope |
|---|---|
| Pricing `--file PATH` | CSV or one ticker per line. |
| Pricing `--all` | Tracked `resources/ticker.csv` universe; potentially a full acquisition. |
| Pricing `--force-download` | Reacquire the full retained history. |
| Pricing `--append-ready` | Only after approved live history checking/replacement; fetch missing sessions. With `--all`, selects the ready summary. |
| Lineage `--symbol TICKER` | Selected dependency group; can acquire missing related pricing. |
| Lineage `--pending` / `--all` | Local changed inputs and stale rules, using hints or a fallback receipt scan. |
| Lineage `--no-download` | Disable pricing acquisition; metadata requests still occur. |
| Lineage `--refresh-metadata` | Reacquire full evidence, including older corrections. |
| Lineage `--refresh-existing` | Fresh live check, same-span replacements and affected lineage; no append. |

Pricing uses up to three workers sharing one 199-request/minute budget. This limit is per invocation,
not account-wide. Lineage allows one worker per store; a second waits.
Standalone batches report individual failures and retain completed work. Standalone lineage prints its
result at completion; quiet stdout is not a progress indicator.

Evidence inspection without `--offline` runs the normal workflow and may acquire/publish data.
Offline evidence reads existing decisions; it does not replay rules.

## Recovery

Rerun the owning producer. Locks, staged files and journals let it finish interrupted publication.
Do not delete lock files, pending markers or staging by hand, or edit selected artifacts.
A lock file's existence does not mean the lock is held.

For interrupted explicit append, first run the ordinary downloader for the affected ticker with the
same `--request-end`, then reload the ready summary before retrying.
Readers reject incomplete publications; they never repair them.

`lineage_update_check.sqlite3` and `download_completion.json` are disposable scheduling hints.
After an out-of-band restore/import, discard those two caches so receipts are rediscovered; retain actual
receipts, locks and journals. Rebuild the summary separately. Copy/restore a complete store only when
authorized, with producers stopped; never test restoration over the live store.

## Read and export

```bash
./read_alpaca_pricing.py --store data_pricing -s DELL --resolve
./read_alpaca_pricing.py --store data_pricing -s DELL --type raw all
./convert_latest_alpaca_pricing_to_blood_hound_parquet --output ./alpaca.parquet -s DELL
```

See [Public API](public-api.md) for identity selection, date ranges and output types.
The standalone reader returns status 3 when busy; `--wait` waits.
Otherwise 0 means success, 1 runtime failure and 2 invalid CLI arguments.

Parquet export additionally needs [requirements-bloodhound.txt](../requirements-bloodhound.txt).
The output parent must exist outside the source store. Default selection is all indexed lineage groups.
`--include-all-passed` permits rejected identities; diagnostics go only to stderr.
`--replace` permits output replacement after successful conversion. Snapshot changes, ID collisions,
memory exhaustion and writer failures abort. The only persistent output is the Parquet file.
Conversion does not register data with Bloodhound.

## Separate catalogue workflow

These commands write `data_vtest/`, not the standalone store. Replace `YYYY-MM-DD` below with the
intended exclusive UTC boundary:

```bash
./run_alpaca_end_of_day_ticker_pipeline.py -s DELL --request-end YYYY-MM-DD
./read_alpaca_end_of_day_ticker.py --dump-flat-online-csv ./online.csv
```

The combined producer resolves canonical symbols and runs the complete catalogue cascade.
`download_alpaca_end_of_day_bars.py` is an alias for this producer.
Its `--file` accepts one label per line, not the recovery CSV. New symbols acquire from 2016;
existing symbols replace selected months. `--request-start` must be a month start.
Moving the end backward can truncate the terminal month; an empty response does not delete an old month.
Its default end is current UTC midnight, unlike the standalone New York cutoff; pin it explicitly.

Use one producer per store. Its lock blocks catalogue readers for the full transaction.
After interruption, rerun the **same canonical symbols and exact boundaries**. The recovery marker does
not enforce that match, so a different request is not a recovery shortcut.

For an explicitly authorized fresh universe:

```bash
./build_alpaca_end_of_day_clean_room.py --universe resources/ticker.csv --store data_clean_room --request-end YYYY-MM-DD
```

The universe is one sorted, unique CSV record. The isolated store starts empty and consumes only this
run's provider responses. Resume with identical inputs and versions. A mandatory full audit follows
construction; `--activate` then selects it as `data_vtest/`, retaining the previous store.

The legacy `inspect_alpaca_security_lineage.py`, `correlate_alpaca_end_of_day_security_segments.py` and
`audit_alpaca_end_of_day_catalog.py` target `data/end_of_day/`. They do not operate on the current catalogue.
