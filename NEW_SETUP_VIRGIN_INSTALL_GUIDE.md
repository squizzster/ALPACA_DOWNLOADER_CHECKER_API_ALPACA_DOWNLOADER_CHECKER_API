# Set up an Alpaca pricing and lineage store from empty

Start with an Alpaca account, a fresh checkout and an empty or unused store path to build a metadata
export keyed by public security ID. For published stores, see
[existing-store setup and recovery](docs/operations.md#adopt-or-reconstruct-an-existing-store).

## 1. Prepare the checkout

From the repository root, use Python 3.11 and choose a store that meets the
[runtime and filesystem requirements](docs/operations.md#prerequisites). Set `ALPACA_API_KEY_ID` and
`ALPACA_API_SECRET_KEY` in your environment with historical SIP access. The checkout supplies the
[recovery ticker registry](resources/ticker.csv) and a
[market-session calendar](libs/common/us_market_hours.json) that must cover your trading dates.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
./alpaca-cli-eod --help
store_dir="$PWD/data_pricing"
```

## 2. Build pricing, lineage and the serving catalogue

```bash
./alpaca-cli-eod universe rebuild --store "$store_dir"
./alpaca-cli-eod pricing acquire --store "$store_dir" --recovery-universe
./alpaca-cli-eod lineage update --store "$store_dir"
./alpaca-cli-eod status --store "$store_dir"
```

Even with another `--store`, the checkout's recovery registry supplies request labels, including retired
tickers today's catalogue may omit. Alpaca supplies prices and identity evidence. History defaults to
2016. To fix the pricing cutoff, give acquisition and lineage update the same `--through YYYY-MM-DD`.
Asset lookups still observe Alpaca at request time.

After lineage publishes, `status` may exit 1. Its JSON blockers name the owners of missing daily-update
boundaries. For a missing or incomplete pricing-check summary, run
`./alpaca-cli-eod summary rebuild --store "$store_dir"`. Bootstrap the catalogue and check readiness.

```bash
./alpaca-cli-eod catalogue bootstrap --store "$store_dir" --plan
./alpaca-cli-eod catalogue bootstrap --store "$store_dir"
./alpaca-cli-eod status --store "$store_dir"
```

When `ready_for_update` is true, update to discover eligible securities and finish pending work. Reuse
any fixed `--through`. Even on exit 0, check `complete` and `warnings`, then `processing_complete` in the
status report. Interrupted producers resume from published work.
[Daily update, registry Git behavior and recovery](docs/operations.md#daily-update)

```bash
./alpaca-cli-eod update --store "$store_dir"
./alpaca-cli-eod status --store "$store_dir"
```

## 3. Recover exchange changes that predate installation

**Run the historical backfill explicitly.** Daily updates record observed exchange changes. The first
asset snapshot supplies one observation. Earlier transfers require historical evidence.

```bash
./alpaca-cli-eod lineage backfill-listings --store "$store_dir" --all-published --plan
./alpaca-cli-eod lineage backfill-listings --store "$store_dir" --all-published --apply
./alpaca-cli-eod lineage backfill-listings --store "$store_dir" --all-published --plan
```

The offline plan reports outstanding online ticker ranges. If prepared metadata is missing, run
`./alpaca-cli-eod catalogue prepare-metadata --store "$store_dir"` and repeat the plan. Apply adds dated
evidence to published lineage under the existing pricing approval.
[Producer handoffs](docs/ARCHITECTURE.md#producer-handoffs)

For a trial, replace `--all-published` with `--symbol SYMBOL` and run `--dry-run` before `--apply`. Dry
runs save live observations locally for application or resumption. The daily update's own plan can
contact Alpaca. In the final all-published plan, `groups_with_outstanding_ranges: 0` means every selected
range has a published assessment, including inconclusive results.
[Backfill modes and recovery](docs/operations.md#backfill-historical-listings)

## 4. Export and interpret the result

```bash
./alpaca-cli-eod export metadata --store "$store_dir"
```

The report names `manifest.json` and `securities.jsonl.gz`. Each JSON line identifies one online security
by `public_id`. Read `historical_listings` for dated trades and evidence limits,
`cross_ticker_listing_changes` for supported transfers across renames, and asset observations for
provider snapshots. [Metadata companion protocol](docs/exports.md#security-metadata-companion)

**What can a fresh install reproduce?** The same public ownership ranges, rules, calendar, access and
dated Alpaca evidence should support the same conclusions. For example, if Alpaca reports a GM trade on
the NYSE tape on November 16, 2026 and the Nasdaq tape the next session, the rule dates its transfer to
November 17. The generic rule needs no GM-specific setup input.

Alpaca's history, available symbols and account entitlements can change. An older store may retain
snapshots a later install cannot retrieve. Sparse samples can miss intervening transfers, and several
exchanges share a tape. Later installs may have less evidence or different date bounds. Observation
timestamps and catalogue generation IDs are run-specific.
[Listing evidence and limits](docs/lineage.md#historical-listing-assessments)
