# Business-equity baseline

This independent public `alpaca_end_of_day` consumer exports evidence for the per-`public_id` [business-equity definition](definition.yaml). It assigns no eligibility verdicts. Run from the repository root.

```bash
./IS_ELIGIBLE_LISTED_BUSINESS_EQUITY/cli baseline --plan
./IS_ELIGIBLE_LISTED_BUSINESS_EQUITY/cli baseline
```

`python3.11 -B -m IS_ELIGIBLE_LISTED_BUSINESS_EQUITY baseline` is equivalent. `--store PATH` selects a published store (default repository `data_pricing/`). `--output PATH` requires a new directory outside it (default Git-ignored `IS_ELIGIBLE_LISTED_BUSINESS_EQUITY/baselines/<UTC timestamp>/`). Runs offline without credentials.

## Scope and files

The compact lineage roster selects **all indexed source groups**. Export every resolved public ID, including historical identities and every reused-ticker match. Unresolved observations create no IDs. Scope is independent of online serving and current business eligibility.

| File | Content |
|---|---|
| `securities.csv` | One row per public ID: names, tickers, latest observed tickers, first/last stored sessions, exchanges, asset classes, CUSIPs and group reference. Lists are JSON arrays inside CSV cells. |
| `securities.jsonl` | The same identities with published dated assignments, canonical-security facts, ownership intervals and matching asset lookups. One JSON object per line. |
| `groups.jsonl` | One complete public-reader evidence document per group, including corporate actions such as splits, name changes, query dates, warnings and source generations. |
| `public_ids.txt` | Sorted unique public IDs, one per line. |
| `definition.yaml` | The exact definition bytes captured for this run. |
| `manifest.json` | Completion selector, exporter/schema versions, timestamps, source roster, counts and relative file names. |

Join on `group_id`. Corporate actions belong to the group. Their dates and ratios retain provider semantics. The baseline neither infers split factors nor recalculates lineage.

Names, exchanges and broad asset classes require a published own-label lookup matching the ID's provider-UUID membership. JSON retains every match, including CUSIP lookups and conflicting descriptions. Undated lookups remain undated. Observation times give no historical effective date. First/last sessions describe stored history only, with no implication for listing, incorporation or current operations. Empty metadata arrays mean this projection lacks those observations.

## Read and publication contract

Planning reads only the source roster. The CLI prints its plan before export, progress to stderr, then final JSON with manifest and counts. `StandaloneReader.open_published_evidence()` consumes one [published lineage snapshot](../docs/public-api.md#published-lineage-metadata). Read the index once and each selected receipt and assignment once. No pricing manifests, locks or payloads, upstream hashes, rule executions, subsequent freshness checks, source edits or repairs.

Timestamps date collection of published evidence. Missing or incompatible boundaries and an empty roster fail closed. Use the [owning commands](../docs/operations.md) for recovery.

Stage beside the destination and publish the directory only after every group succeeds. Consume only runs with `manifest.json`. Ordinary failures remove staging. Termination may leave an unselected `.baseline-*` directory. Retained baselines cannot be overwritten. Exit codes: 0 success, 1 failure, 2 argument error, 3 busy reader, 130 interruption.

The subsystem owns `VERSION`, `SCHEMA_VERSION` and [its changelog](change_log.json). The reader is versioned in the public API changelog. Repository [verification](../docs/development.md#verification) includes `tests/test_business_equity_baseline.py`.
