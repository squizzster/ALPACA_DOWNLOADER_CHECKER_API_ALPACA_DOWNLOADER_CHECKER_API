# Business-equity baseline

An independent consumer of the repository's public `alpaca_end_of_day` API. The
[definition](definition.yaml) specifies `IS_ELIGIBLE_LISTED_BUSINESS_EQUITY` per
`public_id`. The CLI exports existing information before assessment; it assigns
no TRUE/FALSE values.

Run from `/data2/alpaca`:

```bash
./IS_ELIGIBLE_LISTED_BUSINESS_EQUITY/cli baseline --plan
./IS_ELIGIBLE_LISTED_BUSINESS_EQUITY/cli baseline
```

`python3.11 -B -m IS_ELIGIBLE_LISTED_BUSINESS_EQUITY baseline` is equivalent.
`--store PATH` selects an existing published store; the default is repository
`data_pricing/`. `--output PATH` selects a new run directory outside that store.
Existing output directories are refused. The default output is
`IS_ELIGIBLE_LISTED_BUSINESS_EQUITY/baselines/<UTC timestamp>/`, ignored by Git.
No credentials or network calls are required.

## Scope and files

The compact published lineage roster selects **all indexed source groups**.
The export includes each resolved `public_id` reported by those groups, including
historical identities. It is a standalone resolved-identity baseline, not the
online serving universe or a list of currently eligible businesses. Unresolved
observations do not become invented identities. A ticker may occur on several
identities, and all matches survive.

Each completed run contains:

| File | Content |
|---|---|
| `securities.csv` | One row per public ID: names, tickers, latest observed tickers, first/last stored sessions, exchanges, asset classes, CUSIPs and group reference. Lists are JSON arrays inside CSV cells. |
| `securities.jsonl` | The same identities with published dated assignments, canonical-security facts, ownership intervals and matching asset lookups. One JSON object per line. |
| `groups.jsonl` | One complete public-reader evidence document per group, including corporate actions such as splits, name changes, query dates, warnings and source generations. |
| `public_ids.txt` | Sorted unique public IDs, one per line. |
| `definition.yaml` | The exact definition bytes captured for this run. |
| `manifest.json` | Completion selector, exporter/schema versions, timestamps, source roster, counts and relative file names. |

Join an identity to its group evidence using `group_id`. Corporate actions remain
**group evidence**, not certified events belonging to each group member. Action
dates and ratios retain their provider semantics. The exporter does not infer
split factors or recalculate lineage.

Names, exchanges and broad asset classes come only from published own-label asset
lookups matching the identity's recorded provider-ID membership. All matching
lookups, including conflicting descriptions and CUSIP lookups, remain in the JSON.
An undated lookup remains undated. Observation times do not become historical
effective dates; first/last sessions describe stored history, not listing dates,
incorporation dates or evidence of current business operations. Empty metadata
lists mean those descriptive observations are unavailable from this projection.

## Read and publication contract

Planning reads the source roster without opening group evidence. The CLI prints
the plan before export and progress to stderr; its final JSON result names the
manifest and counts. Export consumes a
[published-lineage evidence snapshot](../docs/public-api.md#published-lineage-metadata)
through `StandaloneReader.open_published_evidence()`. It reads the accepted index
once and each selected receipt and assignment document once. There are no pricing
manifests, pricing locks, price payloads, upstream hashes, rule executions or later
freshness checks. It does not modify or repair the source store.

Timestamps describe collection of already-published evidence; they do not claim
fresh provider observations. Missing or incompatible lineage boundaries fail
closed without reconstruction. An empty roster is refused. Resolve source issues
through the owning [operations](../docs/operations.md) commands.

Files are staged beside the destination; the completed directory becomes visible
only after every selected group succeeds. Readers consume only
a run containing `manifest.json`. Ordinary failures remove staging files;
process termination can leave an unselected `.baseline-*` directory. No retained
baseline is overwritten. Exit codes: 0 success, 1 failure, 2 argument error,
3 busy reader, 130 interruption.

The subsystem owns `VERSION`, `SCHEMA_VERSION` and [its changelog](change_log.json).
The new public metadata reader is recorded in the public API's owning changelog. Repository
[verification](../docs/development.md#verification) includes the focused baseline
tests in `tests/test_business_equity_baseline.py`.
