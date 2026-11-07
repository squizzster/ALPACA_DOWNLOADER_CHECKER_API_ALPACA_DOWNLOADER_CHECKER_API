# Alpaca to Bloodhound converter

A standalone offline converter for completed Alpaca per-identity CSV exports and legacy resolved histories.
It writes an 18-column Bloodhound Parquet file and a JSON report. This application has its own dependencies
and CLI; the engine's separate [Parquet exporter](../docs/exports.md#parquet-conversion) remains available.

Imported from `alpaca_to_bloodhound_app_current_csv.zip`, application version 1.1.0. The accepted package
handoff records SHA-256 `fd03f336b3bc4e955f58ec675658e14f1a885f4e45d394f9db95f5cbd332fe67`.
Source, tests and synthetic examples are kept here. Generated data, environments and execution logs stay
outside Git. The [repository trust-boundary review](../docs/trust-boundary-handoff.html) identifies the
scope of the engine's guarantees; this app's known limitations are described below.
Repository runtime changes are recorded in [change_log.json](change_log.json); one-based array position
matches `RUNTIME_VERSION`. The CLI and output metadata retain the application's semantic version string.

```bash
python alpaca_to_bloodhound.py --data-in /path/to/top_331 --blood-out bloodhound.parquet
```

The corrected default conversion **copies each supplied fully adjusted OHLC value**, preserves raw close and raw volume, computes dollar volume from the raw pair, and keeps the split-only share factor separate from the overall price-adjustment ratio.

The application is offline after installation. It does not require Alpaca credentials or fetch prices or corporate actions.

## 1. Install and run

Use Python 3.11 or later. Application dependencies are pinned in [requirements.txt](requirements.txt).

From this directory:

```bash
python -m venv .venv
```

Activate the environment with `source .venv/bin/activate` on macOS/Linux, or `.venv\Scripts\activate` in Windows Command Prompt. Then:

```bash
python -m pip install -r requirements.txt
python alpaca_to_bloodhound.py --data-in /path/to/extracted_data --blood-out bloodhound.parquet
```

If the input is still zipped, extract it first:

```bash
python -m zipfile -e /path/to/top_331.zip /path/to/top_331
```

Two output files are created:

| File | Contents |
| --- | --- |
| `bloodhound.parquet` | The 18 typed data columns, with price/volume/evidence declarations in Parquet metadata |
| `bloodhound.parquet.report.json` | Source hashes, source-to-series-ID mapping, factor intervals and provenance, row counts, validation results, and any rejected histories |

The output suffix must be `.parquet`. A directory with one security, many security directories, or an extra wrapper directory works. Existing output/report files require `--overwrite`.

## 2. Exactly what is calculated

The default `--price-basis all` implements:

```python
adjusted_open = float64(all.open)
adjusted_high = float64(all.high)
adjusted_low = float64(all.low)
adjusted_close = float64(all.close)

raw_close = float64(raw.close)
volume = float64(raw.volume)

dollar_volume = raw_close * volume
adjustment_ratio = adjusted_close / raw_close

split_adjustment_ratio = separately_established_share_unit_price_factor
```

The factor is inferred from source views by default, or constructed from explicitly supplied corporate-action records as described below. Its provenance is declared in both output files.

The implementation does not reconstruct adjusted open/high/low using `raw_price * (adjusted_close / raw_close)`. Those calculations can change individually rounded provider values. For example, a supplied adjusted open of 33.33 is preserved even when reconstruction from the rounded close would give about 33.3366.

`volume` is `float64`, so fractional supplied quantities are retained. It is not multiplied by a split or dividend adjustment before storage. `dollar_volume` is the raw-close-based measure required by the supplied contract, not the sum of actual transaction values throughout the day.

The downstream cutoff conversion, if needed by Bloodhound calculations, is:

```text
volume_at_cutoff(t, c) = raw_volume(t) * S(c) / S(t)
```

That formula is documented in the report but is deliberately not applied to the stored `volume` column.

### Explicit split-only price basis

To construct split-only adjusted prices:

```bash
python alpaca_to_bloodhound.py \
  --data-in /path/to/data \
  --blood-out split_only.parquet \
  --price-basis split
```

This mode writes `raw.open/high/low/close * split_adjustment_ratio`. It continues to store raw close and raw volume, and computes the overall adjustment ratio from the resulting adjusted close. Its price basis is declared as `split`. It can be combined with the action-evidence options below. Inference limitations still apply when the selected factor source is `infer`.

## 3. Supported input layouts

### Current per-identity CSV export

Point `--data-in` at one completed timestamped run containing `final_manifest.json`:

```bash
python alpaca_to_bloodhound.py \
  --data-in /path/to/exports/csv/COMPLETED_RUN \
  --blood-out /path/to/output/bloodhound.parquet
```

The application reads schema-1 and schema-2 `alpaca_public_id_eod_csv` exports. The final manifest selects
identities and names each digest/CSV pair. Digests supply the eight `views` mappings and
direct `column_aliases`; physical series can be shared across views and fields. Each selected
CSV is read and parsed once, then projected into the converter's existing aligned logical
views. No physical filename or column-prefix guess is used. Pricing, factor inference,
action arithmetic and the 18-column output schema retain their existing behavior.

Repeat `--public-id ID` to convert a bounded selection. Missing IDs fail before any digest
or CSV is opened. The adapter consumes only members selected by the completed manifest;
it does not crawl the run directory or open a source store. Identity/schema/catalogue-token
checks apply at the accepted handoff. Published CSV checksums are retained as provenance,
not recomputed to authenticate completed upstream work. Newly produced Parquet is still
validated and hashed by this converter.

The current CSV adapter constructs every view from the same accepted session and dated-label base, including
any explicit metadata override. The history consumer retains that alignment guarantee without comparing the
shared arrays again. Independently loaded legacy frames establish alignment once per distinct frame; aliases
reuse it for that history load.

Current CSVs contain dates and numeric columns, without a per-row ticker column. Schema 2 supplies the
completed observed-label decision in `security.ticker_ranges`, with inclusive `first_session` and
`last_session` endpoints. The adapter projects those ranges once into row labels shared by all eight views.
It does not reconcile ownership evidence, audit membership against descriptive ticker lists, reopen the
source catalogue or recalculate the producer's decision. Missing schema-2 handoffs fail before reading CSV
payloads. An empty or incomplete handoff never falls back to broader evidence or a current ticker.

Multiple actual labels or missing labels fail closed. Supply `--metadata` with explicit dated `symbol`
intervals to resolve such rows. Undated company names, security types and exchanges are not projected over
historical rows. The source of ticker labels is recorded with each history's payload provenance.

Schema 1 retains its compatibility interpretation: a unique `ticker_ownership_intervals` match
(inclusive start, exclusive end), or the sole published ticker when no interval matches. This older format
does not guarantee a complete dated-label map. Obtain the completed handoff through a new schema-2 export;
the converter never fills an old export's metadata from a newer store snapshot.

For schema 1, interval selection uses the digest's published CSV `range` once per identity, before any additional
output date filter. Intervals ending on or before its first session, or starting after its last session,
are outside that input range and their ticker labels are ignored. Intervals wholly between actual input
sessions are also irrelevant; selection uses the producer's ordered session grid without sorting or
authenticating it again. Only intervals covering an input session must name a published ticker.
Selected intervals and the single-ticker fallback are retained for all input rows;
the resulting row labels are shared by all eight views. An interval covering even one input session
remains relevant, including when that session is removed by a later output filter.

The existing `--price-basis`, action-evidence and output-date filters also work with this
layout. Factors are computed on the entire supplied CSV before additional date filtering.
A previously filtered CSV export cannot supply observations it no longer contains.
Incomplete runs without a final manifest, unsupported schemas and incompatible selected digests
are not accepted as completed current exports. Select the exact run directory, not the
parent holding multiple runs. Existing legacy per-view layouts remain supported below.
Current-layout regressions are in [tests/test_current_csv.py](tests/test_current_csv.py).


### Legacy resolved exporter manifests: the originally supplied layout

Each security directory contains `manifest.json` and one or more `.csv.gz` or `.csv` files. The manifest identifies the security and maps logical adjustments to physical payloads:

```json
{
  "public_id": "security-identity",
  "read_symbol": "ABC",
  "adjustments": [
    {"adjustment": "raw", "file": "raw.csv.gz"},
    {"adjustment": "split", "file": "raw.csv.gz", "equivalent_to": "raw"},
    {"adjustment": "all", "file": "dividend.csv.gz", "equivalent_to": "dividend"}
  ]
}
```

The supplied manifests list all eight logical adjustments. Many use `storage: "exact_copy"` and point to an existing payload. **The `file` mapping is authoritative.** An `all.csv.gz` physical file is not required if the manifest maps `all` to another payload. Each distinct file is read once per security, while all declared logical views participate in validation.

When present, compressed-file and decompressed-CSV SHA-256 values, byte counts, row counts, and first/last session declarations are checked. Payload paths must remain inside the security directory. Duplicate security identities across directories are rejected; the converter does not choose between source generations implicitly.

The eight recognized adjustments are:

```text
raw
split
dividend
spin-off
split,dividend
split,spin-off
dividend,spin-off
all
```

`raw`, `split` and `all` are required. Missing intermediate views provide no numerical constraints and are listed in the factor diagnostics. Having fewer views does not establish that the omitted adjustments equal one.

### Simple directories without manifests

If no manifests exist beneath `--data-in`, the application discovers directories containing `raw.csv` or `raw.csv.gz`. Each directory must also contain explicit `split` and `all` files with either suffix. Optional combined views use names such as `split_dividend.csv.gz` and `dividend_spin-off.csv.gz`. Supplying both `.csv` and `.csv.gz` copies of one view is treated as ambiguous.

All source files require:

```text
session,open,high,low,close,volume
```

They must also contain a single dated ticker in either `symbol` or `tickers`. Optional fields include `public_id`, `timestamp`, `trade_count` and `vwap`. A `date` column containing YYYY-MM-DD labels is accepted as an alias for `session`.

If `public_id` is absent in a simple input, identity is `local:<security-directory-name>`. Use a real stable `public_id` when directories may be renamed or when tickers change. Without a manifest, payload files and their hashes are still recorded, but there are no supplied checksums to compare against.

Every view must contain the same session keys and dated symbols. Legacy CSV session rows are sorted before conversion, and duplicates are rejected. Current completed exports supply one accepted ordered session grid shared by all views; the adapter does not sort or validate that grid again. CSV headers and record widths are validated before numeric parsing. Empty/nonfinite/nonpositive prices, negative volume, and invalid OHLC geometry are rejected. VWAP may be zero; it is used as an additional inference constraint when available.

`session` supplies the output date. When `timestamp` exists, this adapter expects a daily timestamp at New York midnight that agrees with `session`, such as `2016-01-04T05:00:00Z` or a summer `04:00:00Z` value. Intraday or otherwise timestamped exports need an explicit adapter to produce daily session labels and should not be passed through silently.

## 4. Split-factor evidence

### Default: inference from source adjustment views

```bash
python alpaca_to_bloodhound.py --data-in data --blood-out output.parquet --split-source infer
```

The `infer` flag is optional because it is the default. This is the usable path for the supplied archive, which contains no corporate-action ledger.

The solver jointly models positive `S`, `D` and `O`, representing split, dividend and spin-off multipliers. The adjusted views constrain `S`, `D`, `O`, `S*D`, `S*O`, `D*O` and `S*D*O`. It uses these model assumptions from the notes:

| Observation | Absolute allowance |
| --- | ---: |
| Adjusted price | 0.01 |
| Raw price | 0.000001 |
| Split-adjusted volume | 1 share |

For corresponding adjusted and raw prices `Pj` and `Pr`, each view supplies a lower bound `(Pj - 0.01) / (Pr + 0.000001)`, clipped below at zero, and an upper bound `(Pj + 0.01) / (Pr - 0.000001)` where that denominator is positive. Open, high, low, close and available VWAP bounds are intersected.

Positive raw volume additionally constrains `S >= Vraw / (Vadjusted + 1)` and, where `Vadjusted > 1`, `S <= Vraw / (Vadjusted - 1)`. The solver uses every available split-containing volume view. A zero raw volume cannot become adjusted volume greater than the allowance. Non-split views are separately checked for unchanged volume; trade counts, when present, must agree across adjustment views.

An analytical projection in log space establishes whether one shared `S,D,O` solution exists. Independently plausible intervals that cannot coexist are rejected. No SciPy solver is required at runtime.

The deterministic selection policy is:

1. Intersect consecutive per-session feasible split intervals into maximal consecutive compatible segments.
2. Prefer the ratio from the largest informative adjusted-volume observation within a segment.
3. If volume is uninformative, prefer factor one if it is feasible; otherwise use a direct split-price ratio as the anchor.
4. Restrict the anchor to the segment's feasible interval.
5. Within intervals no wider than 1%, optionally prefer the smallest feasible rational denominator, capped at 1,000.
6. Recheck a joint dividend/spin-off solution for every selected factor.

The report lists each segment's selected factor, lower/upper feasible bounds, dates, anchor, and relative interval width. Widths exceeding the 1% policy, or unbounded intervals, are marked `ambiguous`. `null` denotes a nonfinite endpoint in JSON. A narrow interval is still an inference; none of these bounds are probabilities or independent event verification.

All factors are calculated from the full held history before output date filters. These are retrospective provider-basis factors, not a point-in-time reconstruction of what data was known historically. The source manifests do not declare an absolute action horizon, so inference does not invent one or force the final factor to one.

This is a new implementation of the described method. The original source repository and its exact anchor/tie-breaking code were not attached, so bit-for-bit identity with that prior solver is not claimed.

### Optional: supplied events with declared complete coverage

```bash
python alpaca_to_bloodhound.py \
  --data-in /path/to/data \
  --blood-out action_based.parquet \
  --split-source actions \
  --split-events split_events.csv \
  --action-coverage action_coverage.csv \
  --horizon 2026-10-14
```

The required events CSV columns are:

```csv
public_id,event_id,effective_date,old_shares,new_shares,event_type,source
example-security,example-event,2024-01-04,1,3,forward_split,Your evidence reference
```

`event_type` and `source` are optional columns. If present, every event type must be `split`, `forward_split`, `reverse_split`, or `stock_dividend`. A stock dividend must change units of the same continuing security. Cash dividends, distributions of another security and unrelated ticker replacements do not belong in this table.

`old_shares=1,new_shares=3` means three new shares for one old share and a historical price multiplier of `1/3`. The application retains exact rational event products until the final conversion to float64:

```text
S(t) = product(old_shares / new_shares for events with t < effective_date <= H)
```

The effective session excludes its own backward adjustment. Repeated event IDs must agree on effective date and reduced ratio. Equivalent same-date/same-ratio records count once, even if they carry different event IDs. Distinct events on the same date with different ratios multiply; these cases are reported for inspection.

The required coverage CSV is:

```csv
public_id,start_date,end_date,source
example-security,2016-01-04,2026-10-14,Your complete dated ledger reference
```

Each security needs a nonempty evidence reference and one interval covering its first held source session through the requested horizon. The horizon must be at least the last held source session. Missing events are allowed to imply factor one only when the security has this explicit coverage declaration. Separate partial intervals are not silently combined into a completeness claim.

The computed factors must also satisfy the source views' joint numerical model. The action horizon and the source adjustment basis therefore need to agree. A mismatch is rejected and should be investigated; the application does not rescale one basis into the other implicitly. Action files may include additional securities or events outside the applicable date range; factors use only the requested identity and the formula's event interval.

The output declares `action_records_with_declared_coverage`. This identifies the supplied evidence and verifies its internal rules; it does not independently authenticate your ledger or certify its completeness.

## 5. Dates, identifiers and metadata

Output dates are timezone-free `timestamp[ns]` values at midnight, derived from the session label. They do not represent actual midnight trades. The application validates date syntax, timestamp/session agreement when available, uniqueness and order. It does not verify membership in a venue-specific trading calendar or claim complete session coverage for each security.

`ohlcv_series_id` is a deterministic positive signed 64-bit integer derived from SHA-256 of `public_id`. This is stable when a dataset is extended, subsetted or reordered, provided the public identity remains the same. Collisions within the input set are rejected. The full public identity and numeric mapping are recorded in the JSON report. These are local application IDs, not a universal security identifier.

The output uses row-level dated tickers, including historic aliases such as FB, rather than applying each manifest's latest symbol to the whole history.

The supplied manifests have no dated CIK, FIGI, security-type or listing-exchange data. A current snapshot company name is not promoted into historical identity metadata. By default:

| Field | Missing-value representation |
| --- | --- |
| `cik`, `composite_figi`, `share_class_figi`, `company_name` | Null |
| `security_type` | `unknown` |
| `exchange` | `unknown` |

The notes did not provide the downstream allowed enumeration for the two required unknown strings. Configure these explicitly if your Bloodhound installation uses different sentinels:

```bash
python alpaca_to_bloodhound.py \
  --data-in data --blood-out output.parquet \
  --unknown-security-type YOUR_UNKNOWN_TYPE \
  --unknown-exchange YOUR_UNKNOWN_MIC
```

To supply observed historical identity/listing data, use `--metadata metadata.csv`. Required columns are `public_id,start_date,end_date`; optional columns are `symbol,cik,composite_figi,share_class_figi,company_name,security_type,exchange`. Date intervals are inclusive and must not overlap within a security. Nonempty values override defaults only on their stated interval; empty cells leave defaults in place. Supply listing MICs and security types using your downstream contract. Metadata records are keyed by stable identity, not just ticker.

## 6. Output schema

Every column is present in this order. Only the four indicated metadata fields are nullable.

| # | Column | Arrow type | Nullable |
| ---: | --- | --- | --- |
| 1 | `date` | `timestamp[ns]`, no timezone | No |
| 2 | `ohlcv_series_id` | `int64` | No |
| 3 | `symbol` | `string` | No |
| 4 | `cik` | `string` | Yes |
| 5 | `composite_figi` | `string` | Yes |
| 6 | `share_class_figi` | `string` | Yes |
| 7 | `company_name` | `string` | Yes |
| 8 | `security_type` | `string` | No |
| 9 | `exchange` | `string` | No |
| 10 | `adjusted_open` | `float64` | No |
| 11 | `adjusted_high` | `float64` | No |
| 12 | `adjusted_low` | `float64` | No |
| 13 | `adjusted_close` | `float64` | No |
| 14 | `raw_close` | `float64` | No |
| 15 | `volume` | `float64` | No |
| 16 | `dollar_volume` | `float64` | No |
| 17 | `adjustment_ratio` | `float64` | No |
| 18 | `split_adjustment_ratio` | `float64` | No |

Rows are ordered by series ID and then session. The writer processes one security at a time and writes incremental Parquet row groups. The default compression is Zstandard. Parquet version 2.6 preserves nanosecond timestamp types.

This implements the 18-column interface provided in the attachments. The separate, full Bloodhound application, its permitted unknown enums, and any catalog-binding/footer requirements were not supplied. Additional integration-specific metadata can therefore require a downstream adapter; the application does not invent that missing contract.

## 7. Options, failures and verification

```bash
python alpaca_to_bloodhound.py --help
```

| Option | Purpose |
| --- | --- |
| `--data-in DIRECTORY` | Completed current CSV run or legacy source directory |
| `--public-id ID` | Select a current-export identity; repeatable |
| `--blood-out FILE.parquet` | Required destination |
| `--split-source infer\|actions` | Choose factor evidence; default `infer` |
| `--split-events FILE.csv` | Event ledger for action mode |
| `--action-coverage FILE.csv` | Completeness declarations for action mode |
| `--horizon YYYY-MM-DD` | Action normalization horizon |
| `--price-basis all\|split` | Price output semantics; default `all` |
| `--metadata FILE.csv` | Dated identity/listing overrides |
| `--unknown-security-type STRING` | Required unknown-type sentinel |
| `--unknown-exchange STRING` | Required unknown-MIC sentinel |
| `--start-date YYYY-MM-DD` | Inclusive output start filter |
| `--end-date YYYY-MM-DD` | Inclusive output end filter |
| `--on-error fail\|skip` | Default strict failure or explicit whole-history omission |
| `--compression zstd\|snappy\|gzip\|none` | Parquet compression |
| `--overwrite` | Permit replacing an existing output/report pair |
| `--quiet` | Suppress progress messages |
| `--version` | Application version |

Exit status `0` means a requested export succeeded. With explicit `--on-error skip`, this can be a partial export: check the final summary and `report.status == "partial"`. Exit status `2` means an input, configuration or output failure.

Strict mode scans histories, records rejections, and publishes no new Parquet if any history fails. A per-history rejection preserves an existing Parquet and report; the failed attempt is recorded in `FILE.parquet.failure.json`. Publication failures have the limitations listed below. If there is no prior Parquet, its failure report uses the usual report name. Discovery/configuration errors may occur before a per-history report can be produced. `skip` applies to identified histories; an unreadable or ambiguous overall directory/manifest configuration still stops the run.

Before writing, normal output, report and failure-report paths are checked against the selected input files,
including metadata/action inputs and filesystem aliases. A collision is rejected even with `--overwrite`.
JSON publication uses atomic replacement without forced filesystem synchronization.

Parquet is staged and reread before publication. The application checks exact types/nullability, finite numerics, midnight session dates, positive series IDs, unique sorted keys and both derived equations using the actual serialized float64 values. Source consistency and factor feasibility are checked before staging. Both the Parquet and its JSON report are prepared before commit. Each file replacement is atomic, and ordinary commit failures restore the prior pair. Filesystems do not offer a two-file atomic rename transaction: a process or power failure in the short commit interval may require recovery from retained backup files. Run only one writer for a destination at a time.

### Run the synthetic example

The bundled example is explicitly synthetic: three daily observations with a 3-for-1 split on 2024-01-04, individually rounded prices, fractional volume and a dated ticker change.

```bash
python alpaca_to_bloodhound.py \
  --data-in examples/demo \
  --blood-out demo.parquet
```

To exercise action-derived factors and dated metadata:

```bash
python alpaca_to_bloodhound.py \
  --data-in examples/demo \
  --blood-out demo_actions.parquet \
  --split-source actions \
  --split-events examples/split_events.csv \
  --action-coverage examples/action_coverage.csv \
  --horizon 2024-01-04 \
  --metadata examples/metadata.csv
```

### Run the regression suite

```bash
python -m pip install -r requirements-dev.txt
python -m unittest discover -s tests -v
```

SciPy is used only by an independent mathematical test oracle. The normal application needs NumPy, pandas and PyArrow. Without SciPy, the oracle test is skipped, while the remaining tests still run. The oracle compares 1,000 generated feasible/infeasible interval systems, including unbounded cases, against the analytical projection. Other tests target the corrected mapping, fractional volume, source aliases, effective-date rules, evidence coverage, date filtering, malformed inputs and output integrity. The tests use synthetic inputs and do not establish production throughput or complete downstream integration.

## 8. Known limitations and scope

The completed current CSV manifest selects membership. Its selected digest and CSV are each read once;
upstream checksums are provenance and are not recalculated. The app does not open the live pricing store
or acquire provider data. The regressions in [tests/test_current_csv.py](tests/test_current_csv.py) reject
directory crawling, unselected reads, repeated boundary reads and upstream checksum recalculation.

The retained audit establishes these unresolved limitations of the packaged app:

- Output and report are replaced separately. Concurrent writers or a process interruption between replacements
  can leave a mismatched pair. There is no destination lock or automatic journal recovery.
- The initial `--overwrite` check does not prevent a file created later by another writer from being replaced.
- Legacy timestamp validation can accept a non-midnight nanosecond component.
- Backup cleanup can report failure after the new output and report have committed successfully.

Use a single writer per destination, keep output and report paths separate from inputs, and inspect interrupted
publications before consuming them. These constraints do not repair the underlying defects. The engine's
publication guarantees must not be assumed for this standalone app. The existing thirty engine findings in the
repository review do not include these separate converter limitations.

Split factors inferred from rounded provider views are model results, not independently verified corporate
actions. Action mode checks supplied records and declared coverage without independently certifying the ledger.
The full downstream Bloodhound application, its allowed unknown-value enums and any private footer/catalogue
requirements are outside this app's tested 18-column interface. Trading-calendar completeness is not asserted.
