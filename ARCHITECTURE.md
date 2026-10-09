# Architecture

## Flows

Update orchestration is defined in
[`libs/alpaca_end_of_day_ticker_pipeline/orchestration.py`](libs/alpaca_end_of_day_ticker_pipeline/orchestration.py):

```text
Alpaca bars or authenticated cache
  -> frozen download publication and integrity validation
  -> Alpaca asset and corporate-action evidence, including explicit asset-lookup HTTP outcomes
  -> symbol disposition (process, retain, just_skip, out_of_scope, quarantine, exclude, or investigate)
  -> exhaustive correlated / uncorrelated row partition
  -> warning and information overlays
  -> completed-data online evaluation
  -> digest-bound global master-ID index
```

Reading is independent and read-only:

```text
master ID + adjustment type
  -> authenticate master index and resolve bound ticker sources
  -> authenticate correlation, warnings, validation, and frozen downloads
  -> return identity metadata, unchanged bars, errors, warnings, and provenance
```

Consumer availability is a smaller public boundary: the selected authenticated master-ID shard contains the boolean
and compact classification computed by the completed ticker pipeline. `online: true` means published identity plus
bars plus current authenticated dependencies plus successful public-reader results for every advertised adjustment
with zero unresolved boundaries. The final check therefore includes conflicts between ticker sources and expected
market sessions absent inside the returned history. A clean readable history with such a gap is offline and classified
`lineage_gap`; it remains readable for investigation. The API exposes none of the provider, request-rate, or
storage-layout mechanics used to produce that state.

## Ownership

| Boundary | Owner | Responsibility |
|---|---|---|
| Bar request | [`libs/download_alpaca_end_of_day_bars/`](libs/download_alpaca_end_of_day_bars/) | Request construction, credentials, rate limits, download-stage decisions |
| Frozen source and hard integrity | [`libs/alpaca_end_of_day_pipeline/`](libs/alpaca_end_of_day_pipeline/) | Direct and empirical-alias gzip/sidecar publication, authenticated hard-link compaction, cache state, deterministic validation, integrity manifests |
| Identity evidence | [`libs/alpaca_security_lineage/`](libs/alpaca_security_lineage/) | Read-only Alpaca asset and corporate-action traversal |
| Correlation and master identity | [`libs/alpaca_end_of_day_security_correlation/`](libs/alpaca_end_of_day_security_correlation/) | Row partition, Alpaca-ID membership, master-ID derivation, master-index publication |
| Advisory checks | [`libs/alpaca_end_of_day_warning_pipeline/`](libs/alpaca_end_of_day_warning_pipeline/) | Cross-adjustment and master-ID-scoped warning overlays |
| Public read | [`libs/alpaca_end_of_day_reader/`](libs/alpaca_end_of_day_reader/) | Master lookup, dependency authentication, in-memory result, stdout adapter |
| Shared primitives | [`libs/common/`](libs/common/) | Atomic writes, hashing, rule evaluation, tracked market calendar |

Root Python files are entry points or compatibility facades; domain logic belongs in the owning library.

## Invariants

- Frozen CSV bytes are never silently repaired or rewritten. Storage compaction may atomically replace a pathname with
  a byte-identical authenticated hard link, while its manifest records that storage relationship.
- An inferred adjustment view is always labeled `empirical_adjustment_alias`; it never claims to be a direct Alpaca
  response. Its manifest binds the policy, observed direct downloads, source manifest, and shared inode.
- Every source row appears exactly once in either the correlated or uncorrelated branch.
- A master ID is derived only from its sorted unique Alpaca-ID membership; evidence determines membership but is not
  part of the digest input.
- A UUID-less Alpaca symbol series may have a stable `market_data_series_id`, but that identifier is explicitly not a
  security identity and never enters the master index. This retains historical data without conflating ticker reuse.
- Recurring batch skips fail closed: orchestration skips only a disposition bound to an authenticated complete
  correlation partition. Explicit single-symbol runs always re-evaluate the symbol.
- Most master IDs contain one Alpaca ID. Explicit lineage may join more; the terminal fallback may join exactly two
  same-ticker candidates only when the complete history was otherwise uncorrelated and has no expected-session gap.
- `data/end_of_day/json_manifests/security_master.index.json` is the reader's small discovery root. It binds
  SHA-256-prefix inventories of immutable per-symbol source objects and per-master-ID aggregate objects, never bars,
  diagnostics, or repair state. A single-ticker update replaces only the applicable objects and inventory buckets.
- Every current per-master-ID object stores one `online` boolean and `availability_status`, aggregated from its
  contributing source records. Online means zero unresolved boundaries; `lineage_gap` is offline even when the reader
  otherwise succeeds. Repository availability is independent of Alpaca's active/inactive metadata. The public API
  reads the stored fields and does not create another monolithic publication.
- Integrity errors preserve and describe affected rows. Warnings are advisory. Neither changes returned values.
- Outputs are written before their digest-binding receipts. Readers fail closed on missing, stale, or changed inputs.
- No pricing-repair layer exists; `repair.overlay` and `repair.repaired_data` remain `null`.

## Dependency rule

Interpretation flows downstream only. A layer may authenticate and describe an upstream artifact, but may not weaken
or mutate that boundary. `libs/common/` imports no application package.
