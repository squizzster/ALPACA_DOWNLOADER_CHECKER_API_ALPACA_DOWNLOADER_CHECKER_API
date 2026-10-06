# Architecture

## Flows

Update orchestration is defined in
[`libs/alpaca_end_of_day_ticker_pipeline/orchestration.py`](libs/alpaca_end_of_day_ticker_pipeline/orchestration.py):

```text
Alpaca bars or authenticated cache
  -> frozen download publication and integrity validation
  -> Alpaca asset and corporate-action evidence
  -> exhaustive correlated / uncorrelated row partition
  -> digest-bound global master-ID index
  -> warning and information overlays
```

Reading is independent and read-only:

```text
master ID + adjustment type
  -> authenticate master index and resolve bound ticker sources
  -> authenticate correlation, warnings, validation, and frozen downloads
  -> return identity metadata, unchanged bars, errors, warnings, and provenance
```

## Ownership

| Boundary | Owner | Responsibility |
|---|---|---|
| Bar request | [`libs/download_alpaca_end_of_day_bars/`](libs/download_alpaca_end_of_day_bars/) | Request construction, credentials, rate limits, download-stage decisions |
| Frozen source and hard integrity | [`libs/alpaca_end_of_day_pipeline/`](libs/alpaca_end_of_day_pipeline/) | Gzip/sidecar publication, cache state, deterministic validation, integrity manifests |
| Identity evidence | [`libs/alpaca_security_lineage/`](libs/alpaca_security_lineage/) | Read-only Alpaca asset and corporate-action traversal |
| Correlation and master identity | [`libs/alpaca_end_of_day_security_correlation/`](libs/alpaca_end_of_day_security_correlation/) | Row partition, Alpaca-ID membership, master-ID derivation, master-index publication |
| Advisory checks | [`libs/alpaca_end_of_day_warning_pipeline/`](libs/alpaca_end_of_day_warning_pipeline/) | Cross-adjustment and master-ID-scoped warning overlays |
| Public read | [`libs/alpaca_end_of_day_reader/`](libs/alpaca_end_of_day_reader/) | Master lookup, dependency authentication, in-memory result, stdout adapter |
| Shared primitives | [`libs/common/`](libs/common/) | Atomic writes, hashing, rule evaluation, tracked market calendar |

Root Python files are entry points or compatibility facades; domain logic belongs in the owning library.

## Invariants

- Frozen CSV bytes are never silently repaired or rewritten.
- Every source row appears exactly once in either the correlated or uncorrelated branch.
- A master ID is derived only from its sorted unique Alpaca-ID membership; evidence determines membership but is not
  part of the digest input.
- Most master IDs contain one Alpaca ID. Explicit lineage may join more; the terminal fallback may join exactly two
  same-ticker candidates only when the complete history was otherwise uncorrelated and has no expected-session gap.
- `security_master.index.json` is the reader's only discovery boundary. It contains identity metadata and source
  digests, never bars, diagnostics, or repair state.
- Integrity errors preserve and describe affected rows. Warnings are advisory. Neither changes returned values.
- Outputs are written before their digest-binding receipts. Readers fail closed on missing, stale, or changed inputs.
- No pricing-repair layer exists; `repair.overlay` and `repair.repaired_data` remain `null`.

## Dependency rule

Interpretation flows downstream only. A layer may authenticate and describe an upstream artifact, but may not weaken
or mutate that boundary. `libs/common/` imports no application package.
