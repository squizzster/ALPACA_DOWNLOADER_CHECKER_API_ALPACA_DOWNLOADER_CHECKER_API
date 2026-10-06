# Architecture

## Objective

The pipeline preserves a frozen, SHA-authenticated serialization of the rows returned by Alpaca and adds
independently authenticated overlays. A future reader can therefore trace a returned value from the frozen download
SHA through integrity, identity correlation, and later decision layers. The reader and repair-decision layers do not
yet exist.

Each layer owns one boundary. A downstream layer may authenticate and interpret an upstream artifact, but it may
not repair, replace, or weaken that artifact's contract.

## Execution flow

The complete ticker pipeline is ordered by
[`run_end_of_day_ticker_pipeline`](libs/alpaca_end_of_day_ticker_pipeline/orchestration.py):

```text
Alpaca bars or authenticated cache
    -> download publication and deterministic integrity validation
    -> direct Alpaca identity evidence
    -> exhaustive correlated / uncorrelated row partition
    -> cross-adjustment and UUID-scoped warning overlays
```

`run_alpaca_end_of_day_ticker_pipeline.py` is the explicit full-pipeline entry point.
`download_alpaca_end_of_day_bars.py` is a compatibility entry point for the same full pipeline despite its older
name. The lineage and correlation entry points expose narrower stages; there is no standalone warning CLI.

Recorded integrity failures do not prevent correlation and warning publication for that ticker. The multi-ticker
command processes every selected ticker before returning a final integrity-failure exit status. Authentication,
correlation, or publication errors still abort because their boundary could not be established.

## Owned boundaries

### Frozen download

`libs/alpaca_end_of_day_pipeline/download_publication.py` alone publishes a compressed CSV and its sidecar. The
pair represents the unchanged Alpaca response and is immutable except through an explicitly authorized replacement
or orphan-pair repair.

`libs/alpaca_end_of_day_pipeline/symbol_manifests.py` separately owns processing receipts and action-required
integrity failures. A clean receipt is inferred from trusted processing plus the absence of a matching failure; no
success field is stored.

### Identity evidence

`libs/alpaca_security_lineage/` reads local metadata and queries Alpaca asset and corporate-action endpoints. It is
read-only: its public API returns an in-memory report and its CLI writes JSON only to stdout. It does not authenticate
the gzip or assign UUIDs to rows.

### Security correlation

`libs/alpaca_end_of_day_security_correlation/` authenticates frozen downloads and owns an exhaustive row partition.
Every source row appears exactly once in either the correlated or uncorrelated branch. Uncorrelated rows and lineage
work items remain explicit pipeline work; they are not integrity failures and are never assigned by guesswork.

The two branch documents are published before their digest-binding receipt. A complete partition proves that every
row was accounted for, not that every row received a security identity.

### Warning overlays

`libs/alpaca_end_of_day_warning_pipeline/` authenticates the current validation and correlation generations before
publishing warning and information overlays. It owns no integrity-error classification. Cross-adjustment checks use
comparable request cohorts; statistical and session-coverage checks do not cross Alpaca UUID boundaries. Its two
outputs are published before their digest-binding receipt.

### Orchestration

`libs/alpaca_end_of_day_ticker_pipeline/` owns stage order, shared rate limiters, and aggregate command reporting. It
does not own data artifacts. `libs/download_alpaca_end_of_day_bars/` owns bar-request construction, credential timing,
and download-stage decisions, but delegates every write to the pipeline library.

## Dependency direction

Dependencies point inward and do not cycle:

```text
root entry points and compatibility facades
    -> alpaca_end_of_day_ticker_pipeline
        -> download_alpaca_end_of_day_bars -> alpaca_end_of_day_pipeline -> common
        -> alpaca_security_lineage
        -> alpaca_end_of_day_security_correlation
            -> alpaca_security_lineage
            -> alpaca_end_of_day_pipeline (read and authenticate)
            -> common
        -> alpaca_end_of_day_warning_pipeline
            -> alpaca_end_of_day_security_correlation (read and authenticate)
            -> alpaca_end_of_day_pipeline (read and authenticate)
            -> common
```

`libs/common/` owns reusable atomic writes, SHA-256, rule evaluators, and the tracked US-session calendar. It imports
no application package. Root Python files remain stable commands or compatibility facades and contain no domain
implementation.

## Naming and certainty

Use the smallest name and nearby context that remove material ambiguity. Keep origin, scope, and certainty visible,
especially in serialized artifacts that outlive the code that produced them. Terms such as `observed`, `inferred`,
`verified`, and `unresolved` must remain materially distinct; a local guard must not compensate for ambiguous
ownership.
