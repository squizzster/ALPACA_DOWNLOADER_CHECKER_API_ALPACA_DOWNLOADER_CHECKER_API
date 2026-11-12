# Public API

Import only [alpaca_end_of_day](../alpaca_end_of_day/__init__.py). Readers are local/read-only;
missing, incomplete, pending or incompatible publications fail without acquisition, repair, discovery or store substitution.

| Reader | Scope |
|---|---|
| `StandaloneReader(store)` | Resolved group histories/evidence, even with other unresolved rows. |
| `DataLibrary(store=...)` | Published online identities; default repository `data_pricing/`. |

`DataLibrary` requires keyword store selection; positional roots and `path=` raise migration errors.
[Bootstrap serving](operations.md#serving-catalogue-maintenance) before opening it.

## Common semantics

Tickers are dated labels. Resolution preserves multiple matches; date filters never silently select an identity.
A 64-hex `public_id` is stable for fixed resolved provider-UUID membership. Membership changes can change the ID;
no old-ID crosswalk is promised.

Discover candidates through ticker resolution/search, then retain `public_id` for finalized-data history,
exports, joins and cache keys. Do not persist ticker identity keys or re-resolve on every read: one label may
name several securities over time; one identity may retain several dated labels. If accepted UUID membership
changes, the old ID becomes unavailable; select explicitly from the new catalogue.

Dates: `datetime.date`, ISO date string or `None`; inclusive New York session bounds. Valid empty ranges return
empty histories. [ADJUSTMENTS](../libs/alpaca_pricing_download/contract.py) defines eight names; `all` means fully
adjusted, not every view. Numbers remain exact.

## Standalone reader

```python
from alpaca_end_of_day import StandaloneReader

reader = StandaloneReader("data_pricing")
for match in reader.resolve_ticker("DELL")["matches"]:
    views = reader.get_histories("DELL", ["raw", "all"], public_id=match["public_id"])
    print(match["public_id"], views["views"]["all"]["bar_count"])
```

Symbol locates the source group and remains required with `public_id`. Multiple matches without an ID raise
`AmbiguousTickerError`. [StandaloneReader](../alpaca_end_of_day/standalone.py) owns signatures/dictionary schemas:
`get_history` returns one view with exact numeric strings; `get_histories` returns `views[adjustment]` (default
all eight) from one snapshot, all succeeding or failing. `include_evidence=True` includes evidence.

`resolve_ticker`/`get_generation` need no price reads. `list_published_sources` returns possibly stale/unresolved
locators, not an online catalogue. `get_evidence` exposes published identities, assignments and normalized
evidence. Its `security_relations` field carries the completed distinct-security conversion projection,
including external endpoints with null provider/public IDs and separate legal/trading labels. A relation does
not merge identities or imply online availability. Legacy receipts expose null, meaning no assertion was
published. The same projection appears in group evidence, included history evidence and published evidence.
Share one [GroupSnapshot](../alpaca_end_of_day/_standalone_group.py) across IDs:

```python
with reader.open_group("DELL") as group:
    for security in group.summary["securities"]:
        views = group.get_histories(public_id=security["public_id"])
group.check_current()  # Optional freshness check before downstream publication.
```

Summary, history and evidence are independent nested projections: mutation cannot change later group reads or
captured metadata. Separate calls may select different generations. Busy groups raise `ReaderBusyError`, or wait
with `wait=True`; stale/incomplete/incompatible publications raise `StandaloneReaderError`.

`cache_bytes` defaults to zero. `cache_info()` reports bounded RAM retention; group `set_cache_limit` stays
within that cap, not a total-process limit. Caching retains locks, current-selection/pending checks and trust in
published digests. Readers consume [producer-selected observations](lineage.md#observation-selection), preserving
accepted dated labels. Competing producer-selected observations require generation-bound owning lineage recovery;
readers never choose between them by numerical equality, ticker preference or source coverage. Assembly precedes filtering, so narrow
queries may read older payloads. Authority: [standalone.py](../alpaca_end_of_day/standalone.py), `_history`.

Reader CLI defaults to `raw`; `--all-adjustments` selects eight. Ambiguity needs symbol plus `--public-id`.
Compound adjustment names are single arguments; `raw,all` is invalid. Multiple-view CSV adds `adjustment`;
output is prepared before printing. [Commands](operations.md#read-and-export).

## Published lineage metadata

```python
from alpaca_end_of_day import StandaloneReader

reader = StandaloneReader("data_pricing")
with reader.open_published_evidence() as selected:
    roster = selected.roster  # Index-only plan: sources, generations and group count.
    for group in selected.iter_groups():
        print(group["securities"], group["sources"])
```

This metadata-only boundary consumes completed lineage publications, including
historical resolved identities. It selects membership and recorded generations
from the lineage index once, after its own schema/completeness checks. Missing
membership/projection coverage fails into explicit `lineage migrate-index`;
there is no receipt census or fallback reconstruction.

One shared lineage publication gate keeps the accepted view stable throughout
the context; lineage publication waits until it closes. Each selected receipt and
assignment document is read once when its body is consumed. The iterator retains
one group's bodies, not a universe of manifests. It does not consult pricing
manifests, locks, notices or payloads, rehash upstream data, replay rules or perform
a later freshness pass. The context releases its gate on failure/interruption.

`roster` is an independent projection. A closed snapshot cannot be consumed.
Group `generation` uses its first source's recorded receipt generation as a
reference; the roster's generation describes the recorded index revision. Prices
may have advanced since lineage publication: this API returns the accepted
lineage evidence, not a claim of current pricing or online availability. Group
actions remain unassigned evidence; observation dates keep their original meaning.
[_published_evidence.py](../alpaca_end_of_day/_published_evidence.py) owns this path.

## Online catalogue reader

```python
from alpaca_end_of_day import DataLibrary

data = DataLibrary(store="data_pricing")
for security in data.search_securities("DELL", limit=5):
    history = data.get_history(security.public_id, "all")
    print(security.public_id, len(history.bars))
```

[DataLibrary and public types](../alpaca_end_of_day/__init__.py) own catalogue/list/get/search/dated-resolution,
history and export contracts. `get_histories` locks once, returning adjustment → typed `History`, default eight
views. OHLC/VWAP/volume are `Decimal`, trade count integral; `to_dict()` emits exact decimal strings.
`wait`/`cache_bytes` match standalone semantics.

Metadata separates known/conflicting observations from supported effective intervals. Undated exact fallbacks
stay undated; catalogue time dates only catalogue-sourced lookups. Ownership can support historical labels/CUSIPs
with unknown endpoints; observed names/exchanges/classes do not establish historical periods. Resolution ranges
describe labels on stored histories; undated names remain searchable. `resolve_ticker` trims/ignores case and
accepts a leading letter or digit followed by letters, digits, periods or hyphens, including historical numeric
labels. FIGIs are not supplied/fabricated. Provider IDs, internal status/reasons and diagnostic paths stay hidden.
[Eligibility](technical-contracts.md#serving-catalogue) bounds approved-session/explained-gap coverage.

Instances retain an opened catalogue snapshot without a long-lived transaction. Search/list/resolve do no ticker
I/O. History checks the selected serving decision and lineage/pricing generations under shared locks. Changed
selected inputs raise `CatalogChangedError`: finish maintenance, then reopen. Unrelated updates remain readable;
catalogues retain no old pricing files. `PublicIdUnavailableError` conceals whether an ID is unknown or internally
offline. Both inherit `DataLibraryError`; malformed arguments raise `ValueError`, busy groups `ReaderBusyError`.

[Exports](exports.md) owns flat/per-identity CSV contracts, snapshot scope and output publication.

## Pipeline execution

`prepare_pipeline(request_json: bytes) -> bytes` performs daily-update preparation
through the [Phase 1 JSON contract](development.md#pipeline-phase-1-prepare).
It is a writer entrypoint, separate from the read-only reader APIs. It acquires the
store operation gate, uses existing provider credential configuration and propagates
`PipelineStageError` on execution failure. Successful output is the complete transient argument for the
next owner; callers must not treat it as completion of the daily update.

`produce_histories_pipeline(prepared_json: bytes) -> bytes` consumes that output and
finishes pricing, dependency closure and lineage through the
[Phase 2 JSON contract](development.md#pipeline-phase-2-history-production).
It is a writer with the same credential and error behavior. It returns a transient
serving handoff, including explicit independent failures. It does not assess or
publish the serving catalogue. Both calls use actual UTF-8 JSON bytes, not privileged
Python objects. Normal calls retain no intermediate archive; DEBUG records the same
arguments/results outside the production store.

`serve_pipeline(histories_json: bytes) -> bytes` consumes the complete history
argument and returns the final compact operation report after catalogue publication
and acknowledgement. It uses no provider client.
`run_pipeline(request_json: bytes) -> bytes` sequences all three JSON stages under
one continuous operation lock. The [Phase 3 contract](development.md#pipeline-phase-3-complete-pipeline)
owns controller locking, independent execution and failure semantics.

## Lineage diagnostics

`debug_lineage(request_json: bytes) -> bytes` and `lineage_debug_cli(argv=None) -> int` expose the
[standalone lineage rule tool](lineage-debug.md). The diagnostic never publishes production data or
requests provider evidence. `LineageDebugError` reports invalid or unavailable required input.

```python
import json
from alpaca_end_of_day import debug_lineage

report = json.loads(
    debug_lineage(
        json.dumps({"schema": "alpaca.lineage.debug.request", "schema_version": 1, "operation": "rules"}).encode(
            "utf-8"
        )
    )
)
```

The request/result schemas are `alpaca.lineage.debug.request` / `alpaca.lineage.debug.report`, version 1.
Operations: `init`; `rules` (`policy` optional); `explain` (`store`, `symbol`, optional `session`,
`adjustment`, `max_segments`, `include_metadata_handoff`); `run` (`store`, optional `policy`, either `symbols` or `all: true`, optional
`max_differences`, `timeout_seconds`, `control_policy`, `screen_report`, `reuse_report`, `metadata_handoff`);
`screen` (`store`, `policy`, `control_policy`); `export` (`manifest`, optional `symbols`, `multiple_only`);
`handoff` (`input`, optional `symbols`). Paths are strings; symbol collections are JSON lists.
The guide owns their semantics, limits and CLI examples. An explicit research replay returns independent
failures and completed peer reports; inspect `execution_complete` separately from comparison `parity`.
