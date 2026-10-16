# Technical contracts

This document owns the durable data, trust, identity, publication, and isolation contracts. Consumer types and
methods belong in [Public API](public-api.md); the executable cascade belongs in [Pipeline](../PIPELINE.md); commands
and operational hazards belong in [Operations](operations.md).

The existing combined pipeline and `DataLibrary` use `data_vtest/`, without fallback to `data/end_of_day/`.
The independent downloader uses the [standalone pricing-source contract](#standalone-pricing-source-contract) and an
explicit destination, currently `data_pricing/` for standalone development. Some internal Python paths, command help strings,
and serialized boundary identifiers still carry an older release label. Those strings are literal compatibility
facts, not a second pipeline or an architectural version choice.

## Boundary laws

Every durable stage has one calculation owner. The following generation and trust rules describe the existing
combined catalogue cascade; the standalone pricing and pending-update contracts are specified separately below:

1. **One owner computes a fact.** A downstream stage consumes the published fact; it does not reproduce the upstream
   calculation.
2. **Only immediate inputs are visible.** Each receipt names the generations it consumed. A stage does not reach
   through an input boundary to reinterpret an earlier producer's domain.
3. **Identical input generations retain completed work.** If the complete immediate-input identity still matches, the
   published downstream generation is selected instead of recalculated. Recalculation is permitted only when an
   owning input generation changes or when explicit acquisition is itself the boundary's job.
4. **Changed work is bounded by ownership.** Monthly source changes are local to selected month partitions;
   validation may also republish the immediate successor seam. Identity changes expand to the complete affected
   identity closure; catalogue publication replaces only affected content-addressed objects and inventory buckets.
5. **A consumer trusts a completed producer boundary.** Ordinary `load_*` paths check selection identity and required
   shape but deliberately do not re-hash payloads, authenticate every output byte, or rerun rules.
6. **Authentication is explicit.** `authenticate_*` paths recalculate digests and structural invariants for audit or
   publication validation. They are not hidden inside normal reads.
7. **Receipts are published last.** Payloads and reports are staged, written, and validated before the selecting
   receipt becomes visible. The final catalogue receipt is the reader-visible publication point.
8. **Failure is fail-closed.** A retained publication marker blocks readers after an interrupted write transaction.
   Ambiguous identity evidence remains uncorrelated or offline; it is never resolved by guessing.

“Trusted” therefore means “selected from a completed publication transaction whose producer validated it,” not
“cryptographically re-audited on every read.” Repeating upstream validation in every consumer would collapse the
boundaries, duplicate calculation, and make downstream behavior depend on work it does not own.

The focused update command still queries a selected existing symbol's eight views and reacquires selected lineage
before semantic equality can be established. Its no-repetition guarantee begins once complete immediate-input
identity is known. The clean-room scheduler has a stronger recovery contract: it records resolution first, calculates
each canonical source once, explicitly authenticates already-completed source/validation/lineage boundaries after an
interruption, and resumes after them without provider reacquisition. A later canonical source can invalidate a prior
correlation result in their shared identity closure; that result is no longer the same calculation because its required
input set changed. Only that affected closure is recalculated.

Trusted consumption can still parse files, materialize rows, and project types. Authentication can re-hash and decode
every owned artifact. Those are trust operations, not a second owner repeating a domain decision.

## Trust operations

| Operation | Intended caller | What it does | What it deliberately does not do |
|---|---|---|---|
| `publish_*` | Owning producer | Calculates its output, validates the new output, then writes its receipt last. | Does not publish another boundary's decision. |
| `load_*` | Normal downstream producer or reader | Selects the already-published output and checks enough identity to bind it to the expected generation. | Does not hash every payload or repeat the producer's rules. |
| `authenticate_*` | Explicit audit or publication verification | Re-hashes owned files and checks the complete serialized contract for that boundary. | Does not repair or republish data. |

The public library uses the trusted `load_*` path. The clean-room constructor composes the individual authenticators,
availability validation inside catalogue authentication, exact membership checks, and a complete public-history pass
into its mandatory final audit. That audit is bound to the constructor's immutable run ledger; it is not a generic
audit-only command for an arbitrary restored store.
[`audit_alpaca_end_of_day_catalog.py`](../audit_alpaca_end_of_day_catalog.py) still targets the legacy
`data/end_of_day/` catalogue and is not an audit of `DataLibrary`'s current store.

## Boundary cascade

```text
submitted label
  -> authoritative-symbol resolution
  -> monthly pricing source
  -> monthly validation
  -> normalized lineage evidence

monthly source + validation + lineage
  -> exhaustive correlation partition and identity evidence

monthly source + validation + correlation
  -> advisory warnings

correlation + lineage
  -> private identity candidate

candidate + warnings + published histories + lineage evidence
  -> availability decisions

candidate + availability
  -> content-addressed catalogue engine
  -> reader-visible catalogue receipt
  -> online-only public projection
```

This is a dependency graph, not permission for later stages to take ownership of earlier work. Availability may read a
published history to prove that it works, for example, but it cannot change a bar, correlation segment, or warning.

## Boundary registry

| Boundary | Version | Immediate inputs | Owned output |
|---|---:|---|---|
| Standalone pricing source | 6 | Exact ticker label, request range/asof, eight complete REST bar responses, and previously published pricing | Three compact pricing boundaries, source-bound endpoint validator, and trusted locked pending-date handoff. |
| Standalone lineage | 2 | Published compact pricing, pending dates, prior approved assignments, and normalized identity evidence | Twin chronological assignment boundaries and durable acknowledgement; one lineage worker. |
| Clean-room run ledger | 1 | Ordered universe/file digests, fixed request range, owning contract versions, per-label provider resolution, and canonical transaction outcomes | Immutable run identity, resolution and terminal outcome records, full-audit report, completion receipt, and optional activation receipt. |
| Monthly pricing source | 2 | One canonical symbol, selected replacement partitions, and eight Alpaca adjustment responses; request identity is bound downstream | Deterministic month payloads, exact-copy aliases, month receipts, and structural source inventory. |
| Monthly validation | 3 | One published source generation, the exclusive request end, validation transform v19, and the tracked market calendar | Per-month row/dataset findings, predecessor seams, compact ticker summary, and validation generation. |
| Lineage source | 2 | Source generation, validation generation, exact asset and corporate-action responses, and lineage report v4 | Normalized lineage evidence and its generation; raw HTTP bodies are not retained. |
| Correlation envelope | 2 | Source, validation, and lineage generations plus the installed correlation transform | Correlated and uncorrelated row partitions plus correlation-owned identity evidence. |
| Warning envelope | 2 | Source, validation, and correlation generations, warning transform version 6, warning configuration, and market calendar | Cross-adjustment and security-segment advisory findings. |
| Identity candidate | 2 | Published correlation partitions and lineage reports, plus the current catalogue base in affected mode | A restartable private snapshot or affected-closure aggregation with provisional offline state. |
| Availability | 1 | Candidate, warning set, published histories, and lineage evidence | Exactly one online/offline/lineage-gap decision for every candidate identity. |
| Catalogue publication | 2 | Candidate and availability generations, current retention state, and catalogue engine v5 | Incremental source/security objects, inventories, retention state, and the final catalogue receipt. |
| Public API | 1 | One completed reader-visible catalogue plus published history dependencies | Online-only typed identities, resolution, search, histories, and flat export. |

The standalone pricing, clean-room ledger, eight monthly boundary, and older transformation changelogs are runtime-validated arrays under
[`resources/`](../resources/). Array position is the version; entries never contain an explicit `version` key.

## Calculation ownership

| Owner | May calculate | Must not calculate or decide |
|---|---|---|
| Symbol resolution | The authoritative Alpaca symbol for a submitted label. | Bar validity, identity membership, or availability. |
| Pricing source | Physical month placement, deterministic gzip bytes, logical row digests, and byte-equality aliases. | Market-session validity, lineage, correlation, warnings, or online state. |
| Validation | Row validity, dataset ordering, session observations, and adjacent-month ordering seams. | Security identity, row ownership, warning policy, or availability. |
| Lineage | Normalized asset and corporate-action evidence. | Pricing-row allocation or UUID membership by itself. |
| Correlation | Dated ownership, exhaustive row partition, UUID-lineage edges, identity evidence issues, and security membership. | Online state or mutation of source values. |
| Warnings | Advisory numerical and cross-view findings. | Source repair, correlation, identity membership, or availability. |
| Identity candidate | Aggregate source contributions into a snapshot or closed affected set. | Re-read provider data, reroute rows, or decide online state. |
| Availability | Exercise advertised reads and classify the already-resolved identity. | Change membership, bars, warnings, or source provenance. |
| Catalogue publication | Persist decisions and replace affected content-addressed objects atomically. | Recalculate domain decisions. |
| Public projection | Filter `online is True`, type exact values, and enforce snapshot consistency. | Expose offline state, reinterpret evidence, repair data, or contact Alpaca. |

`last_run.json` is unlinked operational bookkeeping. It records the latest request and counts, contains no generation
identity, and is not an input to any boundary.

## Clean-room run contract

`clean_room_run/manifest.json` is immutable. Its `run_id` is the canonical digest of:

- schema/boundary identity and `provider_only_clean_room` mode;
- `2016-01-01T00:00:00Z` and one exclusive request end;
- the universe file bytes, ordered normalized labels, and exact count;
- all source-through-catalogue and transform versions consumed by the run; and
- the allowed-input policy forbidding another runtime store, migration, or repair from a prior generation.

The scheduler refuses a valid but pre-populated monthly store when no matching run manifest exists. This makes the
absence of fallback behavioral, not merely documentary.

`resolutions/<POSITION>.json` binds the universe position and submitted label to the complete normalized exact-lookup
response and its canonical digest. It is published before price work, so resume never repeats a completed resolution.

`outcomes/<POSITION>.json` has one of three terminal states:

- `published`: the canonical source passed validation and the complete cascade published;
- `rejected_validation`: source and failed validation are authenticated but deliberately have no lineage,
  correlation, warning, or catalogue membership; or
- `canonical_reused`: this label resolved to a canonical symbol already owned by one earlier terminal outcome and
  binds that owner's position, state, and outcome digest.

Only the first two states may own a canonical symbol, and exactly one owner is permitted. Attempt records are
diagnostics, not terminal outcomes. Exhausting bounded retries stops the scheduler and leaves the universe position
unaccounted until a successful resume.

The final audit requires exact ledger and directory closure. Rejected owners must exist only in source and validation;
published owners must exist in every per-symbol boundary and exactly equal current catalogue source membership. It
authenticates all owned artifacts and then reads every advertised adjustment for every online identity through
`DataLibrary`. `audit.json` records counts and the selected public generation. `completion.json` binds the audit bytes;
its presence is the only activation authority.

The ledger is operational authority for scheduling and handoff, never an input to source, identity, availability, or
public values.

## Request and symbol authority

The existing combined pipeline's bar requests use SIP, `1Day`, ascending order, a 10,000-row limit, and these eight exact adjustment labels:

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

The historical lower bound is `2016-01-01T00:00:00Z`. The request end is an exclusive UTC midnight and is also the
`asof` date. A next-page token is rejected rather than silently truncating a response.

All submitted labels are resolved through exact asset lookup before the write transaction starts. A successful
US-equity response makes Alpaca's returned punctuation authoritative, such as `BFA -> BF.A`; duplicate canonical
symbols are processed once. The submitted label remains provenance in the lineage report.

For a symbol with no source inventory, all history from the lower bound to the exclusive end is requested. For an
existing source, `request_start` must be the first UTC midnight of a month and defaults to the first day of the end
month. The selected range replaces each nonempty month partition as a whole; it is not an append cursor, and its final
partition contains observations only before the exclusive request end.

The bar client and identity client each own a separate 199-request-per-minute limiter. The current command processes
symbols and all eight adjustment requests sequentially; it has no worker option.

## Standalone pricing-source contract

Owner: `libs/alpaca_pricing_download/`; entrypoint: `download_alpaca_pricing.py`; source contract: **6**.
The store marker/layout remains v3. The only additional persistent artifact is the per-ticker endpoint validator.
The downloader requests a ticker, preserves its returned pricing, publishes pending dates, and stops. Asset UUIDs,
public IDs, lineage, row allocation, and catalogue decisions are outside this boundary.

### Effective REST source

The stored CSV/gzip is the effective downloaded source, not an HTTP archive. JSON decimals become `Decimal` and
integers remain arbitrary-precision integers; neither passes through binary float or price arithmetic. The columns are
`timestamp,open,high,low,close,volume,trade_count,vwap`. Canonical rows must survive encoding/decoding exactly before
publication. HTTP formatting, response envelopes, and unknown fields are not retained.

All eight adjustment views are requested using SIP, USD, `1Day`, ascending order, and complete pagination. Duplicate
JSON keys, non-finite or injected float values, missing fields, wrong-symbol responses, repeated page tokens,
out-of-range rows, duplicate timestamps, and non-ascending rows fail closed. Market-validity and identity rules are
not downloader responsibilities.

The default historical lower bound is `2016-01-01T00:00:00Z`; an explicit `FROM` may start later. The source receipt
retains that acquisition floor unless an explicit valid `FROM` changes it. An earlier `FROM` backfills through full acquisition; any `FROM` after an
existing session is a ticker-specific hard error, even on force. Supported starts are on/after 2016-01-01.
Request end is exclusive UTC midnight; the inclusive API end is
one microsecond earlier. `asof` uses the request-end date for symbol mapping, **not historical adjustment vintage**:
requesting an older range does not recreate prices as they were adjusted then. See the
[live probe evidence](VERSION_TRACKER.md#live-adjustment-and-refresh-checks--2026-10-14).

### Three pricing boundaries

For October 2026, the selected layout is:

```text
data_pricing/
  store.manifest.json
  end_of_day/
    pricing/GOOG/
      .download.lock
      source.manifest.json
      validator_first_last_sessions.json
      completed_years.manifest.json
      completed_years.raw.csv.gz
      2026/
        year.manifest.json
        raw.csv.gz
        Oct/
          month.manifest.json
          raw.csv.gz
    manifests/GOOG/
      update.json                     # exists only while work is pending
```

| Boundary | October example | Rebuilt when |
|---|---|---|
| Completed years | 2016–2025 together | Initial acquisition, year rollover, or full refresh. |
| Current year | 2026 snapshot, **including October** as last consolidated | Initial acquisition, month rollover, or full refresh. |
| Current month | Authoritative October observations | Each ordinary update, rollover, or full refresh. |

Effective history = completed years + year snapshot **excluding the active month** + authoritative active month.
Daily month updates do not rewrite the year snapshot. Consolidation replaces stale overlap, never duplicates it.
Completed years have no retained individual year directories; consolidated months have no retained month directories.
The active month contains the day preceding the exclusive UTC end; rows are partitioned by New York session month.

The tree shows only `raw`. The **adjustment-to-payload boundary** is owned by each pricing manifest: eight logical
views select only their distinct physical payloads. Within that boundary, byte-identical gzip results use direct
`storage: "exact_copy"` aliases to one existing payload, not duplicate files or hard links. Consumers follow this
mapping rather than rediscovering equality. Gzip uses level 5 and `mtime=0`; empty views own no gzip.

The source receipt selects exactly three manifest paths and hashes. Boundary receipts bind owner, version, anchor,
contributing request, row counts, timestamps, logical digests, and payload sizes/hashes. Request metadata does not imply retained
rows were re-downloaded. Unchanged v1–v5 pricing receipts remain readable inside the same v3 store; old layouts are not
automatically migrated.

### First/last comparison validator

`validator_first_last_sessions.json` holds both endpoints for all eight views. Its JSON binds schema version, owner,
symbol, and an `adjustments` mapping from view to `first`/`last` canonical row arrays. Numbers remain exact strings;
empty views have two nulls. The source receipt selects this file by name and SHA-256.

The downloader derives it from the effective rows it publishes and installs it through the same journal, before the
source receipt. Normal probes load this small boundary, not completed-years/year gzip files. V4 validators remain
readable without bootstrap or cold-payload reads. Legacy normal updates
bootstrap once from existing pricing without rewriting cold payloads. Force can use intact legacy receipt extents
without decoding old prices. Damaged ownership receipts or validator metadata fail closed; force is not permission
to guess ownership. No separate endpoint files, raw-response archive, or new store is introduced.

### Update or replace

Before an ordinary update, request the **first and last effective stored session in every adjustment view**, once
if they are the same session. Probes use the update's `asof`, not each probe day's mapping. Compare timestamps and
every numeric field exactly, ignoring equivalent numeric spelling.

- All endpoints match: acquire from the stored active month's opening through the new end and consolidate as needed.
- Any endpoint differs or is missing: reacquire all eight views from the retained history start and replace the history.
- Initial acquisition, no stored sessions, an earlier start, or explicit force: also acquire the full range.
- After acquisition, compare returned endpoint overlap and re-request only the first candidate session per nonempty
  view. A mismatch allows one full-acquisition restart; repeated instability or request failure does not publish.
- A complete successful full response is authoritative, even if shorter or empty. Absent old sessions are discarded.
  Force has the same clean replacement semantics; clipped-history retention is deliberately deferred in a code comment.

This is the agreed endpoint-consistency refresh policy, not an audit of every older observation. Full replacement
keeps no archive of superseded pricing. Backwards and future request ends are refused, including during full refresh.
Full replacement may repair damaged owned gzip files using intact receipts; it does not bypass ownership checks.

### Pending lineage update

`end_of_day/manifests/<TICKER>/update.json` is one pending-work notice, separate from pricing receipts. It contains
**only** inclusive New York session dates:

```json
{"from":"2026-10-13","to":"2026-10-13"}
```

| Event | Notice after publication |
|---|---|
| Incremental change, no pending notice | Earliest to latest session whose canonical row was added, changed, or removed across any view. |
| Incremental change, notice already pending | Union of pending and changed ranges: earliest `from`, latest `to`. |
| Full acquisition/replacement | Envelope of old and replacement history plus pending dates, including removed sessions—even if replacement prices compare equal. |
| No changed rows in an ordinary update | Leave a pending notice alone; otherwise create neither notice nor ticker manifest directory. |
| Successful consumption | Remove the notice and its ticker directory; the next notice starts from newly changed dates. |

The range bounds work; it does not assert every intervening date changed. Re-requesting the month, rewriting provenance,
or consolidating unchanged rows does not by itself mark the whole month/year as changed. Presence means unprocessed work,
not a permanent receipt or audit log. Failed consumption leaves it pending.

### Locking, publication, and consumption

One worker owns one complete ticker transaction. A batch has at most three active ticker jobs, never per-adjustment
jobs; its shared HTTP admission/cooldown state is in memory and adds no datastore artifact. This execution boundary
does not change pricing ownership or the consumer lock protocol. Request limits and retry behaviour are specified in
[Operations](operations.md#standalone-pricing-download).

Both downloader and lineage consumer take the same **exclusive process-independent `flock`** on
`end_of_day/pricing/<TICKER>/.download.lock`. The downloader holds it before any provider request through publication;
the consumer holds it through reading, durable processing, and acknowledgement. The lock file is permanent: release
the OS lock, never unlink its inode. Process death releases the lock. Different tickers are independent.

Initial store-marker creation is directory-locked and atomically renamed from a reserved temporary file; it adds no
persistent lock file. Only changed ticker outputs are staged under `.staging/`. A durable `.pending.json` journals writes,
hashes, retirements, and notice publication. Pricing is installed and verified, with `source.manifest.json` last among
pricing writes; obsolete partitions are retired; the notice is atomically replaced; then staging/journal are cleared.
Interrupted publication blocks consumers until downloader recovery rolls it forward. There is no backup tree.
These locks are advisory: unlocked direct file reads do not provide atomic multi-file visibility.

The implemented internal handoff is
[`consume_pricing_update(store, symbol)`](../libs/alpaca_pricing_download/handoff.py). Inside its context, `None` means
no work; otherwise `batch.dates` supplies the range and `batch.read_views()` reads the effective pricing under the
already-held lock. The caller must durably finish processing before a successful context exit removes the notice.
An exception retains it. A crash after processing but before acknowledgement can repeat work, so the consumer
must be idempotent. Do not acquire a second ticker lock inside this context.

Normal handoff reads use `load_published_boundary` / `load_published_views`: they trust producer-approved receipts,
decode selected payloads, and follow approved aliases without re-hashing prices or repeating validation. Source
selection and incomplete-publication checks are not a repeated data audit. The separate internal
`read_pricing_views` helper retains shared-lock explicit authentication. None is a supported application API.
Standalone lineage consumes this handoff; `DataLibrary` integration remains separate work.

## Standalone lineage contract

Owner: `libs/alpaca_lineage_build/`; entrypoint: `build_alpaca_lineage.py`; contract **2**. Compatible v1 receipts,
indexes and recovery journals remain readable. The standalone pricing
store/marker remains unchanged. Lineage acquires asset/CUSIP and corporate-action evidence itself, but requests
pricing only through the downloader. It reuses the existing ownership, row-allocation, UUID-membership and
`security_id` transforms; it does not publish online status or the public catalogue.

### Twin chronological outputs

```text
end_of_day/lineage/
  index.sqlite3                       # one small dependency/receipt selector, no prices
  GOOG/
    lineage.manifest.json             # selected inputs, normalized evidence, output receipts
    completed_years.manifest.json     # all years before 2026 together
    2026/
      year.manifest.json              # snapshot including October when consolidated
      Oct/month.manifest.json         # authoritative October assignments
```

These use **the downloader's anchor month and identical overlap/consolidation semantics**. Each boundary contains
eight logical adjustment views with exhaustive dated correlated/unresolved segments, source-relative row positions,
identity membership and the selecting rule. No price values or gzip copies are stored. Effective assignments replace
the year snapshot's active month with the month boundary. Consolidated month/year files are retired; no history of
superseded generations is retained. Evidence and provenance live once in the selecting receipt, not in a growing
per-segment diagnostic audit trail.

The single SQLite index updates only affected ticker rows; it avoids rewriting a universe-sized JSON selector per
ticker. It contains receipt identities, dependency labels, UUID evidence membership and owning versions—not a second
price or identity-rule engine. Normal reads trust published receipts; they do not re-hash approved boundary outputs.

### Resume and invalidation

Ordinary action acquisition starts at the **previously active month**, retaining older normalized actions. Exact
asset/CUSIP lookups are refreshed. Newly discovered labels acquire their full requested evidence range. First use,
an older pricing invalidation, changed rules/calendar, or explicit metadata refresh reacquires the full evidence
range. Historical provider corrections outside the resumed window require explicit refresh; matching prices are
not claimed to prove metadata freshness. Conflicting current action identities fail before publication.

A month-only calculation reuses closed assignments when all of these hold: same anchor and closed pricing receipts;
unchanged owning versions and identity evidence; one resolved UUID and no related-source dependencies; every previous
assignment used the established unique-UUID rule; no older pending dates; and the new month's observed sessions and
predecessor seam remain continuous in every view. Only the month is opened/calculated; approved cold prices,
assignments and receipt digests are reused. A changed ownership decision, ambiguity, dependency, gap, rollover, or
full refresh instead recalculates the affected chronology with the existing rules. That consumes trusted pricing;
it does not re-run upstream pricing approval. Unchanged output boundaries retain their prior receipts.

The affected set includes prior and newly discovered dependencies, reverse dependants, and shared UUID evidence.
Dependency edges do **not** imply security membership. Thus FB → META may preserve one internal ID while reused FB
remains separate. A later notice for a related source reactivates earlier processed dependants. An identity-rule change
can require all previously processed sources; unrelated unchanged sources are otherwise left alone.

### Lock, delegate, publish, acknowledge

One directory-inode `flock` serializes lineage workers. The worker takes the complete known set of exclusive pricing
locks in sorted order. If discovery expands it, all pricing locks are released before reacquisition. Missing initial
or related pricing, or short request coverage, is handed to the downloader **outside those locks**. Without a published
range, null request bounds select the downloader's defaults; no second range policy is introduced. The worker then reloads the current notices and
receipts. A published empty response is acquired evidence. Discovery is capped at 100 labels and three pricing
acquisition waves; exhaustion/failure leaves notices pending. No queue-file hierarchy is introduced.

All resulting JSON is prepared before publication. One temporary `.pending.json` journals only changed outputs,
retirements, affected index entries, selected pricing generations and notices. Outputs are installed, then the index
is committed, then notices are acknowledged. The journal is removed last. Cooperating readers hold the lineage lock
and reject a pending transaction. Recovery rolls forward; a newer pricing generation is never acknowledged using an
older journal, even when its notice has identical dates. Process death releases locks. Unresolved assignments are
durable results; API or processing failure is not successful consumption.

`libs/alpaca_lineage_build/reader.py:read_lineage` is an internal downstream handoff, not a public application import.
It selects approved assignments, applies the month overlay, and refuses incomplete/stale input generations or pending
work. It does not read price payloads, acquire metadata, or rerun identity rules. These identity checks prevent mixed
generations; the approved facts themselves are trusted.

## Operator lineage evidence

Owner: [`evidence.py`](../libs/alpaca_lineage_build/evidence.py), `output_evidence` and `EvidenceCapture`;
entrypoint: [`output_current_lineage_evidence.py`](../output_current_lineage_evidence.py). Schema 2 is validated against
[`alpaca_lineage_evidence_change_log.json`](../resources/alpaca_lineage_evidence_change_log.json). This is an on-demand
operator response, not another durable datastore boundary or a public `DataLibrary` interface.

The document identifies `schema_version`, `boundary`, requested `symbol`, `mode`, `installed_versions`, the actual
`workflow` result and its `sources`. Each published source exposes the selecting receipt and normalized evidence,
pricing receipt/session-grid context, effective assignments, input freshness and decision origin. Fresh calculations
also expose the actual transient report and qualifying predecessor-prefix comparisons. Price payloads are not included.

`rule_path` explains first-match control flow from the recorded selection and ordered registry. It labels earlier
fall-through and later unevaluated rules; it does not replay predicates or invent their failure reasons. Reused and
offline publications retain their own versions and selected decisions, not a fictional trace from the installed code.
Transient assessments absent from a reused publication are explicitly unavailable. Normal pipeline observation runs
under the existing locks; offline inspection also respects publication locks and makes no writes or repairs.

See [Operations](operations.md#current-lineage-evidence) for acquisition effects and flags, and
[Development](development.md#lineage-research-and-rule-improvement-loop) for the research/implementation procedure.

## Monthly pricing-source contract

The store manifest fixes symbol/year/month placement, month abbreviations, column order, adjustment order, gzip level
5 with `mtime=0`, and direct manifest aliases for exact-copy views. The CSV columns are:

```text
timestamp,open,high,low,close,volume,trade_count,vwap
```

Each `<YEAR>/<Mon>/month.manifest.json` contains all eight logical adjustment entries in canonical order. A nonempty
entry is either:

- `storage: "file"`, owning one physical gzip whose size and SHA-256 are recorded; or
- `storage: "exact_copy"`, directly naming an earlier canonical entry in the same month with identical bytes,
  timestamp bounds, row count, and logical digest.

An empty logical view uses `storage: "empty"` and owns no gzip. Exact-copy aliases are receipt facts, not hard links or
alternate filesystem paths. An explicit authenticator rejects symlinks, unsafe filenames, unowned gzip payloads,
indirect aliases, digest differences, row-count differences, or inconsistent timestamp bounds.

`source.manifest.json` is intentionally structural: it lists years/months, whether an `unplaced` partition exists,
and adjustments empty across the complete source. The source generation binds that inventory digest and the ordered
month-receipt digests. Request identity is owned by validation, not duplicated into the source inventory.

A selected month is built in a sibling staging directory, authenticated, and directory-replaced. Identical month
documents are retained without replacement. A new month also changes the small source inventory; an in-place month
refresh does not. Previously published months outside the explicit replacement range are neither opened for write nor
republished.

## Validation contract

Validation mirrors source placement under `validation/<SYMBOL>/<YEAR>/<Mon>/month.validation.json`. Each month records
the source month receipt, all eight adjustment results, observed sessions, row and dataset failures, and its result
digest. From the second month onward it also records a predecessor seam proving timestamps remain strictly increasing
across the month boundary.

The ticker receipt binds:

- the complete source generation;
- validation transform version 19;
- the tracked market-calendar digest;
- the full request identity from the historical lower bound to the current exclusive end;
- every month validation generation and output digest; and
- the compact pass/fail summary.

An unchanged month result is reused when both its source month receipt and predecessor result digest match. If a month
changes, its eight views are revalidated. The immediate successor artifact may be republished because its predecessor
seam changed; propagation stops once both the month result and predecessor identity are unchanged. Changing only the
overall request end republishes the compact ticker summary but does not rerun unchanged month validation.

Pipeline execution stops before lineage/correlation publication if any selected validation boundary fails.

## Lineage-source contract

Lineage acquisition owns provider contact for current normalized identity evidence. It consumes the already-published
pricing and validation generations and requires validation's request end to equal the run boundary. Its receipt binds
both generations, lineage report version 4, the normalized evidence digest, and the output bytes.

Raw HTTP response bodies and request history are not retained. The report preserves normalized asset lookups,
corporate actions, identities, unresolved CUSIPs, provider request count, generated time, and submitted-to-canonical
symbol resolution when present.

Lineage acquisition is the current exception to a pure no-recalculation cascade: a selected symbol contacts Alpaca and
rebuilds normalized evidence before it can know that the semantic evidence is unchanged. When the evidence digest and
its pricing inputs are identical, the existing lineage files and receipt are retained byte-for-byte.

A complete name-change action may expand the search only through the existing exact, bounded evidence rule described
in [Pipeline](../PIPELINE.md#lineage-acquisition-boundary). Acquisition supplies facts; correlation still owns row
allocation and UUID membership.

## Correlation contract

Correlation consumes one trusted source, validation, and lineage generation per processing symbol. It invokes the
single established correlation transform and publishes:

- `correlated.json`;
- `uncorrelated.json`; and
- `correlation.manifest.json`.

The envelope owns the verified session exclusions, UUID-lineage edges, ticker ownership intervals, and evidence
issues needed by later identity stages. Every logical source row ordinal must occur exactly once across the two
branches. Segment bounds, row counts, source logical digests, adjustment labels, and canonical security IDs are
validated before the receipt is selected.

Cross-symbol episode logic runs to a fixed point: one preliminary pass, a second pass using the first, and a final
pass using the second. The second and final reports must be identical. Rule order and evidence shapes live in
[Pipeline](../PIPELINE.md#the-correlation-cascade).

## Warning contract

Warnings consume source, validation, and correlation generations. They publish separate cross-adjustment and
security-segment-statistics documents plus one receipt binding configuration, calendar digest, output digests, and
summary counts. Findings are advisory. They never alter a bar, segment, identity, or availability decision.

## Identity-candidate contract

The private candidate is a durable restart boundary, not an in-memory handoff. Its output contains sorted source
contributions and aggregated identities, all provisionally offline, plus exact correlation-generation bindings.

Two modes exist:

- `snapshot` builds the first catalogue from the complete supplied source set; and
- `affected_closure` binds one existing catalogue generation, replacement symbols, unchanged supporting symbols in
  the identity closure, and identities retired by the replacement.

The candidate receipt is written only after the output is re-aggregated and validated. Ordinary downstream loading
does not repeat that aggregation.

## Availability contract

Availability exercises every advertised adjustment through the published history reader for every candidate identity.
It also consumes correlation/lineage evidence, raw session grids, and the warning-set generation. Exactly one decision
must exist for every candidate master ID:

```text
online
offline
lineage_gap
```

`online` is true only for status `online`. At least one bar must be readable; all advertised adjustments and their
published dependencies must load; source conflicts, integrity failures, or unexplained internal session/identity gaps
keep the identity offline. Provider `active` and `tradable` fields do not define public availability.

Availability does not repair a history or change membership. It records whether the already-published candidate can
be served under the existing online-status transform.

## Catalogue-publication contract

The catalogue publisher applies availability to candidate source contributions, then uses the content-addressed
catalogue engine to publish source objects, security objects, two-hex digest buckets, inventory roots, and a small
security-master index.

The first publication is a complete snapshot. Later publications replace only the affected closure:

- unchanged source and security objects are not rewritten;
- only changed object descriptors, affected buckets, and inventory roots are replaced;
- retired identity objects are removed from the current inventory;
- newly superseded files remain for one additional successful publication; and
- files superseded by the prior publication are reclaimed unless the current generation still names them.

`catalogue/catalogue.manifest.json` binds the candidate, availability, candidate correlation set, catalogue-engine
generation, and engine output digest. It is deliberately written last and is the reader-visible publication point.
`catalogue/retention.json` is retention bookkeeping, not the selection receipt.

The public generation exposed by `DataLibrary` is the selected security-master generation. The outer catalogue
generation additionally binds the private candidate and availability generation and is an operator-level publication
identity.

## Publication isolation and recovery

The store owns one stable `.publication.lock` inode. A pipeline run takes the exclusive lock for the complete mutation
transaction, including source acquisition and downstream publication. Public catalogue and history reads take the
shared lock. Consequently, current reads wait while a write run is active.

The writer creates `.publication.pending.json` after acquiring the lock and removes it only after the final catalogue
receipt and `last_run.json` attempt. If the run fails, the marker remains. Readers then fail closed until the complete
pipeline is rerun successfully. A pre-existing marker disables downstream publication reuse during recovery.

The clean-room scheduler has one narrow terminal exception: a newly acquired source whose validation receipt is
complete and explicitly failed. No lineage or global catalogue artifact has been touched, so that isolated
transaction records the rejection and removes its marker without publishing a new catalogue receipt. The already
selected catalogue remains coherent and does not name the rejected source.

Each source symbol also owns an `end_of_day/<SYMBOL>/.publish.lock` for month-directory and inventory replacement. All
regular JSON and byte-file replacements use a same-directory temporary file, `fsync`, and `os.replace`. Month
directories use staged directory replacement with rollback of the immediately prior directory if installation fails.

Each transaction holds the lock during network work and processes symbols serially. The focused command can place a
whole cohort in one such transaction. The clean-room scheduler bounds a transaction to one new canonical symbol and
runs it against an isolated store.

## Reuse and recomputation matrix

| Boundary | Reuse key | Work retained | Work repeated when selected |
|---|---|---|---|
| Source month | Exact expected month receipt | Existing directory and payload bytes. | Focused update: provider request constructs the expected selected month. Clean-room recovery: a complete authenticated source is retained without a request. |
| Validation month | Source month receipt plus predecessor month-result digest | Month rule results and unchanged seams. | Changed month validation; at most the next seam artifact changes solely because of its predecessor. |
| Lineage | Source generation, validation generation, and semantic evidence digest | Existing report and receipt bytes. | Focused update reacquires evidence before equality is known. Clean-room recovery authenticates and retains a complete matching lineage boundary. |
| Correlation and warnings | Exact source, validation, lineage, correlation, and warning receipt identities | Entire existing downstream publication when all links match. | Complete processing-symbol closure after any relevant generation changes. |
| Candidate and availability | Correlation set plus current catalogue base and warning set | Existing candidate, availability, and catalogue receipts when fully linked. | Affected identity closure when inputs change. |
| Catalogue engine | Object equality within the affected closure | Unrelated objects, buckets, and identities are neither rewritten nor read during delta publication. | Changed source/security objects and their inventory path. |
| Public read | Selected catalogue and history receipts | Published identity, correlation, warnings, and monthly values. | No domain calculation or digest audit; only typed projection, filtering, and requested history assembly. |

The focused command can reacquire selected source and lineage inputs. The clean-room scheduler never reacquires a
completed checkpointed source and never independently calculates the same canonical owner twice. Correlation and
warnings remain sharded at processing-symbol/identity-closure granularity; a changed member legitimately invalidates
the complete affected closure.

## Identity model

Corporate lineage, security identity, and pricing episodes remain distinct:

- corporate lineage describes company or transaction relationships;
- security identity describes the shares anchored by authenticated Alpaca asset UUIDs and dated evidence; and
- a pricing episode describes a continuous interval of comparable observations.

The public/master ID is deterministic for resolved membership:

```text
master_id = SHA-256("\n".join(sorted(unique_alpaca_asset_ids)))
```

Ticker, name, CUSIP, FIGI, date, exchange, and action evidence determine or describe membership but do not alter the
digest for a fixed UUID set. A ticker is a dated label and may resolve to multiple identities. A retained UUID-less
series may receive a non-security series ID, but it never enters the public catalogue.

This digest is stable for fixed membership, not a persistent identifier across membership expansion. A newly accepted
split UUID changes the digest without itself changing historical ticker/UUID attribution. Membership-only split edges
never route rows. No persistent-ID registry or archive of superseded snapshots is implied by this contract.

## Runtime artifact layout

```text
data_vtest/
  store.manifest.json
  .publication.lock
  .publication.pending.json          # transient; retained after failure
  last_run.json                      # unlinked operational record
  clean_room_run/                    # present only for a clean-room generation
    manifest.json                    # immutable universe/request/contract identity
    resolutions/<POSITION>.json      # durable provider resolution before price work
    outcomes/<POSITION>.json         # one terminal outcome per input label
    attempts/<POSITION>.<N>.json     # nonterminal failure diagnostics
    audit.json                       # complete boundary/public-reader audit result
    completion.json                  # digest binding audit.json
  identity_candidate.json
  identity_candidate.manifest.json
  end_of_day/<SYMBOL>/
    source.manifest.json
    .publish.lock
    <YEAR>/<Mon>/
      month.manifest.json
      <canonical-adjustment>.csv.gz
    unplaced/                         # only when timestamp placement failed
  validation/<SYMBOL>/
    validation.json
    validation.manifest.json
    <YEAR>/<Mon>/month.validation.json
  lineage/<SYMBOL>/
    lineage.json
    lineage.manifest.json
  correlation/<SYMBOL>/
    correlated.json
    uncorrelated.json
    correlation.manifest.json
  warnings/<SYMBOL>/
    cross_adjustment.json
    security_segment_statistics.json
    warnings.manifest.json
  availability/
    availability.json
    availability.manifest.json
  catalogue/
    catalogue.manifest.json
    retention.json
  catalogue_engine/data/end_of_day/json_manifests/
    security_master.index.json
    security_master.manifest.json
    objects/{sources,securities}/...
    inventories/{sources,securities}/{buckets,roots}/...
```

Runtime data is ignored by Git. [`resources/ticker.csv`](../resources/ticker.csv) is a one-record, comma-separated
recovery snapshot containing 18,092 historical source labels. It is not a catalogue. The combined producer's
line-oriented `--file` parser does not accept it directly; the standalone siblings' CSV-aware parsers do. It cannot
reproduce a generation without provider acquisition and the complete boundary cascade.

## Authoritative implementation

| Contract | Source |
|---|---|
| Compact pricing and locked pending dates | [`alpaca_pricing_download`](../libs/alpaca_pricing_download/) |
| Standalone lineage execution, versions and publication | [`pipeline.py`](../libs/alpaca_lineage_build/pipeline.py), [`contract.py`](../libs/alpaca_lineage_build/contract.py), [`storage.py`](../libs/alpaca_lineage_build/storage.py) |
| Operator evidence and recorded rule paths | [`evidence.py`](../libs/alpaca_lineage_build/evidence.py) |
| Request defaults and adjustment labels | [`settings.py`](../libs/download_alpaca_end_of_day_bars/settings.py) |
| Bar HTTP, rate limiting, and page-token rejection | [`alpaca_http.py`](../libs/download_alpaca_end_of_day_bars/alpaca_http.py) |
| Monthly source and authentication | [`source.py`](../libs/alpaca_end_of_day_v6/source.py) |
| Incremental monthly validation | [`validation.py`](../libs/alpaca_end_of_day_v6/validation.py) |
| Normalized lineage source | [`lineage.py`](../libs/alpaca_end_of_day_v6/lineage.py) |
| Correlation envelope | [`correlation.py`](../libs/alpaca_end_of_day_v6/correlation.py) |
| Warning envelope | [`warnings.py`](../libs/alpaca_end_of_day_v6/warnings.py) |
| Candidate, availability, and catalogue publication | [`catalogue.py`](../libs/alpaca_end_of_day_v6/catalogue.py) |
| Trusted published history adapter | [`reader.py`](../libs/alpaca_end_of_day_v6/reader.py) |
| Store lock, atomic replacement, and recovery marker | [`common.py`](../libs/alpaca_end_of_day_v6/common.py) |
| Complete orchestration and reuse selection | [`pipeline.py`](../libs/alpaca_end_of_day_v6/pipeline.py) |
| Ordered identity transforms | [`alpaca_end_of_day_security_correlation`](../libs/alpaca_end_of_day_security_correlation/) |
| Public consumer projection | [`alpaca_end_of_day/__init__.py`](../alpaca_end_of_day/__init__.py) |

Code and runtime validators win if prose becomes stale. Any serialized or behavioral change must bump its owning
version and append exactly one changelog entry as described in [Development](development.md#versioned-boundaries).
