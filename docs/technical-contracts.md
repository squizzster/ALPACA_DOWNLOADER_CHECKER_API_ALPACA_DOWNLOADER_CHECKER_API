# Technical contracts

## Trust and ownership

[AGENTS.md](../AGENTS.md#trust-before-touching-storage) owns storage-trust rules. After a completed selector's
required schema/version/token checks, consume selected work. Reacquiring locks requires generation checks, not
existence/symlink inventories, stat-based cache keys, payload hashes or upstream rule replay. Producers validate
new outputs/destinations and compare recorded digests; explicit authentication audits existing data. Readers fail
closed without repair.

[OperationContext](../libs/alpaca_lineage_build/context.py) retains coherent accepted compatibility, index and
summary views per invocation. Revisions/publisher-returned changes refresh them; no persistent context cache or
alternative authority. Changed inputs invalidate dependent work only.

## Standalone artifacts

Paths are relative to the standalone store:

| Artifact | Authority |
|---|---|
| `store.manifest.json` | Store format. |
| `end_of_day/pricing/<TICKER>/source.manifest.json` | Pricing generation, boundaries, payload map. |
| Adjacent `validator_first_last_sessions.json` | Source-bound exact first/last rows for every view. |
| Pricing `completed_years`, `current_year`, `current_month` | Selected CSV/gzip boundaries. |
| `end_of_day/manifests/<TICKER>/update.json` | Unconsumed inclusive changed range: `from`, `to`. |
| `end_of_day/lineage/<TICKER>/lineage.manifest.json` | Evidence, input generations, chronological assignments. |
| `end_of_day/lineage/index.sqlite3` | Group/generation selection, complete membership, lookup projection, serving obligations. |
| `end_of_day/catalogue/serving_catalogue.sqlite3` | Serving selector, public fields, decisions, previous contributions. |
| `universe_updates.sqlite3` | Accepted observations, registration, targets, attempts, outcomes. |
| `pricing_check_summary.sqlite3` | Live-check baseline. |
| `lineage_update_check.sqlite3`, `download_completion.json` | Disposable scheduling hints, not data authority. |

Effective history: completed years + current year excluding active month + authoritative current month.
Rollover consolidates; ordinary month updates preserve cold payloads. Eight logical views may alias exact-copy
physical files. Columns: `timestamp,open,high,low,close,volume,trade_count,vwap`. Provider JSON numbers become exact
strings without binary-float arithmetic. Structural checks reject wrong symbols, malformed values, duplicate/
out-of-range timestamps and incomplete pagination; they do not establish market validity or identity.
Authorities: [contract.py](../libs/alpaca_pricing_download/contract.py),
[storage.py](../libs/alpaca_pricing_download/storage.py).

Pricing format 31 remains readable in place. Supported sources may select older unchanged boundary receipts;
versions need not match. Compatibility requires no migration, relabeling or price rewrite; unknown future formats
fail closed. [Runtime versus publication versions](../VERSION_TRACKER.md#compatibility) also preserves lineage-34
formats and accepted policy identity; runtime repairs alone do not invalidate completed decisions.

[load_pricing](../libs/alpaca_lineage_build/pricing.py) supplies disposition with
[PublishedPricingEvidence](../libs/alpaca_pricing_download/evidence.py): generation, symbol, request scope and exact
counts for eight views, including zero/retained rows when correlation reads only an added range.
[classify_symbol_disposition](../libs/alpaca_security_lineage/symbol_disposition.py) checks that handoff's schema/scope,
without payload/sidecar reads or repeated authentication. Invalid handoffs stay unresolved. Completed lineage
evidence remains authoritative; selected evidence refresh produces the corrected projection.

## Pricing acquisition and continuation

Ordinary downloader updates compare each view's first/last endpoints; mismatch requires full replacement.
Daily live checks compare only summary `last_all`; any changed field requires all-view retained-span replacement
before append. Neither check detects every interior historical correction. Replacement notices include removed
sessions. Authorities: [download_pricing / publish_acquired](../libs/alpaca_pricing_download/download.py),
[HTTP execution](../libs/alpaca_pricing_download/http.py), [completed responses](../libs/alpaca_pricing_download/responses.py).

These owners enforce acquisition stages, row budgets, exact fallbacks, overlap and bounded retries. Completed
normalized responses are sliced without renormalization; fresh first-session probes remain independent.
Only after successful full batch pagination do [omitted symbols](https://docs.alpaca.markets/us/reference/stockbars)
complete empty for the requested range, without
individual rechecks. Transport, shape or pagination errors leave actions incomplete. Replacement emptying a
previously nonempty view needs bounded fresh producer confirmation; ordinary empty appends/initial empty histories
retain batched acquisition.

Response bodies live with their consumer. Request-size splits preserve ordered pagination without retaining
older cohorts through recursive closures/handled tracebacks. Retained [AcquisitionFailure](../libs/alpaca_pricing_download/failures.py)
records contain immutable scalar facts; fresh exceptions preserve outcome/retry classification without holding
completed response buffers.

[Append approval](../libs/alpaca_pricing_download/append.py) binds checked generations to selected missing intervals.
Publication rechecks generations under ticker locks even when the last row matches; new summary membership cannot
inherit approval. Empty append requires all eight retained views empty; partial views require ordinary checks.
Buffered acquisition binds a generation or explicit absence; concurrent replacement requires reselection.

Retained-span refresh preserves start, end and mapping date. Explicit historical entity mappings remain pinned
across retries/cutoff extensions; endpoint windows/action discovery use the independent observation cutoff.
Mapping change requires complete generation-bound replacement. Completed acquisition and endpoint responses
remain distinct handoffs.

[validate_activity](../libs/alpaca_pricing_download/activity.py) checks new partitions: trade counts agree across
views; views without split adjustment preserve raw share volume. Exact decimals preserve units; missing rows are
not zeroes. `activity_consistent` covers retained history only after full validation or inheritance of that
completed guarantee. Legacy appends cannot certify untouched cold partitions; readers trust published guarantees.

[Pricing ancestry](../libs/alpaca_pricing_download/changes.py) keeps at most 64 ancestor generations plus earliest
possibly changed session across all views/removals; null means identical observations. It survives acknowledgement
and rollover. Missing/expired ancestry proves no unchanged prefix: reevaluate selected affected histories,
without a store-wide metadata audit.

## Check summary

Entries contain only `symbol`, saved mapping `asof`, completed source `end_exclusive`, exact eight-field `last_all`.
Eligibility: unique stored same-symbol active US-equity lookup and nonempty `all` endpoint; stored activity is not
online availability. Last-session/asof pairs group checks. Last row defines session window; `asof` independently
selects provider identity; completed exclusive end independently starts append. Epoch/revision/coverage and
separate owner-pending markers control publication. No identities, other views, first rows or saved comparisons.

Pricing owns baselines; lineage owns membership. Missing/incomplete/pending coverage fails closed; snapshot tokens
guard comparisons without ticker enumeration. Authorities: [summary storage](../libs/alpaca_pricing_download/check_summary.py),
[project / rebuild](../libs/alpaca_lineage_build/check_summary.py).

Daily recovery finishes only named pending inputs and their complete lineage groups before selection. Remaining
pending inputs block selection; pending summary never means empty membership or permission to check unaffected
active sources' retained histories.

## Universe observation and work state

Observations retain duplicate labels. Unique symbol records may supply metadata; missing/duplicate records need
exact fallback. CUSIPs always stay exact. Absence proves neither inactivity nor unavailable prices.
[catalogue.py](../libs/alpaca_lineage_build/catalogue.py) and [projection.py](../libs/alpaca_lineage_build/projection.py)
separate current evidence from last-positive own-label facts; absent historical times remain unknown.

[Registration](../libs/alpaca_lineage_build/registration.py) persists intentions/work before registry union and
acknowledgement. First-read label selection stays frozen across execution/batch memo; later file changes affect
later commands. Registry/work records do not establish publication. Work initialization is separate from
lineage/summary coverage; interrupted registration is local recovery, not a delayed provider failure.

[WorkPolicy](../libs/alpaca_lineage_build/work_contract.py) and [durable state](../libs/alpaca_lineage_build/universe_state.py)
retain stable obligation IDs, accepted endpoints, evidence fingerprints, complete groups, refresh/replacement
requirements and retry state. Persist dependency/correction requirements before requests. Lineage stamps only
satisfied locked inputs: coverage cannot erase unperformed refresh/correction or failed checks. Completed
replacements/extensions survive interrupted acknowledgement; a matching last row alone proves neither.
Workflow acknowledgement alone does not create a lineage or serving generation. Unchanged retry completion
uses the accepted index's coverage and group, together with a fresh check bound to its pricing generation.
The lineage producer passes already-checked input completion in memory, bound to the obligation and every
selected group generation; a later history approval consumes that handoff without reopening the group.
Pending work, renewed obligations or changed group generations invalidate that handoff. Durable work records
acknowledge the selected publication; they never replace it as data authority. Interruption before acknowledgement
leaves retry work pending, while interruption afterward does not require republication. Changed evidence,
pricing, assignments or policy still follow the owning publication path.
Recovery reselects acquisition from accepted requirements/completed summary; a saved `acquire` flag cannot divert
a summarized maintained source from live check → approved append.

Every lineage entrypoint reconciles used lookup facts before acquisition. Identity changes require complete-group
replacement, including newly discovered dependency evidence. Pricing binds evidence fingerprint/source generation;
lineage cannot stamp different facts. Catalogue-versus-exact provenance alone does not invalidate identical facts.

New mapping changes bind every affected member to one group fingerprint, including absent/unchanged own lookups.
Retries/metadata-only changes preserve it. Selection/evidence reconciliation binds older pending replacements
lacking fingerprints from accepted group evidence, preserving existing non-null requirements and completed
pricing handoffs. Required-member failure blocks sibling publication. Failure transactions refresh in-memory
policy without losing attempts/deadlines when obligations expand. [Retry/coverage rules](operations.md#recovery-registry-and-retries).

[inspect_status](../libs/alpaca_lineage_build/status.py) reads compact boundaries, only pricing journals named by
scheduling/summary work, and the lineage marker under its shared gate. No payloads, receipts, ticker census or
recovery. [Report meanings and consistency](cli-reporting.md#completion-and-status).

## API plan

Pure [build_plan](../libs/alpaca_data_api_solver/__init__.py) maps `steps[].needs[]` to versioned ordered actions;
code owns fields, strategies, delivery slices and batching limits. Needs require nonempty reasons; within-step
needs are independent. Exhaust pagination before advancing; construct dependent needs only after responses.

Bars: 1Day/SIP/USD, separate adjustment requests, mapping `asof` (not adjustment vintage). Date-only solver ranges
mean New York midnight; timestamps need a timezone. Corporate-action bounds are inclusive. Compatible symbol/
range requests batch; adjustments/asof stay distinct. Symbol lookup may use catalogue batches/exact fallback;
CUSIPs never do. Page estimates exclude response-dependent correction/expansion; no global HTTP minimum or ETA.

## Lineage reuse

[Lineage contracts](lineage.md) own complete-group reuse, policy migration, observation selection, ownership
constraints, metadata continuation and missing auxiliary prices.

## Publication and recovery

Stable `.download.lock` inodes provide producer-exclusive/reader-shared `flock`; one lineage `.worker.lock`
serializes writers. Sorted pricing locks plus brief publication gate protect selected groups while allowing
unrelated reads/publications. Readers select → lock → recheck → consume. Lock-file existence is not ownership.

[operation_lock](../libs/common/store_lock.py) uses stable `.<store-name>.operation.lock` beside the store. CSV export
holds nonblocking shared `flock` from catalogue selection through run manifest; every store-writing CLI command
holds nonblocking exclusive `flock` throughout. Store gate precedes owner locks. Writers exclude writers/exports;
read-only exports may coexist. Exit, including `SIGKILL`, releases the kernel lock; path remains, with no PID or
stale-lock recovery.

Owners close temporary files and atomically replace journaled outputs: payloads before receipts/index, summary
before acknowledgement, journal removal last. Acknowledgement cannot consume newer notices/hints. Pricing hands
accepted source/endpoints downstream, reuses cold references and inventories only replaced boundaries. Recovery
reconstructs durable inputs; owners clean abandoned staging under ticker locks. Interrupted multi-file publication
blocks readers until owner recovery. Authorities: [pricing storage](../libs/alpaca_pricing_download/storage.py),
[lineage storage](../libs/alpaca_lineage_build/storage.py).

History replacements retain payload files when newly produced bytes match their recorded digests; matching
files are not reopened, checked or rewritten. Changed payloads, receipt changes and required update notices
still use the normal publication journal. Physical repair is a separate explicit pricing-owner option.

[connect_sqlite](../libs/common/sqlite_connections.py) uses transactions, disk rollback journals and write
`synchronous=OFF`. Producers own native recovery; readers/previews fail closed when it is needed. No forced
synchronization: process interruption is covered, machine/power failure is not.

## Serving catalogue

[policy.py](../libs/alpaca_serving_catalogue/policy.py), `evaluate`, consumes completed IDs/assignments and exact
prices without upstream authentication or identity-rule replay. Eligibility covers complete selected history:

- Eight views share one nonempty unique market-session set and deterministic source selection. Conflicts are
  ineligible; unreadable publications leave work pending.
- Timestamps are New York session midnight. Positive OHLC; open/close and positive VWAP within low/high.
  Nonnegative exact-decimal volume and integral trade count. Ordinary activity requires both positive, except
  split-adjusted volume may round to zero. Flat positive OHLC with zero volume/count/VWAP is the explicit
  zero-activity exception. Upstream already guarantees finite exact numbers.
- Explain continuity; never fill/trim. A rename gap of at most five sessions requires complete action evidence,
  unchanged UUID/CUSIP/exchange, one matching dated name-change action, compatible exact symbol/CUSIP lookups,
  no conflicting occupant/event and no source observations in any view, including unresolved rows.
  Exclusions neither assign observations nor merge identities.
- Unresolved assignments within the advertised identity span veto serving; unrelated/outside-span uncertainty
  does not discard supported history. Empty/entirely unresolved groups complete without public identities.

Inactivity, nontradability or missing descriptions alone do not disqualify. Coverage means approved stored history,
not lifetime completeness/current freshness. Policy version/calendar digest select positive and negative decisions.
Names/exchanges/classes come only from own-symbol observations of the identity's UUIDs, not current CUSIP lookup.
Observation timestamps remain observation times; historical effective dates need dated provenance. Omit unknown descriptions.

### Checkpoint reuse

[reuse.py](../libs/alpaca_serving_catalogue/reuse.py) checkpoints generations, ownership runs, source-coverage order,
numeric outcomes, labels and gap evidence, never copied prices. Reuse is per identity across group merges/splits;
independent identity changes do not invalidate it. Proven appends skip old rows/closed payloads; suffix containers
may contain older rows. Changed adjusted-source selection, ownership, replacement or absent ancestry requires
affected-history evaluation.

Reuse covers the latest validated session in every view; changed coverage can clear negative cross-view decisions.
Raw continuity labels remain separate from public all-view labels. Gaps use current evidence; changed group inputs
refresh bounded gap-presence reads without repeating unaffected numeric checks. Physical aliases share decoding.
Checkpoints/decisions commit together. Policy changes require explicit rebuild of the single current checkpoint format.

The policy-5→6 exception translates scalar ownership labels to lists and reuses completed facts only when the whole
group's pricing generations and every row/source/owner/label selection match. Comparison uses selected checkpoints/
assignments without price reads. Changed groups receive ordinary evaluation; both paths project current metadata.
Consumers still require current policy. No extension to other policy/calendar changes.

### Publication and maintenance

[storage.py](../libs/alpaca_serving_catalogue/storage.py) atomically replaces a component's decisions, public fields,
dated matches and ID/group/input references. Incompatible duplicate IDs abort. Selector publishes last: schema,
policy, time, completeness, transaction token; readers never hash the database. Compact selection serves status,
preview, public snapshots and unchanged work; only changed selected groups open checkpoints. Matching contributions
are neither revalidated nor rewritten.

[maintain](../libs/alpaca_serving_catalogue/producer.py) follows published membership and previous contributions,
including absent dependencies, without rediscovering evidence closure. Lineage transactions record exact-generation
[serving obligations](../libs/alpaca_serving_catalogue/work.py) before upstream acknowledgement. Catalogue commit
precedes obligation deletion; retries reuse committed work without history reads. The lineage worker serializes
maintenance; selected pricing locks span publication. Failures/focused commands leave unrelated obligations pending.

Serving retains the accepted store compatibility and lineage index in a view owned by its held lineage writer
lock. Selected readers consume those entries without reopening the index, rechecking its protocol or repeating
store path checks. The view is restricted to the owning process/thread and expires before the writer lock is
released. Pricing locks, source-generation checks and pending-publication checks still protect each selected
group. Ordinary readers without this writer-owned view retain selection, locking and reselection. Runtime-only
changes to this handoff preserve the serving policy and its completed checkpoints.

Explicit bootstrap covers indexed groups and may project missing membership from receipts in bounded batches,
preserving digests/identity decisions. It may read full selected histories; ordinary maintenance/readers never
bootstrap. Policy rebuild follows this path; incompatible schemas need supported explicit migration.
[Operations](operations.md#serving-catalogue-maintenance); [public snapshots](public-api.md#online-catalogue-reader).

## Parquet conversion

[Exports](exports.md#parquet-conversion) owns conversion, memory and publication semantics.
