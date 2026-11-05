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

Retained pricing uses only the first/latest fully adjusted (`all`) comparison to select history reuse,
including ordinary downloads, dependencies and daily-summary members. A single-session history needs one
comparison. A match approves the selected generation and only missing sessions are acquired; other historical
views, overlap and old first-session confirmations are forbidden. Any changed field selects full retained-span
replacement. This boundary does not detect every interior correction. Replacement notices include removed
sessions. Authorities: [download_pricing / publish_acquired](../libs/alpaca_pricing_download/download.py),
[HTTP execution](../libs/alpaca_pricing_download/http.py), [completed responses](../libs/alpaca_pricing_download/responses.py).

Each pricing HTTP attempt uses a 300-second socket timeout for blocking network operations, including
endpoint checks, pagination and retries. This is not a total operation or retry deadline. An earlier provider
HTTP 504 remains a server failure; the client timeout cannot extend the provider's own backend deadline.

These owners enforce acquisition stages, row budgets, exact fallbacks and bounded retries. Completed
normalized responses are sliced without renormalization. Only newly acquired full histories or selected
replacements receive independent producer confirmation; an approved append never confirms old history again.
Endpoint checks batch identical session windows across symbols but never widen their date coverage to save
pages. Acquisition carries immutable accepted endpoints and completed comparison results to execution, bound
to store, symbol, source generation, cutoff and mapping. Execution rechecks the generation under its lock;
it does not reread the validator or replay the comparison. Completed comparisons survive a peer or suffix
acquisition failure within the same operation and binding. Missing endpoint validators fail closed without
decoding retained prices to reconstruct them. Failed requests retain bounded failure handling.
Only after successful full batch pagination do omitted symbols complete empty at the transport boundary.
Ordinary acquisition consumes that completed response without individual rechecks. The history-comparison owner
has additional evidence: its compact baseline names an already retained endpoint. Provider batch companions can suppress
that bar, so a completed omission triggers one singleton confirmation action with the identical session, adjustment
and mapping date. Returned endpoints are retained; failed batches are not interpreted as absence. A request
already executed singly, including a 414 subdivision, is not repeated. Confirmations exhaust pagination through
the same limiter/retry policy; failed confirmations remain failures and completed empty confirmations remain
missing. Results and debug events identify each missing endpoint; request metrics count probes and recovered
endpoints. This rule does not infer ticker identity from a companion's returned bar.
Transport, shape or pagination errors leave actions incomplete. Replacement emptying a
previously nonempty view needs bounded fresh producer confirmation; ordinary empty appends/initial empty histories
retain batched acquisition.

Response bodies live with their consumer. Request-size splits preserve ordered pagination without retaining
older cohorts through recursive closures/handled tracebacks. Retained [AcquisitionFailure](../libs/alpaca_pricing_download/failures.py)
records contain immutable scalar facts; fresh exceptions preserve outcome/retry classification without holding
completed response buffers.

[Append approval](../libs/alpaca_pricing_download/append.py) binds checked generations to selected missing intervals.
Publication rechecks generations under ticker locks even when the last row matches; new summary membership cannot
inherit approval. Empty append requires all eight retained views empty; a nonempty source without an `all`
baseline requires explicit replacement. Empty same-month suffixes advance coverage without decoding payloads.
Buffered acquisition binds a generation or explicit absence; concurrent replacement requires reselection.

Retained-span refresh preserves start, end and mapping date. Explicit historical entity mappings remain pinned
across retries/cutoff extensions; endpoint windows/action discovery use the independent observation cutoff.
Mapping change requires complete generation-bound replacement. Completed acquisition and endpoint responses
remain distinct handoffs.

A completed serving decision may schedule a provider-late-bar probe for exact unexplained raw-absent
source/session pairs. The producer records each source generation, checked dates and present raw dates in its
history checkpoint, then publishes the version-2 scheduling projection in the compact selector. Planning
consumes that projection without group-body or source reads. Adjusted-only presence is not raw presence;
present but unassigned raw observations remain lineage work. Contributors and explained gaps retain their
existing ownership rules; absence alone does not establish that a historical label owns a requested date.

Missing or incompatible projections require explicit `catalogue rebuild`. Projection conversion reads completed
ineligible bodies, including old empty projections, and only the raw source/date boundaries needed for missing
provenance. It preserves decisions, decision generations, numerical checkpoints and unrelated pending work.
Matching pricing generations are required under selected locks. Completed all-view absence may prove raw absence
for each unchanged source; group-wide presence cannot be attributed to a particular source. Prefix reuse carries
these facts under producer ancestry and reads only changed or new gap scope.

Exact raw probes remain generation-bound; compact selections include completed online and offline decisions.
A confirmed late provider row enters the ordinary eight-view retained-span replacement and complete-group
lineage path. Orchestration does not predict eligibility. Stale pricing or serving generations require
reselection rather than widening the probe. This projection does not introduce a negative-probe freshness cache.

[validate_activity](../libs/alpaca_pricing_download/activity.py) checks new partitions: trade counts agree across
views; views without split adjustment preserve raw share volume. Exact decimals preserve units; missing rows are
not zeroes. `activity_consistent` covers retained history only after full validation or inheritance of that
completed guarantee. Legacy appends cannot certify untouched cold partitions; readers trust published guarantees.

[Pricing ancestry](../libs/alpaca_pricing_download/changes.py) keeps at most 64 ancestor generations plus earliest
possibly changed session across all views/removals; null means identical observations. It survives acknowledgement
and rollover. Missing/expired ancestry proves no unchanged prefix: reevaluate selected affected histories,
without a store-wide metadata audit.
A full refresh can prove identical effective observations from newly acquired rows and accepted logical
digests/counts, without reading old prices. Same-anchor proof covers all eight views, complete years, the
active month, and the recorded year-snapshot bounds; the active month overrides its year overlap. New
non-active-month rows outside those bounds, unproved overlap or rollover keep ordinary changed-span treatment.
Proven equality preserves retained payload references and controls ancestry, new notices and the returned
`changed` flag together. An existing pending correction is never erased by an equal retry. Mapping/request
context still reaches the ordinary lineage doorman.

## Check summary

Format-3 entries contain only `symbol`, saved mapping `asof`, completed source `end_exclusive`, and exact
eight-field `first_all` and `last_all` rows from the fully adjusted view.
Eligibility: unique stored same-symbol active US-equity lookup and nonempty `all` endpoint; stored activity is not
online availability. Endpoint-session/asof pairs group checks, with identical windows requested once. Each row defines its session window; `asof` independently
selects provider identity; completed exclusive end independently starts append. Epoch/revision/coverage and
separate owner-pending markers control publication. No identities, other views or saved comparisons.

Format 1/2 requires explicit offline `summary rebuild`; ordinary reads never fill missing first endpoints by
opening every validator. Frozen publications can finish format-2 pending bookkeeping before rebuilding, but
format-2 entries cannot approve a live check or append. The rebuild reads indexed eligibility and selected
source validators, without acquiring prices or rewriting retained histories.

Pricing owns baselines; lineage owns membership. Missing/incomplete/pending coverage fails closed; snapshot tokens
guard comparisons without ticker enumeration. Authorities: [summary storage](../libs/alpaca_pricing_download/check_summary.py),
[project / rebuild](../libs/alpaca_lineage_build/check_summary.py).

Unchanged lineage leaves a completed selected summary alone. A changed membership publication reuses an
accepted endpoint when pricing has no pending change; it does not reopen the endpoint validator. A pricing
invalidation, new eligibility or incomplete baseline requires the selected pricing owner's endpoint. These
producer projections read only the named summary entries and pending owners under their held pricing locks.

Recovery and discovery share one accepted provider catalogue and exact asset/action lookup cache for the
operation. A completed lookup, negative outcome or failed request is not discarded at a phase transition.
New invocations acquire fresh observations. [Lineage](lineage.md) owns this evidence lifetime.

Daily recovery finishes only named pending inputs and their complete lineage groups before selection. Remaining
pending inputs block selection; pending summary never means empty membership or permission to check unaffected
active sources' retained histories.

Daily boundary acceptance, work selection and estimation run under a compact-only storage guard. Pricing and
lineage path owners reject ticker reads, notices, locks and payload paths during those phases before touching
the filesystem. After selection, the guard permits the selected complete groups, summary maintenance,
late-bar sources and pending serving components. Completed metadata dependency and serving-component
handoffs can enlarge that scope explicitly. An unexpected path or inventory aborts the operation through
the CLI's terminal error path, releases locks and closes debug spans; ordinary per-symbol retry handlers cannot
swallow it. Worker tasks inherit the scope; a latched violation rejects subsequent owner access and prevents
successful scope completion even if a worker exception was caught. The guard performs lexical checks only and does not authenticate
selectors or inspect payloads. It guards the owner APIs, not arbitrary filesystem calls added outside them,
and does not equate estimated page counts with hard budgets or prove that every selected read is necessary.
Authority: [work scope](../libs/common/work_scope.py).

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
Completion batches share one transaction and compare the accepted obligation ID before acknowledgement;
a newer obligation rejects the stale batch. A batch is restricted to its store, process, thread and active
transaction lifetime. Interruption before commit rolls back the batch, while committed
outcomes survive. Complete dependency groups and maintenance requirements also persist together before requests.
Recovery reselects acquisition from accepted requirements/completed summary; a saved `acquire` flag cannot divert
a summarized maintained source from live check → approved append.

Ordinary eligible summary members complete that sequence before discovery advances their dependencies.
Selection's changed-identity and lost-eligibility facts reconcile before those members enter maintenance.
Later phases retain each completed comparison even when an independent member fails. A comparison is bound to
its summary entry and pricing generation; reuse requires the same accepted selectors and current allowed scope.
Matching comparisons and differing comparisons are distinct from append approval: differences and outstanding
corrections still use the ordinary replacement producer. Append approval additionally binds the current correction
obligation, so a renewed correction cannot inherit it. Successful corrections and appends retain their approvals
once lineage accepts their exact generations, even if a peer fails; incomplete lineage cannot lend that approval.
Late-bar replacement supplies its completed approval before daily comparison resumes. An interior replacement
invalidates earlier comparisons even if endpoints are identical. These handoffs are not persisted across restarts,
and retaining partial successes does not turn a failed overall update into success.

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

Credential-bearing pricing and metadata transports reject HTTP redirects. The accepted request target does
not authorize a different destination. Metadata JSON rejects duplicate keys and non-finite numbers at ingress;
only a missing/null page token completes pagination, while a nonempty new string continues it. Malformed or
repeated tokens fail the acquisition before it can publish a completed evidence interval.

The shared [HTTP executor](../libs/common/provider_http.py) owns attempt sequencing, balanced symbol
subdivision and pagination. Pricing and metadata adapters retain endpoint validation, parsing, timeout,
limiter and retry policies: pricing uses its shared 199/minute budget and retries 429/5xx and transport
failures up to six attempts; metadata keeps its separate limiter, 60-second timeout and 429-only retries.
Exact asset lookups and catalogue requests use the same attempt lifecycle without symbol subdivision.

Each logical bars or corporate-action roster is divided into `ceil(symbols / 1000)` sequential physical
batches, differing by at most one symbol, with smaller batches first. A 3,386-symbol action therefore uses
846, 846, 847 and 847 symbols. Dates, adjustment, mapping date and other parameters stay unchanged.
Each batch exhausts its own pagination before the next starts. HTTP 414 further bisects only the rejected
batch, restarting that batch's pagination and discarding its partial pages. Results cross the logical-action
boundary only after all batches validate; a later failure discards all results for that action and blocks
dependent steps. Independent actions retain existing continuation-on-failure behavior. Completed omissions
remain valid empty results and never trigger individual-symbol fanout. A rejected exact child after a split
cannot establish unavailability of its siblings.

Page estimates include physical subdivision. Live checks report planned batches, executed batches (including
414-rejected attempts at a batch), page fetches and 414 splits. Each transport attempt increments the existing
client request counter, including retries. Existing debug HTTP spans describe each physical page and its attempt count.
The cap limits workload; it does not guarantee provider latency or prevent backend timeouts.

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
The index's completed group membership selects locks without opening receipts. After locking, an unchanged
index revision preserves that selection; a changed revision reselects before exposing data. Required receipt
bodies are read once in that coherent view, including offline evidence inspection. Missing group membership
fails closed into explicit index migration.

[operation_lock](../libs/common/store_lock.py) uses stable `.<store-name>.operation.lock` beside the store. CSV export
holds nonblocking shared `flock` from catalogue selection through run manifest; every store-writing CLI command
holds nonblocking exclusive `flock` throughout. Store gate precedes owner locks. Writers exclude writers/exports;
read-only exports may coexist. Exit, including `SIGKILL`, releases the kernel lock; path remains, with no PID or
stale-lock recovery.
Direct internal producer functions require their caller to provide this outer exclusion when used alongside
exports; they are not independent concurrent writer entrypoints. Public consumers use `alpaca_end_of_day`.

Owners close temporary files and atomically replace journaled outputs: payloads before receipts/index, summary
before acknowledgement, journal removal last. Acknowledgement cannot consume newer notices/hints. Pricing hands
accepted source/endpoints downstream, reuses cold references and inventories only replaced boundaries. Recovery
reconstructs durable inputs; owners clean abandoned staging under ticker locks. Interrupted multi-file publication
blocks readers until owner recovery. Authorities: [pricing storage](../libs/alpaca_pricing_download/storage.py),
[lineage storage](../libs/alpaca_lineage_build/storage.py).
Lineage recovery reselects journal-named pricing sources, notices and hints once after reacquiring locks,
then carries them through summary refresh and acknowledgement. The accepted values expire with those locks.

History replacements retain payload files when newly produced bytes match their recorded digests; matching
files are not reopened, checked or rewritten. Changed payloads, receipt changes and required update notices
still use the normal publication journal. Physical repair is a separate explicit pricing-owner option.

[connect_sqlite](../libs/common/sqlite_connections.py) uses transactions, disk rollback journals and write
`synchronous=OFF`. Producers own native recovery; readers/previews fail closed when it is needed. No forced
synchronization: process interruption is covered, machine/power failure is not.

## Serving catalogue

[policy.py](../libs/alpaca_serving_catalogue/policy.py), `evaluate`, consumes completed IDs/assignments and exact
prices without upstream authentication or identity-rule replay. Numerical checks cover complete selected history:

- Eight views share one nonempty unique market-session set and deterministic source selection. Conflicts are
  ineligible; unreadable publications leave work pending.
- Timestamps are New York session midnight. Positive OHLC; open/close and positive VWAP within low/high.
  Nonnegative exact-decimal volume and integral trade count. Ordinary activity requires both positive, except
  split-adjusted volume may round to zero. Flat positive OHLC with zero volume/count/VWAP is the explicit
  zero-activity exception. Upstream already guarantees finite exact numbers.
- Explain continuity; never fill missing sessions. A rename gap of at most five sessions requires complete action evidence,
  unchanged UUID/CUSIP/exchange, one matching dated name-change action, compatible exact symbol/CUSIP lookups,
  no conflicting occupant/event and no source observations in any view, including unresolved rows.
  Exclusions neither assign observations nor merge identities.
- Relevant unresolved assignments constrain their dates. Explicit candidate UUIDs remain conflicts; anonymous
  observations under a former ticker are relevant only within its published ownership bounds, when available.
  If the complete assigned history passes numerical and session checks, serving may publish its assigned suffix
  strictly after the last relevant unresolved interval. Uncertainty reaching the latest assigned session still
  vetoes publication. Readers and CSV exports consume only the advertised date range. This assigns no unresolved
  rows and makes no new identity links.
- Correlation's exact certified vacant-ticker and pre-entry carry-forward non-observation dispositions are not
  unresolved observations: serving trusts each producer certificate without replaying it. Additional reasons
  retain their ordinary uncertainty meaning. Empty/entirely unresolved groups complete without public identities.

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

Partial publication retains its full-history numerical checkpoint. When relevant uncertainty still overlaps,
evaluation reads the selected history to count and project the supported subrange; unchanged maintenance reuses
the completed decision without reads. Earlier-policy checkpoints migrate only when the complete group's pricing
generations and row ownership selections are identical; current eligibility is evaluated again.

Reuse covers the latest validated session in every view; changed coverage can clear negative cross-view decisions.
Raw continuity labels remain separate from public all-view labels. Gaps use current evidence; changed group inputs
refresh bounded gap-presence reads without repeating unaffected numeric checks. Physical aliases share decoding.
Checkpoints/decisions commit together. Policy changes require explicit rebuild of the single current checkpoint format.

The policy-5/6/7/8→9 exception translates policy-5 scalar ownership labels to lists and reuses completed facts only
when the whole group's pricing generations and every row/source/owner/label selection match. Policies 7, 8 and 9 change
no numeric history check; eligibility is always recalculated from the current completed receipt and its producer
dispositions. Comparison uses selected checkpoints/assignments without price reads. Changed histories receive
ordinary evaluation; both paths project current metadata. Consumers still require current policy. No extension to
other policy/calendar changes.

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
An exact producer-issued unchanged-decision handoff may accompany a lineage version-header publication.
The journal persists its old/new complete-group receipt generations and unchanged pricing inputs into the
version-2 serving work transaction. Serving trusts that guarantee, retargets matching references atomically,
and preserves decisions and numerical checkpoints. A missing or mismatched handoff retains ordinary selected
maintenance. Explicit work conversion preserves queued obligations; no implicit universe reconstruction occurs.

Maintenance first accepts the compact catalogue state and complete work queue. An empty queue returns without
loading catalogue group/member maps or lineage membership; only selected work requires those maps.

Serving retains the accepted store compatibility and lineage index in a view owned by its held lineage writer
lock. Selected readers consume those entries without reopening the index, rechecking its protocol or repeating
store path checks. The view is restricted to the owning process/thread and expires before the writer lock is
released. Pricing locks, source-generation checks and pending-publication checks still protect each selected
group. Ordinary readers without this writer-owned view retain selection, locking and revision-based reselection. Runtime-only
changes to this handoff preserve the serving policy and its completed checkpoints.

Explicit bootstrap covers indexed groups and may project missing membership from receipts in bounded batches,
preserving digests/identity decisions. It may read full selected histories; ordinary maintenance/readers never
bootstrap. Policy rebuild follows this path; incompatible schemas need supported explicit migration.
[Operations](operations.md#serving-catalogue-maintenance); [public snapshots](public-api.md#online-catalogue-reader).

## Parquet conversion

[Exports](exports.md#parquet-conversion) owns conversion, memory and publication semantics.
