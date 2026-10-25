# Technical contracts

## Trust and ownership

Follow the repository-wide [storage trust rules](../AGENTS.md#trust-before-touching-storage).
A completed selector is authoritative on first read: check its required schema/version/token, then consume
selected work. Reacquired locks require generation checks, not existence/symlink inventories, stat-based
cache keys, payload hashing or upstream rule replay. Producers validate new outputs/destinations and
compare recorded digests; explicit authentication audits existing data. Readers fail closed and never repair.

The [invocation context](../libs/alpaca_lineage_build/context.py) retains coherent accepted compatibility,
index and summary views. Revisions and publisher-returned changes refresh them; there is no persistent
context cache or alternative authority. Changed inputs invalidate their dependent work only.

## Standalone artifacts

Paths relative to the standalone store:

| Artifact | Authority |
|---|---|
| `store.manifest.json` | Store format. |
| `end_of_day/pricing/<TICKER>/source.manifest.json` | Pricing generation, boundaries and payload map. |
| `validator_first_last_sessions.json` beside source | Source-bound exact first/last rows for every view. |
| Pricing `completed_years`, `current_year`, `current_month` | Selected CSV/gzip boundaries. |
| `end_of_day/manifests/<TICKER>/update.json` | Unconsumed inclusive changed range (`from`, `to`). |
| `end_of_day/lineage/<TICKER>/lineage.manifest.json` | Evidence, input generations and chronological assignments. |
| `end_of_day/lineage/index.sqlite3` | Groups/generations, complete membership, lookup projection, serving obligations. |
| `end_of_day/catalogue/serving_catalogue.sqlite3` | Serving selector, public fields, decisions and previous contributions. |
| `universe_updates.sqlite3` | Accepted observations, registration, targets, attempts and outcomes. |
| `pricing_check_summary.sqlite3` | Live-check baseline. |
| `lineage_update_check.sqlite3`, `download_completion.json` | Disposable scheduling hints, never data authority. |

Effective history is completed years + current year excluding active month + authoritative current month.
Rollover consolidates; ordinary month updates preserve cold payloads. Eight logical views may alias exact-copy
physical files. Columns are `timestamp,open,high,low,close,volume,trade_count,vwap`; provider JSON numbers become
exact strings without binary-float arithmetic. Structural validation rejects wrong symbols, malformed values,
duplicate/out-of-range timestamps and incomplete pagination; it does not establish market validity or identity.
See [pricing schemas](../libs/alpaca_pricing_download/contract.py) and [storage](../libs/alpaca_pricing_download/storage.py).

Ordinary updates compare every view's first/last endpoints; mismatch requires full replacement.
The integrated live check compares only the summary's last `all` row; any changed field requires all-view
retained-span replacement before append. These checks do not detect every possible interior historical correction.
Replacement notices include removed sessions. [Download](../libs/alpaca_pricing_download/download.py),
[HTTP/pagination](../libs/alpaca_pricing_download/http.py) and [completed responses](../libs/alpaca_pricing_download/responses.py)
own endpoint/acquisition stages, row budgets, exact fallback, overlap checks and bounded retries.
Completed normalized responses are sliced downstream without renormalization; fresh first-session probes remain independent.
After successful full batch pagination, [omitted symbols](https://docs.alpaca.markets/us/reference/stockbars)
have no bars in the requested range; they complete
as empty without individual rechecks. Transport, response-shape and pagination errors leave the action incomplete.
A full replacement that empties a previously nonempty view needs a bounded fresh confirmation at the pricing
producer. Ordinary empty appends and initial empty histories retain batched acquisition.
Completed request executors release response bodies with their consumer; request-size splits preserve ordered
pagination without retaining earlier acquisition cohorts through recursive closures or handled tracebacks.

[Append approval](../libs/alpaca_pricing_download/append.py) binds checked source generations to selected missing
intervals. Publication rechecks generations under ticker locks even when the last row is unchanged.
New summary membership cannot inherit approval. Empty append requires all eight retained views empty;
partial views take ordinary checks. Buffered acquisitions bind either a generation or explicit absence;
a concurrent replacement requires reselection. Retained-span refresh preserves start, end and mapping date.
Explicit historical entity mappings remain pinned across retries and cutoff extensions. Endpoint windows and
corporate-action discovery use the independent observation cutoff. A mapping change requires a complete,
generation-bound replacement; completed acquisition and endpoint responses remain distinct handoffs.

The [pricing producer](../libs/alpaca_pricing_download/activity.py) validates new partitions: trade counts agree
across all views, and views without split adjustment retain raw share volume. Exact decimals preserve share
units; absent rows are not zeroes. The source's `activity_consistent` flag covers the complete retained history
only after complete validation or inheritance of that completed guarantee. Appending to a legacy source cannot
certify untouched cold partitions. Readers trust the published guarantee.

[Pricing ancestry](../libs/alpaca_pricing_download/changes.py) retains at most 64 ancestor generations and
the earliest possibly changed session across all views, including removals; null means identical observations.
It survives acknowledgement and rollover. Missing/expired ancestry proves no unchanged prefix: the selected
affected histories must be reevaluated. This recovery cost never authorizes a store-wide metadata audit.

## Check summary

Each entry contains only `symbol`, saved mapping `asof`, completed source `end_exclusive` and the exact
eight-field `last_all`. Eligibility requires a unique stored same-symbol active US-equity lookup and nonempty
`all` endpoint; stored activity is not online availability. Last-session/asof pairs group checks: the exact last
row supplies its session window while saved `asof` remains the independent provider identity selector. The
completed exclusive end starts any append independently of both. Epoch/revision/coverage and separate
owner-pending markers control publication; the summary contains no identities, other views, first rows or saved
comparison outcomes.
Pricing owns baselines; lineage owns membership. Missing/incomplete/pending coverage fails closed; snapshot
tokens guard comparisons without ticker enumeration. Authorities: [storage](../libs/alpaca_pricing_download/check_summary.py),
[eligibility/rebuild](../libs/alpaca_lineage_build/check_summary.py).

Daily recovery finishes only the summary's named pending inputs and their complete lineage groups before
ordinary selection. If recovery leaves inputs pending, selection stays blocked. A pending summary is never
empty membership and never authorizes retained-history checks for unaffected active sources.

## Universe observation and work state

An observation retains duplicate labels. Unique symbol records may supply metadata; missing/duplicate
records need exact fallback, and CUSIPs always remain exact. Absence proves neither inactivity nor unavailable
prices. [Discovery](../libs/alpaca_lineage_build/catalogue.py) and [projection](../libs/alpaca_lineage_build/projection.py)
separate current evidence from retained last-positive own-label facts; absent historical times stay unknown.

[Registration](../libs/alpaca_lineage_build/registration.py) persists intentions/work before publishing and
acknowledging registry union. It freezes first-read label selection across execution and batch memo;
later file changes affect later commands. Registry membership and work records cannot establish publication.
Work initialization is independent of lineage/summary coverage. Interrupted registration is local recovery,
not a provider failure subject to retry delay.

[Work contracts](../libs/alpaca_lineage_build/work_contract.py) and [durable state](../libs/alpaca_lineage_build/universe_state.py)
retain stable obligation IDs, accepted endpoints, evidence fingerprints, complete groups, refresh/replacement
requirements and retry state. Persist dependency/correction requirements before requests. Lineage stamps only
satisfied locked inputs: coverage alone cannot erase an unperformed refresh, correction or failed check.
Completed replacements/extensions survive interrupted acknowledgement; a matching last row alone proves neither.
The acquisition route is reselected from these requirements and the completed summary. A saved `acquire` flag
cannot force a summarized maintained source away from live check → approved append after recovery.

Every lineage entry point reconciles used lookup facts before acquisition. Identity changes require complete-group
replacement, including evidence discovered for new dependencies. Pricing binds the evidence fingerprint and source
generation; lineage cannot stamp different facts. Catalogue-versus-exact provenance alone does not invalidate
identical facts. A newly accepted mapping change binds every affected member to one group mapping fingerprint,
including members whose own lookup is absent or unchanged. Retries and metadata-only changes preserve that
requirement. Selection or evidence reconciliation binds older pending replacements with no fingerprint from
accepted group evidence, retaining existing non-null requirements and completed pricing handoffs.
A required-member failure blocks sibling publication. Failure transactions refresh in-memory
policy without losing attempts/deadlines when obligations expand. [Operator retry/coverage rules](operations.md#recovery-registry-and-retries).

[Status](../libs/alpaca_lineage_build/status.py) reads compact boundaries and only the pricing journals selected
by scheduling/summary work, plus the lineage marker under its shared gate. No payloads, receipts, ticker census
or recovery. Each boundary is coherent separately; the report is not a cross-producer transaction.

## API plan

Pure [build_plan](../libs/alpaca_data_api_solver/__init__.py) maps `steps[].needs[]` to versioned ordered actions.
The source defines exact need/action fields, strategies, delivery slices and batching limits. Needs require
nonempty reasons; within-step needs are independent. Exhaust pagination before advancing; dependent needs
must be constructed after responses.

Bars use 1Day/SIP/USD, separate adjustment requests and mapping `asof` (not adjustment vintage).
Date-only solver ranges mean New York midnight; timestamps require a timezone. Corporate-action bounds are
inclusive. Compatible symbol/range requests batch; adjustments/asof remain distinct. Symbol lookups may use
catalogue batches with exact fallbacks; CUSIPs never do. Page estimates exclude response-dependent corrections
and expansion; neither a global HTTP minimum nor an ETA is promised.

## Lineage reuse

[Reuse gates](../libs/alpaca_lineage_build/pipeline.py) require matching complete-group inputs, dependency and
recorded-absence sets, compatible versions, no pending pricing work and no explicit metadata refresh.
Evidence fingerprints remain independent of history approval: a missing approval republishes the completed
decision through its journal before acknowledgement, without pricing decode/correlation. Already projected
approval needs no group reopening. Rule-only changes reuse approved evidence. The lineage producer may promote
a completed observation decision through its journal when the exact version transition proves its assignments
unchanged; otherwise it recalculates the selected complete group. This migration proof uses selected receipt
metadata only and is not repeated after the current-version handoff is published.

[Observation selection](../libs/alpaca_lineage_build/observations.py) is a separate producer decision from
identity assignment. For each identity/session, a complete eight-view bundle from the latest independently
observed own-label source supplies every numerical view. This is an explicit source-selection policy, not proof
of historical completeness. Equal complete bundles can collapse; unresolved source authority remains explicit.
The producer preserves all accepted dated labels independently, including ambiguity. Readers and exports consume
these decisions without repeating selection or activity checks. Prior lineage versions require one selected-group
rebuild using retained evidence and prices; subsequent unchanged groups reuse the completed decisions.

Complete established correlation is evaluated before either occupancy fallback. For a source that remains
unresolved, a reused active label may be bounded from its first positive provider action after all competing UUID
actions, provided exact active symbol and CUSIP lookups agree; the unsupported prefix remains ambiguous. A foreign-
label distribution may be ignored for occupancy only when the same completed group contains the exact own-label
action, its source assigns that session to the exact UUID in all eight views, current-label actions bracket it, and
no related structural action exists. These rules preserve provider evidence and do not derive identity from equal
prices, returned-bar timing or external research.

When overlapping bundles disagree in any numerical view and the selected request's mapping date lies outside
its accepted occupancy, lineage requests only that source's full retained span with an observed own-label mapping
date. Accepted departures across CUSIP/UUID changes within the same public identity also bound this route.
Equal bundles, disjoint histories and superseded sources do not trigger acquisition. A source containing multiple
public identities cannot be globally remapped to one occupant. Pricing publishes the correction before lineage
publishes its selection; failure leaves the new selection unpublished.

An older occupant's closure does not disprove the source-wide mapping when that same source independently
observes a later own-label occupant. The per-identity observation choice remains valid without a replacement.
For identity inference, a cash-terminal date alone cannot establish unbounded prior ownership across a distinct
occupant with positive dated evidence on both sides of that boundary. Existing dated intervals and accepted
split continuity remain authoritative; provider actions and candidate relations remain retained evidence.

A request cutoff is not ownership evidence. After a verified ticker-reuse gap, a current active suffix that is
complete through its last observed session remains bounded to those observations when a later request horizon
adds no contradictory owner. This requires one exact active symbol lookup, complete agreeing view grids, no
interior missing session, no current UUID in the earlier episode and no conflicting UUID in the suffix. A closed
earlier episode is assessed against its own evidence; an incomplete later request does not revoke it.

A deliberately historical mapping date is not a general observation cutoff. When it is itself the final session
of a complete informative eight-view prefix, an immediate market-session gap follows, and complete provider
evidence later names one inactive predecessor and a distinct-symbol, distinct-CUSIP and distinct-UUID successor,
the producer preserves that prefix and stops the as-of ownership inference at the gap. Observations after the gap
and before the provider endpoint remain unresolved unless the ordinary independent mapped-continuation proof owns
their exact sessions. This establishes no effective transition date and does not use returned-bar timing or
external research as dated ownership evidence.

The terminal continuous-ticker fallback does not freely union disconnected UUIDs. Existing accepted components
remain valid. A new two-component union additionally requires a complete informative history without prolonged
carry-forward and retained provider structural-action evidence; three or more disconnected components remain
unresolved. Independently certified closed-history departures keep their narrower behavior. Ordinary inactive
sessions within an established identity do not create a new component or invalidate it.

A mapped predecessor source can contribute successor-period rows only after an accepted cross-UUID provider
edge, an as-of selection inside the predecessor interval, unique incoming and outgoing branches, and an
independently correlated successor source establish ownership for the exact sessions. Every field in all eight
selected views must match. Correlation then publishes the successor-owned assignment and observation selection
marks the predecessor copy superseded. Equality alone, incomplete views, reused labels, competing branches and
different values leave the rows unresolved.

Corporate-action dates retain their provider field semantics. The producer uses exact supported endpoints and
does not substitute an earliest date, next session, returned-bar boundary, researched marketplace date or
ticker-specific override. Missing or conflicting dated evidence remains unresolved; external research may audit
the result but is not lineage input.

[Selection](../libs/alpaca_lineage_build/selection.py) retains previous published membership until retirement.
When evidence links disappear, the producer correlates replacement components separately and publishes all
replacements through one journal. Consumers never repair incompatible groups or infer replacement membership.
Missing indexed membership requires explicit `lineage migrate-index`, not an implicit receipt crawl.

Completed scheduling hints are authoritative; lost coverage permits one explicit source-generation census,
with no receipts for matching sources. [Metadata selection](../libs/alpaca_lineage_build/pipeline.py) plans from completed
pricing results (including accepted ranges and effective pending changes for already-covered requests) and
accepted index ranges. Covered acquisition consumes its source boundary without reopening the endpoint
validator. After interruption, only selected pending source receipts supply
missing planning facts, one bounded cohort at a time; publication reselects generations under its own locks.
Prefetch reads selected lineage receipt bodies for evidence queries;
[EvidenceClient](../libs/alpaca_lineage_build/metadata.py#L57) reuses containing completed action intervals only within
an invocation. Failed/incomplete responses establish no coverage; wider requests/new invocations acquire evidence.
Conflicting action IDs block only evidence containing those IDs, including previously cached intervals.
Unrelated symbols in a completely acquired and routed batch remain usable; transport or routing failure
leaves the affected request incomplete. Conflicts remain blocking for that invocation without choosing a winner.
Ordinary action continuation starts at the prior active month; older corrections require explicit refresh.
An unchanged action's completed positive CUSIP-to-ID lookup is reusable when the fresh catalogue contains
that exact asset ID with identical, unambiguous asset facts. This consumes the published mapping; it does
not infer a CUSIP from a ticker. New or changed actions, changed/missing/ambiguous asset facts, negative
lookups and explicit refresh retain exact acquisition. A later traversal that replaces an already consumed
lookup restarts metadata assembly using the operation's now-authoritative exact lookup cache.
Known dependencies retain continuation/refresh obligations; new dependencies need full evidence.
Expanded groups reselect under sorted enlarged locks; decoded input reuse requires matching generation/scope.
Assignment continuation uses the pricing producer's ancestor/change-range proof and
[single-identity gates](../libs/alpaca_lineage_build/resume.py). With compatible resolved assignments, unchanged
evidence and a complete added session range, correlation receives only the added rows. Receipt assignment
summaries supply accepted counts and endpoints without reopening closed assignments. Older receipts acquire
that summary from their selected assignment bodies, without reopening prices or rerunning old decisions.
Month and year rollover combine accepted assignments with new decisions; only required output bodies are
opened and published. Historical corrections, missing change proof, changed evidence, unresolved assignments
and dependency groups retain complete-group reconciliation.
When the pricing producer confirms that every group input has unchanged rows and identity evidence remains
unchanged, the group carries its accepted assignments forward without pricing reads or rule execution.
This includes unresolved assignments and empty histories. Advancing the cutoff or crossing a month/year
boundary alone does not invalidate those decisions; rollover reads only the assignment bodies it must regroup.

An exactly rejected missing auxiliary can retain metadata and complete its group alongside independently
supported histories, without inventing an empty source. This exception cannot cover arbitrary failure, seeds
or retained series. The [absence owner](../libs/alpaca_lineage_build/acquisition.py) records rejected request,
evidence and obligation before acknowledgement; auxiliary-only reselection preserves its group.

Name changes establish dated endpoints, not unlimited predecessor history. Conflicting effective/process
dates or component-arrival evidence leave the disputed window unresolved. A different UUID joins only with
independent continuity proof: an already accepted identity-preserving event or an exact, informative raw
predecessor prefix through the boundary, with complete matching session grids in all eight views. The proof
uses selected group inputs, never decisions derived from the disputed edge. Independent divergent concurrent
sources retain their own histories. A shared UUID still needs bounded historical-label evidence: its own
source and an informative exact matching suffix, an existing independent-history proof, or an accepted bounded
round trip. Exact matching historical spans can establish row ownership without certifying a UUID join;
unmatched spans remain unresolved. No source can extend that evidence into an earlier unobserved prefix.

## Publication and recovery

Stable `.download.lock` inodes provide exclusive producer/shared reader `flock`; one lineage `.worker.lock`
serializes writers. Sorted pricing locks and a brief publication gate protect selected groups, allowing
unrelated reads/publications. Readers select → lock → recheck before consuming. Lock-file existence is not ownership.

One stable `.<store-name>.operation.lock` beside the store is the outer operation gate. CSV export holds a
nonblocking shared `flock` from catalogue selection through its run manifest; every store-writing CLI command
holds a nonblocking exclusive `flock` for its complete operation. The order is store gate before existing owner
locks. This excludes writer/writer and writer/export overlap while allowing read-only exports together. Process
exit, including `SIGKILL`, closes the descriptor and releases the kernel lock; the path remains because its
existence is not lock state, and there is no PID or stale-lock recovery.

Owners close temporary files and atomically replace outputs under journals: payloads before receipts/index,
summary before acknowledgement, journal cleared last. Acknowledgement cannot consume newer notices/hints.
Pricing hands accepted source/endpoints directly downstream, reuses cold references and inventories replaced
boundaries only. Recovery reconstructs durable inputs; owners clean abandoned staging under ticker locks.
Interrupted multi-file publication blocks reads until owner recovery. Authorities:
[pricing](../libs/alpaca_pricing_download/storage.py), [lineage](../libs/alpaca_lineage_build/storage.py).

[SQLite](../libs/common/sqlite_connections.py) uses transactions, disk rollback journals and write
`synchronous=OFF`. Native recovery belongs to producers; readers/previews fail closed when recovery is needed.
No forced synchronization: the contract covers process interruption, not machine/power failure.

## Serving catalogue

The [policy evaluator](../libs/alpaca_serving_catalogue/policy.py) consumes completed IDs/assignments and exact
prices, without upstream authentication or identity-rule replay. Eligibility covers the complete selected
identity history:

- All eight views have the same nonempty unique market-session set and deterministic source selection.
  Conflicting histories are ineligible; unreadable publications leave work pending.
- Timestamps are New York session midnight. OHLC is positive; open/close and positive VWAP lie within low/high.
  Volume is nonnegative exact decimal; trade count is nonnegative integral. Ordinary activity requires both
  positive, except split-adjusted volume may round to zero. Flat positive OHLC with volume/count/VWAP all zero
  is the explicit zero-activity exception. Finite exact numbers are already guaranteed upstream.
- Continuity is explained, never manufactured by filling/trimming. A rename gap of at most five sessions
  needs complete action evidence, unchanged UUID/CUSIP/exchange, one matching dated name-change action,
  compatible exact symbol/CUSIP lookups, no conflicting occupant/event and no source observations in any view,
  including unresolved rows. Recorded exclusions neither assign observations nor merge identities.
- Unresolved assignments affecting the advertised identity span veto serving; unrelated/outside-span uncertainty
  does not discard supported history. Empty/entirely unresolved groups complete with no public identities.

Inactivity, nontradability or missing descriptive metadata alone does not disqualify. Coverage means approved
stored history, not lifetime completeness or current freshness. Policy version/calendar digest select both
positive and negative completed decisions.

Names, exchanges and asset classes come only from own-symbol observations of the identity's observed UUIDs.
Current CUSIP lookups do not supply historical descriptions. Observation timestamps remain observation times;
historical effective dates require dated provenance. Missing descriptions are omitted.

[History checkpoints](../libs/alpaca_serving_catalogue/reuse.py) record selected generations, ownership runs,
source-coverage ordering, numeric outcomes, labels and gap evidence, never copied prices. Reuse is per identity
across merges/splits; independent identity changes do not invalidate its checks. Proven appends skip prior rows
and closed payloads; suffix-containing containers may still contain older rows. Changed adjusted-source selection,
ownership, replacement or missing ancestry requires affected-history evaluation.

Reuse covers the latest validated session across **all** views, so changed coverage can clear a negative
cross-view decision. Raw continuity labels are retained separately from public all-view labels. Gap explanations
use current evidence; changed group inputs refresh bounded gap-presence reads without repeating unaffected
numeric checks. Physical aliases share decoding. Checkpoints and decisions commit together; policy changes
require explicit rebuild of the single current checkpoint format. During the policy-5 to policy-6
transition, the producer translates scalar ownership labels to lists and reuses completed history facts
only when the complete group's pricing generations and every row/source/owner/label selection are identical.
This comparison uses already selected checkpoints and assignments, without price reads. Changed groups
receive ordinary evaluation; current metadata is projected in either case. Consumers still require the
current policy. The migration does not apply to other policy or calendar changes.

[Catalogue storage](../libs/alpaca_serving_catalogue/storage.py) atomically replaces a component's old decisions,
public fields, dated matches and ID/group/input references. Incompatible duplicate IDs abort publication.
The selector is last: schema, policy, time, completeness and transaction token; readers never hash the database.
Compact selections serve status/preview/public snapshots and unchanged work; only selected changed groups open
checkpoint bodies. Matching contributions are neither revalidated nor rewritten.

[Maintenance](../libs/alpaca_serving_catalogue/producer.py) follows published membership plus previous catalogue
contributions, including absent dependencies, without rediscovering evidence closure. The lineage transaction
records exact-generation [serving obligations](../libs/alpaca_serving_catalogue/work.py) before upstream
acknowledgement. Catalogue commit precedes obligation deletion; post-commit retries reuse completed work without
history reads. The lineage worker serializes maintenance; selected pricing locks span decision publication.
Failures and focused commands remain scoped; unrelated obligations stay pending.

Explicit bootstrap covers indexed groups and may project missing membership from receipts in bounded batches,
preserving digests/identity decisions. It may read full selected histories; ordinary maintenance/readers never
bootstrap. Policy rebuild uses this path; incompatible schemas need a supported explicit migration.
[Operational scope](operations.md#serving-catalogue-maintenance), [public metadata/snapshot semantics](public-api.md#online-catalogue-reader).

## Parquet conversion

[Conversion](../libs/alpaca_bloodhound_export/command_line.py) uses complete resolved eight-view histories to
produce fully adjusted OHLC, raw close/volume and a jointly inferred split factor. Exact
[factors](../libs/alpaca_bloodhound_export/factors.py) and [projection](../libs/alpaca_bloodhound_export/projection.py)
are authoritative; schema compatibility is not independent corporate-action certification. Assumptions and full
public-ID mappings live in the `alpaca_bloodhound` footer.

Convert full histories before filtering. Ambiguous labels or incompatible factors reject an identity;
ID collisions, snapshot changes, memory exhaustion or writer failures abort the export.
Accepted output stays within the RAM allowance, serializes
once, checks source freshness and installs atomically; no spill, diagnostic sidecar or Bloodhound registration.
[Commands and output constraints](operations.md#read-and-export).
