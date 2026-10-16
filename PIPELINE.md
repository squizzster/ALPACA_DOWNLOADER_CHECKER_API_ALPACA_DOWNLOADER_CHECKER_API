# Pricing acquisition and the existing catalogue pipeline

The standalone [`download_alpaca_pricing.py`](download_alpaca_pricing.py) owns pricing-only acquisition. It never calls
the existing catalogue pipeline, and its successful publication does not depend on lineage or identity resolution.

The existing combined calculation path is the monthly acquisition-to-catalogue cascade invoked directly by
[`run_alpaca_end_of_day_ticker_pipeline.py`](run_alpaca_end_of_day_ticker_pipeline.py) and scheduled as bounded
transactions by [`build_alpaca_end_of_day_clean_room.py`](build_alpaca_end_of_day_clean_room.py).
[`download_alpaca_end_of_day_bars.py`](download_alpaca_end_of_day_bars.py) is a compatibility executable for the same
focused orchestrator. Those older commands remain coupled; the new standalone command is a separate pricing entrypoint,
not an alternate identity-rule engine. The online catalogue reader retains its separate format; the standalone
resolved reader consumes the compact twins directly as described below.

## Standalone pricing execution

1. Select and deduplicate repeated symbols, a CSV/line file, or the tracked universe. Freeze one request end and run
   at most three ticker workers. One worker owns one entire ticker transaction through steps 2–8; adjustments are not
   separate jobs. All workers share one paced 199-request-per-rolling-minute budget and HTTP retry cooldown.
   Ticker failures are reported and do not stop the remaining batch; final results retain input order.
2. Take the ticker's exclusive lock and finish interrupted publication. Reject backwards ends and any `FROM` that
   excludes stored observations, including on force. Recovering an old request does not suppress explicit force.
3. Before ordinary acquisition, compare live first/last sessions with the published validator in all eight views,
   using the update's symbol-mapping date. Older sources bootstrap the validator once from their stored pricing.
4. Initial acquisition, empty history, endpoint mismatch, earlier `FROM`, or force selects full acquisition from the
   retained history start (default 2016). Otherwise request the stored active month through the new end.
5. Fetch all eight adjustments, exhaust pagination, parse decimal numbers without binary-float conversion, and check
   field shape, requested range, and strict timestamp ordering. Do not evaluate market validity or security identity.
6. Check acquired endpoint overlap without further requests, then re-request only the candidate first session per
   nonempty view. A mismatch restarts as full acquisition once; continued instability fails without publishing.
   During an ordinary in-month update, rebuild only the current-month pricing boundary, validator, and source receipt.
   On month rollover, replace the previous active month's overlap in the year snapshot with its finalized response,
   incorporate newly acquired rows, and publish the new year snapshot and current month. On year rollover, merge the
   completed portion into the single completed-years boundary exactly once.
7. Calculate pending work from actually changed session dates, extending any unconsumed range. Full replacement
   covers old, new, and pending date extents, including removed sessions. Force is also a clean replacement; no clipped
   edges are retained. An unchanged ordinary acquisition creates no new work notice.
8. Verify the CSV/gzip round trip, stage only changed files, and journal publication. Install pricing with its source
   receipt last, retire superseded partitions, publish the pending notice, clear the journal, and release the lock.

The year snapshot includes the active month as observed at initial acquisition or consolidation; subsequent month
updates do not rewrite it. Consumption replaces its overlapping month rather than appending duplicate observations.
Once the validator exists, ordinary endpoint checks do not read cold/year gzip payloads. Rollover reads the boundaries
it consolidates. Matching in-month updates do not rewrite completed-years or year payloads.
The standalone lineage consumer holds the same exclusive lock through durable processing and acknowledgement. Exact date-range,
locking, and overlap semantics live in
[Technical contracts](docs/technical-contracts.md#standalone-pricing-source-contract).

## Standalone lineage execution

1. Take the single lineage-worker lock and finish any interrupted publication. Select pending notices, explicit
   symbols/file, or all locally published sources; changed rule/calendar versions also invalidate their prior results.
2. Expand the dependency closure through old relationships and UUID evidence. Lock known pricing inputs in sorted
   order. Trust completed pricing receipts; reject an incomplete pricing transaction without repairing it. A missing
   starting ticker takes the same downloader handoff as step 4; absent a published range, use downloader defaults.
   Before loading price rows or acquiring metadata, return `unchanged` for a completed group satisfying the
   [reuse gate](docs/technical-contracts.md#resume-and-invalidation). Otherwise continue through the stages below.
3. Resume corporate-action acquisition from the previously active month, retaining earlier normalized evidence;
   refresh asset/CUSIP lookups. First acquisition, older invalidation, changed rules, or `--refresh-metadata` acquires
   the full evidence range. Discover newly related labels to a bounded fixed point.
4. If related pricing is missing or has insufficient request coverage, release pricing locks and delegate to the
   downloader. Then reacquire the full lock set and reload the latest notices and inputs. Published empty responses
   are acquired evidence, not missing downloads. No separate request queue is created.
5. Reuse approved closed single-identity assignments when their input/evidence/continuity gates hold; calculate only
   the active month. Otherwise run the established preliminary, cross-source and fixed-point correlation passes over
   the affected chronology. Do not repeat pricing authentication or market-price validation.
6. Publish completed-years, year snapshot and authoritative month assignments. Preserve unchanged receipts; retire
   consolidated month/year files. Ambiguous rows remain explicitly unassigned; dependency relationships never merge IDs.
7. Take the brief publication gate while holding group pricing locks. Install the affected group under a recoverable transaction, update its small dependency index, then acknowledge
   exactly the handled pricing notices. A crash may repeat acknowledgement but cannot consume a newer generation.

No public catalogue is produced. The [lineage contract](docs/technical-contracts.md#standalone-lineage-contract)
defines the trust boundary, bounded work, chronological reuse and internal downstream handoff. The
[restart guide](docs/operations.md#restart-and-catch-up) distinguishes pending work from an all-local-source scan.

## Standalone reader execution

[`read_alpaca_pricing.py`](read_alpaca_pricing.py) enters `alpaca_end_of_day.StandaloneReader`:

1. Select the input group under the brief shared publication gate, release it, then acquire sorted pricing shared
   locks. Reacquire the gate to confirm selection and load metadata; retry a changed selection at most three times.
   Keep only pricing locks through consumption. Report busy by default; `--wait` opts into waiting. Never recover
   or acknowledge a producer transaction. Unrelated lineage work can continue.
2. Select the source's indexed lineage receipt and its existing group. Require completed, current-rule receipts
   selecting the same pricing generations, without pending work. No universe scan or new persisted index is needed.
3. Apply the published month-over-year assignment overlay. Return every matching public ID for resolution, or require
   an explicit ID when several match. Unresolved rows do not become history; resolved portions remain usable.
4. Load only the selected adjustment's contributing payloads through their manifest aliases. Assemble one ID's
   continuation using the established history-source selection policy, then filter inclusive session dates.
5. Release the locks and return exact values as JSON or CSV. No downloads, pricing copies, lineage calculations,
   upstream re-audits, or online-availability decisions occur.

See the [consumer contract](docs/public-api.md#standalone-resolved-pricing-reader) for lookup scope and source preference.

The existing combined cascade below has one calculation owner per boundary. A downstream stage consumes a trusted completed boundary, never
repeats that boundary's rule calculation, and never writes its artifacts. Publication-time authentication and ordinary
trusted consumption are distinct operations; the exact contract is in
[Technical contracts](docs/technical-contracts.md#trust-operations).

## One ordered execution path

The [canonical monthly orchestrator](libs/alpaca_end_of_day_v6/pipeline.py) currently owns this exact order:

1. Validate credentials and freeze one exclusive request end and one month-opening request start.
2. Normalize and deduplicate submitted labels.
3. Resolve every submitted label through exact Alpaca asset lookup before opening the write transaction.
4. Initialize or verify the sole monthly store and acquire its exclusive publication lock.
5. Publish the recovery marker.
6. Expand canonical symbols to the current source/identity closure when a catalogue already exists.
7. For each submitted canonical symbol, request all eight adjustment views sequentially and publish either its first
   complete monthly source or the explicitly selected replacement months.
8. Publish or reuse validation for every submitted canonical symbol and load the trusted validation boundary for
   every processing symbol. A failed validation stops the focused command; the clean-room outcome policy described
   below terminates that canonical input without admitting it.
9. Build trusted downstream pricing views, acquire current lineage for submitted canonical symbols, and load trusted
   lineage for every processing symbol.
10. If source, validation, lineage, correlation, warning, candidate, availability, and catalogue receipt identities all
    still form one current chain, retain the complete downstream publication and skip steps 11–15.
11. Run correlation over the processing set three times: preliminary, cross-symbol, and fixed-point confirmation. The
    second and final reports must match. Publish correlation for every processing symbol.
12. Build and publish advisory warnings for every processing symbol.
13. Publish a private identity candidate: a full snapshot for the first catalogue or a closed affected candidate over
    the current catalogue base.
14. Exercise all advertised histories, bind the warning set, and publish exactly one availability decision per
    candidate identity.
15. Publish changed content-addressed source/security objects, affected inventory buckets and roots, retention state,
    and finally the reader-visible catalogue receipt.
16. Attempt to write unlinked `last_run.json` bookkeeping, remove the recovery marker, and release the lock.

The run returns a JSON summary on standard output. Progress is written to standard error. Failure before step 16
retains the recovery marker and readers fail closed until a complete rerun succeeds.

## Clean-room universe execution

The clean-room constructor wraps that exact cascade; it does not own alternate source, validation, identity, or
availability rules. Its immutable run identity binds the ordered CSV universe, raw universe-file digest, exclusive
request end, historical lower bound, all owning boundary/transform versions, and a policy forbidding other runtime
stores and migration.

For each universe position, in order:

1. Reuse its authenticated resolution checkpoint, or resolve the submitted label and publish that checkpoint before
   price work.
2. If an earlier completed label owns the same canonical symbol, publish a label outcome bound to that owner and do no
   canonical work.
3. Otherwise run one complete canonical-symbol transaction with the run's shared bar and identity rate limiters.
4. If a prior attempt already published source, validation, or lineage, explicitly authenticate that completed
   boundary and resume after it. A complete source is never requested again merely because later work was interrupted.
5. Treat failed pricing validation as a terminal rejected outcome: source and validation remain auditable, lineage and
   catalogue membership are absent, and the transaction marker is cleared because the public catalogue was not
   mutated.
6. Publish a terminal `published`, `rejected_validation`, or `canonical_reused` outcome. Any other exception is retried
   a bounded number of times and then stops the scheduler without inventing a terminal result.

Each successful canonical transaction recalculates only the identity/source closure made stale by that new input.
Prior correlation in that closure is not reusable because its immediate input set changed; unrelated correlation and
catalogue objects remain trusted. This is the precise distinction between “never repeat completed work” and “never
ignore a newly required dependency.”

After every input position has exactly one outcome, the constructor:

1. verifies resolution/outcome bindings and canonical-owner uniqueness;
2. proves exact directory membership for admitted and rejected sources;
3. authenticates every source and validation boundary, and every admitted lineage, correlation, and warning boundary;
4. authenticates candidate, availability, the complete content-addressed catalogue, and their generation closure;
5. requires catalogue source membership to equal the admitted canonical set exactly;
6. opens the isolated store through a `DataLibrary` facade and reads all eight adjustments for every online public ID;
7. verifies the catalogue generation remained fixed; and
8. writes the audit report and its digest-bound completion receipt.

Optional activation is a post-audit selection operation, not part of calculation. It retains the prior public store,
renames the audited store into `data_vtest/`, and rolls back if `DataLibrary` does not select the audited generation and
online count.

## Cascade dependencies

```text
source ───────────────┐
  │                   │
  v                   v
validation ──────> correlation ─────> warnings
  │                   │                  │
  v                   │                  │
lineage ──────────────┘                  │
  │                   │                  │
  └────────────> identity candidate <────┘
                           │
                           v
                     availability
                           │
                           v
                       catalogue
                           │
                           v
                     public reader
```

The graph records dependency, not shared ownership. Correlation may consume source rows and lineage facts, but source
encoding remains owned by the source boundary and lineage interpretation remains evidence until correlation applies
its own registered rules.

## Reuse cascade

The pipeline does not use timestamps or file presence alone as cache authority. Reuse follows generation identity:

1. An unchanged expected month receipt retains its existing month directory.
2. Validation reuses a month when its source receipt and predecessor month-result digest match.
3. Lineage retains identical output bytes when source generation, validation generation, and semantic evidence digest
   match, although selected lineage is currently reacquired before equality is known.
4. Correlation is reusable only when its source, validation, and lineage generations match.
5. Warnings are reusable only when their source, validation, and correlation generations match.
6. Candidate, availability, and catalogue receipts must all bind the same current correlation set.
7. Only when that entire chain matches does the pipeline retain the downstream publication without recalculation.

When any required identity differs, work restarts at the owning invalidated boundary. Source installation is local to
selected month partitions; validation can also republish the immediate successor's seam. Correlation and warnings
currently recalculate the complete processing-symbol closure. Catalogue delta publication writes only changed objects
and inventory paths; unrelated catalogue objects are neither read nor rewritten.

## Request and update behavior

Every run requests all eight logical adjustments. There is no partial-adjustment, adaptive-acquisition, repair, force,
or worker mode in the current command.

- A new canonical symbol requests all history from `2016-01-01T00:00:00Z` to the exclusive request end.
- An existing symbol requests only the selected month-partition range. Each nonempty selected partition is replaced
  as a whole; the final one contains only observations before the exclusive request end.
- Without `--request-start`, the start is the first day of the request-end month.
- Without `--request-end`, the end is the current UTC date at midnight.
- A returned row outside the selected replacement months is an error.
- A provider page token is an error rather than permission to publish a partial response.

Bar and identity traffic use separate 199-request-per-minute limiters. A focused multi-symbol command processes all
symbols inside one global transaction. The clean-room constructor shares those limiters across restartable
single-canonical transactions, so no transaction materializes the 18,092-label universe.

## Lineage acquisition boundary

Lineage follows old and new symbols named by corporate actions. A structurally complete name change may expose one
further label only when exact HTTP-200 lookups of both endpoint CUSIPs return US equities on non-OTC exchanges, both
assets carry the same Alpaca UUID, and both return the same single valid symbol. A CUSIP echoed as a symbol is not a
label. Search continues in deterministic order to a bounded fixed point.

Missing fields, failed lookups, different UUIDs, different returned symbols, malformed symbols, OTC or non-equity
assets, and traversal-limit exhaustion all fail closed. Provider `active` and `tradable` flags are not acquisition
gates. This stage publishes evidence only; ownership and UUID-lineage rules still decide allocation and membership.

## The correlation cascade

Rules describe generic evidence shapes. They never name a ticker, silently choose between ambiguous identities, or
rewrite monthly source rows. Earlier row rules have precedence over later fallbacks.

### 1. Dated ownership intervals

`OWNERSHIP_INTERVAL_RULES` in
[`ownership_interval_rules.py`](libs/alpaca_end_of_day_security_correlation/ownership_interval_rules.py) is the only
ownership registry, in this order:

1. `name_change_ticker_transition`
2. `same_ticker_stock_merger_uuid_transition`
3. `cash_merger_acquiree_terminal_boundary`

These rules establish dated ticker/CUSIP/UUID intervals. They do not by themselves join every related UUID into one
security.

For a stock merger with exactly one omitted symbol, `complete_stock_merger_endpoints` in
[`action_identity.py`](libs/alpaca_end_of_day_security_correlation/action_identity.py) may complete the label from
agreeing exact successful CUSIP lookups: both identify the explicit symbol as a non-OTC US equity, with no conflicting
occupancy UUID. Raw evidence and explicit labels remain unchanged. Ownership rule 2 and UUID membership share this
interpretation, then apply their own date/conflict gates. If the inferred transition contradicts earlier name-change
ownership, preserve that chronology while retaining an independently supported membership link. Completing metadata
does not authorize overriding an established historical owner or joining different merger branches.

Overlapping evidence for the exact same ticker/CUSIP/UUID episode preserves its earliest explicit start and earliest
explicit end. A later repeated arrival cannot erase an already dated beginning. Unknown dates do not erase known
dates; separate or touching episodes remain separate, and a constraint overlapping multiple episodes is rejected.

### 2. Row allocation

`ROW_CORRELATION_RULES` in
[`row_correlation.py`](libs/alpaca_end_of_day_security_correlation/row_correlation.py) is the only row registry, in
this order:

1. `exchange_test_symbol_exclusion`
2. `complete_current_identity_suffix_after_ticker_reuse_gap`
3. `inactive_current_identity_suffix_after_ticker_reuse_gap`
4. `dated_terminal_identity_suffix_after_ticker_reuse_gap`
5. `ticker_reuse_gap_guard`
6. `one_session_distinct_uuid_transition`
7. `complete_current_symbol_run_after_authenticated_ticker_reuse`
8. `asof_selected_uuid_lineage_interval`
9. `exact_independently_resolved_predecessor_prefix`
10. `latest_dated_ownership_interval_overlapping_claims`
11. `requested_symbol_dated_uuid_interval`
12. `unique_uuid_candidate_with_warnings`
13. `unresolved_uuid_candidates`
14. `stable_continuous_ticker_identity_set`, the terminal fallback after the registry

Consequential gates include:

- A gap of at least 60 expected sessions blocks generic fallback unless stronger evidence proves an episode.
- A complete current suffix needs at least 100 missing sessions, identical eight-view grids, one exact current UUID,
  and no dated evidence placing that UUID on the left.
- An internally complete inactive suffix may end before the request boundary; an active incomplete suffix remains
  unresolved.
- A dated terminal suffix requires an exact CUSIP lookup, one ownership interval, terminal coverage, and no left-side
  evidence for its UUID.
- Rule 4 also recognizes complete informative traded episodes separated by shared inert carry-forwards or a physical
  reuse gap. Exact dated endpoints and noncompeting ownership identify the episode; the inert bridge is not assigned.
  A complete initial episode before physical reuse needs a nearby exact terminal event and an independently supported
  current suffix. A middle episode needs its own nearby departure; an anchored current suffix can use a nearby exact
  acquirer action. Shared grids, complete calendars and competing-owner vetoes apply across all eight views.
- One-session transition inference uses exact distinct symbol identities, a dated action and matching session grids;
  an explicitly superseded action cannot support it. Conflicting action-endpoint UUID/CUSIP evidence alone does not
  erase the original inference. Exact gates are owned by `assess_one_session_distinct_uuid_transition` in
  [`identity_partition.py`](libs/alpaca_end_of_day_security_correlation/identity_partition.py).
- Continuous-episode assessments require complete, non-overlapping runs in all eight views; recurrence, overlap,
  missing views and ambiguity veto those assessments. Pricing gaps, episode assessments and current-symbol vacancy
  do not revoke an otherwise accepted UUID-continuation edge at membership construction.
- Current-symbol reuse requires the old occupant to have renamed away at least 100 sessions earlier, exact current
  identity evidence, complete matching grids, and no competing structural evidence.
- Within the as-of rule, one explicitly started interval for the exact selected UUID takes precedence over other
  connected predecessor branches. `asof` selects the entity, not historical adjustment values. Without that exact
  dated match, concurrent different-UUID predecessors need comparative continuity evidence or a unique independent
  raw-transaction certificate. A fully dated, uninterrupted same-label chain can distinguish itself from foreign
  converging branches through agreeing exact full names, classes and exchanges, with no prior foreign history of its
  members. Ticker spelling and processing-date order alone do not select the branch.
- After stronger rules, latest-start competition is restricted to occupants of the requested historical label.
  Another label's later start cannot supersede that occupant. A tie or wholly undated same-label overlap fails closed;
  competing branches with no matching source-label interval also remain unresolved rather than reaching an undated
  current-UUID fallback. An independently anchored sole buyer may exclude foreign intervals supported exclusively by
  exact cash-acquiree terminal actions naming that buyer; targets retain their separate histories and memberships.
  Same-UUID overlaps defer to exact requested-symbol provenance.
- A retired requested label can retain same-UUID continuity through a unique chronological accepted rename chain to
  agreeing exact current symbol/CUSIP lookups. This supplements missing historical selection without pretending the API
  performed an as-of mapping or replacing existing directly dated requested-label provenance. Newly inferred backward
  attribution stops at the last prolonged shared inert run before that requested-label epoch. A later halt does not
  erase predecessor history, and earlier independent ownership remains available through the cascade.
- Requested-label fallback cannot infer a new occupant's beginning from another occupant's ending. When different
  UUIDs used that label, an undated-start interval remains unresolved unless a stronger rule already established it.
  One exception preserves the original predecessor's inferred lower bound when all known occupants form one
  nonrecurrent same-symbol handoff chain: each predecessor end equals the next explicit start, and an accepted
  same-symbol name-change or stock-merger edge binds the exact ticker/CUSIP/UUID endpoints and shared action.
  Extra undated occupants, gaps, overlaps, branching, recurrence, foreign-label paths, split-only links and common
  future successors gain no exception. This stays inside the requested-label rule; earlier gap/as-of vetoes still win.
- Exact component-separation or positive cash-dividend ex-date evidence can establish occupancy from that date onward.
  It does not establish inception, end a prior episode, or override a competing positive ownership interval.
- The terminal fallback requires a gapless source, stable same-ticker candidates, one current anchor, agreeing names,
  and no missing or unexpected sessions.
- The fallback can also assign complete informative observed history to an exact inactive split component or narrowly
  supported successive dividend identities. It requires complete grids and no prolonged synthetic carries. Known
  component membership need not establish the per-session UUID/CUSIP vintage; those fields remain null when unknown.
  An inactive split component does not require invented termination metadata or a join to the acquiring security.

Rule 9 is assessed once per source by `assess_predecessor_prefixes` in
[`predecessor_prefix.py`](libs/alpaca_end_of_day_security_correlation/predecessor_prefix.py), using the shared cross-source
loaders. A reused label's complete held prefix must match an accepted dated name-change edge and reach the last market
session before the handoff. Exact raw timestamps and all raw bar fields must match a related source whose prefix was
independently assigned by rule 8 to the exact predecessor UUID/CUSIP/label, with matching feed, timeframe and generation.
All eight source adjustment grids must agree; their adjusted price values need not. At least two traded observations
must have different OHLC values. The same module can certify a complete independently owned donor interval within a
mixed source, including a donor anchored by exact dated occupancy, a sole known predecessor-label occupant, or a unique accepted
same-label split chain. A leading constant zero-trade donor pad is compared in full before trimming; matching only an isolated island is
insufficient. A complete initial prefix may end at a shared inert bridge when the exact outgoing action was processed
later inside that bridge. Conflicting owners, missing observations and recursive donor inference reject the proof.
These certificates can distinguish an as-of branch at rule 8 or let the gap guard reach rule 9. They assign only the
supported interval and do not change membership or claim the remainder. Compact comparison assessments are transient
operator evidence, not another persistent pricing or audit boundary.

Every trusted source row appears exactly once in the correlated or uncorrelated branch.

### 3. Security membership

Row allocation and UUID membership are separate calculations. UUID-lineage edges connect membership only through:

- an authenticated name-change edge;
- an authenticated same-ticker stock-merger edge; or
- an authenticated identity-preserving same-symbol split edge.

A same-symbol split edge requires one dated action, positive directionally consistent rates, distinct old/new CUSIPs,
no ticker change, and exactly one successful US-equity endpoint lookup for each CUSIP. Both assets must carry the
action symbol and different UUIDs. It records provenance but never routes a price row.

An immediately corrective next-day name change that preserves the predecessor UUID supersedes the earlier edge. The
earlier action remains an evidence issue but does not join its unrelated successor. This is the correlation transform
v24 membership rule.

An acquirer adopting its target's ticker is a separate shape: a unique same-day foreign-label rename into the exact
successor, together with an unequal-ratio same-label target merger and three distinct exact endpoint UUIDs, prevents
the target merger from joining the continuing acquirer. The acquirer rename and the target's dated ownership remain.
Contradictory target-continuity evidence prevents that inference. Raw ownership is decided separately by the row rules.

Accepted continuity edges remain at membership construction even when pricing-episode rules distinguish dated rows.
Unrelated reused-symbol occupants have no accepted connecting edge and remain separate. A common corporate successor
does not make parallel predecessor observations interchangeable. The public ID is the SHA-256 digest of sorted unique
member Alpaca UUIDs. Later split evidence can
extend membership and change that digest without changing earlier row ownership; see the
[identity model](docs/technical-contracts.md#identity-model).

### 4. Availability

Correlation never makes an identity online. Availability must read every advertised adjustment successfully, return
at least one bar, load all published dependencies, find no unresolved source conflict, and find no unexplained internal
session or identity boundary. It may classify the result but cannot change membership or values.

## First publication and incremental publication

With no existing catalogue, the candidate mode is `snapshot` and contains every processing source. With an existing
catalogue, the candidate mode is `affected_closure`:

1. Begin with replacement sources and their newly contributed master IDs.
2. Add their prior master IDs from the base catalogue.
3. Expand through every base identity and source symbol sharing those IDs until stable.
4. Carry unchanged closure sources as supporting inputs.
5. Record old IDs absent from the replacement aggregate as retired.

Only this closure is recalculated. Delta catalogue publication preserves all unrelated content-addressed objects and
replaces affected objects, buckets, and roots before writing the final selection receipt.

## Non-divergence controls

- Within the existing combined workflow, acquisition, focused update, recovery rerun, and universe scheduling rejoin
  the same cascade. The standalone pricing downloader deliberately stops before it.
- The standalone pricing command owns no correlation rules and performs no migration. There is no ticker-specific
  rule path or alternate catalogue publisher.
- New ownership or row behavior enters the owning explicit registry at a deliberate precedence point.
- Identity-only edges cannot allocate rows without a separately reviewed row rule.
- Ambiguity fails closed into uncorrelated or offline state.
- A boundary consumes only its declared immediate inputs and never recalculates another boundary's facts.
- Behavioral and serialized changes bump their owning versions and append exactly one runtime changelog entry.
- Registry order is asserted by tests and serialized into correlation output.
- The evidence CLI observes this same standalone execution path; it owns no resolver or ticker-specific overrides.
- The smallest complete affected identity closure is the unit of focused recalculation.
- Publication receipts are written last; a failed transaction retains its recovery marker.

Current schemas and trust semantics are in [Technical contracts](docs/technical-contracts.md). Current command options
and hazards are in [Operations](docs/operations.md).
