# Lineage contracts

[Architecture](ARCHITECTURE.md#lineage-and-identity) maps owners; [policy lab](policy-lab.md) defines executable
profiles and read-only comparisons. These contracts govern completed-group reuse and evidence limits.

## Lineage reuse

[contract.py](../libs/alpaca_lineage_build/contract.py), `reusable_decision`, and
[pipeline.py](../libs/alpaca_lineage_build/pipeline.py), `process_group`, require matching complete-group inputs,
dependency/recorded-absence sets, compatible versions, no pending pricing work and no explicit metadata refresh.
Receipts/index entries bind authorized policy ID, role, implementation tuple and semantic digest. File digest
records exact YAML bytes; formatting is not reuse identity.

Authorize the canonical policy once after journal recovery, before new selection/evaluation/network work.
Recovery validates frozen journal provenance without reinterpreting it through newly installed policy.
Evidence fingerprints are independent of history approval. [Workflow completion](technical-contracts.md#universe-observation-and-work-state)
consumes fresh source-bound approval without republishing an unchanged decision. A completed producer handoff
does not need its group reopened merely to acknowledge that approval. Rule-only changes reuse approved evidence.

[policy_engine.py](../libs/alpaca_lineage_build/policy_engine.py), `evaluate_policy`, constructs detailed row traces,
pass digests and audit summaries only when requested. Mandatory phase/certificate checks still run; published
and explicitly traced digest semantics remain unchanged.

The correlation owner prepares dated occupancy and name-change continuity once before freezing the first
allocation. Chronology that places a later unrelated occupant may disambiguate an earlier rename, but its
fallback interval is not exposed to baseline row allocation. Both phases consume the same completed continuity
certificate. The second phase reserves established correlated segments before invoking row rules, including
their UUID membership, provenance and bounds; completed source reports remain closed. Newly produced fallback
intervals must not contradict those assignments. Only unresolved rows enter the existing cascade, and terminal
fallback still receives the complete source partition. The original phase integrity guard remains mandatory.

## Policy migration

Correlation-78 scopes continuing-acquirer and terminal evidence and consumes completed related-symbol queries. Earlier
policy-less 33/59 and correlation-60 through 75 decisions still require selected complete-group evaluation;
the historical 33/59→34/60 proof remains restricted to its original production target.

[Migration](../libs/alpaca_lineage_build/certificate_migration.py) owns the exact 34/76 or 34/77→34/78 transition.
It tests changed-rule prerequisites and concrete mapping-dependency conflicts using accepted complete-group
metadata, including unavailable auxiliaries. This is an applicability decision, never a prediction of row
ownership. A complete singleton group with at most one accepted UUID cannot invoke the changed certificates.
Affected or unproved groups use the existing evaluator; unchanged groups keep their evidence, assignment
references and pricing generations. Migration requires an empty completed pricing-work boundary before selection.

`lineage migrate-policy` publishes compatible headers through the existing journal and reports the remaining
affected labels for `lineage update --symbols-file FILE`. Its durable exact-generation handoff lets serving
retarget matching completed decisions without reopening assignments or prices, or repeating numerical checks.
Catalogue commit precedes acknowledgement; interruption preserves the handoff and cannot consume a newer
obligation. Run `catalogue rebuild` first to convert older scheduling/work projections, then `catalogue update`
to consume compatible header handoffs. This transition does not authorize any other policy promotion.

## Observation selection

[observations.py](../libs/alpaca_lineage_build/observations.py), `select_observations`, owns numerical selection
separately from identity assignment. Each identity/session takes all numerical views from one complete eight-view
bundle: the latest independently observed own-label source. This selects observations, not historical completeness.
Equal complete bundles can collapse; unresolved authority stays explicit. All accepted dated labels survive
independently, including ambiguity. Readers/exports consume decisions without repeating selection/activity checks.

When overlapping bundles disagree in any field and request mapping `asof` lies outside accepted occupancy, lineage
requests only that source's full retained span at an observed own-label mapping date. Accepted departures across
CUSIP/UUID changes within one public identity also bound this route. Equal bundles, disjoint histories and
superseded sources do not trigger acquisition. A multi-identity source cannot be globally remapped to one occupant.
Pricing publishes correction before lineage selection; failure leaves the new selection unpublished.

An older occupant's closure does not disprove source-wide mapping when the same source independently observes a
later own-label occupant. Per-identity selection remains valid without replacement.

## Ownership evidence limits

Complete established correlation precedes occupancy fallbacks. A still-unresolved reused active label may be
bounded from its first positive provider action after every competing UUID's actions, with exact active
symbol/CUSIP agreement; unsupported prefix stays ambiguous. A separately closed prior label interval does not
veto that later suffix, while any open or overlapping interval still does. The later dated interval likewise
cannot retroactively invalidate the closed interval's already-supported rows. A foreign-label distribution may be ignored for
occupancy only if the same completed group contains its exact own-label action, whose source assigns that session
to the exact UUID in all eight views, with bracketing current-label actions and no related structural action.
Provider evidence is retained; equal prices, returned-bar timing and external research do not establish identity.
Authorities: [occupancy.py](../libs/alpaca_lineage_build/occupancy.py),
[ownership rules](../libs/alpaca_end_of_day_security_correlation/ownership_interval_rules.py).

- **Cash terminal:** a terminal date alone cannot establish unbounded prior ownership across a distinct occupant
  with positive dated evidence on both sides, or across an established closed earlier owner of that label.
  The latter veto applies only to a newly inferred unbounded interval. A separate positive own-CUSIP dividend
  or component action may still bound a later suffix, including an inactive issue, when closed prior intervals
  and no rival or unresolved occupancy overlap it. Its first accepted positive action supplies the lower bound;
  a prior departure or the terminal payment never invents the start.
  Existing dated intervals/accepted split continuity remain
  authoritative; provider actions/candidate relations remain evidence.
- **Ticker-reuse gap:** request cutoff is not ownership evidence. A complete current active suffix remains bounded
  to its last observed session when a later request adds no contradictory owner. Requires one exact active-symbol
  lookup, complete agreeing view grids, no interior missing session, no current UUID in the earlier episode and
  no conflicting suffix UUID. Assess a closed earlier episode independently; incomplete later requests do not
  revoke it. [ticker_reuse_gap.py](../libs/alpaca_end_of_day_security_correlation/ticker_reuse_gap.py).
- **Historical mapping:** `asof` is not a general observation cutoff. Preserve a complete informative eight-view
  prefix when mapping date is its last session, a market-session gap follows immediately, and complete later
  provider evidence names one inactive predecessor plus a distinct-symbol/CUSIP/UUID successor. Stop as-of
  ownership inference at the gap. Later rows before the provider endpoint stay unresolved unless ordinary
  independent mapped-continuation proof owns those exact sessions. This establishes no transition date or
  ownership from bar timing/external research. [predecessor_prefix.py](../libs/alpaca_end_of_day_security_correlation/predecessor_prefix.py).
- **Vacant reused ticker:** a complete prolonged source bridge may be certified as non-observation only between
  accepted direct same-identity renames belonging to two distinct UUID/CUSIP occupants. The label must have no
  intervening owner; all eight views must contain the exact complete market-session grid with flat zero-activity
  rows and zero VWAP; raw must carry the departing close; adjacent rows must trade; and both independent successor
  and predecessor-label sources must own and trade every bridge session. Adjusted padding may step only on exact
  positive dividend ex-dates, which do not establish tenure. The producer leaves every bridge row unassigned and
  changes no edge, interval or action date. Incomplete evidence, raw changes, any genuine activity, third occupant
  or structural conflict retains ordinary unresolved coverage. [terminal_episode.py](../libs/alpaca_end_of_day_security_correlation/terminal_episode.py).
  The same non-observation disposition may consume a closed departure interval and a later exact action-anchored
  unit-component suffix when those producer handoffs leave a complete inert gap. Every eight-view, calendar,
  carry, adjacent-trade, occupant and conflict veto remains; an unresolved CUSIP is admissible only when its dated
  evidence lies outside the certified gap. This assigns neither endpoint identity to the gap and joins no UUIDs.
- **Pre-entry carry-forward:** when one direct unchanged-UUID/CUSIP rename has a complete action query, exact
  endpoint ownership and complete aligned sources, the destination label's prolonged flat zero-activity tail may
  be certified as non-observation only if it repeats its last traded close through the boundary and the independently
  owned predecessor source overlaps that tail and reaches the immediately preceding market session. The earlier
  traded destination-label issue remains anonymous; the producer assigns no tail row and changes no action, interval
  or membership. Missing overlap, competing ownership, incomplete views or any changed/positive tail row retains
  ordinary unresolved coverage. [name_change.py](../libs/alpaca_end_of_day_security_correlation/name_change.py).
- **Terminal continuous ticker:** preserve accepted components; a new two-component UUID union additionally needs
  complete informative history without prolonged carry-forward and retained structural-action evidence.
  Three or more disconnected components stay unresolved. Independently certified closed-history departures keep
  narrower rules; ordinary inactive sessions neither create nor invalidate components.
  [terminal_fallback.py](../libs/alpaca_end_of_day_security_correlation/terminal_fallback.py).
- **Mapped predecessor tail:** successor ownership needs an accepted cross-UUID provider edge, as-of inside the
  predecessor interval, unique incoming/outgoing branches and an independently correlated successor source for
  exact sessions. Every field in all eight views must match. Correlation assigns successor rows; observation
  selection marks predecessor copies superseded. Equality alone, incomplete views, reused labels, competing
  branches or differing values leave rows unresolved.
- **Mapped successor prefix:** when a successor-labelled source begins inside a leading prolonged inert donor run
  and reaches an informative traded donor tail, one accepted shared-UUID, unchanged-CUSIP rename may transfer the
  exact owner of that complete prefix from the completed predecessor report. The prefix must end on the market
  session immediately before the retained boundary, every timestamp and nonsplit-view field must match, and every
  donor position in all eight complete aligned views must already be correlated to that exact predecessor-label
  owner. Ordinary active overlaps retain their existing stricter producers. A later extra UUID on the successor
  label is noncompeting only when
  accepted same-label identity-preserving split edges form one complete, nonbranching chain after the rename;
  unrelated, pre-handoff or branched occupants remain vetoes. Only that chain permits split-bearing numerical
  vintages to differ; it does not make adjusted values identity evidence. This producer rejects recursive mapped
  donors, incomplete sources, changed acquisition context, competing current owners or rename branches, gaps and
  any mismatch as a whole. It reuses the donor assignment; it does not infer identity from equal prices, move the
  action boundary or change UUID membership.
  [predecessor_prefix.py](../libs/alpaca_end_of_day_security_correlation/predecessor_prefix.py).

Corporate-action dates retain provider field semantics and exact supported endpoints. Do not rewrite them from
earliest or next-session dates, bar boundaries, researched marketplace dates or ticker exceptions. A separately
certified source span may date only its own observed rows; it does not replace the action endpoint. Missing or
conflicting evidence stays unresolved. Research may supply the exact sourced event facts below; it cannot
establish an opaque provider UUID, move provider dates or directly assign rows.

### Sourced event facts

`lineage update --symbol SYMBOL --event-supplements FILE.json` accepts a bounded schema-1 JSON document with
`schema_version` and a nonempty `supplements` array. Each item has `kind`, `match`, `facts` and `sources`;
[event_supplements.py](../libs/alpaca_security_lineage/event_supplements.py) defines the exact fields. Each source
records an HTTPS document URL, title, document date and issuer/exchange/clearinghouse/regulator authority.
The operator establishes those facts through the [research loop](development.md#lineage-research-and-rule-improvement-loop).
Runtime does not fetch or interpret web pages. Facts must match the provider event ID, type, labels, identifiers,
dates and applicable ratios; changed or conflicting events fail rather than silently inheriting the disposition.

- `split_endpoints` adds issuer-documented old/new CUSIPs to an ordinary forward/reverse split. The existing
  continuity rule still requires actual exact provider lookups for both endpoints; the supplement supplies no UUID.
- `derivative_only` identifies an option-root consolidation by exact underlying CUSIP, effective date and affected
  option series. The original provider action remains retained, but contributes no equity occupancy, rename,
  UUID edge or acquisition dependency. Similar labels alone never establish this scope.
- `listing_transfer_only` identifies a researched exchange transfer with unchanged equity ticker and CUSIP.
  An exactly matched, incorrectly classified rename stays in raw evidence but contributes no identity event.
  Distinct exchanges and the provider event date must match the sourced facts; a transfer alone creates no UUID edge.

The metadata producer retains the raw action union, applied facts and equity projection in its completed receipt.
New facts trigger selected complete-group calculation; unchanged updates reuse the receipt and never reread the
input file or source documents. Metadata refresh reapplies retained facts to the raw provider events. Only missing
or invalidated exact mappings require acquisition. Explicit symbol selection is required; unmatched submitted
facts are reported as failure. Frozen price payloads are unaffected by this input mechanism.

### Name changes

Name changes establish dated endpoints, not unlimited predecessor history. Conflicting effective/process dates
or component-arrival evidence leave disputed windows unresolved. Different UUIDs join only through independent
continuity proof: an accepted identity-preserving event or exact informative raw predecessor prefix through the
boundary, with complete matching eight-view grids. Use selected group inputs, never decisions derived from the
disputed edge. Divergent independent concurrent sources keep their histories.

Shared UUID still requires bounded historical-label evidence: own source plus informative exact matching suffix,
existing independent-history proof, or accepted bounded round trip. Exact matching historical spans can establish
row ownership without certifying a UUID join; unmatched spans remain unresolved. No source extends proof into an
earlier unobserved prefix. [name_change.py](../libs/alpaca_end_of_day_security_correlation/name_change.py) and
[source_continuity.py](../libs/alpaca_end_of_day_security_correlation/source_continuity.py).

When a destination source starts inside the independently owned predecessor interval, its observed dates must be
one complete contiguous suffix through that interval's end. Only a shared-UUID rename may consume an informative
exact raw suffix in that shape, and its historical-label certificate starts at the destination source's first
observed session. Distinct-UUID joins continue to require the complete predecessor grid.

Earlier prolonged provider padding remains neither trading-absence nor ownership evidence. It does not invalidate
that bounded suffix only when one complete aligned prolonged padding run follows a traded observation and ends
immediately before the destination source begins; the rename preserves one nonempty CUSIP; a complete action query
and unique active non-OTC current-symbol/CUSIP lookups identify the same UUID; the destination prefix is the complete
exact raw predecessor-source suffix through the market session immediately before the retained action boundary; and
the overlap has informative traded endpoints. The padding is only a shape gate. The certificate assigns those
observed source rows to the already-dated predecessor label, not to early destination-label tenure. It does not
consume the padding, move the action boundary, join UUIDs or extend unseen history. Adjustment-view inert values
may change only on an accepted same-symbol split date within the separator; every row remains flat and inactive.
A complete later stock merger does not veto the earlier certificate only when the successor is the continuing
acquirer on the unchanged CUSIP, both effective and processing dates exist, every recorded action date is strictly
after the retained rename boundary, and the acquiree is a distinct label/CUSIP. This neither assigns acquiree
history nor adds identity membership. Ticker reuse, unrelated occupants on or before the boundary, any other or
incomplete structural action, incomplete grids, detached padding, gaps and raw differences preserve the veto.

A complete predecessor source containing exactly one session may use the same certificate only for one unique
direct shared-UUID rename with unchanged nonempty CUSIP, when the session is the last market session before the
provider boundary and the destination source contains the exact same positive-volume, positive-trade raw
transaction. All eight selected views must be complete and session-aligned; their adjusted values remain numerical
vintages and are not repeated identity evidence. Certify only that observed session. Different UUID/CUSIP,
additional structural edges, nonadjacent dates, incomplete sources, inert or divergent raw rows and multi-session
non-informative histories remain unresolved.

A retained name-change processing date may follow an observed successor-label handoff without being rewritten.
Date only that bounded early successor span when one complete action query and unique direct shared-UUID,
unchanged-provider-CUSIP edge have exactly one predecessor and one successor interval; the complete predecessor
source ends on the market session immediately before the complete successor source begins; and the independently
resolved current-CUSIP source is the exact raw concatenation of both complete histories. All selected views must
be complete and session-aligned, and the early raw span must be informative and free of prolonged carry-forward.
The certificate moves no endpoint, extends no unseen history and joins no UUIDs. Missing or ambiguous current
owners, related structural actions, ticker recurrence, gaps, overlaps, incomplete sources and divergent raw rows
retain their existing vetoes.

A provider component-separation action may independently date that same bounded early-successor shape when it is
the unique positive unit split naming the predecessor as an exact output component on the adjacent source-handoff
session. Its dated occupancy and, when identified, exact component-CUSIP endpoint must agree; undated symbol
lookups do not become historical occupants. The direct rename must preserve one UUID/CUSIP, the predecessor and
successor sources must be complete and adjacent, and the early successor span must be the exact informative
market-session prefix before the retained rename date. An older predecessor source containing prolonged padding
is admissible only when its final five observations are actively traded and nonconstant. A later exit must be
either a linear same-identity rename or one exact positive-rate cash merger whose successor symbol/CUSIP, ownership
interval end and next-market-session boundary all agree. This dates only observed component rows; it does not
rewrite an action, infer an issuer-wide boundary, join UUIDs or admit missing endpoints, third dated occupants,
gaps, inert endpoints, competing events or structurally ambiguous evidence.

When the successor source instead retains the predecessor's complete history, the same component producer may
date only the internal sessions after the unique component event and before the unchanged-UUID/CUSIP rename. Every
field in all eight inherited prefixes must match through a traded event-day observation; the predecessor-label
rows in the internal span must be one exact zero-activity carry while every successor-label row trades, and the
calendar span must be complete. A later reused predecessor label is noncompeting only through its already accepted,
strictly later action-anchored interval. Missing endpoints, prefix differences, overlapping or undated occupants,
positive predecessor activity, inert successor rows, incomplete views and intervening structural actions veto the
certificate. It changes neither provider date nor UUID membership.

A predecessor-labelled suffix after that dated endpoint may be identified as terminal non-trading carry-forward,
not assigned history, only when a complete action query contains one exact stock-merger acquiree with a distinct
inactive, nontradable, non-OTC UUID and exact current-symbol/CUSIP lookups; the ordinary-share name-change series
has the same UUID/CUSIP on both sides; every complete aligned adjustment view begins the suffix at the boundary
and exactly repeats its own final supported close with zero volume, trades and VWAP; and the independently owned
successor source trades informatively on every suffix session. The completed producer records the retired UUID as
the uncorrelated suffix's sole candidate so serving can scope the unresolved rows to the identity they may affect.
It does not assign or delete a row, join UUIDs, move an action date or relax serving policy. Recurrence, competing
occupants/actions, active or OTC acquirees, incomplete/gapped views, divergent or positive tail activity and
absent/uninformative successor trading preserve the anonymous unresolved conflict.

One exact same-symbol, distinct-UUID provider substitution can also certify the handoff without price comparison.
The direct form has exactly one related action and candidate and an unbounded predecessor interval. The bounded form
permits exactly one strictly earlier incoming different-label rename that preserves the exact predecessor CUSIP and
UUID and supplies that interval's lower bound; rows before the entry remain unresolved. In both forms the action
query must be complete and cover the handoff; unique exact CUSIP lookups must identify the inactive/nontradable
predecessor and active/tradable successor; the unique current-symbol lookup must identify that successor; both
endpoints must share one non-OTC exchange; exactly those two occupants may be known; and exactly two same-label
ownership intervals must meet at the handoff. Missing, duplicated, additional, recurrent or conflicting evidence
vetoes the certificate. The continuity owner publishes one accepted action ID; UUID membership and dated row
chronology consume that same certificate without repeating endpoint authentication. Existing ticker-reuse
ambiguity guards remain in force.

An earlier stock acquisition does not count as an additional conflicting action for this certificate only when
the predecessor is its exact continuing acquirer on the same CUSIP; the target has a distinct nonempty label and
CUSIP outside both endpoints; both exchange ratios are positive finite numbers; effective and process dates are
present; and every recorded action date lies strictly before the handoff and within any bounded predecessor
interval. Target history never joins the acquirer. Incomplete evidence, acquiree roles, same-day/later acquisitions
and other structural actions retain the veto.

Complete action coverage applies to every exact label in the producer's completed `queried_symbols` set, as well
as the original query label, only within its recorded date window. Partial queries and unqueried labels provide no
coverage. Certificates consume this completed acquisition guarantee without fetching the same actions again.

Unresolved positive occupancy remains a conflict within the asserted label/date scope for same-symbol,
component-history and current-source bridge certificates. A validated unit-component anchor permits only its
exact supported unresolved component evidence. Unrelated or out-of-span uncertainty does not erase supported
history. The current-source bridge also considers dated raw structural actions, including nested recipient
labels and incomplete actions that produced no UUID candidate. Chronology and membership trust the resulting
single producer certificate; downstream consumers do not repeat these checks.

## Membership and metadata continuation

[DependencyGraph](../libs/alpaca_lineage_build/selection.py) retains published membership until retirement.
Disappearing evidence links cause separate replacement-component correlation and one journal publishing all
replacements. Consumers neither repair groups nor infer replacement membership. Missing indexed membership needs
explicit `lineage migrate-index`, never an implicit receipt crawl.

Completed scheduling hints are authoritative. Lost coverage permits one explicit source-generation census,
without receipt reads for matching sources. [pipeline.py](../libs/alpaca_lineage_build/pipeline.py) plans metadata
from completed pricing results—including accepted ranges/effective pending changes for covered requests—and
accepted index ranges. Covered acquisition consumes its source boundary without reopening endpoint validators.
After interruption, only selected pending source receipts supply missing planning facts, in bounded cohorts;
publication reselects under its own locks.

Prefetch opens selected lineage receipts for evidence queries. [EvidenceClient](../libs/alpaca_lineage_build/metadata.py)
reuses containing completed action intervals within an invocation. During a rule-only retained-mapping repair,
its metadata adapter also consumes the prior report's completed action coverage and symbol observations; only
a changed mapping or genuinely uncovered label/interval acquires evidence. Failed/incomplete responses prove no
coverage; ordinary wider requests/new observation work retain their existing acquisition path. Conflicting action IDs block only evidence containing
those IDs, including cached intervals; unrelated symbols in fully acquired/routed batches remain usable.
Transport/routing failure leaves affected requests incomplete. Conflicts remain blocking for the invocation;
no winner is chosen.

A prefetch plan selects maximal requested intervals per symbol so nested needs share their containing
response. All pagination and routing must complete before that response establishes subset coverage;
overlapping intervals and gaps do not acquire invented coverage.

Ordinary action continuation starts at the prior active month; older corrections require explicit refresh.
An unchanged action's completed CUSIP→ID lookup, including a negative result, remains authoritative. Current
catalogue facts may join an already accepted UUID; name/status changes or absence from the catalogue do not
invalidate the historical mapping. This never infers CUSIP from ticker. New/changed actions, a changed current
endpoint UUID or explicit refresh require exact acquisition. Replacing an already consumed lookup during traversal
restarts metadata assembly from the authoritative exact cache. Known dependencies retain continuation/refresh
obligations; new ones need full evidence.
Expanded groups reselect under sorted enlarged locks; decoded reuse requires matching generation/scope.
Exact and catalogue endpoint observations have the same invalidation authority, including canonical endpoints
recorded by retained CUSIP lookups. A late observation refreshes
affected reports already assembled in the selected group from the operation's in-memory acquisition cache before
publication. This also applies when a later symbol in one traversal changes an earlier retained dependency.
Dependency baselines use the explicit symbol observation recorded with the mapping, falling back to the CUSIP's
canonical UUID only when that observation is absent. A fresh exact mapping remains authoritative even if it
returns an older occupant. The metadata producer records the endpoint observations used for that acquisition
so the same unchanged dependency does not trigger another lookup. Selected pre-77 receipts reconcile these
dependencies once before new publication, including already-published stale mappings.
Rebuilt reports restore those accepted baselines before their coherence check. Historical mappings that differ
from current ticker occupants therefore remain usable; changed endpoint/action facts still invalidate them.

## Assignment continuation

[resume.py](../libs/alpaca_lineage_build/resume.py) is a yes/no input boundary. It does not map ownership rules,
predict assignments or execute an alternate lineage algorithm. Completed pricing generations/ancestry establish
row equality; the accepted pricing request (including mapping `asof`, range, feed and timeframe), complete evidence,
group membership and policy establish the remaining inputs. Only acquisition counters, catalogue observation
wall time, lookup transport provenance and metadata-owned mapping dependency baselines are excluded from evidence
equality. Metadata uses those baselines to invalidate retained lookups before the handoff; changed lookup results
still participate. Adding or updating the baselines alone does not reopen lineage. New evidence fields participate
automatically, including disposition changes.

When the accepted inputs are unchanged, `lineage_required=false` carries completed decisions forward without
pricing payload reads or rule execution, including linked groups, ambiguity and empty histories. Producer-proven
unchanged observations remain reusable when only coverage and its normal current-label mapping cutoff advance.
Pinned mappings must stay identical. Changed actual metadata still selects reconciliation; action-query bounds
and diagnostic row counts alone are acquisition bookkeeping within this accepted continuation scope.
The latest HTTP window's `start` is transport bookkeeping; the source's `history_start` supplies the retained start.

Any compatible completed group may retain its accepted prefix when pricing ancestry proves that prefix unchanged.
Group size, cross-label membership, structural events and an initially empty history describe the completed
decision; they are not evidence that retained prices or assignments changed. Effective source context must match,
allowing only advancing observation/mapping cutoffs. Fresh mapping, action, disposition and coverage-quality facts
still participate. A descriptive name/status/tradable change on the same provider UUID, symbol, class and exchange
affects new-session projection but does not revoke accepted row owners; accepted continuation also excludes
action-query bounds and the diagnostic maximum row count from historical equality.

Validate each new session grid and its accepted seam, then send only added rows from every group member through
the same compiled policy evaluator. An empty accepted prefix has no seam; the pricing producer validates its first
rows. No new rows means no rule execution. Closed pricing/assignment payloads stay closed except for assignment
bodies needed to regroup publication at month/year rollover or project an older receipt without a summary. The
active pricing partition supplies the added rows. A completed unresolved or incomplete prior row partition is
explicitly conditional on later group facts: new rows in that group select complete-group work until an owning
producer can certify a narrower affected interval. Resolved completed assignments remain closed.

A pricing producer's latest-session correction certificate permits retaining resolved assignments before that
session when mapping and ownership evidence remain applicable. The same evaluator processes the corrected row
and any appended rows in all eight views. Assignment summaries and affected boundary metadata supply the retained
prefix; older pricing is not reopened to authenticate it. Correction scope and ancestry travel through metadata
prefetch as well as execution, so a sparse correction before the current month does not reacquire historical
actions merely because its notice begins earlier. [correction_prefix.py](../libs/alpaca_lineage_build/correction_prefix.py)
owns this projection; the [pricing and serving contracts](technical-contracts.md#pricing-acquisition-and-continuation)
own its publication and downstream guarantees.

One accepted catalogue and evidence client serve recovery and discovery within an update. Recovery acquires
the catalogue once when it has selected work; discovery consumes that completed observation, exact positive
and negative asset lookups, completed corporate-action intervals, failed requests and conflict latches.
A phase transition neither invalidates these facts nor permits reacquisition or repeated normalization.
Optional lookup prefetch retains its completed catalogue response for the full discovery producer; the
stronger discovery schema must still succeed without a second provider request. A new invocation obtains
fresh observations. Action coverage never approves an unqueried date interval.

If newly acquired metadata invalidates this prefix after pricing has been decoded, full-history fallback
retains the already decoded partitions under the same source generation and locks. It loads only the missing
partitions. Single-source observation selection skips multi-source ownership/ranking preparation and retains
the existing legacy activity validation.

Changed mapping/action facts, incompatible context or policy, corrections beyond the certified latest session, missing equality proof, or
new group rows that can reconsider accepted unresolved/incomplete decisions select complete-group work. This
selection precedes evaluation; the boundary never tries one set of rules and checks its outcome to choose another.
The existing evaluator retains its normal evidence reconciliation and ownership fallback. There is no separate
continuation executor or mandatory context preparation.

The normal selector-last publication, unchanged output-reference reuse, pricing handoff and journal recovery
contracts apply to both paths. Opt-in `--debug` records whether lineage is required, whether retained history is
selected, its retained row count and the scope (`none`, `added_sessions` or `complete_group`); ordinary commands add
no trace logging.

## Missing auxiliary prices

An exactly rejected missing auxiliary may retain metadata and complete its group alongside independently
supported histories, without an invented empty source. This excludes arbitrary failure, seeds and retained
series. [acquisition.py](../libs/alpaca_lineage_build/acquisition.py) records rejected request, evidence and
obligation before acknowledgement; auxiliary-only reselection preserves its group.
