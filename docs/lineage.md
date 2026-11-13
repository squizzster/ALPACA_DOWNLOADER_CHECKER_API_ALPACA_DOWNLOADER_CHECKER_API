# Lineage contracts

[Architecture](ARCHITECTURE.md#lineage-and-identity) maps owners; [policy lab](policy-lab.md) defines executable
profiles and read-only comparisons. These contracts govern completed-group reuse and evidence limits.

## Ownership allocation criterion

Every decision asks which outcome is most likely to be correct given the available API evidence.
Explicitly compare plausible connections and leaving history unconnected; choose the best supported
interpretation. Both outcomes can be correct, but neither is an automatic default.

Use generic rules to choose the most likely ownership, historical labels, identity membership,
continuity and numerical source authority supported by provider observations and applicable completed
producer decisions. Absolute proof or a particular certificate is not required when the combined
admissible evidence reasonably favors an owner over its alternatives.

Research establishes an external reference for improving and testing those generic rules. It may
corroborate a connection or show that the API evidence cannot reasonably establish ownership; both
findings can improve or validate a generic rule. It must not
supply runtime facts, ticker-specific exceptions, factual applications, event supplements, assertion
overlays or research certificates. This restriction applies to production, debug, lab and fresh recovery
decisions. YAML selects registered generic rules and generic parameters only. The same admissible
evidence conditions select the same inference logic regardless of ticker spelling.

The objective is the best supported interpretation of the available admissible source evidence.
Connect and publish when a connection is reasonably supported; leave history ownerless when it cannot
reasonably be connected. Both outcomes can be correct. Do not force an allocation because history must
belong somewhere, or withhold a reasonably supported connection because absolute proof is unavailable.
An ownerless outcome is completed, processed lineage: persist its unresolved disposition and evidence
limits, keep that history unpublished to consumers, acknowledge processing and let independent work
continue. It is not a workflow failure or blocker.

Evaluate the complete affected group. Weigh prior accepted ownership, dated provider observations,
security class and identifiers, provider mappings, continuity and competing owners together. Research
can corroborate an expected result and expose a missing generic rule; the rule must establish its result
from admissible runtime inputs. Record reasons, applicable confidence, material weaknesses and genuine
alternatives without inventing numerical probabilities. Ticker spelling, issuer identity or price
equality alone does not establish security identity.

Keep technical publication integrity separate from ownership inference. Selected generations, readable
handoffs, coherent numerical bundles and interruption recovery remain required. Missing technical
capabilities select their owning recovery path while preserving applicable completed decisions; they
do not prove that history has no owner or authorize repeated historical checks. Frozen journals install
completed outputs under their recorded provenance; retained research assertions cannot be extended to
justify new rows.

Fresh inference rejects authored assertions and selects provider observations separately from retained
legacy provenance. [F67](trust-boundary-handoff.html#f67) records the implementation review and its remaining
limits. Existing certificate gates can still withhold reasonably supported ownership;
[F64](trust-boundary-handoff.html#f64) remains open. Mechanism tests do not establish correct ownership in every case.

## Lineage reuse

[contract.py](../libs/alpaca_lineage_build/contract.py), `reusable_decision`, and
[group_engine.py](../libs/alpaca_lineage_build/group_engine.py), `process_group`, require matching complete-group inputs,
dependency/recorded-absence sets, compatible versions, no pending pricing work and no explicit metadata refresh.
Receipts/index entries bind authorized policy ID, role, implementation tuple and semantic digest. File digest
records exact YAML bytes; formatting is not reuse identity.

Authorize the canonical policy once after journal recovery, before new selection/evaluation/network work.
Recovery validates frozen journal provenance without reinterpreting it through newly installed policy.
The [ruleset loader](../libs/alpaca_lineage_build/ruleset.py) accepts the schema-2 canonical YAML's registered
generic policy and optional generic producers as one operation configuration. Their identities remain separate:
a container or formatting change does not establish new ownership or invalidate an applicable prior policy.
Its [completed value contract](../libs/alpaca_lineage_build/ruleset_contract.py) crosses stage JSON without
reopening YAML or compiling rules again. It carries no active factual applications or supplements.
Evidence fingerprints are independent of history approval. [Workflow completion](technical-contracts.md#universe-observation-and-work-state)
consumes fresh source-bound approval without republishing an unchanged decision. A completed producer handoff
does not need its group reopened merely to acknowledge that approval. Rule-only changes reuse approved evidence.

[policy_engine.py](../libs/alpaca_lineage_build/policy_engine.py), `evaluate_policy`, constructs detailed row traces,
pass digests and audit summaries only when requested. Mandatory phase/certificate checks still run; published
and explicitly traced digest semantics remain unchanged.
Each fixed-point result records the counterpart reports it actually consulted, including unavailable reports.
Only a changed consulted counterpart selects another pass for that source. Exact alias views share completed
row allocations and bind their own file/adjustment provenance without traversing the same rows again.

The correlation owner prepares dated occupancy and name-change continuity once before freezing the first
allocation. Chronology that places a later unrelated occupant may disambiguate an earlier rename, but its
fallback interval is not exposed to baseline row allocation. Both phases consume the same completed continuity
certificate. The second phase reserves established correlated segments before invoking row rules, including
their UUID membership, provenance and bounds; completed source reports remain closed. Newly produced fallback
intervals must not contradict those assignments. Only unresolved rows enter the existing cascade, and terminal
fallback still receives the complete source partition. The original phase integrity guard remains mandatory.

Incremental continuation carries accepted UUID membership separately from each dated provider UUID. Version-1 continuation handoffs publish compact membership, all-view identity/date contributions and own-label observation routes. Receipt ingress validates the declared handoff once; missing required new fields fail into owning recovery. Unmarked selected legacy assignments supply the bounded compatibility projection, including the named last-session boundary when the hot partition is empty. A corrected date includes unchanged competing contributors and their accepted dated labels, with their ownership decisions frozen. Earlier assignments remain closed. The effective request mapping is compared by the owning continuation gate; a changed pinned mapping cannot inherit old ownership. Fresh already-acquired metadata invalidates decision reuse independently of whether retained metadata can still supply acquisition inputs.

A separate `computation_revision` identifies completed lineage calculation semantics. Revision 2 requires full retained-input reconstruction of earlier completed outputs, including revision-1 decisions under correlation-78 or correlation-79. It is carried by receipts and compact index entries; metadata compatibility, pricing generations and authorized rule policy remain independent. An obsolete computation must enter full owning evaluation and cannot acquire the new revision through header-only promotion. The explicit retained-input rebuild and its retry/publication scope are described in [Operations](operations.md#rebuild-published-lineage-after-a-computation-repair).

Explicit [targeted retained recovery](technical-contracts.md#targeted-retained-lineage-recovery) can select
known defective current-revision decisions without invalidating unrelated groups. It carries the accepted
comparison's exact membership, receipt and pricing generations into full owning evaluation and journaled
publication. Its completion handoff is independent of computation revision and of ownership certificates.
[Operations](operations.md#recover-selected-current-revision-lineage) owns invocation and retry behavior.

## Policy migration

Publication compatibility and executable runtime behavior are distinct. Removing fact-injection inputs
does not change the canonical ordered rule-policy identity or revoke completed assignments, membership,
labels or numerical choices. Supported earlier publication formats and policy identities remain readable
under their recorded provenance. They authorize reuse within their accepted scope, never new inference from
legacy assertions or a promotion obtained by rewriting a header. Runtime changes and exact compatibility
identities belong to the [version contracts](../VERSION_TRACKER.md#compatibility).

The explicit computation-revision rebuild remains the owner of obsolete calculations. It does not make
a retained research assertion admissible, select a store-wide migration implicitly or authorize repeating
applicable completed work. Fresh evaluation uses retained provider observations and current generic rules.

[Migration](../libs/alpaca_lineage_build/certificate_migration.py) retains the exact 34/76 or 34/77 applicability
proof under its recorded target and prerequisites. It tests changed-rule prerequisites and concrete mapping-dependency conflicts using accepted complete-group
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
bundle. Applicable completed market and source decisions keep their accepted scope. For fresh selection,
the latest independently observed own-label source normally wins. This selects observations, not historical completeness.
Equal complete bundles can collapse; unresolved authority stays explicit. All accepted dated labels survive
independently, including ambiguity. Readers/exports consume decisions without repeating selection/activity checks.

When overlapping bundles disagree in any field and request mapping `asof` lies outside accepted occupancy, lineage
requests only that source's full retained span at an observed own-label mapping date. Accepted departures across
CUSIP/UUID changes within one public identity also bound this route. Equal bundles, disjoint histories and
superseded sources do not trigger acquisition. A multi-identity source cannot be globally remapped to one occupant.
Pricing publishes correction before lineage selection; failure leaves the new selection unpublished.

An older occupant's closure does not disprove source-wide mapping when the same source independently observes a
later own-label occupant. Per-identity selection remains valid without replacement.

Completed dated-market label projections and bounded numerical-source revisions remain consumable
producer decisions. Their accepted generation, exact sessions and selected source bindings carry into
serving without replaying ownership, comparing accepted prices or reconstructing assignment artifacts.
A completed unpublished projection can carry through interruption under those same bindings. Only a
genuinely invalidated numerical scope enters owning selection; unknown choice flags or missing declared
capabilities require owning recovery, never an audit of the remaining accepted history. These closed
results do not authorize accepting new market assertions or extending legacy chronology to new rows.

Ordinary source work also retains its first completed equal-bundle result with explicit previously selected
donors, independently of public-component changes. The [source-equivalence contract](technical-contracts.md#standalone-artifacts)
defines its generation/date binding and serving reuse; equality supplies numerical continuity, not ownership evidence.

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
- **Current source after a closed departure:** the existing unique-candidate rule may consume a bounded
  certificate for a complete informative current-mapped source wholly after an accepted direct unchanged-UUID,
  unchanged-CUSIP rename to another label. Its retained request scope must begin on or before the departure, all eight
  views must agree on a complete session grid through accepted coverage, and an exact active/tradable own-label
  lookup must identify the distinct current UUID. The selected group must contain exactly those two labels and
  two UUIDs. Additional relevant occupants, conflicting actions or ownership,
  incomplete evidence and historical/mapped-alias selection retain the existing rejection. No current CUSIP,
  dividend or minimum vacancy is inferred or required. The complete renamed source must cover the observed
  sessions; an exact raw copy of that source vetoes certification, while differing prices do not establish identity.
  Only actual observed rows gain ownership; the certificate
  creates no global interval, IPO date or UUID edge. Existing prepared source and chronology facts produce it once;
  the lineage receipt carries applicable completion through continuation and unresolved-peer reconciliation.
  [closed_departure.py](../libs/alpaca_end_of_day_security_correlation/closed_departure.py) owns the certificate.
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
certified source span may date only its own observed rows; it does not replace the action endpoint. The current
certificate paths can leave missing or conflicting inputs unresolved; their applicability limits are not a
general preference for nonallocation over the [most likely owner](#ownership-allocation-criterion).
Research can improve and test a generic rule when retained provider observations reasonably support its
result. It cannot supply event assertions, establish an opaque provider UUID, move provider dates or assign rows.

### Sourced event facts

Fresh classification also excludes hardcoded ticker snapshots as evidence. Reserved-looking labels and names
receive the same provider-evidence rules as other labels; no pattern substitute is used. The legacy exchange-test
classification remains readable within completed publications, but is inadmissible as fresh inference evidence.

Authored factual applications, event supplements, assertion overlays and research certificates are rejected
in production, debug, lab and fresh recovery decisions. The schema-2 canonical YAML contains generic policy
and producer selection only; no JSON fact import or legacy configuration fallback is an inference authority.
The removed `--event-supplements` input cannot select a repair. Research remains external to runtime under
the [generic research loop](development.md#lineage-research-and-rule-improvement-loop).

The [provider-evidence owner](../libs/alpaca_security_lineage/inference_evidence.py) selects retained raw
provider actions and exact observations for new inference. Where a legacy receipt retains a separate raw
provider union, it rebuilds only the derived provider projection, preserving accepted query coverage and
local-market evidence. Missing required raw observations select owning metadata recovery. A completed
`ProviderEvidence` result is accepted once and lent to all downstream consumers without another traversal,
projection, store read or provider request. Fresh externally supplied assertions are rejected rather than
silently converted into evidence.

Published assignments, component membership, dated labels, descriptions, numerical states and applicable
completed handoffs remain authoritative in their accepted scope. Legacy research provenance may be inspected;
it does not become active assertions, supply new generic-rule predicates or extend an old certificate into
new rows. Frozen journals install their completed artifacts under their recorded provenance. Removing an
injection input neither rolls back those publications nor selects historical replay or a storage crawl.

An unresolved legacy fact-application obligation without an applicable completed output requires explicit
owning generic recovery. It cannot be satisfied by rebinding the old research fact, copying a new assertion
or treating a missing handoff as permission to reconstruct accepted history. Independent completed work
continues. A new generic assessment may connect reasonably supported ownership or complete ownerless lineage;
its reasons, evidence limits and competing interpretations remain recorded.

The optional `accepted_same_security_containing_component_v1` membership producer consumes completed
same-security rename evidence, a containing accepted component and certified extension edges. It remains
disabled when `producers.membership_refinement` is null. Mere UUID containment does not authorize a join;
missing legacy edge metadata preserves existing assignments without a provider or rebuild fallback.
[Membership updates](../libs/alpaca_lineage_build/membership_updates.py) can also consume an explicitly supplied
completed plan under the same store, complete group, pricing generations and changed-source/donor receipt
generations. It applies the recorded positions without repeating the route or UUID-edge proof. This is an
internal operation-context handoff, not a CLI plan-import option. Its completed values cross stage JSON;
only actually changed membership intervals and their new numerical overlap reopen selection.


Closed source-work and publication mechanics remain separate from fresh inference. Applicable accepted owners
and numerical choices stay closed; only selected new rows, newly joined overlap or genuinely changed bindings
open their owning work. Entirely superseded histories whose accepted runs need no interior timestamp assembly
remain closed to prices. Completed source projections, timestamp keys, assignment artifacts and numerical
choices survive construction/publication interruption under their actual generation, metadata and policy
bindings. Missing or incomparable component capabilities require owning recovery, never a union inferred
from shared UUIDs or a repeated completed calculation.

### Name changes

The following certificate routes describe current mechanisms. Their absence is not a global negative
ownership finding; the [allocation criterion](#ownership-allocation-criterion) governs the choice among
plausible explanations.

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

Ordinary action continuation starts at the prior active month; older action corrections require explicit refresh.
A same-span price correction, even in older history, cannot select historical action or CUSIP acquisition.
A backward history extension acquires the uncovered action prefix up to the day before the accepted query start,
plus the ordinary current window; the accepted middle and unchanged mappings remain reusable. Missing retained
query coverage requires explicit metadata refresh. Workflow
refresh classifies accepted endpoint facts first: descriptive changes preserve the continuation window;
changed identity/class/exchange facts or explicit historical refresh select their owning evidence scope.
Fresh prefetched actions take precedence over a retained report covering the same dates, including at an
unchanged cutoff. That completed current window can discover new dependencies without reopening older actions.
An unchanged action's completed CUSIP→ID lookup, including a negative result, remains authoritative. Current
catalogue facts may join an already accepted UUID; name/status changes or absence from the catalogue do not
invalidate the historical mapping. This never infers CUSIP from ticker. New/changed actions, a changed current
endpoint UUID or explicit refresh require exact acquisition. Replacing an already consumed lookup during traversal
restarts metadata assembly from the authoritative exact cache. Known dependencies retain continuation/refresh
obligations; new ones need full evidence.
Expanded groups reselect under sorted enlarged locks. The operation retains decoded partitions by store,
boundary kind and selected boundary generation; an envelope-only publication does not discard them. Changed
partitions invalidate only their entries. Selected assignment bodies likewise survive unchanged generations.
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
The previous receipt's `processed_through` also carries accepted empty coverage. Append-grid checks begin at
that covered boundary, rather than the last physical bar. A genuinely missing session inside the new interval
still selects owning work; an old accepted empty interval cannot become a reason to reopen earlier choices.

Accepted UUID membership retains each source's request scope. Two peers may carry different completed public
components containing the same UUID; group connectivity does not merge those components or invalidate either
source's historical owner. Only unambiguous peer membership may fill a missing local entry. Conflicting owners
inside one source still require explicit recovery. When a peer selects complete-group work, applicable sources
carry their positive dated row decisions in full-source positions; new and unresolved rows enter the cascade.
Explicit numerical states remain separate from row ownership. A dated market with zero new rows across all of its episode
sources retains its accepted public binding even though suffix reports contain no segments. This supplies no row
template or new allocation; fresh unresolved rows and conflicting memberships still select owning resolution.

Retained legacy envelopes and publication bodies remain readable as completed provenance. Their known
transport spellings do not authorize fresh fact applications or repeat their old proofs. Unknown or conflicting
required handoffs fail into owning recovery. A selected catalogue observation is read by its persisted ID and
timestamp; recovery consumes its accepted records and digest without requesting or calculating them again.
The owning post-correlation consumer can finish from held source inputs, reports and retained-choice scope
under normal writer/pricing locks. Applicability checks do not repeat acquisition, materialization or correlation.

Selected retained-input recovery can carry completed action-query coverage when it covers every admitted
market date in the selected pricing request. It retains the actual query bounds. A later admitted date or
explicit refresh cannot inherit that scope. New metadata projections consume retained provider observations
only; old research provenance cannot expand the source scope or become a new continuity assertion.

Any compatible completed group may retain its accepted prefix when pricing ancestry proves that prefix unchanged.
Group size, cross-label membership, structural events and an initially empty history describe the completed
decision; they are not evidence that retained prices or assignments changed. Effective source context must match,
allowing only advancing observation/mapping cutoffs. Fresh mapping, action, disposition and coverage-quality facts
still participate. A descriptive name/status/tradable change on the same provider UUID, symbol, class and exchange
affects new-session projection but does not revoke accepted row owners; accepted continuation also excludes
action-query bounds and the diagnostic maximum row count from historical equality.
The workflow's metadata-refresh obligation uses this same classification before choosing historical pricing
work; requesting a fresh description is not itself a request to recalculate historical ownership.

Validate each new session grid and its accepted seam, then send only added rows from every group member through
the same compiled policy evaluator. An empty accepted prefix has no seam; the pricing producer validates its first
rows. No new rows means no rule execution. Closed pricing/assignment payloads stay closed except for assignment
bodies needed to regroup publication at month/year rollover or project an older receipt without a summary. The
active pricing partition supplies the added rows. A completed unresolved or incomplete prior row partition is
explicitly conditional on later group facts: new rows in that group select complete-group work until an owning
producer can certify a narrower affected interval. Resolved completed assignments remain closed.

A pricing producer's latest-session correction certificate permits retaining resolved assignments before that
session when mapping and ownership evidence remain applicable. The same evaluator processes the corrected row
and any appended rows in all eight views. At an unchanged cutoff, the corrected session is the entire new input;
the empty append interval requires no calendar calculation. Assignment summaries and affected boundary metadata
supply the retained prefix; older pricing is not reopened to authenticate it. Correction scope and ancestry travel through metadata
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
Query-label bookkeeping alone does not invalidate an accepted append prefix. A strictly later suffix preserves
both resolved and unresolved historical decisions when completed assignment bounds establish disjointness.
Overlapping new observations, a previously observed future action whose date is reached by the suffix, missing
bounds and incomplete partitions retain the owning reconciliation path.
The existing evaluator retains its normal evidence reconciliation and ownership fallback. There is no separate
continuation executor or mandatory context preparation.

Each in-memory pricing input declares whether it contains `complete_history`, a `retained_suffix` or an
`active_partition`; this scope is assigned by the loader request, never inferred from advertised retained counts.
After metadata and append invalidation finalize complete-group work, every selected priced member is materialized
as complete history under its already selected generation. The operation reuses decoded partitions while loading
only missing data. Mixed complete/suffix inputs fail before correlation or publication. Accepted prefix reports
and certificates remain available only to the added-session path; they cannot mark one member of a new
complete-group decision as resumed. The separately certified closed-source path retains its explicit authority.

The normal selector-last publication, unchanged output-reference reuse, pricing handoff and journal recovery
contracts apply to both paths. Opt-in `--debug` records whether lineage is required, whether retained history is
selected, its retained row count and the scope (`none`, `added_sessions` or `complete_group`); ordinary commands add
no trace logging. Before each ordinary correlation, the trace also records every selected input's pricing
generation and materialization scope from held values.

## Missing auxiliary prices

An exactly rejected missing auxiliary may retain metadata and complete its group alongside independently
supported histories, without an invented empty source. This excludes arbitrary failure, seeds and retained
series. [acquisition.py](../libs/alpaca_lineage_build/acquisition.py) records rejected request, evidence and
obligation before acknowledgement; auxiliary-only reselection preserves its group.
