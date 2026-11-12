# Lineage contracts

[Architecture](ARCHITECTURE.md#lineage-and-identity) maps owners; [policy lab](policy-lab.md) defines executable
profiles and read-only comparisons. These contracts govern completed-group reuse and evidence limits.

## Ownership allocation criterion

Publish the most likely ownership, historical labels, identity membership, continuity and numerical source
authority supported by the available evidence.
The objective is to become less wrong as evidence improves, rather than wait for certainty. Incorrect
allocation and withholding history that credibly belongs to an identity are both errors. An unallocated
result is one candidate explanation: choose it when it fits the evidence better than the plausible owners,
not as the automatic consequence of incomplete evidence or a missing certificate.

Evaluate the complete affected group. Weigh prior accepted ownership, dated marketplace occupancy, security
class and identifiers, provider mappings, continuity or conversion terms, and evidence for competing owners
together. A certificate carries a completed rule decision; its absence means that particular proof is absent,
not that every credible allocation is false. Missing evidence must be weighed alongside the positive evidence
and alternatives. Use generic, explainable rules, record provenance and applicable confidence, reasons and
warnings, and retain genuine ambiguity when the alternatives cannot be distinguished. Do not invent numerical
probabilities or make ticker spelling, issuer identity or price equality sufficient security-identity evidence.

Keep technical publication integrity separate from ownership inference. Selected generations, readable
handoffs, coherent numerical bundles and interruption recovery remain required. Repair a missing technical
capability through its owner while preserving applicable completed decisions; do not treat the missing
capability as proof that the history has no owner or repeat accepted historical checks.
Owning recovery must produce a valid publication of the best-supported decision; missing technical capability
is not a terminal ownership outcome.

The certificate-specific rules below describe the current implementation. Some still leave rows unresolved
or take an identity offline when a particular proof is missing. Those gates do not by themselves establish
that nonallocation is the most likely result. The [implementation review](trust-boundary-handoff.html#f64)
records this remaining policy gap; offline containment alone does not resolve disputed ownership.

## Lineage reuse

[contract.py](../libs/alpaca_lineage_build/contract.py), `reusable_decision`, and
[group_engine.py](../libs/alpaca_lineage_build/group_engine.py), `process_group`, require matching complete-group inputs,
dependency/recorded-absence sets, compatible versions, no pending pricing work and no explicit metadata refresh.
Receipts/index entries bind authorized policy ID, role, implementation tuple and semantic digest. File digest
records exact YAML bytes; formatting is not reuse identity.

Authorize the canonical policy once after journal recovery, before new selection/evaluation/network work.
Recovery validates frozen journal provenance without reinterpreting it through newly installed policy.
The [ruleset loader](../libs/alpaca_lineage_build/ruleset.py) accepts the canonical YAML's executable policy,
scoped factual applications and optional producers as one operation configuration. Their identities remain
separate: a container or formatting change does not establish new ownership or invalidate an applicable
prior policy. Its [completed value contract](../libs/alpaca_lineage_build/ruleset_contract.py) crosses stage
JSON without reopening YAML, compiling rules or accepting the same facts again.
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

Correlation-91 composes a separately evidenced public-common exchange with primary dated security episodes.
It can refine positive labels and identifiers within that completed continuing component, including a new
public parent whose legal identity and governance differ. Earlier completed correlation-90 decisions retain
their exact provenance until selected inputs change; frozen execution cannot consume the new refinement plan.

Correlation-90 admits explicitly documented automatic one-for-one exchange into a new public holding
company. Version-5 primary chronology can refine dated labels and identifiers inside that continuing public
component while preserving explicit numerical decisions and source positions. Exact completed correlation-89
decisions keep their original provenance until actual selected inputs change; earlier frozen execution cannot
consume the new assertion.

Correlation-89 admits explicitly documented automatic one-for-one reclassification of a continuing public
common holding. It records the actual legal instant and changed share rights separately from provider
identifier dates. Exact completed correlation-88 decisions retain their original provenance until actual
selected inputs change; frozen earlier execution cannot consume the new fact version.

Correlation-88 lets a documented security binding consume one completed public component that contains every
earlier source-scoped component for that observed UUID. It does not union incomparable components merely
because they share a UUID. Earlier completed correlation-87 decisions retain their original provenance:
those bindings had already resolved without this previously blocked containing-component case.

Correlation-87 admits version-3 documented public-security trading episodes. Exact completed CUSIP
observations and explicit security continuity can establish a first owner without a previously owned row.
Earlier exact revision-2 decisions, including correlation-86, retain their original provenance until actual
input changes select work. The new fact schema cannot execute under an earlier policy.

Correlation-86 interprets exactly observed own-common capital repayments using separately sourced
consolidation terms and dated CUSIPs. Existing correlation-85 revision-2 decisions retain their original
policy and generations until actual changed inputs select work. Missing provider UUIDs do not negate
documented common ownership; the dated identifier can remain explicit without inventing a UUID.

Correlation-85 admits explicitly sourced new-public-parent listing succession and component conversion terms.
A distinct legal registrant can continue the eligible public-common lineage under the version-2 conditions
below. Earlier exact revision-2 decisions retain their original policy and generations outside newly submitted
facts; correlation-82/83/84 are reusable authorities, not current production execution profiles.

Correlation-84 distinguishes dated market occupancy from same-security adjustment continuity. Exact observed
CUSIP bindings select an existing owner; regular-way and when-issued episodes supply historical labels.
The stage creates no UUID membership or observations. Explicit version-2 same-security/CUSIP facts can
retain a continuing bundle when matching non-dividend observations and accepted later cash dividends
establish missing adjustments in a temporary source. Existing correlation-82/83 revision-2 decisions retain
their original provenance until actual input changes, including a newly submitted scoped fact, select work.

Correlation-82 adds separately sourced public-common continuity and distinct-security conversion relations.
New facts select their complete affected group; earlier correlation-81/revision-2 decisions retain original
provenance when those facts are absent. No computation revision or blanket header promotion is introduced.

Correlation-81 adds the sourced identity-preserving rename described below. Its behavior requires the new
fact kind, which correlation-80 could not publish. Completed correlation-80/revision-2 decisions therefore
retain their original provenance and generations until their actual inputs change. Newly submitted facts
invalidate the selected group through the metadata owner; they cannot promote an earlier decision by changing
its header. Preserved earlier policy identities authorize reuse only, not new production execution.

Correlation-80 adds the bounded current-source exception after a closed label departure described below.
Completed correlation-79/revision-2 decisions retain their original provenance and generations when the accepted
receipt or compact index proves zero unresolved rows, a known UUID count other than two, or a completed group
size other than two. The exception requires exactly two labels and two UUIDs and cannot change those decisions.
Missing applicability proof selects owning evaluation; no implicit header
promotion, metadata crawl or historical audit establishes compatibility. Actual changed inputs still enter their
ordinary reconciliation gates. New format-35 output carries its completed current-source ownership handoff.

Correlation-79 added the explicitly sourced holding-company succession described below. Computation revision 2
requires owning reconstruction of all earlier completed decisions, including exact 34/78 revision-1 outputs.
Their approved metadata and pricing remain usable inputs with original provenance; an updated header cannot
establish a completed calculation. Newly computed output uses the current authorized policy and revision.
Completed revision-2 groups remain reusable and are skipped before upstream body reads on rebuild retries.
Newly submitted facts independently invalidate their selected group. Earlier policy identities are not
accepted for new production execution.

Correlation-78 scopes continuing-acquirer and terminal evidence and consumes completed related-symbol queries. Earlier
policy-less 33/59 and correlation-60 through 75 decisions still require selected complete-group evaluation;
the historical 33/59→34/60 proof remains restricted to its original production target.

[Migration](../libs/alpaca_lineage_build/certificate_migration.py) retains the exact 34/76 or 34/77 applicability
proof for the current target. The sourced-only extension cannot apply to those older inputs.
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
bundle. A complete dedicated source for a documented temporary market normally takes precedence there.
The positive adjustment-continuity exception below can preserve the continuing bundle. Outside those
markets, the latest independently observed own-label source wins. This selects observations, not historical completeness.
Equal complete bundles can collapse; unresolved authority stays explicit. All accepted dated labels survive
independently, including ambiguity. Readers/exports consume decisions without repeating selection/activity checks.

When overlapping bundles disagree in any field and request mapping `asof` lies outside accepted occupancy, lineage
requests only that source's full retained span at an observed own-label mapping date. Accepted departures across
CUSIP/UUID changes within one public identity also bound this route. Equal bundles, disjoint histories and
superseded sources do not trigger acquisition. A multi-identity source cannot be globally remapped to one occupant.
Pricing publishes correction before lineage selection; failure leaves the new selection unpublished.

An older occupant's closure does not disprove source-wide mapping when the same source independently observes a
later own-label occupant. Per-identity selection remains valid without replacement.

[Dated-market occupancy](../libs/alpaca_end_of_day_security_correlation/market_occupancy.py) separates actual
marketplace labels from later provider aliases. Version-1 sourced facts bind exact CUSIPs to an existing group
identity and carry contiguous dated episodes. A supported own-label observation can recover a withheld row;
a contradictory observed mapping prevents that direct authority. An incomplete temporary source cannot
displace a complete bundle. Whole accepted segments receive labels from metadata; interior label boundaries
require their exact stored timestamps. Unknown document publication dates remain explicitly null.

Version-2 facts additionally assert that delivery-basis markets represent the same security and CUSIP.
They can preserve a complete continuing bundle only when all non-dividend views match the temporary
source, the temporary dividend views omit their adjustments, and the continuing views differ. An accepted
same-CUSIP cash dividend must fall after temporary trading closes and before both acquisition cutoffs,
with a continuing own-label observation on or after that event. A later route or broader coverage alone
cannot establish this exception. The temporary historical labels remain intact.

For regular-way label corrections with applicable accepted owners and unchanged non-market evidence,
[market_assignments.py](../libs/alpaca_lineage_build/market_assignments.py) constructs dated assignments from
selected metadata. It preserves explicit numerical states and unrelated assignment references. The completed
generation-bound label projection carries into serving without historical numerical checks.

When newly documented temporary-market authority changes numerical source choice,
[market_observations.py](../libs/alpaca_lineage_build/market_observations.py) selects only its exact competing
sessions. The physical year/month overlap consumes that completed decision. The producer retains every
outside choice, source membership and the session grid. Exact complete-bundle equality carries its numerical
approval forward; serving checks only newly selected values that differ. Generation-bound revision metadata
and alias-preserving selected-bundle handoffs survive prepared publication and bounded journal recovery.
A completed unpublished label projection survives a changed adjustment decision: the refinement consumes
its labels and assignment artifacts, reselects only the invalidated dates, and measures numerical changes
against the original published states. Unchanged artifacts retain their completed handoffs.
Unknown choice flags require owning selection. A legacy negative outcome without its scoped projection
requires owning recovery; it cannot trigger an implicit audit of the remaining accepted history.

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
Research may supply the exact sourced event facts below; it cannot
establish an opaque provider UUID, move provider dates or directly assign rows.

### Sourced event facts

`dated_market_occupancy` version 3 separately supplies `documented_public_security` support: primary dated
names, exchanges, instrument kind, optional registrant CIK, and either the same CUSIP security or explicit
preexisting common shares that remained outstanding. The latter can span changed CUSIPs; it cannot join
unlike instruments or turn acquisition consideration into continuity. A bank and a later fund using its ticker
remain separate owners. A source's documented continuing provider identity outranks a different security's
historical use of its current ticker. Tied documented owners remain ambiguous; positions outside the known
episodes remain unallocated. Missing exact observations select owning metadata recovery. A completed 404 can
leave the dated UUID null when another observed UUID establishes the documented public owner.

Version 4 separately records `automatic_public_common_reclassification`: the legal entity continues and
each existing public common share automatically becomes one successor common or ordinary share. Its
`public_share_reclassification` records the timezone-qualified legal instant, explicit one-for-one units,
continued public holders and nullable unchanged-rights assertion. Governance may change. This treatment
does not claim that the earlier legal share class remained outstanding. Private targets, subsidiaries,
rights, units and funds cannot supply this public-common assertion. Dated provider identifier episodes
remain separate from legal effectiveness; a retained provider transition is not presented as an independently
confirmed exchange-effective date.

Version 5 records `automatic_public_holding_company_exchange`. Each outstanding public common share is
automatically exchanged one-for-one into a new parent, with the same proportional ownership and rights;
the operating predecessor becomes a subsidiary. `public_share_exchange` retains the actual legal date
separately from marketplace identifier episodes. This describes a different legal corporation and does not
assert continued legal existence. Private-target shares, subsidiary sales, CVRs, rights and units do not
inherit this public-common treatment. Earlier dated share changes require their own documented chronology
and retained provider observations.

The [documented ownership producer](../libs/alpaca_end_of_day_security_correlation/documented_markets.py)
fills unresolved positions and preserves explicit numerical states. Correlation-91 can refine positive
labels, CUSIPs, dated UUIDs and descriptions from a new primary chronology within its completed continuing
component. A separate public-common certificate can establish a new-parent exchange; each dated fact still
describes its actual security and registrant. Only newly submitted continuity proofs are certified;
accepted earlier proofs remain authoritative. Conflicting assertions or changes to an accepted fact require
owning reconciliation. Foreign owners remain outside the metadata refinement; competing primary
chronologies require owning reconciliation. This metadata refinement preserves row positions, values and
every explicit numerical state, including superseded, conflicted and incomplete observations. Unknown states
do not become numerical approval. New ownership
does not approve a numerical bundle. The owning selector evaluates only the newly owned positions and actual
new contests. Production and explicit documented research use the same
[documented-update owner](../libs/alpaca_lineage_build/documented_updates.py) for completed-plan merging,
new-fact binding, positive-date projection and numerical change scope. It performs no storage or provider
I/O and does not publish. [Source-work planning](../libs/alpaca_lineage_build/documented_work.py) closes unchanged peers
before payload access. Its completed version-1 identity binding and row projection survive publication and
ordinary suffix work under their original mappings and evidence. An interior episode boundary selects exact
timestamp assembly; endpoint ranges alone cannot establish sparse interior membership. Primary descriptions
travel in versioned dated assignment metadata; serving consumes them without accepting today's reused-ticker
description as historical issuer evidence. Other contradicted positive owners require their owning reconciliation.

When accepted source scopes contain different generations of one documented public component, the binding
can select the unique completed component containing all of them. A membership-only projection preserves
dated asset owners, labels and positions. Newly evidenced public-continuity edges compose complete accepted
components as an undirected relation; a shared UUID alone does not select a union. An already completed
containing component supplies its public ID without another identity calculation. Only newly joined numerical
overlap reopens its dated source contest. Incomparable components require owning membership recovery, not
a union inferred from a shared UUID. Completed bindings, numerical decisions and per-source publication parts
remain cached under actual receipt/pricing generations, accepted metadata and policy through construction or
publication interruption; a new phase or acknowledgement does not repeat them.

The optional `accepted_same_security_containing_component_v1` membership producer consumes completed
same-security rename evidence, a containing accepted component and certified extension edges. It remains
disabled when `producers.membership_refinement` is null. Mere UUID containment does not authorize a join;
missing legacy edge metadata preserves existing assignments without a provider or rebuild fallback.
[Membership updates](../libs/alpaca_lineage_build/membership_updates.py) can also consume an explicitly supplied
completed plan under the same store, complete group, pricing generations and changed-source/donor receipt
generations. It applies the recorded positions without repeating the route or UUID-edge proof. This is an
internal operation-context handoff, not a CLI plan-import option. Its completed values cross stage JSON;
only actually changed membership intervals and their new numerical overlap reopen selection.

For a version-5 public holding exchange that preserves rights and proportional ownership, a newly allocated
peer is superseded where exact retained timestamps already have
an explicitly selected complete eight-view donor bundle for the same public identity. This consumes the
numerical selection independently of serving eligibility. It reads no price values, compares no accepted
prices and requires no new certificate. Equal sparse endpoints or row counts do not establish overlap;
complementary partial donors cannot supply a complete bundle. Only actual new dates and a new overlap between
previously distinct accepted numerical owners enter selection. Completed timestamp keys carry into that
selection. Unchanged peer metadata also retains its original producer object through retry; another accepted
copy of the same receipt does not replace that binding.

An entirely superseded history with no unresolved positions stays closed to prices when its accepted dated
runs need no interior timestamp subdivision. Its completed primary metadata projection supplies the new
assignments, including canonical public references, labels, descriptions and dated identifiers. The numerical
supersession remains unchanged. Actual interior boundaries select their owning timestamp assembly; absent
numerical flags cannot supply this closed capability.

`issuer_capital_repayment` version 1 binds a recorded cash dividend or same-symbol stock-and-cash event
to documented continuing-issuer common-share consolidation terms, cash repayment and legal/exchange dates.
The metadata producer preserves raw provider records and supplies a separate split interpretation for
identity reasoning. A documented endpoint discrepancy remains explicit. This interpretation constructs no
prices or adjustment factors. Exact lookup results and the documented CUSIP chronology select dated owners;
alternatives already within one public identity do not create competing public owners. A completed 404 may
leave the dated predecessor UUID null while its CUSIP and public owner remain supported. Unknown lookups,
foreign candidates and tied or conflicting chronology remain unresolved.

When new interpretations all precede accepted later ownership, unchanged raw observations, endpoint mappings
and pricing permit a scoped carry into the documented enlarged component. Accepted owners, labels and explicit
numerical states remain closed. Only unresolved or freshly appended rows enter the sourced chronological
projection. A complete projection bypasses the ordinary row cascade and its source-grid calculations; any
residual alternatives retain those completed assignments during owning reconciliation.

`public_common_continuation` version 1 describes positively evidenced participation of non-redeemed public
common holders in the continuing registrant's public common security. Exact predecessor/successor CUSIP
lookups and documented one-for-one continued/reclassified units establish membership. Unreported unchanged
rights and express-exchange clauses remain null. Founder/private shares, redeemed shares, units, warrants,
ADSs, unknown ratios, branches and contradictory same-transaction cash treatment cannot use this rule.
An independent later cash termination does not revoke the earlier continuation.

Version 2 separately describes a new public parent. It requires an explicit non-continuing legal registrant,
different CIKs, exactly one already-public common predecessor, a privately held operating target, a listed
successor, and a documented one-for-one eligible public-share exchange. The exchange's dated public-market
succession and resolved provider endpoints establish the public lineage. A 1:1 acquisition ratio alone cannot
establish it; private-target, founder, right and unit paths remain distinct. This route does not assert that
the legal issuer or legal security is unchanged. Conflicting endpoints and competing public branches retain
explicit ambiguity. Excluded redemptions do not contradict the eligible non-redeemed holder path.

`distinct_security_conversion` version 1 records consideration between distinct securities without joining
their UUIDs. External or ambiguous endpoints retain identifiers and null resolved IDs. Legal effectiveness,
first-successor trading labels, alternative final sessions, consideration and settlement dates remain separate.
A real provider event, when explicitly named, must match its endpoint roles, ownership dates and jointly
reported ratios; each reported rate must be finite and positive. Missing target mappings and unreported rates
remain unknown. Only an exact continuing-acquirer certificate can exempt that acquisition from a later
same-security rename's conflict check; settlement timing does not move ownership.
Version-2 conversion terms can identify each unit constituent and holder-aggregate downward rounding without
cash for fractional entitlements. The relation declares `consideration_schema_version: 2`; the common and
right components retain separate terms rather than a fabricated unit-share price or an unconditional 1.2-share
execution. Neither the relation nor common consideration unions historical right/unit identities.

Both kinds use `match.source_id` and their own `facts.schema_version`, and require sourced provenance.
Ordinary updates load scoped applications from `evidence.applications` in the
[canonical lineage YAML](../resources/alpaca_security_correlation_policy.yaml) once. Each entry has a unique
`id`, explicit `symbols`, complete `supplements`, and optional boolean `refine_accepted_dates` (default true).
`policy` contains the executable cascade; `producers.membership_refinement` is an explicit optional producer
selection, null by default. These are configuration of generic implementations and sourced assertions,
not ticker-specific branches in Python. The legacy JSON loader remains an explicit compatibility adapter,
not a fallback or a second live registry.

Application symbols explicitly select existing source groups; historical ticker spellings do not select
unrelated current occupants. The compact lineage index records exact incorporated application fingerprints,
so new facts select their named anchors without reopening every receipt. Workflow completion requires those
facts and their applicable refinement intent in the journaled publication independently of price approval.
Omitted intent and explicit true have the same meaning and fingerprint. False preserves accepted positive
dates and labels while permitting the documented owner to assess unresolved positions; it does not retract
already published decisions. Changing only an explicit intent selects its affected owning projection even
when the fact body is unchanged. Accepted owner bindings remain inputs, not work to repeat.
This inventory records incorporation; the published certificates and assignments establish the resulting
lineage. Applications are additive: removing an entry does not retract already published evidence; a changed
or withdrawn accepted fact requires its owning reconciliation.
They retain separate metadata extensions and never manufacture provider events. A mapped successor history
claim is a separate source-bound producer result, restricted to accepted predecessor ownership and accepted
same-CUSIP label routes. It feeds the existing ordered row cascade; the production rule order is unchanged.
Accepted predecessor rows retain their UUIDs and dated labels when only public membership changes.
The source-work owner scopes new metadata to the prior source's accepted owners and source-bound
certificates, independently of the enlarged public component. Added foreign actions, occupancy and unresolved
endpoints cannot reopen those rows. Existing observations still participate in change/removal detection;
new contradictory own-label or exact-owner evidence, changed effective mappings and unapproved pricing
generations select owning reconciliation. Completed metadata and ownership survive a peer's prepublication
failure within their operation bindings. Newly selected numerical overlaps may assemble bounded counterpart
values while retaining their accepted owners. Completed donor checkpoints embedded in an offline decision
remain available under that selecting body's token; their applicable complete-grid result can cover a newly
owned duplicate without reopening the accepted donor. Source-work retains that selection within the operation.
The [format-37 contract](technical-contracts.md#standalone-artifacts) defines projection and recovery boundaries.

`lineage update --symbol SYMBOL --event-supplements FILE.json` accepts a bounded schema-1 JSON document with
`schema_version` and a nonempty `supplements` array. Each item has `kind`, `match`, `facts` and `sources`;
[event_supplements.py](../libs/alpaca_security_lineage/event_supplements.py) defines the exact fields. Each source
records an HTTPS document URL, title, document date and issuer/exchange/clearinghouse/regulator authority.
The operator establishes those facts through the [research loop](development.md#lineage-research-and-rule-improvement-loop).
Runtime does not fetch or interpret web pages. Facts must match the provider event ID, type, labels, identifiers,
dates and applicable ratios; changed or conflicting events fail rather than silently inheriting the disposition.
The following event-backed kinds use a real provider action ID:

- `split_endpoints` adds issuer-documented old/new CUSIPs to an ordinary forward/reverse split. The existing
  continuity rule still requires actual exact provider lookups for both endpoints; the supplement supplies no UUID.
- `derivative_only` identifies an option-root consolidation by exact underlying CUSIP, effective date and affected
  option series. The original provider action remains retained, but contributes no equity occupancy, rename,
  UUID edge or acquisition dependency. Similar labels alone never establish this scope.
- `listing_transfer_only` identifies a researched exchange transfer with unchanged equity ticker and CUSIP.
  An exactly matched, incorrectly classified rename stays in raw evidence but contributes no identity event.
  Distinct exchanges and the provider event date must match the sourced facts; a transfer alone creates no UUID edge.
- `identity_preserving_rename` supplies documented continuity of the same common security with unchanged
  shareholder rights and no share exchange. It matches the complete recorded rename, including its ID, endpoint
  labels/CUSIPs and processing date; its exchange-effective date must agree, while legal effectiveness remains
  separate. Version-1 facts include a bounded contiguous `prior_labels` sequence, each with `symbol`, the same
  old `cusip`, `start_inclusive` and `end_exclusive`. The final label must be the provider predecessor and end
  at its retained boundary. These are sourced historical labels, not fabricated provider events or present-day
  symbol lookup/acquisition dependencies.

The identity-preserving rename producer binds exact accepted CUSIP lookups to the documented endpoints and
reconciles conflicting endpoint/action observations from the complete selected group, including unavailable
peers. Unsupported conversions, structural branches, ambiguous mappings and contradictory chronology leave
membership unresolved. A completed certificate joins only the continuing security's UUIDs into one public ID;
current owners of reused labels and acquisition targets remain separate. Current descriptive names alone
establish neither continuity nor historical ownership.

A separate mapped-source handoff binds the source generation, actual current effective mapping and supported
dated labels to the observed complete source. Its predecessor rows enter the existing as-of machinery before
requested-label fallback. A dated historical occupant of the request label cannot redirect this mapped history
outside the selected continuing component. In disputed current-mapped scope, missing membership or source proof
leaves the prefix explicitly unresolved in the current implementation; this certificate-specific outcome
does not establish that no allocation is most likely. Historically pinned mappings retain their existing ownership rules.
Neither an identity certificate alone nor a matching price series authorizes arbitrary historical rows.

Metadata membership results, including empty or negative results, remain separate from source-history results.
Price-only changes cannot revoke that completed metadata decision. Changed prices/mappings select a new source
proof; changed applicable metadata selects the membership owner. Compatible append and recovery carry both
handoffs without historical row-rule execution, membership recalculation or source-payload reconstruction.

`holding_company_succession` supplies an independently documented missing parent substitution. Its
`match` contains an independent `source_id` and unchanged `symbol`, never a fabricated provider action ID.
Its version-1 `facts` record distinct old/new common-share CUSIPs, the legal and exchange effective dates,
an automatic one-for-one exchange and unchanged rights. The exchange date must be a market session and
cannot precede legal effectiveness. The metadata producer retains these facts under the separately versioned
`sourced_successions` extension and selects exact endpoint lookups; provider actions remain unchanged.

The correlation owner certifies only a direct same-ticker, same-exchange substitution between exactly two
known equity UUIDs. Exact CUSIP lookups must identify the inactive, nontradable predecessor and active,
tradable successor; the exact current-symbol lookup must agree with the successor. Complete retained action
coverage must span both dates. Additional or unknown occupants, related structural actions and contradictory
dated distributions veto certification. Distributions without a CUSIP supply neither endpoint support nor a
conflict. Legal effectiveness and exchange substitution remain separate facts; only the latter supplies the
row-ownership boundary. No prices or source-document fetches participate in this certificate.

One completed certificate crosses into established ownership and UUID membership before the frozen baseline.
Both phases and all views retain it, including a retry after later preparation fails. Ownership support names
the sourced fact explicitly; it never reports a provider corporate-action ID. Earlier policies do not consume
this new fact kind. Missing or conflicting endpoint mappings leave the history unresolved.

The metadata producer retains the raw action union, applied facts and equity projection in its completed receipt.
Newly submitted input crosses validation once; an immutable accepted handoff spans CLI, pipeline and metadata.
New facts trigger selected complete-group calculation; unchanged updates reuse the receipt and never reread the
input file or source documents. Metadata refresh reapplies retained facts to the raw provider events. Only missing
or invalidated exact mappings require acquisition. Explicit symbol selection is required; unmatched submitted
facts are reported as failure. Frozen price payloads are unaffected by this input mechanism.

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

Retained sourced-fact envelopes accept the known `version: 1` prepared spelling as the canonical
`schema_version: 1` envelope without replaying its completed facts. This transport spelling alone is not new
evidence. Unknown or conflicting headers fail into owning recovery. A selected catalogue observation is likewise
read by its persisted observation ID and timestamp; recovery consumes its accepted records and digest without
requesting or calculating them again.
The owning post-correlation publication consumer can finish from held source inputs, reports and retained-choice
scope under the normal writer and pricing locks. Current selector bindings establish applicability; acquisition,
materialization and correlation are not repeated after an interruption.

Selected retained-input recovery can carry that completed action-query scope when it covers every market date
admitted by the selected pricing request. It keeps the actual query bounds. Current descriptions or sourced
facts may require a new metadata projection using the retained action union; a later admitted market date or
an explicit refresh cannot inherit this scope. Broader sourced facts attached by a completed group producer retain
their accepted per-source scope; narrower consumer discovery cannot delete that evidence. Explicitly updated facts
remain actual changes. Conversion-recipient lookup outputs do not expand newly discovered fact scope by themselves.

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
