# Lineage contracts

| Question | Where to look |
|---|---|
| Could the same ticker refer to two securities? | [Reused labels](#reused-trading-labels), [dated-label collisions](#dated-label-collisions) and [limits of ownership evidence](#ownership-evidence-limits) |
| Did an exchange change, and on which session? | [Observed changes during updates](#changes-of-listing-exchange) and [historical listing assessments](#historical-listing-assessments) |
| Which prices or corporate actions belong to this ID? | [Dated numerical mapping](#dated-numerical-mapping), [sourced event facts](#sourced-event-facts) and [terminal ownership](#cash-distributions-and-terminal-ownership) |
| What can a later update reuse? | [Group reuse](#lineage-reuse), [membership and metadata](#membership-and-metadata-continuation) and [assignment continuation](#assignment-continuation) |

## Ownership allocation criterion

Evaluate the complete affected group. Compare each plausible owner with leaving the history unconnected. Weigh
accepted prior ownership, dated API observations, security class, identifiers, mappings, continuity and competitors
together. Ticker, issuer name or price equality alone cannot establish security identity.

Publish the reasonably supported most likely connection despite uncertainty. Otherwise record the unresolved
disposition and limits, leave those rows unpublished and acknowledge completed processing so peers can continue.
Record reasons, material weaknesses, applicable confidence and plausible alternatives without invented
probabilities.

The [generic inference instructions](../AGENTS.md#generic-lineage-inference) apply in production, debug, lab and
fresh recovery. Research may expose a missed or unrecoverable connection. Runtime uses provider observations,
applicable completed decisions and YAML-selected generic rules/parameters. Frozen journals retain original
provenance. Legacy research assertions cannot authorize new rows.

Readable publications, coherent numerical inputs and matching generations remain prerequisites. Missing capabilities
select owning recovery and preserve applicable decisions. See
[legacy-provenance safeguards](trust-boundary-handoff.html#f67) and
[open certificate gates that can still withhold supported ownership](trust-boundary-handoff.html#f64).

### Duplicate traded histories

`alpaca-cli-eod lineage repair-overlap --group-key KEY --store PATH` selects one published serving group. A strict
subset of a later split-linked provider UUID component can yield to its complete existing owner when dated labels
and every raw traded session match. An explicitly dated ticker change in a split can instead bound two existing IDs.
Exact old/new CUSIP lookups select endpoints. Matching raw OHLCV, VWAP and trade counts identify the duplicate
prefix. Each ID keeps its valid dates. Standalone `provider_split_relations` reports endpoint IDs, CUSIPs, terms and
date.

Distinct classes, carry-only overlap, conflicting routes or incomplete coverage retain prior decisions. Only raw
histories supply new cross-owner evidence. Serving uses slices of accepted history checkpoints and their numerical
choices. Retry resumes the group journal.

### Reused trading labels

`alpaca-cli-eod lineage repair-occupation --public-id ID --store PATH` assesses one online identity. A flat
zero-trade interval between traded periods marks a possible reuse boundary. Literal ticker and date-mapped issuer
routes must distinguish the periods. A prior route trading against the frozen literal price and then supplying a
distinct concurrent tape supports separate occupations. If that route ends, the current route must exclude older
trades, the prior route must exclude later trades and the carry must span ≥21 sessions. A contemporary CUSIP
identifying the accepted owner on the older trade date protects its scope. Later corporate actions can identify
their endpoints without retroactive ownership.

When a unit’s own mappings span both offerings, a common-stock component can distinguish two issues. Each unit
trades before its common. The common tape stops with the earlier unit and its two dated issuer routes contain
distinct trades. A dated common rename and matching successor history corroborate the later issue. A separation
whose exact CUSIP resolves to the accepted old issuer, with both named components merging that day, preserves the
older scope instead. Later retired-unit assignments become ownerless. Materially changed native terminal evidence or
historical pricing selects owning recovery.

After a terminal unit decision, `pricing repair-retired-unit --public-id ID --session YYYY-MM-DD` can remove a
post-retirement split factor from adjusted prices. The session selects the reported split. Its exact terms must
explain affected fields throughout the owned interval against raw and independently selected cash/spin views.
Consume the completed terminal decision and preserve all raw trades. Full lineage rebuild carries the accepted
interval if retirement, group and terms still apply and each member’s generation is unchanged or its mapped history
changed only after the last witnessed session. Overlap or missing binding requires selected recovery before
publication.

The journal binds completed route responses and unresolved outcomes to original group generations. Publication
retains the ID, supported dates and numerical observations. Lineage keeps displaced literal observations and
registers their last trade date for independent attribution to another issuer with matching traded history.

#### Former tickers identified by corporate actions

For a fresh OTC equity with a five-letter ticker ending in Q, Alpaca may return a listed-tape historical SIP trade
under the mapped terminal label. If ordinary traversal and native CUSIP paths find neither a former ticker nor a
usable identifier, metadata may query corporate actions once by four-letter root, reusing completed coverage. A
positive native cash dividend must name that root on an ex-date within the raw span and no later than the
listed-tape trade. With fresh daily rows in memory, require positive ex-date volume or trade count, or a directly
observed mapped trade that day. Exact dividend-CUSIP lookup must return the terminal US equity’s UUID and ticker
before the returned symbol enters ordinary metadata/pricing discovery. `otc_status_symbol_query` records filter,
coverage, supporting dividend, lookup and outcome. Empty response, unavailable lookup or different issuer leaves
spelling unconfirmed. Preserve applicable published assignments.

A selected inactive/OTC equity can yield a former label via its terminal ticker. Prefer the asset API’s licensed
CUSIP. Alternatively, a dated action must name the terminal ticker and CUSIP on the same participant and its
completed exact CUSIP lookup must identify the terminal UUID. Among independently resolved action CUSIPs select the
latest processing date. A same-day reverse split or rename can explicitly order old/new CUSIPs. Record equally
supported alternatives without selecting a filter.

After ordinary symbol-action traversal, if native endpoints revealed no other label, query complete US actions by
selected CUSIP over selected dates. `cusip_action_discovery` records origin, bounds, outcome, action IDs, process
dates and matching participant fields. Only the documented participant carrying that CUSIP can supply a ticker for
ordinary acquisition. A returned single-ticker action joins its normal evidence even if the symbol query omitted it.
For distinct parties retain the full transaction and matching role in CUSIP evidence. An already observed action and
exact CUSIP mapping may name the ticker when its terminal asset is later selected. Asset CUSIP remains a snapshot.
Empty responses, unmatched actions and denied access supply no dated label. Extensions request only uncovered
intervals. Legacy receipts keep completed scope.

In fresh group work, a positive cash dividend can identify its ex-date trading ticker. A stock or stock-and-cash
merger can identify the effective-date acquirer ticker if Alpaca supplies both parties with distinct tickers/CUSIPs
and positive consideration. The observation concerns the acquirer. The acquiree keeps its history. An exact
participant CUSIP lookup must resolve to an inactive/OTC equity under another ticker, confirmed by that terminal
ticker’s own asset lookup.

Compare the two already selected daily raw SIP histories. The terminal route’s mapping must postdate the traded
period. Literal requests directly support the trading label. Alpaca also treats an absent ticker as literal on the
mapping date. Absence can be supported by a contemporaneous exact 404, or after a subsequent occupant’s dated entry
and departure by complete action coverage and an exact 404.

Apply the dated label to the supported shared period. Varying prices and matching activity support continuity. An
interior mapped aggregate may contain fewer trades when opens/closes agree and its range fits within the former
ticker’s range. Matching full aggregates on both sides distinguish continuous trading from merely sharing a close.
Matching raw trades plus a dated event may support the event date alone when surrounding spelling is unknown. A
successful empty literal trade observation that day contradicts mapped trades. Disputed spellings keep their
ordinary disposition. Record the material limit in the source receipt.

Carry existing public ID, CUSIP, supporting action IDs and observed endpoints with the label. First/last observed
sessions bound price assignment. Formal rename dates and OTC trades need separate evidence. Cache assessment across
views/passes. Published groups and accepted prefixes keep applicable handoffs.

Adjacent independently stored labels may lack a connecting action, CUSIP or shared UUID. Once both are known, a
dated bar query can bridge their boundary. Its response identifies the requested route but may use a different
adjustment basis from independent generations. Fresh lineage discovers candidates through provider dependencies and
preserves each numerical generation until its mapping is established.

Both mapped fresh histories can share prices from an earlier ticker. The group may query the former ticker’s literal
daily SIP raw bar on the first and last eligible sessions, each a singleton with `asof=-`.
`former_ticker_literal_bars` retains exact requests, returned OHLC, volume, count, VWAP and both pricing
generations/mappings. Successful emptiness records `no_bars`, explicit provider rejection `unavailable`. Direct
lookups record the time of asset snapshot or 404. Catalogue-selected assets use catalogue observation time. An
existing literal route or supported current mapping can suffice. Completed literal observations survive peer
failures for the same store, former generation and mapping. A changed terminal generation selects a new pair
assessment that can reuse them.

#### Dated trade tape evidence

On a priced source, select the earliest qualifying dividend ex-date or acquirer effective date whose entire New York
date lies in the source span. Request its first chronological SIP trade under the literal event ticker (`asof=-`).
`historical_trade_tape` appears in the selected source’s `group.evidence["sources"][source_symbol]["evidence"]`.
[Standalone reader](public-api.md#standalone-reader)

An OTC asset with a gap after its last stored raw SIP session selects a sample on that date with the pricing ticker
and `asof`. A week of elapsed coverage qualifies. A shorter gap requires a known subsequent session within the
request. For the same raw date and provider UUID, an advancing current-label mapping keeps the original sample. A
changed pinned mapping or UUID selects its own route. `mapped_historical_trade_tape` identifies the provider’s
returned mapped symbol. The pricing route binds the UUID from its asset lookup. A historical ticker requires
separate evidence.

After HTTP 200 with no trades on the mapped raw-end date, a complete archived source may seek its latest earlier SIP
print. Query from the first complete New York date of the selected raw span through the preceding day, same
symbol/mapping, descending, limit one. Retain the endpoint response. `last_trade_search` records raw/request bounds,
pricing generation, UUID, mapping, actual request/response and proven absence dates. A subsequent empty interval may
retain an earlier positive response with the new request. Applicable pricing ancestry carries completed intervals.
Request only newly uncovered dates, newest first. A selected metadata refresh can extend an older empty point
receipt. An unchanged ordinary update preserves completed scope.

Each one-trade New York date sample records requested ticker, session, bounds, feed, mapping and HTTP outcome.
Returned trade fields, when present, are timestamp `t`, tape `z`, execution venue `x`, price `p`, size `s` and sale
conditions `c`. A print can coexist with zero eligible daily volume/trades. C denotes Nasdaq UTP, A/B their
respective CTA networks, other/missing codes unknown. An explicitly null/empty trade collection for the requested
ticker with no continuation records `no_trades`. Tape dates only the observed trade. Asset lookups retain separate
current/terminal exchange snapshots.

A fresh listed equity can have a zero-volume, zero-trade final SIP bar on a cash merger’s effective date. If
acquiree ticker and CUSIP resolve to the same inactive asset, request the last reported SIP trade from the latest
preceding positive daily bar through merger date. `terminal_cash_merger_trade` retains the mapped request/result,
target UUID, native action ID and cash rate. A print may fall after regular close. Daily prices, eligible activity,
public coverage and cash consideration retain separate producer records. Accepted histories do not trigger
retrospective searches.

An earlier listed-tape print selects the next market session by its actual New York date. An empty/unavailable
backward search supplies no date. If that session falls wholly within the source request’s New York dates, sample up
to one literal terminal-ticker trade with `feed=otc`. `otc_historical_trade_tape` records the actual response
separately. A print dates OTC trading to its timestamp. Successful emptiness records `no_trades`, HTTP 403/404
`unavailable`. Reuse an explicit OTC subscription denial within the operation for the same credentials. Record its
HTTP result only on the requested route. Unchanged sources retain the original outcome. A new route or explicit
metadata refresh can observe current access. Listing intervals and transfer dates need additional evidence.

Given a listed mapped SIP print and a complete empty first literal OTC page, `following_trade_search` can locate the
first later OTC print. Query once, ascending, limit one, from the next calendar day to the last complete selected
New York date. A dated departure, different observed occupant or accepted corporate-action departure may shorten the
search. Retain pricing generation, mapping UUID, contiguous empty coverage, latest physical request/response and,
for a print, literal symbol, New York date and original timestamp. Extend emptiness only beyond covered dates. A
completed first print stays bound to its original search. Ownership and OTC pricing require separate evidence.

#### Changes of listing exchange

Ordinary update retains both dated Alpaca asset observations when the same US-equity UUID changes listing exchange.
Source receipt `listing_exchange_changes` records each symbol, exchange and observation time. Ticker reuse by
another UUID cannot establish a transfer. A fresh store starts with its first snapshot. Earlier transfers need
earlier evidence.

For a stable ticker moving between NYSE and Nasdaq, retain literal SIP samples within the latest 15 eligible
sessions around the observations. Old/new network trades on consecutive scheduled sessions establish `prior_session`
and `effective_session`. If the new snapshot precedes trading, an old-network print can select one follow-up next
eligible session. The compact index schedules it after session close plus data delay, including weekends and early
closes. An intervening unchanged update leaves it pending. Indistinguishable networks, changed ticker or missing
trade can leave the effective session unknown while both asset observations publish.
[Metadata companion by public ID](exports.md#security-metadata-companion)

#### Historical listing assessments

The [historical listing migration](operations.md#backfill-historical-listings) requests literal-symbol Alpaca SIP
auctions and trades within a public ID’s accepted ticker dates. Reuse its attributed literal observations. Sample
endpoints and market-centre opening/closing auction venue changes. Auctions select candidate dates. Dated trade tape
A identifies NYSE, C Nasdaq, B multiple possible primary listings. The terminal asset exchange remains a separately
dated observation.

Opposing confirmed tapes select additional sessions within the bracket. On adjacent scheduled sessions the later
date is the supported effective date. Otherwise retain both bounds. Adjacent ticker endpoints may establish a
transfer across a rename for the same ID. Sampling may miss temporary moves or transfers within one network. Record
limits for unavailable prints, conflicting tapes and overlapping published owners.

Lineage owns durable observations and completed assessments. A changed competition interval selects only its
affected scope for correction. Other confirmed points retain their original identity. The catalogue and
[metadata companion](exports.md#security-metadata-companion) consume the completed decision.

## Lineage reuse

[`reusable_decision`](../libs/alpaca_lineage_build/contract.py) and
[`process_group`](../libs/alpaca_lineage_build/group_engine.py) require matching complete-group inputs, dependencies
and recorded absences, compatible versions, no pending pricing work and no explicit metadata refresh. Receipt/index
bind authorized policy ID, role, implementation tuple and semantic digest. The file digest identifies exact YAML
bytes. Formatting has no effect on reuse.

Recover the frozen journal under recorded provenance, then authorize canonical policy once before new selection,
evaluation or network work. The [loader](../libs/alpaca_lineage_build/ruleset.py) accepts schema-2 registered
generic policy and optional generic producers as one configuration with separate identities. Container/format
changes neither establish ownership nor invalidate applicable policy. Its
[completed value](../libs/alpaca_lineage_build/ruleset_contract.py) crosses stage JSON without reopening YAML,
recompilation, active factual applications or supplements.

Evidence fingerprints and history approval are independent.
[Workflow completion](technical-contracts.md#universe-observation-and-work-state) acknowledges fresh source-bound
approval without reopening its group or republishing an unchanged decision. Rule-only changes reuse approved
evidence.

[`evaluate_policy`](../libs/alpaca_lineage_build/policy_engine.py) builds row traces, pass digests and audit
summaries only on request. Mandatory phase/certificate checks and published/explicit trace digest semantics remain.
Each fixed-point result records consulted counterpart reports, including unavailable ones. Rerun a source only when
a consulted counterpart changes. Exact aliases share row allocations, each binding its own file/adjustment
provenance without retraversal.

Prepare dated occupancy and name-change continuity once before freezing first allocation. Later unrelated occupancy
can disambiguate an earlier rename. Keep its fallback interval out of baseline allocation. Both phases consume the
same continuity certificate. Before phase-two row rules, reserve established correlated segments, UUID membership,
provenance and bounds. Keep completed source reports closed. New fallback intervals must respect those assignments.
Only unresolved rows enter the cascade. Terminal fallback receives the full source partition. Preserve the phase
integrity guard.

Continuation retains accepted UUID membership apart from each dated provider UUID. Version-1 handoffs publish
compact membership, all-view identity/date contributions and own-label routes. Receipt ingress validates declared
handoffs once. Missing required new fields select owning recovery. Unmarked selected legacy assignments supply
bounded compatibility, including the named last-session boundary if the hot partition is empty. For a corrected
date, include unchanged competing contributors with accepted labels and frozen owners. Earlier assignments stay
closed. The continuation owner compares effective request mapping. Changed pinned mapping cannot inherit ownership.
Fresh acquired metadata can invalidate reuse even if retained acquisition inputs remain usable.

Receipt/index `computation_revision` identifies completed calculation semantics independently of metadata
compatibility, pricing generation and authorized policy. Revision 2 requires full retained-input reconstruction of
earlier calculations, including revision-1 decisions under correlation-78 or correlation-79. Only full owning
evaluation, never header-only promotion, can advance an obsolete revision.
[Rebuild scope, publication and retry](operations.md#rebuild-published-lineage-after-a-computation-repair)

Explicit [targeted retained recovery](technical-contracts.md#targeted-retained-lineage-recovery) can reopen
defective current-revision decisions alone. Full owning evaluation and journaled publication retain accepted
comparison membership and exact receipt/pricing generations. Its completion handoff is independent of computation
revision and ownership certificates. [Invocation and retry](operations.md#recover-selected-current-revision-lineage)

## Policy migration

Removing fact injection changes neither canonical ordered rule-policy identity nor completed assignments,
membership, labels or numerical choices. Earlier supported publications/policies remain readable under recorded
provenance and reusable only within accepted scope. Legacy assertions and rewritten headers cannot authorize new
inference or promotion. [Runtime and compatibility authorities](../VERSION_TRACKER.md#compatibility)

Explicit computation-revision rebuild owns obsolete calculations. Fresh evaluation uses retained provider
observations and current generic rules. No implicit store-wide migration, admission of retained research assertions
or replay of applicable work.

[Migration](../libs/alpaca_lineage_build/certificate_migration.py) retains the exact 34/76 or 34/77 proof for its
recorded target/prerequisites. Accepted complete-group metadata, including unavailable auxiliaries, tests
changed-rule prerequisites and concrete mapping-dependency conflicts. This determines applicability only. A complete
singleton with ≤1 accepted UUID cannot invoke the changed certificates. Affected/unproved groups use the existing
evaluator. Unchanged groups retain evidence, assignment references and pricing generations. Selection requires an
empty completed pricing-work boundary.

`lineage migrate-policy` journals compatible headers and lists remaining affected labels for
`lineage update --symbols-file FILE`. Exact-generation durable handoffs retarget matching serving decisions without
reopening assignments/prices or repeating numerical checks. Commit catalogue before acknowledgement. Interruption
preserves the handoff and cannot consume newer work. Run `catalogue rebuild` to convert older scheduling/work
projections, then `catalogue update` for compatible header handoffs. No other promotion follows.

## Observation selection

[`select_observations`](../libs/alpaca_lineage_build/observations.py) chooses one complete eight-view numerical
bundle per identity/session independently of ownership and dated labels. Retain applicable completed market/source
decisions within scope. Fresh selection normally favors the latest independently observed own-label source, without
asserting historical completeness. Exact-decimal-equal complete bundles may collapse across different provider
spellings. Preserve selected rows’ original precision/spelling. Keep unresolved numerical authority and label
ambiguity explicit. Dated-label resolution may settle a supported collision without changing prices. Readers/exports
reuse completed decisions without repeating selection/activity checks.

When a dated dividend or qualifying acquirer establishes a former ticker, that identity/session normally retains the
terminal bundle. Mark `former_ticker_numerical_basis_unconfirmed` when raw values agree but adjusted views differ,
or label evidence exceeds support for switching numerical source. Use the former route’s eight views if its raw
aggregate was strictly fuller, adjusted opens/closes match, its range contains mapped range and VWAP, and split
volumes admit a common scale within one reported share per route. Compare the selected pair using the completed raw
result. Common basis extends only to that session and provider precision.

If overlapping bundles differ in any field and request `asof` falls outside accepted occupancy, request only that
source’s full retained span at an observed own-label mapping date. Accepted departures across CUSIP/UUID changes
within one public ID also bound this route. Equal bundles, disjoint histories and superseded sources trigger no
acquisition. Never globally remap a multi-identity source to one occupant. Pricing publishes correction first.
Failure withholds new lineage selection. Closure of an older occupant alone cannot disprove source-wide mapping if
the source independently observes a later own-label occupant. Per-identity selection remains valid without
replacement.

Completed dated-market labels and bounded numerical revisions carry their accepted generation, exact dates and
source bindings into serving without ownership replay, price comparison or assignment reconstruction. Completed
unpublished projections can survive interruption under the same bindings. Only invalidated numerical scope reenters
selection. Unknown choice flags or missing declared capabilities select owning recovery without historical audit.
Closed results cannot authorize new market assertions or extend old chronology to new rows.

Ordinary source work retains its first completed equal-bundle result and explicit previously selected donors
independently of public-component changes. Equality supports numerical continuity only.
[Generation/date binding and serving reuse](technical-contracts.md#standalone-artifacts)

Legacy local `selected` flags and completed assignment projections cannot establish group-wide numerical coherence.
Two sources may select the same identity/session despite an earlier eligible serving checkpoint.
[Numerical-only completion](technical-contracts.md#numerical-only-completion) selects diagnosed boundary-local row
intervals under exact receipt/pricing generations. The shared numerical owner retains owners/labels, chooses only
that scope and hands the result through publication without ownership replay or price checks. Outside selections,
independent IDs and completed ownerless outcomes remain accepted. Applicable completed source equivalence selects
its direct carry owner before numerical reads and precludes rerunning its contest.

Eight-view equality retains the corresponding serving numerical outcome. Changed selected values require only their
dated serving assessment. A completed unchanged-label projection carries accepted sessions/ranges without
reconstruction. Missing required capabilities, a newly required mapping correction or an unresolved source contest
selects owning work. Ordinary update consumes explicitly enrolled numerical-only work without universe scans for
legacy overlap or treating absent optional capabilities as defects. Readers consume published choices and reject
competing selected observations.

### Dated numerical mapping

`alpaca-cli-eod pricing repair-rename --public-id ID --session YYYY-MM-DD` acquires a published rename’s earlier
eight-view interval under the successor’s dated mapping. Repeat ID/session pairs in matching order. If a batch
selects a group twice, serving publishes the first correction before reselection. ID/date select work. Accepted
ticker history selects the route. Require matching traded successor sessions in every view, all earlier sessions
present and independently dated original-label observations corroborating revised raw bars. Pricing stages completed
requests/comparisons, then commits immutable prices. The lineage journal selects their token for the complete group
under original ownership provenance. Standalone reads, online exports and applicable closed assignments on
continuation retain that decision. Changed inputs invalidate affected scope.

`alpaca-cli-eod pricing repair-carried-rename --public-id ID --session YYYY-MM-DD` addresses a stale close across a
provider rename. Traded witnesses on both sides, complete successor coverage, literal ticker observations and
competing dated ownership determine replaceable prices/labels. The catalogue projects accepted dates through ticker
lookup and public history.

`alpaca-cli-eod pricing repair-same-label-basis --public-id ID --session YYYY-MM-DD` addresses a basis change within
the opening ticker range. The dated route must reproduce the entire opening raw tape and all eight views of
successor witnesses. Traded prices must support comparable raw units across the step. Replace only the earlier
numerical basis. Preserve dated ownership.

`alpaca-cli-eod pricing repair-rename-split-basis --public-id ID --session YYYY-MM-DD` addresses a rename joining
disjoint routes or a successor mapping using a different later adjustment basis. Use accepted ID and predecessor raw
bars. Later provider split terms must reproduce traded successor witnesses, including volume/VWAP, at provider
precision. Earlier views must isolate split adjustments and support earlier splits. Competing traded occupations,
unresolved share conversion or a split at rename are unsupported. Preserve successor share basis, historical labels
and raw observations. Changed supporting terms or overlapping ownership invalidate selection.

`alpaca-cli-eod pricing review-split-basis --public-id ID --session YYYY-MM-DD` assesses a reported split against
native terms, eight boundary views, nearby traded daily opens and literal regular-session closing/opening trades. If
a material reported share change conflicts and other views lack independent economic terms, serving starts complete
public history on the selected event date. The receipt preserves ownership, source prices, witness window and
restriction reason. Applicable continuation retains the decision. Changed witness prices, terms or numerical
selection require reassessment. Repetition can restore earlier online history once observations positively resolve
share units. Completed checkpoints survive interruption. Repeat ID/session pairs in matching order.

`alpaca-cli-eod pricing repair-split-timing --public-id ID --session YYYY-MM-DD` compares preceding, selected and
following exchange sessions for a factor change bracketed by trades. Dated actions, opening share units and all
eight views must support one timing correction. Reconstruct only the affected session’s split-containing views when
observed price/volume precision determines their values. A single flat zero-activity session may carry each adjusted
view’s previous close if raw tape and action establish a unit change next session. The receipt selects the bounded
fragment or records insufficient support. Changed relevant actions/price window invalidate that scope. Full rebuild
carries a one-session correction when witness contributors retain prices, owner, trading label, selection and terms.
Changed peers can carry when completed choices exclude them on every affected witness both before and after change.
A complete legacy ownership handoff can establish absence throughout its partition. Incompatible/unknown witness
bindings require pricing recovery before publication.

`pricing review-historical-split-basis` reviews a flagged day before the published ticker traded under its name.
Single-session market actions select candidate terms. Literal raw OHLC, VWAP, volume and trade count must identify
one historical ticker throughout the traded window. Its regular-session trades supply ordinary price eligibility.
The ticker identifies only the selected window. Whole-occupation labelling needs dated evidence.

`alpaca-cli-eod pricing project-historical-label --public-id ID --session YYYY-MM-DD` uses the completed historical
review’s original pivot. Extend its literal trading-label witness on additional sessions using actual
former/successor routes. A unique traded match in every raw field, supported successor observations and other
selected owners’ dated occupations determine the interval. Journal labels independently of prices. Online
views/exports expose only eligible sessions. Standalone history exposes dates on the original resolved source.
Changed supporting ownership, source prices, relevant actions or original review can revoke labels.

`alpaca-cli-eod pricing project-displaced-label --public-id ID --session YYYY-MM-DD` selects the existing owner’s
first session under its present ticker after another completed lineage decision displaced an earlier occupation.
Select former tickers from the preceding exchange session in the index. Recorded literal raw observations must match
the earlier tape. The current ticker must lack competing earlier trades and match following traded observations.
Only a unique supported occupation supplies the former label. Preserve public ID, all prices and prior CUSIP scope.
Use null without independent earlier-label CUSIP evidence. Later unrelated securities may use the same ticker.

`pricing repair-former-label-basis` selects the first published session after rename. Test whether future-ticker
terms explain a price step during predecessor tenure. The earlier dated route must cover the prefix and match traded
witnesses in all views on both sides of the actual rename. Literal raw observations corroborate revisions. A
simultaneous genuine split retains documented units and requires matching pre-rename adjusted witnesses.

`pricing repair-split-carry` selects an ID/session with traded bounds `--from` and `--through`. Intervening
zero-activity bars may carry the last traded value in corrected units if closes, VWAP, provider endpoints and the
full bar sequence support it. `pricing repair-duplicate-split` tests whether two reported factors explain one
observed unit change. Remove a supported extra application only on the affected prefix. Preserve independently
witnessed cash adjustments and the actual trading-date transition.

`pricing repair-conversion-timing` selects the first session where a reported split changed adjusted basis. A later
same-ticker conversion must identify destination CUSIP, agree on the ratio and coincide with a traded change in
close and VWAP. If intervening sessions still trade in old units, correct their split-adjusted views. Preserve raw
trades and published conversion-session values.

`pricing review-untraded-split` reviews the last traded session through the next trade or stored endpoint. If a
native split changes share units and the literal tape repeats the old price without trades, omit unsupported public
marks from the split date. Retain prior prices and supported later trades. A reopening compatible with numerical
carry correction selects its separate assessment. Standalone source bars remain accessible.

`pricing review-product-quote` checks provider product metadata and traded quote convention for dollar-price
support. A Nasdaq NextShares fund quoting relative to NAV near 100 on both sides of a corroborated share change
supplies a premium/discount. Without dollar NAV the whole ID is ineligible for public dollar-price history. Source
and eligibility decision remain inspectable.

For a direct rename with disjoint histories, selected retained-source work may request the continuing label at its
accepted observation date, supported by shared UUID/CUSIP and post-transition cash distribution. A documented
same-label split chain including a successor split can support mapping at rename, witnessed by the predecessor’s
last observed session. Both reports must select one public membership with supporting dated ownership and provider
CUSIP lookups. An older outside UUID qualifies only as a retired predecessor split endpoint before that witness and
gains no ownership.

The [planner](../libs/alpaca_lineage_build/numerical_mapping.py) runs after the unchanged-work shortcut, before
selected acquisition/numerical materialization. Exclude fresh sources without applicable completed ownership routes.
Durable work policy retains exact selected requests across interruption. Resume pending mapping before ordinary
maintenance advances those sources.

First request one exact fully adjusted predecessor session. Empty response records the mapping limit in existing
receipt evidence issues. For a split chain, next request the last accepted successor session under the same mapping.
If absent, record insufficient coverage before contribution-grid assembly or history acquisition. Positive witness
rows carry into remaining acquisition/endpoint checks across interruption. Neither outcome changes membership, fills
gaps or derives prices from distributions. A missing internal contribution-grid capability requires owning recovery.
Missing known required contributions in an acquired history complete a provider-coverage assessment without
replacing accepted prices. Provider failures retain ordinary execution/retry handling.

For the selected mapping only, a serving owner may construct an unmarked legacy checkpoint’s optional session-date
projection from accepted raw positions under exact pricing/lineage bindings. Preserve numerical outcomes and
ownership. Lend raw rows and selected manifest to subsequent assembly. Persist dates before acquisition. Declared
missing projection requires owning recovery. Internal recovery uses blocked work without transient-request retry
timers. Restore the most advanced journal stage. Missing bindings, malformed observation envelopes and unreadable
acquisition state block recovery with original decoding cause. Preserve selected serving-control exceptions as
causes instead of inferring an absent date capability.

Applicable assessments survive receipt publication/acknowledgement. Changed source, mapping, provider facts or scope
select owning work. Older receipts lacking the optional capability trigger no scan or automatic repair. Historical
adoption is explicit. Numerical-only recovery keeps its zero-provider contract. Readers/exports consume resulting
provider-view choices without asserting an absolute corporate-action horizon.
[Pricing handoff, coverage and interruption](technical-contracts.md#dated-mapping-acquisition)

## Dated-label collisions

Canonical `observed_rename_periods_over_mapped_alias_v1` uses a provider rename and disjoint old/new own-label
source periods within one accepted public owner. These dated labels can outweigh a mapped alias spanning both
periods. Remove the broad alias only from an existing collision containing exactly one supported chain label.
Preserve unique labels, prices, owners and membership. Never invent labels. Source periods date the observations.
Provider processing date cannot alone date the trading transition. Record missing/inconsistent CUSIPs as limits.
Competing rename routes, overlapping direct periods or contrary dated alias actions preserve ambiguity. A uniquely
directed round trip may prefer its temporary label within an observed contained interval if a provider split is
effective at its start and return follows its end. Never extend the temporary label beyond that collision interval.
[Dated-label rule](../libs/alpaca_lineage_build/dated_labels.py)

[Normal lineage processing](../libs/alpaca_lineage_build/label_updates.py) journals selected historical collisions.
For exactly two coextensive same-owner sources, prefer the only label named by provider actions within their span if
its alias has no competing dated action/rename route and both accepted latest requests began after that history
ended. This distinguishes stopped from ongoing mapped histories. Empty/manual request scopes limit inference.
Ex-dividend/effective dates precede processing dates in priority. This cannot date a rename or extend into future
alias-only history.

Without a three-source rename pattern or two-source named dated action, skip assignment traversal. Otherwise
accepted assignment metadata supplies remaining conditions. Whole-segment relabelling reads no prices. Interior
splits assemble only required timestamps from manifest-selected physical views, sharing file aliases. No price
comparison, ownership replay, provider requests or store scan. Receipt completion binds source/assignment
generations, accepted request scopes and held actions. Owning source work invalidates changed bindings while
preserving produced labels. Consumers never clear or reconstruct stale handoffs. Applicable completion survives
retry and journal recovery. An accepted prefix can carry the archive/rename relation onto newly owned
continuing-label suffix rows without reconstructing historical decisions. Changed relevant observations/generations
select owning work. Legacy research annotations supply no evidence.

For a published collision following a [split between two renames](#name-changes), run
`alpaca-cli-eod lineage update --symbol TICKER --no-pricing-acquisition` at stored cutoff. Use group native events,
accepted public membership, request mapping and dated assignments. If the successor starts at recorded departure and
the old mapped source spans it, close the retired route and correct affected CUSIP, UUID and ticker when supported.
Preserve selected numerical source in every view. Serving projects corrected ticker ranges from accepted session
dates and the existing dated-label handoff without changing numerical outcomes.

Resolving an existing collision establishes neither all unique labels nor all unresolved owners. Public online
screening excludes offline identities and superseded source-label records.
[F62/F64 limits](trust-boundary-handoff.html#f62)

## Ownership evidence limits

Complete established correlation before occupancy fallbacks. An unresolved reused active ticker may start at its
first positive provider action after every rival UUID’s actions, with exact active symbol/CUSIP agreement. Earlier
unsupported rows stay ambiguous. A closed earlier label interval permits the later suffix and retains its own
supported rows. Open/overlapping intervals veto. Ignore a foreign-label distribution for occupancy only if the
completed group contains its exact own-label action, that source assigns the session to the exact UUID in all eight
views, current-label actions bracket it and no related structural action exists. Retain provider evidence. Equal
prices, bar timing and research cannot establish identity. [Occupancy](../libs/alpaca_lineage_build/occupancy.py)
and [ownership rules](../libs/alpaca_end_of_day_security_correlation/ownership_interval_rules.py)

- **Same-ticker distribution.** An isolated one-for-one `unit_split` can join continuing listed old/new UUIDs if
  exact provider lookups identify both CUSIPs, agree with active ticker lookup and separately identify the
  recipient. Use ex-date or processing date corroborated by effective/payment date. Earlier rows use old UUID, rows
  from cutover use new UUID, both under one public ID. Recipient keeps its own ID. Established owners, competing
  actions and conflicting lookups retain precedence. The
  [distribution rule](../libs/alpaca_end_of_day_security_correlation/same_ticker_distribution.py) consumes evidence
  once per fresh source. An applicable prefix carries cutover into append/correction. Unchanged inputs preserve
  completed receipts. At unchanged cutoff reopen a previously completed unresolved group through
  [targeted retained recovery](technical-contracts.md#targeted-retained-lineage-recovery).
- **Cash terminal.** A terminal date alone cannot establish unbounded prior ownership across a distinct occupant
  with positive claims on both sides or an established closed earlier owner of that label. The closed-owner veto
  concerns only newly inferred unbounded intervals. A positive own-CUSIP dividend/component action may bound a later
  suffix, including an inactive issue, after closed prior intervals and without rival/unresolved occupancy overlap.
  Start at first accepted positive action, never inferred from prior departure or terminal payment. Accepted dated
  intervals/split continuity remain authoritative. Provider actions/candidate relations remain evidence.
- **Distinct-security stock merger.** A dated merger may end a former issue’s interval and start the current issue’s
  on the requested ticker. Require inactive acquiree matching former interval, active acquirer matching both exact
  symbol/CUSIP observations and agreeing event dates. Same-acquirer transfers may preserve the boundary. Assess
  competing dated owners separately.
  [Preserve both UUID memberships](../libs/alpaca_end_of_day_security_correlation/stock_merger_occupation.py).
- **Ticker-reuse gap.** A complete active suffix ends at its last observed session if a later request adds no
  contrary owner. Cutoff confers no ownership. Require one exact active-symbol lookup, complete agreeing grids
  without internal missing sessions, no current UUID in the earlier episode and no conflicting suffix UUID. An
  incomplete later request cannot revoke an independently assessed closed earlier episode.
  [Gap rule](../libs/alpaca_end_of_day_security_correlation/ticker_reuse_gap.py)
- **Historical mapping.** `asof` selects mapping, never a general observation cutoff. Preserve a complete
  informative eight-view prefix if its last session is the mapping date, an immediate market-session gap follows,
  and complete later provider evidence names one inactive predecessor and a distinct-symbol/CUSIP/UUID successor.
  Stop mapped ownership at the gap. Subsequent pre-endpoint rows stay unresolved absent ordinary independent
  mapped-continuation proof for those exact sessions. Bar timing/research supplies neither ownership nor a transfer
  date. [Predecessor prefix](../libs/alpaca_end_of_day_security_correlation/predecessor_prefix.py)
- **Closed departure and current source.** Unique-candidate ownership can cover a complete informative
  current-mapped source wholly after a direct accepted unchanged-UUID/CUSIP rename. The retained request starts by
  departure, all eight views share a complete session grid through accepted coverage, and exact active/tradable
  own-label lookup identifies the distinct current UUID. The group must contain exactly those two labels and UUIDs.
  Additional relevant occupants, conflicting actions/owners, incomplete evidence or historical/mapped-alias
  selection veto. Neither infer nor require current CUSIP, dividend or minimum vacancy. The complete renamed source
  must cover observed sessions. An exact raw copy vetoes, and differing prices cannot establish identity. Certify
  observed rows only. No global interval, IPO date or UUID edge. Produce once from prepared source and chronology.
  Receipt carries applicable result through continuation/unresolved-peer reconciliation.
  - A separately identified current occupant may own a complete later source if retained actions identify all rivals
    and establish their exit before its start. Terminal exit requires dated cash merger or worthless removal
    supported by the former occupant’s own endpoint. Retain exact current-lookup/complete-source requirements. Any
    competing claim on source dates leaves history unresolved. No UUID link.
  - A complete current-mapped tape can support its exact own-label ID when the sole inactive rival resolves
    independently by CUSIP and all retained actions are dividends predating the tape. Keep owners separate and
    record observed date limits. Rival claims within the tape, structural actions or incomplete scope select
    ordinary assessment. If all-view inert rows separate older dividend claims from resumed trading, the separately
    observed current episode can bound its own start. Earlier episode/inert rows retain their dispositions.
  - For a single-label source, exact active ticker and one inactive competing UUID, a same-ticker split may bound
    current observations. Require corroborating raw/split-adjusted units and complete later grid in all views. For
    several splits naming the same inactive CUSIP use latest corroborated date. For one corroborated split and
    matching equity endpoints, connect the preceding continuous traded episode, bounded by missing sessions,
    prolonged flat carry or competing ownership. Carry completed split comparison and post-split assignments.
    Preserve public membership. Exact vendor UUID cutover remains unknown.
  - A contemporaneous active catalogue observation can support only the latest traded session if ordinary mapping
    agrees, eight views align and action coverage through observation finds no competing claim. Observe on the
    session date or before the following regular open. Compatible continuation can extend without rechecking prior
    observations.
  - A completed same-symbol split to the exact active UUID or two dated dividends for its exact CUSIP can support a
    complete traded current-mapped span with agreeing views from its first dated witness. Earlier ambiguity keeps
    its scope. An open claim by a competing owner/overlapping peer blocks assignment. Dated unit separation can
    distinguish an independent, separately identified unit/warrant trading alongside the current security. Inherit
    existing UUID links only.
    [Closed-departure certificate](../libs/alpaca_end_of_day_security_correlation/closed_departure.py)
- **Vacant reused ticker.** Certify a complete prolonged bridge as non-observation only between accepted direct
  same-identity renames of distinct UUID/CUSIP occupants. Require no intervening owner, exact full session grid of
  flat zero-activity/zero-VWAP rows in eight views, raw carry of departing close, traded adjacent rows and
  independent predecessor/successor-label sources owning and trading every bridge session. Adjusted padding may step
  only on exact positive dividend ex-dates, which cannot establish tenure. Leave bridge rows unassigned and retain
  edges, intervals and action dates. Incomplete evidence, raw change, actual activity, third occupant or structural
  conflict leaves ordinary unresolved coverage. The same disposition may consume a closed departure and a later
  exact action-anchored unit-component suffix separated by a complete inert gap. All eight-view, calendar, carry,
  adjacent-trade, occupant and conflict vetoes apply. Unresolved CUSIP evidence must lie outside the gap. Assign
  neither endpoint and join no UUIDs.
  [Terminal episode](../libs/alpaca_end_of_day_security_correlation/terminal_episode.py)
- **Pre-entry carry-forward.** For a direct unchanged-UUID/CUSIP rename with a complete action query, exact endpoint
  ownership and complete aligned sources, a prolonged flat inactive destination tail may be non-observation if it
  repeats its last traded close through boundary and an independently owned predecessor overlaps it through the
  immediately preceding market session. Keep earlier traded destination issue anonymous and tail unassigned.
  Preserve actions, intervals and membership. Missing overlap, competing owner, incomplete views or any
  changed/positive tail row preserve ordinary unresolved coverage.
  [Name-change rule](../libs/alpaca_end_of_day_security_correlation/name_change.py)
- **Terminal continuous ticker.** Preserve accepted components. Joining two UUID components additionally requires
  complete informative history without prolonged carry and retained structural-action evidence. ≥3 disconnected
  components stay unresolved. Independently certified closed departures retain narrower rules. Ordinary inactive
  sessions neither create nor invalidate components.
  [Terminal fallback](../libs/alpaca_end_of_day_security_correlation/terminal_fallback.py)
- **Mapped predecessor tail.** Successor ownership requires an accepted cross-UUID provider edge, mapping date
  inside predecessor interval, unique incoming/outgoing branches and an independent correlated successor for those
  exact sessions. Match every field in all eight views. Correlation assigns successor rows. Numerical selection
  supersedes predecessor copies. Equality alone, incomplete views, reused labels, competing branches or differing
  values leave rows unresolved.
- **Mapped successor prefix.** A successor starting inside a leading prolonged inert donor run and reaching an
  informative traded tail may inherit exact owner of that complete prefix from a completed predecessor report
  through one accepted shared-UUID/unchanged-CUSIP rename. End immediately before the retained boundary. Require
  matching timestamps and all nonsplit fields, complete aligned views and accepted predecessor-label ownership of
  every donor position across eight views. Ordinary active overlaps retain stricter producers. A later extra UUID on
  the successor label is noncompeting only on one complete unbranched post-rename chain of accepted same-label,
  identity-preserving splits. Unrelated, pre-handoff or branched occupants veto. Only this chain permits differing
  split-bearing vintages. Adjusted values supply no identity evidence. Reject the whole candidate for recursive
  mapped donor, incomplete source, changed acquisition context, competing current owner/rename branch, gap or
  mismatch. Reuse donor assignment. Equal prices alone cannot supply identity. Preserve the action boundary and UUID
  membership. [Predecessor prefix](../libs/alpaca_end_of_day_security_correlation/predecessor_prefix.py)
- **Multiple dated renames and intervening splits.** An actively traded successor request may carry each
  original-label donor’s independently owned dates across all eight views. Match donor raw trade fields to mapped
  prefix, reject competing routes and carry dated ownership. The continuing request supplies common-basis prices. If
  it starts later, test first two overlapping sessions for consistent scale change in split-adjusted OHLC. Once
  established, public eight-view history starts at overlap. Contiguous earlier donor-only positions retain internal
  owners/prices and `incompatible_split_adjustment_basis`. A terminal traded episode can bound the matched prefix
  despite an earlier mapped ticker episode. Require actual donor’s complete interval assignments and one complete
  later source. Supersede earlier donor-only prices only if first two traded matches establish incompatible split
  basis. Alternatively a single dated same-label CUSIP change can bound entry to the donor’s accepted share-class
  route. This scope must cover the start of public history and cannot move a discontinuity to an earlier public
  session. Preserve outside labels/assignments on both paths. Ordinary continuation carries completed choices and
  compares only reopened positions.
  [Mapped ownership](../libs/alpaca_end_of_day_security_correlation/mapped_rename_chain.py) and
  [numerical selection](../libs/alpaca_lineage_build/adjustment_basis.py)

Preserve provider action-date field semantics and supported endpoints. Earliest/next-session dates, bar boundaries,
external marketplace dates and ticker exceptions cannot rewrite them. A separately certified source span dates only
its observed rows. Missing/conflicting inputs can remain unresolved under current certificates without preferring
nonallocation over the [most likely owner](#ownership-allocation-criterion). Research may improve/test a generic
rule supported by retained provider observations. It cannot supply event assertions, identify opaque provider UUIDs,
move action dates or assign rows.

### Sourced event facts

Fresh classification applies identical provider-evidence rules to reserved-looking labels/names. No hardcoded ticker
snapshots or pattern substitutes. Completed legacy exchange-test classifications remain readable only within their
publication scope.

Production, debug, lab and fresh recovery reject authored factual applications, event supplements, assertion
overlays and research certificates. Schema-2 canonical YAML selects only generic policy/producers. JSON fact
imports, legacy configuration fallbacks and removed `--event-supplements` provide no inference or repair authority.
[External research loop](development.md#lineage-research-and-rule-improvement-loop)

The [provider-evidence owner](../libs/alpaca_security_lineage/inference_evidence.py) selects retained raw provider
actions and exact observations for fresh inference. From a legacy receipt’s separate raw provider union, rebuild
only the derived projection, preserving accepted query coverage and local-market evidence. Missing required raw
observations select owning metadata recovery. Lend the once-accepted `ProviderEvidence` downstream without
retraversal, reprojection, store reads or provider calls. Reject external assertions without converting them to
evidence.

Published assignments, membership, labels, descriptions, numerical states and applicable handoffs retain authority
in accepted scope. Legacy research provenance remains inspectable and supplies neither assertions, rule predicates
nor certificates for new rows. Frozen journals install completed artifacts under original provenance. Removing
injection authorizes no rollback, historical replay or storage crawl.

An unresolved legacy factual obligation lacking applicable completed output requires explicit owning generic
recovery. Never rebind the old fact, supply another assertion or reconstruct accepted history to fill a missing
handoff. Completed peers continue. New generic assessments may connect supported owners or complete ownerless
lineage. Record reasons, limitations and alternatives.

Optional `accepted_same_security_containing_component_v1` compares an accepted predecessor assignment within the
same current component to the donor’s completed membership, directed split edges, dated UUID, CUSIP and sessions. A
same-UUID/CUSIP label change can support the route. Competing transitions or conflicting owners keep their
dispositions. Coverage may span runs. A hole limits transfer to supported positions. Choose one source across views
for newly joined contests and carry unrelated assignments and applicable numerical choices.

Fresh correlation lends completed edges to refinement and publication. For a selected legacy group with unchanged
inputs and containing memberships, retained provider metadata can recover missing split and same-UUID alias edges.
Donors needing historical continuity evidence require explicit complete-group retained recovery. At unchanged
cutoff, ordinary update can select applicable pre-v94 legacy groups with multiple sources/IDs whose published
identities overlap under one ticker. Screen dated label ranges, including accepted ranges of ineligible IDs, and
retain results in the compact index. Overlap selects the whole group. Lineage assesses dated source evidence and
records completion. Preserve each source’s accepted cutoff unless pricing independently owes advancement.

[Membership updates](../libs/alpaca_lineage_build/membership_updates.py) can consume a completed internal plan bound
to store, component, pricing, metadata and assignments. Stage transport carries selected donor ID and exact
positions. Negative decisions bind every selected receipt generation, potential competitors included, and
fresh-correlation metadata. Changed binding selects owning work. A one-way transition cannot trigger the round-trip
rule. Setting `producers.membership_refinement` to null disables refinement.

An exact own-label 404 plus retained last positive UUID selects metadata assessment. Use a held UUID catalogue or
exact observation first. Request the UUID only as required. New catalogue conflicts select one exact UUID lookup.
Reordering unchanged conflicts reuses it. Keep old-label 404 distinct from current spelling. Completed UUID
responses for unfinished work survive interruption. The compact projection selects pending retirement/changed UUID
observation without opening receipts. Correlation uses prior observation date, mapping, dated claims and competitors
to establish supported sessions. A UUID observation cannot date a rename. Missing identity evidence may complete
ownerless history. Request failure retains its workflow obligation.

Fresh inference, closed source work and publication have distinct authority. Accepted owners/numerical choices stay
closed. Only new selected rows, newly joined overlap or changed bindings reopen work. Entirely superseded histories
requiring no interior timestamps stay closed to prices. Completed projections, timestamp keys, assignments and
numerical choices survive construction/publication interruption under actual generation, metadata and policy
bindings. Missing/incomparable component capabilities require owning recovery. Shared UUIDs or recalculating
completed work cannot supply them.

### Cash distributions and terminal ownership

Alpaca may report a cash merger and special cash distribution with identical security/amount and merger effective on
dividend ex-date. Due-bill terms or pre-ex-date payment can support a distribution. A later action must name the
original ticker/CUSIP as departing and identify a distinct replacement. With positive volume and count after
ex-date, original shares may own observed active sessions before that later action. Retain both cash records and
their dates. Competing issuer or incompatible payment terms preserve existing assessment for affected dates.

### Name changes

These certificate routes bound specific inferences. Their absence leaves the
[allocation criterion](#ownership-allocation-criterion) in control.

Renames establish dated endpoints. Conflicting effective/processing dates or component-arrival evidence leave
disputed windows unresolved. Join distinct UUIDs only with independent continuity, either an accepted
identity-preserving event or an exact informative raw predecessor prefix through the boundary with complete matching
eight-view grids. Use selected group inputs and exclude decisions derived from the disputed edge. Divergent
independent concurrent sources keep their histories.

A forward/reverse split between entry and exit renames can change CUSIP/UUID under a continuing ticker. Follow
native Alpaca events chronologically. Each rename preserves the issue and each split connects successive issues.
Apply the split’s respective UUID/CUSIP to each side. Competing issues, branches, incompatible dates or
contradictory positive literal trading in the same occupation retain ordinary assessment. A mapped response may
continue under a retired request symbol past tenure.
[Split and rename rule](../libs/alpaca_end_of_day_security_correlation/split_rename_ownership.py)

A cross-ticker stock merger closes an established acquiree interval on its effective date if the dated action agrees
with exact inactive acquiree and distinct acquirer endpoints. A separate distribution or later competing evidence
leaves the boundary undecided. A same-day one-for-one record with identical source/destination ticker and CUSIP adds
no claimant alone. Record terminal provenance. A later traded session may use an existing contemporaneous
current-label observation if no other interval covers it, including when only the writable suffix is materialized.
Preserve historical membership/assignments.
[Stock merger ownership](../libs/alpaca_end_of_day_security_correlation/stock_merger_occupation.py) and
[bounded current source](../libs/alpaca_end_of_day_security_correlation/closed_departure.py)

Even shared UUIDs need bounded historical-label evidence from an own source plus informative exact matching suffix,
independent-history proof or accepted bounded round trip. Exact matches can establish row ownership without
certifying UUID membership. Unmatched spans stay unresolved. Never extend proof into an unobserved earlier prefix.
[Name changes](../libs/alpaca_end_of_day_security_correlation/name_change.py) and
[source continuity](../libs/alpaca_end_of_day_security_correlation/source_continuity.py)

A destination beginning inside an independently owned predecessor interval must cover one contiguous suffix through
its end. Only a shared-UUID rename can use its informative exact raw suffix. Begin historical-label certification on
the destination’s first observed session. Distinct-UUID joins require the complete predecessor grid.

Earlier prolonged padding proves neither trading absence nor ownership. It permits the bounded suffix only if all
conditions hold:

- One complete aligned prolonged padding run follows a trade and ends immediately before destination history begins.
  All rows stay flat and inactive. Adjusted padding values may change only on an accepted same-symbol split date
  within the separator.
- Rename preserves one nonempty CUSIP. The complete action query and unique active non-OTC current-symbol/CUSIP
  lookups identify one UUID.
- Destination prefix exactly matches the complete raw predecessor suffix through the session immediately preceding
  the retained boundary. Both overlap endpoints trade informatively.

This dates only observed source rows under the established predecessor label. It neither establishes early
destination tenure nor consumes padding, moves the action, joins UUIDs or extends unseen history. A complete later
stock merger avoids veto only if the unchanged-CUSIP successor is continuing acquirer, effective and process dates
exist, all recorded dates strictly follow the rename, and acquiree label and CUSIP are distinct. It adds no acquiree
history or identity membership. Ticker reuse, unrelated occupants on/before boundary, other or incomplete structural
actions, incomplete grids, detached padding, gaps or raw differences veto.

A complete one-session predecessor may use that certificate only for a unique direct shared-UUID rename preserving a
nonempty CUSIP. The session must immediately precede the provider boundary and the destination must contain the
exact same positive-volume, positive-trade raw transaction. All eight views must be complete and aligned. Adjusted
values remain numerical vintages. Certify only that session. Different UUID/CUSIP, additional structural edges,
nonadjacent dates, incomplete sources, inert/divergent raw rows and noninformative multiday histories remain
unresolved.

A processing date may follow the observed handoff. Date only the bounded early successor span when a complete action
query and unique direct shared-UUID/unchanged-CUSIP edge have exactly one predecessor and one successor interval.
Require complete predecessor/successor sources on adjacent sessions, an independently resolved current-CUSIP source
exactly concatenating both complete raw histories, complete aligned views and informative early raw span without
prolonged carry. Retain the action date and UUIDs. No unseen extension. Missing/ambiguous current owners, related
structural actions, ticker recurrence, gaps, overlap, incomplete sources and divergent raw rows veto.

For a middle ticker between two renames, trading can precede the first action’s sole processing date. If both
renames preserve UUID/CUSIP, later departure identifies Alpaca’s combined-history terminal ticker. Compare
predecessor’s last traded and middle ticker’s first traded sessions through processing date with the terminal
source. Adjacent sessions, matching raw observations and no competing occupant support the middle label on observed
dates. Conflicting owners/observations preserve ordinary assessment and the action retains its processing date.

A processing-date-only rename can also use a complete predecessor request ending immediately before successor
history begins. Require one direct unchanged-UUID/CUSIP route, agreeing active CUSIP and successor lookups
identifying the security, complete coverage of early successor sessions in eight views and traded raw rows. Date
only observed sessions and preserve processing date. A competing occupant, structural action or explicit earlier
boundary retains ordinary assessment.

An exact one-for-one main component of a dated unit separation can establish bounded arrival from its own trades
before a later processing-only rename. Keep provider dates and other component assignments. A second applicable
rename conflict or intervening route prevents ownership based on this observation alone.

A component separation can independently date that bounded early successor span only if it is the unique positive
unit split naming the predecessor as an exact output on the adjacent source-handoff session. Require agreeing dated
occupancy and component-CUSIP endpoint when identified. Undated symbol lookups confer no historical occupancy.
Direct rename preserves UUID/CUSIP. Sources must be complete and adjacent. Early successor span must be the exact
informative session prefix before retained rename date. An older predecessor with prolonged padding requires its
final five observations actively traded and nonconstant. Later exit must be a linear same-identity rename or one
exact positive-rate cash merger with agreeing successor symbol/CUSIP, interval end and next-session boundary. Date
observed component rows only. No rewritten action, issuer-wide boundary, UUID join, missing endpoints, third dated
occupant, gap, inert endpoints, competing events or structurally ambiguous evidence.

If successor history retains the predecessor’s complete history, the same producer may date internal sessions only
after the unique component event and before the unchanged-UUID/CUSIP rename. Require all fields equal across eight
inherited prefixes through the traded event day, complete calendar, one exact zero-activity predecessor carry
internally and trading on every successor row. Predecessor reuse is noncompeting only through an accepted strictly
later action-anchored interval. Missing endpoints, prefix differences, overlapping/undated occupants, positive
predecessor activity, inert successor rows, incomplete views or intervening structural actions veto. Preserve action
date and membership.

A predecessor-labelled suffix after that endpoint may be terminal nontrading carry only if a complete action query
yields one exact stock-merger acquiree with distinct inactive, nontradable, non-OTC UUID and exact
current-symbol/CUSIP lookups. Ordinary-share rename must preserve UUID/CUSIP. Every complete aligned adjustment view
must start its suffix at the boundary and repeat its own last supported close with zero volume, trades and VWAP.
Independently owned successor must trade informatively every suffix session. Record retired UUID as sole candidate
for the uncorrelated suffix so serving scopes unresolved rows to affected identity. Preserve rows unassigned, UUIDs,
action dates and serving policy. Recurrence, competing occupants/actions, active/OTC acquirees, gaps/incomplete
views, divergent/positive tail activity or absent/uninformative successor trading retain anonymous unresolved
conflict.

One exact same-symbol, distinct-UUID provider substitution may certify a handoff without price comparison. Direct
form requires exactly one related action/candidate and unbounded predecessor interval. Bounded form allows exactly
one strictly earlier incoming different-label rename preserving predecessor CUSIP/UUID and supplying its lower
bound. Pre-entry rows remain unresolved. Both require complete action coverage of the handoff, unique exact CUSIP
lookups for inactive/nontradable predecessor and active/tradable successor, unique current-symbol lookup identifying
successor, one shared non-OTC exchange, exactly two known occupants and exactly two same-label intervals meeting at
handoff. Missing, duplicate, additional, recurrent or conflicting evidence vetoes. Continuity publishes one accepted
action ID. UUID membership and row chronology reuse its certificate without endpoint reauthentication. Ticker-reuse
guards still apply.

Disregard an earlier stock acquisition as a conflicting action here only if the predecessor is its exact continuing
acquirer on the same CUSIP, the target has a distinct nonempty label and CUSIP outside both endpoints, both ratios
are positive finite, effective and process dates exist and all recorded dates strictly precede handoff within any
bounded predecessor interval. Target history never joins acquirer. Incomplete evidence, acquiree roles,
same-day/later acquisitions and other structural actions veto.

Complete action coverage applies only within recorded dates to the original query label and each exact label in
completed `queried_symbols`. Partial queries and unqueried labels supply no coverage. Certificates consume that
guarantee without refetching actions.

Unresolved positive occupancy conflicts only within the asserted label/date scope for same-symbol, component-history
and current-source bridge certificates. A validated unit-component anchor admits only its exact supported unresolved
evidence. Unrelated/out-of-span uncertainty preserves supported history. Current-source bridge also considers dated
raw structural actions, including nested recipients and incomplete actions without UUID candidates.
Chronology/membership consume one producer certificate without repeating its checks.

## Membership and metadata continuation

[DependencyGraph](../libs/alpaca_lineage_build/selection.py) preserves published membership until retirement.
Disappearing evidence links select separate replacement-component correlation and one journal for all replacements.
Consumers neither repair groups nor infer replacements. Missing indexed membership requires explicit
`lineage migrate-index`, never a receipt crawl.

Completed scheduling hints are authoritative. Lost coverage permits one explicit source-generation census with no
receipt reads for matching sources. [Pipeline planning](../libs/alpaca_lineage_build/pipeline.py) uses completed
pricing results, including accepted ranges/effective pending changes for covered requests, and accepted index
ranges. Covered acquisition uses its source boundary without reopening endpoint validators. After interruption, read
only selected pending source receipts for missing planning facts in bounded cohorts. Publication reselects under its
own locks.

Prefetch opens selected receipts for evidence queries. [EvidenceClient](../libs/alpaca_lineage_build/metadata.py)
reuses containing completed action intervals within an invocation. For rule-only retained-mapping repairs also reuse
prior completed action coverage and symbol observations. Acquire only changed mappings or uncovered
labels/intervals. Failed/incomplete responses establish no coverage. Ordinary wider requests/new observation work
retain their acquisition paths. Conflicting action IDs block only evidence containing them, cached intervals
included, for the entire invocation without choosing a winner. Unrelated fully acquired/routed batch results remain
usable. Transport/routing failures leave affected requests incomplete.

Plan maximal intervals per symbol. Nested requests share their containing response only after pagination/routing
complete. Never infer extra coverage from overlap or gaps.

Ordinary action continuation starts at the prior active month. Older action corrections need explicit refresh.
Same-span price corrections, however old, cannot select historical actions/CUSIPs. For a backward extension acquire
the uncovered action prefix through the day before accepted query start, plus the ordinary current window. Reuse
accepted middle and unchanged mappings. Missing retained coverage needs explicit metadata refresh.

Workflow refresh first classifies accepted endpoints. A new exchange may accompany an unknown rename/reorganization.
Changed identity, class, exchange or explicit historical refresh selects the owning action discovery window.
Resulting evidence governs pricing reuse. Fresh prefetched actions outrank retained reports for the same dates, even
at unchanged cutoff. Their completed current window can discover dependencies without reopening older actions.

Retain unchanged actions’ completed CUSIP→ID lookups, including negative results. Current catalogue facts may join
an accepted UUID. Name/status changes or catalogue absence do not invalidate historical mapping. Never infer CUSIP
from ticker. New/changed action, changed current endpoint UUID or explicit refresh requires exact acquisition.
Replacing a consumed lookup during traversal restarts assembly from the authoritative exact cache. Known
dependencies keep continuation/refresh obligations. New dependencies need full evidence.

Expanded groups reselect under sorted enlarged locks. Cache decoded partitions by store, boundary kind and
generation. Envelope-only publication retains decoded partitions. Invalidate only changed entries. Selected
assignment bodies survive unchanged generations. Exact and catalogue endpoints have equal invalidation authority,
including canonical endpoints from retained CUSIP lookups. A late observation, including a later symbol changing an
earlier retained dependency during traversal, refreshes affected assembled reports from the operation’s in-memory
acquisition cache before publication.

Dependency baselines use the mapping’s explicit symbol observation, falling back to canonical CUSIP UUID only if
absent. A fresh exact mapping remains authoritative even for an older occupant. Metadata records the endpoint
observations used, preventing repeat lookups for unchanged dependencies. Selected pre-77 receipts reconcile these
dependencies once before new publication, including stale published mappings. Rebuilt reports restore accepted
baselines before checking coherence. Historical mappings differing from current occupants remain usable until
changed endpoint/action facts invalidate them.

## Assignment continuation

[Continuation selection](../libs/alpaca_lineage_build/resume.py) decides whether inputs permit reuse. It does not
map rules, predict assignments or execute another algorithm. Completed pricing generations/ancestry establish row
equality. Remaining inputs are accepted request (mapping `asof`, range, feed, timeframe), complete evidence,
membership and policy. Evidence equality excludes only acquisition counters, catalogue observation wall time, lookup
transport provenance and metadata-owned mapping dependency baselines. Metadata invalidates lookups against these
baselines before handoff. Changed lookup results and new evidence fields, including dispositions, automatically
participate. Baseline bookkeeping alone cannot reopen lineage.

Unchanged accepted inputs yield `lineage_required=false` without price payload reads or rule execution, including
linked groups, ambiguity and empty histories. Reuse producer-proven unchanged observations when only coverage and
normal current-label mapping cutoff advance. Pinned mappings must match. Changed metadata selects reconciliation.
Within accepted continuation, action-query bounds and diagnostic row counts alone are bookkeeping. HTTP-window
`start` describes transport. `history_start` supplies retained start. Begin append-grid checks at the previous
receipt’s `processed_through`, including accepted empty coverage. Genuinely missing new sessions select owning work.
Old accepted empty intervals cannot reopen earlier choices.

UUID membership retains each source’s request scope. Connected peers may have distinct completed public components
containing the same UUID. Preserve their historical owners and memberships. Only unambiguous peer membership can
fill a missing local entry. Within a source, accepted contribution dates distinguish different IDs sharing a UUID.
Frozen rows and closed-source metadata carry their IDs directly. Newly evaluated rows consult dated membership only
after UUID resolution. Final component can continue on its own observed ticker route through dated departure. Other
dates use ordinary row evidence. Empty suffix skips membership projection. Required projection with overlapping
accepted ownership dates or an omitted competing ID selects technical recovery.

During peer-selected complete-group work, applicable sources carry positive dated decisions in full-source
positions. New/unresolved rows enter the cascade. Numerical states remain separate from ownership. Zero new rows
across all sources in a dated market episode preserve its accepted public binding despite empty suffix reports. This
creates no row template/allocation. New unresolved rows and conflicting memberships need owning resolution.

Read legacy envelopes/publication bodies as completed provenance. Recognized transport spellings cannot authorize
fresh fact applications or repeat old proofs. Unknown/conflicting required handoffs select owning recovery. Within
an operation recovery consumes evidence-client-held accepted catalogue records. The
[workflow database](technical-contracts.md#universe-observation-and-work-state) records their acceptance reference.
The post-correlation owner can finish under normal writer/pricing locks from held source inputs, reports and
retained-choice scope. Applicability checks repeat no acquisition, materialization or correlation.

Selected retained-input recovery can carry completed action coverage under actual query bounds if every admitted
market date is covered. Later admitted dates and explicit refresh cannot inherit it. New metadata projections use
retained provider observations only. Legacy research cannot extend scope or establish new continuity.

Any compatible complete group may retain its prefix with unchanged pricing ancestry. Size, cross-label membership,
structural events and initially empty history alone cannot show retained prices or assignments changed. Require
matching effective source context allowing advancing observation/mapping cutoffs. Fresh mappings, actions,
dispositions and coverage quality participate. Changed name/status/tradability on unchanged UUID, symbol, class and
exchange affects new-session projection only. Historical equality excludes action-query bounds and diagnostic
maximum row count.

A listed single-source, single-UUID security with fully resolved rows can retain them across an exchange transfer if
other historical facts agree and actions comprise only cash dividends or forward/reverse splits. Competing owner,
structural event or OTC endpoint preserves ordinary ownership review. Metadata refresh applies the same
classification before selecting historical pricing. A description refresh alone cannot require historical ownership
recalculation.

Validate new grids and accepted seam, then use the same compiled evaluator on only added rows from every group
member’s active pricing partition. Empty accepted prefixes have no seam. Pricing validates their first rows. Empty
append with applicable ownership/numerical decisions skips both evaluations. Newly supported membership or
retired-label identity selects only affected assignments/newly joined numerical contests. Uncompleted dated-label
collision may select metadata-only work. Rollover or legacy missing summary can require retained assignment bodies
for publication. Previously unresolved/incomplete partitions remain conditional on later group facts. New group rows
select complete-group work until an owning producer certifies a narrower affected interval. Resolved completed
assignments remain closed.

A latest-session pricing correction certificate retains prior resolved assignments if mapping/ownership evidence
still applies. Evaluate the corrected row plus appends in all eight views. Unchanged cutoff means only the
correction and no append calendar calculation. Assignment summaries and affected boundary metadata supply the prefix
without authenticating older prices. Carry correction scope/ancestry through prefetch and execution. An earlier
notice for sparse pre-current-month correction alone cannot reacquire historical actions.
[Correction projection](../libs/alpaca_lineage_build/correction_prefix.py) and
[publication/serving contract](technical-contracts.md#pricing-acquisition-and-continuation)

Recovery and discovery share one accepted catalogue and evidence client per update. Recovery acquires the catalogue
once when it has selected work. Discovery consumes that observation, exact positive/negative lookups, completed
action intervals, failures and conflict latches without reacquisition or renormalization on phase change. Optional
lookup prefetch lends its catalogue response to full discovery, whose stronger schema must still succeed without a
second request. New invocations get fresh observations. Coverage cannot extend to unqueried action dates.

If newly acquired metadata invalidates the prefix after decoding, full-history fallback under the same
generation/locks loads only missing partitions. Single-source observation selection skips multi-source
ownership/ranking preparation and retains legacy activity validation.

Changed mapping/action facts, incompatible context/policy, corrections beyond certified latest session, missing
equality proof or new rows that can reopen unresolved/incomplete decisions select complete-group work **before**
evaluation, without trying alternate rules. Query-label bookkeeping alone preserves the prefix. A strictly later
suffix with completed disjoint assignment bounds preserves resolved and unresolved history. Overlap, a previously
observed future action reached by the suffix, missing bounds or incomplete partitions select owning reconciliation.
The existing evaluator retains normal evidence reconciliation/ownership fallback without separate continuation
executor or mandatory context preparation.

Loader requests declare `complete_history`, `retained_suffix` or `active_partition` for each in-memory input.
Retained counts cannot imply scope. Once metadata/append invalidation selects complete-group work, materialize each
priced member’s complete history at its selected generation, retaining decoded partitions and loading only missing
data. Reject mixed complete/suffix inputs before correlation/publication. Prefix reports/certificates apply only to
added-session work, never to resuming one member of a new complete-group decision. Separately certified closed
sources retain their explicit authority.

Both paths publish selector last, reuse unchanged output references and share pricing handoff/journal recovery
contracts. Only opt-in `--debug` traces lineage requirement, retained-history selection/count and scope (`none`,
`added_sessions`, `complete_group`). Before ordinary correlation it also records each selected input’s
generation/materialization scope from held values.

## Missing auxiliary prices

An exactly rejected missing auxiliary may retain metadata and complete alongside independently supported group
histories without inventing an empty source. Exclude arbitrary failures, seeds and retained series.
[Acquisition](../libs/alpaca_lineage_build/acquisition.py) records rejected request, evidence and obligation before
acknowledgement. Auxiliary-only reselection preserves the group.
