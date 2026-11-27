# Technical contracts

Find the authorizing artifact and its invalidation condition.
[Cross-producer example](ARCHITECTURE.md#producer-handoffs)

| Change | Contract |
|---|---|
| Files, versions, stale generations | [Standalone artifacts](#standalone-artifacts), [trust and ownership](#trust-and-ownership) |
| Requests, retained-price comparisons | [API plan](#api-plan), [pricing acquisition](#pricing-acquisition-and-continuation), [check summary](#check-summary) |
| Discovery, retries, scheduling | [Universe observation and work state](#universe-observation-and-work-state) |
| Targeted repair, interrupted write | [Retained lineage recovery](#targeted-retained-lineage-recovery), [numerical-only completion](#numerical-only-completion), [publication and recovery](#publication-and-recovery) |
| Eligibility, metadata, numerical history reuse | [Serving catalogue](#serving-catalogue), [checkpoint reuse](#checkpoint-reuse) |

## Trust and ownership

Accept each completed selector after its required schema/version/token checks and consume only its selected
work. On lock reacquisition check generation only. No existence/symlink inventory, stat cache key, payload hash
or upstream replay. Producers validate new outputs/destinations and compare recorded digests. Explicit
authentication audits existing data. Readers fail closed without repair.
[Storage trust rules](../AGENTS.md#trust-before-touching-storage)

[Ownership allocation](lineage.md#ownership-allocation-criterion) weighs missing historical evidence against
plausible owners and accepted ownership. Missing technical publication metadata requires owning recovery and
carries no evidence against an owner.

Fresh inference uses provider observations and applicable completed decisions. The
[evidence boundary](../libs/alpaca_security_lineage/inference_evidence.py) rejects authored assertions.
Consumers use its completed `ProviderEvidence` without revalidation or reprojection. Selected legacy receipts
may supply raw provider observations. Research provenance stays frozen. Removing fact inputs preserves accepted
numerical decisions and frozen-journal installation authority.
[Evidence and pending obligations](lineage.md#sourced-event-facts)

Per-invocation [OperationContext](../libs/alpaca_lineage_build/context.py) holds coherent, accepted
compatibility/index/summary views. Revisions and publisher-returned changes refresh them. No persistent cache or
alternative authority. Changed inputs invalidate only dependent work.

Lineage computation revision 2 rebuilds earlier derived decisions from complete retained inputs. Receipt/index
revisions govern only after publication. Pricing and metadata acceptance remain independent. Explicit rebuild
selects one seed per complete group, preserving individual request bounds and auxiliary mappings. Retry skips
revision-current groups before body reads. Missing evidence or a newly required correction blocks. Serving
preserves the numerical prefix through the last accepted owned source position and checks only the suffix newly
received from a repaired spurious identity.

### Targeted retained lineage recovery

`lineage recover` accepts v1 `alpaca_retained_lineage_recovery`: absolute `store`, `correlation_policy_sha256`,
`groups`. Each group names sorted complete `members`, and `pricing_generations`/`receipt_generations` for every
priced member. Name absent auxiliaries without fabricating generations. Each accepted receipt binds mappings,
actions and other evidence. A completed comparison supplies selection. Never repeat it, rediscover the failed
universe or admit unresolved semantic review.

Plan checks only selected membership/generations in the compact lineage index. Execute finishes the owning
journal before authorizing policy work, accepts selection once, and checks selected pricing generations under
group locks. Stale bindings fail before price reads. Ordinary producer/journal evaluates complete histories for
selected current-revision decisions. No metadata acquisition, price comparison, payload authentication or
pricing writes. Newly required pricing correction blocks.

Optional v1 `retained_recovery` in each selected receipt/index projection binds `operation_id` to store, policy,
members and exact input receipt/pricing generations. It certifies only this recovery. Continuity and unchanged
numerical ownership require their own evidence. Retry skips completed groups before receipts, payloads or
calculation. Later evaluations replace the envelope. Changed generations/membership cannot inherit it. Peer
failures preserve independent completions. Interrupted journals retain finished calculations. Selected serving
maintenance follows successful lineage. Its ownership/numerical gates still govern availability.

`restore_accepted_owner` selects one complete group with a current accepted serving ownership checkpoint. Read
only its lineage index selection and indexed catalogue membership. Subsequent lock scopes check selector tokens.
Changed components cannot inherit narrower selection. Independent group selectors and price histories stay
closed.

- **Single-source cold prefix.** The checkpoint must approve the same identity/label in all eight views and
  pricing ancestry preserve the positions. Restore only the candidate-free contiguous cold prefix named by the
  ownership-loss decision. A different owner, competing source or changed ancestry leaves it unresolved. Journal
  the changed assignment without replaying pricing or ownership. Serving carries the prefix and checks added
  sessions.
- **Successive ticker changes, same UUID/CUSIP.** The previous owner and middle ticker may survive a
  processing-date discrepancy in the first action. Current native action/source bounds identify disputed
  positions. Prior checkpoint must select the same pricing generations. Restoring candidate-free middle
  positions must reproduce the entire prior owner, source-row and label selection across all eight views. Its
  numerical/calendar decision then covers the whole restored grid. Journal the assignment to serving under full
  group selection.
- **Single-source suffix.** Conflicting native cash actions and a later departure of the original CUSIP may
  permit restoration under the [cash-distribution rule](lineage.md#cash-distributions-and-terminal-ownership).
  Use dated activity from the selected generation's completed comparison tail, lent by the applicable summary
  or, for an omitted source, its selected pricing validator. Every lost position must qualify and all eight
  prior source/owner selections must apply. Current/legacy checkpoints provide the same accepted contributor
  membership. Restore matching positions across affected partitions, including overlapping year/month
  representations. Serving reuses the original grid's numerical/calendar decision.

The same entrypoint accepts v1 `alpaca_retained_candidate_handoff` after semantic review. Bind absolute store,
complete members, original receipt/pricing generations, production policy and computation revision. Supply
baseline and completed candidate receipts, assignment boundaries and producer-built projections. `review`
requires v1, `approved` status and nonempty `conclusion`. The saved comparison must certify completed candidate
assignments and no required mapping correction. Unresolved review prevents publication. Consume these guarantees
without repeating comparison, numerical selection, authentication or assignment construction.

Check current compact receipt/membership binding and select pricing generations once under normal locks. Finish
only group/workflow bookkeeping and the existing completion envelope. Send completed artifacts directly to the
ordinary journal. Serving receives journal output and accepted choice capabilities without receipt/projection
rereads or schema recalculation. The publisher carries selected pricing source and computed generation. On
pricing-lock reacquisition compare only selecting-file identity. Reuse an unchanged selector's source without
reread/hash. Changed selectors require fresh selection. Pending publication/update gates remain active. This
handoff expires with its operation. Changed generation, effective mapping, membership or policy rejects the
candidate. Missing prepared artifacts require explicit owning preparation. Serving validates new numerical
contributions. Export consumes its completed catalogue.

Unchanged candidate boundaries retain accepted references and already-read bodies. Optional
`selected_observation_bundles` in a prepared numerical revision carries all revised dates in v1, exactly revised
dates outside `numeric_reuse_sessions` in v2. Per view, bind the shared physical part to date, winner and
revision. Consume retained values before assembling separately requested missing dates. Full equality can carry
the dated decision without a values payload.

[Research admission](../libs/alpaca_lineage_build/research_admission.py) can prepare a completed generic result
only under exact review, store, complete-group, policy and receipt/pricing bindings. Reject active candidate
assertions and new assertion-bearing outputs. Require separately selected baseline bodies. Candidates cannot
manufacture baselines or omitted decisions. Consume the approved scope once. Ordinary prepared publisher/journal
installs outputs without redoing ownership, numerical selection or assignments. Original research reports retain
provenance only. Missing required outputs or changed bindings stop admission. A generic runtime change cannot
admit factual candidates.

Legacy application-inventory markers retain publication provenance. Fresh application preparation/publication
and metadata acknowledgement reject before I/O. Frozen journals install completed metadata directly under
recorded provenance, bypassing retired functions. Legacy fact obligations lacking applicable completion require
owning generic recovery. Peer markers cannot provide it. Matching publication stamps apply only to accepted
obligations/coverage. Pricing, history checks and corrections retain independent completion.

[Correction binding](../libs/alpaca_lineage_build/correction_binding.py) may carry completed owners and
numerical choices through latest-session count correction. Inputs: durable comparison selection, published
correction, producer-held corrected rows, generation-bound current-month assignments, complete component's
accepted choices. Require positive volume/trade-count revisions with identical timestamps, OHLC and VWAP,
still-applicable recorded as-of UUID or terminal-continuity predicates, and no same-identity competitor that
date. Missing handoffs, changed mapping/coverage, unsupported rules or value-bound numerical capabilities select
owning work. Preparation performs no storage I/O or ownership/numerical replay. Preserve assignment bodies,
retarget only pricing bindings, check named generations under group locks at publication. Changed values are new
serving inputs on the corrected date.

### Numerical-only completion

`lineage recover --selection` accepts v1 `alpaca_lineage_numerical_recovery`, mode `numerical_only`. Bind
absolute store, authorized `correlation_policy_sha256`, complete groups' sorted `members`,
`receipt_generations`, `pricing_generations`. Each group's `scopes` maps sources to boundary-local `adjustment`,
`boundary`, `public_id`, `row_start`, `row_end`. Positions are one-based and inclusive. Counterpart scope must
cover all eight views. Name empty peers without inventing observations. Selection authorizes numerical work
only.

The [owner](../libs/alpaca_lineage_build/numerical_completion.py) applies the shared numerical selector once to
accepted assignments and prices at selected positions. Preserve owners, membership, dated labels and all choices
outside scope. Inside a diagnosed conflict, legacy `selected` flags cannot preserve the conflict. Researched
`observation_authority` stays inspectable but outside the contest. Accepted owners/labels/full-source routes
remain completed inputs. Serving ownership and coverage counts cannot establish unknown capabilities. Missing
counterpart views, ambiguous authority or required mapping correction block without replacement publication or
price acquisition.

Include every retained potential competitor for the opened identity/dates. Before prices, compact accepted row
extents and choice runs can exclude empty/disjoint omitted branches. Issuer chronology alone cannot prove
absence. Unknown omitted-peer/branch overlap rejects without assignment-body reads. Selected assignment
endpoints can reject known omitted competitors. Before numerical selection, close included partial segments'
remaining positions against opened dates using only selected and already-required adjacent cut timestamps. This
admits a competing prefix and preserves its later suffix. Uncertain omitted overlap can reject even when wider
containing bounds overstate selected dates. Never widen scope or audit outside history.

Open only selected sources/assignment branches. Accepted receipt summaries and full-source observation routes
supply remaining membership/authority metadata. Missing required route selects owning metadata preparation.
Clipped rows cannot rank it. Empty peers stay closed. Applicable completed source-equivalence rejects before
numerical payload reads. Direct carry owns its winner/equality. Metadata publication retains original donor
receipt tokens. Pricing ancestry can prove preservation through the decision date. Missing ancestry or uncertain
interior overlap cannot prove invalidation. Reject and defer to the carry owner. Proven changes affecting that
date can reopen its contest. Carry disjoint equivalences forward.

Retain the completed choice, exact equality, selected eight-view bundles and constructed publication while
bindings apply. Publish through the journal and bounded numerical-source revision. Complete equality may retain
serving approval. Check differing new values only on their dates. V2 `labels_unchanged: true` requires
producer-established equality of the selected label union with the accepted dated-label projection. Serving
retains checkpoint dates/ticker ranges without reconstruction. V1 retains its relabelling semantics. Pricing,
lineage format and serving-eligibility policy versions stay unchanged.

Previously served IDs require applicable accepted checkpoints and dated labels. With only a conflicting daily
history decision and no accepted checkpoint, first serving validation can consume a completed choice between
complete, exactly equal eight-view bundles. Ordinary serving then evaluates the history. Partial competing
bundles or any different values require owning preparation.

Exact-selection completion publishes through the owner's journal. Unchanged retry consumes it before
receipt/payload reads. Interrupted installation uses frozen outputs without repeating selection or construction.
Receipt, pricing, mapping, membership, policy or scope changes invalidate affected work only. Ownerless
assessments are independently complete and nonblocking. Missing/inconsistent technical publication capability
supplies no ownership evidence.

Daily update drains numerical-only selections explicitly enrolled in work state before ordinary pricing/lineage.
Direct history/lineage entrypoints drain matching work before unchanged-input continuation. A diagnostic
selection defines scope and may require source reads. Ordinary execution neither discovers legacy conflicts nor
selects every absent optional handoff. Applicable completed work remains accepted.

## Standalone artifacts

Paths are relative to the standalone store:

| Artifact | Authority |
|---|---|
| `store.manifest.json` | Store format. |
| `end_of_day/pricing/<TICKER>/source.manifest.json` | Pricing generation, boundaries, payload map. |
| Adjacent `validator_first_last_sessions.json` | Source-bound exact first/last rows for every view. |
| Adjacent `correction.complete.json` | Atomic equal-price correction completion, bound to the exact correction requirement ID and unchanged source generation. Read only for selected correction recovery. |
| Adjacent `correction.observations.json` | Owning recovery of an unaccepted replacement's changed `all` witness. Records the old and newer exact observations under the source generation, correction ID and request binding. |
| Pricing `completed_years`, `current_year`, `current_month` | Selected CSV/gzip boundaries. |
| `end_of_day/manifests/<TICKER>/update.json` | Unconsumed inclusive changed range: `from`, `to`. |
| `end_of_day/lineage/<TICKER>/lineage.manifest.json` | Evidence, input generations, chronological assignments. |
| `end_of_day/lineage/index.sqlite3` | Group/generation selection, complete membership, lookup projection, serving obligations. |
| `end_of_day/catalogue/serving_catalogue.sqlite3` | Serving selector, public fields, decisions, previous contributions. |
| `universe_updates.sqlite3` | Latest catalogue observation ID and time, registration, work obligations and outcomes. |
| `pricing_check_summary.sqlite3` | Live-check baseline. |
| `lineage_update_check.sqlite3`, `download_completion.json` | Disposable scheduling hints, not data authority. |

Effective history = completed years + current year excluding active month + authoritative current month.
Rollover consolidates. Ordinary month updates preserve cold payloads. Eight logical views can share exact-copy
physical files. Columns: `timestamp,open,high,low,close,volume,trade_count,vwap`. Convert provider JSON numbers
directly to exact strings. Reject wrong symbols, malformed values, duplicate/out-of-range timestamps and
incomplete pagination. Structural validity establishes neither market validity nor identity.
[Pricing contract](../libs/alpaca_pricing_download/contract.py),
[storage](../libs/alpaca_pricing_download/storage.py)

Pricing format 32 publishes the comparison tail. Format 31 remains readable in place and takes the selected
metadata upgrade below for a missing tail. Supported sources may select older unchanged boundary receipts.
Compatibility requires no price rewrite/relabel. Unknown future formats fail closed. Runtime fixes alone
preserve supported lineage formats, applicable accepted policy identity and completed decisions.
[Version compatibility](../VERSION_TRACKER.md#compatibility)

Lineage receipt ingress accepts complete envelopes and references once without proof replay. First selected
ingress retains the accepted body under its actual index generation for the operation. Later readers use that
schema acceptance without reopening it. Missing/incompatible required handoffs select owning recovery.

| Lineage format | Required fields and scope |
|---|---|
| 35 | `current_source_ownership` is null or a completed bounded v1 certificate. The receipt binds pricing, effective request context and accepted group evidence. Compatible continuation, including a peer's unresolved-history fallback, carries it in memory without repeating source-grid, chronology or activity proof. No extra storage artifact/acquisition. [Eligibility and invalidation](lineage.md#ownership-evidence-limits) |
| 36 additions | `sourced_rename_membership` is null or a completed v1 group result with certificates/issues, including empty results. `sourced_rename_ownership` is null or a v1 source scope with symbol, observed first/last dates and membership certificate's `action_id`. Metadata applicability governs membership independently of price ancestry. Source applicability governs row ownership. Legacy completions retain their accepted scope. They neither supply fresh assertions nor extend certificates to new rows. [Sourced events](lineage.md#sourced-event-facts) |
| 37 additions | `public_common_continuation_membership` and mapped-source `public_common_continuation_ownership` are independent v1 handoffs. `security_relations` completes a non-union projection with nullable provider/public endpoint IDs. `membership_only_rebind` is null or binds exact old receipt, pricing, assignment and unchanged numerical source/view/row/date selection to an accepted public ID. V1 preserves labels. V3 records separate `old_labels` and current `labels` for primary dated metadata refinement, including unchanged ID. Changed numerical selection cannot claim unchanged-run approval. |

Approved appends retain original donor generations until serving consumes them. Rollover projects donor
positions from accepted assignment metadata into current partitions. Donor-intersecting correction or changed
selection requires owning transfer recovery, never implicit historical validation.

Closed-source format 37 carries both earlier ownership fields, null where legacy asserted no certificate, and
`closed_source_handoff_version: 1`. Ordinary recovery selects only pending serving groups whose compact index
records the exact lineage-37/correlation-82 shape. Use indexed membership and optional
`source_ownership_envelopes_complete`/`closed_source_handoff_version` flags. Known complete groups require no
receipt reads. Missing older flags select only the bounded pending legacy group. No application registry or
universe scan. Reject other missing required envelopes, malformed certificates or evidence of missing newly
calculated source proof. Journal only missing nulls and lift marker, preserving assignments, membership,
pricing/donor references, summaries, notices and checks. Consumers stay strict. No historical reconstruction or
repeated continuity certification.

Optional compact `has_membership_rebind` identifies completed source rebind. `membership_transfers_complete`
identifies completed projection. Pending-group recovery selects exact metadata work from these flags. Legacy
repair requires incomplete positive transfer or unknown status. Explicitly absent peer transfer cannot reopen a
completed positive. Unknown status retains complete-group recovery. Changed donor generations reject stale
approvals. Missing selection capability requires explicit recovery, never a pending-receipt census. Dated
ownership, numerical choices and receipt format remain unchanged.

Generic source work closes dependencies before payload reads. `ClosedAcceptedSource` carries actual accepted
counts, generations and request context without a pricing reader. Completed membership work assembles selected
assignment projections. Changed row work and new numerical contests separately name source/date scope. Numerical
counterparts retain accepted owners. No-new-row updates may journal completed generic membership plans.

Format 37 may retain legacy `documented_identity_membership`, source routes and v1 `security_description` in
dated ownership intervals. Fixed-scope completed outputs supply owners, names, exchanges, classes, instrument
and registrant descriptions to serving. No fresh assertions, document/lookup replay, implicit historical audit
or reconstruction of research. Missing required capabilities require owning recovery.

- `dated_market_projection_version: 1` declares completed `dated_market_projection`, bound to prior receipt,
  pricing generation and boundary map. Accepted unchanged selection, identity episodes and dates let serving
  retain numerical outcomes and project labels from accepted sessions. Whole segments require no price reads. A
  selected interior cut may need exact stored timestamps. An unmarked legacy checkpoint may assemble only
  selected raw dates after binding. Missing declared dates require owning recovery. No fresh market assertions,
  numerical reselection or UUID union.
- `dated_label_completion` and `dated_label_plan` carry the generic collision producer's completed
  source/assignment bindings, held request scopes, provider actions and accepted labels. Unknown/incomplete
  declared completion requires recovery. The [dated-label owner](lineage.md#dated-label-collisions) uses the
  label-only serving contract, preserving owners and numerical selections. A split between native renames also
  binds selected-group evidence generations. Changed partitions and continuation routes journal together. A
  clean serving checkpoint bound to prior lineage/pricing generations may supply interior-cut session dates.
  Whole segments already have timestamps. Serving retains numerical validation and publishes revised labels.
- `numerical_source_revision_version: 1` or `2` binds prior receipt/pricing generations, exact changed sessions,
  selected sources, contributor membership and preserved session grid. Outside choices stay accepted.
  `numeric_reuse_sessions` carries complete-bundle equality. Serving checks only differing newly selected
  bundles and combines outcomes with accepted history. Interruptions use the frozen journal. Unfinished serving
  may assemble only declared dates. Missing scoped negative-outcome/date capability requires owning recovery,
  never full-history audit. Source/historical-label gates remain active. An unpublished completed market
  projection may supply labels/unchanged artifacts to a later adjustment refinement. Reselect only invalidated
  numerical scope. Compare final revisions with the actual published baseline. Returning to accepted source
  choices yields a label-only handoff without numerical checks. V2 supports unchanged labels. V1 retains its
  label projection. [Numerical-only completion](#numerical-only-completion)
- Ordinary source work publishes `numerical_source_equivalence_version: 1` and `numerical_source_equivalence`
  when original selection establishes an equal eight-view bundle and explicitly selects a retained donor. Retain
  original prior receipt/pricing generations, observed pricing generations, exact dates, selected/donor public
  IDs and sources. Ownership alone, unknown flags and partial bundles cannot approve numbers. Serving consumes
  the applicable donor's completed clean numerical outcome and dates without comparing prices or rechecking
  bars. Missing declared date/scoped negative-outcome capabilities require owning recovery. New contests retire
  only reopened dates. Disjoint decisions retain original bindings. Ingress freezes projection once for later
  consumers/metadata publications. Construction/publication interruption preserves the completed result. Select
  equality metadata only for the current ID's fresh serving dates. No unused legacy donor-date capability is
  required for an approved prefix.

Within the operation, serving retains completed input/prefix preparation, localized revision validation and
assembled histories under exact receipt/pricing generations, prior serving decisions, policy and rebuild scope.
Downstream retries consume completed stages. Changed bindings select owning work. Decoded immutable partitions
are lent by store, source, boundary kind and generation. Under lock, select current source and consume matching
held partitions before manifest/payload reads. Changed boundaries select their own assembly. Lost operation
memory permits unfinished selected assembly and preserves durable equality decisions.

Serving composes complete disjoint donors under the new ID, including negative/partial capabilities, without
rechecking old OHLC. A surviving complete sole-source numerical selection can retain its checkpoint through a
dated-label change. Raw dates cannot establish alignment for an `incompatible_sessions` donor. For a whole
legacy donor missing both date-projection fields, bind exact generations and exhaustive old/current
row-and-label transfer, then assemble timestamps only from selected donor raw positions. Validate/publish new
date projection, preserving accepted numerical outcomes and per-view coverage for subsequent consumers. Newly
recovered earlier history may extend the span, including alongside append, if exact old selections/labels remain
unchanged, for either unchanged ID or accepted membership rebind. Assemble only outside positions. Check their
bars, calendar intervals and new seams. Combine dates, labels and scoped outcomes with the accepted middle.
Interior change or changed donor binding requires bounded recovery. Missing declared current dates is an error.
A changed donor subrange lacking applicable numerical/date outcomes stays pending at its affected scope.
Preserve donor records and unrelated work. No arbitrary-run recovery command exists for this capability, and no
implicit whole-history rebuild.

New serving checkpoints borrow `history_validation.observation_choice_handoffs` v1, bound to original
receipt/pricing generations. Selected membership-transfer recovery consumes selected, superseded, conflicted and
incomplete states independently of ownership. A non-null owner cannot establish a choice. Missing/changed
capability selects retained-lineage reconciliation before payload access/publication. Explicit unknown requires
owning selection.

Retained/materialized sources produce membership transfers after final observation selection. Only actual
retained positions/labels advertise a donor. Changed ID without selected contribution emits no numerical
transfer. Selected ordinary-update recovery fixes earlier transfer omissions from completed serving checkpoints
without changing assignments or prices. Transfer v2 binds old selection to checkpoint decision generation and
original lineage/pricing generations without fabricating lost assignment-boundary maps. V1 retains original
boundary binding. Consumer gates match for both. Recovery restores accepted disjoint source selections before
constructing transfers. Only observation-selection flags and their accepted labels may change in selected
assignment metadata. Ownership, positions, dates and prices stay fixed. Journal selection projections/transfer
metadata without price invalidation.

Normal retained/materialized paths protect accepted selections before new contests. Suppressing a newly
supported duplicate needs both target common-source grid proof and donor complete-grid approval. Equal
counts/endpoints alone cannot establish date-grid equality. Unknown grids/uncovered observations retain ordinary
selected work. Recovery fails closed rather than discard them. Actual donor overlap or changed owner/mapping
still requires owning reconciliation. Carry selected receipts, assignments and checkpoints through the
operation. A compact marker skips completed recovery.

Append assembly consumes pricing's already-read changed-session notice. A month/year rollover weekend cannot
select an older partition without new rows. Correction/projection dates retain independent scopes, without
calendar reconstruction or historical price audit.

New lineage assignments declare required `assignment_handoff_kinds` and v1 `assignment_handoffs` in the
selecting receipt. Each binds boundary generation, canonical comparison facts, adjacent selected
`.projection.json` with shared equal-view segment lists and compact ownership runs. Historical segment bodies
stay outside metadata receipts. Missing/incomplete/mismatched required handoffs select owning lineage recovery,
never historical reconstruction. Unmarked legacy references remain readable through selected assignment bodies.
Ordinary work does not migrate unselected stores. Carry prior handoffs unchanged. Journal new handoffs only for
newly constructed boundaries. Full reconciliation compares new canonical outputs with accepted facts, preserving
identical cold references without opening old bodies. Current-year comparison excludes active-month overlap.
Completed-years comparison ignores only anchor label. Serving consumes shared projection and `serving_ownership`
runs without scanning old boundaries. Ownership rules stay unchanged.

New handoffs also declare `observation_choices_version: 1` and boundary-local `observation_choices`. Shared view
slots select runs with owner, state, labels, row bounds, dates and timestamps across the full boundary,
including current-year/active-month overlap omitted by serving ownership. Superseded/conflicted/incomplete keep
recorded owner. Missing legacy flags remain unknown. Unlabelled observations have an empty label set.
Source-local projections certify neither group coherence nor numerical selection from ownership. The producer
constructs the immutable runtime capability during existing assignment traversal. Ingress accepts each unique
serialized run's schema once and carries capability through caches, copies and recovery. Consumers check
declaration and selected generation only. Unmarked legacy handoffs stay readable without implicit capability.
Missing declared metadata selects owning receipt recovery. Recovery installs completed output without
reprojection.

Explicit `lineage migrate-assignments` builds missing projections from accepted bodies of selected complete
groups. Apply group limit and check compact policy before receipt/body assembly. Open each missing selected body
once. No pricing payload, provider request, rule replay or numerical reevaluation. Generation checks under
selected locks reject changed inputs. Journal staged projections and publish compact index coverage. Retry skips
migrated groups before receipt reads. Serving handoff preserves decisions. Run owning serving queue upgrade once
if publication needed. Projection-only publication/recovery preserves pricing summaries, notices and pending
checks. Completed peers retain compact entries without projection/digest recalculation. Already-projected legacy
receipts missing only the compact marker publish it once. Ordinary updates never invoke this migration.

[load_pricing](../libs/alpaca_lineage_build/pricing.py) supplies
[PublishedPricingEvidence](../libs/alpaca_pricing_download/evidence.py): generation, symbol, request scope and
exact eight-view counts including zero/retained rows when correlation reads only additions.
[classify_symbol_disposition](../libs/alpaca_security_lineage/symbol_disposition.py) checks handoff schema/scope
without payload/sidecar reads or repeated authentication. Invalid handoffs stay unresolved. Completed lineage
evidence remains authoritative. Selected evidence refresh produces the corrected projection.

Loader requests declare `complete_history`, `retained_suffix` or `active_partition`. Retained counts do not
imply materialization. Once ordinary lineage selects complete-group work, upgrade every partial member under its
held generation, share decoded partitions, and require `complete_history` before correlation/publication. Prefix
assignments/reports govern retained-suffix work only. Preserve the explicit `ClosedAcceptedSource` contract. No
new pricing acceptance, provider request or payload audit.

## Pricing acquisition and continuation

Ordinary downloads, retained dependencies and daily-summary members compare the first stored fully adjusted
(`all`) row and latest up to ten stored `all` rows. Request the first full New York session, capped at accepted
coverage, then one selected tail window. Request/compare overlapping dates once. A one-session history takes one
request. Compare timestamp, OHLC, volume, trade count and VWAP exactly under existing numerical equality.

| Completed comparison | Work |
|---|---|
| All match | Reuse selected generation, acquire missing sessions. |
| Only latest session differs | Correct latest session, acquire missing sessions. |
| Earlier selected session differs, including first | Replace full retained span. |
| Missing/incomplete observation | Preserve completed peers and resolve incomplete comparison. |

With one session, latest correction and full replacement cover the same row. After approval, never check the
other historical views, overlap or old first-session confirmations. Unselected revisions can go undetected.
Historical audit is separate. Replacement notices include removed sessions.
[Comparison](../libs/alpaca_pricing_download/endpoints.py),
[`download_pricing` / `publish_acquired`](../libs/alpaca_pricing_download/download.py),
[HTTP](../libs/alpaca_pricing_download/http.py),
[completed responses](../libs/alpaca_pricing_download/responses.py)

The validator owns the first row and ordered `tail_all` (ten rows or complete shorter history). Initial
acquisition, append, correction, replacement and rollover publish them with selected prices. The existing
journal owns summary completion. Live observations cannot replace the baseline before price publication. Pending
publication stays unreadable until owning recovery.

[Legacy transition](../libs/alpaca_pricing_download/history_tail.py) recognizes supported old publications
without a tail. Select manifest-referenced `all` payloads newest first. Use `readall_rsplit`, excluding header
and year rows superseded by current month. Open older year/completed-year payloads only to reach ten effective
rows. Journal this metadata-only assembly before provider work without reauthentication. Retry uses completed
tail. Missing/inconsistent tail in a completed new format requires owning recovery, never another gzip
reconstruction. Plan reports upgrades/bounded reads. Unchanged-price ancestry preserves prior work.

Local correction replaces one session across all eight views. Seed adaptive acquisition with the accepted live
`all` row and acquire only remaining policy-selected observations. Share assembly/publication with append in the
same partition. Rebuild its gzip without a full-history request. Preserve unaffected payload references. Record
cause, prior generation, preceding session, exact-timestamp membership preservation and ancestry beginning at
the corrected session.

When lineage materializes corrected partitions before append, lend decoded rows and read publication documents.
Append/later lineage reuse matching store, partition and boundary generation across source-envelope changes and
rollover. Only changed boundaries need assembly. Native controller records or explicit JSON adapter carry these
transient values without separate archive. Normal publication and recovery use the same handoff of completed
receipt/new assignment bodies to serving under exact selected generations.

Earlier correction completion alone cannot approve an advance beyond coverage. Compare as required for the newly
selected advance. Consume the earlier disposition only when new witnesses support it. Changed rows select their
new scope. Applies to a changed-price publication with correction ID and a separate equal-price completion
receipt. At original cutoff return the selected source's completed outcome. A fresh comparison selecting full
replacement for an advance records `advance_end_exclusive` on the correction requirement. If interrupted after
published replacement, acquire the remaining suffix directly only for the same correction ID, source ancestry,
retained interval, mapping and exact target. Different target/source takes ordinary comparison. Older contracts
authorize only recorded scope. Current-operation completion remains authoritative within its span.

**New-range adjustment acquisition.**
[`alpaca_adjustment_equivalence_v2`](../libs/alpaca_pricing_download/adjustment_policy.py) requests `raw`, then
`all` directly. Equal complete new payloads select all-equal after two views. Otherwise `dividend` then required
split/spin-off observations distinguish four-/five-view patterns. When `dividend` = `all`, direct `split`
completes the four-view branch even if different from `raw` or equal to `all`. Alias `spin-off` to `raw`,
`split,spin-off` to `split`, both dividend combination views to equal `dividend`/`all`. Direct values preserve
precision without tolerance/rounding. Empirical aliases neither explain economic cause nor exclude anomalies in
unrequested views. Unrecognized patterns fetch remaining views. Contradictory empty/nonempty responses require
all eight direct. View counts exclude pagination, retries and producer confirmations.

[Adaptive acquisition](../libs/alpaca_pricing_download/adaptive.py) applies to new tickers, replacements,
missing suffixes and local corrections seeded by accepted live `all`.

- Fresh/full replacement: one symbol per action at its exact range for each adaptive stage and each direct
  view's first-session confirmation. A mapped batch can return an entire shared episode under one label yet
  later sessions under another, producing a nonempty history with missing prefix. Independent completed symbols
  survive peer acquisition/confirmation failures.
- Missing suffix/local latest correction: batch identical ranges. Separate related labels in a physical request
  and fill available slots with unrelated symbols. Use held published membership or already-selected dependency
  group for transport only. With no known related companion, use the ordinary suffix roster.
- Finish pagination before each dependent decision. Exact transport fallback retains successful observations.
  Publication consumes completed decisions without rehash, renormalization or policy replay. Each new scope
  selects independently of old payloads/endpoint approvals. All eight logical views remain published. Optional
  `latest_adjustment_acquisition` records latest range/mapping, policy, observed hashes and inferred aliases.
  Exact-session corrections record bounded proofs together. Past policy provenance remains readable and
  preserves accepted pricing. Provenance neither authorizes later aliases nor reinterprets retained partitions.

Completed trigger observations confirm the same adjustment/session without another request. Before full
replacement publication compare new output against carried live `all` rows. Account for every accumulated
witness once and pass the completed disposition downstream. A present differing `all` witness from the retained
comparison contradicts the request context:

- If exact full-range adaptive acquisition produces all three replacement boundaries logically identical to the
  accepted generation, complete without a new generation. Use publisher's required accepted digests, no extra
  provider or payload verification.
- For unequal replacement with one unique present conflicting `all` witness, only a newer exact-session response
  at the same mapping uniquely matching the acquired output can supersede it. Persist both observations and
  their binding atomically before publication. Retry consumes the supersession. This resolves unaccepted output
  without repeating accepted comparison or expiring evidence by age.
- Missing/ambiguous sessions, conflicting original witnesses for one session, other-view conflicts, a still-old
  response or a third value remain pending.

The workflow carries equal-source correction contracts/receipts through native and standalone JSON stages.
Identical newly selected witnesses use the completed disposition without a new correction ID, full acquisition,
payload read or digest calculation. No numerical revalidation at ingress, even between publication and
acknowledgement. New work preserves those fields. Append retargets the receipt only through its certified
unchanged prefix. Later comparison combines handled prefix with fresh tail. The comparison producer lends
added-session evidence without repeating membership calculation. Changed witnesses outside the prefix, new
session membership, incompatible mapping/store or unaccepted generation retain owning correction. No new
persistence format.

**Positive raw late bars.** Acquire consecutive selected sessions at exact ranges/mapping with accepted raw
response as first adaptive stage. Require one row per selected session per view, reject extra/missing sessions
and conflicts with retained rows. Under generation lock, merge only absent selected sessions, never an enclosing
gap, broad replacement or raw-only splice. Assemble only affected partitions, sharing decoded data. Keep
unaffected references closed. Build compact boundary from accepted endpoint/tail plus inserts. Validate new rows
once, never revalidate retained rows during assembly.

One bounded exact acquisition retry remains for a view omitted from a nonempty replacement. No repeat of
original comparison. Persistent conflict preserves publication. Approved appends never reconfirm historical
rows. Debug emits completed selection. Progress counts each HTTP page once. Planning minimum = two views,
action/dense-history budget = eight. Local correction holds `all`, leaving one to seven direct views. Dependent
plans report actual selections as responses arrive.

For a lagging retained dependency, run adaptive acquisition on the missing suffix first. Complete empty suffix
leaves retained endpoints, payloads and coverage untouched, without historical comparison. Nonempty selects
ordinary dependency acquisition. After mandatory generation-bound first/tail `all` comparison, consume the same
in-memory responses, hashes and decision. Failed/incompletely paginated selections stay pending. Across
subsequent boundary failures in the operation, successful partial responses/completed acquisitions retain their
generation, mapping and exact request scope. Retry only unfinished observations. Metadata activity/inactivity
cannot substitute.

Each pricing HTTP attempt uses a 300-second socket timeout for blocking network operations, including checks,
pages and retries. There is no total operation/retry deadline here. An earlier HTTP 504 is a server failure. The
client timeout cannot extend the provider deadline.

Acquisition owners enforce stages, row budgets, exact fallbacks and bounded retries. Slice completed normalized
responses without renormalizing. Only newly acquired full histories/selected replacements receive independent
producer confirmation. Batch identical retained-check windows without widening dates. Carry immutable endpoints
and comparison to execution bound to store, symbol, source generation, cutoff and mapping. Recheck generation
under lock without rereading validator or comparing again. Completed comparisons survive peer/suffix failure
within operation and binding. Missing endpoint validator fails closed without decoding prices. Retain bounded
request failure handling.

Summary entries carry row/tail acceptance and per-target append requests through planning, selection and
execution. New entry/target selects a new request. An unchanged one reuses validation/compilation. Carry
completed adjustment endpoint/extent projections through publication. Lineage carries its locked source
generation and completed correction-scope decision (including negative) into payload loading and broader
reconciliation.

**Omitted endpoints and historical labels.** A successfully paginated full batch establishes transport-level
empty for omitted symbols. Missing-session acquisition consumes that response. For retained comparison, the
baseline identifies an existing endpoint that batch companions may suppress. A completed omission triggers one
identical-session/adjustment/mapping singleton confirmation. Keep returned endpoints. Never repeat
already-single requests, including 414 subdivisions. Use same limiter/retry policy and exhaust pagination.
Failed batches/confirmations cannot establish absence. Confirmed empties remain missing.

For a missing selected endpoint, alternate dated label requires the selected source-generation-bound receipt's
positive UUID membership and same-CUSIP rename observations tied to that UUID. Normally the current label's
positive exact lookup supplies it. After exact 404 only, a retained last-positive exact own-label lookup can
supply it if one accepted membership and every traversed same-CUSIP rename agree. Request only that session in
`all` at its date, retaining other acquired endpoints and the response's actual historical ticker/mapping
provenance. Unknown membership, unlike exchanges, conflicting identifiers/routes or changed binding prohibit an
alternate. Read only missing selected sources' receipts, once per selected comparison, and carry them. Binding
to receipt generation invalidates only the affected routed comparison. Plan from accepted facts without peer
lookup or new identity calculation. Backward routing stops at each earlier rename, preventing later reuse from
redirecting the former owner.

Successive owners of one ticker: compact index supplies dated scope from completed merger boundary and former
owner's last selected observation. Before request planning, select matching scope for first/tail observations.
Split a crossing tail into one window per mapping. Historical run uses retained mapping date, current run its
current request mapping. Require selected pricing/lineage generations without receipt bodies. Current-mapping
differences can still select latest-session correction. V2 endpoint selection carries lineage generation through
correction/recovery. Changed binding invalidates comparison. A difference requiring historical mapped
acquisition selects explicit owning recovery.

Fallback plans requests without allocating dated rows or inferring identity from a companion's bar. Recovered
observations enter the original single comparison. Missing alternate stays incomplete. Route failures preserve
completed peers. Legacy missing-only fallback differences still need owning correction. Results/debug identify
missing endpoints. Metrics count probes/recoveries. Once the resulting suffix and exact obligation commit in the
lineage projection, retry acknowledges from that compact publication, skipping another live comparison for the
published label. Transport, shape and pagination errors leave actions incomplete. Emptying a previously nonempty
view by replacement requires bounded fresh confirmation. Completed initial singleton acquisition can establish
empty on its own.

Consumers own response bodies. Size splits preserve ordered pagination without older cohorts surviving via
recursive closures/handled tracebacks. [AcquisitionFailure](../libs/alpaca_pricing_download/failures.py) retains
immutable scalars. Fresh exceptions preserve outcome/retry classification without completed buffers.

[Append approval](../libs/alpaca_pricing_download/append.py) binds checked generations to selected missing
intervals. Recheck under ticker lock even when the last row matches. New summary membership cannot inherit
approval. Empty append requires all eight retained views empty. A nonempty source missing an `all` baseline
needs explicit replacement. Empty same-month suffix advances coverage without decoding payloads. Buffered
acquisition binds a generation or explicit absence. Concurrent replacement requires reselection.

Retained-span refresh preserves start/end/mapping. Explicit historical mappings stay pinned through
retries/extensions. Endpoint windows/action discovery use independent observation cutoff. Mapping change
requires complete generation-bound replacement. Completed acquisition and endpoint responses remain distinct
handoffs.

**Scheduling late bars.** Completed serving decisions may select exact unexplained raw-absent source/session
pairs. In the checkpoint retain source generation, checked dates and present raw dates. Publish v2 scheduling
projection in the compact selector. Plan without group/source reads. Adjusted-only presence supplies no raw
presence. Present unassigned raw remains lineage work. Existing ownership rules cover contributors/explained
gaps. Absence alone cannot establish historical label ownership.

Missing/incompatible projection requires explicit `catalogue rebuild`. Convert completed raw-presence handoffs
of online partial/offline groups. For older offline decisions missing provenance, select only required raw
source/date boundaries. Preserve decisions/generations, numerical checkpoints and unrelated pending work. Match
pricing generations under selected locks. Completed all-view absence may establish raw absence for each
unchanged source. Group presence cannot identify a source. Prefix reuse carries these facts under ancestry,
reading only changed/new gaps.

Compact selections include completed online/offline decisions. Exact raw probes bind generation. Consecutive
selected exchange sessions, including weekend/holiday seams, share one source range (≤10,000 sessions).
Unselected market session breaks range. Batch only identical ranges/mapping dates. Disable generic
enclosing-range merge. Route rows only to selected sessions. Persist completed positive action before next
action. Independent completed negatives survive peer failure within operation and source/mapping/range binding.
Unfinished pagination yields no observation. Plan and execution share mapped ranges. Exact-session override
preserves selection/freshness. Confirmed late row enters selected-session adaptive acquisition and
complete-group lineage. Preserve earlier full correction's historical cause, scope and accumulated observations.
Orchestration cannot predict eligibility. Stale pricing/serving generations require reselection without widening
probes. No negative-probe freshness cache.

[validate_activity](../libs/alpaca_pricing_download/activity.py) checks new partitions for equal trade counts
across views and raw share volume in non-split views. Exact decimals preserve units. Missing rows differ from
zero. `activity_consistent` covers retained history only after full validation or inheritance. Legacy appends
cannot certify untouched cold partitions. Readers trust published guarantees.

[Pricing ancestry](../libs/alpaca_pricing_download/changes.py) retains up to 64 ancestor generations plus
earliest possibly changed session across all views/removals (null = identical observations), surviving
acknowledgement/rollover. Missing/expired ancestry proves no unchanged prefix. Reevaluate selected affected
histories, never audit store-wide metadata. Full refresh can prove equality from new rows and accepted logical
digests/counts without old price reads. Same-anchor proof covers eight views, complete years, active month and
recorded year-snapshot bounds, with active month overriding year overlap. New non-active-month rows outside
bounds, unproved overlap or rollover use ordinary changed-span handling. Proven equality retains payload
references and governs ancestry, new notices and returned `changed` together. Equal retry preserves pending
correction. Mapping/request context still reaches ordinary lineage selection.

### Dated mapping acquisition

Lineage's [mapping planner](../libs/alpaca_lineage_build/numerical_mapping.py) selects genuinely new
retained-source requests under [dated mapping rules](lineage.md#dated-numerical-mapping). Source-local
`.numerical-mapping.json` journals provider request, generations, accepted identity/routes and relevant provider
facts. Publishing its own result does not invalidate the decision via receipt digests or acknowledgement fields.
Open selected journals only, without directory/universe discovery.

[MappingAcquisition](../libs/alpaca_pricing_download/mapping.py) carries completed `all` witness, remaining
disjoint request ranges, adaptive views/digests/decisions, coverage and endpoint confirmations. A positive
witness establishes one observation only. Request missing `all` ranges and additional policy-selected views.
Assemble completed rows without renormalization or historical authentication. Persist each stage before its
dependent request. Restore by checking journal schema/binding, never rehashing rows, rebuilding projections or
repeating completed confirmations.

Required contribution dates come from complete held producer inputs or an applicable selected serving
checkpoint's accepted date projection. Counts, endpoints and market calendar cannot substitute. Pricing compares
new views with those dates. Stop on known omission preventing a complete bundle. Never authenticate the
replacement from old validators/gzip. An exactly matching bound witness session window supplies the first fully
adjusted endpoint. Confirm each other selected endpoint and retain it through interruption.

For exactly bound unmarked legacy checkpoint without dates, serving assembles missing dates from completed raw
ownership positions, reading only contributing raw partitions. Validate new projection against accepted
count/endpoints. No price comparison, ownership selection or calendar reconstruction. On lock reacquisition use
pricing selector identities captured under preceding locks. Changed selector requires owning reselection. Lend
held raw rows and accepted boundary document to partition assembly without manifest/payload rereads. Journal
completed dates before constructing acquisition so interruption repeats neither date assembly nor witness.
Missing declared dates select explicit owning recovery.

Pricing installs through its existing journal, source selector last. Its correction binding carries installation
across interruption before lineage acknowledgement. Resume only work invalidated by resulting source/mapping.
Preserve applicable peer decisions/assessments. Native/JSON stages carry identical accepted state and aliases
without rerunning durable ingress or calculations. No serving-policy rebuild, research assertion or
ticker-specific rule.

## Check summary

Format 4 entries carry `symbol`, mapping `asof`, completed source `end_exclusive`, pricing `source_sha256`,
exact eight-field `first_all`/`last_all` and producer `tail_all`/`tail_version`. Only pricing owns the
generation, even when the lineage index awaits acknowledgement. Eligibility requires a unique stored same-symbol
active US-equity lookup and nonempty `all` endpoint. Stored activity alone cannot establish online availability.
Group checks by endpoint session/`asof`, requesting identical windows once. Row determines session window,
`asof` the provider identity, completed exclusive end the append start. Epoch/revision/coverage and separate
owner-pending markers govern publication. No identities, other views or saved live comparisons.

- Format 3 owns membership/endpoints and remains readable for bounded legacy-tail upgrade. Writer adds columns
  transactionally and projects each completed pricing upgrade without discovering ticker directories or
  rebuilding eligibility. Missing new-format tail/generation fails closed.
- Formats 1/2 require explicit offline `summary rebuild`, using indexed eligibility and selected source
  validators without price acquisition or history rewrites. Reads never search all validators for missing first
  endpoints. Frozen publications may finish format-2 pending bookkeeping before rebuild. Format-2 entries cannot
  approve live checks or appends.

Pricing owns baselines, lineage membership. Missing/incomplete/pending coverage fails closed. Snapshot tokens
guard comparison without ticker enumeration.
[Summary storage](../libs/alpaca_pricing_download/check_summary.py),
[projection/rebuild](../libs/alpaca_lineage_build/check_summary.py)

Unchanged lineage leaves completed selected summary intact. Changed membership reuses an accepted endpoint when
pricing has no pending change, without reopening its validator. Price invalidation, new eligibility or
incomplete baseline requires the pricing owner's selected endpoint. Under held pricing locks, read only named
entries/pending owners.

Recovery/discovery share one accepted catalogue and exact asset/action lookup cache per operation. Carry
completed lookups, negative outcomes and failed requests across phases. New invocations acquire fresh
observations. [Evidence lifetime](lineage.md)

Daily recovery finishes only named pending inputs and their complete groups before selection. Remaining pending
inputs block selection. Pending summary neither implies empty membership nor authorizes checks of unaffected
active histories.

During daily boundary acceptance, work selection and estimation, the compact-only
[scope guard](../libs/common/work_scope.py) rejects ticker reads, notices, locks and payload paths at
pricing/lineage APIs before filesystem access. After selection it permits selected complete groups, summary
maintenance, late-bar sources and pending serving components. Completed metadata dependency/serving-component
handoffs may explicitly expand scope. Unexpected path/inventory aborts via terminal CLI error, releases locks
and closes spans. Per-symbol retries cannot swallow it. Workers inherit scope. A latched violation rejects
subsequent owner access and successful scope completion even after a caught worker exception. Lexical checks
only, without selector authentication or payload inspection. The guard covers owner APIs only. Estimated pages
are no hard budget, and inclusion does not prove every read necessary.

## Universe observation and work state

The operation's [catalogue](../libs/alpaca_lineage_build/catalogue.py) retains duplicate labels. Unique symbols
may supply metadata. Missing/duplicate records need exact fallback. CUSIPs always use exact lookup. Absence
proves neither inactivity nor unavailable prices. [Projection](../libs/alpaca_lineage_build/projection.py)
separates current evidence from last-positive own-label facts. Missing historical times remain unknown.

[Registration](../libs/alpaca_lineage_build/registration.py) persists intentions/work before registry union and
acknowledgement. Freeze first-read label selection across execution/batch memo. File changes apply to later
commands. Registry/work records cannot establish publication. Initialize work independently of lineage/summary
coverage. Interrupted registration selects local recovery without provider failure delay.

[WorkPolicy](../libs/alpaca_lineage_build/work_contract.py) and
[durable state](../libs/alpaca_lineage_build/universe_state.py) retain stable obligation IDs, accepted
endpoints, evidence fingerprints, complete groups, refresh/replacement requirements and retry state. Persist
dependencies/corrections before requests.

Accept work and record latest catalogue observation ID/UTC time in one transaction. Selected symbols name that
acceptance ID. Existing obligations retain their original IDs. Only the latest ID has a timestamp in the
catalogue selector. Full provider response travels with evidence client or standalone Prepare handoff. Workflow
stores only obligation-selected evidence. A new operation needing current evidence observes its own catalogue.
For a pending [dated listing change](lineage.md#changes-of-listing-exchange), the source's compact lineage
projection carries the earliest outstanding assessment time for the next eligible session. Ordinary planner
consumes it. Completion removes it through the lineage journal.

Explicit v1 migration takes latest ID/time from old catalogue table and removes that table transactionally.
Preserve registration rows, work bodies and revision. Workflow owner compacts the database to reclaim space.

Scoped work-state reads select SQL symbol keys before decoding and may exclude completed work. Use only for
already-bounded complete groups. Partial views establish neither global census nor independent reverse-pending
relationships. Registration intents/state revision remain compact global authorities. Ordinary unscoped
registration retains full selection.

**Acknowledgement.** Lineage stamps only satisfied locked inputs. Coverage cannot retire unperformed
refresh/correction or failed checks. Completed replacements/extensions survive interrupted acknowledgement. A
matching last row cannot establish either. Acknowledgement alone creates no lineage/serving generation.
Unchanged retry uses accepted index coverage/group plus fresh check bound to pricing generation. Producer lends
checked input completion in memory, bound to obligation and every selected group generation. Later history
approval consumes it without reopening the group. Pending work, renewed obligation or changed group generation
invalidates. Durable records acknowledge the actual publication. Interruption before acknowledgement leaves work
pending, afterwards requires no republication. Changed evidence, pricing, assignments or policy use owning
publication.

Batch completions transactionally, comparing accepted obligation ID first. A newer obligation rejects the stale
batch. Bind batch to store, process, thread and active transaction. Precommit interruption rolls back. Committed
outcomes survive. Persist complete dependency groups/maintenance requirements together before requests. Recovery
reselects acquisition from accepted requirements/summary. A saved `acquire` flag cannot divert summarized
maintenance from live check → approved append.

Published numerical repair retires only the matching prerequisite transactionally. Compare obligation and
numerical operation IDs. Retain other requirements/target. After interruption acknowledge from completed lineage
projection before ordinary pricing/lineage advance. Apply within recorded scope. Changed selected numerical
inputs follow numerical-owner recovery.

**Dependencies.** Register known membership before missing-suffix selector request. Its completed result
determines whether acquisition exists. Positive selection records requirement before retained
comparison/publication. Empty selection for unfinished group work checkpoints obligation, peer generation,
mapping and exact suffix bounds. Restart consumes it while applicable. Independent later operations obtain their
own observations. Older work lacking producer result must obtain selection.

Lineage retry uses published group's required span. Workflow cutoff alone cannot extend lagging peer coverage.
Applicable empty suffix can satisfy dependency check while its stored endpoint stays fixed. Independent
coverage/mapping/correction/numerical obligations keep their conditions. For an independently required advance
whose full replacement already published, exact correction ID, predecessor generation, mapping and selected
cutoff approve the suffix even after leaving summary membership. Daily/focused lineage journal that coverage via
normal pricing append. Use a completed all-view empty selection directly within original source/request scope
and acquire missing suffix once. After interruption, installed request/ancestry establish which corrected prefix
reached cutoff. Different predecessor, earlier changed price or changed mapping invalidates.

**Comparison, correction and append.** Ordinary eligible summary members complete maintenance before discovery
advances dependencies. Reconcile selected changed identity/lost eligibility before maintenance. Missing older
endpoints may select retained lineage review first. Bind missing sessions/mapping to current summary boundary.
For older decisions, an open own-label assignment to another UUID plus retained outgoing merger select the
affected group. Evaluate accepted evidence through ordinary lineage/journal. Completed review supplies a route
or leaves comparison outstanding. On restart current receipt supplies decision, pricing owner any completed
comparison bound to selected route generation.

Carry completed comparisons, including differing results, through peer failures. Each binds summary entry,
pricing generation, accepted selectors and current allowed scope. It selects unchanged, latest correction or
full replacement via pricing. Append approval additionally binds current correction obligation, excluding
renewed corrections. Successful corrections/appends retain approvals once lineage accepts exact generations,
even on peer failure. Incomplete lineage cannot lend approval. Refresh forwards completed independent subset
even when aggregate incomplete. A legacy-tail metadata publication also completes its lineage generation handoff
without repeated tail assembly or comparison. Exclude failed/restricted dependency groups. Successful peers
acknowledge without reopening completed lineage.

Missing-session completion approves only selected sessions. Otherwise daily first/tail comparison still requires
applicable retained-history approval. Insertion preserving every retained observation and effective mapping
carries existing source-bound approval to the new generation. Changed interior invalidates even with identical
endpoints.

| Operation-local acceptance | Producer and limits |
|---|---|
| Full acquisition | After successful initial, replacement or equal-price full installation, build immutable acceptance from held endpoints. Distinct from retained comparison. Maintenance may consume only when compact lineage and summary both select resulting generation/mapping/coverage/endpoints and current acquisition/correction requirements match. No provider request, validator/payload read or endpoint recalculation. Incomplete publications, covered histories, ordinary appends and selected-session corrections do not produce it. |
| Approved dependency append | After new rows publish, bind completed predecessor comparison and resulting generation's first/tail facts. Mapping change selects one new comparison. Successful append cannot repeat it due solely to generation advance. Both use the same compact applicability gates. Fresh invocation retains live checking. |

Every cached comparison/successful approval carries its own original target, required mapping-evidence
fingerprint and correction ID. Later bookkeeping preserves that scope. A newer differing peer comparison cannot
widen earlier approval. Producer-derived approvals require actual lineage index generation independently of
summary. Missing mapping evidence selects metadata/lineage recovery before provider work. Another price
comparison cannot provide it.

**Durable corrections.** Peer approvals expire with the operation. Pending corrections retain scope, cause,
source generation, mapping, coverage and usable observations. Consume applicable pending decisions without
comparing again. A changed generation invalidates both full and latest-session scope. Fresh completed result
owns the new scope. V3 accumulates raw/adjusted trigger rows under store, symbol, generation, mapping and
retained bounds. Changed binding discards scope. Changed requirements renew correction ID. Merge numerically
equivalent witnesses. Conflicting duplicates remain constraints. Each witnessed New York session needs exactly
one acquired row matching timestamp and values. V2 row-bearing contracts stay readable. External date-only
interpretation is unsupported. Missing required trigger evidence selects owning recovery.

Equal effective prices with unchanged request/mapping/identity evidence publish only an atomic completion
receipt under ticker lock. Retain data generation and acquisition provenance. Workflow accepts that source-bound
result transactionally against current obligation/correction IDs. Interruption after atomic replacement repeats
neither acquisition nor lineage/serving publication. Changed generation/requirement invalidates. Changed-price
publication retains its journal and embedded completion ID.

Explicit symbol refresh accepts equal-price completion before workflow acknowledgement. Unfinished matching work
consumes its atomic receipt before acquisition planning. A fresh invocation after full acknowledgement creates a
new requirement. Completed changed-price publication retains ID while lineage is pending. Producer-certified
append may carry equal-price completion to its new generation only under same correction ID and unchanged
retained span, with ancestry proving every change starts at/after correction's exclusive end. Renewed
requirement, earlier change or missing ancestry rejects carry. After pre-acknowledgement interruption recover
from selected source metadata without reacquiring.

Work-state ingress structurally validates trigger rows once: supported view, eight string fields, timezone-aware
timestamp, finite numbers, nondecreasing order. Conflicting witnesses remain evidence. Initialization checks
only compact work-state gate. Final pending counts use status without decoding/revalidating accepted correction
rows. Legacy correction ID lacking sufficient cause retains full replacement. Identity changes and confirmed
missing historical rows keep their explicit owning scope. Started journals finish selected publication.
Completed replacements remain complete. Partial success cannot complete a failed update.

**Estimates and evidence.** Work estimates use accepted compact selectors' durable scopes. Exclude deferred work
and applicable pending comparisons from fresh checks. Count local corrections, full replacements, metadata-only
acknowledgements and combined correction/append publications separately. Source-manifest-only completion ID may
reduce execution estimate. Plan never opens manifests to resolve it. Debug `update_work_estimate` records
selections alongside comparison, acquisition, publication and serving evidence. Response-dependent
differences/group expansion stay conditional. Counts establish neither throughput nor the 60/90-minute
performance target.

Every lineage entrypoint reconciles used lookup facts before acquisition. Identity change requires
complete-group replacement, including newly discovered dependency evidence. Pricing binds evidence
fingerprint/source generation. Lineage cannot stamp different facts. Catalogue/exact provenance alone cannot
invalidate identical facts.

Mapping changes bind every affected member to one group fingerprint, including absent/unchanged own lookups.
Retries/metadata-only changes preserve it. For older pending replacements missing fingerprints,
selection/evidence reconciliation binds accepted group evidence, preserving non-null requirements and completed
pricing handoffs. Required-member failure blocks siblings' publication. Failure transactions refresh in-memory
policy, retaining attempts/deadlines on obligation expansion.
[Retry and coverage](operations.md#recovery-registry-and-retries)

[inspect_status](../libs/alpaca_lineage_build/status.py) reads compact boundaries, only scheduling/summary-named
pricing journals, and lineage marker under shared gate. No payloads, receipts, ticker census or recovery.
[Status guarantees](cli-reporting.md#completion-and-status)

## API plan

Credential-bearing pricing/metadata transports reject redirects. An accepted target authorizes only that
destination. Metadata ingress rejects duplicate JSON keys and nonfinite numbers. Missing/null page token
completes pagination. A new nonempty string continues. Malformed/repeated tokens fail before completed evidence
interval publication.

The shared [HTTP executor](../libs/common/provider_http.py) sequences attempts, balanced subdivision and pages.
Adapters retain endpoint validation, parsing, timeout, limiter and retries. Pricing shares a 199/minute budget
with up to six attempts on 429/5xx or transport failure. Metadata uses a separate limiter, 60-second timeout and
429-only retries. Exact assets/catalogue share attempt lifecycle without symbol subdivision.

Bars/corporate-action rosters use `ceil(symbols / 1000)` sequential physical batches, smaller first, differing
by ≤1 symbol. Thus 3,386 symbols → 846, 846, 847, 847. Preserve dates, adjustment, mapping and other parameters.
Exhaust each batch's pages before next. HTTP 414 bisects only the rejected batch, discarding its partial pages
and restarting its pagination. Hand off completely normalized physical subsets before proceeding. Preserve
positive/negative subsets on independent failure, attributing only unfinished deliveries. Dependent steps remain
blocked. A routing contradiction in newly assembled corporate actions invalidates evidence separately from
independent transport failure. Completed omissions remain empty without symbol fanout. Rejected exact split
child cannot establish unavailable siblings.

Page estimates include physical subdivision. Live checks report planned/executed batches (including 414-rejected
batch attempts), page fetches and 414 splits. All transport attempts/retries increment existing client count.
HTTP debug spans show each physical page and attempt count. Workload cap guarantees neither latency nor
protection from backend timeouts.

Pure [build_plan](../libs/alpaca_data_api_solver/__init__.py) maps `steps[].needs[]` to versioned ordered
actions. Code owns fields, strategies, delivery slices and batching limits. Needs require nonempty reasons.
Within-step needs are independent. Exhaust pagination before advancing. Build dependent needs only after
responses.

Bars use 1Day/SIP/USD, separate adjustments and symbol-mapping `asof`, independent of adjustment vintage.
Date-only ranges mean New York midnight. Timestamps require timezone. Corporate-action bounds inclusive. Batch
compatible symbols/ranges, keeping adjustments/asof distinct. Symbol lookups may use catalogue batches and exact
fallback. CUSIPs stay exact. Page estimates exclude response-dependent correction/expansion. No global HTTP
minimum or ETA.

Historical listing migration consumes literal SIP auction pages for selected label/dates. Share one bounded
auction window among ≤48 eligible tickers. On a sparse day, one page of up to 2 × requested symbol count (≤96
rows total) can establish multiple tape observations. An exhausted day route also establishes empty labels. A
truncated page establishes only observed labels. Omit them from subsequent requests. Individual point request
handles remaining scope. The evidence database retains each accepted physical result and calendar-bound auction
screen. Operational denials and malformed responses remain acquisition errors.

## Lineage reuse

[Lineage contracts](lineage.md) own complete-group reuse, policy migration, observation selection, ownership
constraints, metadata continuation and missing auxiliary prices.

## Publication and recovery

Stable `.download.lock` inodes provide producer-exclusive/reader-shared `flock`. One lineage `.worker.lock`
serializes writers. Sorted pricing locks plus a brief publication gate protect selected groups while permitting
unrelated reads/publications. Readers select → lock → recheck → consume. Lock-file existence proves no
ownership. Completed indexed group membership selects locks without opening receipts. After locking, an
unchanged index revision retains selection. Otherwise reselect before exposing data. Read required receipts once
within this coherent view, including offline inspection. Missing membership requires explicit index migration.

[`operation_lock`](../libs/common/store_lock.py) uses stable `.<store-name>.operation.lock` beside the store,
before owner locks. CSV export holds nonblocking shared `flock` from catalogue selection through run manifest.
Every store-writing CLI holds exclusive `flock` throughout. On contention, try once nonblockingly, report wait,
then block on the same descriptor until acquisition/interruption. No polling or boundary reads. Internal callers
default nonblocking and opt into waiting. Writers exclude writers/exports. Read-only exports may coexist. Exit,
including `SIGKILL`, releases the kernel lock and leaves the pathname. No PID/stale-lock recovery.

The public [Prepare JSON entrypoint](development.md#pipeline-phase-1-prepare) takes the same exclusive gate
nonblockingly after receiving complete input and releases it before output. Next owner reacquires and checks
named selectors without replaying preparation. Direct internal producers require caller-provided outer exclusion
alongside exports and cannot independently write concurrently. Public consumers use `alpaca_end_of_day`.

Owners close temporary files, then atomically replace journaled outputs: payloads → receipts/index, summary →
acknowledgement, journal deletion last. Acknowledge only selected notices/hints. Pricing hands off accepted
source/endpoints, retaining cold references and inventorying only replaced boundaries. Interrupted multi-file
publication blocks readers until owner recovery.

- V1 pricing installation handoff journals only producer-passed new outputs with closed staging files. Install
  with retained digests, never rereading/hashing staged or installed bytes. Under the stable ticker lock, a
  missing staging file means its rename completed. Journal's accepted source and fully adjusted endpoints/tail
  supply summary recovery without reopening source/validator. Matching summary generation selects
  acknowledgement only.
- Missing/unknown declared handoffs require recovery, never legacy reconstruction. Unmarked legacy journals
  accept each selected staged/installed output once and parse updates from the same bytes. Clean abandoned
  staging under ticker locks.
- On lock reacquisition lineage recovery reselects journal-named pricing sources, notices and hints once. Carry
  these values through summary refresh/acknowledgement until lock release.
  [Pricing storage](../libs/alpaca_pricing_download/storage.py),
  [lineage storage](../libs/alpaca_lineage_build/storage.py)

The [historical listing producer](../libs/alpaca_lineage_build/historical_listing_publication.py) binds literal
assessments to the complete selected group's pricing/lineage references, serving decision and public ticker
ranges. Record completed unknowns and positive evidence in the first priced member's receipt (label order).
Compact scope roster selects prior results if a former anchor becomes a peer. Ordinary lineage retains original
public ID, literal label and dates per observation. New serving decisions apply those bindings to their
published ranges.

Changed competing ownership may supersede an assessment on identical dates. Current plan names original scopes
and every replacement slice. Replace them and their affected listing changes together. Direct trades on
consecutive market sessions can establish a change across adjacent assessed ranges, including same-ticker
extension. Each new/superseded slice selects affected endpoint comparisons. Move the same owner's observations
from former to current anchor and clear the superseded copy.

Batch writes changed anchor receipts. One index transaction selects them and exact serving obligations. One
serving transaction installs descriptive fields and advances lineage references for the batch. Acknowledge exact
generations. Recovery checks installed selector, rolls frozen journal forward if needed, then consumes
outstanding serving obligations. Pricing receipts, assignments, checks and price notices remain with their
owners.

Replacement preserves payload files whose recorded digests match new bytes, without reopening, checking or
rewriting them. Journal changed payloads, receipts and required notices normally. Physical repair requires the
explicit pricing-owner option.

[connect_sqlite](../libs/common/sqlite_connections.py) uses transactions, disk rollback journals and write
`synchronous=OFF`. Producers own native recovery. Readers/previews fail closed pending recovery. Process
interruption is covered without forced synchronization. Machine/power failure remains outside the contract.

## Serving catalogue

The [allocation criterion](lineage.md#ownership-allocation-criterion) weighs uncertainty with the competing
outcomes. Current lost-ownership recovery publishes affected IDs offline. V1 `offline_ownership` records reason,
affected UUID-connected identities and original checkpoint references. It neither approves shortened history nor
merges IDs. Publish negative decisions for affected IDs, retain independently supported identities including
rights/units, and skip offline numerical-prefix evaluation. Unchanged retries consume the result. Actual input
changes select owning evaluation and can reuse frozen checkpoints without checking accepted prices. Operators
see `offline_reviews` in `catalogue status`. `DataLibrary` serves only online IDs and returns ordinary
unavailable-ID for offline ones. This preserves recoverable history while best allocation remains
[open](trust-boundary-handoff.html#f64).

Under lineage worker lock with no publication journal, maintenance reconciles lineage export intentions. Only
compact serving selections matching current lineage index authorize acknowledgement. Conditional deletion
consumes exact selected intention, including abandoned pre-journal intentions, without receipt reads or
ownership/numerical replay. Pricing intentions retain their owner gate. Export enforces applicability, never
repairing or skipping changed selected input. Online selection excludes offline IDs.

[`policy.evaluate`](../libs/alpaca_serving_catalogue/policy.py) consumes completed IDs/assignments and exact
prices without upstream authentication or identity replay. For the complete selected history:

- Require one nonempty unique market-session set across eight views and deterministic source selection.
  Conflicts make an ID ineligible. Unreadable publications leave work pending.
- Require timestamps at New York session midnight, positive OHLC, open/close and positive VWAP inside low/high,
  nonnegative exact-decimal volume and integral trade count. Ordinary volume/count both positive, except
  split-adjusted volume may round to zero. Explicit zero-activity exception: flat positive OHLC, zero
  volume/count/VWAP. Upstream guarantees finite exact numbers.
- Explain continuity without filling sessions. Exclude a rename gap ≤5 sessions only with complete action
  evidence, unchanged UUID/CUSIP/exchange, one matching dated rename, compatible exact symbol/CUSIP lookups, no
  conflicting occupant/event and no source observations in any view (including unresolved). Exclusions cannot
  allocate rows or merge identities.
- Relevant unresolved assignments restrict their dates. Explicit candidate UUIDs remain conflicts. Anonymous
  former-ticker observations matter only within published ownership bounds when available. After complete
  assigned history passes numerical/session checks, serving may publish its assigned suffix strictly after the
  last relevant unresolved interval. Uncertainty through the latest assigned session vetoes. Readers/CSV consume
  only advertised range. This gate makes no allocations or identity links and cannot establish optimal
  withholding under the semantic criterion.
- Trust exact certified vacant-ticker/pre-entry carry-forward non-observation dispositions without replay. They
  carry no unresolved observation. Additional reasons retain ordinary uncertainty. Empty/entirely unresolved
  groups complete without public IDs.

Inactivity, nontradability or missing description alone cannot disqualify. Coverage means approved stored
history and guarantees neither lifetime completeness nor current freshness. Policy version/calendar digest
select positive and negative decisions. Names/exchanges/classes come only from own-symbol observations of that
ID's UUIDs. Current CUSIP lookup cannot supply them. Observation time gives no effective date without dated
provenance. Omit unknown descriptions.

### Checkpoint reuse

[Checkpoints](../libs/alpaca_serving_catalogue/reuse.py) retain generations, ownership runs, contributor
membership, numerical outcomes, labels and gap evidence without copying prices. V2 source projection:
`{version: 2, sources: [...]}`. Legacy coverage selectors lend accepted source keys without reconstructing
subsets/overlaps. Ordinary history reads build no coverage ranking. Reuse per identity across group
merges/splits. Independent identity changes preserve it. Proven appends skip old rows and closed payloads even
if suffix containers hold earlier rows. Changed adjusted-source selection, ownership, replacement or missing
ancestry requires affected-history evaluation.

For whole-ID substitution of one physical source, numerical/calendar outcomes may carry only if original
generation still applies and both selections cover exactly identical partition positions in all views. Compare
counts/eight logical digests in compact pricing partition manifests only for this selected substitution. Equal
selections retain checkpoint with new membership. Use accepted raw dates for changed displayed labels. Any
unequal view selects scoped numerical revision or ordinary first validation.

Producer-certified latest-session correction may reuse checkpoint before that row. Normal serving evaluation
publishes preceding-session raw labels and numerical outcomes. Bind shortened checkpoint to current ownership
runs/price ancestry. Evaluate changed and new rows normally. For multiple contributors preserve accepted
membership and obtain corrected date again from each source that contributed it. Earlier payloads/checks stay
closed. Changed ownership, mapping or selection requires scoped reconciliation.

Legacy checkpoints lacking preceding-row facts may reuse a completed all-passing numerical outcome. Project raw
boundary from accepted ownership and session compatibility only when one final source supplies preceding row and
all views of other sources end earlier. Equal overlapping coverage cannot identify the previous raw-label
winner. Missing numerical outcome or ambiguous raw provenance requires explicit catalogue rebuild. Report
without implicit historical audit. Metadata-only publication consumes accepted prices without reads and
preserves legacy capabilities. Omit revision-prefix marker when preceding raw boundary absent. A later append
can establish it from the previously published last row and publish the marker.

Partial publication retains full-history numerical checkpoint. New records carry accepted session dates. Project
range/count/labels from dates and completed ticker ranges without price reads or numerical replay. Changed
uncertainty may change supported range independently. Legacy partial checkpoints may retain published range
across metadata changes/suffix advances. For a new cutoff needing a different range, an applicable unmarked
legacy checkpoint missing dates selects one assembly/publication of accepted raw dates. Exact existing ranges
remain payload-free. Numerical/calendar outcomes stay accepted. Missing declared current dates fails.
Earlier-policy migration requires identical complete-group pricing generations and row ownership, then
reevaluates current eligibility.

An ID's accepted sessions may flank a gap supplied by different sources. On assignment into the gap retain
applicable selections on both sides, assessing only new sessions in all eight views. Remove filled raw dates
from missing list, retaining remaining gaps and dated label bounds. An additional choice on an already accepted
date requires its owning reconciliation. Catalogue commits coverage, labels and eligibility together.

Earlier assignments exposing a gap before a published recent range preserve that range if ownership, numerical
approval and other eligibility still apply. Public range begins at first supported session after latest
unresolved gap or applicable ownership/price restriction. Private checkpoint keeps earlier assignments and exact
gaps. An active ID may gain its first public range if exact own-label lookup agrees with final selected session
in all eight views and the entire run after the earlier unresolved gap passes ownership/numerical checks. Retain
exact unresolved dates and source-presence facts. Explicit selected gap-only recovery consumes completed
date/numerical checkpoint under identical lineage/pricing generations to project range/metadata without price
reads, repeated checks or peer changes. For a clean unmarked gap-only checkpoint, assemble/publish dates once
from its exact accepted market bounds and missing sessions.

The compact late-bar selector schedules contributing raw-absent source dates even for online IDs. On returned
bar pricing publishes complete selected bundle. Serving binds insertion positions to generation/current dated
ownership and retains unchanged rows. Closed gaps can extend same ID's public range backward.
Unassigned/conflicting bars cannot establish that ID's missing session.

For a one-session ticker-change gap, developers can query raw SIP daily bars for both dated labels. Compare
stored `asof` with session-date mapping or query literal symbols with `asof=-`. A paginated empty establishes
only what that request returned then. Gap remains eligible for later updates. Retain any positive alternate
mapping with its observation so lineage can assess dated ownership.

Older catalogues may have withdrawn the recent range. One-time review selects only gap-bearing groups via
compact repair selections. A gap-only decision may support the distinct later ticker range if dated witnesses
isolate it and every contributor records raw absence across separating gap. Record selected reviews before
publication and retire them with component commit. Completed negative screens need no group publication.
Preserve exact price, owner, calendar and unrelated approvals.

Reuse covers each view's latest validated session. Changed coverage can clear negative cross-view decisions.
Keep raw continuity labels separate from public all-view labels. Use current gap evidence. Changed group inputs
refresh bounded gap-presence reads without repeating unaffected numerical checks. Share physical-alias decoding.
Commit checkpoints/decisions together. Policy changes require explicit rebuild of the single current checkpoint
format.

Policy 5/6/7/8→9 exception: translate policy-5 scalar ownership labels to lists. Reuse completed facts only when
whole-group pricing generations and every row/source/owner/label match. Policies 7–9 change no numerical history
checks. Always recalculate current eligibility from completed receipt/producer dispositions. Compare selected
checkpoints/assignments without price reads. Evaluate changed history normally. Both paths project current
metadata. Consumers still require current policy. No other policy/calendar changes qualify.

If `outside_supported_name_change_history` unassigns an old owner while price ancestry preserves accepted
positions, do not silently shorten its same-ID history. Before payload reads, check narrow loss using accepted
checkpoint runs/current assignment metadata. Retain prior checkpoint and publish completed offline containment
with lost source/row/date scope. The [ownership review](trust-boundary-handoff.html#f58) remains open. Current
receipts lack the prior mapping/evidence invalidation handoff needed to restore or revoke ownership. Changed
pricing, public membership, correlated reallocation and other unresolved causes retain their own paths.

### Publication and maintenance

[Storage](../libs/alpaca_serving_catalogue/storage.py) atomically replaces component decisions, public fields,
dated matches and ID/group/input references. Incompatible duplicate IDs abort. Publish selector last with
schema, policy, time, completeness and transaction token. Readers never hash the database. Compact selection
serves status, preview, snapshots and unchanged work. Open checkpoints only for selected changed groups.
Matching contributions require neither validation nor rewrite.

Serving SQLite writers wait up to 120 seconds on contention with public snapshots. Timeout/interruption rolls
back transaction. Pricing/lineage journals retain constructed artifacts. Existing rollback
journals/synchronization remain in force.

[maintain](../libs/alpaca_serving_catalogue/producer.py) follows published membership and prior contributions
including absent dependencies, without rediscovering closure. Lineage transaction records exact-generation
[serving obligations](../libs/alpaca_serving_catalogue/work.py) before upstream acknowledgement. Commit
catalogue before deleting obligation. Retry consumes committed work without history reads. Lineage worker
serializes maintenance. Hold selected pricing locks through publication. Failures/focused commands leave
unrelated obligations pending.

A lineage version-header publication may carry an exact producer-issued unchanged decision. Journal old/new
complete-group receipt generations and unchanged pricing in the v2 serving-work transaction. Atomically retarget
matching references while preserving decisions/numerical checkpoints. Missing/mismatched handoff selects
ordinary maintenance. Explicit work conversion preserves queued obligations without implicit universe
reconstruction.

First accept compact catalogue state and complete work queue. Empty queue skips group/member maps and lineage
membership. Only selected work needs them. Aggregate `pending_export_inputs` by pricing/lineage owner against
current published-component membership in one compact query. Outstanding applicable invalidation prevents
completion even with empty queue. Focused requests cover every member of selected published components.
Never-served/retired sources cannot invalidate an existing export. No upstream rereads, schema or eligibility
changes to establish completion.

New component publication consumes held superseded lineage intentions transactionally, restricted to exact
selected intention and newly published members. Renewed intentions, unrelated components and rolled-back
publications retain obligations. Worker carries completed applicability comparisons into queue consumption.

Held lineage writer lock lends process/thread-bound accepted store compatibility and lineage index to selected
readers. Consume without index/protocol/path rechecks. View expires before release. Per-group pricing locks,
source-generation and pending-publication checks still apply. Ordinary readers without this view select, lock
and reselect on revision changes. Runtime-only handoff changes preserve serving policy and completed
checkpoints.

Explicit bootstrap covers indexed groups and may build missing membership from receipts in bounded batches,
preserving digests/identity decisions. It may read full selected histories. Ordinary maintenance/readers never
bootstrap. Policy rebuild uses this path. Incompatible schemas need supported explicit migration.
[Operations](operations.md#serving-catalogue-maintenance),
[public snapshots](public-api.md#online-catalogue-reader)

## Parquet conversion

[Exports](exports.md#parquet-conversion) owns conversion, memory and publication semantics.
