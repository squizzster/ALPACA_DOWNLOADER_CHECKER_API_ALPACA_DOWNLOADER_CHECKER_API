# Technical contracts

## Trust and ownership

[AGENTS.md](../AGENTS.md#trust-before-touching-storage) owns storage-trust rules. After a completed selector's
required schema/version/token checks, consume selected work. Reacquiring locks requires generation checks, not
existence/symlink inventories, stat-based cache keys, payload hashes or upstream rule replay. Producers validate
new outputs/destinations and compare recorded digests; explicit authentication audits existing data. Readers fail
closed without repair.

Semantic ownership decisions follow the [allocation criterion](lineage.md#ownership-allocation-criterion).
Missing historical evidence is weighed with plausible owners and prior accepted ownership; missing technical
publication metadata remains an owning recovery obligation rather than evidence against an owner.

[OperationContext](../libs/alpaca_lineage_build/context.py) retains coherent accepted compatibility, index and
summary views per invocation. Revisions/publisher-returned changes refresh them; no persistent context cache or
alternative authority. Changed inputs invalidate dependent work only.

Lineage computation revision 2 selects full retained-input reconstruction of earlier derived decisions. Receipt and compact-index revision fields become authoritative only after the new calculation publishes; metadata and pricing acceptance stay independent. Explicit rebuild selects one seed per complete group, keeps individual retained request bounds and auxiliary mappings, and skips completed revision-current groups before body reads on retry. Missing retained evidence or a newly required pricing correction remains a blocker. Serving preserves an identity's numerical prefix only through its accepted last owned source position, so receiving later rows from a repaired spurious identity checks that new suffix without replaying earlier history.

### Targeted retained lineage recovery

The lineage owner's explicit `lineage recover` command consumes a version-1 selection with boundary
`alpaca_retained_lineage_recovery`, an absolute `store`, `correlation_policy_sha256` and `groups`. Each group
names sorted complete `members`, `pricing_generations` and `receipt_generations`; the two generation maps name
every priced member. Absent auxiliary members remain named without a fabricated pricing generation.
The accepted receipt generation binds its mappings, actions and other evidence without revalidating them.
The selection comes from an already completed comparison; recovery neither repeats it nor rediscovers a
failed universe. Groups requiring unresolved semantic review stay outside the selected work.

Planning checks only selected membership and generations in the compact lineage index. Execution finishes the
owning publication journal before authorizing new policy work, accepts the compact selection once, and checks
the selected pricing generation under group locks. Changed bindings fail before loading prices. The selected
current-revision decisions enter complete-history evaluation through the ordinary producer and publication
journal, without metadata acquisition, pricing comparison, payload authentication or pricing writes. A newly
required pricing correction remains a blocker; this command cannot acquire it.

The publisher places an optional version-1 `retained_recovery` completion envelope in each selected receipt
and its compact index projection. Its `operation_id` binds the selection's store, policy, members and exact
input receipt/pricing generations. It certifies only completion of that recovery, never generic continuity or
unchanged numerical ownership. Completed groups skip receipt, payload and calculation work on retry. Genuine
later evaluations replace that envelope; changed generations or membership cannot inherit its applicability.
Independent completed groups survive peer failure, and journal interruption retains finished calculations.
Selected serving maintenance follows successful lineage completion; its ownership and numerical gates remain
unchanged. A successful lineage calculation alone does not establish online availability.

The same recovery entrypoint also accepts a version-1 `alpaca_retained_candidate_handoff` after its semantic
review completes. It binds the absolute store, complete members, original receipt/pricing generations,
production policy and computation revision, and carries baseline receipts, completed candidate receipts,
assignment boundaries and their producer-built projections. Its `review` declares version 1, status `approved`
and a nonempty `conclusion`; unresolved review stays outside publication. The saved comparison must declare
completed candidate assignments and no required mapping correction. These are consumed producer guarantees,
not instructions to repeat comparison, numerical selection, payload authentication or assignment construction.

Recovery checks current compact receipt/membership bindings and selects pricing generations once under the
normal locks. It finalizes only group/workflow bookkeeping and the existing recovery completion envelope,
then sends the completed artifacts directly to the ordinary journal. Journal output and accepted choice
capabilities carry into serving without receipt/projection rereads or repeated schema calculations. A changed
generation, effective mapping, membership or policy rejects the candidate; a missing prepared artifact fails
into explicit owning preparation rather than silently recalculating. Serving still owns validation of newly
selected numerical contributions, and export consumes its completed catalogue normally.

Unchanged candidate boundaries retain their accepted references and already-read bodies; preparation does
not reconstruct them. A prepared numerical source revision may additionally carry version-1
`selected_observation_bundles`. Explicit view slots select shared physical parts, bound to the revision's
exact sessions and chosen sources. Ingress accepts that transport once and retains its immutable aliases.
Publication carries those selected values into serving rather than opening their pricing payloads again.

Completed accepted-publication research can enter a separate explicit admission through
[research_admission.py](../libs/alpaca_lineage_build/research_admission.py). Preparation consumes separately
selected baseline receipts, the completed candidate, and the accepted canonical ruleset. It establishes
equivalent executable rules, exact scoped facts and date intent, complete assignments and no outstanding
mapping correction once. Approval consumes that exact review scope; it does not rerun preparation. The
original research remains unpublishable and unchanged. Newly admitted receipts record its origin and the
separate approval, while the normal prepared publisher and journal install the selected result. Only changed
assignment selectors are written; unchanged legacy bodies and omitted optional equality remain as supplied.
The operation token binds this specific approved candidate. A changed selected application, producer,
receipt or pricing generation stops publication; unrelated factual scopes do not invalidate the admission.

The [application-inventory owner](../libs/alpaca_lineage_build/application_inventory.py) separately repairs
missing application bookkeeping from an already accepted peer marker in the exact selected input component.
It cannot bind new facts or select owners or prices. Its journal mode permits changes only to
`evidence.sourced_fact_applications`, preserves summary/notices/checks, and carries unchanged serving decisions.
Its explicit metadata-completion handoff admits only sourced-fact or lineage-evidence jobs without pricing,
history-check or correction requirements. That job can complete without advancing a retired source's pricing
coverage. Exact obligations, required lookups and group/generation bindings still apply. Actual pricing work
remains outstanding and cannot inherit this metadata-only completion.

The [correction-binding owner](../libs/alpaca_lineage_build/correction_binding.py) can carry completed
ownership and numerical source choices through a narrowly supported latest-session count correction.
It consumes the durable comparison selection, published correction and producer-held corrected rows,
generation-bound current-month assignments and the complete component's accepted choices. Only positive
volume/trade-count revisions with identical timestamps, OHLC and VWAP qualify. The recorded as-of UUID or
terminal-continuity decision must retain its applicable predicates, and no same-identity contributor may
compete on that date. Missing handoffs, changed mapping/coverage, unsupported rules and value-bound numerical
capabilities fail into their owning work. Preparation performs no storage I/O or ownership/numerical replay.
Its completed plan preserves assignment bodies and retargets only pricing bindings; publication checks the
named generations under group locks. Changed values remain new serving inputs on the corrected date.

## Standalone artifacts

Paths are relative to the standalone store:

| Artifact | Authority |
|---|---|
| `store.manifest.json` | Store format. |
| `end_of_day/pricing/<TICKER>/source.manifest.json` | Pricing generation, boundaries, payload map. |
| Adjacent `validator_first_last_sessions.json` | Source-bound exact first/last rows for every view. |
| Adjacent `correction.complete.json` | Atomic equal-price correction completion, bound to the exact correction requirement ID and unchanged source generation; read only for selected correction recovery. |
| Adjacent `correction.observations.json` | Owning recovery of an unaccepted replacement's changed `all` witness; records the old and newer exact observations under the source generation, correction ID and request binding. |
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

Pricing format 32 publishes the comparison tail; format 31 remains readable in place and obtains the missing tail
through the selected metadata upgrade below. Supported sources may select older unchanged boundary receipts;
versions need not match. Compatibility requires no price rewrite or relabeling; unknown future formats
fail closed. [Runtime versus publication versions](../VERSION_TRACKER.md#compatibility) also preserves supported
lineage formats and applicable accepted policy identity; runtime repairs alone do not invalidate completed decisions.

Lineage format 35 requires `current_source_ownership`, either null or the completed version-1 bounded current-source
certificate. The enclosing receipt binds it to selected pricing, effective request context and accepted group
evidence. Receipt ingress accepts its envelope once. Compatible continuation carries it through the existing
in-memory handoff, including a peer's unresolved-history fallback; consumers do not repeat its source-grid,
chronology or activity proof. Missing or incomplete required handoffs select owning recovery. The
[lineage rules](lineage.md#ownership-evidence-limits) define initial eligibility and invalidation; no additional
storage artifact or upstream acquisition is introduced.

Lineage format 36 additionally requires `sourced_rename_membership` and `sourced_rename_ownership`. The former
is null or a version-1 completed group result containing certificates and explicit issues; empty results remain
completed handoffs. The latter is null or a version-1 source scope naming its symbol, observed first/last dates
and a certificate's `action_id`. It references the membership handoff rather than duplicating that certificate.
Receipt ingress validates these envelopes once. Metadata applicability governs membership independently of
pricing ancestry; source applicability governs row ownership. Missing or incompatible required handoffs fail
closed into owning recovery. [Sourced event facts](lineage.md#sourced-event-facts) own their evidence gates.

Lineage format 37 also requires `public_common_continuation_membership`,
`public_common_continuation_ownership`, `security_relations` and `membership_only_rebind`.
The membership result and mapped-source scope are independently completed version-1 handoffs.
Relations are a completed non-union projection with nullable provider/public endpoint IDs.
The rebind is null or an exact old-receipt/pricing/assignment binding and unchanged numerical source/view/row/date
selection transferred to an accepted public ID. Version 1 preserves labels; version 3 records independent
`old_labels` and current `labels` for a primary dated metadata refinement, including an unchanged public ID.
Changed numerical selections cannot claim that unchanged-run guarantee. Ingress checks complete envelopes and
references once, without replaying proofs. An approved append retains the original donor generations until
serving consumes them; rollover projects their positions into current partitions from accepted assignment
metadata. A correction intersecting a donor or a genuinely changed selection requires owning transfer recovery
and cannot silently trigger historical validation.
The first selected receipt ingress retains its accepted body for the operation under its actual index
generation. Later readers consume that schema acceptance without reopening the same selected body.

Closed-source format-37 publications explicitly carry both earlier source-ownership fields, including null
where their legacy publication asserted no certificate, and mark the completed lift with
`closed_source_handoff_version: 1`. Ordinary-update recovery can repair the known earlier lift omission only
in pending serving groups selected by exact incorporated rule applications. The owner rejects missing other
required envelopes, malformed present certificates and indications of a missing newly calculated source proof.
It journals only the missing null fields and lift marker, preserving assignments, membership, pricing references
and donor-generation references. The metadata-only journal leaves pricing summaries, notices and checks intact.
Consumers remain strict; recovery does not reconstruct history or repeat continuity certification.

New sourced metadata closes its dependency group before payload loading. `ClosedAcceptedSource` carries true
accepted counts, generations and request context and has no pricing-reader interface. Membership-only work
assembles selected assignment projections; changed row work and newly exposed numerical contests name their
source/date scopes separately. Numerical counterparts retain their accepted owners. A no-new-row update still
publishes changed membership and relation projections through the existing journal.

Version-3/4/5 documented public-security facts can select first ownership for unresolved positions while preserving
explicit numerical choices. The versioned primary chronology can also refine dated metadata within its continuing
public component, as described in the lineage guide. The metadata-only binding produces a version-1
`documented_identity_membership` with owners and source routes. Receipt ingress accepts its envelope once;
ordinary continuation borrows it only under applicable evidence and mappings. Missing or unknown declared
handoffs require owning recovery, never a new implicit historical audit. Newly owned positions still require
their numerical source decision. Version-1 `security_description` inside a dated ownership interval supplies
primary name, exchange, class, instrument and registrant metadata. Serving consumes its existing descriptive
fields without replaying primary documents or exact provider lookups. The [lineage guide](lineage.md#sourced-event-facts)
owns the semantic conditions and limits.
The source-work producer can compose that chronology with a separately completed public-common exchange.
Accepted continuity certificates and existing containing component IDs are consumed without re-proving them.
Fully superseded source histories can publish a metadata-only projection when no interior timestamp lookup
is required; unknown numerical states still select owning selection.

Regular-way dated-market facts select a separate metadata projection when accepted pricing, owners and
non-market evidence still apply. Exact observed CUSIPs bind existing public IDs before any assignment body
is opened; only sources contributing those IDs need assignment assembly. Historical labels and their dated
provenance change while ownership, row/date positions and explicit numerical choices remain accepted.
Whole segments need no pricing reads. A genuinely interior cut selects its exact stored timestamp assembly.
`dated_market_projection_version: 1` declares a complete `dated_market_projection`, bound to the prior
receipt, pricing generation and boundary map. Its `selection_unchanged` result and dated identity episodes
allow serving to retain numerical outcomes and project labels from accepted session dates. An unmarked
legacy checkpoint may assemble only its selected raw dates after those bindings apply; a declared projection
missing dates requires owning recovery. This path does not authorize numerical reselection or UUID unions.

`numerical_source_revision_version: 1` declares the separate bounded source-choice result. It binds prior
receipt/pricing generations, exact changed sessions, selected sources, contributing-source membership and
the preserved session grid. All outside choices remain accepted. Its `numeric_reuse_sessions` carries the
completed complete-bundle equality result into serving; those values require no new numerical checks.
Serving validates only differing newly selected bundles, then combines those outcomes with the accepted
history. Interrupted installation uses the frozen journal; unfinished serving may assemble only the declared
selected dates. Missing scoped negative-outcome or date capabilities select owning recovery and do not
authorize a full-history audit. The regular source and historical-label consumer gates remain intact. A completed unpublished market
projection may supply labels and unchanged artifacts to a later adjustment refinement. Only its invalidated
numerical scope is selected again; final revisions compare against the actual published baseline. Returning
to those accepted source choices produces a label-only handoff and no new numerical checks.

Ordinary source work separately publishes `numerical_source_equivalence_version: 1` and its
`numerical_source_equivalence` projection when the original numerical selection establishes an equal
eight-view bundle with an explicitly selected retained donor. Each decision retains its original prior
receipt and pricing generations, the observed pricing generations, exact dates, selected source and donor
public IDs/sources. Ownership alone, unknown flags and partial bundles cannot supply numerical approval.
Serving consumes an applicable donor's completed clean numerical outcome and accepted session dates;
it does not compare prices or recheck equal bars. Missing declared date or scoped negative-outcome
capabilities require owning recovery. New contests retire only their reopened dates; disjoint completed
decisions retain their original bindings. Schema acceptance freezes the projection once; later consumers
and metadata publications borrow it. Construction/publication interruption retains the completed result.
Equality metadata is selected only for fresh serving dates of the current identity. A completed numerical
prefix requires no unused legacy donor-date capability. Within the operation, serving also retains completed
input/prefix preparation, localized revision validation and assembled histories under the exact selected
receipt/pricing generations, prior serving decisions, policy and rebuild scope. A downstream failure consumes
those completed stages on retry; changed bindings select their owning work.
The operation also lends already decoded immutable pricing partitions under store, source, boundary kind
and boundary generation. Readers select the current source under their lock and consume matching held
partitions before manifest/payload access. An actual changed boundary selects its own assembly; loss of
operation memory permits unfinished selected data assembly and does not revoke durable equality decisions.

Serving composes complete disjoint numerical donors under the new ID, including negative and partial
capabilities, without rechecking old OHLC. A complete surviving sole-source numerical selection can retain its
checkpoint when dated labels change. Raw date projections do not assert alignment for an `incompatible_sessions`
donor. When a whole legacy donor lacks both date-projection fields, the serving owner first binds its exact
generations and exhaustive old/current row-and-label transfer, then assembles timestamps from only that donor's
selected raw positions. It validates and publishes the new date projection while retaining accepted numerical
outcomes and per-view coverage; subsequent consumers use the completed projection. Newly recovered history may
extend before that accepted span, including alongside an append, only while exact old selections and labels
remain unchanged. This applies both to an unchanged public ID and an accepted membership rebind. The reader
assembles positions outside the accepted span; serving checks only their bars,
calendar intervals and new seams. It combines dates, labels and scoped outcomes with the accepted middle.
An interior change or changed donor binding requires its own bounded recovery and cannot silently reopen the
accepted history. A declared current projection
with missing dates remains an error. A changed donor subrange lacking applicable numerical/date outcomes stays explicitly pending with its
affected scope; accepted donor records and unrelated work survive. There is currently no arbitrary-run recovery
command for that missing capability. A whole-history catalogue rebuild is not an implicit remedy.

New serving checkpoints borrow explicit lineage choice projections under
`history_validation.observation_choice_handoffs` version 1, bound to each original receipt and pricing
generation. Selected membership-transfer recovery consumes those states independently of serving ownership;
selected, superseded, conflicted and incomplete states survive. A missing or changed capability selects owning
retained-lineage reconciliation before payload access or publication. It never infers a choice from a non-null
owner. A separate unknown state requires owning selection.

Membership transfers are produced after final observation selection for retained and materialized sources alike.
Only actual retained positions and labels advertise a donor; a changed identity with no selected contribution
emits no numerical transfer. Selected ordinary-update recovery corrects earlier transfer omissions from the
completed serving checkpoint without changing assignments or prices. Transfer version 2 binds the old selection
to that checkpoint's decision generation and original lineage/pricing generations; it does not invent a lost
old assignment-boundary map. Version 1 retains its original boundary binding. Both envelopes feed the same
unchanged consumer acceptance gates. Recovery restores accepted disjoint source selections before constructing transfers. It may change only
observation-selection flags and their accepted labels in selected assignment metadata; row ownership, positions,
dates and pricing remain fixed. The journal carries those selection projections and transfer metadata without
invalidating prices. Retained and materialized normal paths protect applicable accepted selections before a new
observation contest. Suppressing a newly supported duplicate requires both the target source's completed
common-source grid proof and the donor checkpoint's applicable complete-grid approval; equal range endpoints
and row counts alone establish no date-grid equivalence. Unknown grids and uncovered observations retain
ordinary selected work, and recovery fails closed rather than discarding them. Genuinely overlapping donors and changed owner/mapping inputs retain the existing owning
reconciliation. Selected receipts, assignments and checkpoint bodies carry through the operation, and a compact
completion marker skips completed recovery.

Append partition assembly consumes pricing's already-read actual changed-session notice. An intervening weekend
at month/year rollover does not select an older pricing partition with no new rows. Correction and projection
dates retain their own selected scopes; no calendar reconstruction or historical price audit establishes them.

Newly produced lineage assignments carry version-1 `assignment_handoffs` in their selecting receipt. Each
handoff binds one boundary generation, canonical comparison facts, a selected adjacent `.projection.json`
artifact with shared segment lists for equal views, and compact completed ownership runs. Historical segment
bodies stay outside the metadata receipt. `assignment_handoff_kinds` names required handoffs. Missing, incomplete or mismatched
required handoffs select owning lineage recovery, never reconstruction from historical bodies. Legacy
references without this marker remain readable through their selected assignment bodies; ordinary work does
not migrate an unselected store. New publications carry prior handoffs unchanged and produce new ones only
for newly constructed boundaries, through the existing journal. Full reconciliation compares newly calculated
canonical output to the accepted comparison facts, preserving identical cold references without reopening
old bodies. Current-year comparison excludes its active-month overlap; completed-years comparison ignores
only the anchor label. Serving consumes the shared assignment projection and `serving_ownership` runs instead
of reading and scanning the old boundary documents. These are representation changes, not new ownership rules.

Newly constructed handoffs additionally declare `observation_choices_version: 1` and carry boundary-local
`observation_choices`. Shared view slots select explicit choice runs with owner, state, labels, row bounds,
dates and timestamps. These describe the full boundary, including the current-year active-month overlap that
serving ownership omits. Superseded, conflicted and incomplete choices retain their recorded owner; missing
legacy choice flags remain unknown. An unlabelled observation has an empty label set. This source-local
projection does not certify complete-group coherence or turn ownership into numerical selection.
The producer constructs its immutable runtime capability in the existing assignment traversal. Receipt
ingress accepts each serialized unique run's own schema once and carries that capability through caches,
copies and recovery; consumers check its declaration and selected generation without repeating validation.
Unmarked legacy handoffs remain readable without acquiring the capability implicitly. Missing declared
metadata selects owning receipt recovery. Journal recovery installs completed output without reprojection.

Explicit `lineage migrate-assignments` assembles missing projections for selected complete groups from their
accepted assignment bodies, through the existing journal. It opens each missing selected body once, without
pricing payloads, provider requests, rule replay or numerical reevaluation. The compact index records completed
projection coverage so retries skip migrated groups before receipt reads. Generation checks under selected locks
reject changed inputs; recovery consumes staged projections. Its serving handoff preserves decisions across the
representation change. Projection-only journals leave pricing summaries, notices and pending checks untouched,
including recovery. The group limit applies before receipt/body reads. Selected compact policy is checked before
receipt assembly; the owning serving queue upgrade runs once when publication is needed. Completed peers retain
their compact index entries without projection or digest recalculation. An already-projected legacy receipt missing only the compact completion marker publishes that marker once without rebuilding projections. Ordinary updates do not invoke this migration.

[load_pricing](../libs/alpaca_lineage_build/pricing.py) supplies disposition with
[PublishedPricingEvidence](../libs/alpaca_pricing_download/evidence.py): generation, symbol, request scope and exact
counts for eight views, including zero/retained rows when correlation reads only an added range.
[classify_symbol_disposition](../libs/alpaca_security_lineage/symbol_disposition.py) checks that handoff's schema/scope,
without payload/sidecar reads or repeated authentication. Invalid handoffs stay unresolved. Completed lineage
evidence remains authoritative; selected evidence refresh produces the corrected projection.
The same in-memory input declares `complete_history`, `retained_suffix` or `active_partition` from its loader
request; retained descriptor counts do not imply that their rows were materialized. Once the ordinary lineage
selector chooses complete-group work, it upgrades every partial selected member under the held source generation,
sharing already decoded partitions, and requires `complete_history` before correlation or publication. Prefix
assignment/report authority is confined to retained-suffix work. This does not alter the explicit
`ClosedAcceptedSource` contract or authorize another pricing acceptance check, provider request or payload audit.

## Pricing acquisition and continuation

Retained pricing compares the first stored fully adjusted (`all`) row and the latest up to ten stored `all` rows,
including the latest session. Ordinary downloads, retained dependencies and daily-summary members share this
boundary. The first-session request covers the full New York session, capped at accepted coverage. The second requests the selected tail in one
window; overlapping first/tail dates are requested and compared once. A one-session history needs one request.
Comparison is exact across timestamp, OHLC, volume, trade count and VWAP, with the existing numerical equality.

| Completed comparison | Selected work |
|---|---|
| All selected rows match | Retain history and acquire missing sessions. |
| Only the latest stored session differs | Correct that session and acquire missing sessions. |
| Any earlier tail row or the first row differs | Replace the full retained span. |
| Required observations are missing or incomplete | Preserve completed peer decisions and resolve the incomplete comparison. |

For a one-session history, latest correction and complete replacement cover the same row. Matching observations
approve the selected source generation; other historical views, overlap and old first-session confirmations
are forbidden. Revisions outside these selected dates can remain undetected; a historical audit is separate work.
Replacement notices include removed sessions. Authorities: [comparison](../libs/alpaca_pricing_download/endpoints.py),
[download_pricing / publish_acquired](../libs/alpaca_pricing_download/download.py),
[HTTP execution](../libs/alpaca_pricing_download/http.py), [completed responses](../libs/alpaca_pricing_download/responses.py).

The pricing validator owns the first row and ordered `tail_all`, containing ten rows or the complete shorter
history. Initial acquisition, append, correction, replacement and rollover publish it with the selected prices;
the existing journal also owns summary completion. Live observations never replace the stored baseline before
their corresponding prices publish. A pending publication remains explicitly unreadable until its owner recovers.

[Legacy transition](../libs/alpaca_pricing_download/history_tail.py) recognizes a supported old publication that
lacks the tail. Its owner selects manifest-referenced `all` payloads newest first and uses `readall_rsplit`, excluding
the CSV header and current-year rows superseded by the current month. It opens older year/completed-year payloads
only as needed for ten effective rows. This assembles accepted data without authenticating it again. The owner
journals a metadata-only publication before provider work; retry uses that completed tail. A missing/inconsistent
tail in a completed new format selects owning recovery, never another gzip reconstruction. Planning exposes the
selected upgrades and their bounded reads before execution. Their unchanged-price ancestry preserves earlier work.

Localized correction has explicit replacement semantics for the affected session in all eight views. It carries
the completed live `all` observation into adaptive acquisition and obtains only the remaining observations that
policy requires. Correction and append share assembly/publication when they affect the same partition. Rewriting
the containing gzip is selected output construction; it is not a full-history provider request. Unaffected payload
references remain retained. The source records correction cause, prior generation, preceding session and whether
exact timestamp membership was preserved, alongside truthful ancestry beginning at the corrected session.

When lineage materializes corrected partitions before a subsequent append, its transient handoff carries
the decoded rows and their already-read publication documents. Append and later lineage assembly consume
matching store, partition and boundary-generation records without reopening their manifests or payloads.
A changed selected boundary needs its own assembly; an unchanged boundary remains reusable across source
envelope changes and rollover. These values cross the ordinary JSON transport and are not retained separately.
Normal lineage publication also carries its completed receipt and newly produced assignment bodies to serving,
under their exact selected generations, using the same handoff as publication recovery.
A correction completion carried from an earlier operation does not by itself complete a newly requested advance
beyond retained coverage. That advance receives its required selected comparison, consuming the earlier disposition
only when the new witnesses support it; a changed selected row takes its newly selected correction scope.
Producer completion obtained in the current operation remains authoritative for its accepted retained span.

New-range acquisition uses the `alpaca_adjustment_equivalence_v2` empirical policy in
[adjustment_policy.py](../libs/alpaca_pricing_download/adjustment_policy.py). It requests `raw`, then `all`.
Equality of their complete newly acquired payloads selects the all-equal pattern after two views. Otherwise,
`dividend`, then the required split/spin-off observations distinguish the recorded four- and five-view patterns.
When `dividend` equals `all`, a direct `split` observation completes the four-view branch even if it differs
from `raw` or coincides with `all`. The remaining aliases are `spin-off` from `raw`, `split,spin-off` from
`split`, and both dividend combination views from the equal `dividend`/`all` observations. Direct responses
retain their exact values, including precision differences; the policy applies no rounding or tolerance.
These aliases remain empirical assumptions and do not establish the economic cause of a difference.
Unrecognized observed patterns fetch the remaining views; contradictory empty/nonempty responses
also require all eight direct observations. Counts describe view acquisitions, excluding pagination, retries
and producer confirmations. The policy infers unrequested views from its empirical patterns; it does not prove
the absence of an anomaly confined to an unrequested view. Both `raw` and `all` are always direct observations.

[Adaptive acquisition](../libs/alpaca_pricing_download/adaptive.py) applies to new tickers, selected replacements
and missing-session appends, as well as localized corrections seeded with the accepted live `all` observation.
Each response-dependent stage batches only its selected symbols at their exact
requested ranges. Completed pagination precedes the next decision. Successful observations survive an exact
transport fallback, and completed decisions cross into publication without rehashing, renormalizing or replaying
the policy. A fresh scope makes its own decision; old payloads and endpoint approvals are not equivalence inputs.
All eight logical views remain available through the existing publication representation. The source's optional
`latest_adjustment_acquisition` records the latest range/mapping, policy, observed hashes and inferred aliases;
an exact selected-session correction records its bounded range proofs together. This is provenance for that
acquisition, not authorization to infer later ranges or reinterpret retained partitions.
Earlier acquisition policy provenance remains readable and does not invalidate completed retained pricing.
New full histories and selected replacements confirm only directly acquired views at their first session.
Completed trigger observations supply confirmation for the same adjustment/session; those observations are not
requested again. Full replacement compares its new output against carried live `all` rows before accepting
publication. A present differing `all` witness selected by the retained-boundary comparison is a request-context
contradiction. It completes without a new pricing generation only when the exact full-range adaptive acquisition
produces all three replacement boundaries logically identical to the accepted generation. That decision uses the
accepted boundary digests already needed by the publisher; it performs no additional provider or payload
verification. An unequal replacement with a unique present conflicting `all` witness can resolve it only through
a newer exact-session response at the same mapping that uniquely agrees with the acquired output. The producer
records both observations and their request binding atomically before publication. Retry consumes this saved
supersession without repeating its completed resolving request. This resolves unaccepted output; it does not
repeat an accepted history comparison or expire evidence by time. Missing or ambiguous sessions, conflicting
original witnesses for one session, other-view conflicts, a still-old response or a third value remain pending.
Reconciliation accounts for every accumulated witness before either disposition. Consumers retain that completed
disposition instead of repeating reconciliation. The accepted workflow boundary carries completed equal-source
correction contracts and their completion receipts across fresh JSON stages. Identical newly selected witnesses
consume that completed disposition without a new correction ID, full-range acquisition, payload reads or digest
calculations. Completed correction evidence is not numerically revalidated at workflow ingress, including after
publication but before acknowledgement. New work preserves those existing fields; append producers retarget the
completion receipt only across their certified unchanged prefix. A later comparison composes the handled prefix
with its freshly checked tail. The comparison producer carries added-session evidence so consumers do not repeat
membership calculations. Changed witnesses outside the accepted prefix, new session membership, incompatible
mapping, store or unaccepted generation retain owning correction work. No new persistence format is introduced.

Positive raw late-bar observations take a separate correction path. Consecutive selected sessions are acquired at
their exact ranges and mapping with the accepted raw response as the adaptive first stage, so raw is not requested
again. Publication requires a unique row for every selected session in every adjustment view, rejects extra or
missing sessions and conflicts with retained rows, and merges only absent selected sessions under the selected-
generation lock. It never requests an enclosing unselected gap, performs a broad replacement or splices raw rows
without their complete adjustment bundle. Only affected partitions are assembled, with decoded data shared within the operation; unaffected boundary references stay closed. Accepted endpoint and comparison-tail facts plus inserted rows produce the new compact boundary. New rows are validated once; assembling a partition does not authorize revalidating its retained rows.
The existing one bounded exact acquisition retry for a view omitted from a nonempty replacement remains available.
It does not repeat the original comparison. A still-conflicting result leaves publication unchanged.
Approved appends retain their existing no-historical-confirmation rule. Debug emits the completed selection;
stage progress counts each HTTP page once. Planning minima assume two views while action/dense-history estimates
budget eight. A local correction already holds its accepted `all` observation, leaving one to seven direct views;
the actual dependent plans report their selected work as responses arrive.

A lagging retained dependency first runs that adaptive producer on only its missing suffix. A completed empty
suffix selects no acquisition: retained endpoints, payloads and coverage stay untouched, and no historical
comparison is performed. A completed nonempty suffix selects ordinary dependency acquisition and crosses that
boundary in memory; after the mandatory generation-bound first/tail `all` comparison, acquisition consumes the
same adjustment responses, hashes and decision without requesting or calculating them again. Selection failure
proves no absence and therefore follows the ordinary fail-closed acquisition path. Successful partial responses
and completed suffix acquisitions remain bound to source generation, mapping and exact request scope in the
operation context across subsequent boundary failures; retries request only unfinished observations. Metadata activity or inactivity
does not substitute for this pricing-owned selection.

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
Accepted summary entries retain their row/tail acceptance and per-target append requests through planning,
source selection and execution. A new summary entry or target selects a new request; consumers do not repeat
validation or compilation for the unchanged entry and target. Completed adjustment acquisitions retain their
endpoint and extent projections through publication. Lineage carries its locked source generation and completed
correction-scope decision, including a negative decision, into payload loading and broader reconciliation.
Only after successful full batch pagination do omitted symbols complete empty at the transport boundary.
Ordinary acquisition consumes that completed response without individual rechecks. The history-comparison owner
has additional evidence: its compact baseline names an already retained endpoint. Provider batch companions can suppress
that bar, so a completed omission triggers one singleton confirmation action with the identical session, adjustment
and mapping date. Returned endpoints are retained; failed batches are not interpreted as absence. A request
already executed singly, including a 414 subdivision, is not repeated. Confirmations exhaust pagination through
the same limiter/retry policy; failed confirmations remain failures and completed empty confirmations remain
missing. A missing selected endpoint may use an alternative dated provider label only when the selected,
source-generation-bound lineage receipt already publishes its positive UUID membership and same-CUSIP rename
observations tied to that identity's UUID. Historical mapping uses the endpoint's date and requests only that
session in `all`; other acquired endpoints remain held. Unknown membership, unlike exchanges, conflicting routes
and changed source bindings select no alternate request. Only missing selected sources need receipt bodies;
carried receipts remain authoritative. This is provider request planning from accepted facts, not a new identity
calculation. Backward routing stops at each earlier rename; later reuse of that label cannot redirect the old identity.
This request choice does not allocate rows or require already allocated ownership of the missing date.
Recovered observations enter the original single comparison. Missing alternate observations stay
incomplete and route errors preserve independent completed peers. Results and debug events identify each missing endpoint; request metrics count probes and recovered
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
A source's consecutive selected exchange sessions share a range, including weekend/holiday seams. Unselected
market sessions break the range; symbol batching requires identical ranges and mapping dates. The generic
enclosing-range merger is disabled; returned rows are routed only to the exact selected sessions. Completed
positive actions are persisted before the next action starts. Independent completed negatives survive peer
failures within the operation and source/mapping/range binding; unfinished pagination yields no observation.
Each source run is capped at 10,000 sessions. Planning and execution share mapped ranges; the exact-session
override preserves the selection and freshness policy.
A confirmed late provider row enters selected-session adaptive acquisition and complete-group lineage. A previously required full correction retains its historical cause, scope and accumulated observations. Orchestration does not predict eligibility. Stale pricing or serving generations require
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

Format-4 entries contain `symbol`, saved mapping `asof`, completed source `end_exclusive`, `source_sha256`, exact
eight-field `first_all`/`last_all`, and `tail_all`/`tail_version` from the pricing producer. The source generation
belongs to pricing; a lineage index awaiting acknowledgement cannot substitute its older generation.
Eligibility: unique stored same-symbol active US-equity lookup and nonempty `all` endpoint; stored activity is not
online availability. Endpoint-session/asof pairs group checks, with identical windows requested once. Each row defines its session window; `asof` independently
selects provider identity; completed exclusive end independently starts append. Epoch/revision/coverage and
separate owner-pending markers control publication. No identities, other views or saved live comparisons.

Format 3 already owns membership and endpoints. It remains readable for selecting a controlled legacy-tail
upgrade; the writer adds the new columns transactionally and projects each completed pricing upgrade. This does
not discover ticker directories or rebuild eligibility. New-format missing tails/generations fail closed.

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
requirements and retry state. Persist dependency/correction requirements before requests.

An explicit scoped work-state read selects symbol keys in SQL before decoding bodies and may exclude completed
work. Its partial view is suitable only for a caller's already bounded complete groups; it is not a global
work census or independent evidence of reverse pending relationships. Registration intents and state revision
remain compact global authorities. Ordinary unscoped registration retains its full work selection.

Lineage stamps only satisfied locked inputs: coverage cannot erase unperformed refresh/correction or failed checks. Completed
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

Known dependency membership is registered before its missing-suffix selector request. That completed selector is
the authority for whether acquisition work exists: empty results create no acquisition obligation; positive or
unproved results persist the selected member with its complete group before any retained-history check or pricing
publication. Thus durable state precedes owned work without turning a negative selector into false acquisition.

Ordinary eligible summary members complete that sequence before discovery advances their dependencies.
Selection's changed-identity and lost-eligibility facts reconcile before those members enter maintenance.
Later phases retain each completed comparison even when an independent member fails. A comparison is bound to
its summary entry and pricing generation; reuse requires the same accepted selectors and current allowed scope.
Matching comparisons and differing comparisons are distinct from append approval. The completed decision selects
unchanged, latest-session correction or full replacement through the existing pricing producer.
Append approval additionally binds the current correction
obligation, so a renewed correction cannot inherit it. Successful corrections and appends retain their approvals
once lineage accepts their exact generations, even if a peer fails; incomplete lineage cannot lend that approval.
Refresh passes its completed independent subset onward even when its aggregate result is incomplete. A selected
legacy-tail metadata publication also finishes its lineage generation handoff without repeating tail assembly or
price comparisons. Failed or restricted dependency groups remain excluded, and successful peers can complete
workflow acknowledgement without reopening their completed lineage decisions.
Missing-session completion approves only its selected sessions. Without an applicable completed retained-history comparison, daily first/tail checking remains required. When insertion preserves every retained observation and the effective mapping, its producer handoff carries an existing source-bound approval into the new generation without repeating the comparison. A genuinely changed interior observation invalidates earlier comparisons even if endpoints are identical. A successful full pricing acquisition also returns a distinct immutable operation-local publication acceptance,
constructed from its held endpoints after installation. Initial, replacement and equal-price full acquisitions
retain their own completed guarantee; they do not manufacture a retained-history comparison. Subsequent
maintenance consumes it only while both compact lineage and summary select its resulting generation, mapping,
coverage and endpoint facts and the current workflow's acquisition/correction requirements still match.
No provider request, validator read, payload read or repeated endpoint calculation establishes that acceptance
again. Incomplete publications, covered histories, ordinary appends and selected-session corrections do not
produce this full-acquisition handoff. An approved dependency append carries a separate acceptance containing
its completed predecessor comparison and the resulting generation's first/tail facts, after the new rows
publish. A mapping change selects one new comparison; its successful append cannot select that comparison
again merely because the publication generation changed. Both producer paths use the same compact applicability
gates. A separate invocation retains fresh live checking.
Each operation-local comparison or successful approval carries its original target, required mapping-evidence
fingerprint and correction ID, bound to that exact cached record. Later bookkeeping carries that scope rather
than restamping it with current requirements. A peer's newer differing comparison cannot expand an older
approval's scope. Producer-derived approvals also require the actual lineage index generation independently of
the pricing summary. Missing required mapping evidence selects owning metadata/lineage recovery before provider
work; another price comparison cannot establish it.

Operation-local peer approvals are not restart
freshness caches. Pending corrections durably retain scope, cause, source generation, mapping, coverage and usable
observations. An applicable pending decision is consumed without comparing again. A new generation invalidates
that decision for full-history as well as latest-session scope; its fresh completed result owns the new scope.
Version-3 contracts accumulate raw and adjusted trigger rows under the same store, symbol, generation, mapping
and retained bounds. A changed binding discards the old scope; changed requirements renew the correction ID.
Numerically equivalent witnesses merge; conflicting duplicates remain constraints. Each witnessed New York
session must have exactly one matching acquired row, including timestamp and values. Version-2 row-bearing
contracts remain readable; this repository does not interpret that version as an external date-only format. Missing
required trigger evidence fails into owning recovery. Equal effective prices with unchanged request/mapping and
identity evidence publish only an atomic correction-completion receipt under the ticker lock. The data generation
and its acquisition provenance stay unchanged. Workflow accepts that source-bound outcome transactionally against the current obligation and correction IDs; interruption
after the atomic replacement cannot repeat acquisition or create lineage/serving generations. A changed generation or correction requirement
invalidates the completion. An actual changed pricing publication retains its journal and embedded completion ID.
Explicit symbol refresh accepts an equal-price completion before workflow acknowledgement. An unfinished
matching requirement consumes its atomic receipt before acquisition planning; a new invocation after complete
acknowledgement creates a new correction requirement. A completed changed-price publication retains its ID
while lineage is pending. A later producer-certified append carries an accepted equal-price correction completion
to its published generation when recorded ancestry proves that every change starts at or after the completed
correction's exclusive end. The same correction ID and unchanged retained span remain mandatory; renewed
requirements, earlier changes and missing ancestry reject that carry. An interruption before workflow
acknowledgement recovers from the already selected source metadata, without reacquiring the completed correction.
Durable observation rows are structurally validated at work-state ingress (supported
views, eight string fields, timezone-aware timestamps, finite numbers and nondecreasing order); conflicting
witnesses remain evidence, not malformed duplicates. Consumers do not repeat that validation. Initialization checks only the compact work-state gate; final pending counts consume status without deserializing or revalidating accepted correction rows.
An old correction ID without sufficient cause information retains full replacement; identity changes and confirmed
missing historical rows retain their explicitly selected owning scope. Started journals finish their selected publication,
and already completed replacements remain completed. Partial successes do not make an overall failed update complete.

Work estimates consume these durable scopes from the accepted compact selectors. Applicable pending comparisons
are excluded from fresh check counts; local corrections, full replacements, metadata-only acknowledgements and
combined correction/append publications are counted separately. Deferred work is excluded. A completion ID known
only to a selected source manifest can reduce that estimate during execution; planning does not open manifests to
resolve it. The debug `update_work_estimate` event records these selections alongside ordinary comparison,
acquisition, publication and serving evidence. Response-dependent differences and group expansion remain conditional;
these counts are neither a throughput prediction nor evidence that the 60/90-minute production target was met.

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
batch, restarting that batch's pagination and discarding its partial pages. Completely normalized physical subsets cross their owning handoff before the next subset starts. Bars and metadata owners preserve positive and negative completed subsets when an independent subset fails; failure attribution names only unfinished deliveries. Dependent steps remain blocked. A routing contradiction in newly assembled corporate-action evidence remains an explicit invalidation, rather than an independent transport failure. Completed omissions
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
waits for exclusive `flock` and holds it throughout. A busy writer makes one nonblocking attempt, reports its wait,
then blocks on the same descriptor until acquisition or interruption, without polling or reading store boundaries.
The internal library defaults to nonblocking acquisition; callers explicitly select waiting. Store gate precedes owner locks. Writers exclude writers/exports;
read-only exports may coexist. Exit, including `SIGKILL`, releases the kernel lock; path remains, with no PID or
stale-lock recovery.
The public [Prepare JSON entrypoint](development.md#pipeline-phase-1-prepare) takes the same exclusive gate
nonblockingly after receiving its complete input, and releases it before delivering output. Its next owner
reacquires the gate and checks named selector applicability; a lock gap does not authorize preparation replay.
Direct internal producer functions require their caller to provide this outer exclusion when used alongside
exports; they are not independent concurrent writer entrypoints. Public consumers use `alpaca_end_of_day`.

Owners close temporary files and atomically replace journaled outputs: payloads before receipts/index, summary
before acknowledgement, journal removal last. Acknowledgement cannot consume newer notices/hints. Pricing hands
accepted source/endpoints downstream, reuses cold references and inventories only replaced boundaries. Recovery
consumes the version-1 pricing installation handoff: the journal is published only after every new output has
passed its producer and its staging file is closed. Installation retains the producer's digests and never rereads
or hashes staged or installed bytes. A missing staged file means its atomic rename already completed under the
stable ticker lock. The journal also carries the accepted source and fully adjusted endpoints/comparison tail,
so summary recovery does not reopen installed source or validator bodies. An already matching summary generation
selects acknowledgement alone. Missing or unknown declared handoffs require owning recovery; they cannot select
legacy reconstruction. Unmarked legacy journals accept each selected staged or installed output once, and parse
an update from those same bytes. Owners clean abandoned staging under ticker locks. Interrupted multi-file publication
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

Ownership decisions follow the [allocation criterion](lineage.md#ownership-allocation-criterion): uncertainty
alone does not establish that withholding history is the most likely result. The current lost-ownership
recovery path publishes a completed offline outcome for affected public IDs. A version-1
`offline_ownership` envelope records the reason, affected UUID-connected identities and accepted prior
checkpoints under their original references. It does not approve the shortened history or merge identities.
The current group publication contains negative decisions for those IDs and retains independently supported
identities, including rights and units. Numerical prefixes for offline IDs are not evaluated. Retried unchanged
work trusts the completed result; a real input change selects owning evaluation and can reuse the frozen
checkpoint without rechecking accepted prices. `catalogue status` exposes `offline_reviews` to operators;
`DataLibrary` exposes only online identities and returns its ordinary unavailable-ID result for offline ones.
This containment preserves recoverable history; it does not establish the best ownership allocation or close
the [remaining policy gap](trust-boundary-handoff.html#f64).

Lineage export intentions are reconciled by maintenance under the lineage worker lock after confirming that
its publication journal is absent. Only compact serving selections matching the current lineage index authorize
acknowledgement. Conditional deletion consumes the exact selected intention, including abandoned pre-journal
intentions, without receipt reads or ownership/numerical replay. Pricing intentions retain their independent
owner gate. Export still consumes its applicability boundary strictly; it does not repair or silently skip a
changed selected input. Offline identities are absent from the published online selection.


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
  rows and makes no new identity links. This describes the current serving gate; it does not establish that
  every withheld allocation is the most likely outcome under the semantic criterion.
- Correlation's exact certified vacant-ticker and pre-entry carry-forward non-observation dispositions are not
  unresolved observations: serving trusts each producer certificate without replaying it. Additional reasons
  retain their ordinary uncertainty meaning. Empty/entirely unresolved groups complete without public identities.

Inactivity, nontradability or missing descriptions alone do not disqualify. Coverage means approved stored history,
not lifetime completeness/current freshness. Policy version/calendar digest select positive and negative decisions.
Names/exchanges/classes come only from own-symbol observations of the identity's UUIDs, not current CUSIP lookup.
Observation timestamps remain observation times; historical effective dates need dated provenance. Omit unknown descriptions.

### Checkpoint reuse

[reuse.py](../libs/alpaca_serving_catalogue/reuse.py) checkpoints generations, ownership runs, contributing-source membership,
numeric outcomes, labels and gap evidence, never copied prices. The version-2 source projection is
`{version: 2, sources: [...]}`. Legacy coverage selectors contribute their accepted source keys without subset or
overlap reconstruction. Ordinary history reads construct no coverage-ranking projection. Reuse is per identity across group merges/splits;
independent identity changes do not invalidate it. Proven appends skip old rows/closed payloads; suffix containers
may contain older rows. Changed adjusted-source selection, ownership, replacement or absent ancestry requires
affected-history evaluation.

A producer-certified latest-session correction can retain the checkpoint before that row. The serving producer
publishes preceding-session raw labels and numerical outcomes during normal evaluation. Current ownership runs
and pricing ancestry bind the shortened checkpoint; changed rows and new rows use the ordinary evaluator. For
multiple contributing sources, preserved producer selections retain accepted source membership;
each source contributing the corrected date supplies that date again. Earlier numeric checks and payloads stay
closed. Genuine changed ownership, mapping or source selection retains the appropriate reconciliation scope.

A legacy checkpoint without preceding-row facts can reuse its completed all-passing numerical outcome. Its raw
boundary can be projected from accepted ownership and session compatibility when one final source supplies the
preceding row and every view of the other contributing sources ends earlier. Overlapping sources do not establish the previous
winner's raw labels merely by having equal coverage. Missing numerical outcomes or ambiguous raw provenance
require explicit catalogue rebuild; maintenance reports that requirement without silently auditing history.
Metadata-only publication preserves a legacy checkpoint's capabilities without adding the revision-prefix
marker when its preceding raw boundary is absent. It reuses accepted prices without reads. A subsequent append
can establish the preceding boundary from the previously published last row and publish the marker normally.

Partial publication retains its full-history numerical checkpoint. New records also carry accepted session
dates; range/count/label projection consumes those dates and completed ticker ranges without reopening prices
or repeating numerical checks. Changed uncertainty can change the supported range independently of numerical
acceptance. A legacy partial checkpoint can retain its already published range across metadata changes and
suffix advancement. When a newly selected cutoff needs a different range whose dates are absent from an
applicable unmarked legacy numerical checkpoint, its owner assembles the accepted raw session dates once and
publishes the completed date projection. Existing exact ranges remain payload-free. Numerical and calendar
outcomes remain accepted; a declared current projection missing its dates still fails.
Earlier-policy checkpoints migrate only when the complete group's pricing
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

A selected old owner becoming unassigned under `outside_supported_name_change_history` cannot silently shorten
its same-ID public history when pricing ancestry preserves those accepted positions. The serving producer
checks that narrow loss from accepted checkpoint runs and current assignment metadata before payload reads,
retains the old checkpoint and publishes completed offline containment with the lost source/row/date scope.
This is the [open ownership review](trust-boundary-handoff.html#f58), not a resolved comparative allocation.
Current receipts lack the
prior mapping/evidence invalidation handoff needed to restore or revoke that historical ownership. Changed
pricing, changed public membership, correlated reallocation and other unresolved causes keep their owning paths.

### Publication and maintenance

[storage.py](../libs/alpaca_serving_catalogue/storage.py) atomically replaces a component's decisions, public fields,
dated matches and ID/group/input references. Incompatible duplicate IDs abort. Selector publishes last: schema,
policy, time, completeness, transaction token; readers never hash the database. Compact selection serves status,
preview, public snapshots and unchanged work; only changed selected groups open checkpoints. Matching contributions
are neither revalidated nor rewritten.

Serving SQLite writers use a 120-second busy timeout for contention with public snapshots. If that wait
expires or publication is interrupted, the transaction rolls back. Pricing and lineage publication journals
preserve their already-built artifacts for recovery. Existing rollback journals and synchronization settings
remain in force.

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
Completion additionally reports `pending_export_inputs` by pricing/lineage owner from one compact aggregate
against current published component membership. An applicable outstanding invalidation keeps maintenance
incomplete even with an empty serving-work queue. A focused request covers every member of its selected
published components; never-served or retired sources do not invalidate an existing export. This completion
fact neither reopens upstream artifacts nor changes catalogue schemas or eligibility decisions.
A new component publication also consumes its held superseded lineage intentions in the same transaction.
Deletion is conditional on the exact selected intention and restricted to the newly published members;
renewed intentions, unrelated components and rolled-back publications retain their obligations.
The worker carries each completed component-applicability comparison into queue consumption.

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
