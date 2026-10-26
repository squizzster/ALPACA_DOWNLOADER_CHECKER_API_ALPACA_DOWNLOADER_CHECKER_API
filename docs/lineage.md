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
Evidence fingerprints are independent of history approval: missing approval republishes the completed decision
through its journal before acknowledgement, without price decoding/correlation. Projected approval needs no group
reopening. Rule-only changes reuse approved evidence.

[policy_engine.py](../libs/alpaca_lineage_build/policy_engine.py), `evaluate_policy`, constructs detailed row traces,
pass digests and audit summaries only when requested. Mandatory phase/certificate checks still run; published
and explicitly traced digest semantics remain unchanged.

## Policy migration

Only the exact completed lineage-33/correlation-59 observation decision may promote to lineage-34/correlation-60
without recalculation. Every selected member must qualify and the authorized YAML must have the pinned,
behavior-identical semantic identity. Proof uses selected receipt metadata, never prices, and is not repeated
once the current handoff is published. Other transitions require selected complete-group recalculation using
retained evidence/prices. Unchanged compatible groups then reuse completed policy-bound decisions.

[Migration](../libs/alpaca_lineage_build/migration.py), `migrate_correlation_policy`, exposes this proof as
`lineage migrate-policy`. Under the exclusive store operation lock it recovers the lineage journal, requires a
complete compact selector and complete empty scheduling hints, then preflights every entry. Each complete group
must retain matching selected pricing generations; mixed/incompatible groups, pending notices and changed sources
fail closed. Publication reuses boundary references; no live orchestration, provider clients, pricing payloads,
assignment-boundary reads, observation selection or rule execution. Reruns are idempotent; interruption rolls
forward through the lineage journal.

Publication records normal serving obligations. After promotion the command reads the compact durable queue:
`serving_catalogue_action: pending_update` requires `catalogue update`; missing/incomplete serving-work coverage
reports `bootstrap_required`. Finish that independent offline stage through
[serving maintenance](operations.md#serving-catalogue-maintenance).

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
symbol/CUSIP agreement; unsupported prefix stays ambiguous. A foreign-label distribution may be ignored for
occupancy only if the same completed group contains its exact own-label action, whose source assigns that session
to the exact UUID in all eight views, with bracketing current-label actions and no related structural action.
Provider evidence is retained; equal prices, returned-bar timing and external research do not establish identity.
Authorities: [occupancy.py](../libs/alpaca_lineage_build/occupancy.py),
[ownership rules](../libs/alpaca_end_of_day_security_correlation/ownership_interval_rules.py).

- **Cash terminal:** a terminal date alone cannot establish unbounded prior ownership across a distinct occupant
  with positive dated evidence on both sides. Existing dated intervals/accepted split continuity remain
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
- **Terminal continuous ticker:** preserve accepted components; a new two-component UUID union additionally needs
  complete informative history without prolonged carry-forward and retained structural-action evidence.
  Three or more disconnected components stay unresolved. Independently certified closed-history departures keep
  narrower rules; ordinary inactive sessions neither create nor invalidate components.
  [terminal_fallback.py](../libs/alpaca_end_of_day_security_correlation/terminal_fallback.py).
- **Mapped predecessor tail:** successor ownership needs an accepted cross-UUID provider edge, as-of inside the
  predecessor interval, unique incoming/outgoing branches and an independently correlated successor source for
  exact sessions. Every field in all eight views must match. Correlation assigns successor rows; observation
  selection marks predecessor copies superseded. Equality alone, incomplete views, reused labels, competing
  branches or differing values leave rows unresolved. [name_change.py](../libs/alpaca_end_of_day_security_correlation/name_change.py).

Corporate-action dates retain provider field semantics and exact supported endpoints. Do not substitute earliest
or next-session dates, bar boundaries, researched marketplace dates or ticker exceptions. Missing/conflicting
dated evidence stays unresolved; external research can audit results but is not lineage input.

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
reuses containing completed action intervals within one invocation only. Failed/incomplete responses prove no
coverage; wider requests/new invocations acquire evidence. Conflicting action IDs block only evidence containing
those IDs, including cached intervals; unrelated symbols in fully acquired/routed batches remain usable.
Transport/routing failure leaves affected requests incomplete. Conflicts remain blocking for the invocation;
no winner is chosen.

Ordinary action continuation starts at the prior active month; older corrections require explicit refresh.
An unchanged action's completed positive CUSIP→ID lookup is reusable only when the fresh catalogue contains that
exact asset ID with identical unambiguous facts. This consumes an accepted mapping, never infers CUSIP from ticker.
New/changed actions, changed/missing/ambiguous asset facts, negative lookups and explicit refresh require exact
acquisition. Replacing an already consumed lookup during traversal restarts metadata assembly from the now-
authoritative exact cache. Known dependencies retain continuation/refresh obligations; new ones need full evidence.
Expanded groups reselect under sorted enlarged locks; decoded reuse requires matching generation/scope.

## Assignment continuation

[resume.py](../libs/alpaca_lineage_build/resume.py), `can_resume`, combines pricing ancestry/change-range proof
with single-identity gates. Compatible resolved assignments, unchanged evidence and a complete added session
range permit correlation of added rows only. Receipt summaries supply accepted counts/endpoints without reopening
closed assignments. Older receipts derive summaries from selected assignment bodies, without price reads or
replaying old decisions.

Month/year rollover combines accepted assignments with new decisions, opening/publishing only required output
bodies. Historical corrections, missing change proof, changed evidence, unresolved assignments and dependency
groups retain complete-group reconciliation.

If pricing proves every group input's rows unchanged and identity evidence is unchanged, carry assignments
forward without pricing reads or rule execution—even unresolved assignments/empty histories. Advancing cutoff or
crossing month/year alone does not invalidate decisions; rollover reads only assignment bodies needing regrouping.

## Missing auxiliary prices

An exactly rejected missing auxiliary may retain metadata and complete its group alongside independently
supported histories, without an invented empty source. This excludes arbitrary failure, seeds and retained
series. [acquisition.py](../libs/alpaca_lineage_build/acquisition.py) records rejected request, evidence and
obligation before acknowledgement; auxiliary-only reselection preserves its group.
