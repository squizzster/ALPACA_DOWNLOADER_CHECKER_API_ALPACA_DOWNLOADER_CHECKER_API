# The single data pipeline

This repository has one production data path. Acquisition, cached replay, focused repair, and future incremental
updates must all converge on the same authenticated inputs, the same correlation builder, the same ordered rule
registries, and the same publication gates. A second correlation builder, migration-only rule engine, or ticker-specific
update path is not permitted.

The canonical complete entrypoint is [`run_alpaca_end_of_day_ticker_pipeline.py`](run_alpaca_end_of_day_ticker_pipeline.py).
[`download_alpaca_end_of_day_bars.py`](download_alpaca_end_of_day_bars.py) is only a compatibility name for that same
orchestrator. Narrow operator commands may inspect or republish one boundary, but they do not own alternative rules.

## One ordered flow

[`run_end_of_day_ticker_pipeline`](libs/alpaca_end_of_day_ticker_pipeline/orchestration.py) owns the stage order:

1. Resolve the submitted label to Alpaca's authoritative US-equity symbol.
2. Acquire or authenticate all eight adjustment views at one shared exclusive request-end boundary.
3. Hard-validate and freeze the source responses.
4. Acquire authenticated asset and corporate-action lineage evidence.
5. Call the sole correlation builder,
   [`build_security_correlation_report`](libs/alpaca_end_of_day_security_correlation/correlation.py).
6. Publish the exhaustive correlated and uncorrelated row partition.
7. Build and publish advisory warning overlays.
8. Publish the affected source contribution provisionally offline.
9. Read every advertised adjustment through the authenticated reader, evaluate availability, and publish final online
   status.
10. Make the new digest-bound master generation visible.

A multi-symbol run may execute independent acquisition work concurrently, but security-master finalization is
serialized. Every selected symbol in one run has the same requested end date; the update contract does not support
multiple per-symbol end dates.

### Lineage acquisition boundary

Stage 4 follows valid old and new symbols named by a corporate action. A structurally complete name change may expose
one further label: when exact HTTP-200 lookups of both endpoint CUSIPs return US equities on a non-OTC exchange, both
assets carry the same Alpaca UUID, and both return the same single valid symbol, that returned symbol is added to the
same lineage search. A CUSIP echoed as a symbol is not a label. The search repeats to a deterministic fixed point.

Missing fields, failed lookups, different UUIDs, different returned symbols, malformed symbols, OTC or non-equity
assets, and the traversal safety limit all fail closed. `active` and `tradable` are deliberately not gates: historical
labels commonly resolve through inactive records. This rule only acquires evidence; the existing ownership and UUID
lineage rules still decide row allocation and security membership.

## The correlation cascade

Rules describe generic evidence shapes. They never name a ticker, silently choose between ambiguous identities, or
rewrite frozen rows. Earlier row rules have precedence over later fallbacks.

### 1. Dated ownership intervals

`OWNERSHIP_INTERVAL_RULES` in
[`ownership_interval_rules.py`](libs/alpaca_end_of_day_security_correlation/ownership_interval_rules.py) is the only
ownership registry, in this order:

1. `name_change_ticker_transition`
2. `same_ticker_stock_merger_uuid_transition`
3. `cash_merger_acquiree_terminal_boundary`

These rules establish dated ticker/CUSIP/UUID intervals. They do not by themselves say that every related UUID is one
security.

### 2. Row allocation

`ROW_CORRELATION_RULES` in
[`row_correlation.py`](libs/alpaca_end_of_day_security_correlation/row_correlation.py) is the only row-decision
registry, in this order:

1. `exchange_test_symbol_exclusion`
2. `complete_current_identity_suffix_after_ticker_reuse_gap`
3. `inactive_current_identity_suffix_after_ticker_reuse_gap`
4. `dated_terminal_identity_suffix_after_ticker_reuse_gap`
5. `ticker_reuse_gap_guard`
6. `one_session_distinct_uuid_transition`
7. `complete_current_symbol_run_after_authenticated_ticker_reuse`
8. `asof_selected_uuid_lineage_interval`
9. `latest_dated_ownership_interval_overlapping_claims`
10. `requested_symbol_dated_uuid_interval`
11. `unique_uuid_candidate_with_warnings`
12. `unresolved_uuid_candidates`
13. `stable_continuous_ticker_identity_set`, the terminal fallback after the registry

The consequential gates are:

- A gap of at least 60 expected sessions is a possible ticker-reuse boundary and blocks generic fallbacks unless a
  stronger earlier rule proves an episode.
- A complete current suffix needs at least 100 missing sessions, identical authenticated adjustment grids, one exact
  current UUID, and no dated evidence placing that UUID on the left.
- An internally complete inactive suffix may end before the request boundary; an active incomplete suffix remains
  unresolved.
- A dated terminal suffix requires an exact CUSIP lookup, one ownership interval, complete coverage to its terminal
  boundary, and no left-side evidence for its UUID.
- A one-session transition requires exact different old/new UUIDs, no competing UUID, a matching action boundary, and
  agreement across all adjustment grids.
- Continuous action-linked UUID episodes remain separate when each has one complete, non-overlapping run in all eight
  views. Recurrence, overlap, a missing view, or ambiguity vetoes the partition.
- Current-symbol reuse requires the old occupant to have renamed away at least 100 sessions earlier, exact current
  identity evidence, complete matching grids, and no competing structural evidence.
- Overlapping intervals for different UUIDs select the one interval with the uniquely latest explicit start date. A
  tied latest date or entirely undated overlap fails closed. Intervals carrying the same UUID are not competing
  security claims, so this rule defers to the existing exact requested-symbol interval rule; multiple exact claims
  for that requested symbol still fail closed. Row routing never joins identities.
- The terminal fallback can promote only a fully unresolved, gapless source with a stable same-ticker candidate set,
  one current lookup anchor, agreeing non-empty names, and no missing or unexpected sessions.

Every authenticated source row appears exactly once in either the correlated or uncorrelated branch.

### 3. Security membership

Row allocation and security membership are separate decisions. UUID-lineage edges in
[`uuid_lineage.py`](libs/alpaca_end_of_day_security_correlation/uuid_lineage.py) connect membership only when their
evidence is exact:

- an authenticated name-change edge;
- an authenticated same-ticker stock-merger edge; or
- an authenticated identity-preserving same-symbol split edge.

A same-symbol forward or reverse split joins the old and new Alpaca UUIDs only when one action supplies a valid date,
positive directionally consistent rates, distinct old and new CUSIPs, no ticker change, and exactly one successful
HTTP-200 US-equity endpoint lookup for each CUSIP; both returned assets must carry the action symbol and different
UUIDs. Any missing, duplicated, malformed, or contradictory fact rejects the edge.

A qualifying split records its action, date, symbols, and CUSIPs as provenance. It never allocates, promotes, or
reassigns a pricing row. Split edges are not candidates for the distinct-security episode partition. Conversely, an
explicit identity partition suppresses only the corporate-action edges that the partition certifies; unrelated valid
lineage remains connected.

The public ID is always the SHA-256 digest of the sorted unique member Alpaca UUIDs. One security therefore has one
membership set and one public ID even when Alpaca changes its UUID and CUSIP during a same-symbol split.

### 4. Availability

Correlation does not make an identity online. The existing availability evaluator must successfully read every
advertised adjustment, return at least one bar, authenticate all dependencies, find no integrity or source conflict,
and find no unexplained internal session or identity boundary. Provider `active` and `tradable` fields are evidence for
specific rules, not substitutes for this gate.

## Non-divergence controls

The following are repository requirements, not suggestions:

- Production, replay, repair, and update work call the same correlation builder and publishers. There is no migration
  correlation engine.
- New ownership or row behavior is added to the owning explicit registry at the correct precedence point.
- Identity-only relationships are implemented as UUID-lineage edges and cannot be used as row-routing evidence unless
  a separately reviewed row rule says so.
- Ambiguity fails closed into the uncorrelated or offline state.
- Every behavioral change bumps its owning version and appends exactly one runtime changelog entry.
- The exact registry orders are asserted by tests and serialized into correlation outputs; changing order requires an
  explicit test and contract change.
- Finish the generic rule, focused tests, and universe-wide read-only impact audit before publishing a replay. Derived
  reports and catalogue generations bind deterministic evidence and ordering, so a later source change invalidates the
  replay evidence and requires the affected closure to be replayed and audited again.
- Evaluate the smallest complete affected-symbol closure, not the whole ticker universe. Run that list through this
  complete pipeline with `--symbol` or `--file`; do not write a migration helper, alternate publisher, or reduced
  processing path. The fact that many historical reports contain a newly recognized evidence type defines audit
  scope, not the set that must be reprocessed.
- A rule-only replay pins the authenticated request boundary already shared by the selected frozen inputs and does not
  use `--force`. Choosing a later boundary is a separate data-advance operation, not part of applying a correlation
  rule.
- The complete affected-symbol closure is evaluated, and the catalogue audit must check disjoint UUID membership
  before handoff.

Future update acquisition may compare the handoff-date bar and choose cached continuation or a full symbol refresh.
That choice belongs before the frozen-source boundary. After it, both paths must rejoin this exact pipeline and cascade.
It must not introduce a second correlation or publication path.

Detailed schemas and evidence contracts live in [Technical contracts](docs/technical-contracts.md). Operator commands
and mutation hazards live in [Operations](docs/operations.md), including the exact
[focused-reprocessing procedure](docs/operations.md#focused-reprocessing).
