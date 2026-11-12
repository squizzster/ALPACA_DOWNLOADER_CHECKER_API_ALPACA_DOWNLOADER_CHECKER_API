# Lineage rule tool

`./alpaca-lineage-debug` explains published lineage and evaluates candidate rules through the
[public JSON API](public-api.md#lineage-diagnostics). It uses the same compiler, rule functions,
correlation engine and numerical selector as Histories. It makes no provider requests and never
publishes to the production store. Research does not rerun the daily update.

## One YAML policy

```bash
./alpaca-lineage-debug rules
./alpaca-lineage-debug init --output /tmp/lineage-candidate.yaml
./alpaca-lineage-debug rules --policy /tmp/lineage-candidate.yaml
```

`init` creates a single self-contained copy of the
[canonical lineage YAML](../resources/alpaca_security_correlation_policy.yaml). Its root sections are
`ruleset_schema_version: 1`, `policy`, `evidence.applications` and `producers`. The generated
`policy.policy_role` is `research_candidate`; `policy.research.control_mode` is `documented_scopes`.
It never overwrites an existing file. Edit the lists under
`policy.execution.established_fixed_point`: reorder or remove registered ownership rules and row rules.
An empty list disables that list's optional decisions. The unresolved default and terminal rule remain
mandatory; frozen-baseline reconciliation, optional ownership fallback and second fixed point retain
their explicit structure. Ownership's fourth fallback is separate from the editable established list.

YAML selects implemented rules; it is not a language for arbitrary Python predicates. New evidence
conditions require generic registered Python implementations and their owning tests. The `rules`
report identifies current function paths/lines, registered order, terminal promotion, fixed identity/
market/recapitalization/documented-public projections and numerical source selection. Those fixed
steps are visible but cannot be reordered through these lists. The index also lists factual application
IDs, explicit symbol scopes, fingerprints, per-application date-refinement intent, producer selection and
which fact versions support the diagnostic chronology projection. Dated ownership and choosing which
source supplies the prices are separate decisions.

Only this diagnostic compiler admits the research role. The production compiler/authorization
rejects it. Flat production and corrected-candidate schema-4 YAML files remain readable; the
[policy lab](policy-lab.md) retains older executable profiles. Combined files keep facts only under
`evidence.applications` and membership selection only under `producers`; duplicating them in
`policy.research` is rejected. The combined document is bounded to 1 MiB and its policy to 64 KiB.
Duplicate/unknown keys, unregistered rules, duplicate rule names, unsafe tags, altered mandatory defaults/phases and incompatible
implementations are rejected. Reports bind both exact YAML bytes and semantic rule configuration.

## Explain a recorded decision

```bash
./alpaca-lineage-debug explain --store data_pricing --ticker CPST --session 2016-01-04
./alpaca-lineage-debug explain --store data_pricing --ticker CPST --format json
```

The completed lineage index selects the full group. Each selected receipt and assignment boundary
is consumed once. No pricing payload, endpoint validator, provider or rule evaluation is used.
With `--session`, only the applicable closed-year, current-year or current-month assignment branch
is read. The receipt's actual assignment handoffs expose explicit numerical-choice capabilities
separately from the segment's recorded selection flag.
Reports retain generation bindings, policy provenance, normalized evidence, selected rule, dated
owner, numerical selection state, warnings and first-match paths. `--adjustment` defaults to `raw`;
`--max-segments` bounds displayed segments per source, while counts cover the complete selection.

Earlier first-match rules are described as having fallen through. Their individual failed predicate
reasons were not recorded by the producer, so the tool does not invent them or apply current rules to
explain an older publication. Missing capabilities remain missing. Overlapping selected segment
bounds are reported separately from exact shared first timestamps; bounds alone do not establish
duplicate interior sessions or different prices. Several ticker labels, several public identities,
and several selected numerical observations are different facts.

## Published lineage coverage

```bash
./alpaca-lineage-debug coverage --store data_pricing --format json > /tmp/lineage-coverage.json
./alpaca-lineage-debug coverage --store data_pricing --unresolved-details \
  --format json > /tmp/lineage-coverage-details.json
./alpaca-lineage-debug coverage --store data_pricing --unresolved-details \
  --reuse-report /tmp/lineage-coverage-details.json --format json > /tmp/lineage-coverage-next.json
```

The compact report reads the completed index once and projects pending workflow outcomes without validating
pricing evidence again. It separates nonempty assigned histories, unresolved histories and empty sources.
Pending work is independent: a published resolved source may still owe a new application or publication.
This is a census of recorded decisions, not independent evidence that every assigned owner is correct.

`--unresolved-details` explicitly selects only indexed sources with unresolved rows. It reads their receipts
and assignment metadata, never prices, provider observations or rule execution. Expected I/O reports that
selection and its maximum three assignment boundaries per source. An unresolved reason naming ambiguity,
conflict or dispute is reported as ambiguous; another recorded reason withholding allocation is unsupported.
Excluded exchange test symbols and missing recorded reasons remain separate. These are explanation classes,
not a new assessment of the evidence. A source can contain multiple classes; row counts include adjustment
views, and overlapping reason counts must not be added as unique rows. Required unreadable metadata leaves
the detail report incomplete while preserving independent completed details.

The operation and publication gates keep the selected metadata coherent. No per-source pricing lock,
directory inventory, historical authentication or repair occurs. A supplied coverage report carries completed
details for the same store, implementation and receipt/pricing generations. Changed workflow status alone
does not repeat them; a genuinely changed selected generation reads only its affected metadata.

## Run a candidate

```bash
./alpaca-lineage-debug run --store data_pricing \
  --policy /tmp/lineage-candidate.yaml --tickers CGRN CPST
./alpaca-lineage-debug run --store data_pricing \
  --policy /tmp/lineage-candidate.yaml --tickers CGRN CPST --format json > /tmp/lineage-comparison.json
./alpaca-lineage-debug run --store data_pricing \
  --policy /tmp/lineage-candidate.yaml --all --format json > /tmp/lineage-all.json
```

Choose tickers **or** `--all`. One index read selects and deduplicates complete groups. Two aliases in
the same group produce one evaluation. `--all` explicitly selects **every indexed group**, without the
policy lab audit's affected-group prefilters. It does not discover directories or select an acquisition
universe. A full research replay can be expensive; no all-group run occurs implicitly.

Selected completed evidence and prices are materialized once per group, used by the existing shared
engine, then released before the next group. Prices are trusted inputs, not authenticated again.
Published assignments serve as comparison data, never candidate evidence. The report separates
assignment, unresolved, membership, rule-explanation, numerical-source and dated-label differences;
it also reports candidate mapping corrections, invariants and fixed-point/fallback traces.
`--max-differences` bounds examples per dimension/group, not total counts. `--format json` retains
the detailed trace and examples; text summarizes each group.

Independent group failures retain completed peer reports with an incomplete overall result. There
is no automatic retry, acquisition, publication or reconstruction. Changed selector/generation
applicability stops the affected consumption; a changed final selector invalidates the operation.
Results are transient until the caller explicitly redirects them. Memory includes one materialized
group plus accumulated compact reports; it is not constant for an arbitrarily large research run.

`--timeout-seconds` is optional and checked between complete groups. An active group finishes before
the deadline is reported. Interrupting the command stops it without production writes. This is not
the policy lab audit's hard POSIX timeout.

Exit codes: `0` complete exact parity (or completed inspection); `1` completed comparison with
differences; `2` invalid input, failed/incomplete diagnostic or output delivery; `124` incomplete
deadline; `130` interruption. Genuine ambiguity is a recorded semantic result, not a technical error.
Candidate differences do not authorize publishing the candidate or automatically establish better lineage.

## Paired candidate impact

Combined YAML uses the current canonical rule configuration as its default control; the report names
`canonical_ruleset_default` as that selection. An explicit unchanged control can select a different accepted
baseline and makes the comparison binding visible in the command:

```bash
./alpaca-lineage-debug run --store data_pricing --tickers CISSV \
  --control-policy resources/alpaca_security_correlation_policy.yaml \
  --policy /tmp/lineage-candidate.yaml --format json > /tmp/lineage-impact.json
```

The same command accepts `--all` instead of `--tickers` after focused validation. Each selected group is
loaded once. The report separates `published_to_control`, `control_to_candidate` and
`published_to_candidate`. Counts and per-source timestamp bounds cover every difference; examples alone
are bounded. Paired decision parity covers ownership, labels, numerical selections, membership and mapping
corrections. Explanation-only differences remain visible separately. Failed and unevaluated groups prevent
a complete conclusion. No production publication occurs.

Both arms execute within this installed implementation. An ordinary YAML candidate that changes the
cascade receives its own evaluation on the held inputs; completed baseline normalization, row indexes and
digests carry forward. Identical executable cascades reuse the completed control. A Python change needs an
explicit preserved control implementation or an opt-in candidate with the unchanged path still present;
changing Python globally and naming two YAML files does not restore the earlier implementation.

The combined YAML supports one optional membership producer:

```yaml
producers:
  membership_refinement: accepted_same_security_containing_component_v1
```

Flat legacy research YAML retains its `research.membership_refinement` compatibility setting.

Omitted producer selection means null. The canonical YAML explicitly enables this producer;
`init` carries that selection into the research copy.
The control must leave the candidate producer disabled. Combined YAML can use its canonical default
control; a flat legacy research file requires explicit `--control-policy`.
The producer consumes an already completed containing identity, the exact retained same-CUSIP rename
observation, the shared dated provider UUID/CUSIP and the donor report's completed directed split edges.
Every additional member needs that same-security extension path. Mere containment, shared UUIDs, ticker
similarity, spin-offs, mergers and competing destinations cannot authorize this refinement. Conflicting
rename dates can agree on identity; the experiment retains the existing labels and does not choose a date.

The candidate replaces only the applicable `security_identity` and `canonical_security.security_id`.
It retains dated UUID/CUSIP, source positions, ownership explanations, prices and unrelated identities.
For the same cascade it consumes completed control reports and numerical choices, opening only the newly
joined observation contest through the existing eight-view selector. No UUID edges or identity hashes are
reconstructed. The applicability plan reports affected positions, source/receipt generations and the accepted
extension edges. A genuinely changed binding requires a new experiment rather than replaying a consumer.

The diagnostic remains read-only. Authorized production can select this same named producer in the canonical
YAML; omitting its selection disables it. A successful research comparison does not authorize publication or by itself prove
ordinary append, interruption recovery or complete production preservation. Production consumes its completed
plan under the selected group and generation bindings, preserving accepted ownership and opening only the
new numerical overlap.

## Screen and carry a membership study

For the named membership candidate with an otherwise unchanged cascade, a metadata screen can establish
which groups cannot be affected without replaying prices or rules:

```bash
./alpaca-lineage-debug screen --store data_pricing \
  --control-policy resources/alpaca_security_correlation_policy.yaml \
  --policy /tmp/lineage-candidate.yaml --format json > /tmp/lineage-screen.json
./alpaca-lineage-debug run --store data_pricing --all \
  --control-policy resources/alpaca_security_correlation_policy.yaml \
  --policy /tmp/lineage-candidate.yaml --screen-report /tmp/lineage-screen.json \
  --format json > /tmp/lineage-impact.json
```

The completed index excludes groups without another indexed donor. Selected multi-source receipts supply
accepted source-local components. No strict cross-source containment pair proves this candidate cannot
apply; missing compact membership remains unknown and is retained for evaluation. Positive pairs identify
possibilities, not confirmed changes. The smaller component must itself be accepted in its source receipt;
a component created solely by control replay cannot gain this research authority.

The screen reports expected pricing-source scope before evaluation and carries its full accepted index,
selected receipts and component pairs through JSON. Consumption checks the selector's actual applicability,
then uses those values without rereading index or receipt bodies. Immutable store/path acceptance likewise
crosses selected reader groups without reopening the store manifest. Each distinct rename and extension
proof is completed once and lent to its view-specific segments.

Proven candidate-unaffected groups have no control replay: their classification does **not** establish
published/control parity or overall lineage correctness. The impact report distinguishes assessed control
drift from unassessed groups, evaluated groups from proven exclusions, failures and remaining scope.
The screen is specific to this named candidate and rejects a changed YAML cascade.

An applicable completed paired report can be supplied with `--reuse-report /tmp/previous-impact.json`.
Completed peer groups from incomplete attempts remain usable; failed groups are not manufactured into
success. The diagnostic's own schema, version, implementation, store, selector and semantic policy bindings
must match. Formatting or moving an equivalent YAML file does not invalidate its meaning. A real invocation
gap checks selected receipt and pricing applicability, never prices or completed calculations. Failed
applicability records the affected group without replaying it or discarding independent peer results.

Reports created before the required implementation/store bindings are present are rejected; the tool never
silently reconstructs them. Saved research reports are explicit operator outputs outside the production store.

## Research a documented chronology

Research YAML can also supply new primary-source facts through the existing
[sourced-fact contract](lineage.md#sourced-event-facts). These are evidence assertions, not ticker exceptions
in the rules. In the combined YAML, `evidence.applications` contains entries with unique `id`, explicit
`symbols`, a `supplements` list and optional `refine_accepted_dates` (boolean, default true). The diagnostic
chronology projection consumes `dated_market_occupancy` facts in schema 3, 4 or 5; other accepted fact kinds
retain their existing production owners and are marked separately in the rule index. Merely adding them to
the YAML does not promise a new research replay path. Each supplement retains complete endpoints, episodes,
actual legal treatment and primary document provenance. No provider UUID or price may be invented.
Flat legacy research files still accept `research.documented_facts` without application IDs.

Use `policy.research.control_mode: documented_scopes` in the combined file. Groups intersecting those fact
scopes consume the accepted publication as their unchanged control; they do not replay the old cascade. Other groups retain the existing replay control
and membership experiment. `control_mode: accepted_publication` selects that control for every evaluated group.
Combined YAML uses its canonical default control unless `--control-policy` overrides it; flat legacy
research requires an explicit unchanged control. A factual update uses the same executable cascade and
consumes the accepted publication. A rule-only edit with unchanged application intent uses paired replay.
Changing both the factual scope and cascade in one group is rejected: test those steps separately.
Accepted-publication parity means the published handoff was carried, not that a full control replay matched
it. Reports count these controls separately from assessed and unassessed replay drift.

Production and research use the same storage-free
[documented-update owner](../libs/alpaca_lineage_build/documented_updates.py). It binds new facts once to
retained exact metadata and accepted source-local components. Changed accepted fact IDs require explicit
owning reconciliation; unchanged facts retain their existing acceptance. Explicit true and omitted date
intent are equivalent. Changing the intent alone selects its affected projection without binding the accepted
fact again; false does not undo previously published positive decisions.
Untouched documented owners/routes survive alongside the fresh plan. Competing chronologies and changed source
contexts remain errors. Positive dated refinement does not authorize filling unknown rows under a foreign
current symbol context. `refine_accepted_dates: false` keeps earlier positive ownership, explanations and
numerical choices closed while the documented owner evaluates unresolved observations.

Only newly owned or joined component positions enter numerical selection with their exact counterpart rows.
The handoff retains completed choices and requested equality outcomes, whose timestamps are JSON strings.
An empty captured equality list differs from an earlier execution that never requested that optional capture;
missing results do not authorize recomputation. Candidate receipts, assignments and accurately clipped report
positions travel in `candidate_handoff`, explicitly marked as research with no publication authorization.
The comparison changes neither production configuration nor retained storage formats.

The named membership screen also carries its accepted necessary-condition proof when new facts are added.
Groups intersecting a fresh factual scope cannot inherit its old unaffected classification. Completed results
for other groups remain applicable when their effective rule/fact/control scope is unchanged. A report containing
a changed group must be explicitly projected to compatible completed groups before `--reuse-report`; retain its
original provenance and mark omitted work incomplete. Supported version-2 and earlier version-3 reports have an
explicit compatibility reader for the unchanged engine/metadata contracts; unknown implementations and unbound
reports are rejected.

Metadata selects possible documented-owner sources before pricing materialization. Other identity components
remain metadata-only. Enabled membership refinement additionally retains both sides of its completed pair
handoffs; missing membership scope/capabilities cannot prove an exclusion. This bounds research input assembly;
it does not authenticate prices or establish live production performance.

Capture the metadata already read during a complete explanation and pass it directly to a subsequent run:

```bash
./alpaca-lineage-debug explain --store data_pricing --ticker SSNT \
  --include-metadata-handoff --format json > /tmp/lineage-case-input.json
./alpaca-lineage-debug run --store data_pricing --tickers SSNT \
  --policy /tmp/lineage-candidate.yaml \
  --control-policy resources/alpaca_security_correlation_policy.yaml \
  --metadata-handoff /tmp/lineage-case-input.json --format json > /tmp/lineage-case-result.json
```

The capture contains selected index entries, receipt/assignment bodies and actual store/selector/generation
bindings. A session-filtered explanation cannot claim a complete capture. Selected consumption uses those
entries without another index-body read; `--all` still needs its full selector or an applicable screen. Changed
bindings stop consumption before pricing assembly. This is an explicitly saved research input, not a normal-run
archive or recovery mechanism.

## Ticker instances in a completed CSV export

```bash
./alpaca-lineage-debug export --manifest /path/to/export/final_manifest.json
./alpaca-lineage-debug export --manifest /path/to/export/final_manifest.json \
  --tickers CPST CGRN --include-single --format json
```

The named completed manifest and its named digest files are consumed once. No CSV rows, upstream
catalogue or pricing payloads are opened, and no hashes are recalculated. This is an explicit
export-wide metadata report, even when its output is filtered to tickers: digests own the dated labels.
By default it reports tickers present in multiple public-ID CSVs, with names, filenames and inclusive
date ranges. It distinguishes disjoint dated reuse from overlapping instance bounds. Multiple labels
inside one public identity do not count as multiple instances of that ticker. Missing legacy dated
label handoffs are reported as incomplete rather than reconstructed from ownership rules.

Overlapping bounds are evidence to investigate, not automatic proof of wrong ownership. An identity's
presence in an earlier export does not establish that run's flags or reader behavior.

## Saved pipeline handoffs

```bash
./alpaca-lineage-debug handoff --input /path/to/debug/histories.output.json \
  --tickers CPST CGRN --format json
```

Prepare establishes the baseline and selected work; Histories completes pricing/lineage and the
numerical choices; Serve assesses and publishes the catalogue. Ordinary execution passes real JSON
under one continuous operation lock. The diagnostic reads a saved prepared/histories/final document
as data. It reports operation/runtime identity, stage results, scope and selected compact index entries
without restoring acceptance objects or executing any stage. Historical runtime bindings are displayed,
not treated as current execution authority. Unrelated encoded prices are not decoded. The full JSON
document is still parsed in memory, so very large DEBUG captures have a corresponding memory cost.

Normal pipeline execution saves no handoffs. DEBUG alone records them; this tool neither changes that
retention rule nor creates a recovery archive. See the
[three-stage contracts](development.md#pipeline-phase-3-complete-pipeline) and
[lineage research loop](development.md#lineage-research-and-rule-improvement-loop).
