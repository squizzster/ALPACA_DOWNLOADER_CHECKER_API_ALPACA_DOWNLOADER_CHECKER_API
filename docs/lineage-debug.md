# Lineage rule tool

`./alpaca-lineage-debug` explains published decisions and tests candidates offline through the
[public JSON API](public-api.md#lineage-diagnostics). It shares Histories’ compiler, rule functions, correlation
engine and numerical selector. No daily update, provider requests or production publication.

## One YAML policy

```bash
./alpaca-lineage-debug rules
./alpaca-lineage-debug init --output /tmp/lineage-candidate.yaml
./alpaca-lineage-debug rules --policy /tmp/lineage-candidate.yaml
```

`init` writes a new, self-contained [canonical lineage YAML](../resources/alpaca_security_correlation_policy.yaml)
copy without overwriting. Its roots are `ruleset_schema_version: 2`, `policy` and `producers`. The copy sets
`policy.policy_role` to `research_candidate` and selects only generic rules/producers.

Under `policy.execution.established_fixed_point`, reorder/remove registered ownership and row rules. Empty lists
disable their optional decisions. Preserve the unresolved default, terminal rule and explicit frozen-baseline
reconciliation, optional ownership fallback and second fixed point. The fourth ownership fallback remains outside
the established list.

New evidence predicates require registered generic Python implementations and owning tests. YAML selects
implementations. `rules` lists paths/lines, registered order, terminal promotion, fixed identity membership,
numerical source selection, selected generic producers and their independent execution identity. Fixed steps cannot
be reordered. Dated ownership and price-source selection are independent decisions.

Only the diagnostic compiler accepts the research role. Production compilation/authorization reject it. Flat
schema-4 production/corrected-candidate YAML remains readable. The [policy lab](policy-lab.md) retains older
executable profiles. Combined YAML selects generic membership and dated labels under `producers`.

Before store/provider access, the loader rejects factual applications, supplements, assertion overlays and
`research.documented_facts`, including legacy or empty factual fields. It rejects retired
`accepted_publication`/`documented_scopes` modes, duplicate/unknown keys, unregistered/duplicate rules, unsafe tags,
altered mandatory defaults/phases and incompatible implementations. Limits are 1 MiB combined, 64 KiB policy.
Reports bind exact YAML bytes and rule semantics.

## Explain a recorded decision

```bash
./alpaca-lineage-debug explain --store data_pricing --ticker CPST --session 2016-01-04
./alpaca-lineage-debug explain --store data_pricing --ticker CPST --format json
```

The completed index selects the full group. Read selected receipts and assignment boundaries once, with no price
payloads, endpoint validators, provider calls or rule evaluation. `--session` selects only the applicable
closed-year, current-year or current-month assignment branch. Assignment handoffs report numerical-choice capability
separately from each segment’s selection flag.

Reports include generation bindings, policy provenance, normalized evidence, selected rule, dated owner, numerical
selection state, warnings and first-match paths. `--adjustment` defaults to `raw`. `--max-segments` limits displayed
segments per source. Counts cover the full selection.

Earlier rules fell through. Their failed predicates were unrecorded. The tool neither invents reasons or missing
capabilities nor reinterprets past publications with current rules. It distinguishes overlapping bounds from
identical first timestamps. Bounds alone establish neither duplicate interior sessions nor price differences. Label,
public-identity and selected-observation counts measure distinct facts.

## Published lineage coverage

```bash
./alpaca-lineage-debug coverage --store data_pricing --format json > /tmp/lineage-coverage.json
./alpaca-lineage-debug coverage --store data_pricing --unresolved-details \
  --format json > /tmp/lineage-coverage-details.json
./alpaca-lineage-debug coverage --store data_pricing --unresolved-details \
  --reuse-report /tmp/lineage-coverage-details.json --format json > /tmp/lineage-coverage-next.json
```

One completed-index read reports nonempty assigned histories, unresolved histories, empty sources and independent
pending workflow outcomes. A resolved source may still owe an application or publication. Counts describe recorded
decisions without revalidating prices or establishing ownership accuracy.

`--unresolved-details` reads receipts and assignment metadata only for indexed sources with unresolved rows.
Expected I/O specifies that selection and at most three assignment boundaries per source. No prices, provider
observations or rule execution. Recorded ambiguity/conflict/dispute counts as ambiguous. Other reasons withholding
allocation count as unsupported. Excluded exchange test symbols and missing reasons have separate classes. Classes
explain existing reasons. A source can span classes. Counts include adjustment views and overlapping reasons cannot
be summed as unique rows. Unreadable required metadata leaves details incomplete and preserves completed peers.

Operation/publication gates maintain coherence without per-source price locks, inventories, historical
authentication or repair. Reuse completed details for the same store, implementation and receipt/pricing
generations. Workflow-status changes alone retain details. A changed selected generation reads only affected
metadata.

## Run a candidate

```bash
./alpaca-lineage-debug run --store data_pricing \
  --policy /tmp/lineage-candidate.yaml --tickers CGRN CPST
./alpaca-lineage-debug run --store data_pricing \
  --policy /tmp/lineage-candidate.yaml --tickers CGRN CPST --format json > /tmp/lineage-comparison.json
./alpaca-lineage-debug run --store data_pricing \
  --policy /tmp/lineage-candidate.yaml --all --format json > /tmp/lineage-all.json
```

Select tickers or `--all`. One index read deduplicates complete groups, including aliases. `--all` explicitly
replays every indexed group without policy-lab audit prefilters, directory discovery or acquisition-universe
selection. It can be expensive.

Materialize accepted evidence and prices once per group, run the shared engine and release its inputs. No price
reauthentication. Published assignments supply comparison data only. Reports distinguish assignment, unresolved,
membership, rule-explanation, numerical-source and dated-label differences, plus candidate mapping corrections,
invariants and fixed-point/fallback traces. `--max-differences` caps examples per dimension/group. Counts remain
complete. `--format json` includes detailed traces/examples. Text summarizes groups.

Failed groups leave overall results incomplete and preserve completed peers. No automatic retry, acquisition,
publication or reconstruction. A stale selector/generation stops affected consumption. A changed final selector
invalidates the operation. Redirect to retain results. Memory includes one materialized group plus accumulated
compact reports.

Optional `--timeout-seconds` checks only between complete groups. The active group finishes even past the deadline,
unlike the policy-lab audit’s hard POSIX timeout. Interruption stops without production writes.

| Exit | Meaning |
|---|---|
| `0` | Complete exact parity or completed inspection |
| `1` | Completed comparison with differences |
| `2` | Invalid input, failed/incomplete diagnostic or output failure |
| `124` | Deadline, incomplete |
| `130` | Interrupted |

Genuine ambiguity is a semantic outcome. Differences alone establish neither improved lineage nor publication
authority.

## Paired candidate impact

Combined YAML defaults to the current canonical control (`canonical_ruleset_default`). An explicit unchanged control
names the binding and can select another accepted baseline.

```bash
./alpaca-lineage-debug run --store data_pricing --tickers CISSV \
  --control-policy resources/alpaca_security_correlation_policy.yaml \
  --policy /tmp/lineage-candidate.yaml --format json > /tmp/lineage-impact.json
```

After focused validation, `--all` can replace `--tickers`. Load each group once. Report `published_to_control`,
`control_to_candidate` and `published_to_candidate`. Counts and per-source timestamp bounds include every
difference. Only examples are bounded. Decision parity covers ownership, labels, numerical choices, membership and
mapping corrections. Explanation-only differences remain separate. Failed/unevaluated groups preclude complete
conclusions. No production publication.

Both arms run the installed implementation. A changed YAML cascade runs on held inputs, reusing baseline
normalization, row indexes and digests. Equal executable cascades reuse the completed control. A Python change
requires a preserved control implementation or an opt-in candidate alongside the unchanged path. Two YAML files
cannot restore globally replaced code.

The canonical combined YAML enables both registered producers.

```yaml
producers:
  membership_refinement: accepted_same_security_containing_component_v1
  dated_labels: observed_rename_periods_over_mapped_alias_v1
```

Flat legacy YAML retains `research.membership_refinement` and requires explicit `--control-policy`. Omitted producer
selection means null. `init` copies the canonical selections. Each arm uses its own YAML. To measure a producer’s
addition/removal, explicitly disable/enable it in the control. Identical selections reuse the completed result.

Membership refinement requires a completed containing identity, exact retained same-CUSIP rename, shared dated
provider UUID/CUSIP and donor report’s completed directed split edges. Every additional member needs that
same-security extension path. Containment alone, shared UUIDs, similar tickers, spin-offs, mergers and competing
destinations cannot authorize refinement. Conflicting rename dates may support identity while leaving labels and
dates unchanged.

The candidate replaces only applicable `security_identity` and `canonical_security.security_id`. It retains dated
UUID/CUSIP, source positions, ownership explanations, prices and unrelated identities. With the same cascade, reuse
control reports and numerical choices. Run the existing eight-view selector only for the newly joined contest. No
UUID-edge or identity-hash reconstruction. The applicability plan records affected positions, source/receipt
generations and accepted extension edges. A changed binding requires a new experiment.

Authorized production can select the named producer in canonical YAML or omit it to disable it. Research remains
read-only. A successful comparison alone cannot authorize publication or establish append, interruption recovery or
full production preservation. Production consumes the completed plan within its group/generation bindings, retaining
ownership and opening only the new numerical overlap.

Disabling candidate membership refinement holds the unrefined candidate and applies refinement once for the control.
Both comparisons share the publication and correlation result. Older saved comparisons for affected toggles need a
fresh group comparison. Applicable completed peers remain reusable.

Dated-label comparisons apply the [production rule](lineage.md#dated-label-collisions) only to label observations.
Retain completed ownership, numerical choices and explanations. Removing the producer restores the held pre-label
frame. Identical selections skip repeat planning/projection. Only changed observation dimensions get new digests.

## Screen and carry a membership study

Copy canonical YAML to `/tmp/membership-control.yaml`, set `membership_refinement` to null and match all other
candidate producers. With an otherwise unchanged cascade, the metadata screen excludes unaffected groups without
replaying prices or rules.

```bash
./alpaca-lineage-debug screen --store data_pricing \
  --control-policy /tmp/membership-control.yaml \
  --policy /tmp/lineage-candidate.yaml --format json > /tmp/lineage-screen.json
./alpaca-lineage-debug run --store data_pricing --all \
  --control-policy /tmp/membership-control.yaml \
  --policy /tmp/lineage-candidate.yaml --screen-report /tmp/lineage-screen.json \
  --format json > /tmp/lineage-impact.json
```

The completed index excludes groups lacking another indexed donor. Selected multi-source receipts provide accepted
source-local components. A confirmed absence of strict cross-source containment pairs excludes the candidate.
Missing compact membership remains unknown and requires evaluation. Positive pairs establish possibility only. The
smaller component needs acceptance in its own receipt. Control replay alone cannot supply it.

Before evaluation, the screen reports expected pricing-source scope and carries its full accepted index, selected
receipts and component pairs through JSON. Consumers check selector applicability, then reuse those bodies.
Immutable store/path acceptance crosses selected reader groups without reopening the manifest. Complete each
distinct rename/extension proof once and lend it to its view-specific segments.

Proven exclusions skip control replay and establish neither published/control parity nor overall correctness. The
impact report distinguishes assessed control drift, unassessed groups, evaluated groups, proven exclusions, failures
and remaining scope. A changed YAML cascade invalidates this candidate-specific screen.

`--reuse-report /tmp/previous-impact.json` carries applicable completed groups, including peers of failed groups in
incomplete attempts. Failure remains failure. Require matching diagnostic schema, version, implementation, store,
selector and semantic policy bindings or explicitly declared implementation compatibility. A release adding a
separate opt-in producer may preserve completed generic reports for unchanged research paths only. Future unknown
implementations require their own compatibility decision. Fact-backed reports remain inadmissible at every version.
Formatting or moving equivalent YAML preserves semantics.

Across invocations check selected receipt and pricing applicability without rereading prices or recalculating
completed decisions. Record stale groups without replay or loss of completed peers. Reject older reports missing
required implementation/store bindings without reconstructing them. Operators explicitly save research reports
outside production.

## Research generic rules from retained observations

Research chronology supplies an external expected result for rule design/evaluation. Runtime excludes researched
assertions, event supplements and per-ticker applications. Registered generic rules and parameters must reach the
same result for future matching evidence using retained provider observations and applicable completed decisions.

Compare plausible connections with leaving history ownerless. Publish the reasonably supported most likely owner
even without certainty or a particular certificate. If no connection is reasonably supported, record the unresolved
disposition, limits and competing interpretations, withhold ownerless history from consumers and acknowledge
completed, nonblocking processing. [Research loop](development.md#lineage-research-and-rule-improvement-loop)

Reuse completed generic paired reports within their exact policy, store, selector and generation bindings. Reports
containing factual candidate inputs cannot authorize execution. Older reports remain inspectable as data. Carry
generic control results and unchanged numerical choices without replay. Changed bindings select owning work.

Pass metadata read by a complete explanation to the next run.

```bash
./alpaca-lineage-debug explain --store data_pricing --ticker SSNT \
  --include-metadata-handoff --format json > /tmp/lineage-case-input.json
./alpaca-lineage-debug run --store data_pricing --tickers SSNT \
  --policy /tmp/lineage-candidate.yaml \
  --control-policy /tmp/membership-control.yaml \
  --metadata-handoff /tmp/lineage-case-input.json --format json > /tmp/lineage-case-result.json
```

Capture includes selected index entries, receipt/assignment bodies and store/selector/generation bindings. A
session-filtered explanation cannot yield a complete capture. Reuse selected entries without an index reread.
`--all` requires the full selector or applicable screen. Changed bindings stop consumption before price assembly.
This explicitly saved research input serves neither normal-run archiving nor recovery.

## Ticker instances in a completed CSV export

```bash
./alpaca-lineage-debug export --manifest /path/to/export/final_manifest.json
./alpaca-lineage-debug export --manifest /path/to/export/final_manifest.json \
  --tickers CPST CGRN --include-single --format json
```

Read the named completed manifest and its named digests once. Digests own dated labels, so even ticker-filtered
output requires export-wide metadata. No CSV rows, upstream catalogue, prices or rehashing. By default report
tickers in multiple public-ID CSVs, with names, filenames and inclusive dates. Distinguish disjoint dated reuse from
overlapping instance bounds. Several labels for one public ID do not multiply instances of a ticker. Missing legacy
dated-label handoffs leave the report incomplete, without rule reconstruction.

Overlapping bounds merit investigation but cannot prove incorrect ownership alone. Presence in an earlier export
cannot establish that run’s flags or reader behavior.

## Saved pipeline handoffs

```bash
./alpaca-lineage-debug handoff --input /path/to/debug/histories.output.json \
  --tickers CPST CGRN --format json
```

Prepare establishes baseline and selected work. Histories completes pricing, lineage and numerical choices. Serve
assesses and publishes the catalogue. Normal execution passes native values under one continuous operation lock and
saves no handoffs by default. Only DEBUG records them. Ordinary DEBUG retains compact operation metadata/counts and
omits held values/index bodies. Diagnostics expose those limits.

Standalone JSON stages retain exact prepared/histories documents. Inspect these or the final report as data. Display
historical runtime bindings without restoring acceptance objects, executing stages, treating the bindings as
execution authority or decoding unrelated prices. Full standalone JSON captures are parsed in memory. This tool
neither changes retention nor creates a recovery archive.
[Three-stage contracts](development.md#pipeline-phase-3-complete-pipeline) and
[lineage research loop](development.md#lineage-research-and-rule-improvement-loop).
