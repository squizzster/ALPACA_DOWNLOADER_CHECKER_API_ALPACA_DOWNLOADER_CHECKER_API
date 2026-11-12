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
[production policy](../resources/alpaca_security_correlation_policy.yaml), marked
`policy_role: research_candidate`. It never overwrites an existing file. Edit the lists under
`execution.established_fixed_point`: reorder or remove registered ownership rules and row rules.
An empty list disables that list's optional decisions. The unresolved default and terminal rule remain
mandatory; frozen-baseline reconciliation, optional ownership fallback and second fixed point retain
their explicit structure. Ownership's fourth fallback is separate from the editable established list.

YAML selects implemented rules; it is not a language for arbitrary Python predicates. New evidence
conditions require generic registered Python implementations and their owning tests. The `rules`
report identifies current function paths/lines, registered order, terminal promotion, fixed identity/
market/recapitalization/documented-public projections and numerical source selection. Those fixed
steps are visible but cannot be reordered through these lists. Dated ownership and choosing which
source supplies the prices are separate decisions.

Only this diagnostic compiler admits the research role. The production compiler/authorization
rejects it. Existing production and corrected-candidate schema-4 YAML files are also readable;
the [policy lab](policy-lab.md) retains older executable profiles. Duplicate/unknown keys, unregistered
rules, duplicate rule names, unsafe tags, altered mandatory defaults/phases and incompatible
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
