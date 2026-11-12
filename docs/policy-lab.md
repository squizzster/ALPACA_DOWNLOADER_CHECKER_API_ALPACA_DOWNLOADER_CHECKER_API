# Correlation policy lab

For a single public-API CLI with an editable research YAML, recorded ticker explanations and explicit
selected/all-group replay, use the [lineage rule tool](lineage-debug.md). Its `--all` evaluates every
indexed group; the lab's `audit` below retains conservative affected-group selection and its hard timeout.

The lab replays YAML-selected correlation against completed lineage. `compare` selects one complete identity
group; explicit `audit` checks the completed selector in one bounded pass. Both are read-only: no provider
requests, evidence/price changes, lineage publication, YAML rewriting or acquisition-universe selection.

```bash
python3.11 -m pip install -r requirements.txt
./alpaca-lineage-lab compare --store data_pricing --symbol AAPL \
  --policy resources/alpaca_security_correlation_policy.yaml
```

Run from repository root. Exact seed symbol selects the complete group through the lineage index; only selected
receipts, assignment boundaries and pricing generations are opened, retained coherently in memory. Missing,
pending, changed or incompatible handoffs fail closed. Comparison never scans receipts or retries acquisition.
Both commands use the [shared stderr heartbeat protocol](cli-reporting.md#shared-lifecycle);
their final JSON reports and parity/timeout exit meanings remain the same.

Published assignments are comparison data, never candidate facts. Candidates consume normalized evidence/pricing
accepted by receipts, run correlation and observation selection in memory, normalize by symbol/view/session and
emit deterministic differences. Authorities: [compare](../libs/alpaca_lineage_policy_lab/comparison.py),
[audit](../libs/alpaca_lineage_policy_lab/audit.py), [CLI](../libs/alpaca_lineage_policy_lab/command_line.py).

## Current schema-4 policies

[Canonical production YAML](../resources/alpaca_security_correlation_policy.yaml) binds correlation-91/lineage-37.
It retains the frozen correlation-59/lineage-33 fixed-point structure and adds the strict exact same-symbol handoff
certificate, bounded-entry form, bounded shared-UUID histories, complete-current-source early-successor bridge and
terminal carry-forward exclusion, post-padding bounded alias, vacant-ticker non-observation, pre-entry carry-forward
non-observation, component-event early-successor and exact mapped-successor-prefix certificates described in
[lineage contracts](lineage.md#name-changes). Production owns the shared
[compiler](../libs/alpaca_lineage_build/correlation_policy.py), `load_policy` / `authorize_production_policy`, and
[evaluator](../libs/alpaca_lineage_build/policy_engine.py), `evaluate_policy`; the lab owns selection/comparison/audit.

The [phase handoff](lineage.md#lineage-reuse) separates prepared chronology/continuity from fallback row authority.

1. Run the established fixed point: first three ownership rules, unchanged ordered row rules, mandatory unresolved
   fallback and terminal promotion.
2. Freeze the complete baseline.
3. Evaluate unresolved-only complete-group occupancy reconciliation once from frozen inputs/baseline.
4. Optionally enable fourth ownership rule `action_anchored_current_ticker_occupancy`.
5. Run the second fixed point seeded by the baseline.

Production preserves one historical dependency: completed foreign-occupancy reconciliation may enable the fourth
fallback for that source. Reports name/trace this dependence, reconciliation evidence issues and every interval
the fourth rule contributed; the producers are not independent.

The strict loader rejects duplicate/unknown keys, unsafe YAML tags, unknown/reordered implementations, mixed
fallback profiles, invalid scalar types, changed fixed-point structure, incompatible implementations, missing
unresolved default, different terminal rule and oversized input. Names resolve through in-process whitelists;
YAML cannot import Python or traverse arbitrary fields. Reports bind exact file and canonical semantic digests.

Explicit legacy resources remain executable:
[correlation 90](../resources/alpaca_security_correlation_v90_legacy_policy.yaml),
[correlation 89](../resources/alpaca_security_correlation_v89_legacy_policy.yaml),
[correlation 88](../resources/alpaca_security_correlation_v88_legacy_policy.yaml),
[correlation 87](../resources/alpaca_security_correlation_v87_legacy_policy.yaml),
[correlation 86](../resources/alpaca_security_correlation_v86_legacy_policy.yaml),
[correlation 85](../resources/alpaca_security_correlation_v85_legacy_policy.yaml),
[correlation 84](../resources/alpaca_security_correlation_v84_legacy_policy.yaml),
[correlation 83](../resources/alpaca_security_correlation_v83_legacy_policy.yaml),
[correlation 82](../resources/alpaca_security_correlation_v82_legacy_policy.yaml),
[correlation 77](../resources/alpaca_security_correlation_v77_legacy_policy.yaml),
[correlation 76](../resources/alpaca_security_correlation_v76_legacy_policy.yaml),
[correlation 59](../resources/alpaca_security_correlation_v59_legacy_policy.yaml),
[correlation 58 schema 1](../resources/alpaca_security_correlation_v58_legacy_policy.yaml),
[experimental schema 3](../resources/alpaca_security_correlation_experimental_v58_legacy_policy.yaml).
The v58 `correlation_v58_single_phase` profile runs one fixed point with frozen first-three ownership rules and
v58 row/default/terminal cascade, never v59 reconciliation/fourth-rule fallback. Focused comparison may use a
completed 33/59, frozen 34/76 or 34/77, retained 37/82 through 37/90, or current 37/91 baseline while replaying historical candidates from trusted inputs. This neither
rolls production back, authorizes publication nor admits v58 to all-lineage audit.

## Corrected candidate profile

[Candidate YAML](../resources/alpaca_security_correlation_evidence_quality_policy.yaml) retains frozen-baseline/
second-fixed-point structure with corrected whitelisted implementations. Both fallback producers run once from
raw frozen evidence and cannot feed each other.

Duplicate foreign-label distributions retain every published-v59 gate plus rate equivalence: parse
`Decimal(str(value))`, quantize to `0.000001` half-even, difference at most `0.0000005`. Boolean, malformed,
nonfinite or nonpositive rates are unknown and cannot reconcile. Raw receipt evidence remains unchanged;
correlation interpretation alone withholds a producer-proved duplicate.

The corrected fourth rule requires a generation-bound positive-evidence certificate: single-receipt complete
group, exactly two independently resolved UUID/CUSIP occupants, exact active symbol/CUSIP route. Lower bound:
first current-CUSIP cash-dividend ex-date strictly after every competing-CUSIP anchor, plus at least one later
current anchor. Same-day overlap requires complete economic-ledger agreement under bounded decimal comparison.

Every requested-label occupancy-capable action on/after the bound must resolve exactly to current UUID. Closure
covers normalized occupancy, raw cash-dividend/capital-gains and future normalized distribution records. Rival/
third UUID vetoes; blank, unresolved, malformed or unmatched routes remain unknown and block. Candidate UUID
edges, structural actions resolved to a candidate, and unresolved requested-label structural actions on/after the
bound also block; conflicting/malformed structural dates fail closed.

Certificates bind normalized-evidence SHA-256, pricing generation, request end-exclusive and exact complete
eight-view session set. Receipt generation is baseline provenance, never candidate evidence. Fourth-rule interval
closes at that exclusive bound: no unobserved later rows. Ambiguous prefix and separate UUIDs remain; no transition
date or security-continuity edge is asserted. [Certificate authority](../libs/alpaca_lineage_build/policy_active_suffix.py):
`prepare_active_suffix_policy`.

```bash
./alpaca-lineage-lab compare --store data_pricing --symbol AAPL \
  --policy resources/alpaca_security_correlation_evidence_quality_policy.yaml
```

A deliberate candidate difference exits nonzero: success means exact published parity, not merely a matching rule.

## All-lineage audit

Current schema-4 policies support a declared broad check:

```bash
./alpaca-lineage-lab audit --store data_pricing \
  --policy resources/alpaca_security_correlation_evidence_quality_policy.yaml
```

Audit reads the complete index once, deduplicates projected groups and applies conservative policy-aware gates.
Each selected receipt is read once; missing hints expand selection rather than omit possible matches.

One sorted set of shared pricing locks covers retained groups, including unavailable members. Recheck only the
selector token; load each selected source, payload set and assignment boundary once, then evaluate from memory.
A final selector-token check makes concurrent lineage change fail the entire audit. No per-group filesystem
reselection, retry, checkpoint or write; audit is not a loop around `compare`.

Hard wall-clock ceiling: 3,600 seconds, reducible with `--timeout-seconds`. Timeout releases locks and returns
incomplete/no conclusion, exit 124. Exact snapshot parity exits 0; any raw diagnostic difference exits 1;
validation/busy failure exits 2. Operational timeout values are excluded from completed-report semantic digests.

## Comparison output

Equivalent decisions may split at different month/year boundaries, so comparison normalizes rather than compares
raw boundary JSON. Separate counts/bounded examples cover assignments, unresolved reasons, identity membership;
rule explanations/evidence issues/fourth-rule intervals; numerical selection/dated labels; complete-group row/
identity invariants; and mapping correction the candidate would require.

`exact_snapshot_parity` factors into `decision_parity` and `diagnostic_snapshot_match`. `migration_compatible`
does not weaken `parity` or exit status. Its sole non-exact allowance remains the historical first-three versus
first-four rule-order provenance: every group receipt must be uniform, every other diagnostic/decision dimension
must match, and there may be no reconciliation, selected suffix, applied interval or second phase in the fallback
trace. This comparison classification does not authorize production publication. The owning pipeline's
generation-bound reuse and recovery contracts determine selected work; applicable completed decisions are
consumed without recalculation. Raw differences remain reported.

Audit conclusions: `exact_parity`, `decision_parity_with_migrated_diagnostics`, `differences`.
Strict parity remains false for the middle case. Develop generic rules with focused `compare` through the
[research loop](development.md#lineage-research-and-rule-improvement-loop); use `audit` only when ready for a declared
broad check. Neither changes production behavior.
