# Correlation policy lab

The correlation policy lab replays a YAML-selected correlation policy against completed published lineage.
`compare` reads one complete identity group. The explicit `audit` command evaluates the current policy across
the completed lineage selector in one bounded pass. Both modes are read-only: they do not fetch provider data,
change retained evidence or prices, publish lineage, rewrite YAML, or select an acquisition universe.

Install the production YAML dependency and run the lab from the repository root:

```bash
python3.11 -m pip install -r requirements.txt
./alpaca-lineage-lab compare \
  --store data_pricing \
  --symbol AAPL \
  --policy resources/alpaca_security_correlation_policy.yaml
```

The symbol is an exact seed label. The lab selects its complete group through the authoritative lineage index,
opens only that group's receipts, assignment boundaries and selected pricing generations, and keeps the coherent
view in memory. Missing, pending, changed or incompatible handoffs fail closed. Ordinary comparison never scans
the receipt universe or retries acquisition.

Published assignments are comparison data only. They never become candidate facts. The candidate consumes the
normalized evidence and pricing views already accepted by the selected receipts, runs correlation and
observation selection in memory, normalizes both boundaries by symbol/view/session, and emits a deterministic
diff.

## Current schema-4 policies

The canonical [production policy](../resources/alpaca_security_correlation_policy.yaml) is the mandatory
correlation-60/lineage-34 boundary. Its semantic identity deliberately reproduces the frozen
correlation-59/lineage-33 behavior exactly. The shared production-owned compiler and evaluator also power the lab;
the lab owns only read-only selection, comparison and audit. The execution contract is explicit:

1. Run the established fixed point with the first three ownership rules, unchanged ordered row rules, mandatory
   unresolved fallback and terminal promotion.
2. Freeze that complete baseline.
3. Compute unresolved-only complete-group occupancy reconciliation once from the frozen inputs and baseline.
4. Optionally enable the fourth ownership rule, `action_anchored_current_ticker_occupancy`.
5. Run a second fixed point seeded by the frozen baseline.

The exact production profile preserves one historical dependency: a completed foreign-occupancy reconciliation
may enable the fourth ownership fallback for the same source. The report names and traces that dependency rather
than claiming the two producers are independent. It records reconciliation evidence issues and every interval to
which the fourth rule contributed.

The strict loader rejects duplicate or unknown keys, unsafe YAML tags, unknown or reordered implementations,
mixed fallback profiles, invalid scalar types, changed fixed-point structure, incompatible implementation
versions, a missing unresolved default, a different terminal rule and oversized input. Rule and producer names
resolve only through in-process whitelists; YAML cannot import Python or traverse arbitrary fields. Reports bind
the exact policy-file digest and a canonical semantic digest.

The exact correlation-59 manifest and the earlier correlation-58 schema-1 and experimental schema-3 manifests
remain executable as explicitly named
legacy resources. Their `correlation_v58_single_phase` profile runs one fixed point with the frozen first three
ownership rules and v58 row/default/terminal cascade; it cannot enter the v59 reconciliation or fourth ownership
fallback. A focused `compare` may use a completed lineage-33/correlation-59 or current lineage-34/correlation-60
publication strictly as its
baseline while replaying the historical candidate from the receipt's trusted inputs. This is differential lab
behavior only: it does not roll back production, authorize publication, or make v58 eligible for the all-lineage
audit.

## Corrected candidate profile

The [corrected candidate](../resources/alpaca_security_correlation_evidence_quality_policy.yaml) retains the same
frozen-baseline and second-fixed-point structure but selects the corrected whitelisted implementations. Its two
fallback producers are computed once from raw frozen evidence and cannot feed each other.

Duplicate foreign-label distributions retain every published-v59 gate and additionally require rate equivalence.
Rates are parsed as `Decimal(str(value))`, quantized to `0.000001` with half-even rounding, and must differ by no
more than `0.0000005`. Boolean, malformed, non-finite and non-positive values are unknown and cannot reconcile an
action. Raw receipt evidence remains unchanged; only the correlation interpretation withholds a producer-proved
duplicate action.

The corrected fourth ownership rule requires a generation-bound positive-evidence certificate. It applies only
to a single-receipt complete group with exactly two independently resolved UUID/CUSIP occupants and an exact
active symbol/CUSIP route. Its lower bound is the first current-CUSIP cash-dividend ex-date strictly after every
competing-CUSIP anchor, with at least one later current anchor. Same-day overlap is allowed only when the complete
economic ledgers agree under the bounded decimal rule.

Every requested-label occupancy-capable action on or after the bound must resolve exactly to the current UUID.
The closure covers normalized occupancy and raw cash-dividend, capital-gains and future normalized distribution
records. Rival or third UUIDs veto the rule; blank, unresolved, malformed or unmatched routes remain unknown and
block it. Candidate UUID edges, structural actions resolved to a candidate, and unresolved requested-label
structural actions on or after the bound also block it. Conflicting or malformed structural dates fail closed.

The certificate binds the exact normalized-evidence SHA-256, pricing generation, request end-exclusive date and
exact complete session set across all eight views. The receipt generation remains baseline comparison provenance,
not candidate evidence. The resulting fourth-rule interval is closed at that end-exclusive boundary,
so it cannot assign an unobserved later row. The ambiguous prefix remains unresolved, UUIDs remain separate, and
the rule does not assert a transition date or security-continuity edge.

Run the corrected candidate with:

```bash
./alpaca-lineage-lab compare \
  --store data_pricing \
  --symbol AAPL \
  --policy resources/alpaca_security_correlation_evidence_quality_policy.yaml
```

A deliberate candidate difference exits nonzero because success means exact published parity, not merely that a
candidate rule matched.

## All-lineage audit

The current schema-4 policies can be checked across the completed selector with:

```bash
./alpaca-lineage-lab audit \
  --store data_pricing \
  --policy resources/alpaca_security_correlation_evidence_quality_policy.yaml
```

This is an explicit broad audit, not a loop around `compare`. It reads the complete index once, deduplicates its
projected groups, and applies conservative policy-aware index gates. Each selected receipt is read once. Missing
hints expand selection instead of silently excluding a possible match.

The audit then acquires one sorted set of shared pricing locks for retained groups, including unavailable group
members. It rechecks only the selector token, loads each selected pricing source, payload set and lineage boundary
once into memory, and evaluates all retained groups from that cache. A final selector-token check makes concurrent
lineage change fail the entire audit. There is no per-group filesystem reselection, retry, checkpoint or write.

The hard wall-clock ceiling is 3,600 seconds and can be configured downward with `--timeout-seconds`. Timeout
releases locks and returns an incomplete report with no conclusion (exit 124). Exact snapshot parity exits 0;
any raw diagnostic difference still exits 1, and validation or busy-state failure exits 2. Operational timeout
values are excluded from the completed report's semantic digest.

## Comparison output

Compatible publications may split equivalent decisions at different month or year boundaries, so raw boundary
JSON is not compared. The lab reports separate counts and bounded examples for:

- assignment semantics, unresolved reasons and identity membership;
- rule explanations, evidence issues and applied fourth-rule intervals;
- observation selection and accepted ticker labels;
- complete-group row and identity invariants;
- any mapping correction the candidate would require.

The report factors strict `exact_snapshot_parity` into `decision_parity` and
`diagnostic_snapshot_match`. It also reports `migration_compatible` without weakening `parity` or the CLI exit
code. The only non-exact diagnostic classified as migration-compatible is rule-order provenance preserved by the
exact lineage-33/correlation-59 to lineage-34/correlation-60 promotion. Every receipt in the complete group must
uniformly be either the exact policy-less 33/59 input or the exact current canonical 34/60 receipt produced by
that promotion, with the stored first-three order unchanged. The canonical production candidate must report the
exact first-four order; every other diagnostic and every decision dimension must match, and the fallback trace
must show no reconciliation, selected suffix, applied interval or second phase. Raw diagnostic differences remain
in the output. Audit conclusions are
`exact_parity`, `decision_parity_with_migrated_diagnostics`, or `differences`; strict parity remains false for the
middle case.

Use `compare` while developing a generic rule on explicitly selected complete groups, following the
[lineage research loop](development.md#lineage-research-and-rule-improvement-loop). Use `audit` only after the
candidate is ready for a declared broad check. Neither command changes production behavior.
