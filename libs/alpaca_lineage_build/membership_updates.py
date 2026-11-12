"""Scoped membership updates from completed source ownership and UUID edges.

This producer performs no provider or storage I/O. A supplied completed research
plan is consumed under its selected generation bindings without repeating its
applicability proof. Other inputs use the shared generic refinement once.
"""

from dataclasses import replace

from libs.alpaca_end_of_day_security_correlation.component_refinement import NAME, membership_pairs, refine
from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp

from .contract import LineageError
from .source_work_contract import binding_applies


def _selected(controls):
    return {
        "store": str(next(iter(controls.values())).store),
        "selected_group": sorted(controls),
        "receipts": {name: control.previous for name, control in controls.items()},
        "pricing_generations": {name: control.source_sha256 for name, control in controls.items()},
        "receipt_generations": {name: control.previous_receipt_generation for name, control in controls.items()},
    }


def apply_completed(reports, selected, completed):
    """Apply exactly a completed refine() plan; do not recertify its explanation."""
    if (
        not isinstance(completed, dict)
        or not isinstance(selected.get("store"), str)
        or completed.get("store") != selected["store"]
    ):
        raise LineageError("completed membership plan belongs to another store")
    completed = completed.get("plan")
    if (
        not isinstance(completed, dict)
        or completed.get("implementation") != NAME
        or not isinstance(completed.get("scope_binding"), dict)
        or completed["scope_binding"].get("selected_group") != selected["selected_group"]
        or completed["scope_binding"].get("pricing_generations") != selected["pricing_generations"]
    ):
        raise LineageError("stale completed membership plan: selected group or pricing changed")
    changes = completed.get("changes")
    if not isinstance(changes, list):
        raise LineageError("incomplete completed membership plan")
    indexed = {}
    for change in changes:
        if (
            not isinstance(change, dict)
            or not {
                "source",
                "donor_source",
                "adjustment",
                "row_start",
                "row_end",
                "old_public_id",
                "new_public_id",
                "source_generation",
                "receipt_generation",
                "donor_receipt_generation",
            }
            <= change.keys()
        ):
            raise LineageError("incomplete completed membership source scope")
        source, donor = change["source"], change["donor_source"]
        if (
            source not in selected["receipts"]
            or donor not in selected["receipts"]
            or change["source_generation"] != selected["pricing_generations"][source]
            or change["receipt_generation"] != selected["receipt_generations"][source]
            or change["donor_receipt_generation"] != selected["receipt_generations"][donor]
        ):
            raise LineageError("stale completed membership plan: selected receipt changed")
        target = next(
            (
                identity
                for identity in selected["receipts"][donor]["identity_memberships"]
                if identity["security_id"] == change["new_public_id"]
            ),
            None,
        )
        if target is None:
            raise LineageError("completed membership plan lost its containing component")
        key = source, change["adjustment"], change["row_start"], change["row_end"]
        if key in indexed:
            raise LineageError("completed membership plan repeats a source scope")
        indexed[key] = change, target
    result, consumed = dict(reports), set()
    for source, report in reports.items():
        segments = []
        changed = False
        for segment in report["correlated"]["segments"]:
            key = source, segment["adjustment"], segment["row_start"], segment["row_end"]
            update = indexed.get(key)
            if update is None:
                segments.append(segment)
                continue
            change, target = update
            if segment["security_identity"]["security_id"] != change["old_public_id"]:
                raise LineageError("completed membership plan does not address its accepted source positions")
            consumed.add(key)
            changed = True
            segments.append(
                {
                    **segment,
                    "security_identity": target,
                    "canonical_security": {
                        **segment["canonical_security"],
                        "security_id": target["security_id"],
                    },
                }
            )
        if changed:
            result[source] = {**report, "correlated": {**report["correlated"], "segments": segments}}
    if consumed != indexed.keys():
        raise LineageError("completed membership plan lacks its selected accepted assignments")
    return result, completed


def _carry(plan, trace, ruleset, evidence, receipts, reports=None):
    changed = {change["source"] for change in trace["changes"]}
    return replace(
        plan,
        dispositions={name: "B" if name in changed else value for name, value in plan.dispositions.items()},
        prepared_reports={**(plan.prepared_reports or {}), **(reports or {})},
        membership_refinement={
            **trace,
            "execution_sha256": ruleset.execution_sha256,
            "input_evidence": evidence,
            "input_receipts": receipts,
        },
    )


def _empty_trace(selected, *, unknown=False):
    return {
        "implementation": NAME,
        "changes": [],
        "issues": [
            {
                "reason": "accepted_membership_scope_unknown",
                "sources": sorted(
                    name
                    for name, receipt in selected["receipts"].items()
                    if not isinstance(receipt, dict) or not isinstance(receipt.get("identity_memberships"), list)
                ),
            }
        ]
        if unknown
        else [],
        "applicability": "unknown" if unknown else "no_containing_pair",
        "scope_binding": {
            "selected_group": selected["selected_group"],
            "pricing_generations": selected["pricing_generations"],
        },
    }


def update_reports(reports, evidence, selected, ruleset, *, completed=None):
    """Lend fresh completed edges, or consume a bound research applicability plan.

    This precedes numerical selection. It never constructs edges, changes a
    dated owner or evaluates prices. Callers carry the returned trace forward.
    """
    if ruleset is None or ruleset.membership_refinement is None:
        return reports, None
    if ruleset.membership_refinement != NAME:
        raise LineageError("unsupported membership refinement producer")
    if completed is None:
        pairs = selected.get("membership_pair_handoff")
        if pairs is None:
            pairs = membership_pairs(selected["receipts"])
        if not pairs:
            # This optional producer cannot turn missing legacy metadata into a
            # rejection of the completed owner report. Unknown is not a proof
            # that the group is unaffected, nor permission to rebuild metadata.
            return reports, _empty_trace(selected, unknown=pairs is None)
        selected = {**selected, "membership_pair_handoff": pairs}
    reports, trace = (
        refine(reports, evidence, selected)
        if completed is None
        else apply_completed(reports, selected, completed)
    )
    result = dict(reports)
    for name, report in reports.items():
        positions = {
            (change["adjustment"], change["row_start"], change["row_end"]): change
            for change in trace["changes"]
            if change["source"] == name
        }
        if not positions:
            continue
        result[name] = {
            **report,
            "correlated": {
                **report["correlated"],
                "segments": [
                    {**segment, "completed_membership_refinement": positions[key]}
                    if (key := (segment["adjustment"], segment["row_start"], segment["row_end"])) in positions
                    else segment
                    for segment in report["correlated"]["segments"]
                ],
            },
        }
    return result, trace


def invalidations(inputs, reports, trace, *, instants=None):
    """Name only the newly joined dated contests for retained-choice projection."""
    result, held, sessions = {}, {}, {}
    instants = {} if instants is None else instants

    def session(value):
        if value not in sessions:
            if value not in instants:
                instants[value] = timestamp(value)
            sessions[value] = instants[value].astimezone(NEW_YORK).date().isoformat()
        return sessions[value]

    for change in (trace or {}).get("changes", ()):
        source = change["source"]
        key = source, change["adjustment"], change["row_start"], change["row_end"]
        if key not in held:
            segment = next(
                segment
                for segment in reports[source]["correlated"]["segments"]
                if (segment["adjustment"], segment["row_start"], segment["row_end"]) == key[1:]
            )
            first, last = (
                segment.get("first_session_date", segment.get("from")),
                segment.get("last_session_date", segment.get("to")),
            )
            if first == last and first is not None:
                held[key] = (first,)
            else:
                held[key] = tuple(
                    session(row[0])
                    for row in inputs[source].views[change["adjustment"]][
                        change["row_start"] - 1 : change["row_end"]
                    ]
                )
        for day in held[key]:
            result.setdefault(day, set()).update((change["old_public_id"], change["new_public_id"]))
    return result


def prepare(controls, evidence, auxiliary, plan, ruleset, cache, report_loader, *, completed=None):
    """Select a bounded retained-source update before any historical price load."""
    if ruleset is None or ruleset.membership_refinement is None:
        return plan
    if ruleset.membership_refinement != NAME:
        raise LineageError("unsupported membership refinement producer")
    # A changed ownership or pricing binding remains with ordinary reconciliation.
    # Missing old edge metadata is not a negative ownership decision.
    if auxiliary or any(
        control.previous is None
        or not binding_applies(control)
        or plan.dispositions[name] not in {"A", "retained"}
        for name, control in controls.items()
    ):
        if completed is not None:
            raise LineageError("completed membership plan requires its unchanged accepted source bindings")
        return plan
    selected = _selected(controls)
    scope = (
        ruleset.execution_sha256,
        tuple(
            (
                name,
                control.store,
                control.source_sha256,
                control.previous_receipt_generation,
                id(evidence[name]),
            )
            for name, control in sorted(controls.items())
        ),
        (),
    )
    plans = cache.setdefault("membership_plans", {})
    if scope in plans:
        return plans[scope]
    if completed is None:
        pairs = membership_pairs(selected["receipts"])
        if not pairs:
            plans[scope] = _carry(
                plan,
                _empty_trace(selected, unknown=pairs is None),
                ruleset,
                evidence,
                selected["receipts"],
            )
            return plans[scope]
        selected["membership_pair_handoff"] = pairs
        names = {name for pair in pairs for name in (pair["source"], pair["donor"])}
    else:
        names = set(controls)
    reports = {name: report_loader(name) for name in sorted(names)}
    reports, trace = update_reports(reports, evidence, selected, ruleset, completed=completed)
    if not trace["changes"]:
        plans[scope] = _carry(plan, trace, ruleset, evidence, selected["receipts"])
        return plans[scope]
    result = _carry(plan, trace, ruleset, evidence, selected["receipts"], reports)
    plans[scope] = result
    return result
