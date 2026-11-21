"""Scoped membership updates from completed source ownership and UUID edges.

This producer performs no provider or storage I/O. A supplied completed research
plan is consumed under its selected generation bindings without repeating its
applicability proof. Other inputs use the shared generic refinement once.
"""

from dataclasses import replace

from libs.alpaca_end_of_day_security_correlation.component_refinement import (
    NAME,
    DatedRoutes,
    ExtensionDecisions,
    membership_pairs,
    membership_policy_version,
    membership_scope,
    refine,
)
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


def apply_completed(reports, selected, completed, *, load_report=None):
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
    binding = completed["scope_binding"]
    version = membership_policy_version(selected)
    if (version >= 94 or "membership_version" in binding) and binding.get("membership_version") != version:
        raise LineageError("stale completed membership plan: membership policy changed")
    if version >= 94 and binding.get("receipt_generations") != selected["receipt_generations"]:
        raise LineageError("stale completed membership plan: selected receipt changed")
    if load_report is not None:
        names = set(binding.get("evidence_generations", {}))
        for change in changes:
            if not isinstance(change, dict) or not {"source", "donor_source"} <= change.keys():
                raise LineageError("incomplete completed membership source scope")
            names.update((change["source"], change["donor_source"]))
        if names - selected["receipts"].keys():
            raise LineageError("stale completed membership plan: selected receipt changed")
        reports = {name: load_report(name) for name in sorted(names)}
    if version >= 94 and binding.get("evidence_generations") != {
        name: report["lineage_evidence_sha256"]
        for name, report in reports.items()
        if "lineage_evidence_sha256" in report
    }:
        raise LineageError("stale completed membership plan: selected metadata changed")
    if not changes:
        return reports, completed
    partitions = {
        (item["source"], item["adjustment"], item["row_start"], item["row_end"]): item["fragments"]
        for item in completed.get("partitions", ())
    }
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
        target = completed.get("target_memberships", {}).get(change["new_public_id"])
        if target is None:
            target = next(
                (
                    identity
                    for identity in (selected["receipts"][donor] or {}).get("identity_memberships", ())
                    if identity["security_id"] == change["new_public_id"]
                ),
                None,
            )
        if target is None or target["security_id"] != change["new_public_id"]:
            raise LineageError("completed membership plan lost its containing component")
        key = source, change["adjustment"], change["row_start"], change["row_end"]
        if key in indexed:
            raise LineageError("completed membership plan repeats a source scope")
        indexed[key] = change, target
    result, consumed = dict(reports), set()
    for source, report in reports.items():
        segments = []
        changed = False
        pieces = []
        for original in report["correlated"]["segments"]:
            key = source, original["adjustment"], original["row_start"], original["row_end"]
            fragments = partitions.get(key)
            pieces.extend([{**original, **fields} for fields in fragments] if fragments else [original])
        for segment in pieces:
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


def _empty_trace(selected, reports, *, unknown=False):
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
        "scope_binding": membership_scope(selected, reports),
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
    version = getattr(getattr(ruleset, "policy", None), "implementation_correlation_version", 94)
    selected = {**selected, "membership_version": version}
    if completed is None:
        pairs = selected.get("membership_pair_handoff")
        if pairs is None:
            pairs = membership_pairs(
                selected["receipts"], reports if version >= 94 else None, selected.get("components")
            )
        if not pairs:
            # This optional producer cannot turn missing legacy metadata into a
            # rejection of the completed owner report. Unknown is not a proof
            # that the group is unaffected, nor permission to rebuild metadata.
            return reports, _empty_trace(selected, reports, unknown=pairs is None)
        selected = {**selected, "membership_pair_handoff": pairs}
    reports, trace = (
        refine(reports, evidence, selected)
        if completed is None
        else apply_completed(reports, selected, completed)
    )
    return _mark_changes(reports, trace), trace


def _mark_changes(reports, trace):
    if not trace["changes"]:
        return reports
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
    return result


def compose_growing_prefixes(
    reports, selected, evidence, inputs, *, load_prefix, load_prices, recover_edges=None
):
    """Compose accepted assignments and a fresh suffix when a donor first grows.

    An ordinary disjoint append has no new containing component. Its compact
    membership screen cannot select a single historical read. An actual growth
    opens only participants in its dated assignment/observation contest.
    """
    receipts = selected["receipts"]
    legacy = {
        name
        for name, receipt in receipts.items()
        if receipt
        and receipt.get("versions", {}).get("correlation", 94) < 94
        and receipt.get("retained_membership_assessed") != receipt.get("input_sources")
    }
    retained_donors = {
        name for name, view in inputs.items() if legacy and view.materialization_scope == "retained_suffix"
    }
    pairs = membership_pairs(receipts, reports, selected["components"], retained_donors=retained_donors)
    selected["membership_pair_handoff"] = pairs
    if not pairs:
        return reports, inputs
    projections, donor_targets, prefixes, recovered = set(), {}, {}, {}
    extensions = selected["membership_extensions"] = ExtensionDecisions()
    routes = selected["membership_routes"] = DatedRoutes()

    def prefix(name):
        if name not in prefixes:
            prefixes[name] = load_prefix(name)
        return prefixes[name]

    def segments(name):
        view = inputs[name]
        return (
            prefix(name)["correlated"]["segments"]
            if view.materialization_scope == "retained_suffix" and receipts[name].get("source_rows")
            else []
        ) + reports[name]["correlated"]["segments"]

    def bounds(item):
        return item["first_session_date"], item["last_session_date"]

    def overlap(left, right):
        return max(left[0], right[0]) <= min(left[1], right[1])

    for pair in pairs:
        source, donor = pair["source"], pair["donor"]
        old, target = pair["old"], pair["target"]
        if not any(inputs[n].materialization_scope == "retained_suffix" for n in (source, donor)):
            continue
        prior_memberships = (receipts[donor] or {}).get("identity_memberships", ())
        if (
            source not in legacy
            and donor not in legacy
            and any(item["security_id"] == target["security_id"] for item in prior_memberships)
        ):
            continue
        departures = [
            item
            for item in evidence[source].get("corporate_actions", ())
            if item.get("action_type") == "name_change" and item.get("old_symbol") == source
        ]
        if not departures or {item.get("new_symbol") for item in departures} != {donor}:
            continue
        candidates = [s for s in segments(source) if s["security_identity"]["security_id"] == old["security_id"]]
        donor_segments = segments(donor)
        touched = []
        possible = {target["security_id"]} | {
            m["security_id"]
            for m in prior_memberships
            if set(m["member_alpaca_asset_ids"]) <= pair["target_members"]
        }
        for segment in candidates:
            route = routes.assess(source, donor, segment, evidence[source])
            if not route:
                continue
            first, last = bounds(segment)
            for peer in donor_segments:
                left, right = bounds(peer)
                canonical = peer["canonical_security"]
                if (
                    peer["adjustment"] == segment["adjustment"]
                    and peer["security_identity"]["security_id"] in possible
                    and canonical.get("alpaca_asset_id") == route["dated_uuid"]
                    and canonical.get("cusip") == route["cusip"]
                    and max(first, left) <= min(last, right)
                ):
                    touched.append(
                        (max(first, left), min(last, right), peer["security_identity"]["security_id"])
                    )
        if not touched:
            continue
        donor_report = recovered.get(donor, reports[donor])
        if "uuid_lineage_edges" not in donor_report and donor in retained_donors and recover_edges is not None:
            donor_report = recovered[donor] = recover_edges(donor_report, donor)
        extension = extensions.assess(old, target, donor_report, donor, pair=pair)
        if not extension:
            continue
        transitions = {
            item["security_id"]: target
            for item in prior_memberships
            if item["security_id"] in {pid for _, _, pid in touched}
            and (
                item["security_id"] == old["security_id"] or extensions.assess(item, target, donor_report, donor)
            )
        }
        intervals = {(a, b) for a, b, pid in touched if pid == target["security_id"] or pid in transitions}
        if not intervals:
            continue
        projections.update((source, donor))
        for pid, identity in transitions.items():
            donor_targets.setdefault(donor, {}).setdefault(pid, {})[target["security_id"]] = identity
        for name in selected["components"][source] & inputs.keys() - {source, donor}:
            prior = receipts[name]
            if prior and any(
                i["security_id"] == target["security_id"]
                and any(overlap((i["from"], i["to"]), span) for span in intervals)
                for i in prior.get("identity_intervals", ())
            ):
                projections.add(name)
    if not projections:
        return ({**reports, **recovered} if recovered else reports), inputs
    result, assembled = {**reports, **recovered}, dict(inputs)
    for name in sorted(projections):
        view = inputs[name]
        if view.materialization_scope != "retained_suffix":
            continue
        report = result[name]
        limits = view.previous_assignment_summary
        if not any(item["rows"] for item in limits.values()):
            continue
        retained = prefix(name)
        assembled[name] = load_prices(name, retained)
        branches = {}
        for branch in ("correlated", "uncorrelated"):
            segments = []
            for prior_segment in retained[branch]["segments"]:
                adj = prior_segment["adjustment"]
                end = min(prior_segment["row_end"], limits[adj]["rows"])
                if end < prior_segment["row_start"]:
                    continue
                segment = prior_segment
                if end < segment["row_end"]:
                    stamp, last = limits[adj]["last_timestamp"], limits[adj]["last_session"]
                    segment = {
                        **segment,
                        "row_end": end,
                        "row_count": end - segment["row_start"] + 1,
                        "last_timestamp": stamp,
                        "last_session_date": last,
                        "to": last,
                    }
                if branch == "correlated":
                    old_id = segment["security_identity"]["security_id"]
                    targets = donor_targets.get(name, {}).get(old_id, {})
                    if len(targets) == 1:
                        target = next(iter(targets.values()))
                        segment = {
                            **segment,
                            "security_identity": target,
                            "canonical_security": {
                                **segment["canonical_security"],
                                "security_id": target["security_id"],
                            },
                            "membership_prefix_owner": old_id,
                        }
                segments.append(segment)
            for segment in report[branch]["segments"]:
                offset = limits[segment["adjustment"]]["rows"]
                segments.append(
                    {
                        **segment,
                        "row_start": segment["row_start"] + offset,
                        "row_end": segment["row_end"] + offset,
                    }
                )
            branches[branch] = {
                **report[branch],
                "segments": segments,
                "segment_count": len(segments),
                "row_count": sum(s["row_end"] - s["row_start"] + 1 for s in segments),
            }
        result[name] = {**report, **branches, "source_row_count": sum(b["row_count"] for b in branches.values())}
    return result, assembled


def invalidations(inputs, reports, trace, *, instants=None):
    """Name only the newly joined dated contests for retained-choice projection."""
    if trace is not None and "invalidated_sessions" in trace:
        result = {}
        for scope in trace["invalidated_sessions"]:
            for day in scope["sessions"]:
                result.setdefault(day, set()).update((scope["old_public_id"], scope["new_public_id"]))
        return result
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


def prepare(
    controls,
    evidence,
    auxiliary,
    plan,
    ruleset,
    cache,
    report_loader,
    *,
    completed=None,
    session_reader=None,
    pairs=None,
    recover_edges=None,
):
    """Select a bounded retained-source update before any historical price load."""
    if ruleset is None or ruleset.membership_refinement is None:
        return plan
    if ruleset.membership_refinement != NAME:
        raise LineageError("unsupported membership refinement producer")
    # A changed ownership or pricing binding remains with ordinary reconciliation.
    # Missing old edge metadata is not a negative ownership decision.
    # A retained pair already includes the group's still-applicable recorded
    # absences in each receipt generation. Such peers have no accepted dates to
    # compete for; reselect the complete group if an absent source appears.
    if (auxiliary and not pairs) or any(
        control.previous is None
        or not binding_applies(control)
        or plan.dispositions[name] not in {"A", "retained"}
        for name, control in controls.items()
    ):
        if completed is not None:
            raise LineageError("completed membership plan requires its unchanged accepted source bindings")
        return plan
    selected = _selected(controls)
    selected["membership_version"] = getattr(
        getattr(ruleset, "policy", None), "implementation_correlation_version", 94
    )
    selected["membership_sessions"] = session_reader
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
        tuple((name, id(facts)) for name, facts in sorted(auxiliary.items())),
    )
    plans = cache.setdefault("membership_plans", {})
    if scope in plans:
        return plans[scope]
    if completed is None:
        if pairs is None:
            pairs = membership_pairs(selected["receipts"])
        if not pairs:
            plans[scope] = _carry(
                plan,
                _empty_trace(selected, {}, unknown=pairs is None),
                ruleset,
                evidence,
                selected["receipts"],
            )
            return plans[scope]
        selected["membership_pair_handoff"] = pairs
        names = {name for pair in pairs for name in (pair["source"], pair["donor"])}
        reports = {name: report_loader(name) for name in sorted(names)}
        if recover_edges is not None:
            selected["recover_membership_edges"] = recover_edges
        reports, trace = update_reports(reports, evidence, selected, ruleset)
    else:
        reports, trace = apply_completed({}, selected, completed, load_report=report_loader)
        reports = _mark_changes(reports, trace)
    if not trace["changes"]:
        plans[scope] = _carry(plan, trace, ruleset, evidence, selected["receipts"])
        return plans[scope]
    result = _carry(plan, trace, ruleset, evidence, selected["receipts"], reports)
    plans[scope] = result
    return result
