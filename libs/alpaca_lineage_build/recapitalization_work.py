"""Carry accepted later owners through a documented earlier consolidation.

Only new, exactly bound own-common interpretations qualify. Their dates precede
all accepted ownership, raw observations and effective pricing are unchanged,
and every newly joined UUID edge is one of those consolidations. The completed
graph is carried into correlation; existing row and numerical decisions stay
closed. Other changes use ordinary owning reconciliation.
"""

from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_security_lineage.event_supplements import RECAPITALIZATION_KIND
from libs.alpaca_security_lineage.sourced_facts import EXTENSIONS

from .resume import continuing_request
from .source_work_contract import SourceWorkPlan, compose_memberships
from .source_work_contract import binding_applies as _binding_applies


def prepare(controls, evidence, auxiliary, cache):
    if auxiliary:
        return None
    if not any(
        item["kind"] == RECAPITALIZATION_KIND
        and item not in (controls[name].previous or {}).get("evidence", {}).get("event_supplements", ())
        for name, facts in evidence.items()
        for item in facts.get("event_supplements", ())
    ):
        return None
    graphs, edges, members = {}, [], {}
    changed = False
    for name, control in controls.items():
        prior = control.previous
        if not prior or not (
            _binding_applies(control)
            or any(
                change["source_sha256"] == prior["source_sha256"]
                and change["changed_from"] is not None
                and change["changed_from"] >= prior["processed_through"][:10]
                for change in control.source.get("history_changes", ())
            )
            and continuing_request(control.source, prior)
        ):
            return None
        before, after = prior["evidence"], evidence[name]
        old_facts, new_facts = before.get("event_supplements", ()), after.get("event_supplements", ())
        added = [item for item in new_facts if item not in old_facts]
        if any(item not in new_facts for item in old_facts) or any(
            item["kind"] != RECAPITALIZATION_KIND for item in added
        ):
            return None
        if any(
            before.get(key) != after.get(key)
            for key in (
                *EXTENSIONS.values(),
                "symbol_changes",
                "identities",
            )
        ):
            return None
        interpreted_ids = {item["match"]["id"] for item in new_facts}

        def other_occupancy(value, key):
            # Reclassifying these exact observed actions removes their former
            # merger-occupancy roles. Only independent occupancy can contradict
            # the unchanged later owner; retain those witnesses exactly.
            result = []
            for record in value.get(key, ()):
                witnesses = [
                    item for item in record.get("evidence", ()) if item.get("action_id") not in interpreted_ids
                ]
                if witnesses:
                    result.append({**record, "evidence": witnesses})
                elif not record.get("evidence"):
                    result.append(record)
            return result

        if any(
            other_occupancy(before, key) != other_occupancy(after, key)
            for key in ("symbol_occupancy_evidence", "input_symbol_occupants")
        ):
            return None
        if before.get("provider_corporate_actions", before.get("corporate_actions")) != after.get(
            "provider_corporate_actions", after.get("corporate_actions")
        ):
            return None
        first = min((item["from"] for item in prior["identity_intervals"]), default=None)
        if not first or any(item["facts"]["exchange_effective_date"] >= first for item in added):
            return None
        old_lookups = {(item["lookup_kind"], item["lookup"]): item for item in before["asset_lookups"]}
        current = {(item["lookup_kind"], item["lookup"]): item for item in after["asset_lookups"]}
        if any(current.get(key) != item for key, item in old_lookups.items()):
            return None
        for identity in prior["identity_memberships"]:
            for identifier in identity["member_alpaca_asset_ids"]:
                members[identifier] = identity
        binding = name, id(after)
        graphs_cache = cache.setdefault("recapitalization_graphs", {})
        if binding not in graphs_cache:
            candidates, issues = build_uuid_lineage_edges(after, candidates_only=True)
            graphs_cache[binding] = after, tuple(candidates), tuple(issues)
        graph = graphs[name] = graphs_cache[binding]
        allowed = {item["match"]["id"] for item in new_facts if item["kind"] == RECAPITALIZATION_KIND}
        interpreted = {
            item["id"]
            for item in after["corporate_actions"]
            if item.get("identity_interpretation") == RECAPITALIZATION_KIND
        }
        # A second provider observation of the same split can corroborate it;
        # require the same dated CUSIP transition rather than an arbitrary edge.
        transitions = {
            (item["facts"]["old_cusip"], item["facts"]["new_cusip"], item["facts"]["exchange_effective_date"])
            for item in new_facts
            if item["kind"] == RECAPITALIZATION_KIND
        }
        if not allowed <= interpreted or any(
            edge.edge_type != "reverse_split"
            or (edge.predecessor_cusip, edge.successor_cusip, edge.effective_date.isoformat()) not in transitions
            for edge in graph[1]
        ):
            return None
        edges.extend(graph[1])
        changed |= bool(added)
    if not changed:
        return None
    memberships = compose_memberships(members, edges)
    empty = {"version": 1, "certificates": [], "issues": []}
    prior = next(iter(controls.values())).previous
    return SourceWorkPlan(
        dict.fromkeys(controls, "B"),
        memberships,
        prior.get("sourced_rename_membership") or empty,
        prior.get("public_common_continuation_membership") or empty,
        {},
        prior.get("security_relations"),
        {},
        False,
        recapitalization_graphs=graphs,
    )


def project(view, prior, evidence, policy):
    """Construct only newly supported intervals, borrowing accepted later rows.

    A complete documented chronology needs no fallback source-grid audit or row
    cascade. Genuine residual alternatives can still enter ordinary allocation
    with these completed assignments protected.
    """
    from libs.alpaca_end_of_day_security_correlation.recapitalizations import project as intervals

    report = append_unresolved(view, prior, view.accepted_report)
    report["correlation_policy"] = policy.provenance()
    return intervals({view.symbol: view}, {view.symbol: report}, {view.symbol: evidence})[view.symbol]


def append_unresolved(view, prior, report):
    """Assemble only genuinely new positions beside the accepted old report."""
    report = {
        **report,
        "uncorrelated": {
            "row_count": report["uncorrelated"]["row_count"],
            "segments": list(report["uncorrelated"]["segments"]),
        },
    }
    report["source_row_count"] = sum(len(rows) for rows in view.views.values())
    for adjustment in ADJUSTMENTS:
        first = prior["assignment_summary"][adjustment]["rows"] + 1
        rows = view.views[adjustment]
        if first <= len(rows):
            report["uncorrelated"]["segments"].append(
                {
                    "adjustment": adjustment,
                    "row_start": first,
                    "row_end": len(rows),
                    "row_count": len(rows) - first + 1,
                    "first_timestamp": rows[first - 1][0],
                    "last_timestamp": rows[-1][0],
                }
            )
            report["uncorrelated"]["row_count"] += len(rows) - first + 1
    return report
