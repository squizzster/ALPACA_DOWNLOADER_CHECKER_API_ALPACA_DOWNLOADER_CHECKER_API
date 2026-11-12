"""Research-only refinement of completed same-security component membership.

The containing component and dated owners are completed producer facts. This
producer decides their applicability to a narrower source scope; it neither
rebuilds membership nor reruns ownership, UUID-edge certification or prices.
Production policy does not invoke this experimental producer.
"""

from collections import defaultdict

from .errors import SecurityCorrelationError

NAME = "accepted_same_security_containing_component_v1"


def membership_pairs(receipts):
    """Select possible cross-source extensions from completed receipt metadata.

    None means the compact capability is unavailable, not proof of no effect.
    A nonempty result authorizes further applicability work, never allocation.
    """
    identities = {}
    for source, receipt in receipts.items():
        if not isinstance(receipt, dict) or not isinstance(receipt.get("identity_memberships"), list):
            return None
        for identity in receipt["identity_memberships"]:
            identities[source, identity["security_id"]] = identity
    members = {key: frozenset(value["member_alpaca_asset_ids"]) for key, value in identities.items()}
    return [
        {
            "source": source,
            "old": old,
            "donor": donor,
            "target": target,
            "old_members": members[source, old_id],
            "target_members": members[donor, target_id],
        }
        for (source, old_id), old in identities.items()
        for (donor, target_id), target in identities.items()
        if source != donor and old_id != target_id and members[source, old_id] < members[donor, target_id]
    ]


def _lookup(report, cusip):
    observations = [
        item
        for item in report.get("asset_lookups", ())
        if item.get("lookup_kind") == "cusip" and item.get("lookup") == cusip
    ]
    if not observations or any(
        item.get("outcome") != "found" or not (item.get("asset") or {}).get("id") for item in observations
    ):
        return None
    values = {item["asset"]["id"] for item in observations}
    return next(iter(values)) if len(values) == 1 else None


def _same_issue_route(source, donor, segment, evidence):
    """Duplicate dates can disagree on label timing while agreeing on identity."""
    security = segment["canonical_security"]
    uuid, cusip = security.get("alpaca_asset_id"), security.get("cusip")
    first = segment.get("first_session_date", segment.get("from"))
    last = segment.get("last_session_date", segment.get("to"))
    if not uuid or not cusip or not first or not last or segment.get("historical_symbol") != source:
        return None
    departures = [
        action
        for action in evidence.get("corporate_actions", ())
        if action.get("action_type") == "name_change" and action.get("old_symbol") == source
    ]
    if {action.get("new_symbol") for action in departures} != {donor}:
        return None
    actions = departures
    if not actions or any(
        action.get("old_cusip") != cusip
        or action.get("new_cusip") != cusip
        or not isinstance(action.get("process_date"), str)
        or last >= action["process_date"]
        for action in actions
    ):
        return None
    if _lookup(evidence, cusip) != uuid:
        return None
    return {"dated_uuid": uuid, "cusip": cusip, "action_ids": sorted({a["id"] for a in actions})}


def _extends(old, target, donor_report, donor_symbol, *, membership_pair=None):
    """Consume already certified directed split edges; never reconstruct them."""
    if membership_pair is None:
        previous = set(old["member_alpaca_asset_ids"])
        desired = set(target["member_alpaca_asset_ids"])
        if not previous < desired:
            return None
    else:
        previous = membership_pair["old_members"]
        desired = membership_pair["target_members"]
    edges = donor_report.get("uuid_lineage_edges", ())
    reached, proof = set(previous), []
    while desired - reached:
        candidates = [
            edge
            for edge in edges
            if edge.get("edge_type") in {"forward_split", "reverse_split"}
            and edge["predecessor"]["asset_id"] in reached
            and edge["successor"]["asset_id"] in desired - reached
            and edge["predecessor"]["symbol"] == donor_symbol
            and edge["successor"]["symbol"] == donor_symbol
        ]
        if not candidates:
            return None
        for edge in candidates:
            incoming = [
                other for other in edges if other["successor"]["asset_id"] == edge["successor"]["asset_id"]
            ]
            outgoing = [
                other for other in edges if other["predecessor"]["asset_id"] == edge["predecessor"]["asset_id"]
            ]
            if len(incoming) != 1 or len(outgoing) != 1:
                return None
        for edge in candidates:
            reached.add(edge["successor"]["asset_id"])
            proof.append(edge)
    return proof


def refine(reports, evidence, selected):
    """Return copy-on-write report changes plus their explicit applicability plan."""
    identities = {}
    for source, receipt in selected["receipts"].items():
        for identity in receipt.get("identity_memberships", ()):
            identities[source, identity["security_id"]] = identity
    pairs = selected.get("membership_pair_handoff")
    if pairs is None:
        pairs = membership_pairs(selected["receipts"])
    if pairs is None:
        raise SecurityCorrelationError(
            "membership refinement needs the selected sources' accepted compact membership; owning metadata recovery required"
        )
    by_source = defaultdict(list)
    for pair in pairs:
        by_source[pair["source"], pair["old"]["security_id"]].append(pair)
    accepted_old, routes, extensions = {}, {}, {}
    changes, issues = [], []
    result = dict(reports)
    for source, report in reports.items():
        segments = []
        for segment in report["correlated"]["segments"]:
            old = segment["security_identity"]
            own = identities.get((source, old["security_id"]))
            key = source, old["security_id"]
            if key not in accepted_old:
                accepted_old[key] = own is not None and (
                    tuple(own["member_alpaca_asset_ids"]) == tuple(old["member_alpaca_asset_ids"])
                )
            if not accepted_old[key]:
                segments.append(segment)
                continue
            targets = {}
            for pair in by_source[key]:
                donor, target = pair["donor"], pair["target"]
                security = segment["canonical_security"]
                route_key = (
                    source,
                    donor,
                    old["security_id"],
                    security.get("alpaca_asset_id"),
                    security.get("cusip"),
                    segment.get("historical_symbol"),
                    segment.get("first_session_date", segment.get("from")),
                    segment.get("last_session_date", segment.get("to")),
                )
                if route_key not in routes:
                    routes[route_key] = _same_issue_route(source, donor, segment, evidence[source])
                route = routes[route_key]
                if route is None:
                    continue
                extension_key = source, old["security_id"], donor, target["security_id"]
                if extension_key not in extensions:
                    extensions[extension_key] = _extends(
                        own, target, reports[donor], donor, membership_pair=pair
                    )
                proof = extensions[extension_key]
                if proof is None or route["dated_uuid"] not in target["member_alpaca_asset_ids"]:
                    continue
                # The donor must itself carry this dated owner under that exact
                # completed component; component size or shared UUID alone is not authority.
                if not any(
                    peer["adjustment"] == segment["adjustment"]
                    and peer["security_identity"]["security_id"] == target["security_id"]
                    and peer["canonical_security"].get("alpaca_asset_id") == route["dated_uuid"]
                    and peer["canonical_security"].get("cusip") == route["cusip"]
                    and peer.get("first_session_date", peer.get("from", "~"))
                    <= segment.get("first_session_date", segment.get("from", ""))
                    <= segment.get("last_session_date", segment.get("to", "~"))
                    <= peer.get("last_session_date", peer.get("to", ""))
                    for peer in reports[donor]["correlated"]["segments"]
                ):
                    continue
                targets[target["security_id"]] = (target, donor, route, proof)
            if len(targets) != 1:
                if targets:
                    issues.append(
                        {
                            "source": source,
                            "old_public_id": old["security_id"],
                            "reason": "competing_containing_components",
                        }
                    )
                segments.append(segment)
                continue
            target, donor, route, proof = next(iter(targets.values()))
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
            changes.append(
                {
                    "source": source,
                    "donor_source": donor,
                    "adjustment": segment["adjustment"],
                    "row_start": segment["row_start"],
                    "row_end": segment["row_end"],
                    "old_public_id": old["security_id"],
                    "new_public_id": target["security_id"],
                    "route": route,
                    "accepted_extension_edges": proof,
                    "source_generation": selected["pricing_generations"][source],
                    "receipt_generation": selected["receipt_generations"][source],
                    "donor_receipt_generation": selected["receipt_generations"][donor],
                }
            )
        if any(left is not right for left, right in zip(segments, report["correlated"]["segments"])):
            result[source] = {**report, "correlated": {**report["correlated"], "segments": segments}}
    scope = defaultdict(int)
    for change in changes:
        scope[change["source"]] += change["row_end"] - change["row_start"] + 1
    return result, {
        "implementation": NAME,
        "changes": changes,
        "issues": issues,
        "changed_source_view_rows": dict(sorted(scope.items())),
        "preserved": [
            "dated UUID/CUSIP",
            "historical labels",
            "rule explanations",
            "source positions",
            "prices",
        ],
        "scope_binding": {
            "selected_group": selected["selected_group"],
            "pricing_generations": selected["pricing_generations"],
        },
    }
