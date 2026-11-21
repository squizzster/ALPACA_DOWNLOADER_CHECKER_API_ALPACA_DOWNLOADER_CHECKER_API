"""Scoped refinement of completed same-security component membership.

The containing component and dated owners are completed producer facts. This
producer decides their applicability to a narrower source scope; it neither
rebuilds membership nor reruns ownership, UUID-edge certification or prices.
The canonical ruleset can select this producer for the shared lineage engine;
research uses the same applicability decisions without publication.
"""

from collections import defaultdict
from itertools import groupby

from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp

from .errors import SecurityCorrelationError

NAME = "accepted_same_security_containing_component_v1"


def membership_pairs(receipts, current_reports=None, components=None, *, retained_donors=()):
    """Match an accepted predecessor with the donor's current completed membership.

    A new donor needs no previous receipt. Missing compact metadata in a
    legacy predecessor is unknown and cannot establish accepted ownership.
    """
    predecessors, donors = {}, {}
    for source, receipt in receipts.items():
        if receipt is None and current_reports is not None:
            continue
        if not isinstance(receipt, dict) or not isinstance(receipt.get("identity_memberships"), list):
            return None
        for identity in receipt["identity_memberships"]:
            predecessors[source, identity["security_id"]] = identity
    if current_reports is None:
        donors = predecessors
    elif len(current_reports) > 1 and predecessors:
        donors.update({key: item for key, item in predecessors.items() if key[0] in retained_donors})
        for donor, report in current_reports.items():
            for segment in report["correlated"]["segments"]:
                identity = segment["security_identity"]
                donors[donor, identity["security_id"]] = identity
    previous_members = {key: frozenset(item["member_alpaca_asset_ids"]) for key, item in predecessors.items()}
    donor_members = (
        previous_members
        if donors is predecessors
        else {key: frozenset(item["member_alpaca_asset_ids"]) for key, item in donors.items()}
    )
    return [
        {
            "source": source,
            "old": old,
            "donor": donor,
            "target": target,
            "old_members": previous_members[source, old_id],
            "target_members": donor_members[donor, target_id],
        }
        for (source, old_id), old in predecessors.items()
        for (donor, target_id), target in donors.items()
        if source != donor
        and (components is None or donor in components[source])
        and old_id != target_id
        and previous_members[source, old_id] < donor_members[donor, target_id]
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


def _transition(edge):
    return (
        edge.get("edge_type"),
        edge.get("effective_date"),
        *(
            edge[side].get(field)
            for side in ("predecessor", "successor")
            for field in ("asset_id", "symbol", "cusip")
        ),
    )


def _only_split_transition(edges, split, side):
    """Other incident edges must comprise one chronological spelling history."""
    incidents = {_transition(item): item for item in edges}
    incidents.pop(_transition(split))
    backwards = side == "predecessor"
    joined, distant = ("successor", "predecessor") if backwards else ("predecessor", "successor")
    endpoint, day = split[side], split.get("effective_date")
    seen = {endpoint["symbol"]}
    while incidents:
        matches = [key for key, item in incidents.items() if item[joined] == endpoint]
        if len(matches) != 1:
            return False
        item = incidents.pop(matches[0])
        other, event_day = item[distant], item.get("effective_date")
        if (
            item.get("edge_type") != "name_change"
            or other.get("asset_id") != endpoint["asset_id"]
            or not other.get("cusip")
            or other["cusip"] != endpoint["cusip"]
            or other.get("symbol") in seen
            or not event_day
            or not day
            or (event_day > day if backwards else event_day < day)
            or len(seen) > 1
            and event_day == day
        ):
            return False
        seen.add(other["symbol"])
        endpoint, day = other, event_day
    return True


def _extends(old, target, donor_report, donor_symbol, *, membership_pair=None, current=True, rejection=None):
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
    if current:
        edges = [edge for edge in edges if edge.get("corporate_action_id") and not edge.get("sourced_fact_id")]
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
            if rejection is not None:
                rejection.update(
                    reason="missing_directed_split_extension",
                    unreached_asset_ids=sorted(desired - reached),
                )
            return None
        for edge in candidates:
            for side in ("predecessor", "successor"):
                incident = [other for other in edges if other[side]["asset_id"] == edge[side]["asset_id"]]
                if current:
                    if not _only_split_transition(incident, edge, side):
                        if rejection is not None:
                            rejection.update(
                                reason="competing_uuid_transitions",
                                asset_id=edge[side]["asset_id"],
                                action_ids=sorted(
                                    {
                                        other["corporate_action_id"]
                                        for other in incident
                                        if other.get("corporate_action_id")
                                    }
                                ),
                            )
                        return None
                elif len(incident) != 1:
                    return None
        for edge in candidates:
            reached.add(edge["successor"]["asset_id"])
            proof.append(edge)
    return proof


class ExtensionDecisions:
    """One operation's directed split decisions for an immutable donor edge list."""

    def __init__(self):
        self._decisions = {}
        self.issues = []

    def assess(self, old, target, donor_report, donor, *, pair=None, current=True):
        edges = donor_report.get("uuid_lineage_edges")
        key = (old["security_id"], target["security_id"], donor, id(edges), current)
        if key not in self._decisions:
            rejection = {}
            proof = _extends(
                old, target, donor_report, donor, membership_pair=pair, current=current, rejection=rejection
            )
            self._decisions[key] = edges, proof
            if current and rejection:
                self.issues.append(
                    {
                        "donor_source": donor,
                        "old_public_id": old["security_id"],
                        "candidate_public_id": target["security_id"],
                        **rejection,
                    }
                )
        return self._decisions[key][1]


class DatedRoutes:
    """Share the completed source route across adjustment views and prefix assembly."""

    def __init__(self):
        self._routes = {}

    def assess(self, source, donor, segment, evidence):
        security = segment["canonical_security"]
        key = (
            id(evidence),
            source,
            donor,
            security.get("alpaca_asset_id"),
            security.get("cusip"),
            segment.get("historical_symbol"),
            *_bounds(segment),
        )
        if key not in self._routes:
            self._routes[key] = evidence, _same_issue_route(source, donor, segment, evidence)
        return self._routes[key][1]


def _bounds(segment):
    return (
        segment.get("first_session_date", segment.get("from")),
        segment.get("last_session_date", segment.get("to")),
    )


def _session_reader(selected):
    """Project selected observation positions only, sharing aliased view rows."""
    if selected.get("membership_sessions") is not None:
        return selected["membership_sessions"]
    if "inputs" not in selected:
        return None
    dates, ranges = {}, {}

    def read(source, segment):
        source_input = selected["inputs"][source]
        view = source_input.views[segment["adjustment"]]
        accepted = getattr(source_input, "membership_session_dates", {})
        first, last = segment["row_start"], segment["row_end"]
        key = id(view), first, last
        if key not in ranges:
            if hasattr(view, "timestamp_at"):
                result = [(view.session(i), view.timestamp_at(i)) for i in range(first - 1, last)]
            else:
                result = []
                for row in view[first - 1 : last]:
                    stamp = row[0]
                    if stamp not in dates:
                        dates[stamp] = (
                            accepted[stamp]
                            if stamp in accepted
                            else timestamp(stamp).astimezone(NEW_YORK).date()
                        ).isoformat()
                    result.append((dates[stamp], stamp))
            if len(result) != last - first + 1:
                raise SecurityCorrelationError("selected membership positions exceed the contributing source")
            ranges[key] = tuple(result)
        return ranges[key]

    return read


def _fragment(segment, offset, sessions):
    first, last = sessions[0], sessions[-1]
    result = dict(
        row_start=offset,
        row_end=offset + len(sessions) - 1,
        row_count=len(sessions),
        first_session_date=first[0],
        last_session_date=last[0],
        first_timestamp=first[1],
        last_timestamp=last[1],
    )
    if "from" in segment:
        result.update({"from": first[0], "to": last[0]})
    return result


def membership_scope(selected, reports):
    scope = {
        "selected_group": selected["selected_group"],
        "pricing_generations": selected["pricing_generations"],
    }
    version = membership_policy_version(selected)
    if version >= 94:
        # An additional group member can introduce a competing owner without a price change.
        scope.update(
            membership_version=version,
            receipt_generations=selected["receipt_generations"],
            evidence_generations={
                name: report["lineage_evidence_sha256"]
                for name, report in reports.items()
                if "lineage_evidence_sha256" in report
            },
        )
    return scope


def membership_policy_version(selected):
    version = selected.get("membership_version", 94)
    # Distribution can supply new completed ownership, but the membership
    # producer still consumes the same exact receipt and evidence bindings.
    return 94 if version in (95, 96, 97, 98, 99, 100, 101, 102) else version


def refine(reports, evidence, selected):
    """Return copy-on-write report changes plus their explicit applicability plan."""
    current = selected.get("membership_version", 94) >= 94
    identities = {}
    for source, receipt in selected["receipts"].items():
        for identity in (receipt or {}).get("identity_memberships", ()):
            identities[source, identity["security_id"]] = identity
    pairs = selected.get("membership_pair_handoff")
    if pairs is None:
        pairs = membership_pairs(
            selected["receipts"], reports if selected.get("current_donors") else None, selected.get("components")
        )
    if pairs is None:
        raise SecurityCorrelationError(
            "membership refinement needs the selected sources' accepted compact membership; owning metadata recovery required"
        )
    by_source = defaultdict(list)
    for pair in pairs:
        by_source[pair["source"], pair["old"]["security_id"]].append(pair)
    accepted_old = {}
    routes = selected.get("membership_routes")
    if type(routes) is not DatedRoutes:
        routes = DatedRoutes()
    extensions = selected.get("membership_extensions")
    if type(extensions) is not ExtensionDecisions:
        extensions = ExtensionDecisions()
    changes, issues, partitions = [], [], []
    targets_used = {}
    invalidated_dates = defaultdict(set)
    session_reader = _session_reader(selected) if current else None
    donor_days, recovered_edges = {}, {}
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
            target_days = defaultdict(set)
            for pair in by_source[key]:
                donor, target = pair["donor"], pair["target"]
                route = routes.assess(source, donor, segment, evidence[source])
                if route is None:
                    continue
                first, last = _bounds(segment)
                peers = [
                    peer
                    for peer in reports[donor]["correlated"]["segments"]
                    if peer["adjustment"] == segment["adjustment"]
                    and peer["security_identity"]["security_id"] == target["security_id"]
                    and peer["canonical_security"].get("alpaca_asset_id") == route["dated_uuid"]
                    and peer["canonical_security"].get("cusip") == route["cusip"]
                    and _bounds(peer)[0] <= last
                    and first <= _bounds(peer)[1]
                ]
                if not peers:
                    continue
                donor_report = reports[donor]
                if "uuid_lineage_edges" not in donor_report and selected.get("recover_membership_edges"):
                    if donor not in recovered_edges:
                        recovered_edges[donor] = selected["recover_membership_edges"](donor_report, donor)
                    donor_report = recovered_edges[donor]
                proof = extensions.assess(own, target, donor_report, donor, pair=pair, current=current)
                if proof is None or route["dated_uuid"] not in target["member_alpaca_asset_ids"]:
                    continue
                # The donor must itself carry this dated owner under that exact
                # completed component; component size or shared UUID alone is not authority.
                if session_reader is None:
                    if not any(_bounds(peer)[0] <= first and last <= _bounds(peer)[1] for peer in peers):
                        continue
                else:
                    for peer in peers:
                        peer_key = donor, peer["adjustment"], peer["row_start"], peer["row_end"]
                        if peer_key not in donor_days:
                            donor_days[peer_key] = frozenset(day for day, _ in session_reader(donor, peer))
                        target_days[target["security_id"]].update(donor_days[peer_key])
                targets[target["security_id"]] = (target, donor, route, proof)
            if not targets:
                segments.append(segment)
                continue
            fragments = [(segment, tuple(targets))]
            if session_reader is not None:
                sessions = session_reader(source, segment)
                selections = [
                    tuple(pid for pid, days in target_days.items() if day in days) for day, _ in sessions
                ]
                groups = [(ids, len(tuple(items))) for ids, items in groupby(selections)]
                if len(groups) == 1:
                    fragments = [(segment, groups[0][0])]
                else:
                    fragments, slices, offset = [], [], 0
                    for ids, count in groups:
                        fields = _fragment(
                            segment, segment["row_start"] + offset, sessions[offset : offset + count]
                        )
                        slices.append(fields)
                        fragments.append(({**segment, **fields}, ids))
                        offset += count
                    partitions.append(
                        {
                            "source": source,
                            "adjustment": segment["adjustment"],
                            "row_start": segment["row_start"],
                            "row_end": segment["row_end"],
                            "fragments": slices,
                        }
                    )
            for part, ids in fragments:
                if len(ids) != 1:
                    if ids:
                        issues.append(
                            {
                                "source": source,
                                "old_public_id": old["security_id"],
                                "row_start": part["row_start"],
                                "row_end": part["row_end"],
                                "reason": "competing_containing_components",
                                **(
                                    {"adjustment": part["adjustment"], "candidate_public_ids": sorted(ids)}
                                    if current
                                    else {}
                                ),
                            }
                        )
                    segments.append(part)
                    continue
                target, donor, route, proof = targets[ids[0]]
                segments.append(
                    {
                        **part,
                        "security_identity": target,
                        "canonical_security": {
                            **part["canonical_security"],
                            "security_id": target["security_id"],
                        },
                    }
                )
                targets_used[target["security_id"]] = target
                if session_reader is not None:
                    start = part["row_start"] - segment["row_start"]
                    stop = part["row_end"] - segment["row_start"] + 1
                    invalidated_dates[old["security_id"], target["security_id"]].update(
                        day for day, _ in sessions[start:stop]
                    )
                changes.append(
                    {
                        "source": source,
                        "donor_source": donor,
                        "adjustment": part["adjustment"],
                        "row_start": part["row_start"],
                        "row_end": part["row_end"],
                        "old_public_id": old["security_id"],
                        "new_public_id": target["security_id"],
                        "route": route,
                        "accepted_extension_edges": proof,
                        "source_generation": selected["pricing_generations"][source],
                        "receipt_generation": selected["receipt_generations"][source],
                        "donor_receipt_generation": selected["receipt_generations"].get(donor),
                    }
                )
        if len(segments) != len(report["correlated"]["segments"]) or any(
            left is not right for left, right in zip(segments, report["correlated"]["segments"])
        ):
            result[source] = {**report, "correlated": {**report["correlated"], "segments": segments}}
    scope = defaultdict(int)
    for change in changes:
        scope[change["source"]] += change["row_end"] - change["row_start"] + 1
    return result, {
        "implementation": NAME,
        "changes": changes,
        **({"partitions": partitions} if partitions else {}),
        **(
            {
                "invalidated_sessions": [
                    {"old_public_id": old, "new_public_id": new, "sessions": sorted(days)}
                    for (old, new), days in sorted(invalidated_dates.items())
                ]
            }
            if invalidated_dates
            else {}
        ),
        **({"target_memberships": targets_used} if current and targets_used else {}),
        "issues": [*extensions.issues, *issues] if current else issues,
        "changed_source_view_rows": dict(sorted(scope.items())),
        "preserved": [
            "dated UUID/CUSIP",
            "historical labels",
            "rule explanations",
            "source positions",
            "prices",
        ],
        "scope_binding": membership_scope(selected, reports),
    }
