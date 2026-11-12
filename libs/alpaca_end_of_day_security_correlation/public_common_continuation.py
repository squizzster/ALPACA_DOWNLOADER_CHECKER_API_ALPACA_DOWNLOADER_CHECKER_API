"""Public-common continuity, distinct from new mapped-source ownership.

Metadata certification consumes accepted treatment facts, including nullable
rights and exchange-clause assertions. It never recalculates public share counts.
"""

from dataclasses import asdict, dataclass
from datetime import date, timedelta

from libs.alpaca_security_lineage.sourced_facts import PUBLIC_COMMON_KIND, items

from .security_relations import bind_provider_event, endpoint
from .uuid_lineage import UuidLineageEdge


@dataclass(frozen=True)
class CommonCertificate:
    source_fact_id: str
    predecessor: dict
    successor: dict
    legal_date: str
    boundary: str
    public_share_treatment: dict

    def to_report(self):
        return asdict(self)

    def identity_edge(self):
        p, s = self.predecessor, self.successor
        return UuidLineageEdge(
            PUBLIC_COMMON_KIND,
            None,
            date.fromisoformat(self.boundary),
            p["asset_id"],
            p["historical_symbol"],
            p["cusip"],
            s["asset_id"],
            s["historical_symbol"],
            s["cusip"],
            self.source_fact_id,
            date.fromisoformat(self.legal_date),
        )


def certify_group(evidences, *, complete=True, new_public_parent=True):
    evidences = tuple(evidences)
    submitted = {}
    for evidence in evidences:
        for item in items(evidence, PUBLIC_COMMON_KIND):
            submitted.setdefault(item["match"]["source_id"], []).append(item)
    accepted, issues = [], []
    for identifier, observations in sorted(submitted.items()):
        item = observations[0]
        fact = item["facts"]
        p, s = (endpoint(evidences, fact[role]) for role in ("predecessor", "successor"))
        valid = (
            complete
            and (fact["schema_version"] == 1 or new_public_parent)
            and all(other == item for other in observations)
            and p["asset_id"] is not None
            and s["asset_id"] is not None
            and p["asset_id"] != s["asset_id"]
            and bind_provider_event(fact, evidences, conversion=False)
        )
        for evidence in evidences:
            for action in evidence.get("provider_corporate_actions", evidence.get("corporate_actions", ())):
                if (
                    action.get("action_type") == "cash_merger"
                    and action.get("acquiree_cusip") == p["cusip"]
                    and any(
                        fact["legal_effective_date"] <= action.get(key, "") <= fact["exchange_effective_date"]
                        for key in ("effective_date", "ex_date")
                    )
                ):
                    valid = False
        if valid:
            accepted.append(
                CommonCertificate(
                    identifier,
                    p,
                    s,
                    fact["legal_effective_date"],
                    fact["exchange_effective_date"],
                    fact["public_share_treatment"],
                )
            )
        else:
            issues.append(
                {"source_fact_id": identifier, "reason_code": "public_common_endpoint_or_treatment_conflict"}
            )
    # v1 admits one direct public-common transition per component. Existing
    # same-UUID renames may extend its labels, never a second public branch.
    conflicted = {
        a.source_fact_id
        for a in accepted
        for b in accepted
        if a != b
        and {a.predecessor["asset_id"], a.successor["asset_id"]}
        & {b.predecessor["asset_id"], b.successor["asset_id"]}
    }
    issues.extend(
        {"source_fact_id": key, "reason_code": "public_common_branch_or_cycle"} for key in sorted(conflicted)
    )
    return {
        "version": 1,
        "certificates": [a.to_report() for a in accepted if a.source_fact_id not in conflicted],
        "issues": issues,
    }


def source_history(
    symbol, lineage, membership, work, consensus, *, complete, informative, accepted_bounds, accepted_routes=()
):
    """First producer proof of only newly claimed current-mapped observations."""
    from libs.alpaca_pricing_download.contract import ADJUSTMENTS
    from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

    from .closed_departure import current_asset
    from .name_change import NameChangeHistory
    from .ownership_interval_rules import AppliedOwnershipIntervalRule, TickerOwnershipInterval

    if (
        not work
        or not complete
        or not informative
        or not consensus.all_authenticated_adjustments_agree
        or consensus.authenticated_adjustment_count != len(ADJUSTMENTS)
    ):
        return None, ()
    _, context, sessions = work[0]
    current = current_asset(lineage, symbol)
    if not current or not sessions or context["asof"] != context["request_to"][:10]:
        return None, ()
    candidates = [
        CommonCertificate(**item)
        for item in membership["certificates"]
        if item["successor"]["asset_id"] == current["id"]
    ]
    if len(candidates) != 1:
        return None, ()
    certificate = candidates[0]
    bounds = accepted_bounds.get(certificate.predecessor["asset_id"])
    if (
        not bounds
        or context["request_from"][:10] > certificate.boundary
        or sessions[-1].isoformat() < certificate.boundary
    ):
        return None, ()
    # The current label must follow exactly one dated same-CUSIP rename chain.
    label, after, route = certificate.successor["historical_symbol"], certificate.boundary, []
    actions = accepted_routes
    visited = set()
    while label != symbol:
        if label in visited:
            return None, ()
        visited.add(label)
        links = [
            a
            for a in actions
            if a.get("action_type") == "name_change"
            and a.get("old_symbol") == label
            and a.get("old_cusip") == a.get("new_cusip") == certificate.successor["cusip"]
            and (a.get("process_date") or "") >= after
        ]
        if len(links) != 1:
            return None, ()
        link = links[0]
        route.append(link["id"])
        label, after = link["new_symbol"], link["process_date"]
    expected = expected_market_session_coverage(sessions[0].isoformat(), context["request_to"])
    if tuple(day.isoformat() for day in sessions) != expected.session_dates:
        return None, ()
    first = max(bounds[0], sessions[0].isoformat())
    last = min(bounds[1], (date.fromisoformat(certificate.boundary) - timedelta(days=1)).isoformat())
    if first > last:
        return None, ()
    rule = AppliedOwnershipIntervalRule(1, PUBLIC_COMMON_KIND, (), ())
    interval = TickerOwnershipInterval(
        certificate.predecessor["historical_symbol"],
        certificate.predecessor["cusip"],
        certificate.predecessor["asset_id"],
        date.fromisoformat(first),
        date.fromisoformat(last) + timedelta(days=1),
        (rule,),
    )
    handoff = {
        "version": 1,
        "symbol": symbol,
        "source_fact_id": certificate.source_fact_id,
        "route": route,
        "first": first,
        "last": last,
    }
    return handoff, (
        NameChangeHistory(
            symbol, date.fromisoformat(first), date.fromisoformat(last), interval, certificate.source_fact_id
        ),
    )


def histories(handoff, membership):
    """Represent completed source bounds without repeating its source proof."""
    from .name_change import NameChangeHistory
    from .ownership_interval_rules import AppliedOwnershipIntervalRule, TickerOwnershipInterval

    certificate = next(
        item for item in membership["certificates"] if item["source_fact_id"] == handoff["source_fact_id"]
    )
    p = certificate["predecessor"]
    first, last = date.fromisoformat(handoff["first"]), date.fromisoformat(handoff["last"])
    interval = TickerOwnershipInterval(
        p["historical_symbol"],
        p["cusip"],
        p["asset_id"],
        first,
        last + timedelta(days=1),
        (AppliedOwnershipIntervalRule(1, PUBLIC_COMMON_KIND, (), (handoff["source_fact_id"],)),),
    )
    return (NameChangeHistory(handoff["symbol"], first, last, interval, handoff["source_fact_id"]),)
