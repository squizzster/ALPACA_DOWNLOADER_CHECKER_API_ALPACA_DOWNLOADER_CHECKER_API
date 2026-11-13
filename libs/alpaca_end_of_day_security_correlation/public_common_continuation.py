"""Inert readers for completed public-common outputs; fresh assertions reject."""

from dataclasses import asdict, dataclass
from datetime import date, timedelta

from libs.alpaca_security_lineage.inference_evidence import reject_assertions
from libs.alpaca_security_lineage.sourced_facts import PUBLIC_COMMON_KIND

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
    """Reject authored assertions instead of certifying a fresh public union."""
    reject_assertions(tuple(evidences), where="public-common certification")
    return {"version": 1, "certificates": [], "issues": []}


def source_history(
    symbol, lineage, membership, work, consensus, *, complete, informative, accepted_bounds, accepted_routes=()
):
    """Retired recipes cannot establish new source ownership bounds."""
    raise ValueError("researched source-history assertions are inadmissible for new inference")


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
