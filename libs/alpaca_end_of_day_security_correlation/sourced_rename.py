"""Inert readers for completed same-security rename outputs; fresh assertions reject."""

from dataclasses import asdict, dataclass
from datetime import date, timedelta
from functools import cached_property

from libs.alpaca_security_lineage.event_supplements import RENAME_KIND
from libs.alpaca_security_lineage.inference_evidence import reject_assertions

from .action_identity import exact_cusip_lookup_asset
from .closed_departure import current_asset
from .name_change import _action_dates
from .uuid_lineage import UuidLineageEdge

CORRELATION_VERSION = 81


def rename_facts(lineage):
    return tuple(item for item in lineage.get("event_supplements", ()) if item["kind"] == RENAME_KIND)


@dataclass(frozen=True)
class RenameCertificate:
    action_id: str
    old_symbol: str
    new_symbol: str
    old_cusip: str
    new_cusip: str
    old_id: str
    new_id: str
    boundary: str
    legal_date: str
    labels: tuple[tuple[str, str, str], ...]

    @cached_property
    def boundary_date(self):
        return date.fromisoformat(self.boundary)

    def to_report(self):
        result = asdict(self)
        result["labels"] = [list(item) for item in self.labels]
        return result

    @classmethod
    def from_report(cls, value):
        return cls(**{**value, "labels": tuple(tuple(item) for item in value["labels"])})

    @cached_property
    def ownership_intervals(self):
        from .ownership_interval_rules import AppliedOwnershipIntervalRule, TickerOwnershipInterval

        rule = AppliedOwnershipIntervalRule(1, "sourced_identity_preserving_rename", (), (self.action_id,))
        return tuple(
            TickerOwnershipInterval(
                label, self.old_cusip, self.old_id, date.fromisoformat(first), date.fromisoformat(last), (rule,)
            )
            for label, first, last in self.labels
        )

    def intervals(self):
        return self.ownership_intervals

    def label_edges(self):
        # A researched historical label is not a fabricated provider event and
        # does not create a current-symbol acquisition dependency.
        return tuple(
            UuidLineageEdge(
                "sourced_ticker_change",
                None,
                date.fromisoformat(left[2]),
                self.old_id,
                left[0],
                self.old_cusip,
                self.old_id,
                right[0],
                self.old_cusip,
                self.action_id,
                date.fromisoformat(left[2]),
            )
            for left, right in zip(self.labels, self.labels[1:])
        )

    def identity_edge(self):
        return UuidLineageEdge(
            "name_change",
            self.action_id,
            date.fromisoformat(self.boundary),
            self.old_id,
            self.old_symbol,
            self.old_cusip,
            self.new_id,
            self.new_symbol,
            self.new_cusip,
        )


def _nodes(value):
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _nodes(item)
    elif isinstance(value, list):
        for item in value:
            yield from _nodes(item)


def _structural_conflict(action, certificate):
    if action.get("id") == certificate.action_id or action.get("action_type") in {
        "cash_dividend",
        "capital_gain",
        "capital_gains_distribution",
    }:
        return False
    identifiers = {certificate.old_cusip, certificate.new_cusip, certificate.old_id, certificate.new_id}
    labels = (*certificate.labels, (certificate.new_symbol, certificate.boundary, "9999-12-31"))
    dates = _action_dates(action)
    for node in _nodes(action):
        for key, value in node.items():
            if (
                isinstance(value, str)
                and (key == "cusip" or key.endswith("_cusip") or key == "asset_id" or key.endswith("_asset_id"))
                and value in identifiers
            ):
                return True
            if not (key == "symbol" or key.endswith("_symbol")):
                continue
            prefix = key[:-6]
            if node.get(prefix + "cusip") or node.get(prefix + "asset_id"):
                continue
            if any(
                value == label and (not dates or any(first <= day.isoformat() < last for day in dates))
                for label, first, last in labels
            ):
                return True
    return False


def certify_group(evidences, *, complete=True, relations=None):
    """Reject authored assertions; no fresh researched certificate is produced."""
    reject_assertions(tuple(evidences), where="same-security rename certification")
    reject_assertions(relations, where="same-security rename relations")
    return (), ()


def apply_chronology(intervals, certificates):
    result = list(intervals)
    for certificate in certificates:
        result = [
            i
            for i in result
            if not (
                i.identity_key() == (certificate.old_symbol, certificate.old_cusip, certificate.old_id)
                and i.end_date_exclusive == date.fromisoformat(certificate.boundary)
            )
        ]
        result.extend(certificate.intervals())
    return tuple(result)


@dataclass(frozen=True)
class DisputedMapping:
    """Negative row scope only; it establishes no predecessor membership."""

    boundary: str
    new_id: str

    @cached_property
    def boundary_date(self):
        return date.fromisoformat(self.boundary)


def mapped_certificate(symbol, lineage, certificates, work):
    if certificates:
        raise ValueError("researched rename mapping recipes are inadmissible for new inference")
    applicable = [c for c in certificates if c.new_symbol == symbol]
    submitted = [item for item in rename_facts(lineage) if item["match"]["new_symbol"] == symbol]
    if (len(applicable) != 1 and len(submitted) != 1) or not work:
        return None
    certificate = applicable[0] if len(applicable) == 1 else None
    boundary = certificate.boundary if certificate else submitted[0]["facts"]["exchange_effective_date"]
    source = work[0][1]
    fields = ("requested_symbol", "asof", "request_from", "request_to", "feed", "timeframe")
    if any(any(context.get(k) != source.get(k) for k in fields) for _, context, _ in work):
        return None
    if (
        source["requested_symbol"] != symbol
        or source["asof"] != source["request_to"][:10]
        or source["asof"] < boundary
    ):
        return None
    asset = current_asset(lineage, symbol)
    if certificate is not None:
        return certificate if asset is not None and asset["id"] == certificate.new_id else None
    endpoint = exact_cusip_lookup_asset(lineage, submitted[0]["match"]["new_cusip"])
    if asset is not None and endpoint is not None and endpoint["id"] == asset["id"]:
        return DisputedMapping(boundary, asset["id"])
    return None


def source_history(symbol, certificate, work, consensus, *, complete, informative):
    """Retired recipes cannot establish new source ownership bounds."""
    raise ValueError("researched source-history assertions are inadmissible for new inference")


def histories(handoff, certificate):
    if handoff is None:
        return ()
    from .name_change import NameChangeHistory

    c = certificate
    first, last = date.fromisoformat(handoff["first"]), date.fromisoformat(handoff["last"])
    return tuple(
        NameChangeHistory(
            handoff["symbol"],
            max(first, interval.start_date_inclusive),
            min(last, interval.end_date_exclusive - timedelta(days=1)),
            interval,
            c.action_id,
        )
        for interval in c.intervals()
        if max(first, interval.start_date_inclusive)
        <= min(last, interval.end_date_exclusive - timedelta(days=1))
    )
