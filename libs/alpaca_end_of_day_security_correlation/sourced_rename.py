"""Sourced unchanged-equity membership and source-scoped mapped label history.

Metadata owns submitted fact validation. This producer binds those facts to exact
provider endpoints once for the selected group. Neither names nor matching prices
prove membership. An independently selected current mapping owns only its actual
complete observations; unrelated users of its labels retain their own sources.
"""

from dataclasses import asdict, dataclass
from datetime import date, timedelta
from functools import cached_property

from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_security_lineage.event_supplements import RENAME_KIND
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .action_identity import exact_cusip_lookup_asset
from .closed_departure import current_asset
from .name_change import _action_dates, _complete_action_query_covers
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


def certify_group(evidences, *, complete=True):
    """Bind each selected fact once, with conflicting group observations vetoed."""
    evidences = tuple({id(item): item for item in evidences}.values())
    submitted = {}
    for lineage in evidences:
        for item in rename_facts(lineage):
            submitted.setdefault(item["match"]["id"], []).append((lineage, item))
    accepted, issues = [], []
    for action_id, observations in submitted.items():
        reference = observations[0][1]
        match, fact = reference["match"], reference["facts"]
        old, new = match["old_cusip"], match["new_cusip"]
        boundary = fact["exchange_effective_date"]
        endpoints = None
        valid = complete
        if any(item != reference for _, item in observations):
            valid = False
        # Metadata has already matched each submitted fact to the raw provider
        # event. Consume that result; group eligibility adds only cross-evidence
        # endpoint consistency, query completeness and structural conflicts.
        for lineage in evidences:
            pair = tuple(exact_cusip_lookup_asset(lineage, cusip) for cusip in (old, new))
            observed = [
                lookup
                for lookup in lineage.get("asset_lookups", ())
                if lookup.get("lookup_kind") == "cusip" and lookup.get("lookup") in {old, new}
            ]
            carries_fact = any(candidate is lineage for candidate, _ in observations)
            if carries_fact and any(
                not _complete_action_query_covers(lineage, label, date.fromisoformat(boundary))
                for label in (match["old_symbol"], match["new_symbol"])
            ):
                valid = False
            if carries_fact or observed:
                if any(
                    asset is None
                    or asset.get("class") != "us_equity"
                    or asset.get("exchange") in (None, "", "OTC")
                    for asset in pair
                ):
                    valid = False
                    break
                ids = tuple(asset["id"] for asset in pair)
                if (
                    endpoints is not None
                    and ids != endpoints
                    or pair[0].get("exchange") != pair[1].get("exchange")
                ):
                    valid = False
                    break
                endpoints = ids
            if not valid:
                break
        certificate = None
        if valid and endpoints is not None:
            certificate = RenameCertificate(
                action_id,
                match["old_symbol"],
                match["new_symbol"],
                old,
                new,
                *endpoints,
                boundary,
                fact["legal_effective_date"],
                tuple((p["symbol"], p["start_inclusive"], p["end_exclusive"]) for p in fact["prior_labels"]),
            )
            for lineage in evidences:
                owners = dict(zip((old, new), endpoints))
                if any(
                    item.get("cusip") in owners and item.get("asset_id") != owners[item["cusip"]]
                    for item in lineage.get("symbol_occupancy_evidence", ())
                ):
                    valid = False
                if any(
                    _structural_conflict(action, certificate)
                    for action in lineage.get("provider_corporate_actions", lineage.get("corporate_actions", ()))
                ):
                    valid = False
        if valid and certificate is not None:
            accepted.append(certificate)
        else:
            issues.append(
                {
                    "reason_code": "sourced_rename_requires_unambiguous_unchanged_security",
                    "corporate_action_id": action_id,
                }
            )
    # Multiple sourced successors/overlapping histories cannot silently join.
    conflicted = {
        c.action_id
        for c in accepted
        for other in accepted
        if c != other and {c.old_id, c.new_id}.intersection({other.old_id, other.new_id})
    }
    issues.extend(
        {"reason_code": "conflicting_sourced_rename_chain", "corporate_action_id": key}
        for key in sorted(conflicted)
    )
    return tuple(c for c in accepted if c.action_id not in conflicted), tuple(issues)


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
    """Select observed history under the source's actual current entity mapping.

    Complete session/grid facts come from the current source producer. The dated
    labels and UUID membership are the independent sourced certificate above.
    A source mapped to today's unrelated predecessor label is never a donor.
    """
    if not isinstance(certificate, RenameCertificate) or not complete or not informative or not work:
        return None
    _, source, sessions = work[0]
    if (
        not sessions
        or not consensus.all_authenticated_adjustments_agree
        or consensus.authenticated_adjustment_count != len(ADJUSTMENTS)
    ):
        return None
    if {context["adjustment"] for _, context, _ in work} != set(ADJUSTMENTS):
        return None
    if sessions[0].isoformat() < certificate.labels[0][1] or sessions[-1].isoformat() < certificate.boundary:
        return None
    if source["request_from"][:10] > certificate.boundary:
        return None
    expected = expected_market_session_coverage(sessions[0].isoformat(), source["request_to"])
    if tuple(day.isoformat() for day in sessions) != expected.session_dates:
        return None
    return {
        "version": 1,
        "symbol": symbol,
        "first": sessions[0].isoformat(),
        "last": sessions[-1].isoformat(),
        "action_id": certificate.action_id,
    }


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
