"""Bounded pricing continuity when a retired name no longer has a vendor UUID.

Provider name changes establish the relationship; adjacent informative source
episodes corroborate its application. This is not exact-price donor evidence,
and the successor UUID must never be presented as a historical endpoint UUID.
"""

from dataclasses import dataclass
from datetime import date, timedelta

from libs.alpaca_pricing_download.contract import ADJUSTMENTS as ALPACA_ADJUSTMENT_MODES
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .action_identity import exact_cusip_lookup_asset
from .errors import SecurityCorrelationError
from .identity_partition import _session_date
from .security_identity import security_identity_for_alpaca_assets
from .terminal_episode import _inert, has_informative_trading, has_prolonged_carry_forward, shared_inert_bridges
from .uuid_lineage import build_uuid_lineage_edges

RETIRED_CONTINUATION_RULE = "bounded_retired_name_change_continuation"


@dataclass(frozen=True)
class RetiredContinuation:
    first: date
    last: date
    anchor_asset_id: str


def _lookup(lineage, kind, value):
    found = [
        item
        for item in lineage.get("asset_lookups", [])
        if item.get("lookup_kind") == kind and item.get("lookup") == value
    ]
    return found[0] if len(found) == 1 else {}


def retired_rename_path(symbol, lineage):
    """Metadata-only candidate gate, also usable for an affected-scope screen."""
    lookup = _lookup(lineage, "symbol", symbol)
    if (lookup.get("found"), lookup.get("outcome"), lookup.get("http_status")) != (False, "not_found", 404):
        return None
    if lineage.get("query", {}).get("corporate_actions_data_quality") != "complete":
        return None
    changes = [a for a in lineage.get("corporate_actions", []) if a.get("action_type") == "name_change"]
    current, cusip, after = symbol, None, date.min
    path, nodes = [], {}
    while current not in nodes:
        outgoing = [a for a in changes if a.get("old_symbol") == current]
        if not outgoing:
            break
        if len(outgoing) != 1:
            return None
        action = outgoing[0]
        if not all(
            isinstance(action.get(k), str) and action[k]
            for k in ("id", "process_date", "old_cusip", "new_symbol", "new_cusip")
        ):
            return None
        try:
            when = date.fromisoformat(action["process_date"])
        except ValueError:
            return None
        if when <= after or cusip is not None and cusip != action["old_cusip"]:
            return None
        nodes[current] = action["old_cusip"]
        path.append(action)
        current, cusip, after = action["new_symbol"], action["new_cusip"], when
    if not path or current in nodes:
        return None  # Recurrence/branching is not a spelling alias.
    nodes[current] = cusip
    anchor = exact_cusip_lookup_asset(lineage, cusip)
    named = _lookup(lineage, "symbol", current)
    if (
        anchor is None
        or anchor.get("symbol") != current
        or (named.get("found"), named.get("outcome"), named.get("http_status")) != (True, "found", 200)
        or not isinstance(named.get("asset"), dict)
        or (named["asset"].get("id"), named["asset"].get("symbol"), named["asset"].get("class"))
        != (anchor["id"], current, "us_equity")
    ):
        return None
    edges, _ = build_uuid_lineage_edges(lineage)
    component = set(
        security_identity_for_alpaca_assets(
            (anchor["id"],), edges, tickers=(current,), basis=RETIRED_CONTINUATION_RULE
        ).member_alpaca_asset_ids
    )
    for label, identifier in nodes.items():
        # Inspect all known occupants, not just a conveniently selected action.
        occupants = [a for a in lineage.get("symbol_occupancy_evidence", []) if a.get("symbol") == label]
        if any(a.get("cusip") != identifier or a.get("asset_id") not in component | {None} for a in occupants):
            return None
        if any(a.get("id") not in component for a in lineage.get("identities", []) if a.get("symbol") == label):
            return None
        endpoint = exact_cusip_lookup_asset(lineage, identifier)
        if endpoint is not None and endpoint["id"] not in component:
            return None
        direct = _lookup(lineage, "symbol", label).get("asset")
        if isinstance(direct, dict) and direct.get("id") not in component:
            return None
        incoming = [a for a in changes if a.get("new_symbol") == label]
        if len(incoming) > 1 or any(a.get("new_cusip") != identifier for a in incoming):
            return None
    return tuple(path), anchor["id"]


def _complete_grid(sessions):
    return (
        bool(sessions)
        and tuple(day.isoformat() for day in sessions)
        == expected_market_session_coverage(
            sessions[0].isoformat(), (sessions[-1] + timedelta(days=1)).isoformat()
        ).session_dates
    )


def assess_retired_continuation(symbol, lineage, work_items, *, correlation_loader, source_rows_loader):
    candidate = retired_rename_path(symbol, lineage)
    if candidate is None or correlation_loader is None or source_rows_loader is None:
        return None
    path, asset_id = candidate
    views = {
        source["adjustment"]: (download.rows, source, sessions) for download, source, sessions in work_items
    }
    if len(views) != len(work_items) or set(views) != set(ALPACA_ADJUSTMENT_MODES):
        return None
    rows, source, sessions = views["raw"]
    if not sessions or any(
        grid != sessions
        or (context.get("feed"), context.get("timeframe")) != (source.get("feed"), source.get("timeframe"))
        for _, context, grid in views.values()
    ):
        return None
    departure = date.fromisoformat(path[0]["process_date"])
    stop = next((i for i, day in enumerate(sessions) if day >= departure), len(sessions))
    for first, _ in shared_inert_bridges(work_items, include_edges=True):
        stop = min(stop, first)
    # A delayed process date is not a license to include the forward-filled pad.
    while stop and all(_inert(values[stop - 1]) is not None for values, _, _ in views.values()):
        stop -= 1
    if (
        not stop
        or not _complete_grid(sessions[:stop])
        or not has_informative_trading(rows[:stop])
        or any(has_prolonged_carry_forward(values[:stop]) for values, _, _ in views.values())
    ):
        return None
    first, last = sessions[0], sessions[stop - 1]
    # Splits can preserve continuity. Competing replacement/branch events cannot
    # be explained by an unqualified rename-only path.
    for action in lineage.get("corporate_actions", []):
        if action.get("action_type") not in {
            "cash_merger",
            "stock_merger",
            "stock_and_cash_merger",
            "spin_off",
            "reorganization",
        }:
            continue
        affected = {
            action.get(role)
            for role in ("symbol", "old_symbol", "new_symbol", "acquiree_symbol", "acquirer_symbol")
        }
        if not affected.intersection(a["old_symbol"] for a in path):
            continue
        value = action.get("effective_date") or action.get("ex_date") or action.get("process_date")
        try:
            when = date.fromisoformat(value)
        except (TypeError, ValueError):
            return None
        start = first
        for transition in path:
            end = date.fromisoformat(transition["process_date"])
            if transition["old_symbol"] in affected and start <= when <= end:
                return None
            start = end
    try:
        successor = correlation_loader(path[0]["new_symbol"])
        descriptors = {d["adjustment"]: d for d in successor.sources_by_filename.values()}
        if len(descriptors) != len(successor.sources_by_filename) or set(descriptors) != set(views):
            return None
        other_views = []
        for adjustment, descriptor in descriptors.items():
            if (descriptor.get("feed"), descriptor.get("timeframe")) != (
                source.get("feed"),
                source.get("timeframe"),
            ):
                return None
            values, generation = source_rows_loader(path[0]["new_symbol"], descriptor["data_file"])
            if generation != descriptor["download_sha256"] or not values:
                return None
            # Source context is in correlated segments only for some adapters;
            # row allocation is not required to use the observed handoff.
            other_views.append(values)
        grid = tuple(row["t"] for row in other_views[0])
        if any(tuple(row["t"] for row in values) != grid for values in other_views):
            return None
        position = 0
        while position < len(grid) and all(_inert(values[position]) is not None for values in other_views):
            position += 1
        if position == len(grid) or not all(
            has_informative_trading(values[position:]) for values in other_views
        ):
            return None
        # EOD timestamps select the New York session, not the UTC spelling.
        next_session = _session_date(grid[position])
        if next_session <= last:
            return None  # Overlap needs exact independent ownership; leave it to rule 9.
        gap_start = last + timedelta(days=1)
        missing = (
            expected_market_session_coverage(gap_start.isoformat(), next_session.isoformat()).session_count
            if gap_start < next_session
            else 0
        )
        if missing > 4:
            return None
    except (SecurityCorrelationError, KeyError, TypeError, ValueError):
        return None
    return RetiredContinuation(first, last, asset_id)
