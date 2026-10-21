from dataclasses import dataclass
from datetime import date
from typing import Any

from libs.alpaca_pricing_download.contract import ADJUSTMENTS as ALPACA_ADJUSTMENT_MODES
from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    expected_market_session_coverage,
)

from .evidence import TickerOwnershipInterval
from .ticker_reuse_gap import AdjustmentSessionConsensus

CURRENT_SYMBOL_REUSE_MINIMUM_VACANCY_SESSIONS = 100
CURRENT_SYMBOL_REUSE_RULE_NAME = "complete_current_symbol_run_after_authenticated_ticker_reuse"
NONSTRUCTURAL_ACTION_TYPES = frozenset(("cash_dividend",))


@dataclass(frozen=True)
class CurrentSymbolReuseAssessment:
    symbol: str
    current_asset_id: str
    current_cusip: str
    current_name: str
    current_exchange: str
    vacated_action_id: str
    vacated_to_symbol: str
    vacancy_date: date
    previous_asset_id: str
    previous_cusip: str
    superseded_interval_asset_id: str
    superseded_interval_cusip: str | None
    vacancy_session_count: int
    first_session: date
    last_session: date
    session_count: int
    request_end_exclusive: str
    supporting_action_ids: tuple[str, ...]
    adjustment_consensus: AdjustmentSessionConsensus

    def as_report_object(self) -> dict[str, Any]:
        return {
            "rule_name": CURRENT_SYMBOL_REUSE_RULE_NAME,
            "minimum_vacancy_session_count": CURRENT_SYMBOL_REUSE_MINIMUM_VACANCY_SESSIONS,
            "symbol": self.symbol,
            "current_identity": {
                "alpaca_asset_id": self.current_asset_id,
                "cusip": self.current_cusip,
                "name": self.current_name,
                "exchange": self.current_exchange,
            },
            "vacated_identity": {
                "alpaca_asset_id": self.previous_asset_id,
                "cusip": self.previous_cusip,
                "renamed_to_symbol": self.vacated_to_symbol,
                "action_id": self.vacated_action_id,
                "vacancy_date": self.vacancy_date.isoformat(),
            },
            "superseded_open_interval": {
                "alpaca_asset_id": self.superseded_interval_asset_id,
                "cusip": self.superseded_interval_cusip,
            },
            "vacancy_session_count": self.vacancy_session_count,
            "first_session": self.first_session.isoformat(),
            "last_session": self.last_session.isoformat(),
            "session_count": self.session_count,
            "request_end_exclusive": self.request_end_exclusive,
            "supporting_nonstructural_action_ids": list(self.supporting_action_ids),
            "adjustment_consensus": self.adjustment_consensus.as_report_object(),
        }


def _date(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _action_involves_symbol(action: dict[str, Any], symbol: str) -> bool:
    return any(action.get(field) == symbol for field in ("symbol", "old_symbol", "new_symbol"))


def _unique_lookup_asset(
    lineage: dict[str, Any],
    lookup_kind: str,
    lookup_value: str,
) -> dict[str, Any] | None:
    asset_lookups = lineage.get("asset_lookups")
    if not isinstance(asset_lookups, list):
        return None
    candidates = []
    for lookup in asset_lookups:
        if (
            not isinstance(lookup, dict)
            or lookup.get("lookup_kind") != lookup_kind
            or lookup.get("lookup") != lookup_value
            or lookup.get("found") is not True
            or lookup.get("outcome") != "found"
        ):
            continue
        asset = lookup.get("asset")
        if isinstance(asset, dict):
            candidates.append(asset)
    unique = {
        (
            asset.get("id"),
            asset.get("symbol"),
            asset.get("name"),
            asset.get("exchange"),
        )
        for asset in candidates
    }
    if len(unique) != 1:
        return None
    asset_id, symbol, name, exchange = next(iter(unique))
    if not all(isinstance(value, str) and value.strip() for value in (asset_id, symbol, name, exchange)):
        return None
    return candidates[0]


def _current_identity(
    lineage: dict[str, Any],
    symbol: str,
) -> tuple[str, str, str, str] | None:
    symbol_asset = _unique_lookup_asset(lineage, "symbol", symbol)
    if symbol_asset is None or symbol_asset.get("symbol") != symbol:
        return None
    asset_id = symbol_asset["id"]
    identities = lineage.get("identities")
    if not isinstance(identities, list):
        return None
    matching_identities = [
        identity for identity in identities if isinstance(identity, dict) and identity.get("id") == asset_id
    ]
    if len(matching_identities) != 1:
        return None
    identity = matching_identities[0]
    lookup_cusips = identity.get("lookup_cusips")
    if not isinstance(lookup_cusips, list):
        return None
    cusips = {cusip for cusip in lookup_cusips if isinstance(cusip, str) and cusip}
    if len(cusips) != 1:
        return None
    cusip = next(iter(cusips))
    cusip_asset = _unique_lookup_asset(lineage, "cusip", cusip)
    if (
        cusip_asset is None
        or cusip_asset.get("id") != asset_id
        or cusip_asset.get("symbol") != symbol
        or identity.get("symbol") != symbol
        or identity.get("name") != symbol_asset["name"]
        or identity.get("exchange") != symbol_asset["exchange"]
    ):
        return None
    return asset_id, cusip, symbol_asset["name"], symbol_asset["exchange"]


def _complete_corporate_action_query(
    lineage: dict[str, Any],
    first_required_date: date,
    request_end_date: date,
) -> bool:
    query = lineage.get("query")
    if not isinstance(query, dict) or query.get("corporate_actions_data_quality") != "complete":
        return False
    query_start = _date(query.get("corporate_actions_start"))
    query_end = _date(query.get("corporate_actions_end"))
    return (
        query_start is not None
        and query_end is not None
        and query_start <= first_required_date
        and query_end >= request_end_date
    )


def _consensus_has_every_adjustment(consensus: AdjustmentSessionConsensus) -> bool:
    observed_adjustments = {
        value.split(":", 1)[0] for value in consensus.authenticated_adjustments if isinstance(value, str)
    }
    return (
        consensus.all_authenticated_adjustments_agree
        and consensus.authenticated_adjustment_count == len(ALPACA_ADJUSTMENT_MODES)
        and observed_adjustments == set(ALPACA_ADJUSTMENT_MODES)
    )


def assess_current_symbol_reuse(
    session_dates: tuple[date, ...],
    source: dict[str, Any],
    lineage: dict[str, Any],
    intervals: list[TickerOwnershipInterval],
    consensus: AdjustmentSessionConsensus | None,
    evidence_issues: list[dict[str, Any]] | None,
) -> CurrentSymbolReuseAssessment | None:
    """Resolve a complete current run after authenticated evidence vacated its ticker."""
    if (
        not session_dates
        or consensus is None
        or not _consensus_has_every_adjustment(consensus)
        or evidence_issues
        or source.get("recorded_integrity_failure") is not None
        or lineage.get("unresolved_cusips") != []
    ):
        return None
    symbol = source.get("requested_symbol")
    request_end = source.get("request_to")
    if not isinstance(symbol, str) or not isinstance(request_end, str):
        return None
    request_end_date = _date(request_end)
    if request_end_date is None:
        return None
    try:
        expected_run = expected_market_session_coverage(session_dates[0].isoformat(), request_end)
    except MarketCalendarError:
        return None
    if tuple(item.isoformat() for item in session_dates) != expected_run.session_dates:
        return None

    current_identity = _current_identity(lineage, symbol)
    if current_identity is None:
        return None
    current_asset_id, current_cusip, current_name, current_exchange = current_identity

    actions = lineage.get("corporate_actions")
    if not isinstance(actions, list):
        return None
    structural_before_run = []
    structural_in_run = []
    supporting_actions = []
    for action in actions:
        if not isinstance(action, dict) or not _action_involves_symbol(action, symbol):
            continue
        action_date = _date(action.get("process_date"))
        if action_date is None:
            return None
        action_type = action.get("action_type")
        if action_type in NONSTRUCTURAL_ACTION_TYPES:
            if (
                session_dates[0] <= action_date < request_end_date
                and action.get("symbol") == symbol
                and action.get("cusip") == current_cusip
                and isinstance(action.get("id"), str)
            ):
                supporting_actions.append(action)
            continue
        if action_date < session_dates[0]:
            structural_before_run.append((action_date, action))
        elif action_date < request_end_date:
            structural_in_run.append(action)
    if structural_in_run or not supporting_actions or not structural_before_run:
        return None

    latest_structural_date = max(item[0] for item in structural_before_run)
    latest_structural = [
        action for action_date, action in structural_before_run if action_date == latest_structural_date
    ]
    if len(latest_structural) != 1:
        return None
    vacated = latest_structural[0]
    previous_cusip = vacated.get("old_cusip")
    vacated_to_symbol = vacated.get("new_symbol")
    vacated_action_id = vacated.get("id")
    if (
        vacated.get("action_type") != "name_change"
        or vacated.get("old_symbol") != symbol
        or not isinstance(vacated_to_symbol, str)
        or not vacated_to_symbol
        or vacated_to_symbol == symbol
        or not isinstance(previous_cusip, str)
        or not previous_cusip
        or previous_cusip == current_cusip
        or not isinstance(vacated_action_id, str)
        or not vacated_action_id
    ):
        return None
    previous_asset = _unique_lookup_asset(lineage, "cusip", previous_cusip)
    if previous_asset is None or previous_asset.get("id") == current_asset_id:
        return None
    previous_asset_id = previous_asset.get("id")
    if not isinstance(previous_asset_id, str) or not previous_asset_id:
        return None

    superseded_intervals = [
        interval
        for interval in intervals
        if interval.historical_symbol == symbol
        and interval.asset_id != current_asset_id
        and interval.start_date_inclusive is not None
        and interval.start_date_inclusive < latest_structural_date
        and interval.end_date_exclusive is None
        and interval.contains(session_dates[0])
    ]
    if len(superseded_intervals) != 1:
        return None
    superseded_interval = superseded_intervals[0]

    if not _complete_corporate_action_query(lineage, latest_structural_date, request_end_date):
        return None
    try:
        vacancy = expected_market_session_coverage(
            latest_structural_date.isoformat(),
            session_dates[0].isoformat(),
        )
    except MarketCalendarError:
        return None
    if vacancy.session_count < CURRENT_SYMBOL_REUSE_MINIMUM_VACANCY_SESSIONS:
        return None

    supporting_action_ids = {action["id"] for action in supporting_actions}
    matching_occupancy = []
    occupancy_evidence = lineage.get("symbol_occupancy_evidence")
    if not isinstance(occupancy_evidence, list):
        return None
    for occupant in occupancy_evidence:
        if not isinstance(occupant, dict) or occupant.get("symbol") != symbol:
            continue
        evidence = occupant.get("evidence")
        if not isinstance(evidence, list):
            return None
        for item in evidence:
            if not isinstance(item, dict):
                continue
            item_date = _date(item.get("process_date"))
            if item_date is None:
                return None
            if session_dates[0] <= item_date < request_end_date:
                matching_occupancy.append((occupant, item))
    if not matching_occupancy or any(
        occupant.get("asset_id") != current_asset_id
        or occupant.get("cusip") != current_cusip
        or item.get("action_id") not in supporting_action_ids
        for occupant, item in matching_occupancy
    ):
        return None

    return CurrentSymbolReuseAssessment(
        symbol=symbol,
        current_asset_id=current_asset_id,
        current_cusip=current_cusip,
        current_name=current_name,
        current_exchange=current_exchange,
        vacated_action_id=vacated_action_id,
        vacated_to_symbol=vacated_to_symbol,
        vacancy_date=latest_structural_date,
        previous_asset_id=previous_asset_id,
        previous_cusip=previous_cusip,
        superseded_interval_asset_id=superseded_interval.asset_id,
        superseded_interval_cusip=superseded_interval.cusip,
        vacancy_session_count=vacancy.session_count,
        first_session=session_dates[0],
        last_session=session_dates[-1],
        session_count=len(session_dates),
        request_end_exclusive=request_end,
        supporting_action_ids=tuple(sorted(supporting_action_ids)),
        adjustment_consensus=consensus,
    )
