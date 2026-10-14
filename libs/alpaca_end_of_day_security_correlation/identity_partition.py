from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    expected_market_session_coverage,
    market_session_dates_sha256,
)
from libs.download_alpaca_end_of_day_bars import ALPACA_ADJUSTMENT_MODES

from .errors import SecurityCorrelationError
from .frozen_downloads import read_authenticated_frozen_download

NEW_YORK_TIME_ZONE = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class DistinctUuidIdentity:
    symbol: str
    asset_id: str
    cusip: str | None

    def as_report_object(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "alpaca_asset_id": self.asset_id,
            "cusip": self.cusip,
        }


@dataclass(frozen=True)
class OneSessionDistinctUuidTransition:
    action_id: str
    action_date: date
    missing_session: date
    left_observed_session: date
    right_observed_session: date
    predecessor: DistinctUuidIdentity
    successor: DistinctUuidIdentity
    authenticated_adjustment_count: int
    union_session_dates_sha256: str
    supporting_downloads: tuple[tuple[str, str, str], ...]

    def as_report_object(self) -> dict[str, Any]:
        return {
            "rule": "one_session_distinct_uuid_transition",
            "corporate_action_id": self.action_id,
            "action_date": self.action_date.isoformat(),
            "missing_session": self.missing_session.isoformat(),
            "left_observed_session": self.left_observed_session.isoformat(),
            "right_observed_session": self.right_observed_session.isoformat(),
            "predecessor": self.predecessor.as_report_object(),
            "successor": self.successor.as_report_object(),
            "authenticated_adjustment_count": self.authenticated_adjustment_count,
            "union_session_dates_sha256": self.union_session_dates_sha256,
            "supporting_downloads": [
                {
                    "symbol": symbol,
                    "adjustment": adjustment,
                    "sha256": download_sha256,
                }
                for symbol, adjustment, download_sha256 in self.supporting_downloads
            ],
        }


@dataclass(frozen=True)
class _TransitionCandidate:
    action_id: str
    action_date: date
    predecessor: DistinctUuidIdentity
    successor: DistinctUuidIdentity


def _exact_symbol_lookup_asset_ids(lineage: dict[str, Any], symbol: str) -> set[str]:
    asset_ids = set()
    lookups = lineage.get("asset_lookups")
    for lookup in lookups if isinstance(lookups, list) else ():
        if (
            not isinstance(lookup, dict)
            or lookup.get("lookup_kind") != "symbol"
            or lookup.get("lookup") != symbol
            or lookup.get("found") is not True
        ):
            continue
        asset = lookup.get("asset")
        if (
            isinstance(asset, dict)
            and asset.get("symbol") == symbol
            and isinstance(asset.get("id"), str)
            and asset["id"]
        ):
            asset_ids.add(asset["id"])
    return asset_ids


def _identity_for_exact_symbol(lineage: dict[str, Any], symbol: str) -> DistinctUuidIdentity | None:
    identities = lineage.get("identities")
    matching = [
        identity
        for identity in (identities if isinstance(identities, list) else ())
        if isinstance(identity, dict)
        and identity.get("symbol") == symbol
        and isinstance(identity.get("id"), str)
        and identity["id"]
    ]
    if len(matching) != 1:
        return None
    identity = matching[0]
    asset_id = identity["id"]
    if _exact_symbol_lookup_asset_ids(lineage, symbol) != {asset_id}:
        return None
    lookup_cusips = identity.get("lookup_cusips")
    if not isinstance(lookup_cusips, list):
        return None
    cusips = {value for value in lookup_cusips if isinstance(value, str) and value}
    if len(cusips) > 1:
        return None
    return DistinctUuidIdentity(
        symbol=symbol,
        asset_id=asset_id,
        cusip=next(iter(cusips), None),
    )


def _transition_candidates(lineage: dict[str, Any]) -> tuple[_TransitionCandidate, ...]:
    actions = lineage.get("corporate_actions")
    if not isinstance(actions, list):
        return ()
    name_changes_by_pair: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for action in actions:
        if not isinstance(action, dict) or action.get("action_type") != "name_change":
            continue
        old_symbol = action.get("old_symbol")
        new_symbol = action.get("new_symbol")
        if (
            not isinstance(old_symbol, str)
            or not old_symbol
            or not isinstance(new_symbol, str)
            or not new_symbol
            or old_symbol == new_symbol
        ):
            continue
        name_changes_by_pair.setdefault((old_symbol, new_symbol), []).append(action)

    candidates = []
    for (old_symbol, new_symbol), matching_actions in name_changes_by_pair.items():
        if len(matching_actions) != 1:
            continue
        predecessor = _identity_for_exact_symbol(lineage, old_symbol)
        successor = _identity_for_exact_symbol(lineage, new_symbol)
        if predecessor is None or successor is None or predecessor.asset_id == successor.asset_id:
            continue
        action = matching_actions[0]
        action_id = action.get("id")
        action_date_value = action.get("process_date")
        if not isinstance(action_id, str) or not action_id or not isinstance(action_date_value, str):
            continue
        try:
            action_date = date.fromisoformat(action_date_value)
        except ValueError:
            continue
        candidates.append(
            _TransitionCandidate(
                action_id=action_id,
                action_date=action_date,
                predecessor=predecessor,
                successor=successor,
            )
        )
    return tuple(candidates)


def _session_date(timestamp: object) -> date:
    if not isinstance(timestamp, str):
        raise SecurityCorrelationError("authenticated transition evidence has a non-string timestamp")
    normalized = timestamp[:-1] + "+00:00" if timestamp.endswith("Z") else timestamp
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise SecurityCorrelationError(
            f"authenticated transition evidence has an invalid timestamp: {timestamp!r}"
        ) from error
    if parsed.tzinfo is None:
        raise SecurityCorrelationError(
            f"authenticated transition evidence timestamp has no timezone: {timestamp!r}"
        )
    return parsed.astimezone(timezone.utc).astimezone(NEW_YORK_TIME_ZONE).date()


def _authenticated_symbol_sessions(
    repository_root: Path,
    symbol: str,
) -> tuple[tuple[date, ...], tuple[tuple[str, str, str], ...]] | None:
    sessions_by_adjustment = []
    supporting_downloads = []
    for adjustment in ALPACA_ADJUSTMENT_MODES:
        filename = f"{adjustment.replace(',', '_')}.csv.gz"
        path = repository_root / "data" / "end_of_day" / symbol / filename
        if not path.is_file() or not path.with_name(path.name + ".manifest.json").is_file():
            return None
        authenticated = read_authenticated_frozen_download(
            repository_root,
            f"data/end_of_day/{symbol}",
            {"file": filename},
        )
        sessions = tuple(_session_date(row.get("t")) for row in authenticated.rows)
        if len(set(sessions)) != len(sessions) or tuple(sorted(sessions)) != sessions:
            raise SecurityCorrelationError(
                f"authenticated {symbol} {adjustment} sessions are not strictly increasing"
            )
        sessions_by_adjustment.append(sessions)
        supporting_downloads.append((symbol, adjustment, authenticated.manifest["sha256"]))
    if not sessions_by_adjustment or any(
        sessions != sessions_by_adjustment[0] for sessions in sessions_by_adjustment[1:]
    ):
        return None
    return sessions_by_adjustment[0], tuple(supporting_downloads)


def _assess_candidate(
    candidate: _TransitionCandidate,
    sessions_by_symbol: dict[str, tuple[date, ...]],
    supporting_downloads: tuple[tuple[str, str, str], ...],
) -> OneSessionDistinctUuidTransition | None:
    predecessor_sessions = sessions_by_symbol.get(candidate.predecessor.symbol)
    successor_sessions = sessions_by_symbol.get(candidate.successor.symbol)
    if not predecessor_sessions or not successor_sessions:
        return None
    observed_sessions = tuple(sorted(set(predecessor_sessions).union(successor_sessions)))
    try:
        coverage = expected_market_session_coverage(
            observed_sessions[0].isoformat(),
            (observed_sessions[-1] + timedelta(days=1)).isoformat(),
        )
    except MarketCalendarError:
        return None
    expected_sessions = tuple(date.fromisoformat(value) for value in coverage.session_dates)
    expected_set = set(expected_sessions)
    if any(session not in expected_set for session in observed_sessions):
        return None
    missing = tuple(session for session in expected_sessions if session not in set(observed_sessions))
    if len(missing) != 1:
        return None
    missing_position = expected_sessions.index(missing[0])
    if missing_position == 0 or missing_position + 1 >= len(expected_sessions):
        return None
    left_observed = expected_sessions[missing_position - 1]
    right_observed = expected_sessions[missing_position + 1]
    if candidate.action_date not in {missing[0], right_observed}:
        return None
    return OneSessionDistinctUuidTransition(
        action_id=candidate.action_id,
        action_date=candidate.action_date,
        missing_session=missing[0],
        left_observed_session=left_observed,
        right_observed_session=right_observed,
        predecessor=candidate.predecessor,
        successor=candidate.successor,
        authenticated_adjustment_count=len(supporting_downloads),
        union_session_dates_sha256=market_session_dates_sha256(
            session.isoformat() for session in observed_sessions
        ),
        supporting_downloads=supporting_downloads,
    )


def assess_one_session_distinct_uuid_transition(
    repository_root: Path,
    lineage: dict[str, Any],
    *,
    authenticated_symbol_sessions: Callable[
        [str], tuple[tuple[date, ...], tuple[tuple[str, str, str], ...]] | None
    ]
    | None = None,
) -> OneSessionDistinctUuidTransition | None:
    """Resolve one absent session between two exact, distinct ticker UUIDs."""
    matches = []
    for candidate in _transition_candidates(lineage):
        sessions_by_symbol = {}
        supporting_downloads = []
        for identity in (candidate.predecessor, candidate.successor):
            try:
                authenticated = (
                    _authenticated_symbol_sessions(repository_root, identity.symbol)
                    if authenticated_symbol_sessions is None
                    else authenticated_symbol_sessions(identity.symbol)
                )
            except (KeyError, OSError, SecurityCorrelationError, TypeError, ValueError):
                authenticated = None
            if authenticated is None:
                sessions_by_symbol = {}
                break
            sessions, downloads = authenticated
            sessions_by_symbol[identity.symbol] = sessions
            supporting_downloads.extend(downloads)
        if not sessions_by_symbol:
            continue
        assessment = _assess_candidate(
            candidate,
            sessions_by_symbol,
            tuple(sorted(supporting_downloads)),
        )
        if assessment is not None:
            matches.append(assessment)
    if len(matches) != 1:
        return None
    return matches[0]
