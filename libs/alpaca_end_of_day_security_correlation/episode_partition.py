from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    expected_market_session_coverage,
    market_session_dates_sha256,
)
from libs.download_alpaca_end_of_day_bars import ALPACA_ADJUSTMENT_MODES

from .authenticated_outputs import authenticate_security_correlation
from .errors import SecurityCorrelationError
from .frozen_downloads import read_authenticated_frozen_download
from .uuid_lineage import (
    IDENTITY_PRESERVING_SPLIT_EDGE_TYPES,
    UuidLineageEdge,
    build_uuid_lineage_edges,
)

NEW_YORK_TIME_ZONE = ZoneInfo("America/New_York")
PARTITION_RULE_NAME = "continuous_action_supported_uuid_episodes"


@dataclass(frozen=True)
class ContinuousUuidEpisode:
    asset_id: str
    first_session: date
    last_session: date
    session_count: int
    session_dates_sha256: str

    def as_report_object(self) -> dict[str, Any]:
        return {
            "alpaca_asset_id": self.asset_id,
            "first_session": self.first_session.isoformat(),
            "last_session": self.last_session.isoformat(),
            "session_count": self.session_count,
            "session_dates_sha256": self.session_dates_sha256,
        }


@dataclass(frozen=True)
class DistinctUuidEpisodePartition:
    asset_ids: tuple[str, ...]
    episodes: tuple[ContinuousUuidEpisode, ...]
    separating_session_count: int
    separating_sessions_sha256: str
    action_edges: tuple[tuple[str, str, str, str], ...]
    supporting_symbols: tuple[str, ...]
    supporting_downloads: tuple[tuple[str, str, str], ...]

    def as_report_object(self) -> dict[str, Any]:
        return {
            "rule": PARTITION_RULE_NAME,
            "alpaca_asset_ids": list(self.asset_ids),
            "episodes": [episode.as_report_object() for episode in self.episodes],
            "separating_session_count": self.separating_session_count,
            "separating_sessions_sha256": self.separating_sessions_sha256,
            "action_edges": [
                {
                    "corporate_action_id": action_id,
                    "effective_date": effective_date,
                    "predecessor_alpaca_asset_id": predecessor_id,
                    "successor_alpaca_asset_id": successor_id,
                }
                for action_id, effective_date, predecessor_id, successor_id in self.action_edges
            ],
            "supporting_symbols": list(self.supporting_symbols),
            "supporting_downloads": [
                {
                    "symbol": symbol,
                    "file": filename,
                    "sha256": download_sha256,
                }
                for symbol, filename, download_sha256 in self.supporting_downloads
            ],
        }


def _session_date(timestamp: object) -> date:
    if not isinstance(timestamp, str):
        raise SecurityCorrelationError("episode-partition evidence has a non-string timestamp")
    normalized = timestamp[:-1] + "+00:00" if timestamp.endswith("Z") else timestamp
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise SecurityCorrelationError(
            f"episode-partition evidence has an invalid timestamp: {timestamp!r}"
        ) from error
    if parsed.tzinfo is None:
        raise SecurityCorrelationError(f"episode-partition evidence timestamp has no timezone: {timestamp!r}")
    return parsed.astimezone(timezone.utc).astimezone(NEW_YORK_TIME_ZONE).date()


def _distinct_uuid_components(edges: list[UuidLineageEdge]) -> tuple[frozenset[str], ...]:
    neighbours: dict[str, set[str]] = {}
    for edge in edges:
        if edge.predecessor_asset_id == edge.successor_asset_id:
            continue
        neighbours.setdefault(edge.predecessor_asset_id, set()).add(edge.successor_asset_id)
        neighbours.setdefault(edge.successor_asset_id, set()).add(edge.predecessor_asset_id)
    components = []
    unseen = set(neighbours)
    while unseen:
        first = min(unseen)
        component = {first}
        pending = [first]
        while pending:
            asset_id = pending.pop()
            for neighbour in neighbours.get(asset_id, ()):
                if neighbour not in component:
                    component.add(neighbour)
                    pending.append(neighbour)
        unseen.difference_update(component)
        components.append(frozenset(component))
    return tuple(sorted(components, key=lambda item: tuple(sorted(item))))


def _component_symbols(component: frozenset[str], edges: list[UuidLineageEdge]) -> tuple[str, ...]:
    symbols = set()
    for edge in edges:
        if edge.predecessor_asset_id in component or edge.successor_asset_id in component:
            symbols.add(edge.predecessor_symbol)
            symbols.add(edge.successor_symbol)
    return tuple(sorted(symbols))


def _continuous_sessions(sessions: tuple[date, ...]) -> bool:
    if not sessions:
        return False
    try:
        expected = expected_market_session_coverage(
            sessions[0].isoformat(),
            (sessions[-1] + timedelta(days=1)).isoformat(),
        ).session_dates
    except MarketCalendarError:
        return False
    return tuple(item.isoformat() for item in sessions) == expected


def _qualify_component(
    component: frozenset[str],
    edges: list[UuidLineageEdge],
    sessions_by_asset_adjustment: dict[tuple[str, str], set[date]],
    supporting_symbols: set[str],
    supporting_downloads: set[tuple[str, str, str]],
) -> DistinctUuidEpisodePartition | None:
    sessions_by_asset: dict[str, tuple[date, ...]] = {}
    episodes = []
    for asset_id in sorted(component):
        adjustment_grids = []
        for adjustment in ALPACA_ADJUSTMENT_MODES:
            sessions = tuple(sorted(sessions_by_asset_adjustment.get((asset_id, adjustment), set())))
            if not sessions:
                return None
            adjustment_grids.append(sessions)
        if any(grid != adjustment_grids[0] for grid in adjustment_grids[1:]):
            return None
        sessions = adjustment_grids[0]
        if not _continuous_sessions(sessions):
            return None
        sessions_by_asset[asset_id] = sessions
        episodes.append(
            ContinuousUuidEpisode(
                asset_id=asset_id,
                first_session=sessions[0],
                last_session=sessions[-1],
                session_count=len(sessions),
                session_dates_sha256=market_session_dates_sha256(item.isoformat() for item in sessions),
            )
        )

    occupied_by_asset = {asset_id: set(sessions) for asset_id, sessions in sessions_by_asset.items()}
    for asset_id, occupied in occupied_by_asset.items():
        if any(
            occupied.intersection(other) for other_id, other in occupied_by_asset.items() if other_id != asset_id
        ):
            return None

    union_sessions = tuple(sorted(set().union(*occupied_by_asset.values())))
    try:
        expected_union = expected_market_session_coverage(
            union_sessions[0].isoformat(),
            (union_sessions[-1] + timedelta(days=1)).isoformat(),
        ).session_dates
    except MarketCalendarError:
        return None
    observed_union = {item.isoformat() for item in union_sessions}
    separating_sessions = tuple(item for item in expected_union if item not in observed_union)
    if not separating_sessions:
        return None

    action_edges = tuple(
        sorted(
            (
                edge.action_id,
                edge.effective_date.isoformat(),
                edge.predecessor_asset_id,
                edge.successor_asset_id,
            )
            for edge in edges
            if edge.predecessor_asset_id in component
            and edge.successor_asset_id in component
            and edge.predecessor_asset_id != edge.successor_asset_id
        )
    )
    if not action_edges:
        return None
    return DistinctUuidEpisodePartition(
        asset_ids=tuple(sorted(component)),
        episodes=tuple(episodes),
        separating_session_count=len(separating_sessions),
        separating_sessions_sha256=market_session_dates_sha256(separating_sessions),
        action_edges=action_edges,
        supporting_symbols=tuple(sorted(supporting_symbols)),
        supporting_downloads=tuple(sorted(supporting_downloads)),
    )


def assess_distinct_uuid_episode_partitions(
    repository_root: Path,
    symbol: str,
    lineage: dict[str, Any],
) -> tuple[DistinctUuidEpisodePartition, ...]:
    """Return action-linked UUID groups whose individual price episodes are continuous."""
    edges, issues = build_uuid_lineage_edges(lineage)
    if issues:
        return ()
    partitionable_edges = [edge for edge in edges if edge.edge_type not in IDENTITY_PRESERVING_SPLIT_EDGE_TYPES]
    matches = []
    for component in _distinct_uuid_components(partitionable_edges):
        candidate_symbols = set(_component_symbols(component, partitionable_edges))
        candidate_symbols.add(symbol)
        sessions_by_asset_adjustment: dict[tuple[str, str], set[date]] = {}
        supporting_symbols: set[str] = set()
        supporting_downloads: set[tuple[str, str, str]] = set()
        current_symbol_participates = False
        component_invalid = False
        for candidate_symbol in sorted(candidate_symbols):
            try:
                correlation = authenticate_security_correlation(repository_root, candidate_symbol)
            except (OSError, SecurityCorrelationError):
                continue
            relevant_segments = [
                segment
                for segment in correlation.correlated["segments"]
                if segment.get("canonical_security", {}).get("alpaca_asset_id") in component
            ]
            if not relevant_segments:
                continue
            if candidate_symbol == symbol:
                current_symbol_participates = True
            supporting_symbols.add(candidate_symbol)
            rows_by_filename = {}
            for segment in relevant_segments:
                if segment.get("identity_confidence") != "action_supported" or not isinstance(
                    segment.get("ticker_ownership_interval"), dict
                ):
                    component_invalid = True
                    break
                filename = segment["data_file"]
                if filename not in rows_by_filename:
                    authenticated = read_authenticated_frozen_download(
                        repository_root,
                        f"data/end_of_day/{candidate_symbol}",
                        {"file": filename},
                    )
                    source = correlation.sources_by_filename[filename]
                    if authenticated.manifest.get("sha256") != source.get("download_sha256"):
                        component_invalid = True
                        break
                    rows_by_filename[filename] = authenticated.rows
                    supporting_downloads.add((candidate_symbol, filename, authenticated.manifest["sha256"]))
                asset_id = segment["canonical_security"]["alpaca_asset_id"]
                adjustment = segment["adjustment"]
                selected_rows = rows_by_filename[filename][segment["row_start"] - 1 : segment["row_end"]]
                sessions_by_asset_adjustment.setdefault((asset_id, adjustment), set()).update(
                    _session_date(row.get("t")) for row in selected_rows
                )
            if component_invalid:
                break
        if component_invalid or not current_symbol_participates:
            continue
        match = _qualify_component(
            component,
            partitionable_edges,
            sessions_by_asset_adjustment,
            supporting_symbols,
            supporting_downloads,
        )
        if match is not None:
            matches.append(match)
    return tuple(matches)
