from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from libs.alpaca_end_of_day_reader import (
    EndOfDayReaderError,
    read_alpaca_end_of_day_security_history,
)
from libs.alpaca_end_of_day_security_correlation import (
    LINEAGE_GAP_AVAILABILITY_STATUS,
    OFFLINE_AVAILABILITY_STATUS,
    ONLINE_AVAILABILITY_STATUS,
    SecurityCorrelationError,
    authenticate_security_master,
    is_master_id,
)
from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    compare_observed_to_expected_sessions,
)

NEW_YORK_TIME_ZONE = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class SecurityOnlineEvaluation:
    online_by_master_id: dict[str, bool]
    availability_status_by_master_id: dict[str, str]
    security_object_sha256_by_master_id: dict[str, str]
    source_object_sha256_by_symbol: dict[str, str]


def _session_grid_has_unresolved_boundary(bars: object) -> tuple[bool, bool]:
    """Return (has_internal_gap, has_invalid_or_unexpected_session)."""
    if not isinstance(bars, list) or not bars:
        return False, True
    observed = []
    for bar in bars:
        timestamp_text = bar.get("timestamp") if isinstance(bar, dict) else None
        if not isinstance(timestamp_text, str):
            return False, True
        try:
            timestamp = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
        except ValueError:
            return False, True
        if timestamp.tzinfo is None:
            return False, True
        observed.append(timestamp.astimezone(NEW_YORK_TIME_ZONE).date().isoformat())
    observed_session_dates = tuple(sorted(set(observed)))
    if len(observed_session_dates) != len(observed):
        return False, True
    end_exclusive = (datetime.fromisoformat(observed_session_dates[-1]).date() + timedelta(days=1)).isoformat()
    try:
        _, missing, unexpected = compare_observed_to_expected_sessions(
            observed_session_dates,
            observed_session_dates[0],
            end_exclusive,
        )
    except MarketCalendarError:
        return False, True
    return bool(missing), bool(unexpected)


def correlated_master_ids(correlation_report: dict[str, Any]) -> list[str]:
    """Return the exact master IDs contributed by one correlation report."""
    correlated = correlation_report.get("correlated")
    segments = correlated.get("segments") if isinstance(correlated, dict) else None
    if not isinstance(segments, list):
        raise SecurityCorrelationError("cannot identify online candidates without correlated segments")
    master_ids = set()
    for segment in segments:
        canonical_security = segment.get("canonical_security") if isinstance(segment, dict) else None
        master_id = canonical_security.get("security_id") if isinstance(canonical_security, dict) else None
        if not is_master_id(master_id):
            raise SecurityCorrelationError("cannot identify an online candidate with an invalid master ID")
        master_ids.add(master_id)
    return sorted(master_ids)


def evaluate_security_online_status(
    repository_root: Path | str,
    master_ids: Iterable[str],
) -> SecurityOnlineEvaluation:
    """Exercise the public reader and compute final status at the master-ID boundary."""
    root = Path(repository_root)
    selected_master_ids = sorted(set(master_ids))
    if any(not is_master_id(master_id) for master_id in selected_master_ids):
        raise ValueError("online evaluation requires valid master IDs")
    try:
        master = authenticate_security_master(root)
    except SecurityCorrelationError as error:
        raise EndOfDayReaderError(f"cannot authenticate security master: {error}") from error

    online_by_master_id = {}
    availability_status_by_master_id = {}
    security_digests = {}
    source_digests = {}
    for master_id in selected_master_ids:
        try:
            identity = master.identities_by_master_id[master_id]
            security_digests[master_id] = master.identities_by_master_id.object_descriptor(master_id)["sha256"]
        except (KeyError, SecurityCorrelationError) as error:
            raise EndOfDayReaderError(
                f"cannot evaluate unpublished or unauthenticated master ID {master_id}: {error}"
            ) from error
        for symbol in identity["source_symbols"]:
            try:
                source_digests[symbol] = master.sources_by_symbol.object_descriptor(symbol)["sha256"]
            except (KeyError, SecurityCorrelationError) as error:
                raise EndOfDayReaderError(
                    f"cannot evaluate master ID {master_id} with source {symbol}: {error}"
                ) from error

        adjustments = identity.get("available_adjustments")
        reader_ready = isinstance(adjustments, list) and bool(adjustments)
        lineage_gap = False
        for adjustment in adjustments if reader_ready else ():
            try:
                result = read_alpaca_end_of_day_security_history(
                    master_id,
                    adjustment,
                    repository_root=root,
                )
            except (EndOfDayReaderError, OSError, KeyError, TypeError, ValueError):
                reader_ready = False
                break
            if result.get("bar_count", 0) <= 0 or result.get("integrity", {}).get("error_count") != 0:
                reader_ready = False
                break
            adjustment_has_gap, adjustment_has_invalid_session = _session_grid_has_unresolved_boundary(
                result.get("bars")
            )
            if adjustment_has_invalid_session:
                reader_ready = False
                break
            lineage_gap = lineage_gap or adjustment_has_gap
        if not reader_ready:
            availability_status = OFFLINE_AVAILABILITY_STATUS
        elif lineage_gap:
            availability_status = LINEAGE_GAP_AVAILABILITY_STATUS
        else:
            availability_status = ONLINE_AVAILABILITY_STATUS
        availability_status_by_master_id[master_id] = availability_status
        online_by_master_id[master_id] = availability_status == ONLINE_AVAILABILITY_STATUS

    return SecurityOnlineEvaluation(
        online_by_master_id=online_by_master_id,
        availability_status_by_master_id=availability_status_by_master_id,
        security_object_sha256_by_master_id=security_digests,
        source_object_sha256_by_symbol=source_digests,
    )
