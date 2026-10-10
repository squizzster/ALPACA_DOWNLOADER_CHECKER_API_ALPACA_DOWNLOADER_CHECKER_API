from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from libs.alpaca_end_of_day_pipeline import (
    EndOfDayPipelineError,
    authenticate_frozen_download,
    is_accepted_zero_activity_bar,
    read_frozen_alpaca_daily_bars,
)
from libs.alpaca_end_of_day_reader import (
    EndOfDayReaderError,
    read_alpaca_end_of_day_security_history,
)
from libs.alpaca_end_of_day_security_correlation import (
    LINEAGE_GAP_AVAILABILITY_STATUS,
    OFFLINE_AVAILABILITY_STATUS,
    ONLINE_AVAILABILITY_STATUS,
    SecurityCorrelationError,
    authenticate_security_correlation,
    authenticate_security_master,
    is_master_id,
    security_correlation_directory,
)
from libs.alpaca_end_of_day_security_correlation.publication import lineage_evidence_sha256
from libs.alpaca_end_of_day_security_correlation.session_exclusions import (
    validate_verified_session_exclusions,
    verified_exclusion_session_dates,
)
from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    compare_observed_to_expected_sessions,
    expected_market_session_coverage,
    us_market_hours_sha256,
)

NEW_YORK_TIME_ZONE = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class SecurityOnlineEvaluation:
    online_by_master_id: dict[str, bool]
    availability_status_by_master_id: dict[str, str]
    security_object_sha256_by_master_id: dict[str, str]
    source_object_sha256_by_symbol: dict[str, str]


def _bar_session_date(bar: object) -> str | None:
    timestamp_text = bar.get("timestamp") if isinstance(bar, dict) else None
    if not isinstance(timestamp_text, str):
        return None
    try:
        timestamp = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if timestamp.tzinfo is None:
        return None
    return timestamp.astimezone(NEW_YORK_TIME_ZONE).date().isoformat()


def _session_grid_has_unresolved_boundary(
    bars: object,
    verified_exclusion_dates: frozenset[str] = frozenset(),
) -> tuple[bool, bool]:
    """Return (has_internal_gap, has_invalid_or_unexpected_session)."""
    if not isinstance(bars, list) or not bars:
        return False, True
    observed = []
    for bar in bars:
        session_date = _bar_session_date(bar)
        if session_date is None:
            return False, True
        observed.append(session_date)
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
    unresolved_missing = set(missing) - verified_exclusion_dates
    return bool(unresolved_missing), bool(unexpected)


def _positive_raw_activity_during_exclusion(
    bars: object,
    verified_exclusion_dates: frozenset[str],
) -> tuple[bool, bool]:
    """Return (has_positive_activity_in_exclusion, has_invalid_bar)."""
    if not isinstance(bars, list) or not bars:
        return False, True
    for bar in bars:
        session_date = _bar_session_date(bar)
        if session_date is None:
            return False, True
        if session_date not in verified_exclusion_dates:
            continue
        if not isinstance(bar, dict):
            return False, True
        try:
            values = {
                field_name: Decimal(str(bar[field_name]))
                for field_name in (
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "trade_count",
                    "vwap",
                )
            }
        except (KeyError, InvalidOperation, TypeError, ValueError):
            return False, True
        if any(not value.is_finite() for value in values.values()):
            return False, True
        if not is_accepted_zero_activity_bar(
            open_price=values["open"],
            high_price=values["high"],
            low_price=values["low"],
            close_price=values["close"],
            share_volume=values["volume"],
            trade_count=values["trade_count"],
            volume_weighted_average_price=values["vwap"],
        ):
            return True, False
    return False, False


def _verified_exclusion_dates_for_identity(
    master: object,
    identity: dict[str, Any],
) -> frozenset[str]:
    dates = set()
    member_asset_ids = set(identity["alpaca_asset_ids"])
    cusips = set(identity["cusips"])
    exchanges = set(identity["exchanges"])
    for symbol in identity["source_symbols"]:
        source = master.sources_by_symbol[symbol]
        exclusions = source.get("verified_session_exclusions")
        if exclusions is None:
            continue
        validate_verified_session_exclusions(exclusions, symbol)
        if exclusions["calendar_sha256"] != us_market_hours_sha256():
            raise SecurityCorrelationError(
                f"verified session exclusions for {symbol} use a different market calendar"
            )
        for event in exclusions["events"]:
            if event["alpaca_asset_id"] not in member_asset_ids:
                continue
            if event["cusip"] not in cusips or event["exchange"] not in exchanges:
                raise SecurityCorrelationError(
                    f"verified session exclusion for {symbol} does not match its master identity"
                )
            dates.update(verified_exclusion_session_dates(event))
    return frozenset(dates)


def _authenticated_lineage_evidence_for_identity(
    repository_root: Path,
    master: object,
    identity: dict[str, Any],
) -> tuple[tuple[dict[str, Any], ...], frozenset[str]]:
    """Return bound lineage receipts and authenticated raw source session dates."""
    receipts = []
    raw_source_session_dates = set()
    for symbol in identity["source_symbols"]:
        source = master.sources_by_symbol[symbol]
        authenticated = authenticate_security_correlation(repository_root, symbol)
        receipt = authenticated.receipt
        lineage_report = receipt.get("lineage_report")
        if (
            receipt.get("correlation_version") != source.get("correlation_version")
            or receipt.get("correlation_generation_sha256") != source.get("correlation_generation_sha256")
            or authenticated.correlated_sha256 != source.get("correlated_output_sha256")
            or authenticated.uncorrelated_sha256 != source.get("uncorrelated_output_sha256")
            or not isinstance(lineage_report, dict)
            or lineage_evidence_sha256(lineage_report) != receipt.get("lineage_evidence_sha256")
        ):
            raise SecurityCorrelationError(
                f"lineage evidence for {symbol} is not bound to the authenticated security master"
            )
        receipts.append(receipt)

        raw_sources = [
            (filename, raw_source)
            for filename, raw_source in authenticated.sources_by_filename.items()
            if raw_source.get("adjustment") == "raw"
        ]
        if len(raw_sources) != 1:
            raise SecurityCorrelationError(f"lineage evidence for {symbol} has no unique raw source")
        filename, raw_source = raw_sources[0]
        download_path = security_correlation_directory(repository_root, symbol) / filename
        authenticate_frozen_download(download_path, {"sha256": raw_source["download_sha256"]})
        for bar in read_frozen_alpaca_daily_bars(download_path):
            session_date = _bar_session_date({"timestamp": bar.get("t")})
            if session_date is None:
                raise SecurityCorrelationError(f"raw lineage source for {symbol} has an invalid timestamp")
            raw_source_session_dates.add(session_date)
    return tuple(receipts), frozenset(raw_source_session_dates)


def _observed_tickers_on_session(identity: dict[str, Any], session_date: str) -> frozenset[str]:
    tickers = set()
    ranges = identity.get("observed_history_ranges")
    if not isinstance(ranges, list):
        return frozenset()
    for observed_range in ranges:
        if not isinstance(observed_range, dict):
            return frozenset()
        ticker = observed_range.get("ticker")
        first_session = observed_range.get("first_session_date")
        last_session = observed_range.get("last_session_date")
        if not all(isinstance(value, str) and value for value in (ticker, first_session, last_session)):
            return frozenset()
        if first_session <= session_date <= last_session:
            tickers.add(ticker)
    return frozenset(tickers)


MAX_NAME_CHANGE_BOUNDARY_GAP_SESSIONS = 5


def _short_same_security_name_change_exclusions(
    identity: dict[str, Any],
    results_by_adjustment: dict[str, dict[str, Any]],
    lineage_receipts: tuple[dict[str, Any], ...],
    raw_source_session_dates: frozenset[str],
) -> frozenset[str]:
    """Resolve short gaps at an unambiguous same-UUID, same-CUSIP ticker rename."""
    asset_ids = identity.get("alpaca_asset_ids")
    cusips = identity.get("cusips")
    exchanges = identity.get("exchanges")
    adjustments = identity.get("available_adjustments")
    if (
        not isinstance(asset_ids, list)
        or len(asset_ids) != 1
        or not isinstance(cusips, list)
        or len(cusips) != 1
        or not isinstance(cusips[0], str)
        or not cusips[0]
        or not isinstance(exchanges, list)
        or len(exchanges) != 1
        or not isinstance(adjustments, list)
        or len(adjustments) != 8
        or set(results_by_adjustment) != set(adjustments)
        or not lineage_receipts
    ):
        return frozenset()
    asset_id = asset_ids[0]
    cusip = cusips[0]

    common_session_dates = None
    for adjustment in adjustments:
        bars = results_by_adjustment[adjustment].get("bars")
        if not isinstance(bars, list) or not bars:
            return frozenset()
        session_dates = tuple(_bar_session_date(bar) for bar in bars)
        if any(session_date is None for session_date in session_dates):
            return frozenset()
        if len(set(session_dates)) != len(session_dates):
            return frozenset()
        if common_session_dates is None:
            common_session_dates = session_dates
        elif session_dates != common_session_dates:
            return frozenset()
    if common_session_dates is None:
        return frozenset()

    try:
        end_exclusive = (date.fromisoformat(common_session_dates[-1]) + timedelta(days=1)).isoformat()
        coverage = expected_market_session_coverage(common_session_dates[0], end_exclusive)
    except (MarketCalendarError, ValueError):
        return frozenset()
    observed_dates = frozenset(common_session_dates)

    actions_by_id: dict[str, dict[str, Any]] = {}
    lookup_records = []
    identity_records = []
    for receipt in lineage_receipts:
        if receipt.get("evidence_issues") != []:
            return frozenset()
        lineage_report = receipt.get("lineage_report")
        lineage = lineage_report.get("lineage") if isinstance(lineage_report, dict) else None
        if not isinstance(lineage, dict) or lineage.get("unresolved_cusips") != []:
            return frozenset()
        actions = lineage.get("corporate_actions")
        lookups = lineage.get("asset_lookups")
        identities = lineage.get("identities")
        if (
            not isinstance(actions, list)
            or not isinstance(lookups, list)
            or not isinstance(identities, list)
            or any(not isinstance(item, dict) for item in identities)
        ):
            return frozenset()
        lookup_records.extend(lookups)
        identity_records.extend(identities)
        for action in actions:
            if not isinstance(action, dict):
                return frozenset()
            action_id = action.get("id")
            if not isinstance(action_id, str) or not action_id:
                continue
            previous = actions_by_id.setdefault(action_id, action)
            if previous != action:
                return frozenset()

    identity_event_ids = {
        event.get("event_id")
        for event in identity.get("identity_events", [])
        if isinstance(event, dict) and event.get("action_type") == "name_change"
    }

    def resolved_lookup_ids(lookup_kind: str, lookup_value: str) -> tuple[bool, frozenset[str]]:
        matching = [
            record
            for record in lookup_records
            if isinstance(record, dict)
            and record.get("lookup_kind") == lookup_kind
            and record.get("lookup") == lookup_value
        ]
        if not matching:
            return False, frozenset()
        found_ids = set()
        for record in matching:
            outcome = record.get("outcome")
            asset = record.get("asset")
            if outcome == "not_found" and asset is None:
                continue
            if outcome != "found" or not isinstance(asset, dict):
                return False, frozenset()
            found_id = asset.get("id")
            if not isinstance(found_id, str) or not found_id:
                return False, frozenset()
            found_ids.add(found_id)
        return True, frozenset(found_ids)

    cusip_seen, cusip_ids = resolved_lookup_ids("cusip", cusip)
    if not cusip_seen or cusip_ids != frozenset((asset_id,)):
        return frozenset()

    exclusions = set()
    expected_dates = coverage.session_dates
    index = 0
    while index < len(expected_dates):
        if expected_dates[index] in observed_dates:
            index += 1
            continue
        missing_start = index
        while index < len(expected_dates) and expected_dates[index] not in observed_dates:
            index += 1
        missing_dates = expected_dates[missing_start:index]
        if (
            missing_start == 0
            or index == len(expected_dates)
            or len(missing_dates) > MAX_NAME_CHANGE_BOUNDARY_GAP_SESSIONS
        ):
            continue
        left_date = expected_dates[missing_start - 1]
        right_date = expected_dates[index]
        left_tickers = _observed_tickers_on_session(identity, left_date)
        right_tickers = _observed_tickers_on_session(identity, right_date)
        if len(left_tickers) != 1 or len(right_tickers) != 1 or left_tickers == right_tickers:
            continue
        left_ticker = next(iter(left_tickers))
        right_ticker = next(iter(right_tickers))

        matching_actions = [
            action
            for action in actions_by_id.values()
            if action.get("action_type") == "name_change"
            and action.get("old_symbol") == left_ticker
            and action.get("new_symbol") == right_ticker
        ]
        if len(matching_actions) != 1:
            continue
        action = matching_actions[0]
        if (
            action.get("id") not in identity_event_ids
            or action.get("old_cusip") != cusip
            or action.get("new_cusip") != cusip
            or action.get("process_date") not in {*missing_dates, right_date}
        ):
            continue

        boundary_dates = {left_date, *missing_dates, right_date}
        if any(
            candidate.get("id") != action.get("id")
            and candidate.get("process_date") in boundary_dates
            and candidate.get("action_type") != "cash_dividend"
            for candidate in actions_by_id.values()
        ):
            continue
        if not set(missing_dates).isdisjoint(raw_source_session_dates):
            continue

        right_seen, right_ids = resolved_lookup_ids("symbol", right_ticker)
        left_seen, left_ids = resolved_lookup_ids("symbol", left_ticker)
        if (
            not right_seen
            or right_ids != frozenset((asset_id,))
            or not left_seen
            or (left_ids and left_ids != frozenset((asset_id,)))
        ):
            continue
        if any(
            candidate.get("symbol") in {left_ticker, right_ticker} and candidate.get("id") != asset_id
            for candidate in identity_records
        ):
            continue
        exclusions.update(missing_dates)
    return frozenset(exclusions)


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
        reader_ready = isinstance(adjustments, list) and bool(adjustments) and "raw" in adjustments
        lineage_gap = False
        results_by_adjustment = {}
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
            results_by_adjustment[adjustment] = result
        try:
            verified_exclusion_dates = (
                _verified_exclusion_dates_for_identity(master, identity) if reader_ready else frozenset()
            )
        except (KeyError, SecurityCorrelationError):
            reader_ready = False
            verified_exclusion_dates = frozenset()
        if reader_ready:
            raw_has_gap, raw_has_invalid_session = _session_grid_has_unresolved_boundary(
                results_by_adjustment["raw"].get("bars"),
                verified_exclusion_dates,
            )
            if raw_has_gap and not raw_has_invalid_session:
                try:
                    lineage_receipts, raw_source_session_dates = _authenticated_lineage_evidence_for_identity(
                        root,
                        master,
                        identity,
                    )
                except (
                    EndOfDayPipelineError,
                    KeyError,
                    OSError,
                    SecurityCorrelationError,
                    TypeError,
                    ValueError,
                ):
                    lineage_receipts = ()
                    raw_source_session_dates = frozenset()
                verified_exclusion_dates = verified_exclusion_dates.union(
                    _short_same_security_name_change_exclusions(
                        identity,
                        results_by_adjustment,
                        lineage_receipts,
                        raw_source_session_dates,
                    )
                )
        if reader_ready:
            positive_during_exclusion, invalid_raw_bar = _positive_raw_activity_during_exclusion(
                results_by_adjustment["raw"].get("bars"),
                verified_exclusion_dates,
            )
            if positive_during_exclusion or invalid_raw_bar:
                reader_ready = False
        for result in results_by_adjustment.values() if reader_ready else ():
            adjustment_has_gap, adjustment_has_invalid_session = _session_grid_has_unresolved_boundary(
                result.get("bars"),
                verified_exclusion_dates,
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
