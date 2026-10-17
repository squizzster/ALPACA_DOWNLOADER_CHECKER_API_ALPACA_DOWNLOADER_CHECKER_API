"""The 18-column conversion owns no pricing, identity or provider acquisition."""

import math
import re
from datetime import date, datetime, timedelta

from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .factors import infer_split_factors

COLUMNS = (
    "date",
    "ohlcv_series_id",
    "symbol",
    "cik",
    "composite_figi",
    "share_class_figi",
    "company_name",
    "security_type",
    "exchange",
    "adjusted_open",
    "adjusted_high",
    "adjusted_low",
    "adjusted_close",
    "raw_close",
    "volume",
    "dollar_volume",
    "adjustment_ratio",
    "split_adjustment_ratio",
)


class ExportError(ValueError):
    """A requested export cannot be published as specified."""


def series_id(public_id):
    if not isinstance(public_id, str) or re.fullmatch(r"[0-9a-f]{64}", public_id) is None:
        raise ExportError("public_id must be a lowercase 64-hex identity")
    return 1 + int(public_id, 16) % (2**63 - 1)


def checked_ids(public_ids):
    mapping, reverse = {}, {}
    for public_id in sorted(set(public_ids)):
        value = series_id(public_id)
        if value in reverse:
            raise ExportError(f"int64 identity collision: {public_id} and {reverse[value]}")
        mapping[public_id], reverse[value] = value, public_id
    return mapping


def _float(value, label, session, *, zero=False):
    try:
        result = float(value)
    except (ValueError, TypeError, OverflowError) as error:
        raise ExportError(f"{session}: invalid {label}") from error
    if not math.isfinite(result) or (result < 0 if zero else result <= 0):
        raise ExportError(f"{session}: {label} must be finite and {'nonnegative' if zero else 'positive'}")
    return result


def project_history(history, identifier, *, start=None, end=None):
    """Return rows and mapping evidence; infer before applying any date filter.

    Labels come from the full identity history. Unknown dated type/exchange and
    optional identifiers are not invented from present-day asset lookups.
    """
    public_id = history["security"]["public_id"]
    if identifier != series_id(public_id):
        raise ExportError("series ID differs from the checked identity mapping")
    evidence = history["evidence"]
    security = next(item for item in evidence["securities"] if item["public_id"] == public_id)
    latest = security["latest_tickers"]
    if len(latest) != 1 or not latest[0]:
        raise ExportError("latest observed ticker is ambiguous")
    views = history["views"]
    raw, adjusted = views["raw"]["bars"], views["all"]["bars"]
    factors, inference = infer_split_factors(views)
    sessions = [row["session"] for row in raw]
    allowed = set()
    if sessions:
        for session in sessions:
            if date.fromisoformat(session).isoformat() != session:
                raise ExportError("invalid session date")
        allowed = set(
            expected_market_session_coverage(
                sessions[0], (date.fromisoformat(sessions[-1]) + timedelta(days=1)).isoformat()
            ).session_dates
        )
    rows = []
    for original, full, split in zip(raw, adjusted, factors, strict=True):
        session = original["session"]
        if session not in allowed:
            raise ExportError(f"{session}: not in the pinned market calendar")
        o, h, low, c = (_float(full[key], f"all.{key}", session) for key in ("open", "high", "low", "close"))
        if not (low <= o <= h and low <= c <= h):
            raise ExportError(f"{session}: invalid all-adjusted OHLC geometry")
        close = _float(original["close"], "raw.close", session)
        volume = _float(original["volume"], "raw.volume", session, zero=True)
        dollars = _float(close * volume, "dollar_volume", session, zero=True)
        ratio = _float(c / close, "adjustment_ratio", session)
        if (start and session < start) or (end and session > end):
            continue
        rows.append(
            dict(
                zip(
                    COLUMNS,
                    (
                        datetime.combine(date.fromisoformat(session), datetime.min.time()),
                        identifier,
                        latest[0],
                        None,
                        None,
                        None,
                        None,
                        "UNKNOWN",
                        "UNKNOWN",
                        o,
                        h,
                        low,
                        c,
                        close,
                        volume,
                        dollars,
                        ratio,
                        split,
                    ),
                    strict=True,
                )
            )
        )
    # Compare labels once across all eight aligned grids, independently of clipping.
    for name, view in views.items():
        if any(a["tickers"] != b["tickers"] for a, b in zip(raw, view["bars"], strict=True)):
            raise ExportError(f"{name}: historical ticker assignments differ from raw")
    return rows, {
        "public_id": public_id,
        "ohlcv_series_id": identifier,
        "symbol": latest[0],
        "aliases": security["tickers"],
        "generation": history["generation"],
        "row_count": len(rows),
        "first_session": rows[0]["date"].date().isoformat() if rows else None,
        "last_session": rows[-1]["date"].date().isoformat() if rows else None,
        "full_history_first_session": sessions[0] if sessions else None,
        "full_history_last_session": sessions[-1] if sessions else None,
        "factor_inference": inference,
    }
