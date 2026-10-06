import csv
import gzip
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from datetime import time as datetime_time
from pathlib import Path
from zoneinfo import ZoneInfo

from libs.common.market_data_rule_evaluators import (
    ERROR,
    evaluate_ordered_bounds,
    evaluate_value_within_bounds,
)
from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    compare_observed_to_expected_sessions,
)

from .contracts import CSV_COLUMN_ALPACA_FIELD_PAIRS, DOWNLOAD_REQUEST_IDENTITY_FIELDS
from .errors import EndOfDayPipelineError

NEW_YORK_TIME_ZONE = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class EndOfDayValidationResult:
    rows_seen: int
    first_bar_timestamp: object
    last_bar_timestamp: object
    observed_session_dates: tuple[str, ...]
    row_failures: tuple[dict, ...]
    dataset_failures: tuple[dict, ...]

    @property
    def has_failures(self) -> bool:
        return bool(self.row_failures or self.dataset_failures)


def read_frozen_alpaca_daily_bars(download_path: Path | str) -> list[dict]:
    """Read a prior compressed CSV fully into memory for pipeline migration."""
    download_path = Path(download_path)
    expected_csv_columns = [csv_column for csv_column, _ in CSV_COLUMN_ALPACA_FIELD_PAIRS]
    alpaca_daily_bars = []
    try:
        with gzip.open(download_path, mode="rt", encoding="utf-8", newline="") as compressed_csv_file:
            csv_rows = csv.DictReader(compressed_csv_file)
            if csv_rows.fieldnames != expected_csv_columns:
                raise EndOfDayPipelineError(f"unexpected CSV fields in {download_path}: {csv_rows.fieldnames!r}")
            for row_number, csv_row in enumerate(csv_rows, start=1):
                if None in csv_row:
                    raise EndOfDayPipelineError(f"row {row_number}: unexpected extra CSV columns")
                alpaca_daily_bars.append(
                    {
                        alpaca_field: csv_row[csv_column]
                        for csv_column, alpaca_field in CSV_COLUMN_ALPACA_FIELD_PAIRS
                    }
                )
    except (OSError, UnicodeError, csv.Error) as compressed_csv_error:
        raise EndOfDayPipelineError(
            f"could not read {download_path}: {compressed_csv_error}"
        ) from compressed_csv_error

    return alpaca_daily_bars


def parse_utc_timestamp(timestamp_text: object, field_description: str) -> datetime:
    if not isinstance(timestamp_text, str):
        raise EndOfDayPipelineError(f"{field_description} is not a timestamp string")
    normalized_timestamp_text = (
        timestamp_text[:-1] + "+00:00" if timestamp_text.endswith("Z") else timestamp_text
    )
    try:
        parsed_timestamp = datetime.fromisoformat(normalized_timestamp_text)
    except ValueError as invalid_timestamp_error:
        raise EndOfDayPipelineError(
            f"invalid {field_description}: {timestamp_text!r}"
        ) from invalid_timestamp_error
    if parsed_timestamp.tzinfo is None:
        raise EndOfDayPipelineError(f"{field_description} has no timezone: {timestamp_text!r}")
    return parsed_timestamp.astimezone(timezone.utc)


def parse_finite_number(field_value: object, field_name: str, row_number: int) -> float:
    if isinstance(field_value, bool):
        raise EndOfDayPipelineError(f"row {row_number}: {field_name} is not numeric")
    try:
        numeric_value = float(field_value)
    except (TypeError, ValueError) as numeric_conversion_error:
        raise EndOfDayPipelineError(
            f"row {row_number}: {field_name} is not numeric"
        ) from numeric_conversion_error
    if not math.isfinite(numeric_value):
        raise EndOfDayPipelineError(f"row {row_number}: {field_name} is not finite")
    return numeric_value


def validate_daily_bar(
    alpaca_bar: object,
    row_number: int,
    previous_bar_timestamp: datetime | None,
    request_start_timestamp: datetime,
    request_end_timestamp: datetime,
) -> tuple[dict, datetime, str]:
    if not isinstance(alpaca_bar, Mapping):
        raise EndOfDayPipelineError(f"row {row_number}: bar is not an object")

    missing_alpaca_fields = [
        alpaca_field for _, alpaca_field in CSV_COLUMN_ALPACA_FIELD_PAIRS if alpaca_bar.get(alpaca_field) is None
    ]
    if missing_alpaca_fields:
        raise EndOfDayPipelineError(f"row {row_number}: missing fields: {', '.join(missing_alpaca_fields)}")

    bar_timestamp = parse_utc_timestamp(alpaca_bar["t"], f"row {row_number} timestamp")
    if bar_timestamp < request_start_timestamp or bar_timestamp >= request_end_timestamp:
        raise EndOfDayPipelineError(f"row {row_number}: timestamp is outside the request range")
    if previous_bar_timestamp is not None and bar_timestamp <= previous_bar_timestamp:
        raise EndOfDayPipelineError(f"row {row_number}: timestamps are not strictly increasing")

    market_session_date = bar_timestamp.astimezone(NEW_YORK_TIME_ZONE).date()
    expected_bar_timestamp = datetime.combine(
        market_session_date, datetime_time.min, tzinfo=NEW_YORK_TIME_ZONE
    ).astimezone(timezone.utc)
    if bar_timestamp != expected_bar_timestamp:
        raise EndOfDayPipelineError(
            f"row {row_number}: daily timestamp {alpaca_bar['t']!r} is not New York session midnight"
        )

    open_price = parse_finite_number(alpaca_bar["o"], "open", row_number)
    high_price = parse_finite_number(alpaca_bar["h"], "high", row_number)
    low_price = parse_finite_number(alpaca_bar["l"], "low", row_number)
    close_price = parse_finite_number(alpaca_bar["c"], "close", row_number)
    volume_weighted_average_price = parse_finite_number(alpaca_bar["vw"], "vwap", row_number)
    share_volume = parse_finite_number(alpaca_bar["v"], "volume", row_number)
    trade_count = parse_finite_number(alpaca_bar["n"], "trade_count", row_number)

    if min(open_price, high_price, low_price, close_price, volume_weighted_average_price) <= 0:
        raise EndOfDayPipelineError(f"row {row_number}: prices must be positive")
    if (
        evaluate_ordered_bounds(
            low_price,
            high_price,
            violation_outcome=ERROR,
        ).outcome
        == ERROR
    ):
        raise EndOfDayPipelineError(f"row {row_number}: high is below low")
    for price_name, price_value in (
        ("open", open_price),
        ("close", close_price),
        ("vwap", volume_weighted_average_price),
    ):
        if (
            evaluate_value_within_bounds(
                price_value,
                low_price,
                high_price,
                violation_outcome=ERROR,
            ).outcome
            == ERROR
        ):
            raise EndOfDayPipelineError(f"row {row_number}: {price_name} is outside the low/high range")
    if share_volume <= 0 or not share_volume.is_integer():
        raise EndOfDayPipelineError(f"row {row_number}: volume is not a positive integer")
    if trade_count <= 0 or not trade_count.is_integer():
        raise EndOfDayPipelineError(f"row {row_number}: trade_count is not a positive integer")
    if trade_count > share_volume:
        raise EndOfDayPipelineError(f"row {row_number}: trade_count exceeds volume")

    csv_row = {
        csv_column: alpaca_bar[alpaca_field] for csv_column, alpaca_field in CSV_COLUMN_ALPACA_FIELD_PAIRS
    }
    return csv_row, bar_timestamp, market_session_date.isoformat()


def serializable_exact_bar(alpaca_bar: object) -> dict:
    if not isinstance(alpaca_bar, Mapping):
        return {"python_type": type(alpaca_bar).__name__, "repr": repr(alpaca_bar)}
    exact_bar_data = dict(alpaca_bar)
    try:
        json.dumps(exact_bar_data)
    except (TypeError, ValueError):
        return {
            str(field_name): field_value
            if isinstance(field_value, (str, int, float, bool)) or field_value is None
            else repr(field_value)
            for field_name, field_value in exact_bar_data.items()
        }
    return exact_bar_data


def alpaca_bar_to_csv_row(alpaca_bar: object) -> dict:
    if not isinstance(alpaca_bar, Mapping):
        return {
            csv_column: repr(alpaca_bar) if csv_column == "timestamp" else ""
            for csv_column, _ in CSV_COLUMN_ALPACA_FIELD_PAIRS
        }
    return {
        csv_column: alpaca_bar.get(alpaca_field) for csv_column, alpaca_field in CSV_COLUMN_ALPACA_FIELD_PAIRS
    }


def validate_download_request_identity(request_identity: dict) -> tuple[datetime, datetime]:
    missing_identity_fields = [
        field_name for field_name in DOWNLOAD_REQUEST_IDENTITY_FIELDS if field_name not in request_identity
    ]
    if missing_identity_fields:
        raise EndOfDayPipelineError(f"missing identity fields: {', '.join(missing_identity_fields)}")
    request_start_timestamp = parse_utc_timestamp(request_identity["request_from"], "request_from")
    request_end_timestamp = parse_utc_timestamp(request_identity["request_to"], "request_to")
    if request_start_timestamp >= request_end_timestamp:
        raise EndOfDayPipelineError("request_from must be before request_to")
    return request_start_timestamp, request_end_timestamp


def validate_end_of_day_bars(
    alpaca_daily_bars: tuple[object, ...] | list[object], request_identity: dict
) -> EndOfDayValidationResult:
    """Run the complete in-memory row and dataset validation pipeline."""
    request_start_timestamp, request_end_timestamp = validate_download_request_identity(request_identity)
    rows_seen = 0
    first_bar_timestamp = None
    last_bar_timestamp = None
    previous_bar_timestamp = None
    observed_session_dates = []
    row_failures = []

    for rows_seen, alpaca_bar in enumerate(alpaca_daily_bars, start=1):
        if isinstance(alpaca_bar, Mapping):
            if first_bar_timestamp is None:
                first_bar_timestamp = alpaca_bar.get("t")
            last_bar_timestamp = alpaca_bar.get("t")

        try:
            _, bar_timestamp, observed_session_date = validate_daily_bar(
                alpaca_bar,
                rows_seen,
                previous_bar_timestamp,
                request_start_timestamp,
                request_end_timestamp,
            )
        except EndOfDayPipelineError as row_validation_error:
            row_failures.append(
                {
                    "row_number": rows_seen,
                    "csv_line": rows_seen + 1,
                    "data": serializable_exact_bar(alpaca_bar),
                    "validation_error": str(row_validation_error),
                }
            )
            if not isinstance(alpaca_bar, Mapping):
                continue
            try:
                bar_timestamp = parse_utc_timestamp(alpaca_bar.get("t"), f"row {rows_seen} timestamp")
            except EndOfDayPipelineError:
                continue
            observed_session_date = bar_timestamp.astimezone(NEW_YORK_TIME_ZONE).date().isoformat()

        previous_bar_timestamp = bar_timestamp
        observed_session_dates.append(observed_session_date)

    dataset_failures = []
    if rows_seen == 0:
        dataset_failures.append({"validation_error": "response contained no bars"})
    elif not observed_session_dates:
        dataset_failures.append({"validation_error": "no row contained a usable timestamp for session coverage"})
    else:
        try:
            _, _, unexpected_session_dates = compare_observed_to_expected_sessions(
                observed_session_dates,
                observed_session_dates[0],
                request_identity["request_to"],
            )
        except MarketCalendarError as calendar_validation_error:
            dataset_failures.append({"validation_error": str(calendar_validation_error)})
        else:
            sessions_are_unique_and_ordered = tuple(observed_session_dates) == tuple(
                sorted(set(observed_session_dates))
            )
            if unexpected_session_dates or not sessions_are_unique_and_ordered:
                session_coverage_failure = {
                    "validation_error": "US market session coverage failed",
                    "unexpected_sessions": list(unexpected_session_dates),
                }
                if not sessions_are_unique_and_ordered:
                    session_coverage_failure["ordering_error"] = "sessions are duplicated or out of order"
                dataset_failures.append(session_coverage_failure)

    return EndOfDayValidationResult(
        rows_seen=rows_seen,
        first_bar_timestamp=first_bar_timestamp,
        last_bar_timestamp=last_bar_timestamp,
        observed_session_dates=tuple(observed_session_dates),
        row_failures=tuple(row_failures),
        dataset_failures=tuple(dataset_failures),
    )
