from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class AnalyzableDailyBar:
    row_number: int
    timestamp_text: str
    timestamp: datetime
    session_date: date
    values: dict[str, Decimal]

    def numeric_value(self, field_name: str) -> float:
        return float(self.values[field_name])

    def exact_evidence(self) -> dict[str, Any]:
        return {
            "row_number": self.row_number,
            "timestamp": self.timestamp_text,
            "session_date": self.session_date.isoformat(),
            **{field_name: str(field_value) for field_name, field_value in self.values.items()},
        }

    def full_signature(self) -> tuple[Decimal, ...]:
        return tuple(
            self.values[field_name]
            for field_name in ("open", "high", "low", "close", "volume", "trade_count", "vwap")
        )


@dataclass(frozen=True)
class DownloadValidationView:
    data_file: str
    download_sha256: str
    adjustment: str
    adjustment_actions: frozenset[str]
    manifest: dict[str, Any]
    source_row_count: int
    timestamp_by_row_number: dict[int, datetime]
    bars_by_row_number: dict[int, AnalyzableDailyBar]
    excluded_rows: tuple[dict[str, Any], ...]

    @property
    def bars_by_timestamp(self) -> dict[datetime, AnalyzableDailyBar]:
        return {bar.timestamp: bar for bar in self.bars_by_row_number.values()}

    @property
    def timestamps(self) -> tuple[datetime, ...]:
        return tuple(self.timestamp_by_row_number.values())

    def request_cohort_key(self) -> tuple[str, ...]:
        return tuple(
            str(self.manifest.get(field_name) or "")
            for field_name in (
                "symbol",
                "feed",
                "timeframe",
                "asof",
                "request_from",
                "request_to",
            )
        )


@dataclass(frozen=True)
class AuthenticatedCorrelationOutputs:
    receipt: dict[str, Any]
    correlated: dict[str, Any]
    uncorrelated: dict[str, Any]
    correlated_sha256: str
    uncorrelated_sha256: str


@dataclass(frozen=True)
class WarningPipelineInputs:
    symbol: str
    source_downloads: tuple[dict[str, Any], ...]
    download_views: tuple[DownloadValidationView, ...]
    correlation: AuthenticatedCorrelationOutputs
    pipeline_receipt_manifest_sha256: str
    integrity_failure_manifest_sha256: str
