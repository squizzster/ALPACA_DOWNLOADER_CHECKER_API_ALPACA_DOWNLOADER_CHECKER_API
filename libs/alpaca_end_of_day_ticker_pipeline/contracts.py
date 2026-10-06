from dataclasses import dataclass
from typing import Any

from libs.download_alpaca_end_of_day_bars import EndOfDayDownloadStageResult


@dataclass(frozen=True)
class EndOfDayTickerPipelineResult:
    download_stage: EndOfDayDownloadStageResult
    security_lineage_report: dict[str, Any]
    security_correlation_receipt: dict[str, Any]
    warning_pipeline_receipt: dict[str, Any]
