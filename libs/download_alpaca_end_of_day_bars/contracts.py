from dataclasses import dataclass


@dataclass(frozen=True)
class EndOfDayDownloadPlan:
    symbol: str
    adjustment_modes: tuple[str, ...]
    force_authenticated_replacement: bool
    repair_orphaned_download_pair: bool
    adaptive_adjustment_downloads: bool = False


@dataclass(frozen=True)
class EndOfDayDownloadStageResult:
    symbol: str
    requested_adjustment_modes: tuple[str, ...]
    pricing_data_integrity_failure_count: int

    @property
    def has_pricing_data_integrity_failures(self) -> bool:
        return self.pricing_data_integrity_failure_count > 0
