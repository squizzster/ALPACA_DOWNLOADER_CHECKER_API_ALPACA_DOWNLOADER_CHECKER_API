from pathlib import Path

CROSS_ADJUSTMENT_WARNINGS_FILENAME = "cross_adjustment_warnings.pipeline.json"
SECURITY_SEGMENT_STATISTICAL_WARNINGS_FILENAME = "security_segment_statistical_warnings.pipeline.json"
WARNING_PIPELINE_MANIFEST_FILENAME = "warning_pipeline.manifest.json"


def warning_pipeline_directory(repository_root: Path | str, symbol: str) -> Path:
    return Path(repository_root) / "data" / "end_of_day" / symbol


def cross_adjustment_warnings_path(repository_root: Path | str, symbol: str) -> Path:
    return warning_pipeline_directory(repository_root, symbol) / CROSS_ADJUSTMENT_WARNINGS_FILENAME


def security_segment_statistical_warnings_path(
    repository_root: Path | str,
    symbol: str,
) -> Path:
    return warning_pipeline_directory(repository_root, symbol) / SECURITY_SEGMENT_STATISTICAL_WARNINGS_FILENAME


def warning_pipeline_manifest_path(repository_root: Path | str, symbol: str) -> Path:
    return warning_pipeline_directory(repository_root, symbol) / WARNING_PIPELINE_MANIFEST_FILENAME
