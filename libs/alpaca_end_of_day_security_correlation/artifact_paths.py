from pathlib import Path

CORRELATED_PIPELINE_FILENAME = "correlated.pipeline.json"
UNCORRELATED_PIPELINE_FILENAME = "uncorrelated.pipeline.json"
CORRELATION_MANIFEST_FILENAME = "security_correlation.manifest.json"


def security_correlation_directory(repository_root: Path | str, symbol: str) -> Path:
    return Path(repository_root) / "data" / "end_of_day" / symbol


def correlated_pipeline_path(repository_root: Path | str, symbol: str) -> Path:
    return security_correlation_directory(repository_root, symbol) / CORRELATED_PIPELINE_FILENAME


def uncorrelated_pipeline_path(repository_root: Path | str, symbol: str) -> Path:
    return security_correlation_directory(repository_root, symbol) / UNCORRELATED_PIPELINE_FILENAME


def security_correlation_manifest_path(repository_root: Path | str, symbol: str) -> Path:
    return security_correlation_directory(repository_root, symbol) / CORRELATION_MANIFEST_FILENAME
