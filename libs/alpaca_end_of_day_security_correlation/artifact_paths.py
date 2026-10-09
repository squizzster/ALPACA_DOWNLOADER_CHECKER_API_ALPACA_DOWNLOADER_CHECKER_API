from pathlib import Path

CORRELATED_PIPELINE_FILENAME = "correlated.pipeline.json"
UNCORRELATED_PIPELINE_FILENAME = "uncorrelated.pipeline.json"
CORRELATION_MANIFEST_FILENAME = "security_correlation.manifest.json"
SECURITY_MASTER_FILENAME = "security_master.index.json"
SECURITY_MASTER_MANIFEST_FILENAME = "security_master.manifest.json"
SECURITY_MASTER_DIRECTORY_NAME = "json_manifests"


def security_correlation_directory(repository_root: Path | str, symbol: str) -> Path:
    return Path(repository_root) / "data" / "end_of_day" / symbol


def correlated_pipeline_path(repository_root: Path | str, symbol: str) -> Path:
    return security_correlation_directory(repository_root, symbol) / CORRELATED_PIPELINE_FILENAME


def uncorrelated_pipeline_path(repository_root: Path | str, symbol: str) -> Path:
    return security_correlation_directory(repository_root, symbol) / UNCORRELATED_PIPELINE_FILENAME


def security_correlation_manifest_path(repository_root: Path | str, symbol: str) -> Path:
    return security_correlation_directory(repository_root, symbol) / CORRELATION_MANIFEST_FILENAME


def security_master_path(repository_root: Path | str) -> Path:
    return security_master_directory(repository_root) / SECURITY_MASTER_FILENAME


def security_master_manifest_path(repository_root: Path | str) -> Path:
    return security_master_directory(repository_root) / SECURITY_MASTER_MANIFEST_FILENAME


def security_master_directory(repository_root: Path | str) -> Path:
    return Path(repository_root) / "data" / "end_of_day" / SECURITY_MASTER_DIRECTORY_NAME


def legacy_security_master_path(repository_root: Path | str) -> Path:
    return Path(repository_root) / "data" / "end_of_day" / SECURITY_MASTER_FILENAME


def legacy_security_master_manifest_path(repository_root: Path | str) -> Path:
    return Path(repository_root) / "data" / "end_of_day" / SECURITY_MASTER_MANIFEST_FILENAME
