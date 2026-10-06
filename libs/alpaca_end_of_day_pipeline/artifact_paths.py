from pathlib import Path

from .contracts import INTEGRITY_FAILURE_MANIFEST_FILENAME, PIPELINE_RECEIPT_MANIFEST_FILENAME


def download_manifest_path(download_path: Path | str) -> Path:
    download_path = Path(download_path)
    return download_path.with_name(download_path.name + ".manifest.json")


def pipeline_receipt_manifest_path(download_path: Path | str) -> Path:
    """Return the processing-receipt manifest for a symbol directory."""
    return Path(download_path).parent / PIPELINE_RECEIPT_MANIFEST_FILENAME


def integrity_failure_manifest_path(download_path: Path | str) -> Path:
    """Return the failure-only integrity manifest for a symbol directory."""
    return Path(download_path).parent / INTEGRITY_FAILURE_MANIFEST_FILENAME
