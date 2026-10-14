from pathlib import Path


def current_store_root(repository_root: Path | str) -> Path:
    """Return the one live application store; no format probing or fallback."""
    return Path(repository_root) / "data_vtest"


def validation_directory(store_root: Path | str, symbol: str) -> Path:
    return Path(store_root) / "validation" / symbol


def lineage_directory(store_root: Path | str, symbol: str) -> Path:
    return Path(store_root) / "lineage" / symbol


def correlation_directory(store_root: Path | str, symbol: str) -> Path:
    return Path(store_root) / "correlation" / symbol


def warning_directory(store_root: Path | str, symbol: str) -> Path:
    return Path(store_root) / "warnings" / symbol


def availability_directory(store_root: Path | str) -> Path:
    return Path(store_root) / "availability"


def catalogue_directory(store_root: Path | str) -> Path:
    return Path(store_root) / "catalogue"


def legacy_engine_root(store_root: Path | str) -> Path:
    """Private root used only by the proven content-addressed catalogue engine."""
    return Path(store_root) / "catalogue_engine"
