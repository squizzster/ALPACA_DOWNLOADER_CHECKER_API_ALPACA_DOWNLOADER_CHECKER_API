"""The absence of requestable auxiliary prices is not an identity decision."""

from libs.alpaca_pricing_download.storage import regular_path

from .contract import LineageError


def unavailable_pricing(receipt):
    """Completed older publications had no deliberately absent pricing inputs."""
    return receipt.get("unavailable_pricing", {}) if receipt else {}


def check_unavailable_pricing(store, symbols, locked):
    """An absent input remains part of the locked publication's freshness boundary.

    Initial rejected downloads own a stable lock inode, but no source receipt.
    A later source or interrupted publication invalidates this absence immediately.
    Identity candidates are never filtered by this pricing-only observation.
    """
    for symbol in sorted(symbols):
        root = regular_path(store, f"end_of_day/pricing/{symbol}")
        if symbol not in locked:
            raise LineageError(f"unavailable pricing input lost its lock: {symbol}")
        if any(regular_path(root, name).exists() for name in ("source.manifest.json", ".pending.json")):
            raise LineageError(f"previously unavailable pricing changed: {symbol}; run lineage first")
