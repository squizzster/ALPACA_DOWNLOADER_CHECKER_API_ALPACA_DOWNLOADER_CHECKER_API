"""The absence of requestable auxiliary prices is not an identity decision."""

from libs.alpaca_pricing_download.storage import load_source, published_path

from .contract import LineageError


def unavailable_pricing(receipt):
    """Completed older publications had no deliberately absent pricing inputs."""
    return receipt.get("unavailable_pricing", {}) if receipt else {}


def check_unavailable_pricing(store, symbols, locked, *, sources=None):
    """An absent input remains part of the locked publication's freshness boundary.

    Initial rejected downloads own a stable lock inode, but no source receipt.
    A later source or interrupted publication invalidates this absence immediately.
    Identity candidates are never filtered by this pricing-only observation.
    """
    for symbol in sorted(symbols):
        if symbol not in locked:
            raise LineageError(f"unavailable pricing input lost its lock: {symbol}")
        if sources is not None:
            changed = symbol in sources  # Selection already checked pending work under these locks.
        else:
            root = published_path(store, f"end_of_day/pricing/{symbol}")
            changed = (root / ".pending.json").exists() or load_source(root, symbol) is not None
        if changed:
            raise LineageError(f"previously unavailable pricing changed: {symbol}; run lineage first")
