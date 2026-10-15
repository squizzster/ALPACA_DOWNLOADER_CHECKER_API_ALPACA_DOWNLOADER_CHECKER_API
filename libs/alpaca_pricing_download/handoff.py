"""Internal pricing handoff; no lineage calculation or public consumer API."""

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from .contract import ADJUSTMENTS, SUPPORTED_VERSIONS, DownloadError, decode_csv, month_of, symbol_label
from .storage import (
    STORE_DOCUMENT,
    acknowledge_update,
    load_source,
    read_document,
    read_update,
    regular_path,
    ticker_lock,
)


def load_published_boundary(root: Path, source: dict, kind: str) -> dict:
    """Trust a published pricing receipt under the caller's ticker lock.

    The producer has already authenticated values and exact-copy aliases. This
    normal handoff decodes selected data; it never recalculates payload digests,
    logical digests, or validation rules. read_boundary remains the explicit audit.
    """
    if regular_path(root, ".pending.json").exists():
        raise DownloadError("pricing publication is incomplete; resume the downloader")
    reference = source["boundaries"][kind]
    document = read_document(regular_path(root, reference["manifest"]))
    if (
        document.get("schema_version") not in SUPPORTED_VERSIONS
        or document.get("symbol") != source["symbol"]
        or document.get("boundary") != "alpaca_pricing_" + kind
        or tuple(entry["adjustment"] for entry in document["adjustments"]) != ADJUSTMENTS
    ):
        raise DownloadError("published pricing receipt has a different identity")
    parent = PurePosixPath(reference["manifest"]).parent
    decoded, views = {}, {}
    for entry in document["adjustments"]:
        if entry["storage"] == "empty":
            views[entry["adjustment"]] = ()
            continue
        filename = entry["file"]
        if filename not in decoded:
            decoded[filename] = decode_csv(regular_path(root, str(parent / filename)).read_bytes())
        views[entry["adjustment"]] = decoded[filename]
    return views


def load_published_views(root: Path, source: dict) -> dict:
    """Assemble the owner's accepted overlap rule, without re-authenticating it."""
    cold = load_published_boundary(root, source, "completed_years")
    year = load_published_boundary(root, source, "current_year")
    month = load_published_boundary(root, source, "current_month")
    return {
        name: cold[name]
        + tuple(row for row in year[name] if month_of(row) < source["anchor_month"])
        + month[name]
        for name in ADJUSTMENTS
    }


@dataclass
class PricingUpdate:
    dates: dict
    _root: Path
    _active: bool = True

    def read_views(self) -> dict:
        """Read inside the already-held lock, without recursively acquiring it."""
        if not self._active:
            raise DownloadError("pricing update is no longer locked")
        source = load_source(self._root, self._root.name)
        if source is None:
            raise DownloadError("no published pricing source")
        return load_published_views(self._root, source)


@contextmanager
def consume_pricing_update(store: Path | str, symbol: str):
    """Hold EX through processing; acknowledge only a successful context exit.

    The caller must durably complete its work before leaving the context. Errors
    retain the pending range. A crash after processing but before acknowledgement
    can repeat work, so the future consumer must be idempotent.
    """
    symbol = symbol_label(symbol)
    store = Path(store).absolute()
    if read_document(store / "store.manifest.json") != STORE_DOCUMENT:
        raise DownloadError("not a standalone pricing store")
    root = regular_path(store, f"end_of_day/pricing/{symbol}")
    if not root.exists():
        yield None
        return
    with ticker_lock(root, exclusive=True):
        if regular_path(root, ".pending.json").exists():
            raise DownloadError("pricing publication is incomplete; resume the downloader")
        dates = read_update(root)
        if dates is None:
            yield None
            return
        batch = PricingUpdate(dict(dates), root)
        try:
            yield batch
            acknowledge_update(root, dates)
        finally:
            batch._active = False
