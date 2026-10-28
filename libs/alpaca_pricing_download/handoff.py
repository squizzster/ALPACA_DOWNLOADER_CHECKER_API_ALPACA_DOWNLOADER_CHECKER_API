"""Internal pricing handoff; no lineage calculation or public consumer API."""

from contextlib import ExitStack, contextmanager
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


def load_published_boundary(root: Path, source: dict, kind: str, *, selected=False, documents=None) -> dict:
    """Trust a published pricing receipt under the caller's ticker lock.

    The producer has already authenticated values and exact-copy aliases. This
    normal handoff decodes selected data without filesystem revalidation or
    recalculating digests. selected means the caller already checked publication
    completion under the continuously held lock. read_boundary uses this trusted
    consumption path; authenticate_boundary is the explicit audit.
    """
    # selected is a caller-owned completion guarantee under a continuous ticker
    # lock, not permission to carry a stale selection across lock lifetimes.
    if not selected and regular_path(root, ".pending.json").exists():
        raise DownloadError("pricing publication is incomplete; resume the downloader")
    reference = source["boundaries"][kind]
    document = read_document(regular_path(root, reference["manifest"], published=True), published=True)
    if (
        document.get("schema_version") not in SUPPORTED_VERSIONS
        or document.get("symbol") != source["symbol"]
        or document.get("boundary") != "alpaca_pricing_" + kind
        or tuple(entry["adjustment"] for entry in document["adjustments"]) != ADJUSTMENTS
    ):
        raise DownloadError("published pricing receipt has a different identity")
    if documents is not None:
        documents[reference["manifest"]] = document
    parent = PurePosixPath(reference["manifest"]).parent
    decoded, views = {}, {}
    for entry in document["adjustments"]:
        if entry["storage"] == "empty":
            views[entry["adjustment"]] = ()
            continue
        filename = entry["file"]
        if filename not in decoded:
            decoded[filename] = decode_csv(
                regular_path(root, str(parent / filename), published=True).read_bytes()
            )
        views[entry["adjustment"]] = decoded[filename]
    return views


def load_published_views(root: Path, source: dict, *, selected=False) -> dict:
    """Assemble the owner's accepted overlap rule, without re-authenticating it."""
    if not selected and regular_path(root, ".pending.json").exists():
        raise DownloadError("pricing publication is incomplete; resume the downloader")
    cold = load_published_boundary(root, source, "completed_years", selected=True)
    year = load_published_boundary(root, source, "current_year", selected=True)
    month = load_published_boundary(root, source, "current_month", selected=True)
    # Exact-copy aliases are already authenticated by the producing boundary.
    # Preserve that identity while assembling the three chronological parts so
    # downstream adapters parse and materialize each distinct history once.
    active_years, assembled = {}, {}
    views = {}
    for name in ADJUSTMENTS:
        year_rows = year[name]
        active_year = active_years.get(id(year_rows))
        if active_year is None:
            active_year = tuple(row for row in year_rows if month_of(row) < source["anchor_month"])
            active_years[id(year_rows)] = active_year
        parts = (id(cold[name]), id(active_year), id(month[name]))
        if parts not in assembled:
            assembled[parts] = cold[name] + active_year + month[name]
        views[name] = assembled[parts]
    return views


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
        return load_published_views(self._root, source, selected=True)


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
    root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
    with ExitStack() as locks:
        try:
            locks.enter_context(ticker_lock(root, exclusive=True))
        except FileNotFoundError:
            if load_source(root, symbol) is not None:
                raise DownloadError("published pricing lock is missing")
            yield None
            return
        if regular_path(root, ".pending.json").exists():
            raise DownloadError("pricing publication is incomplete; resume the downloader")
        dates = read_update(root)
        if dates is None:
            yield None
            return
        # The owning notice and lock delimit this consumption. DO NOT repair
        # pricing or rediscover other tickers from inside the consumer context.
        batch = PricingUpdate(dict(dates), root)
        try:
            yield batch
            acknowledge_update(root, dates, selected=True)
        finally:
            batch._active = False
