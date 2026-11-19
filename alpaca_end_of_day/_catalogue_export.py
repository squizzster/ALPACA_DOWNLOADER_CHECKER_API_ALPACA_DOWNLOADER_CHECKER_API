"""One accepted online selection shared by catalogue export formats.

The serving catalogue owns eligibility, membership, generation references and
dated labels. Its export-input selector establishes applicability once under the
shared operation gate. Consumers assemble selected histories; they do not reopen
the index, authenticate upstream bytes or replay completed serving decisions.
The gate remains held until the format owner finishes publication. A changed
catalogue token or dirty selected input rejects before any history body is read.
"""

from bisect import bisect_left, bisect_right
from contextlib import closing, contextmanager
from dataclasses import dataclass
from operator import itemgetter

from libs.alpaca_serving_catalogue import export_inputs
from libs.alpaca_serving_catalogue.contract import CatalogueError
from libs.common.store_lock import StoreBusyError, operation_lock

from . import CatalogChangedError, ReaderBusyError, StandaloneReaderError
from .standalone import _date


def selection(data, public_ids, start, end):
    """Project requested online identities from the already opened catalogue."""
    start, end = _date(start, "start"), _date(end, "end")
    if start is not None and end is not None and start > end:
        raise ValueError("start must be on or before end")
    if public_ids is None:
        securities = data._securities
    else:
        if not isinstance(public_ids, (list, tuple)) or not public_ids:
            raise ValueError("public_ids must be a nonempty list or tuple, or None for all online IDs")
        securities = {security.public_id: security for security in map(data.get_security, public_ids)}
    selected, groups = {}, {}
    for public_id, security in sorted(securities.items()):
        if (start is not None and security.last_session < start) or (
            end is not None and security.first_session > end
        ):
            continue
        selected[public_id] = security
        groups.setdefault(data._records[public_id]["group_key"], []).append(public_id)
    return selected, groups, start, end


def read_group(data, selected, groups, key, start, end):
    """Assemble one selected group, sharing its decoded sources across identities."""
    expected = data._groups[key]
    try:
        with data._reader._open_group(
            expected["locator"], expected=expected["references"], wait=False, export_selection=True
        ) as group:
            starts, visible = {}, {}
            for pid in groups[key]:
                if data._records[pid].get("public", {}).get("price_basis_limits"):
                    online = selected[pid].first_session
                    starts[pid] = max(start, online) if start else online
                intervals = data._records[pid].get("public", {}).get("price_exclusions")
                if intervals:
                    from ._online_price_spans import spans

                    last = selected[pid].last_session
                    visible[pid] = list(
                        spans(
                            starts.get(pid, start or selected[pid].first_session),
                            min(end, last) if end else last,
                            intervals,
                        )
                    )
            scoped = {"online_starts": starts} if starts else {}
            if visible:
                scoped["online_windows"] = visible
            with closing(group._iter_export_histories(groups[key], start=start, end=end, **scoped)) as histories:
                for public_id, views in histories:
                    security = selected[public_id]
                    first, last = security.first_session.isoformat(), security.last_session.isoformat()
                    # The producer owns the ordered session grid. Clip only the
                    # accepted online span and retain physical aliases by reference.
                    clipped, output = {}, {}
                    for name, view in views.items():
                        bars = view["bars"]
                        bars_id = id(bars)
                        if bars_id not in clipped:
                            if bars and (bars[0]["session"] < first or bars[-1]["session"] > last):
                                session = itemgetter("session")
                                lo = bisect_left(bars, first, key=session)
                                hi = bisect_right(bars, last, lo=lo, key=session)
                                clipped[bars_id] = bars[lo:hi]
                            else:
                                clipped[bars_id] = bars
                        output[name] = {**view, "bars": clipped[bars_id]}
                    yield public_id, output
    except ReaderBusyError as error:
        raise ReaderBusyError("export source is busy; retry later") from error
    except (CatalogueError, StandaloneReaderError) as error:
        raise CatalogChangedError(
            "selected serving history is no longer compatible; finish maintenance and open a new DataLibrary"
        ) from error


@dataclass(frozen=True)
class ExportHistory:
    """Completed identity views and dated-label decisions for format conversion.

    All eight views carry the reader's accepted ordered session grid. Numeric
    fields retain exact decimal strings. Ticker ranges are the catalogue's
    completed inclusive ranges, including genuine ambiguity. Treat held values
    as read-only; no upstream authentication or alignment calculation is needed.
    """

    security: object
    views: dict
    ticker_ranges: tuple
    catalogue_generation: str
    error: Exception | None = None


class CatalogueExport:
    """A single-use history stream whose publication must remain inside its context."""

    def __init__(self, data, selected, groups, start, end):
        self._data = data
        self.securities, self.groups = selected, groups
        self.start, self.end = start, end
        self.catalogue = data.catalog()
        self.ticker_ranges = {
            public_id: tuple(data._records[public_id]["public"]["ticker_ranges"]) for public_id in selected
        }
        self._active, self._started = True, False
        self._iterator = None

    def iter_histories(self, *, on_error="raise"):
        if not self._active or self._started:
            raise RuntimeError("export histories require an active, unused catalogue export")
        if on_error not in ("raise", "record"):
            raise ValueError("on_error must be raise or record")
        self._started = True
        self._iterator = self._histories(on_error)
        return self._iterator

    def _histories(self, on_error):
        for key in sorted(self.groups):
            completed = set()
            try:
                with closing(
                    read_group(self._data, self.securities, self.groups, key, self.start, self.end)
                ) as histories:
                    for public_id, views in histories:
                        completed.add(public_id)
                        yield ExportHistory(
                            self.securities[public_id],
                            views,
                            self.ticker_ranges[public_id],
                            self.catalogue.generation,
                        )
            except (OSError, CatalogChangedError) as error:
                if on_error == "raise":
                    raise
                # A format may retain a complete failure inventory or explicitly
                # omit unreadable histories. Preserve already assembled peers,
                # report remaining selected IDs, and never retry the group.
                for public_id in self.groups[key]:
                    if public_id not in completed:
                        yield ExportHistory(
                            self.securities[public_id],
                            {},
                            self.ticker_ranges[public_id],
                            self.catalogue.generation,
                            error,
                        )


@contextmanager
def open_export(data, *, public_ids=None, start=None, end=None, full_histories=False):
    try:
        with operation_lock(data.store, exclusive=False):
            selected, groups, start, end = selection(data, public_ids, start, end)
            try:
                export_inputs.select(
                    data.store,
                    data.catalog_generation(),
                    {symbol for key in groups for symbol in data._groups[key]["references"]["lineage"]},
                )
            except (CatalogueError, OSError, ValueError, KeyError, TypeError) as error:
                raise CatalogChangedError(
                    "selected serving catalogue or export inputs changed; finish maintenance and open a new DataLibrary"
                ) from error
            exported = CatalogueExport(
                data, selected, groups, None if full_histories else start, None if full_histories else end
            )
            try:
                yield exported
            finally:
                exported._active = False
                if exported._iterator is not None:
                    exported._iterator.close()
    except StoreBusyError as error:
        raise ReaderBusyError("export source store is busy; retry later") from error
