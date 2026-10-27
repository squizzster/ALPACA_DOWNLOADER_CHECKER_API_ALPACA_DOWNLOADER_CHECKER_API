"""Metadata-only consumption of one accepted lineage publication boundary."""

from contextlib import ExitStack, contextmanager
from copy import deepcopy

from libs.alpaca_lineage_build.contract import LineageBusyError, LineageError
from libs.alpaca_lineage_build.projection import require_complete
from libs.alpaca_lineage_build.storage import publication_lock, read_boundaries, read_index, read_receipt

from .standalone import ReaderBusyError, StandaloneReaderError, _assignments, _group_evidence


class PublishedEvidenceSnapshot:
    """A selected lineage index and a bounded stream of its published bodies."""

    def __init__(self, root, index):
        from . import API_VERSION

        self._root, self._index = root, index
        self._consumed = False
        self._groups = []
        seen = set()
        for symbol, entry in sorted(index["symbols"].items()):
            members = entry.get("group_members")
            if not isinstance(members, list) or not members or symbol not in members:
                raise StandaloneReaderError(
                    "lineage membership incomplete; run alpaca-cli-eod lineage migrate-index"
                )
            if symbol in seen:
                continue
            selected = sorted(name for name in members if name in index["symbols"])
            self._groups.append(selected)
            seen.update(selected)
        generations = {name: entry["sha256"] for name, entry in index["symbols"].items()}
        self._roster = {
            "api_version": API_VERSION,
            "boundary": "published_lineage_evidence",
            "generation": f"lineage:{index['projection_state']['schema_version']}:{index['projection_state']['revision']}",
            "snapshot_token": deepcopy(index["projection_state"]),
            "group_count": len(self._groups),
            "sources": [{"symbol": name, "generation": generation} for name, generation in generations.items()],
        }

    def _require_open(self):
        if self._index is None:
            raise StandaloneReaderError("published evidence snapshot is closed")

    @property
    def roster(self):
        self._require_open()
        return deepcopy(self._roster)

    def iter_groups(self):
        self._require_open()
        if self._consumed:
            raise StandaloneReaderError("published evidence snapshot was already consumed")
        self._consumed = True
        for members in self._groups:
            self._require_open()
            with _boundary_errors():
                receipts, assignments, generations = {}, {}, {}
                for symbol in members:
                    # Membership and selecting generations already came from the
                    # accepted index. Read bodies only because they are the export.
                    receipt = read_receipt(self._root, symbol, self._index)
                    receipts[symbol] = receipt
                    assignments[symbol] = tuple(_assignments(read_boundaries(self._root, receipt), receipt))
                    generations[symbol] = self._index["symbols"][symbol]["sha256"]
                group = _group_evidence(assignments, None, receipts, generations, generations[members[0]])
            yield group

    def close(self):
        self._index = self._groups = None


@contextmanager
def _boundary_errors():
    try:
        yield
    except LineageBusyError as error:
        raise ReaderBusyError(str(error)) from error
    except (LineageError, OSError, ValueError, KeyError, TypeError) as error:
        raise StandaloneReaderError(f"cannot read published lineage evidence: {error}") from error


@contextmanager
def open_published_evidence(store, *, wait=False):
    """Hold only the owning publication gate; trust its selected index and bodies."""
    root = store / "end_of_day/lineage"
    with ExitStack() as locks:
        with _boundary_errors():
            locks.enter_context(publication_lock(root, wait=wait))
            if (root / ".pending.json").exists():
                raise StandaloneReaderError("lineage publication incomplete; resume its owner first")
            index = read_index(root)
            require_complete(index)
            snapshot = PublishedEvidenceSnapshot(root, index)
        try:
            yield snapshot
        finally:
            snapshot.close()
