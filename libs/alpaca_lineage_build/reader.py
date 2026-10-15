"""Internal trusted handoff of dated assignments; no pricing reads or audits."""

import fcntl
import os
from pathlib import Path

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, symbol_label
from libs.alpaca_pricing_download.storage import load_source, read_update, regular_path

from .contract import LineageError, document_digest, versions
from .storage import check_store, path_at, pricing_locks, read_boundaries, read_index, read_receipt


def read_lineage(store, symbol):
    """Select one completed publication. Not a public DataLibrary interface.

    Receipt identities prevent mixing generations; they are not a re-audit of
    approved data. No price payloads, metadata APIs, or identity rules run here.
    """
    store, symbol = Path(store).absolute(), symbol_label(symbol)
    check_store(store)
    root = regular_path(store, "end_of_day/lineage")
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_SH)
        if path_at(root, ".pending.json").exists():
            raise LineageError("lineage publication incomplete; resume lineage first")
        receipt = read_receipt(root, symbol, read_index(root, symbol))
        if receipt is None:
            raise LineageError(f"no published lineage for {symbol}")
        if receipt["versions"] != versions():
            raise LineageError("lineage rules changed; process pending lineage work")
        with pricing_locks(store, receipt["input_sources"]) as locked:
            for dependency, expected in receipt["input_sources"].items():
                pricing = regular_path(store, f"end_of_day/pricing/{dependency}")
                if dependency not in locked or regular_path(pricing, ".pending.json").exists():
                    raise LineageError(f"pricing input unavailable: {dependency}")
                source = load_source(pricing, dependency)
                if source is None or read_update(pricing) is not None or document_digest(source) != expected:
                    raise LineageError(f"lineage awaits changed pricing input: {dependency}")
            boundaries = read_boundaries(root, receipt)
            assignments = {name: [] for name in ADJUSTMENTS}
            for kind in ("completed_years", "current_year", "current_month"):
                for item in boundaries[kind]["adjustments"]:
                    assignments[item["adjustment"]].extend(
                        {**segment, "pricing_boundary": kind}
                        for segment in item["segments"]
                        if kind != "current_year" or segment["from"][:7] < receipt["anchor_month"]
                    )
            return {"receipt": receipt, "adjustments": assignments}
    finally:
        os.close(descriptor)
