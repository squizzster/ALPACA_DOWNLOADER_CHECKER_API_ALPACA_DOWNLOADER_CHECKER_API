"""Internal trusted handoff of dated assignments; no pricing reads or audits."""

from pathlib import Path

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, symbol_label
from libs.alpaca_pricing_download.storage import load_source, published_path, read_update

from .acquisition import check_unavailable_pricing, unavailable_pricing
from .contract import LineageError, document_digest, reusable_decision
from .storage import reader_snapshot, select_inputs


def read_lineage(store, symbol):
    # Reader freshness is selected generations under locks, not a new identity audit.
    # DO NOT add pricing payload hashes, rule replay or repair to establish trust in
    # the producer's already published assignments; changed inputs remain pending.
    """Select one completed publication. Not a public DataLibrary interface.

    Receipt identities prevent mixing generations; they are not a re-audit of
    approved data. No price payloads, metadata APIs, or identity rules run here.
    """
    store, symbol = Path(store).absolute(), symbol_label(symbol)
    with reader_snapshot(store, lambda root: select_inputs(root, symbol)) as snap:
        if any(
            receipt is not None
            and snap["entries"][label].get("correlation_policy") != receipt.get("correlation_policy")
            for label, receipt in snap["receipts"].items()
        ):
            raise LineageError("lineage selector and receipt policy provenance differ")
        receipt = snap["receipts"][symbol]
        if not reusable_decision(receipt):
            raise LineageError("lineage rules changed; process pending lineage work")
        check_unavailable_pricing(store, unavailable_pricing(receipt), snap["locked"])
        for dependency, expected in receipt["input_sources"].items():
            pricing = published_path(store, f"end_of_day/pricing/{dependency}")
            if dependency not in snap["locked"] or published_path(pricing, ".pending.json").exists():
                raise LineageError(f"pricing input unavailable: {dependency}")
            source = load_source(pricing, dependency)
            if source is None or read_update(pricing) is not None or document_digest(source) != expected:
                raise LineageError(f"lineage awaits changed pricing input: {dependency}")
        boundaries = snap["boundaries"][symbol]
        assignments = {name: [] for name in ADJUSTMENTS}
        for kind in ("completed_years", "current_year", "current_month"):
            for item in boundaries[kind]["adjustments"]:
                assignments[item["adjustment"]].extend(
                    {**segment, "pricing_boundary": kind}
                    for segment in item["segments"]
                    if kind != "current_year" or segment["from"][:7] < receipt["anchor_month"]
                )
        return {"receipt": receipt, "adjustments": assignments}
