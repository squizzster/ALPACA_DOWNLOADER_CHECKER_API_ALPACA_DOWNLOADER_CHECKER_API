"""Offline status from compact producer controls; never recover or inventory stores."""

from datetime import datetime, timezone
from pathlib import Path

from libs.alpaca_pricing_download.check_summary import read_status as summary_status
from libs.alpaca_pricing_download.storage import published_path
from libs.alpaca_pricing_download.update_checks import read_checks

from . import storage, universe_state
from .contract import LineageError
from .projection import PROJECTION_VERSION, require_complete


def inspect_status(store):
    # Read-only control-plane boundary: report compact owner state and only named
    # pending journals. DO NOT add repair, receipt/payload verification or a ticker
    # census to make status look more certain; those are separate owner operations.
    store = Path(store).absolute()
    result = {
        "store": str(store),
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "ready_for_update": False,
        "processing_complete": False,
        "blockers": [],
    }

    def read(name, function):
        try:
            value = function()
            result[name] = value
            return value
        except (RuntimeError, OSError, ValueError) as error:
            result["blockers"].append(f"{name}: {error}")
            return None

    try:
        storage.check_store(store)
    except (RuntimeError, OSError, ValueError) as error:
        result["blockers"].append(str(error))
        return result

    def lineage_status():
        root = store / "end_of_day/lineage"
        if not (root / "index.sqlite3").exists():
            raise LineageError("lineage index missing; run alpaca-cli-eod lineage update")
        with storage.publication_lock(root, wait=False):
            pending = (root / ".pending.json").exists()
            index = storage.read_index(root)
            try:
                require_complete(index)
            except LineageError as error:
                result["blockers"].append(str(error))
            projections = [entry.get("projection") for entry in index["symbols"].values()]
            known = [p for p in projections if p and p.get("schema_version") == PROJECTION_VERSION]
            return {
                "symbols": len(index["symbols"]),
                "projection": index.get("projection_state"),
                "publication_pending": pending,
                "published_histories": {
                    "nonempty": sum(p["source_rows"] > 0 for p in known),
                    "empty": sum(p["source_rows"] == 0 for p in known),
                    "with_unresolved_rows": sum(p["unresolved_rows"] > 0 for p in known),
                    "with_all_rows_assigned": sum(
                        p["source_rows"] > 0 and p["unresolved_rows"] == 0 for p in known
                    ),
                    "unknown_projection": len(projections) - len(known),
                },
                "coverage_end_exclusive": {
                    "earliest": min((p["processed_through"] for p in known), default=None),
                    "latest": max((p["processed_through"] for p in known), default=None),
                },
            }

    lineage = read("lineage", lineage_status)
    summary = read("summary", lambda: summary_status(store))
    if summary is not None and not summary["coverage_complete"]:
        result["blockers"].append("summary coverage incomplete; run alpaca-cli-eod summary rebuild")
    # Read scheduling after publications: producers queue before changing inputs.
    scheduling = read("scheduling", lambda: read_checks(store, readonly=True))
    if scheduling is not None and not scheduling["complete"]:
        result["blockers"].append(
            "lineage scheduling coverage missing or incomplete; run alpaca-cli-eod lineage update"
        )

    def universe_status():
        state = universe_state.read_state(store)
        return {
            **state,
            "completed_work": sum(item["status"] == "complete" for item in state["work"].values()),
            "work": {symbol: item for symbol, item in state["work"].items() if item["status"] != "complete"},
        }

    universe = read("universe", universe_status)
    from libs.alpaca_serving_catalogue.producer import status as serving_status

    serving = read("serving_catalogue", lambda: serving_status(store))
    if serving is not None and not serving["coverage_complete"]:
        result["blockers"].append("serving catalogue incomplete; run alpaca-cli-eod catalogue bootstrap")
    selected = set(scheduling["checks"] if scheduling else ())
    if summary:
        selected.update(item["symbol"] for item in summary["pending_inputs"])
    journals = read(
        "pricing_publications",
        lambda: {
            "selection_coverage_complete": bool(scheduling and scheduling["complete"]),
            "pending_symbols": [
                symbol
                for symbol in sorted(selected)
                if (published_path(store, f"end_of_day/pricing/{symbol}") / ".pending.json").exists()
            ],
        },
    )
    result["ready_for_update"] = not result["blockers"]
    result["processing_complete"] = result["ready_for_update"] and not (
        lineage["publication_pending"]
        or summary["pending_inputs"]
        or scheduling["checks"]
        or universe["work"]
        or universe["registration_pending"]
        or journals["pending_symbols"]
        or serving["pending_symbols"]
    )
    return result
