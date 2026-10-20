"""Guard request boundaries before starting a combined-store publication."""

from .common import PUBLICATION_PENDING, read_json_object
from .errors import V6Error
from .source import MONTH_MANIFEST, SOURCE_MANIFEST, _inventory_placements
from .versions import validate_boundary_change_log


def validate_update_request(store, marker):
    validate_boundary_change_log("update_execution")
    pending = store / PUBLICATION_PENDING
    if pending.exists():
        previous = read_json_object(pending, "pending publication")
        fields = ("request_end", "request_start", "submitted_symbols", "canonical_symbols")
        if any(previous.get(key) != marker.get(key) for key in fields):
            raise V6Error("pending catalogue publication requires its original symbols and request boundaries")
    end = marker["request_end"]
    for symbol in marker["canonical_symbols"]:
        root = store / "end_of_day" / symbol
        source_path = root / SOURCE_MANIFEST
        if not source_path.exists():
            continue
        # Read the selected terminal month only. No payload reads, source audit
        # or complete-store enumeration is needed to prohibit truncation.
        inventory = read_json_object(source_path, "source inventory")
        placements = [
            (placement, relative)
            for placement, relative in _inventory_placements(inventory)
            if placement != "unplaced"
        ]
        if placements:
            _, relative = max(placements)
            month = read_json_object(root / relative / MONTH_MANIFEST, "terminal source month")
            if any(entry.get("last_timestamp", "") >= end for entry in month["adjustments"]):
                raise V6Error(f"{symbol}: requested cutoff would truncate retained source observations")
        validation = store / "validation" / symbol / "validation.manifest.json"
        if validation.exists():
            receipt = read_json_object(validation, "validation receipt")
            if receipt.get("request", {}).get("request_to", "") > end:
                raise V6Error(f"{symbol}: requested cutoff precedes retained source coverage")
