"""Transport completed selected bundles with explicit physical alias slots."""

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, COLUMNS

from .assignment_handoff import _FrozenDict
from .contract import LineageError


class AcceptedSourceBundles(_FrozenDict):
    def __init__(self, envelope, observations):
        super().__init__(envelope)
        object.__setattr__(self, "observations", observations)

    __setattr__ = __delattr__ = _FrozenDict._immutable


def accept(value, revision):
    if isinstance(value, AcceptedSourceBundles) and value["revision_id"] == revision["id"]:
        return value
    if (
        not isinstance(value, dict)
        or set(value) != {"version", "revision_id", "identities"}
        or type(value.get("version")) is not int
        or value["version"] != 1
        or value["revision_id"] != revision["id"]
        or not isinstance(value["identities"], dict)
        or value["identities"].keys() != revision["sessions"].keys()
    ):
        raise LineageError("incomplete selected-observation bundle handoff")
    identities, observations = {}, {}
    for pid, days in value["identities"].items():
        if not isinstance(days, dict) or set(days) != set(revision["sessions"][pid]):
            raise LineageError("selected-observation sessions differ from their completed revision")
        encoded, expanded = {}, {}
        for day, entry in days.items():
            if (
                not isinstance(entry, dict)
                or set(entry) != {"views", "partitions"}
                or not isinstance(entry["views"], dict)
                or set(entry["views"]) != set(ADJUSTMENTS)
                or not isinstance(entry["partitions"], (list, tuple))
                or not entry["partitions"]
                or any(
                    type(slot) is not int or not 0 <= slot < len(entry["partitions"])
                    for slot in entry["views"].values()
                )
            ):
                raise LineageError("selected-observation aliases require complete view slots")
            parts = []
            for part in entry["partitions"]:
                if (
                    not isinstance(part, dict)
                    or set(part) != {"source", "row"}
                    or part["source"] != revision["selected_sources"][pid][day]
                    or not isinstance(part["row"], (list, tuple))
                    or len(part["row"]) != len(COLUMNS)
                    or any(not isinstance(field, str) for field in part["row"])
                ):
                    raise LineageError("selected-observation part has an incompatible source or shape")
                parts.append(_FrozenDict(source=part["source"], row=tuple(part["row"])))
            encoded[day] = _FrozenDict(views=_FrozenDict(entry["views"]), partitions=tuple(parts))
            expanded[day] = _FrozenDict({view: parts[slot] for view, slot in entry["views"].items()})
        identities[pid] = _FrozenDict(encoded)
        observations[pid] = _FrozenDict(expanded)
    return AcceptedSourceBundles(
        {"version": 1, "revision_id": revision["id"], "identities": _FrozenDict(identities)},
        _FrozenDict(observations),
    )


def produce(bundles, revision):
    identities = {}
    for pid, days in bundles.items():
        identities[pid] = {}
        for day, views in days.items():
            parts, slots, physical = [], {}, {}
            for view in ADJUSTMENTS:
                value = views[view]
                key = value["source"], id(value["row"])
                if key not in physical:
                    physical[key] = len(parts)
                    parts.append(value)
                slots[view] = physical[key]
            identities[pid][day] = {"views": slots, "partitions": parts}
    return accept({"version": 1, "revision_id": revision["id"], "identities": identities}, revision)
