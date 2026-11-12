"""Inspect a saved stage argument as data, without restoring execution state."""

import json
from pathlib import Path


class Graph:
    """Select mapping fields without decoding unrelated price/approval objects."""

    def __init__(self, objects):
        if not isinstance(objects, list):
            raise ValueError("saved handoff objects must be a list")
        self.objects = objects

    def record(self, value):
        if isinstance(value, dict) and set(value) == {"ref"}:
            ref = value["ref"]
            if type(ref) is not int or not 0 <= ref < len(self.objects):
                raise ValueError("invalid saved handoff reference")
            return self.objects[ref]
        return value

    def mapping(self, value):
        value = self.record(value)
        if not isinstance(value, dict):
            raise ValueError("expected a saved mapping")
        if "kind" not in value:
            return value
        if value["kind"] not in ("dict", "assignment_mapping", "fact_mapping"):
            raise ValueError("selected handoff field is not a mapping")
        items = value.get("items")
        if not isinstance(items, list) or any(
            not isinstance(item, list) or len(item) != 2 or not isinstance(item[0], str) for item in items
        ):
            raise ValueError("selected saved mapping requires string-key metadata")
        return dict(items)

    def field(self, value, name):
        return self.mapping(value).get(name)

    def plain(self, value, seen=None):
        # Only explicitly selected metadata is traversed. No constructors,
        # validators or current policy/runtime compatibility checks are called.
        seen = set() if seen is None else seen
        if isinstance(value, dict) and set(value) == {"ref"}:
            ref = value["ref"]
            if ref in seen:
                raise ValueError("cyclic saved metadata reference")
            return self.plain(self.record(value), seen | {ref})
        if isinstance(value, list):
            return [self.plain(item, seen) for item in value]
        if not isinstance(value, dict):
            return value
        kind = value.get("kind")
        if kind in ("dict", "assignment_mapping", "fact_mapping"):
            return {key: self.plain(item, seen) for key, item in value["items"]}
        if kind in ("list", "tuple", "set", "frozenset", "fact_list"):
            return [self.plain(item, seen) for item in value["items"]]
        if kind is not None:
            return {"omitted_encoded_kind": kind}
        return {key: self.plain(item, seen) for key, item in value.items()}


def inspect_handoff(path, symbols):
    path = Path(path)
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError("saved handoff must be a JSON object")
    schema = value.get("schema")
    if schema not in ("alpaca.pipeline.prepared", "alpaca.pipeline.histories"):
        if isinstance(value.get("stages"), dict) and type(value.get("complete")) is bool:
            return {
                "status": "complete",
                "execution_complete": True,
                "kind": "pipeline_result",
                "result": value,
            }
        raise ValueError("expected a saved prepared, histories or final result document")
    graph = Graph(value["objects"])
    body = value["prepared" if schema.endswith("prepared") else "histories"]
    context = body.get("context", body.get("state", {}).get("context", {}))
    index = context.get("index")
    entries = graph.field(index, "symbols") if index is not None else None
    selected = {}
    for symbol in symbols:
        entry = graph.field(entries, symbol) if entries is not None else None
        selected[symbol] = graph.plain(entry)
    return {
        "status": "complete",
        "execution_complete": True,
        "kind": schema,
        "schema_version": value.get("schema_version"),
        "operation": value.get("operation"),
        "requests": value.get("requests"),
        "object_count": len(value["objects"]),
        "stage_result": graph.plain(body.get("result")),
        "storage_symbols": graph.plain(body.get("storage_symbols")),
        "selected_index_entries": selected,
        "limits": [
            "Historical argument inspection does not establish current applicability or authorize execution.",
            "Encoded prices and acceptance objects are not restored; unknown kinds remain explicitly omitted.",
        ],
    }
