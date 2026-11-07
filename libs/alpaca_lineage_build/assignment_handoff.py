"""Producer-owned comparison facts and shared assignment bodies.

The receipt binds each handoff to one assignment generation. Alias views select
the same segment list; readers assemble it without hashing or deriving ownership.
Legacy references without a handoff retain selected-body assembly. A present but
incomplete handoff fails closed instead of silently reconstructing it.
"""

from .contract import LineageError, document_digest

VERSION = 1


def comparison(document):
    kind = document["boundary"].removeprefix("alpaca_lineage_")
    projected = {key: value for key, value in document.items() if key != "anchor_month"}
    if kind == "current_year":
        projected["adjustments"] = []
        for item in document["adjustments"]:
            segments = [s for s in item["segments"] if s["from"][:7] < document["anchor_month"]]
            projected["adjustments"].append(
                {**item, "segments": segments, "rows": sum(s["row_count"] for s in segments)}
            )
    return document_digest(projected)


def produce(document, generation, *, compared=None):
    segments, views, selected, ownership = [], [], {}, []
    for item in document["adjustments"]:
        # New output crosses its producer boundary once. This digest identifies
        # equal view bodies for serialization; no historical payload is hashed.
        key = document_digest(item["segments"])
        if key not in selected:
            selected[key] = len(segments)
            segments.append(item["segments"])
            runs, count, last = [], 0, None
            for segment in item["segments"]:
                if (
                    document["boundary"] == "alpaca_lineage_current_year"
                    and segment["from"][:7] >= document["anchor_month"]
                ):
                    continue
                identity = segment.get("security_identity") or {}
                owner = (
                    [
                        identity.get("security_id"),
                        segment.get("observation_tickers", [segment.get("historical_symbol")]),
                    ]
                    if segment["branch"] == "correlated" and segment.get("observation_selection") != "superseded"
                    else [None, None]
                )
                if runs and runs[-1]["owner"] == owner:
                    runs[-1]["rows"] += segment["row_count"]
                else:
                    runs.append({"owner": owner, "rows": segment["row_count"]})
                count += segment["row_count"]
                last = max(last or segment["to"], segment["to"])
            ownership.append({"rows": count, "last_session": last, "ownership": runs})
        views.append({"adjustment": item["adjustment"], "rows": item["rows"], "segments": selected[key]})
    projection = {
        "version": VERSION,
        "header": {k: v for k, v in document.items() if k != "adjustments"},
        "views": views,
        "segments": segments,
    }
    return {
        "version": VERSION,
        "generation": generation,
        "comparison": comparison(document) if compared is None else compared,
        "projection_sha256": document_digest(projection),
        "ownership": {view["adjustment"]: ownership[view["segments"]] for view in views},
        "_projection": projection,
    }


def selected(receipt, kind):
    required = receipt.get("assignment_handoff_kinds", ())
    handoff = receipt.get("assignment_handoffs", {}).get(kind)
    if handoff is None:
        if kind in required:
            raise LineageError("missing assignment handoff; resume lineage publication")
        return None
    if handoff.get("version") != VERSION or handoff.get("generation") != receipt["boundaries"][kind]["sha256"]:
        raise LineageError("assignment handoff differs from selected generation; resume lineage publication")
    if not {"comparison", "projection_sha256", "ownership"} <= handoff.keys():
        raise LineageError("incomplete assignment handoff; resume lineage publication")
    return handoff


def assemble(projection):
    if projection.get("version") != VERSION:
        raise LineageError("unsupported assignment projection; resume lineage publication")
    return {
        **projection["header"],
        "adjustments": [
            {**view, "segments": projection["segments"][view["segments"]]} for view in projection["views"]
        ],
    }


def serving_ownership(receipt):
    """Join completed boundary runs at the producer, once per new combination."""
    selected_parts = {kind: selected(receipt, kind) for kind in receipt["boundaries"]}
    if any(value is None for value in selected_parts.values()):
        return None  # Legacy publication: the reader assembles selected bodies.
    views, offsets = {}, {}
    from libs.alpaca_pricing_download.contract import ADJUSTMENTS

    for adjustment in ADJUSTMENTS:
        count, last, runs, positions = 0, None, [], {}
        for kind in ("completed_years", "current_year", "current_month"):
            part = selected_parts[kind]
            accepted = part["ownership"][adjustment]
            positions[kind] = count
            count += accepted["rows"]
            if accepted["last_session"]:
                last = max(last or accepted["last_session"], accepted["last_session"])
            for run in accepted["ownership"]:
                if runs and runs[-1]["owner"] == run["owner"]:
                    runs[-1]["rows"] += run["rows"]
                else:
                    runs.append(dict(run))
        views[adjustment] = {"rows": count, "last_session": last, "ownership": runs}
        offsets[adjustment] = positions
    return {"views": views, "offsets": offsets}
