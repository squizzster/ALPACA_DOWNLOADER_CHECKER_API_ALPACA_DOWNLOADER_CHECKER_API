"""Producer-owned comparison facts and shared assignment bodies.

The receipt binds each handoff to one assignment generation. Alias views select
the same segment list; readers assemble it without hashing or deriving ownership.
Legacy references without a handoff retain selected-body assembly. A present but
incomplete handoff fails closed instead of silently reconstructing it.
"""

from .contract import LineageError, document_digest

VERSION = 1
OBSERVATION_CHOICES_VERSION = 1


class _FrozenDict(dict):
    """JSON-compatible accepted metadata; copies retain the completed value."""

    def _immutable(self, *args, **kwargs):
        raise TypeError("accepted observation choices are immutable")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = __ior__ = _immutable

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


class AcceptedObservationChoices(_FrozenDict):
    """Operation-local schema acceptance, bound to its assignment generation."""

    def __init__(self, views, partitions, generation):
        super().__init__(views=_FrozenDict(views), partitions=tuple(partitions))
        object.__setattr__(self, "generation", generation)

    __setattr__ = __delattr__ = _FrozenDict._immutable


def _valid_run(run):
    fields = {
        "row_start",
        "row_end",
        "row_count",
        "from",
        "to",
        "first_timestamp",
        "last_timestamp",
        "branch",
        "owner",
        "state",
        "historical_symbol",
        "tickers",
    }
    return (
        isinstance(run, dict)
        and fields <= run.keys()
        and all(type(run[key]) is int and run[key] > 0 for key in ("row_start", "row_end", "row_count"))
        and run["row_start"] <= run["row_end"]
        and all(
            isinstance(run[key], str) and run[key] for key in ("from", "to", "first_timestamp", "last_timestamp")
        )
        and run["branch"] in ("correlated", "uncorrelated")
        and run["state"] in (None, "selected", "superseded", "conflict", "incomplete", "unassigned")
        and (run["owner"] is None or isinstance(run["owner"], str) and bool(run["owner"]))
        and (run["historical_symbol"] is None or isinstance(run["historical_symbol"], str))
        and isinstance(run["tickers"], list)
        and all(isinstance(label, str) and label for label in run["tickers"])
        and (run["branch"] != "uncorrelated" or run["state"] == "unassigned" and run["owner"] is None)
        and (run["branch"] != "correlated" or run["state"] != "unassigned")
    )


def accept_observation_choices(receipt):
    """Accept declared metadata once at receipt ingress, without upstream work."""
    from libs.alpaca_pricing_download.contract import ADJUSTMENTS

    for kind, handoff in receipt.get("assignment_handoffs", {}).items():
        if "observation_choices_version" not in handoff and "observation_choices" not in handoff:
            continue
        choices = handoff.get("observation_choices")
        generation = receipt["boundaries"][kind]["sha256"]
        if (
            type(handoff.get("observation_choices_version")) is not int
            or handoff["observation_choices_version"] != OBSERVATION_CHOICES_VERSION
            or handoff.get("generation") != generation
            or not isinstance(choices, dict)
        ):
            raise LineageError("incomplete observation-choice handoff; owning lineage recovery required")
        if isinstance(choices, AcceptedObservationChoices) and choices.generation == generation:
            continue
        if (
            not isinstance(choices.get("views"), dict)
            or set(choices["views"]) != set(ADJUSTMENTS)
            or not isinstance(choices.get("partitions"), list)
            or any(
                type(slot) is not int or not 0 <= slot < len(choices["partitions"])
                for slot in choices["views"].values()
            )
        ):
            raise LineageError("incomplete observation-choice handoff; owning lineage recovery required")
        parts = []
        for part in choices["partitions"]:
            if (
                not isinstance(part, dict)
                or type(part.get("rows")) is not int
                or part["rows"] < 0
                or not isinstance(part.get("runs"), list)
            ):
                raise LineageError("incomplete observation-choice handoff; owning lineage recovery required")
            runs = []
            for run in part["runs"]:
                if not _valid_run(run):
                    raise LineageError("incomplete observation-choice run; owning lineage recovery required")
                runs.append(_FrozenDict({**run, "tickers": tuple(run["tickers"])}))
            parts.append(_FrozenDict(rows=part["rows"], runs=tuple(runs)))
        handoff["observation_choices"] = AcceptedObservationChoices(choices["views"], parts, generation)


def _choice(segment):
    """Project the recorded decision, independently of serving ownership."""
    correlated = segment["branch"] == "correlated"
    identity = segment.get("security_identity") or {}
    tickers = segment.get("observation_tickers")
    if tickers is None:
        tickers = (segment["historical_symbol"],) if segment.get("historical_symbol") else ()
    return _FrozenDict(
        {
            **{
                key: segment[key]
                for key in (
                    "row_start",
                    "row_end",
                    "row_count",
                    "from",
                    "to",
                    "first_timestamp",
                    "last_timestamp",
                    "branch",
                )
            },
            "owner": identity.get("security_id") if correlated else None,
            # An older accepted assignment can have ownership without an explicit
            # numerical choice. Projection never promotes that absence to selected.
            "state": segment.get("observation_selection") if correlated else "unassigned",
            "historical_symbol": segment.get("historical_symbol"),
            "tickers": tuple(tickers),
        }
    )


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
    segments, views, selected, ownership, choices, physical = [], [], {}, [], [], {}
    for item in document["adjustments"]:
        # New output crosses its producer boundary once. This digest identifies
        # equal view bodies for serialization; no historical payload is hashed.
        body = item["segments"]
        slot = physical.get(id(body))
        if slot is None:
            key = document_digest(body)
            slot = selected.get(key)
        if slot is None:
            slot = selected[key] = len(segments)
            segments.append(item["segments"])
            runs, choice_runs, count, last = [], [], 0, None
            for segment in item["segments"]:
                # Choices describe the full physical boundary. Serving ownership
                # separately omits current_year's active-month overlap below.
                choice_runs.append(_choice(segment))
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
            choices.append(_FrozenDict(rows=item["rows"], runs=tuple(choice_runs)))
        physical[id(body)] = slot
        views.append({"adjustment": item["adjustment"], "rows": item["rows"], "segments": slot})
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
        "observation_choices_version": OBSERVATION_CHOICES_VERSION,
        "observation_choices": AcceptedObservationChoices(
            {view["adjustment"]: view["segments"] for view in views}, choices, generation
        ),
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
    if "observation_choices_version" in handoff or "observation_choices" in handoff:
        choices = handoff.get("observation_choices")
        if (
            type(handoff.get("observation_choices_version")) is not int
            or handoff["observation_choices_version"] != OBSERVATION_CHOICES_VERSION
            or not isinstance(choices, AcceptedObservationChoices)
            or choices.generation != handoff["generation"]
        ):
            raise LineageError("unaccepted observation-choice handoff; owning lineage receipt ingress required")
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
