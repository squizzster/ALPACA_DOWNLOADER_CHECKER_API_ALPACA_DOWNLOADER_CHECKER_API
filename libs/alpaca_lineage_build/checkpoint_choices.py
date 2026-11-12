"""Explicit choice capabilities carried by completed serving checkpoints.

The checkpoint borrows lineage's completed projections. No serving ownership
run can supply a missing numerical-selection state.
"""

from copy import deepcopy

from .contract import LineageError
from .observation_carry import membership_choices

VERSION = 1


def project(receipts, generations):
    sources = {}
    for name, receipt in receipts.items():
        handoffs = receipt.get("assignment_handoffs", {})
        if not all(
            kind in handoffs and "observation_choices" in handoffs[kind] for kind in receipt["boundaries"]
        ):
            continue
        sources[name] = {
            "receipt_generation": generations[name],
            "pricing_generation": receipt["source_sha256"],
            "projection": {
                key: receipt[key]
                for key in ("anchor_month", "boundaries", "assignment_summary", "serving_ownership")
            }
            | {
                "assignment_handoffs": {
                    kind: {"observation_choices": handoffs[kind]["observation_choices"]}
                    for kind in receipt["boundaries"]
                }
            },
        }
    return {"version": VERSION, "sources": sources}


def select(checkpoint, name):
    history = checkpoint["history_validation"]
    envelope = history.get("observation_choice_handoffs")
    source = (envelope or {}).get("sources", {}).get(name)
    if (
        not envelope
        or type(envelope.get("version")) is not int
        or envelope["version"] != VERSION
        or not isinstance(source, dict)
        or source.get("receipt_generation") != checkpoint["references"]["lineage"][name]
        or source.get("pricing_generation") != history["inputs"][name]["pricing_generation"]
        or not isinstance(source.get("projection"), dict)
        or not {"anchor_month", "boundaries", "assignment_summary", "serving_ownership", "assignment_handoffs"}
        <= source["projection"].keys()
    ):
        raise LineageError(
            f"{name}: membership recovery lacks explicit accepted numerical choices; "
            "select owning retained lineage reconciliation"
        )
    # This is a completed checkpoint's declared projection, not new evidence
    # requiring another receipt/body read or upstream schema validation.
    return membership_choices(
        source["projection"],
        source_generation=source["pricing_generation"],
        receipt_generation=source["receipt_generation"],
        legacy_boundary=lambda _kind: (_ for _ in ()).throw(
            LineageError("incomplete checkpoint choice projection")
        ),
    )


def copied(value):
    """Keep the completed projection independent of mutable caller metadata."""
    return deepcopy(value)
