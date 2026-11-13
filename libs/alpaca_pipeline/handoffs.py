"""Native completed stage values; no serialization or producer replay."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Prepared:
    operation: dict
    prepared: dict = field(repr=False)
    requests: dict
    recovery: dict = field(repr=False)
    pricing_client: object = field(repr=False)
    identity_client: object = field(repr=False)


@dataclass(frozen=True)
class Histories:
    operation: dict
    histories: dict = field(repr=False)
    context: object = field(repr=False)
    recovery: dict = field(repr=False)


def summary(handoff):
    """Bounded DEBUG metadata, never a recursive rendering of held values."""
    if isinstance(handoff, Prepared):
        stage, body = "prepare", handoff.prepared
        requests = handoff.requests
    elif isinstance(handoff, Histories):
        stage, body = "histories", handoff.histories
        requests = body["result"]["requests"]
    else:
        raise TypeError("expected a completed native stage handoff")
    result = body["result"]
    return {
        "schema": "alpaca.pipeline.native",
        "schema_version": 1,
        "stage": stage,
        "operation": handoff.operation,
        "requests": requests,
        "counts": {
            "storage_symbols": len(body["storage_symbols"]),
            "recovery_values": len(handoff.recovery["values"]),
            "failed": result["failed"],
            "pending_work": result.get("pending_work"),
        },
        "history_complete": body.get("history_complete"),
        "limits": ["Compact native handoff metadata; held data is neither serialized nor replayable."],
    }
