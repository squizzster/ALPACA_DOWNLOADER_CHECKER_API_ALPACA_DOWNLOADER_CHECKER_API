"""Compact evidence from an accepted pricing publication, with no storage access."""

from dataclasses import asdict, dataclass
from datetime import date

from .contract import ADJUSTMENTS, symbol_label, timestamp


@dataclass(frozen=True)
class PublishedPricingEvidence:
    symbol: str
    generation: str
    request_from: str
    request_to: str
    asof: str
    feed: str
    timeframe: str
    row_counts: tuple[tuple[str, int], ...]

    def to_report(self):
        return {"schema_version": 1, **asdict(self), "row_counts": dict(self.row_counts)}

    @classmethod
    def from_report(cls, value):
        """Check the handoff itself; its producer already validated the prices."""
        if (
            not isinstance(value, dict)
            or type(value.get("schema_version")) is not int
            or value["schema_version"] != 1
        ):
            raise ValueError("unsupported published-pricing evidence")
        # This is the evidence envelope contract, not authentication of its
        # source or CSVs. Counts describe the producer's complete selected scope,
        # including retained/empty views when the consumer reads only a suffix.
        # DO NOT reopen payloads or validators to prove these counts again.
        generation = value.get("generation")
        if (
            not isinstance(generation, str)
            or len(generation) != 64
            or any(c not in "0123456789abcdef" for c in generation)
        ):
            raise ValueError("invalid published pricing generation")
        scope = {}
        for name in ("symbol", "request_from", "request_to", "asof", "feed", "timeframe"):
            if not isinstance(value.get(name), str) or not value[name]:
                raise ValueError("incomplete published pricing scope")
            scope[name] = value[name]
        if symbol_label(scope["symbol"]) != scope["symbol"] or timestamp(scope["request_from"]) >= timestamp(
            scope["request_to"]
        ):
            raise ValueError("invalid published pricing scope")
        if date.fromisoformat(scope["asof"]).isoformat() != scope["asof"]:
            raise ValueError("invalid published pricing mapping date")
        counts = value.get("row_counts")
        if (
            not isinstance(counts, dict)
            or set(counts) != set(ADJUSTMENTS)
            or any(type(count) is not int or count < 0 for count in counts.values())
        ):
            raise ValueError("incomplete published pricing row counts")
        return cls(
            generation=generation, **scope, row_counts=tuple((name, counts[name]) for name in ADJUSTMENTS)
        )
