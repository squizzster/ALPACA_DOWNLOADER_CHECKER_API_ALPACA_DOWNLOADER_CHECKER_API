"""Restrict only the selected public ID's assignments to dates supported by its issuer route."""

from bisect import bisect_left, bisect_right

from .duplicate_assignments import UnsupportedOverlap


class OccupationProjection:
    def __init__(self, tape, owner, model, label, span, ranges, ownership, *, terminal=None):
        self.tape = tape
        self.dates = sorted(tape)
        self.owners = {owner["security_id"]: owner}
        self.completed = model
        self.first = model["selection"][0]["first"]
        self.terminal = terminal
        self.pivot = terminal["effective_date"] if terminal else self.first
        self.label = label
        self.span_first = span["first_session"]
        self.end = next((r["first_session"] for r in ranges if r["first_session"] > span["last_session"]), None)
        if terminal:
            self.end = self.pivot
        bounds = [
            r
            for r in ownership
            if r["ticker"] == label
            and (r["start_inclusive"] or self.first) <= self.first
            and (r["end_exclusive"] is None or self.first < r["end_exclusive"])
        ]
        cusips = {r["cusip"] for r in bounds}
        self.cusip = terminal["old_cusip"] if terminal else next(iter(cusips)) if len(cusips) == 1 else None

    def segments(self, seg, mode):
        if seg["branch"] != "correlated" or seg["security_identity"]["security_id"] not in self.owners:
            return [seg]

        if (self.terminal or seg["from"] < self.pivot) and (
            seg.get("historical_symbol") != self.label or seg["from"] < self.span_first
        ):
            raise UnsupportedOverlap(
                "another dated label or earlier assignment lies outside the occupation scope"
            )

        def unassigned(part):
            return {
                "correlation_rule": {"order": 1, "name": "provider_dated_trading_occupation"},
                "warnings": [],
                "reason_codes": [
                    "later_unit_occupation_owner_unresolved"
                    if self.terminal
                    else "earlier_ticker_occupation_owner_unresolved"
                ],
                "candidate_securities": [],
                "market_data_series_identity": None,
                "branch": "uncorrelated",
                **{
                    k: part[k]
                    for k in (
                        "row_start",
                        "row_end",
                        "row_count",
                        "first_timestamp",
                        "last_timestamp",
                        "from",
                        "to",
                    )
                },
            }

        def assigned(part):
            op = part.get("ticker_ownership_interval")
            if part.get("historical_symbol") != self.label or self.end is not None and part["from"] >= self.end:
                return part
            if (
                self.terminal
                and op
                and (
                    op.get("cusip") not in (None, self.cusip)
                    or op.get("end_date_exclusive")
                    and op["end_date_exclusive"] < self.end
                )
            ):
                raise UnsupportedOverlap("a competing unit scope restricts the retained issuer")
            actions = [self.terminal["id"]] if self.terminal else []
            if self.terminal or not op:
                return {
                    **part,
                    "ticker_ownership_interval": {
                        "historical_symbol": self.label,
                        "start_date_inclusive": self.first,
                        "end_date_exclusive": self.end,
                        "asset_id": part["canonical_security"]["alpaca_asset_id"],
                        "cusip": self.cusip,
                        "supporting_corporate_action_ids": actions,
                        "ownership_interval_rules": [
                            {
                                "order": 1,
                                "name": "provider_dated_trading_occupation",
                                "supporting_corporate_action_ids": actions,
                            }
                        ],
                    },
                }
            if (op.get("start_date_inclusive") or "") >= self.first:
                return part
            return {**part, "ticker_ownership_interval": {**op, "start_date_inclusive": self.first}}

        earlier, later = (assigned, unassigned) if self.terminal else (unassigned, assigned)
        if seg["to"] < self.pivot:
            return [earlier(seg)]
        if self.pivot <= seg["from"]:
            return [later(seg)]
        lo = bisect_left(self.dates, seg["from"])
        hi = bisect_right(self.dates, seg["to"])
        pivot = bisect_left(self.dates, self.pivot)
        if (
            not lo < pivot < hi
            or hi - lo != seg["row_count"]
            or self.dates[lo] != seg["from"]
            or self.dates[hi - 1] != seg["to"]
        ):
            raise UnsupportedOverlap("physical daily positions differ at first supported trade")
        left, right = self.dates[pivot - 1], self.dates[pivot]
        split = seg["row_start"] + pivot - lo
        return [
            earlier(
                {
                    **seg,
                    "to": left,
                    "row_end": split - 1,
                    "row_count": pivot - lo,
                    "last_timestamp": self.tape[left]["timestamp"],
                }
            ),
            later(
                {
                    **seg,
                    "from": right,
                    "row_start": split,
                    "row_count": hi - pivot,
                    "first_timestamp": self.tape[right]["timestamp"],
                }
            ),
        ]
