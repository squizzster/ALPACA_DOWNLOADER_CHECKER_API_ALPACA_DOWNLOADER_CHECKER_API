"""Construct replacement assignment metadata for a newly selected overlap.

All numerical selections and all assigned positions come from the previously
selected public histories. Raw tapes, already needed to identify the duplicate
trades, supply timestamps solely for subdivision of accepted daily positions.
"""

from bisect import bisect_left, bisect_right
from copy import deepcopy
from itertools import groupby

from .contract import document_digest


class UnsupportedOverlap(ValueError):
    """The selected histories cannot form a single publishable sequence."""


class AssignmentProjection:
    def __init__(self, tapes, identities, completed, documents, receipts, transition=None):
        self.owners = identities
        self.completed = completed
        self.transition = transition
        self.tapes = tapes
        self.dates = {pid: sorted(rows) for pid, rows in tapes.items()}
        self.selections = {}
        self.metadata = {}
        for piece in completed["selection"]:
            self.selections.update(
                (day, piece["source_id"])
                for day in self.dates[piece["source_id"]]
                if piece["first"] <= day <= piece["last"]
            )
        # Physical current-year assignments overlap the independently selected
        # current-month partition. Consume the logical last month once.
        for symbol, boundaries in documents.items():
            for kind, document in boundaries.items():
                for view in document["adjustments"]:
                    adjustment = view["adjustment"]
                    for segment in view["segments"]:
                        if (
                            kind == "current_year"
                            and segment["from"][:7] >= receipts[symbol]["anchor_month"]
                            or segment["branch"] != "correlated"
                            or segment.get("observation_selection") == "superseded"
                        ):
                            continue
                        pid = segment["security_identity"]["security_id"]
                        if pid not in identities:
                            continue
                        if segment.get("observation_selection") != "selected":
                            raise UnsupportedOverlap("one affected source lacks a complete numerical selection")
                        for day in self.segment_days(pid, segment):
                            if self.selections.get(day) != pid:
                                continue
                            key = adjustment, day
                            if key in self.metadata:
                                raise UnsupportedOverlap(
                                    "two contributing sources claim the selected observation"
                                )
                            self.metadata[key] = segment

    def segment_days(self, pid, segment):
        days = self.dates[pid]
        lo = bisect_left(days, segment["from"])
        hi = bisect_right(days, segment["to"])
        if (
            lo >= hi
            or hi - lo != segment["row_count"]
            or days[lo] != segment["from"]
            or days[hi - 1] != segment["to"]
        ):
            raise UnsupportedOverlap("published dates do not cover the entire selected source assignment")
        return days[lo:hi]

    def segments(self, segment, mode):
        if segment["branch"] != "correlated":
            return [segment]
        pid = segment["security_identity"]["security_id"]
        if pid not in self.owners:
            return [segment]
        days = self.segment_days(pid, segment)
        offset = segment["row_start"]

        def selection(day):
            peer = self.metadata.get((mode, day))
            if peer is None:
                raise UnsupportedOverlap("selected owner lacks a dated observation")
            return self.selections[day], id(peer)

        output = []
        for (source, _), group in groupby(days, selection):
            period = list(group)
            first, last = period[0], period[-1]
            peer = self.metadata[mode, first]
            target = self.owners[source]
            new = {
                **segment,
                "row_start": offset,
                "row_end": offset + len(period) - 1,
                "row_count": len(period),
                "first_timestamp": self.tapes[pid][first]["timestamp"],
                "last_timestamp": self.tapes[pid][last]["timestamp"],
                "from": first,
                "to": last,
                "security_identity": target,
                "canonical_security": {**peer["canonical_security"], "security_id": target["security_id"]},
                "historical_symbol": peer["historical_symbol"],
                "observation_tickers": peer["observation_tickers"],
                "ticker_ownership_interval": deepcopy(peer.get("ticker_ownership_interval")),
                "observation_selection": segment.get("observation_selection") if pid == source else "superseded",
            }
            if self.transition:
                action = self.transition
                label = new["historical_symbol"]
                interval = new["ticker_ownership_interval"]
                if label == action["new_symbol"] and first >= action["ex_date"]:
                    new["ticker_ownership_interval"] = {
                        "historical_symbol": label,
                        "start_date_inclusive": action["ex_date"],
                        "end_date_exclusive": None,
                        "supporting_corporate_action_ids": [action["id"]],
                        "asset_id": new["canonical_security"]["alpaca_asset_id"],
                        "cusip": action["new_cusip"],
                        "ownership_interval_rules": [
                            {
                                "order": 1,
                                "name": "split_with_dated_ticker_change",
                                "supporting_corporate_action_ids": [action["id"]],
                            }
                        ],
                    }
                elif interval and label == action["symbol"]:
                    interval["end_date_exclusive"] = min(
                        interval["end_date_exclusive"] or action["ex_date"], action["ex_date"]
                    )
            output.append(new)
            offset += len(period)
        return output


def rewrite(source, receipt, original, projection, relation=None):
    """Keep unrelated positions and all original numerical payloads closed."""
    changed = {}
    newly_unresolved = 0
    periods = {
        (x["from"], x["to"], x["security_id"])
        for x in receipt["identity_intervals"]
        if x["security_id"] not in projection.owners
    }
    routes = {pid: value for pid, value in receipt["observation_routes"].items() if pid not in projection.owners}
    for kind, body in original.items():
        adjustments = []
        dirty = False
        for view in body["adjustments"]:
            segments = []
            for before in view["segments"]:
                pieces = projection.segments(before, view["adjustment"])
                dirty |= pieces != [before]
                segments.extend(pieces)
                for seg in pieces:
                    if kind == "current_year" and seg["from"][:7] >= receipt["anchor_month"]:
                        continue
                    if seg["branch"] != "correlated":
                        if before["branch"] == "correlated":
                            newly_unresolved += seg["row_count"]
                        continue
                    pid = seg["security_identity"]["security_id"]
                    if pid not in projection.owners:
                        continue
                    periods.add((seg["from"], seg["to"], pid))
                    if view["adjustment"] == "raw" and seg["historical_symbol"] == source:
                        bound = (seg["last_timestamp"], seg["ticker_ownership_interval"] or {})
                        if pid not in routes or routes[pid][0] < bound[0]:
                            routes[pid] = bound
            adjustments.append({**view, "segments": segments})
        if dirty:
            changed[kind] = {**body, "adjustments": adjustments}
    new = {**receipt, "duplicate_source_slices": projection.completed}
    if newly_unresolved:
        new["correlated_rows"] -= newly_unresolved
        new["unresolved_rows"] += newly_unresolved
    if relation is not None:
        new["provider_split_relations"] = [*receipt.get("provider_split_relations", ()), relation]
    if changed:
        owners = {
            i["security_id"]: i
            for i in receipt["identity_memberships"]
            if i["security_id"] not in projection.owners
        }
        owners.update({pid: projection.owners[pid] for _, _, pid in periods if pid in projection.owners})
        new.update(
            identity_memberships=list(owners.values()),
            observation_routes={pid: list(value) for pid, value in routes.items()},
            identity_intervals=[dict(zip(("from", "to", "security_id"), part)) for part in sorted(periods)],
            boundaries={
                k: {**ref, "sha256": document_digest(changed[k])} if k in changed else ref
                for k, ref in receipt["boundaries"].items()
            },
            assignment_handoffs={
                k: entry for k, entry in receipt.get("assignment_handoffs", {}).items() if k not in changed
            },
        )
        new.pop("numerical_source_equivalence", None)
        new.pop("numerical_source_equivalence_version", None)
    return new, changed


def bind_retained_labels(receipts, affected_ids):
    """Old market-label plans on disjoint identities keep their accepted assignments."""
    assignments = {name: receipt["boundaries"] for name, receipt in receipts.items()}
    for receipt in receipts.values():
        completion = receipt.get("dated_label_completion")
        if completion is None:
            continue
        plan = receipt["dated_label_plan"]
        if any(item.get("public_id") in affected_ids for item in (*plan["preferences"], *plan["reasons"])):
            raise UnsupportedOverlap("a dated market-label decision intersects the selected ownership scope")
        receipt["dated_label_completion"] = {**completion, "assignments": assignments}
