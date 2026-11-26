"""Refine a selected, previously open ticker occupation without choosing prices again."""

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import datetime, timezone

from libs.alpaca_end_of_day_security_correlation.split_rename_ownership import (
    contrary_literal_trades,
    occupation_conflict,
    split_rename_occupations,
)
from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp

_OBSERVATIONS = ("corporate_actions", "symbol_occupancy_evidence", "input_symbol_occupants")
_TAPES = ("historical_trade_tape", "otc_historical_trade_tape")


@dataclass(frozen=True)
class Projection:
    owner: str
    old: str
    new: str
    stale: dict
    intervals: tuple[dict, ...]
    successor: dict
    through: str


def possible(previous):
    """Old, accepted label collisions select this capability, never a policy sweep."""
    if len(previous) < 2:
        return False
    for old, prior in previous.items():
        if not prior:
            continue
        edges = prior.get("uuid_lineage_edges", ())
        if not any(
            edge.get("corporate_action_id")
            and edge["edge_type"] in ("reverse_split", "forward_split")
            and edge["predecessor"]["symbol"] == old == edge["successor"]["symbol"]
            for edge in edges
        ):
            continue
        successors = {
            edge["successor"]["symbol"]
            for edge in edges
            if edge.get("corporate_action_id")
            and edge["edge_type"] == "name_change"
            and edge["predecessor"]["symbol"] == old
            and edge["successor"]["symbol"] in previous
        }
        if not successors:
            continue
        for item in previous.values():
            raw = (item.get("serving_ownership") or {}).get("views", {}).get("raw")
            if raw is None or any(
                entry["owner"]
                and entry["owner"][1]
                and old in entry["owner"][1]
                and successors.intersection(entry["owner"][1])
                for entry in raw["ownership"]
            ):
                return True
    return False


def evidence_binding(previous):
    # The group producer binds all its native observations, including competing
    # occupations. The existing label completion already binds requests, pricing
    # and assignment generations. A fresh evidence generation invalidates this part.
    from .metadata import provider_evidence

    binding = {}
    for name, item in previous.items():
        generation = item.get("lineage_evidence_sha256")
        if generation:
            binding[name] = generation
        else:
            facts = provider_evidence(item["evidence"])
            binding[name] = {field: facts.get(field, []) for field in _OBSERVATIONS}
            for field in _TAPES:
                binding[name][field] = facts.get(field)
    return binding


def valid_evidence_binding(binding, names):
    from .contract import valid_sha256

    return (
        isinstance(binding, dict)
        and binding.keys() == names
        and all(
            valid_sha256(value)
            or isinstance(value, dict)
            and set(value) == {*_OBSERVATIONS, *_TAPES}
            and all(isinstance(value[field], list) for field in _OBSERVATIONS)
            and all(value[field] is None or isinstance(value[field], dict) for field in _TAPES)
            for value in binding.values()
        )
    )


def plan(previous, evidence, context):
    from libs.alpaca_end_of_day_security_correlation.ownership_interval_rules import split_rename_intervals

    edges = {}
    for receipt in previous.values():
        for edge in receipt.get("uuid_lineage_edges", ()):
            action_id = edge.get("corporate_action_id")
            if not action_id:
                continue
            if action_id in edges and edges[action_id] != edge:
                return (), [{"assessment": "conflicting_native_edges", "action_id": action_id}]
            edges[action_id] = edge
    observations = {
        field: [value for facts in evidence.values() for value in facts.get(field, ())]
        for field in _OBSERVATIONS
    }
    projections, reasons = [], []

    def conflict_reason(occupation, combined):
        known = occupation_conflict(occupation, combined)
        if known:
            return known
        assets = {edge.successor_asset_id for edge in occupation.edges}

        def another_issue(segment, source):
            interval = segment.get("ticker_ownership_interval") or {}
            members = (segment.get("security_identity") or {}).get("member_alpaca_asset_ids")
            return (
                interval.get("historical_symbol") == source
                and members
                and assets.isdisjoint(members)
                and interval.get("asset_id") in members
                and interval.get("cusip")
            )

        for facts in evidence.values():
            for symbol, session in contrary_literal_trades(occupation, facts):
                day = session.isoformat()
                dated = [
                    s
                    for s in context.get(symbol, {}).get("correlated", {}).get("segments", ())
                    if s["adjustment"] == "raw" and s["from"] <= day <= s["to"]
                ]
                if not dated or not all(another_issue(segment, symbol) for segment in dated):
                    return "split_rename_contrary_literal_trading"
        # A legacy literal assignment without a traded-activity capability can
        # contradict the proposed date. Keep a separately identified issue in
        # its own scope; leave other uncertain rows untouched.
        for symbol, prior in previous.items():
            if prior.get("pricing_request", {}).get("asof") != "-":
                continue
            for lower, stop in occupation.literal_windows(symbol):
                first = lower.isoformat() if lower else ""
                end = stop.isoformat() if stop else None
                for kind in ("correlated", "uncorrelated"):
                    for segment in context[symbol].get(kind, {}).get("segments", ()):
                        if (
                            segment["adjustment"] == "raw"
                            and segment["to"] >= first
                            and (end is None or segment["from"] < end)
                            and not another_issue(segment, symbol)
                        ):
                            return "split_rename_contrary_literal_trading"
        return None

    for occupation in split_rename_occupations(
        observations, edges.values(), conflict_reason=conflict_reason, issues=reasons
    ):
        arrival, departure = occupation.arrival, occupation.departure
        old, new = arrival.successor_symbol, departure.successor_symbol
        if old not in previous or new not in previous:
            continue
        middle, last = previous[old], previous[new]
        first, cut = arrival.effective_date.isoformat(), departure.effective_date.isoformat()
        if not first <= (middle.get("pricing_request", {}).get("asof") or "") < cut:
            continue  # A literal old-label source or a different occupation competes.
        path_assets = {edge.successor_asset_id for edge in occupation.edges}
        owners = {
            item["security_id"]
            for prior in previous.values()
            for item in prior.get("identity_memberships", ())
            if path_assets <= set(item["member_alpaca_asset_ids"])
        }
        if len(owners) != 1:
            continue
        pid = next(iter(owners))
        before = middle.get("observation_routes", {}).get(pid)
        after = last.get("observation_routes", {}).get(pid)
        if not before or not after:
            continue
        stale, successor = before[1], after[1]
        if (
            (
                stale.get("historical_symbol"),
                stale.get("asset_id"),
                stale.get("cusip"),
                stale.get("start_date_inclusive"),
                stale.get("end_date_exclusive"),
            )
            != (old, arrival.successor_asset_id, arrival.successor_cusip, first, None)
            or (
                successor.get("historical_symbol"),
                successor.get("asset_id"),
                successor.get("cusip"),
                successor.get("start_date_inclusive"),
            )
            != (new, departure.successor_asset_id, departure.successor_cusip, cut)
            or departure.action_id not in successor.get("supporting_corporate_action_ids", ())
            or arrival.action_id not in stale.get("supporting_corporate_action_ids", ())
        ):
            continue
        through = timestamp(after[0]).astimezone(NEW_YORK).date().isoformat()
        observations_after = [
            (source, segment)
            for source, report in context.items()
            for segment in report["correlated"]["segments"]
            if segment["adjustment"] == "raw"
            and segment["from"] <= through
            and segment["to"] >= cut
            and {old, new}.intersection(segment.get("observation_tickers", ()) or [segment["historical_symbol"]])
        ]
        if not any(
            source == new and segment["from"] == cut and segment["historical_symbol"] == new
            for source, segment in observations_after
        ) or any(
            segment["security_identity"]["security_id"] != pid
            or set(segment.get("observation_tickers", ()) or [segment["historical_symbol"]]) - {old, new}
            or source == old
            and segment["historical_symbol"] == old
            and set(segment.get("observation_tickers", ())) != {old, new}
            for source, segment in observations_after
        ):
            continue
        applied = next(
            (
                rule
                for rule in stale.get("ownership_interval_rules", ())
                if rule["name"] == "name_change_ticker_transition"
            ),
            None,
        )
        if not applied:
            continue
        intervals = tuple(
            {**item.as_report_object(), "cusip": item.cusip, "asset_id": item.asset_id}
            for item in split_rename_intervals(occupation, applied["order"], applied["name"])
        )
        projections.append(Projection(pid, old, new, stale, intervals, successor, through))
        reasons.append(
            {
                "assessment": "bounded_split_rename_occupation",
                "public_id": pid,
                "historical_symbol": old,
                "successor": new,
                "observed_transition": cut,
                "observed_through": through,
                "native_action_ids": [edge.action_id for edge in occupation.edges],
                "reason": "A native split path connects the ticker's arrival and departure. The successor source begins at the departure, and the overlapping older request is mapped to its earlier occupation.",
                "limits": "Dates apply within the accepted public owner and observed successor scope. Conflicting owners, competing labels and unsupported sources retain their assignments.",
            }
        )
    if not projections and not reasons:
        reasons.append(
            {
                "assessment": "split_rename_occupation_unresolved",
                "limits": "The native events, dated source occupations and accepted owner do not establish a unique transition for these labels.",
            }
        )
    return tuple(projections), reasons


def project(reports, projections, dates):
    changed, result = [], {}
    for source, report in reports.items():
        output = []
        for segment in report["correlated"]["segments"]:
            pid = segment["security_identity"]["security_id"]
            labels = segment.get("observation_tickers", ())
            matches = [
                p
                for p in projections
                if p.owner == pid
                and (
                    segment.get("ticker_ownership_interval") == p.stale
                    or set(labels) == {p.old, p.new}
                    and segment["to"] >= p.successor["start_date_inclusive"]
                )
            ]
            if len(matches) != 1:
                output.append(segment)
                continue
            p = matches[0]
            first, last = segment["from"], segment["to"]
            cut = p.successor["start_date_inclusive"]
            points = [
                i["start_date_inclusive"]
                for i in (*p.intervals, p.successor)
                if first < i["start_date_inclusive"] <= last
            ]
            if first <= p.through < last:
                from datetime import date, timedelta

                points.append((date.fromisoformat(p.through) + timedelta(days=1)).isoformat())
            parts = dates(source, segment, points) if points else (segment,)
            for part in parts:
                when = part["from"]
                replacement = part
                if cut <= when <= p.through and set(labels) == {p.old, p.new}:
                    replacement = {**part, "observation_tickers": [p.new]}
                if segment.get("ticker_ownership_interval") == p.stale:
                    interval = next(
                        (i for i in p.intervals if i["start_date_inclusive"] <= when < i["end_date_exclusive"]),
                        None,
                    )
                    if interval is None and cut <= when <= p.through and set(labels) == {p.old, p.new}:
                        interval = p.successor
                    if interval:
                        replacement = {
                            **replacement,
                            "historical_symbol": interval["historical_symbol"],
                            "ticker_ownership_interval": interval,
                            "canonical_security": {
                                **segment["canonical_security"],
                                "alpaca_asset_id": interval["asset_id"],
                                "cusip": interval["cusip"],
                            },
                        }
                output.append(replacement)
                if replacement != segment:
                    changed.append(
                        {
                            "source": source,
                            "public_id": pid,
                            "adjustment": part["adjustment"],
                            "row_start": part["row_start"],
                            "row_end": part["row_end"],
                            "before": list(labels),
                            "after": replacement.get("observation_tickers", []),
                        }
                    )
        result[source] = (
            report
            if output == report["correlated"]["segments"]
            else {**report, "correlated": {**report["correlated"], "segments": output}}
        )
    return result, changed


def cut_dates(segment, days, instants, points):
    marks = sorted({0, len(days), *(bisect_left(days, day) for day in points)})
    parts = []
    for left, stop in zip(marks, marks[1:]):
        parts.append(
            {
                **segment,
                "from": days[left],
                "to": days[stop - 1],
                "row_start": segment["row_start"] + left,
                "row_end": segment["row_start"] + stop - 1,
                "row_count": stop - left,
                "first_timestamp": segment["first_timestamp"] if left == 0 else instants(left),
                "last_timestamp": segment["last_timestamp"] if stop == len(days) else instants(stop - 1),
            }
        )
    return parts


class SessionCuts:
    """Borrow the selected clean serving date grid for already-selected numerical rows."""

    def __init__(self, store, previous, generations, serving_groups=None):
        self.store, self.previous, self.generations = store, previous, generations
        self.records = {}
        self.partitions = {}
        self.serving_groups = serving_groups
        self.group = None

    def _record(self, pid):
        from libs.alpaca_serving_catalogue import storage
        from libs.alpaca_serving_catalogue.contract import CatalogueError

        if self.group is None:
            if not storage.path(self.store).is_file():
                return None
            try:
                generation, group = storage.price_review_group(self.store, pid)
            except CatalogueError as error:
                if str(error) != "price review identity unavailable":
                    raise
                return None
            if group["references"]["lineage"] != self.generations or group["references"]["pricing"] != {
                name: item["source_sha256"] for name, item in self.previous.items()
            }:
                return None
            self.group = group
            if self.serving_groups is not None:
                self.serving_groups[group["locator"]] = group
            else:
                from .recovery_handoffs import remember

                remember(self.store, "serving_group", (generation, group["locator"]), group)
        from .contract import LineageError

        record = next(
            (item for item in self.group["history_validation"]["prefix_records"] if item["public_id"] == pid),
            None,
        )
        if record is None:
            return None
        if record.get("session_projection_version") is not None and (
            record["session_projection_version"] != 1
            or not isinstance(record.get("session_dates"), list)
            or len(record["session_dates"]) != record["coverage"]["session_count"]
        ):
            raise LineageError("incomplete serving session-date projection; owning catalogue recovery required")
        return (
            record
            if not record["price_reasons"]
            and not record.get("dated_price_token")
            and record.get("session_projection_version") == 1
            else None
        )

    def split(self, segment, points, get_rows):
        pid = segment["security_identity"]["security_id"]
        if segment.get("observation_selection") == "selected":
            if pid not in self.records:
                self.records[pid] = self._record(pid)
            record = self.records[pid]
            if record is not None:
                key = pid, segment["from"], segment["to"]
                if key not in self.partitions:
                    dates = record["session_dates"]
                    days = dates[bisect_left(dates, segment["from"]) : bisect_right(dates, segment["to"])]
                    self.partitions[key] = days, {}
                days, timestamps = self.partitions[key]
                if len(days) == segment["row_end"] - segment["row_start"] + 1:
                    # Serving already accepted midnight New York and identical
                    # selected sessions across all views. Preserve both stored
                    # endpoints and express a newly cut interior instant in UTC.
                    def instant(index):
                        if index not in timestamps:
                            timestamps[index] = (
                                datetime.fromisoformat(days[index])
                                .replace(tzinfo=NEW_YORK)
                                .astimezone(timezone.utc)
                                .isoformat()
                                .replace("+00:00", "Z")
                            )
                        return timestamps[index]

                    return cut_dates(segment, days, instant, points)
        rows = get_rows()[segment["row_start"] - 1 : segment["row_end"]]
        return cut_dates(
            segment,
            [timestamp(r[0]).astimezone(NEW_YORK).date().isoformat() for r in rows],
            lambda i: rows[i][0],
            points,
        )
