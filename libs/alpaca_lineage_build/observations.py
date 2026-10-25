"""Choose numerical observations once, while producing a complete group handoff.

Identity assignments answer who owned a row. They do not make an old ticker's
symbol-only response authoritative over the continuing entity's mapped history.
Keep those assignments intact and publish a separate choice for all eight views.
"""

from collections import defaultdict
from copy import deepcopy

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp

from .contract import LineageError

# DO NOT REMOVE WITHOUT USER AUTH!#
# This is the producer's numerical decision, independent of identity membership.
# Retried consumers must never rediscover it or open unrelated source histories.


def _own_interval(report, segment, last):
    interval = dict(segment.get("ticker_ownership_interval") or {})
    members = set(segment["security_identity"].get("member_alpaca_asset_ids", ()))
    day = timestamp(last).astimezone(NEW_YORK).date().isoformat()
    # DO NOT REMOVE WITHOUT USER AUTH!#
    # A split may change CUSIP/UUID within an already accepted public identity.
    # Its old UUID's arrival interval can remain open while the new UUID carries
    # the departure. Consume the producer's complete accepted identity and dated
    # intervals; do not mistake that old open interval for a still-current route.
    # An unrelated occupant or an interval outside the observed day cannot bind it.
    for other in report.get("ticker_ownership_intervals", ()):
        if other["historical_symbol"] != segment["historical_symbol"] or other.get("asset_id") not in members:
            continue
        lower, upper = other.get("start_date_inclusive"), other.get("end_date_exclusive")
        if (lower is not None and day < lower) or (upper is not None and day >= upper):
            continue
        if upper is not None:
            interval["end_date_exclusive"] = min(interval.get("end_date_exclusive") or upper, upper)
    return interval


def _mapping_correction(symbol, pid, inputs, own, occupants):
    route = own[pid].get(symbol)
    if route is None:
        return None
    last, interval = route
    source = inputs[symbol].source
    asof = source["latest_request"]["asof"]
    lower, upper = interval.get("start_date_inclusive"), interval.get("end_date_exclusive")
    if (lower is None or lower <= asof) and (upper is None or asof < upper):
        return None
    # DO NOT REMOVE WITHOUT USER AUTH!#
    # For an actual observation contest, an accepted occupancy disproves this
    # request's entity mapping independently of whether raw values happen to
    # agree. Do not remap a reused label's other ID as a side effect of repair.
    if occupants != {pid}:
        # DO NOT REMOVE WITHOUT USER AUTH!#
        # A source request serves the whole retained history, not this PID alone.
        # An older occupant's closure cannot disprove that request when the same
        # source independently observes a later own-label occupant. Keep the
        # per-PID numerical choice; that older closure authorizes no replacement.
        # The later occupant need not win its own numerical contest. Conversely,
        # borrowed labels, equal dates and an invalid latest occupant still cannot
        # authorize a source-wide remap that would overwrite another accepted ID.
        if any(
            other_pid != pid and symbol in routes and routes[symbol][0] > last
            for other_pid, routes in own.items()
        ):
            return None
        raise LineageError(f"{symbol}: conflicting observations require an unambiguous mapping route")
    return {
        "from": source["history_start"][:10],
        "end_exclusive": source["latest_request"]["end_exclusive"][:10],
        "refresh_all": True,
        "selected_source_sha256": inputs[symbol].source_sha256,
        "mapping_asof": timestamp(last).astimezone(NEW_YORK).date().isoformat(),
    }


def _single_source(inputs, reports):
    """The common case has no numerical contest and needs no per-row candidates."""
    symbol, report = next(iter(reports.items()))
    selected = deepcopy(reports)
    segments = selected[symbol]["correlated"]["segments"]
    for segment in segments:
        segment["observation_selection"] = "selected"
        segment["observation_tickers"] = [segment["historical_symbol"]]
    # DO NOT REMOVE WITHOUT USER AUTH!#
    # Trust a completed pricing guarantee. Legacy bundles need this new guarantee
    # established once during the explicit lineage-version migration, using only
    # the producer's already selected rows; never reopen pricing at a consumer.
    if not inputs[symbol].source.get("activity_consistent"):
        from libs.alpaca_pricing_download.activity import validate_activity

        assembled = {name: [] for name in ADJUSTMENTS}
        for segment in segments:
            name = segment["adjustment"]
            assembled[name].extend(inputs[symbol].views[name][segment["row_start"] - 1 : segment["row_end"]])
        validate_activity(assembled)
    return selected, {}


def select_observations(inputs, reports):
    """Return selected reports and only the mapping corrections they require.

    Inputs are the producer's already decoded, locked group. This function does
    no storage discovery, authentication or acquisition. Consumers later use the
    published row choices without repeating any of this work.
    """
    ownership, rows, own = {}, {}, defaultdict(dict)
    labels = defaultdict(set)
    for symbol, report in reports.items():
        view = inputs[symbol]
        rows[symbol] = view.views
        ownership[symbol] = {}
        for adjustment in ADJUSTMENTS:
            owners = [None] * len(view.views[adjustment])
            for segment in report["correlated"]["segments"]:
                if segment["adjustment"] != adjustment:
                    continue
                pid = segment["security_identity"]["security_id"]
                for index in range(segment["row_start"] - 1, segment["row_end"]):
                    owners[index] = pid
                    if len(reports) > 1:
                        labels[(pid, view.views[adjustment][index][0])].add(segment["historical_symbol"])
                if adjustment == "raw" and segment["historical_symbol"] == symbol:
                    last = view.views[adjustment][segment["row_end"] - 1][0]
                    if symbol not in own[pid] or last > own[pid][symbol][0]:
                        own[pid][symbol] = (last, _own_interval(report, segment, last))
            ownership[symbol][adjustment] = owners

    if len(reports) == 1:
        return _single_source(inputs, reports)

    candidates = defaultdict(dict)
    for symbol, views in rows.items():
        for adjustment, values in views.items():
            for index, row in enumerate(values):
                pid = ownership[symbol][adjustment][index]
                if pid is not None:
                    candidates[(pid, row[0])].setdefault(symbol, {})[adjustment] = row

    choices, corrections, contested = {}, {}, set()
    for (pid, instant), sources in candidates.items():
        if len(sources) == 1:
            choices[(pid, instant)] = next(iter(sources))
            continue
        if any(len({views[a] for views in sources.values() if a in views}) > 1 for a in ADJUSTMENTS):
            contested.add((pid, instant))
        complete = {name: views for name, views in sources.items() if set(views) == set(ADJUSTMENTS)}
        # The most recent independently observed own label identifies the
        # continuing entity's request route, rather than a price/count maximum.
        # Never let a partial eight-view source displace a complete bundle.
        ranks = {name: own[pid].get(name, ("", {}))[0] for name in complete}
        latest = max(ranks.values(), default="")
        preferred = [name for name, rank in ranks.items() if rank == latest and rank]
        if len(preferred) != 1:
            # Exact bundles need no authority contest. A lexical tie is safe only
            # after complete numerical equality; genuine ambiguity stays explicit.
            bundles = {tuple(views[a] for a in ADJUSTMENTS) for views in complete.values()}
            choices[(pid, instant)] = min(complete) if len(bundles) == 1 else None
            continue
        winner = preferred[0]
        choices[(pid, instant)] = winner

    # DO NOT REMOVE WITHOUT USER AUTH!#
    # Repair only a selected source involved in an actual numerical disagreement.
    # Retirement alone does not invalidate trusted retained history. Equal bundles
    # and disjoint histories need no network work; raw equality alone is weaker
    # and cannot conceal a disagreement in any of the seven adjusted views.
    selected_routes = {(pid, winner) for (pid, _), winner in choices.items() if winner is not None}
    correction_routes = {
        (pid, choices[(pid, instant)]) for pid, instant in contested if choices[(pid, instant)] is not None
    }
    for pid, winner in sorted(correction_routes):
        occupants = {owner for owner in ownership[winner]["raw"] if owner is not None}
        correction = _mapping_correction(winner, pid, inputs, own, occupants)
        if correction is not None:
            corrections[winner] = correction

    selected = deepcopy(reports)
    assembled = defaultdict(lambda: {name: [] for name in ADJUSTMENTS})
    for symbol, report in selected.items():
        segments = []
        for segment in report["correlated"]["segments"]:
            pid = segment["security_identity"]["security_id"]
            values = rows[symbol][segment["adjustment"]]
            run = None
            for index in range(segment["row_start"] - 1, segment["row_end"]):
                winner = choices[(pid, values[index][0])]
                tickers = sorted(labels[(pid, values[index][0])])
                state = "conflict" if winner is None else "selected" if winner == symbol else "superseded"
                if state == "selected":
                    assembled[pid][segment["adjustment"]].append(values[index])
                # DO NOT REMOVE WITHOUT USER AUTH!#
                # Price authority cannot erase independently accepted ticker
                # labels. Publish their union separately, including ambiguity,
                # and split a run whenever either accepted decision changes.
                if run is None or run["observation_selection"] != state or run["observation_tickers"] != tickers:
                    run = {
                        **segment,
                        "row_start": index + 1,
                        "row_end": index + 1,
                        "observation_selection": state,
                        "observation_tickers": tickers,
                    }
                    segments.append(run)
                else:
                    run["row_end"] = index + 1
        report["correlated"]["segments"] = segments
    if not corrections and any(
        not inputs[symbol].source.get("activity_consistent") for _, symbol in selected_routes
    ):
        from libs.alpaca_pricing_download.activity import validate_activity

        # DO NOT REMOVE WITHOUT USER AUTH!#
        # Establish the new handoff's activity guarantee from the selected rows.
        # Legacy inputs promised exact numbers, not cross-view consistency. No
        # reader or export repeats this producer check after publication.
        for views in assembled.values():
            validate_activity(views)
    return selected, corrections
