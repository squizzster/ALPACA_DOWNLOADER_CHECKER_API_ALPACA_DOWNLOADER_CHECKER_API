"""Choose numerical observations once, while producing a complete group handoff.

Identity assignments answer who owned a row. They do not make an old ticker's
symbol-only response authoritative over the continuing entity's mapped history.
Keep those assignments intact and publish a separate choice for all eight views.
"""

from bisect import bisect_left
from collections import defaultdict
from copy import deepcopy

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp

from .contract import LineageError

# DO NOT REMOVE WITHOUT USER AUTH!#
# This is the producer's numerical decision, independent of identity membership.
# Retried consumers must never rediscover it or open unrelated source histories.


def _own_interval(report, segment, last, *, day=None):
    interval = dict(segment.get("ticker_ownership_interval") or {})
    members = set(segment["security_identity"].get("member_alpaca_asset_ids", ()))
    day = day or timestamp(last).astimezone(NEW_YORK).date().isoformat()
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


def _mapping_correction(symbol, pid, inputs, own, occupants, *, instants=None):
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
            other_pid != pid
            and symbol in routes
            and _instant(routes[symbol][0], instants) > _instant(last, instants)
            for other_pid, routes in own.items()
        ):
            return None
        raise LineageError(f"{symbol}: conflicting observations require an unambiguous mapping route")
    return {
        "from": source["history_start"][:10],
        "end_exclusive": source["latest_request"]["end_exclusive"][:10],
        "refresh_all": True,
        "selected_source_sha256": inputs[symbol].source_sha256,
        "mapping_asof": _instant(last, instants).astimezone(NEW_YORK).date().isoformat(),
    }


def _instant(value, cache):
    if cache is None:
        return timestamp(value)
    if value not in cache:
        cache[value] = timestamp(value)
    return cache[value]


def _single_alias_bundle(inputs, reports):
    """Exact row aliases and equal new ownership runs need no session join."""
    symbol, report = next(iter(reports.items()))
    views = inputs[symbol].views
    if any(views[name] is not views["raw"] for name in ADJUSTMENTS):
        return False
    runs = {name: [] for name in ADJUSTMENTS}
    for segment in report["correlated"]["segments"]:
        runs[segment["adjustment"]].append(
            (
                segment["row_start"],
                segment["row_end"],
                segment["security_identity"]["security_id"],
                segment["historical_symbol"],
            )
        )
    return all(runs[name] == runs["raw"] for name in ADJUSTMENTS)


def _single_source(inputs, reports):
    """The common case has no numerical contest and needs no per-row candidates."""
    symbol, report = next(iter(reports.items()))
    selected = deepcopy(reports)
    segments = selected[symbol]["correlated"]["segments"]
    for segment in segments:
        segment["observation_selection"] = "selected"
        segment["observation_tickers"] = sorted(
            set(segment.get("observation_tickers") or [segment["historical_symbol"]])
        )
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


def _partition_choices(reports, retained, opened):
    active, frozen, occupants = {}, {}, {}
    for symbol, report in reports.items():
        carry = retained.get(symbol)
        segments, held = [], []
        occupants[symbol] = set()
        for segment in report["correlated"]["segments"]:
            adjustment = segment["adjustment"]
            owner = segment["security_identity"]["security_id"]
            if adjustment == "raw":
                occupants[symbol].add(owner)
            position, end = segment["row_start"], segment["row_end"]
            for run in carry.runs[adjustment] if carry is not None else ():
                if run.state is None:
                    continue  # Ownership without a choice needs owning selection.
                if run.row_end < position:
                    continue
                if run.row_start > end:
                    break
                left, right = max(position, run.row_start), min(end, run.row_end)
                if left > position:
                    segments.append({**segment, "row_start": position, "row_end": left - 1})
                if owner != run.owner:
                    raise LineageError(f"{symbol}: applicable retained numerical choice changed row ownership")
                first = left
                for index in (
                    *sorted(i for i in opened.get((symbol, adjustment), ()) if left <= i <= right),
                    right + 1,
                ):
                    if first < index:
                        held.append(
                            (
                                {
                                    **segment,
                                    "row_start": first,
                                    "row_end": index - 1,
                                    "observation_selection": run.state,
                                    "observation_tickers": list(run.tickers),
                                },
                                run.first,
                                run.last,
                            )
                        )
                    if index <= right:
                        segments.append(
                            {
                                **segment,
                                "row_start": index,
                                "row_end": index,
                                "observation_tickers": list(run.tickers),
                            }
                        )
                    first = index + 1
                position = right + 1
            if position <= end:
                segments.append({**segment, "row_start": position, "row_end": end})
        active[symbol] = {**report, "correlated": {**report["correlated"], "segments": segments}}
        frozen[symbol] = held
    return active, frozen, occupants


def _open_new_contests(inputs, active, frozen, row_cache, invalidated, instants):
    """Compose only newly selected timestamps with exact held counterpart rows."""
    fresh = set()
    for symbol, report in active.items():
        for segment in report["correlated"]["segments"]:
            rows = inputs[symbol].views[segment["adjustment"]]
            for index in range(segment["row_start"] - 1, segment["row_end"]):
                key = id(rows), index
                if key not in row_cache:
                    row_cache[key] = rows[index]
                fresh.add((segment["security_identity"]["security_id"], _instant(row_cache[key][0], instants)))
    opened, lookups = defaultdict(set), {}
    for owner, instant in fresh:
        day = instant.astimezone(NEW_YORK).date().isoformat()
        if owner in invalidated.get(day, ()):
            continue  # The owning correction already reopened this exact scope.
        for symbol, segments in frozen.items():
            for segment, first, last in segments:
                if segment["security_identity"]["security_id"] != owner:
                    continue
                if not first <= day <= last:
                    continue
                name = segment["adjustment"]
                rows = inputs[symbol].views[name]
                left, right = segment["row_start"] - 1, segment["row_end"]
                key = id(rows), left, right, instant
                if key not in lookups:
                    index = bisect_left(
                        rows, instant, lo=left, hi=right, key=lambda row: _instant(row[0], instants)
                    )
                    if index < right:
                        row_key = id(rows), index
                        if row_key not in row_cache:
                            row_cache[row_key] = rows[index]
                        lookups[key] = index if _instant(row_cache[row_key][0], instants) == instant else None
                    else:
                        lookups[key] = None
                if lookups[key] is not None:
                    opened[symbol, name].add(lookups[key] + 1)
    return opened


def select_observations(
    inputs, reports, *, retained_choices=None, invalidated_scopes=None, instant_keys=None, equality_outcomes=None
):
    """Carry completed choices, selecting only genuinely active observations."""
    if not retained_choices:
        return _select_new_observations(
            inputs, reports, instants=instant_keys, equality_outcomes=equality_outcomes
        )
    for name, carry in retained_choices.items():
        if carry.source_generation != inputs[name].source_sha256:
            raise LineageError(f"{name}: retained numerical choice belongs to another pricing generation")
    active, frozen, occupants = _partition_choices(reports, retained_choices, {})
    row_cache, instants = {}, {} if instant_keys is None else instant_keys
    if any(report["correlated"]["segments"] for report in active.values()):
        opened = _open_new_contests(inputs, active, frozen, row_cache, invalidated_scopes or {}, instants)
        if opened:
            active, frozen, occupants = _partition_choices(reports, retained_choices, opened)
        selected, corrections = _select_new_observations(
            inputs, active, row_cache=row_cache, source_occupants=occupants, instants=instants
        )
    else:
        selected, corrections = deepcopy(active), {}
        for report in selected.values():
            report["observation_routes"] = deepcopy(report.get("retained_observation_routes", {}))
    order = {name: number for number, name in enumerate(ADJUSTMENTS)}
    for name, report in selected.items():
        report["correlated"]["segments"] = sorted(
            [*report["correlated"]["segments"], *(segment for segment, _, _ in frozen[name])],
            key=lambda segment: (order[segment["adjustment"]], segment["row_start"]),
        )
    return selected, corrections


def _continuous_adjustments(pid, complete, dated, authorities, values, equal, inputs, own, instants):
    """Prefer positive adjustment continuity over an otherwise direct WI route.

    The sourced same-security assertion and accepted later dividend are both
    required. Matching non-dividend observations isolate adjustment coverage;
    a broader history or a later request route alone supplies no such evidence.
    """
    preferred = set()
    pairs = (
        ("dividend", "raw"),
        ("split,dividend", "split"),
        ("dividend,spin-off", "spin-off"),
        ("all", "split,spin-off"),
    )
    if not all(equal[view] for view in ("raw", "split", "spin-off", "split,spin-off")):
        return preferred
    for name, proof in authorities.items():
        temporary = proof["temporary_source"]
        if name not in complete or temporary not in dated or name != proof["continuing_source"]:
            continue
        through = min(
            min(
                inputs[source].source["latest_request"]["asof"],
                inputs[source].source["latest_request"]["end_exclusive"][:10],
            )
            for source in (name, temporary)
        )
        route = own[pid].get(name)
        observed = _instant(route[0], instants).astimezone(NEW_YORK).date().isoformat() if route else None
        if not any(
            proof["to"] <= day < through and observed is not None and day <= observed
            for day in proof["post_market_dividends"].values()
        ):
            continue
        if all(values[temporary][adjusted] == values[temporary][base] for adjusted, base in pairs) and all(
            values[name][adjusted] != values[temporary][adjusted] for adjusted, _base in pairs
        ):
            preferred.add(name)
    return preferred


def _select_new_observations(
    inputs, reports, *, row_cache=None, source_occupants=None, instants=None, equality_outcomes=None
):
    """Return selected reports and only the mapping corrections they require.

    Inputs are the producer's already decoded, locked group. This function does
    no storage discovery, authentication or acquisition. Consumers later use the
    published row choices without repeating any of this work.
    """
    sole_source = len(reports) == 1
    if sole_source and _single_alias_bundle(inputs, reports):
        return _single_source(inputs, reports)

    ownership, rows, own = {}, {}, defaultdict(dict)
    row_cache = {} if row_cache is None else row_cache
    instants = {} if instants is None else instants
    candidates = defaultdict(dict)
    labels = defaultdict(set)
    market_sources = defaultdict(set)
    adjustment_sources = defaultdict(dict)
    for symbol, report in reports.items():
        for pid, route in report.get("retained_observation_routes", {}).items():
            own[pid][symbol] = route
        view = inputs[symbol]
        rows[symbol] = view.views
        ownership[symbol] = {}
        for adjustment in ADJUSTMENTS:
            owners = {}
            for segment in report["correlated"]["segments"]:
                if segment["adjustment"] != adjustment:
                    continue
                pid = segment["security_identity"]["security_id"]
                for index in range(segment["row_start"] - 1, segment["row_end"]):
                    key = id(view.views[adjustment]), index
                    if key not in row_cache:
                        row_cache[key] = view.views[adjustment][index]
                    row = row_cache[key]
                    owners[index] = pid
                    instant = _instant(row[0], instants)
                    labels[(pid, instant)].update(
                        segment.get("observation_tickers") or [segment["historical_symbol"]]
                    )
                    candidates[(pid, instant)].setdefault(symbol, {})[adjustment] = row
                    authority = segment.get("observation_authority") or {}
                    if (
                        authority.get("market") == "when_issued"
                        and authority.get("direct_market_source") is True
                    ):
                        market_sources[(pid, instant)].add(symbol)
                    if authority.get("continuing_adjustments"):
                        adjustment_sources[(pid, instant)][symbol] = authority["continuing_adjustments"]
                if not sole_source and adjustment == "raw" and segment["historical_symbol"] == symbol:
                    last = view.views[adjustment][segment["row_end"] - 1][0]
                    if symbol not in own[pid] or _instant(last, instants) > _instant(
                        own[pid][symbol][0], instants
                    ):
                        own[pid][symbol] = (
                            last,
                            _own_interval(
                                report,
                                segment,
                                last,
                                day=_instant(last, instants).astimezone(NEW_YORK).date().isoformat(),
                            ),
                        )
            ownership[symbol][adjustment] = owners

    choices, corrections, contested, incomplete = {}, {}, set(), set()
    for (pid, instant), sources in candidates.items():
        if len(sources) == 1:
            name, bundle = next(iter(sources.items()))
            choices[(pid, instant)] = name
            if set(bundle) != set(ADJUSTMENTS):
                # This source's actual owned observations remain usable as a
                # partial capability. Missing views are neither a completed
                # bundle nor competing numerical authority. Never compose two
                # complementary partial sources into a false complete bundle.
                incomplete.add((pid, instant))
            continue
        values = {name: {view: row[1:] for view, row in bundle.items()} for name, bundle in sources.items()}
        equal = {
            view: len({bundle[view] for bundle in values.values() if view in bundle}) <= 1
            for view in ADJUSTMENTS
        }
        if not all(equal.values()):
            contested.add((pid, instant))
        complete = {name: views for name, views in sources.items() if set(views) == set(ADJUSTMENTS)}
        dated = market_sources[(pid, instant)] & complete.keys()
        adjusted = _continuous_adjustments(
            pid, complete, dated, adjustment_sources[(pid, instant)], values, equal, inputs, own, instants
        )
        if len(adjusted) == 1:
            choices[(pid, instant)] = next(iter(adjusted))
            continue
        if len(dated) == 1:
            choices[(pid, instant)] = next(iter(dated))
            continue
        if dated:
            complete = {name: complete[name] for name in dated}
        # The most recent independently observed own label identifies the
        # continuing entity's request route, rather than a price/count maximum.
        # Never let a partial eight-view source displace a complete bundle.
        ranks = {name: _instant(own[pid][name][0], instants) if name in own[pid] else None for name in complete}
        latest = max((rank for rank in ranks.values() if rank is not None), default=None)
        preferred = [name for name, rank in ranks.items() if rank == latest and rank]
        if len(preferred) != 1:
            # Exact bundles need no authority contest. A lexical tie is safe only
            # after complete numerical equality; genuine ambiguity stays explicit.
            bundles = {tuple(values[name][a] for a in ADJUSTMENTS) for name in complete}
            choices[(pid, instant)] = min(complete) if len(bundles) == 1 else None
            continue
        winner = preferred[0]
        choices[(pid, instant)] = winner

    if equality_outcomes is not None:
        equality_outcomes.update(
            {
                key: True
                for key, sources in candidates.items()
                if len(sources) > 1
                and key not in contested
                and choices.get(key) is not None
                and key not in incomplete
            }
        )

    # DO NOT REMOVE WITHOUT USER AUTH!#
    # Repair only a selected source involved in an actual numerical disagreement.
    # Retirement alone does not invalidate trusted retained history. Equal bundles
    # and disjoint histories need no network work; raw equality alone is weaker
    # and cannot conceal a disagreement in any of the seven adjusted views.
    selected_routes = {
        (pid, winner)
        for (pid, instant), winner in choices.items()
        if winner is not None and (pid, instant) not in incomplete
    }
    correction_routes = {
        (pid, choices[(pid, instant)])
        for pid, instant in contested
        if choices[(pid, instant)] is not None and choices[(pid, instant)] not in market_sources[(pid, instant)]
    }
    for pid, winner in sorted(correction_routes):
        occupants = (
            source_occupants[winner] if source_occupants is not None else set(ownership[winner]["raw"].values())
        )
        correction = _mapping_correction(winner, pid, inputs, own, occupants, instants=instants)
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
                instant = _instant(values[index][0], instants)
                winner = choices[(pid, instant)]
                tickers = sorted(labels[(pid, instant)])
                state = (
                    "conflict"
                    if winner is None
                    else "incomplete"
                    if (pid, instant) in incomplete
                    else "selected"
                    if winner == symbol
                    else "superseded"
                )
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
        report["observation_routes"] = {pid: routes[symbol] for pid, routes in own.items() if symbol in routes}
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
