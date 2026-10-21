"""Select completed serving prefixes using producer-owned change decisions."""

from libs.alpaca_pricing_download.contract import ADJUSTMENTS

from .contract import policy


def validate_history_validation(group):
    """Validate the serving producer's new checkpoint before selecting it."""
    completed = group["history_validation"]
    if completed["inputs"].keys() != group["references"]["pricing"].keys():
        raise ValueError("history checkpoint differs from selected inputs")
    for symbol, source in completed["inputs"].items():
        if source["pricing_generation"] != group["references"]["pricing"][symbol] or set(source["views"]) != set(
            ADJUSTMENTS
        ):
            raise ValueError("invalid history checkpoint generations or views")
        for view in source["views"].values():
            if view["rows"] != sum(run["rows"] for run in view["ownership"]) or any(
                run["rows"] < 1 or len(run["owner"]) != 2 for run in view["ownership"]
            ):
                raise ValueError("invalid published ownership checkpoint")
    decisions = {record["public_id"] for record in group["decisions"]}
    seen = set()
    for record in completed["prefix_records"]:
        public_id = record["public_id"]
        if (
            public_id not in decisions
            or public_id in seen
            or record["first_session"] > record["last_session"]
            or record["validated_through"] < record["last_session"]
            or record["coverage"]["session_count"] < 1
        ):
            raise ValueError("invalid completed history prefix")
        missing = record["missing_sessions"]
        if (
            missing != sorted(set(missing))
            or any(not record["first_session"] < day < record["last_session"] for day in missing)
            or not set(record["coverage"]["explained_missing_sessions"]) <= set(missing)
            or not set(record["source_sessions_in_gaps"]) <= set(missing)
            or record["last_raw_bar"]["session"] != record["last_session"]
            or not record["last_raw_bar"]["tickers"]
            or any(
                not record["first_session"] <= bar["session"] <= record["last_session"] or not bar["tickers"]
                for bar in record["gap_bars"]
            )
        ):
            raise ValueError("invalid completed continuity evidence")
        seen.add(public_id)
        coverage = completed["identities"][public_id]
        if set(coverage) != set(ADJUSTMENTS):
            raise ValueError("incomplete history coverage checkpoint")
        for order in coverage.values():
            sources = set(order["subsets"])
            if (
                not sources <= completed["inputs"].keys()
                or any(
                    name not in subsets or not set(subsets) <= sources
                    for name, subsets in order["subsets"].items()
                )
                or any(len(pair) != 2 or not set(pair) <= sources for pair in order["overlaps"])
            ):
                raise ValueError("invalid source coverage order")


def _identity_prefix(runs, count, public_id):
    """Project this identity's published row positions and labels, not its neighbours."""
    selected, offset = [], 0
    for run in runs:
        size = min(count, run["rows"])
        if size and run["owner"][0] == public_id:
            selected.append((offset, offset + size, run["owner"][1]))
        offset += size
        count -= size
        if not count:
            break
    return selected


def select_prefixes(group, previous, inputs, offsets):
    prefixes = {}
    for prior_group in previous:
        if prior_group["policy"] != policy():
            continue
        completed = prior_group["history_validation"]
        for record in completed["prefix_records"]:
            prefix = _select_identity_prefix(group, completed, record, inputs, offsets)
            if prefix is not None:
                prefixes[record["public_id"]] = prefix
    return prefixes


def _select_identity_prefix(group, completed, record, inputs, offsets):
    public_id = record["public_id"]
    coverage = completed["identities"][public_id]
    prior_inputs = completed["inputs"]
    contributing = {symbol for view in coverage.values() for symbol in view["subsets"]}
    if not contributing <= inputs.keys():
        return None
    rows = {}
    for symbol in contributing:
        prior = prior_inputs[symbol]
        # Numeric and session-compatibility outcomes cover every selected view,
        # including adjusted observations beyond raw's last session. A change
        # within that span invalidates this identity's completed facts together.
        if not group.preserves_pricing_prefix(symbol, prior, through=record["validated_through"]):
            return None
        for adjustment, old in prior["views"].items():
            current = inputs[symbol]["views"][adjustment]
            if _identity_prefix(current["ownership"], old["rows"], public_id) != _identity_prefix(
                old["ownership"], old["rows"], public_id
            ):
                return None
        rows[symbol] = {adjustment: view["rows"] for adjustment, view in prior["views"].items()}
    # Gap source-presence facts concern the complete group, independently of the
    # identity's numeric history. Refresh those facts only if their inputs changed.
    missing = record["missing_sessions"]
    refresh_gap_sources = bool(missing) and (
        inputs.keys() != prior_inputs.keys()
        or any(
            not group.preserves_pricing_prefix(symbol, prior, through=max(missing))
            for symbol, prior in prior_inputs.items()
        )
    )
    return {
        "rows": rows,
        "offsets": offsets,
        "coverage": coverage,
        "last_session": record["validated_through"],
        "record": record,
        "refresh_gap_sources": refresh_gap_sources,
    }


def extend_ticker_ranges(previous, suffix):
    ranges = [dict(item) for item in previous["ticker_ranges"]]
    if not suffix:
        return ranges
    first = min(item["first_session"] for item in suffix)
    for item in suffix:
        earlier = next(
            (
                old
                for old in ranges
                if old["ticker"] == item["ticker"]
                and old["last_session"] == previous["last_session"]
                and item["first_session"] == first
            ),
            None,
        )
        if earlier is None:
            ranges.append(item)
        else:
            earlier["last_session"] = item["last_session"]
    return sorted(ranges, key=lambda item: (item["ticker"], item["first_session"]))
