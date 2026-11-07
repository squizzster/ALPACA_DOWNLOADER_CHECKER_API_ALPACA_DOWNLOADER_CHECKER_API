"""Select completed serving prefixes using producer-owned change decisions."""

from copy import deepcopy

from libs.alpaca_pricing_download.contract import ADJUSTMENTS

from .contract import policy


def validate_history_validation(group):
    """Validate the serving producer's new checkpoint before selecting it."""
    completed = group["history_validation"]
    if completed.get("revision_prefix_version", 1) != 1:
        raise ValueError("unsupported serving revision prefix")
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
        preceding = record.get("before_last_raw_bar")
        if (
            completed.get("revision_prefix_version") == 1
            and record["coverage"]["session_count"] >= 2
            and preceding is None
        ):
            raise ValueError("completed serving revision checkpoint lacks its preceding raw boundary")
        if preceding is not None and (
            not record["first_session"] <= preceding["session"] < record["last_session"]
            or not preceding["tickers"]
        ):
            raise ValueError("invalid preceding-session serving boundary")
        earlier_reasons = record.get("before_last_price_reasons")
        if earlier_reasons is not None and (
            not isinstance(earlier_reasons, list) or earlier_reasons != sorted(set(earlier_reasons))
        ):
            raise ValueError("invalid preceding-session serving reasons")
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
    # These bodies came from the accepted serving selector. Its producer already
    # validated the checkpoint. Compare current input guarantees in memory; DO
    # NOT revalidate old records or scan old prices to earn permission to reuse.
    # Failed ancestry/ownership gates leave that identity to ordinary evaluation.
    prefixes = {}
    for prior_group in previous:
        if prior_group["policy"] == policy():
            completed = prior_group["history_validation"]
        else:
            completed = _unchanged_history_checkpoint(prior_group, inputs)
            if completed is None:
                continue
        for record in completed["prefix_records"]:
            prefix = _select_identity_prefix(group, completed, record, inputs, offsets)
            if prefix is None:
                prefix = _select_revision_prefix(group, completed, record, inputs, offsets)
            if prefix is not None:
                prefixes[record["public_id"]] = prefix
    return prefixes


def _owner_at(runs, position):
    for run in runs:
        if position < run["rows"]:
            return run["owner"]
        position -= run["rows"]
    return None


def _legacy_previous_raw(completed, record, contributing, revisions):
    """Project a legacy disjoint seam only when accepted facts identify it.

    A lone final source supplies the preceding identity row when every other
    contributing view ends before that date. Accepted cross-view session
    compatibility and both penultimate owners connect its all predecessor to
    raw. With competing raw sources the checkpoint lacks the exact provenance;
    coverage subset relations alone cannot recover a previous winner's labels.
    """
    if len(revisions) != 1 or "incompatible_sessions" in record["price_reasons"]:
        return None
    symbol, revision = next(iter(revisions.items()))
    prior = completed["inputs"][symbol]
    owners = {}
    for name in ("raw", "all"):
        view = prior["views"][name]
        if view["rows"] < 2 or view["last_session"] != record["last_session"]:
            return None
        for offset in (1, 2):
            owner = _owner_at(view["ownership"], view["rows"] - offset)
            if owner is None or owner[0] != record["public_id"]:
                return None
        owners[name] = _owner_at(view["ownership"], view["rows"] - 2)
    day = revision["previous_session"]
    if any(
        view["last_session"] is not None and view["last_session"] >= day
        for other in contributing - {symbol}
        for view in completed["inputs"][other]["views"].values()
    ):
        return None
    return {"session": day, "tickers": owners["raw"][1]}


def _select_revision_prefix(group, completed, record, inputs, offsets):
    """Recheck the final row while retaining completed earlier history facts.

    Unchanged prior ownership and exact timestamp membership retain the accepted
    source-coverage relationships. Read the corrected date from every source
    which contributed it, including an unchanged overlapping source. Earlier
    observations and their subset/overlap decisions remain completed facts.
    """
    public_id = record["public_id"]
    contributing = {symbol for view in completed["identities"][public_id].values() for symbol in view["subsets"]}
    if not contributing or record["coverage"]["session_count"] < 2:
        return None
    if not contributing <= inputs.keys() or not hasattr(group, "latest_pricing_revision"):
        return None
    day = record["last_session"]
    if record["validated_through"] != day:
        return None
    selected_rows, revisions = {}, {}
    for symbol in contributing:
        prior = completed["inputs"][symbol]
        if not group.preserves_pricing_prefix(symbol, prior, through=day):
            revision = group.latest_pricing_revision(symbol, prior)
            if (
                not revision
                or revision["session"] != day
                or (len(contributing) > 1 and not revision.get("preserves_timestamps"))
            ):
                return None
            revisions[symbol] = revision
        selected_rows[symbol] = {}
        for adjustment, old in prior["views"].items():
            if old["last_session"] is not None and old["last_session"] > day:
                return None
            current = inputs[symbol]["views"][adjustment]
            if _identity_prefix(current["ownership"], old["rows"], public_id) != _identity_prefix(
                old["ownership"], old["rows"], public_id
            ):
                return None
            selected_rows[symbol][adjustment] = old["rows"] - int(old["last_session"] == day)
    if not revisions:
        return None
    reasons = record.get("before_last_price_reasons")
    if reasons is None:
        if record["price_reasons"]:
            raise ValueError(
                "legacy serving checkpoint lacks preceding numerical outcomes; explicit catalogue rebuild required"
            )
        reasons = []  # Every earlier row passed the completed numeric checks.
    preceding = record.get("before_last_raw_bar")
    if preceding is None:
        preceding = _legacy_previous_raw(completed, record, contributing, revisions)
        if preceding is None:
            raise ValueError(
                "legacy serving checkpoint lacks preceding raw provenance; explicit catalogue rebuild required"
            )
    through = preceding["session"]
    shortened = deepcopy(record)
    shortened.update(
        last_session=through,
        validated_through=through,
        price_reasons=reasons,
        last_raw_bar=preceding,
    )
    shortened["coverage"]["session_count"] -= 1
    shortened["coverage"]["explained_missing_sessions"] = [
        day for day in shortened["coverage"]["explained_missing_sessions"] if day < through
    ]
    for key in ("missing_sessions", "source_sessions_in_gaps"):
        shortened[key] = [day for day in shortened[key] if day < through]
    shortened["gap_bars"] = [bar for bar in shortened["gap_bars"] if bar["session"] <= through]
    shortened["ticker_ranges"] = [
        {**item, "last_session": min(item["last_session"], through)}
        for item in shortened["ticker_ranges"]
        if item["first_session"] <= through
    ]
    prefix = _select_identity_prefix(group, completed, shortened, inputs, offsets)
    if prefix is not None:
        prefix["rows"] = selected_rows
    return prefix


def _unchanged_history_checkpoint(prior_group, inputs):
    """Translate unchanged earlier numeric checkpoints at their maintenance owner."""
    # DO NOT REMOVE WITHOUT USER AUTH!#
    # Policy 6 changed numerical source selection and made each ownership label
    # a list. Policies 7 and 8 trust producer-owned non-observation dispositions;
    # neither changes selected numeric history. Policy 9 can restrict the public
    # range after these full-history checks; it still validates the same numbers.
    # If the COMPLETE group's published
    # pricing generations and EVERY row/source/owner/label selection are
    # unchanged, the completed numeric and session checks still describe the
    # identical history while eligibility is recalculated from today's receipt.
    # Compare only these already loaded producer handoffs: no payload reads,
    # upstream authentication, or identity calculation belongs in this proof.
    current = policy()
    prior_policy = prior_group["policy"]
    if current["version"] != 9 or prior_policy not in (
        {**current, "version": 5},
        {**current, "version": 6},
        {**current, "version": 7},
        {**current, "version": 8},
    ):
        return None
    completed = prior_group["history_validation"]
    translated = completed["inputs"]
    if prior_policy["version"] == 5:
        translated = {
            symbol: {
                **source,
                "views": {
                    adjustment: {
                        **view,
                        "ownership": [
                            {
                                **run,
                                "owner": [run["owner"][0], [run["owner"][1]] if run["owner"][0] else None],
                            }
                            for run in view["ownership"]
                        ],
                    }
                    for adjustment, view in source["views"].items()
                },
            }
            for symbol, source in completed["inputs"].items()
        }
    # DO NOT REMOVE WITHOUT USER AUTH!#
    # A changed source, generation, row count, owner, label or session defeats
    # migration and retains the ordinary full evaluation. A newly superseded
    # source changes these runs even if its numbers equal the winner's. Existing
    # assembly consumes published conflicts before using any prefix; migration
    # cannot turn a conflict into an eligible history. Metadata is still projected
    # from the current group by evaluate(); only identical completed history
    # facts are reused.
    # These exact transitions deliberately expire at the next policy version.
    return {**completed, "inputs": inputs} if translated == inputs else None


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
