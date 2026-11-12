"""Select completed serving prefixes using producer-owned change decisions."""

from copy import deepcopy

from alpaca_end_of_day._history_coverage import coverage_sources, extend_coverage
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.common.debug_trace import event

from .contract import policy


def validate_history_validation(group):
    """Validate the serving producer's new checkpoint before selecting it."""
    completed = group["history_validation"]
    if completed.get("revision_prefix_version", 1) != 1:
        raise ValueError("unsupported serving revision prefix")
    choices = completed.get("observation_choice_handoffs")
    if choices is not None and (
        not isinstance(choices, dict)
        or type(choices.get("version")) is not int
        or choices["version"] != 1
        or not isinstance(choices.get("sources"), dict)
        or not choices["sources"].keys() <= completed["inputs"].keys()
        or any(
            value.get("receipt_generation") != group["references"]["lineage"][name]
            or value.get("pricing_generation") != group["references"]["pricing"][name]
            for name, value in choices["sources"].items()
        )
    ):
        raise ValueError("invalid serving numerical-choice capability binding")
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
        dates = record.get("session_dates")
        if (
            record.get("session_projection_version") not in (None, 1)
            or record.get("session_projection_version") == 1
            and dates is None
        ):
            raise ValueError("missing or unsupported serving date projection")
        if dates is not None and (
            dates != sorted(set(dates))
            or len(dates) != record["coverage"]["session_count"]
            or not dates
            or dates[0] != record["first_session"]
            or dates[-1] != record["last_session"]
        ):
            raise ValueError("invalid serving date projection")
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
            sources = coverage_sources(order)
            if not sources <= completed["inputs"].keys():
                raise ValueError("invalid contributing-source projection")
            if order.get("version") == 2:
                if set(order) != {"version", "sources"} or order["sources"] != sorted(sources):
                    raise ValueError("invalid contributing-source projection")
            elif any(
                name not in subsets or not set(subsets) <= sources for name, subsets in order["subsets"].items()
            ) or any(len(pair) != 2 or not set(pair) <= sources for pair in order["overlaps"]):
                raise ValueError("invalid legacy source coverage order")


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


def select_prefixes(group, previous, inputs, offsets, *, rebuild_projection=False):
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
            if (
                record.get("session_projection_version") not in (None, 1)
                or record.get("session_projection_version") == 1
                and "session_dates" not in record
                and not rebuild_projection
            ):
                raise ValueError(
                    "missing or unsupported serving date projection; explicit catalogue rebuild required"
                )
            prefix = _select_identity_prefix(group, completed, record, inputs, offsets)
            if prefix is None:
                prefix = _select_revision_prefix(group, completed, record, inputs, offsets)
            if prefix is not None:
                prefixes[record["public_id"]] = prefix
    from .source_revision import select_revisions

    prefixes.update(select_revisions(group, previous, inputs, offsets, prefixes))
    prefixes.update(_dated_market_prefixes(group, previous, inputs, offsets, prefixes))
    prefixes.update(_relabelled_whole_donors(group, previous, inputs, offsets, prefixes))
    prefixes.update(compose_membership_rebind(group, previous, inputs, offsets, completed=prefixes))
    return prefixes


def _dated_market_prefixes(group, previous, inputs, offsets, completed):
    """Consume completed label-only producer work without rechecking numbers."""
    handoffs = group.dated_market_projections() if hasattr(group, "dated_market_projections") else {}
    result = {}
    for prior in previous:
        if prior["policy"] != policy():
            continue
        history = prior["history_validation"]
        for record in history["prefix_records"]:
            pid = record["public_id"]
            if pid in completed or pid in result:
                continue
            sources = {
                source
                for coverage in history["identities"][pid].values()
                for source in coverage_sources(coverage)
            }
            if not sources or not sources <= handoffs.keys() or inputs.keys() != history["inputs"].keys():
                continue
            if any(
                inputs[source]["pricing_generation"] != old["pricing_generation"]
                for source, old in history["inputs"].items()
            ):
                continue
            if any(
                handoffs[source].get("version") != 1
                or handoffs[source].get("selection_unchanged") is not True
                or handoffs[source].get("previous_receipt_generation") != prior["references"]["lineage"][source]
                or handoffs[source].get("pricing_generation") != inputs[source]["pricing_generation"]
                for source in sources
            ):
                continue
            episodes = [handoffs[source].get("identities", {}).get(pid) for source in sorted(sources)]
            if not episodes[0] or any(value != episodes[0] for value in episodes):
                continue
            dates = record.get("session_dates")
            if dates is None:
                if record.get("session_projection_version") is not None:
                    raise ValueError(
                        "dated label projection lacks declared session dates; owning recovery required"
                    )
                # An unmarked legacy checkpoint accepted the same exact raw
                # positions above. Assemble its dates once; numerical outcomes
                # and all-view compatibility remain authoritative.
                dates = group.serving_session_dates(pid)
                if (
                    not dates
                    or len(dates) != record["coverage"]["session_count"]
                    or dates[0] != record["first_session"]
                    or dates[-1] != record["last_session"]
                ):
                    raise ValueError("dated label timestamp assembly differs from accepted raw positions")

            projected = project_dated_labels(record, episodes[0], dates)
            rows = {}
            for source in sources:
                rows[source] = {}
                for adjustment, old in history["inputs"][source]["views"].items():
                    selected = _identity_prefix(old["ownership"], old["rows"], pid)
                    rows[source][adjustment] = selected[-1][1] if selected else 0
            result[pid] = {
                "rows": rows,
                "offsets": offsets,
                "coverage": history["identities"][pid],
                "last_session": record["validated_through"],
                "record": projected,
                "refresh_gap_sources": False,
            }
    return result


def project_dated_labels(record, episodes, dates):
    """Construct public label ranges once from accepted session dates."""
    from .policy import _ticker_ranges

    selected = {}

    def labels(day):
        if day not in selected:
            matches = {
                episode["symbol"]
                for episode in episodes
                if episode["start_inclusive"] <= day
                and (episode["end_exclusive"] is None or day < episode["end_exclusive"])
            }
            selected[day] = sorted(
                matches
                or {
                    item["ticker"]
                    for item in record["ticker_ranges"]
                    if item["first_session"] <= day <= item["last_session"]
                }
            )
        return selected[day]

    projected = deepcopy(record)
    projected.update(session_dates=dates, session_projection_version=1)
    projected["ticker_ranges"] = _ticker_ranges([{"session": day, "tickers": labels(day)} for day in dates])
    for key in ("last_raw_bar", "before_last_raw_bar"):
        if projected.get(key):
            projected[key]["tickers"] = labels(projected[key]["session"])
    for bar in projected["gap_bars"]:
        bar["tickers"] = labels(bar["session"])
    return projected


def _relabelled_whole_donors(group, previous, inputs, offsets, completed):
    """Retain a complete numerical selection whose owner or labels changed.

    A whole, sole-source donor can supply exact dates for every selected row.
    Losing sources contribute no approval. Partial or unaligned donors require
    their owning projection and are left to the explicit bounded boundary.
    """
    from .policy import _ticker_ranges

    result = {}
    for prior in previous:
        if prior["policy"] != policy():
            continue
        history = prior["history_validation"]
        for record in history["prefix_records"]:
            old_id = record["public_id"]
            sources = {
                source
                for coverage in history["identities"][old_id].values()
                for source in coverage_sources(coverage)
            }
            if (
                len(sources) != 1
                or not record.get("session_dates")
                or "incompatible_sessions" in record["price_reasons"]
            ):
                continue
            source = next(iter(sources))
            if source not in inputs:
                continue
            old_input = history["inputs"][source]
            if not group.preserves_pricing_prefix(source, old_input, through=record["validated_through"]):
                continue
            ids, labels, valid = set(), {day: set() for day in record["session_dates"]}, True
            for adjustment in ADJUSTMENTS:
                old_view = old_input["views"][adjustment]
                positions = [
                    index
                    for first, last, _ in _identity_prefix(old_view["ownership"], old_view["rows"], old_id)
                    for index in range(first, last)
                ]
                if len(positions) != len(record["session_dates"]):
                    valid = False
                    break
                for day, index in zip(record["session_dates"], positions):
                    owner = _owner_at(inputs[source]["views"][adjustment]["ownership"], index)
                    if owner is None or owner[0] is None:
                        valid = False
                        break
                    ids.add(owner[0])
                    labels[day].update(owner[1])
            if not valid or len(ids) != 1:
                continue
            new_id = next(iter(ids))
            if new_id in completed or new_id in result:
                continue
            # Exact exhaustion excludes a newly selected row or another source
            # hidden behind equal counts. Labels are metadata, positions bind
            # the accepted numerical values independently of those labels.
            for name, selected in inputs.items():
                for adjustment, view in selected["views"].items():
                    actual = [
                        (first, last)
                        for first, last, _ in _identity_prefix(view["ownership"], view["rows"], new_id)
                    ]
                    expected = (
                        [
                            (first, last)
                            for first, last, _ in _identity_prefix(
                                old_input["views"][adjustment]["ownership"],
                                old_input["views"][adjustment]["rows"],
                                old_id,
                            )
                        ]
                        if name == source
                        else []
                    )

                    def expanded(runs):
                        return {index for first, last in runs for index in range(first, last)}

                    if expanded(actual) != expanded(expected):
                        valid = False
            if not valid:
                continue
            projected = deepcopy(record)
            projected["public_id"] = new_id
            projected["ticker_ranges"] = _ticker_ranges(
                [{"session": day, "tickers": sorted(values)} for day, values in labels.items()]
            )
            for key in ("last_raw_bar", "before_last_raw_bar"):
                if projected.get(key):
                    projected[key]["tickers"] = sorted(labels[projected[key]["session"]])
            for bar in projected["gap_bars"]:
                bar["tickers"] = sorted(labels[bar["session"]])
            result[new_id] = {
                "rows": {source: {adjustment: view["rows"] for adjustment, view in old_input["views"].items()}},
                "offsets": offsets,
                "coverage": history["identities"][old_id],
                "last_session": record["validated_through"],
                "record": projected,
                "refresh_gap_sources": bool(record["missing_sessions"]),
            }
    return result


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
    contributing = {
        symbol for view in completed["identities"][public_id].values() for symbol in coverage_sources(view)
    }
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
    if "session_dates" in shortened:
        shortened["session_dates"] = shortened["session_dates"][:-1]
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
    contributing = {symbol for view in coverage.values() for symbol in coverage_sources(view)}
    if not contributing <= inputs.keys():
        event("serving_prefix", public_id=public_id, accepted=False, reason="contributing_source_removed")
        return None
    rows, first_rows, extends_before = {}, {}, False
    for symbol in contributing:
        prior = prior_inputs[symbol]
        # Numeric and session-compatibility outcomes cover every selected view,
        # including adjusted observations beyond raw's last session. A change
        # within that span invalidates this identity's completed facts together.
        if not group.preserves_pricing_prefix(symbol, prior, through=record["validated_through"]):
            event(
                "serving_prefix",
                public_id=public_id,
                symbol=symbol,
                accepted=False,
                reason="pricing_ancestry_changed",
                through=record["validated_through"],
            )
            return None
        rows[symbol], first_rows[symbol] = {}, {}
        for adjustment, old in prior["views"].items():
            current = inputs[symbol]["views"][adjustment]
            accepted = _identity_prefix(old["ownership"], old["rows"], public_id)
            through = accepted[-1][1] if accepted else 0
            # A source can already contain later rows belonging to a different
            # identity. Reassigning those rows extends this identity's accepted
            # history; it does not revoke its earlier numerical approval. The
            # completed ownership runs give the exact last selected position.
            # Exact old selections and labels inside the accepted span remain
            # fixed. New earlier positions select bounded prepend assembly;
            # the reader rejects dates overlapping the accepted middle.
            first = accepted[0][0] if accepted else 0
            actual = _identity_prefix(current["ownership"], through, public_id)
            retained = [(max(left, first), right, labels) for left, right, labels in actual if right > first]
            if retained != accepted:
                event(
                    "serving_prefix",
                    public_id=public_id,
                    symbol=symbol,
                    adjustment=adjustment,
                    accepted=False,
                    reason="ownership_or_labels_changed",
                )
                return None
            rows[symbol][adjustment] = through
            first_rows[symbol][adjustment] = first
            extends_before |= any(left < first for left, _, _ in actual)
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
    event(
        "serving_prefix",
        public_id=public_id,
        accepted=True,
        reason="accepted_numeric_history",
        through=record["validated_through"],
    )
    return {
        "rows": rows,
        "offsets": offsets,
        "coverage": coverage,
        "last_session": record["validated_through"],
        "record": record,
        "refresh_gap_sources": refresh_gap_sources,
        **(
            {"accepted_span": (record["first_session"], record["last_session"]), "first_rows": first_rows}
            if extends_before
            else {}
        ),
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


def compose_membership_rebind(group, previous, inputs, offsets, *, completed=()):
    """Compose exhaustive unchanged selections under already-published new IDs.

    Donors are completed numerical authorities. Match exact row selections and
    generations, then assemble their dates/outcomes. Only new seams select a
    calendar/gap calculation. No OHLC or membership hash is recomputed here.
    """
    from datetime import date, timedelta

    from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

    handoffs = group.membership_rebinds() if hasattr(group, "membership_rebinds") else {}
    if not handoffs:
        return {}
    transfers = {}
    for source, handoff in handoffs.items():
        for old, identity in handoff["identities"].items():
            transfers.setdefault(identity["security_id"], {}).setdefault(old, {})[source] = handoff
    result = {}
    for new_id, old_ids in transfers.items():
        if new_id in completed:
            continue
        donors, rows, dated_sources = [], {}, {name: set() for name in ADJUSTMENTS}
        donor_selections = []
        valid = True
        for old_id, source_handoffs in old_ids.items():
            matches = [
                (prior, prior["history_validation"], record)
                for prior in previous
                if prior["policy"] == policy()
                for record in prior["history_validation"]["prefix_records"]
                if record["public_id"] == old_id
            ]
            if len(matches) != 1:
                raise ValueError(
                    "membership transfer lacks one exact numerical donor; explicit serving projection required"
                )
            prior, donor_checkpoint, record = matches[0]
            if record.get("session_dates") is None and record.get("session_projection_version") is not None:
                raise ValueError(
                    "membership transfer has incomplete declared date projection; explicit serving projection required"
                )
            selections = []
            contributing = {
                symbol
                for coverage in donor_checkpoint["identities"][old_id].values()
                for symbol in coverage_sources(coverage)
            }
            if contributing != source_handoffs.keys():
                raise ValueError("membership transfer does not exhaust its numerical donor sources")
            for source in contributing:
                handoff = source_handoffs[source]
                old_input = donor_checkpoint["inputs"][source]
                if (
                    prior["references"]["lineage"].get(source) != handoff["previous_receipt_generation"]
                    or old_input["pricing_generation"] != handoff["previous_pricing_generation"]
                    or not group.preserves_pricing_prefix(source, old_input, through=record["validated_through"])
                ):
                    valid = False
                    break
                rows.setdefault(source, {})
                for adjustment, old_view in old_input["views"].items():
                    selected_runs = [
                        run
                        for run in handoff["runs"]
                        if run["old_public_id"] == old_id and run["adjustment"] == adjustment
                    ]
                    declared = sorted(
                        (run["row_start"] - 1, run["row_end"], run.get("old_labels", run["labels"]))
                        for run in selected_runs
                    )
                    accepted = _identity_prefix(old_view["ownership"], old_view["rows"], old_id)
                    # Adjacent segments may be split at month boundaries. Their
                    # exact positions/labels still form one accepted run.
                    merged = []
                    for first, last, labels in declared:
                        if merged and first < merged[-1][1]:
                            raise ValueError("duplicate/overlapping membership transfer rows")
                        if merged and first == merged[-1][1] and labels == merged[-1][2]:
                            merged[-1] = (merged[-1][0], last, labels)
                        else:
                            merged.append((first, last, labels))
                    if merged != accepted:
                        retained = [(run["from"], run["to"]) for run in selected_runs]
                        raise ValueError(
                            f"bounded serving projection pending: source={source}, adjustment={adjustment}, "
                            f"old_public_id={old_id}, new_public_id={new_id}, retained_date_scopes={retained}; "
                            "missing applicable numerical/date outcome for changed donor subrange; "
                            "accepted donor retained, arbitrary-run recovery is unavailable"
                        )
                    rows[source][adjustment] = max(rows[source].get(adjustment, 0), old_view["rows"])
                    selections.append((source, adjustment, selected_runs, accepted))
            donors.append(record)
            donor_selections.append(selections)
        if not valid:
            raise ValueError("membership transfer source generation changed; selected serving recovery required")
        first_rows, extends_before = {}, False
        for source, views in rows.items():
            first_rows[source] = {}
            for adjustment, through in views.items():
                actual = _identity_prefix(inputs[source]["views"][adjustment]["ownership"], through, new_id)
                # Current labels and positions are verified against the exact
                # producer transfer, not inferred from session-count equality.
                declared = sorted(
                    (run["row_start"] - 1, run["row_end"], run["labels"])
                    for handoff in handoffs.values()
                    if handoff["source"] == source
                    for run in handoff["runs"]
                    if run["new_public_id"] == new_id and run["adjustment"] == adjustment
                )
                merged = []
                for first, last, labels in declared:
                    if merged and first == merged[-1][1] and labels == merged[-1][2]:
                        merged[-1] = (merged[-1][0], last, labels)
                    else:
                        merged.append((first, last, labels))
                first = min((run[0] for run in merged), default=through)
                first_rows[source][adjustment] = first
                retained_actual = [
                    (max(left, first), right, labels) for left, right, labels in actual if right > first
                ]
                if merged != retained_actual:
                    raise ValueError("new membership transfer changed row/label selection")
                extends_before |= any(left < first for left, _, _ in actual)
        ordered = sorted(donors, key=lambda record: record["first_session"])
        if any(left["last_session"] >= right["first_session"] for left, right in zip(ordered, ordered[1:])):
            raise ValueError("membership donors overlap; bounded observation-selection reconciliation required")
        # Only an unmarked legacy whole donor lacks this compact projection.
        # All generation and row-selection gates above precede data assembly.
        # Its numerical outcomes remain accepted; this constructs dates only.
        projected = []
        for record, selections in zip(donors, donor_selections):
            dates = record.get("session_dates")
            if dates is None:
                raw_runs = [run for _, adjustment, runs, _ in selections if adjustment == "raw" for run in runs]
                dates = group._serving_transferred_session_dates(raw_runs, offsets)
                if (
                    not dates
                    or len(dates) != record["coverage"]["session_count"]
                    or dates[0] != record["first_session"]
                    or dates[-1] != record["last_session"]
                ):
                    raise ValueError("legacy donor date assembly differs from its accepted raw selection")
                record = {**record, "session_dates": dates, "session_projection_version": 1}
            for source, adjustment, runs, accepted in selections:
                selected_dates = {day for run in runs for day in dates if run["from"] <= day <= run["to"]}
                if len(selected_dates) != sum(last - first for first, last, _ in accepted):
                    if "incompatible_sessions" not in record["price_reasons"]:
                        raise ValueError("membership donor lacks exact selected date projection")
                dated_sources[adjustment].add(source)
            relabelled = any(
                "old_labels" in run and run["old_labels"] != run["labels"]
                for _source, _view, runs, _accepted in selections
                for run in runs
            )
            if relabelled:
                from .policy import _ticker_ranges

                labels = {day: set() for day in dates}
                for _source, adjustment, runs, _accepted in selections:
                    if adjustment != "raw":
                        continue
                    for run in runs:
                        for day in dates:
                            if run["from"] <= day <= run["to"]:
                                labels[day].update(run["labels"])
                if any(not values for values in labels.values()):
                    raise ValueError("dated membership transfer lacks a complete accepted label projection")
                record = deepcopy(record)
                record["ticker_ranges"] = _ticker_ranges(
                    [{"session": day, "tickers": sorted(values)} for day, values in labels.items()]
                )
                for key in ("last_raw_bar", "before_last_raw_bar"):
                    if record.get(key):
                        record[key]["tickers"] = sorted(labels[record[key]["session"]])
                for bar in record["gap_bars"]:
                    if bar["session"] in labels:
                        bar["tickers"] = sorted(labels[bar["session"]])
            projected.append(record)
        donors = sorted(projected, key=lambda record: record["first_session"])
        all_dates = [day for donor in donors for day in donor["session_dates"]]
        missing = {day for donor in donors for day in donor["missing_sessions"]}
        gap_bars = [bar for donor in donors for bar in donor["gap_bars"]]
        source_sessions = {day for donor in donors for day in donor["source_sessions_in_gaps"]}
        for left, right in zip(donors, donors[1:]):
            start = (date.fromisoformat(left["last_session"]) + timedelta(days=1)).isoformat()
            seam = (
                expected_market_session_coverage(start, right["first_session"]).session_dates
                if start < right["first_session"]
                else ()
            )
            if seam:
                missing.update(seam)
                source_sessions.update(group.source_sessions(start=seam[0], end=seam[-1]) & set(seam))
                labels = sorted(
                    {
                        run["ticker"]
                        for run in right["ticker_ranges"]
                        if run["first_session"] == right["first_session"]
                    }
                )
                gap_bars.extend((left["last_raw_bar"], {"session": right["first_session"], "tickers": labels}))
        final = donors[-1]
        prior_raw = final.get("before_last_raw_bar")
        if len(final["session_dates"]) == 1 and len(donors) > 1:
            prior_raw = donors[-2]["last_raw_bar"]
        reasons = sorted({reason for donor in donors for reason in donor["price_reasons"]})
        record = {
            **deepcopy(final),
            "public_id": new_id,
            "first_session": donors[0]["first_session"],
            "validated_through": max(donor["validated_through"] for donor in donors),
            "session_dates": all_dates,
            "session_projection_version": 1,
            "ticker_ranges": sorted(
                [deepcopy(run) for donor in donors for run in donor["ticker_ranges"]],
                key=lambda run: (run["ticker"], run["first_session"]),
            ),
            "coverage": {
                **final["coverage"],
                "session_count": len(all_dates),
                "explained_missing_sessions": sorted(
                    {day for donor in donors for day in donor["coverage"]["explained_missing_sessions"]}
                ),
            },
            "price_reasons": reasons,
            "before_last_raw_bar": prior_raw,
            "before_last_price_reasons": sorted(
                {reason for donor in donors[:-1] for reason in donor["price_reasons"]}
                | set(final.get("before_last_price_reasons") or ())
            )
            if final.get("before_last_price_reasons") is not None
            else None,
            "missing_sessions": sorted(missing),
            "gap_bars": sorted(
                {bar["session"]: bar for bar in gap_bars}.values(), key=lambda bar: bar["session"]
            ),
            "source_sessions_in_gaps": sorted(source_sessions),
        }
        # Completed disjoint donor scopes establish membership independently of
        # numerical alignment. No coverage ranking or timestamp-set comparison
        # selects a source at this downstream boundary.
        coverage = {adjustment: extend_coverage(sources) for adjustment, sources in dated_sources.items()}
        result[new_id] = {
            "rows": rows,
            "offsets": offsets,
            "coverage": coverage,
            "last_session": record["validated_through"],
            "record": record,
            "refresh_gap_sources": False,
            **(
                {"accepted_span": (record["first_session"], record["last_session"]), "first_rows": first_rows}
                if extends_before
                else {}
            ),
        }
    return result
