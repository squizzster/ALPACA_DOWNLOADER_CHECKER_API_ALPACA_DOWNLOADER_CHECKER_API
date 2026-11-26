"""A yes/no boundary over accepted lineage inputs, without identity rules."""

from copy import deepcopy

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp
from libs.common.debug_trace import event
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .contract import reusable_decision
from .correction_prefix import correction_from


def request_context(request):
    # The latest HTTP page/window start is not the source's retained start.
    # history_start is the producer's effective request_from handoff.
    return {key: value for key, value in request.items() if key != "start"}


def unchanged_source(source, previous, *, selected=None):
    """Unchanged pricing stays unchanged when only accepted coverage advances."""
    if (
        not previous
        or not reusable_decision(previous)
        or not unchanged_rows(source, previous, selected=selected)
    ):
        return False
    return continuing_request(source, previous)


def source_reuse(source, previous, *, selected, selected_correction):
    """Complete both intrinsic pricing-reuse outcomes from one held binding."""
    if not previous or not reusable_decision(previous):
        return False, False
    if not continuing_request(source, previous):
        event(
            "retained_prefix",
            symbol=source["symbol"],
            accepted=False,
            reason="effective_pricing_context_changed",
        )
        return False, False
    if selected == previous["source_sha256"]:
        return True, True
    prefix = selected_correction is not None
    for change in source.get("history_changes", ()):
        if change["source_sha256"] != previous["source_sha256"]:
            continue
        if change["changed_from"] is None:
            return True, True
        prefix |= change["changed_from"] >= previous["processed_through"][:10]
    event(
        "retained_prefix",
        symbol=source["symbol"],
        accepted=prefix,
        reason="accepted_pricing_ancestry" if prefix else "changed_or_unproven_prefix",
    )
    return False, prefix


def continuing_request(source, previous):
    """Preserve the effective mapping, except its normal coverage-bound advance."""
    before = request_context(previous.get("pricing_request", {}))
    after = request_context({**source["latest_request"], "history_start": source.get("history_start")})
    old_end, new_end = before.pop("end_exclusive", None), after.pop("end_exclusive", None)
    old_asof, new_asof = before.pop("asof", None), after.pop("asof", None)
    if not old_end or not new_end or new_end < old_end:
        return False
    # Normal current-label mappings advance with coverage; historical mappings
    # remain pinned. A genuinely changed mapping still selects reconciliation.
    if old_asof != new_asof and (old_asof != old_end[:10] or new_asof != new_end[:10]):
        return False
    return before == after


def unchanged_rows(source, previous, *, selected=None):
    """Consume pricing's completed all-view equality, independently of requests."""
    return previous is not None and (
        selected == previous["source_sha256"]
        or any(
            change["source_sha256"] == previous["source_sha256"] and change["changed_from"] is None
            for change in source.get("history_changes", ())
        )
    )


def reusable_prefix(source, previous, *, selected=None, refresh=False, selected_correction=...):
    """Accept pricing's unchanged-prefix handoff without replaying lineage rules.

    Pricing owns retained-row equality and the completed lineage receipt owns its
    assignments. Group topology, ambiguity, an empty history and event complexity
    describe that accepted result; none proves its retained prices changed. Actual
    evidence changes and append effects on unresolved decisions are selected later,
    after their producers have supplied the new facts. No ownership rule executes
    at this boundary.
    """

    def selected_prefix(accepted, reason):
        event("retained_prefix", symbol=source["symbol"], accepted=accepted, reason=reason)
        return accepted

    if refresh or not previous or not reusable_decision(previous):
        return selected_prefix(False, "refresh_or_missing_compatible_receipt")
    if not continuing_request(source, previous):
        return selected_prefix(False, "effective_pricing_context_changed")
    accepted = (
        (correction_from(source, previous) if selected_correction is ... else selected_correction) is not None
        or selected == previous["source_sha256"]
        or any(
            change["source_sha256"] == previous["source_sha256"]
            and (change["changed_from"] is None or change["changed_from"] >= previous["processed_through"][:10])
            for change in source.get("history_changes", ())
        )
    )
    return selected_prefix(accepted, "accepted_pricing_ancestry" if accepted else "changed_or_unproven_prefix")


def complete_append(view):
    """Only the new session grid and its accepted seam require validation."""
    summaries = view.previous_assignment_summary
    if not any(view.views.values()):
        return True
    checked = set()
    for adjustment in ADJUSTMENTS:
        rows = view.views[adjustment]
        last = summaries[adjustment]["last_session"]
        if last is None:
            if summaries[adjustment]["rows"] == 0:
                continue  # The producer validates first rows; there is no retained seam.
            return False
        key = id(rows), last
        if key in checked:
            continue
        checked.add(key)
        if getattr(view, "reprojected_sessions", None) is not None:
            # These accepted rows were selected for numerical projection only.
            # Their completed pricing grid is not another seam to authenticate.
            added = rows[len(view.reprojected_sessions[adjustment]) :]
            first, end = view.previous_coverage_end, view.source["latest_request"]["end_exclusive"]
            expected = (
                () if first[:10] == end[:10] else expected_market_session_coverage(first, end).session_dates
            )
            if tuple(timestamp(row[0]).astimezone(NEW_YORK).date().isoformat() for row in added) != expected:
                return False
            continue
        # Completed empty coverage is authority too. A thin or retired source
        # can have a last bar far before its accepted request boundary. Only
        # the newly selected interval needs a grid; reopening the earlier empty
        # span would falsely select complete historical reconstruction.
        first = view.previous_coverage_end
        if first is None:
            return False  # Missing coverage needs owning recovery, never a historical grid audit.
        end = view.source["latest_request"]["end_exclusive"]
        # A corrected last row is work even when coverage has not advanced.
        # Its producer certificate supplies that session; only a nonempty
        # appended interval needs a calendar grid. Retained history stays closed.
        added_sessions = (
            ()
            if view.correction_from and first[:10] == end[:10]
            else expected_market_session_coverage(first, end).session_dates
        )
        sessions = tuple(timestamp(row[0]).astimezone(NEW_YORK).date().isoformat() for row in rows)
        accepted_sessions = (view.correction_from, *added_sessions) if view.correction_from else added_sessions
        if sessions != accepted_sessions:
            return False
    return True


def assignment_summary(previous, boundaries):
    """Consume published assignments when an older receipt lacks their summary.

    This is a projection of accepted results, not another rule execution or
    pricing audit. New receipts carry it so closed assignment bodies stay closed.
    """
    if "assignment_summary" in previous:
        return previous["assignment_summary"]
    result = {}
    for adjustment in ADJUSTMENTS:
        segments = [
            segment
            for kind, document in boundaries.items()
            for item in document["adjustments"]
            if item["adjustment"] == adjustment
            for segment in item["segments"]
            if kind != "current_year" or segment["from"][:7] < previous["anchor_month"]
        ]
        result[adjustment] = {
            "rows": sum(segment["row_count"] for segment in segments),
            "first_timestamp": min((s["first_timestamp"] for s in segments), default=None),
            "last_timestamp": max((s["last_timestamp"] for s in segments), default=None),
            "last_session": max((s["to"] for s in segments), default=None),
        }
    return result


# Changes to native actions, lookups and group membership can change ownership.
# These producer receipts carry discovery, descriptive tape or transport details.
_NON_ASSIGNMENT_FIELDS = frozenset(
    {
        "api_request_count",
        "archive_cusip_action_endpoints",
        "catalogue_observed_at_utc",
        "cusip_action_discovery",
        "historical_trade_tape",
        "listing_exchange_changes",
        "mapped_historical_trade_tape",
        "mapping_endpoint_observations",
        "otc_historical_trade_tape",
        "otc_status_symbol_query",
        "sourced_fact_applications",
        "sourced_fact_intents",
        "terminal_cash_merger_trade",
    }
)


def _evidence_facts(value, *, append=False, historical=False):
    from libs.alpaca_security_lineage.sourced_facts import EXTENSIONS, canonical_extension

    result = deepcopy({k: v for k, v in value.items() if k not in _NON_ASSIGNMENT_FIELDS})
    for extension in EXTENSIONS.values():
        if extension in result:
            result[extension] = canonical_extension(result[extension])
    for lookup in result.get("asset_lookups", ()):
        if isinstance(lookup.get("asset"), dict):
            lookup["asset"].pop("cusip", None)  # Optional current snapshot, never a dated owner.
        prior_asset = (lookup.get("retained_symbol_lookup") or {}).get("asset")
        if isinstance(prior_asset, dict):
            prior_asset.pop("cusip", None)
        lookup.pop("source", None)
        if lookup.get("lookup_kind") == "symbol":
            lookup.pop("observed_at_utc", None)
        elif lookup.get("lookup_kind") == "asset_id":
            lookup.pop("observed_at_utc", None)
            lookup.pop("catalogue_conflict", None)
    if append:
        # The accepted pricing handoff already covers this advancement.
        # Actual actions, disposition and coverage quality still participate;
        # acquisition bookkeeping cannot invalidate accepted assignments.
        query = result.get("query", {})
        query.pop("corporate_actions_start", None)
        query.pop("corporate_actions_end", None)
        local = result.get("symbol_disposition", {}).get("local_market_data")
        if isinstance(local, dict):
            local.pop("maximum_rows", None)
        # The metadata owner has already completed dependency closure. Its
        # query labels record where it looked, not another ownership fact.
        # Returned actions, lookups and unresolved endpoints remain compared.
        result.pop("queried_symbols", None)
    return _historical_facts(result) if historical else result


def _historical_facts(result, *, listed_asset=None):
    """Narrow an already prepared comparison to facts affecting historical ownership."""
    if "asset_lookups" in result:
        # A newly observed negative UUID lookup supplies no historical owner.
        result["asset_lookups"] = [
            item
            for item in result["asset_lookups"]
            if not (
                item.get("lookup_kind") == "asset_id"
                and item.get("outcome") == "not_found"
                and item.get("http_status") == 404
                and item.get("asset") is None
            )
        ]

    def without_descriptions(item):
        if isinstance(item, list):
            return [without_descriptions(value) for value in item]
        if not isinstance(item, dict):
            return item
        projected = {key: without_descriptions(value) for key, value in item.items()}
        if {"id", "symbol", "class", "exchange"} <= projected.keys():
            for field in ("name", "status", "tradable"):
                projected.pop(field, None)
            if listed_asset == (projected["id"], projected["exchange"]):
                projected.pop("exchange")
        return projected

    return without_descriptions(result)


def _sole_listed_owner(prior, current):
    """Scope a transfer to accepted rows having only one possible listed owner."""
    if not current.get("listing_exchange_changes") or prior.get("unresolved_rows") != 0:
        return None
    left = prior["evidence"].get("identities", ())
    right = current.get("identities", ())
    members = prior.get("identity_memberships", ())
    if len(left) != 1 or len(right) != 1 or len(members) != 1:
        return None
    before, after = left[0], right[0]
    asset_id = before.get("id")
    if (
        not asset_id
        or members[0]["member_alpaca_asset_ids"] != [asset_id]
        or after.get("id") != asset_id
        or any(
            item.get("symbol") != prior["symbol"] or item.get("class") != "us_equity" for item in (before, after)
        )
        or any(item.get("exchange") in (None, "", "OTC") for item in (before, after))
        or before["exchange"] == after["exchange"]
        or any(
            action.get("action_type") not in {"cash_dividend", "forward_split", "reverse_split"}
            for action in current.get("corporate_actions", ())
        )
    ):
        return None
    if not any(
        change["asset_id"] == asset_id
        and change["from_exchange"] == before["exchange"]
        and change["to_exchange"] == after["exchange"]
        and change["from_symbol"] == change["to_symbol"] == prior["symbol"]
        for change in reversed(current["listing_exchange_changes"])
    ):
        return None
    return ((asset_id, before["exchange"]), (asset_id, after["exchange"]))


def preserves_retired_prefix(prior, current):
    """An own-label retirement does not revoke an already assigned single-UUID tape.

    Callers establish pricing/mapping applicability. A previously unresolved
    position or a competing action/mapping remains work for the ownership producer.
    """
    if not prior or prior.get("unresolved_rows") != 0 or not prior.get("source_rows"):
        return False
    before = prior["evidence"]
    record = next((r for r in current.get("asset_lookups", ()) if r.get("lookup_kind") == "asset_id"), None)
    if record is None:
        return False
    anchor = record["lookup"]
    memberships = prior.get("identity_memberships", ())
    if len(memberships) != 1 or memberships[0]["member_alpaca_asset_ids"] != [anchor]:
        return False
    previous = next(
        (
            item
            for item in before.get("asset_lookups", ())
            if item.get("lookup_kind") == "symbol" and item.get("lookup") == prior["symbol"]
        ),
        None,
    )
    if (previous or {}).get("asset") != record.get("retained_symbol_lookup", {}).get("asset") or not (
        previous or {}
    ).get("asset"):
        return False
    if before.get("corporate_actions") != current.get("corporate_actions"):
        return False
    return all(
        not (lookup.get("asset") or {}).get("id") or lookup["asset"]["id"] == anchor
        for lookup in current["asset_lookups"]
    ) and all(lookup in current["asset_lookups"] for lookup in before["asset_lookups"] if lookup is not previous)


def evidence_delta(previous, current, *, append=False, sole_owner_receipt=None):
    """Classify held metadata facts by the narrowest lineage scope they can change."""
    before = _evidence_facts(previous, append=append)
    after = _evidence_facts(current, append=append)
    if before == after:
        return {"scope": "none", "changed_fields": []}
    changed = sorted(key for key in before.keys() | after.keys() if before.get(key) != after.get(key))
    if append:
        listed = _sole_listed_owner(sole_owner_receipt, current) if sole_owner_receipt else None
        old_listing, new_listing = listed or (None, None)
        if _historical_facts(before, listed_asset=old_listing) == _historical_facts(
            after, listed_asset=new_listing
        ):
            return {"scope": "added_sessions", "changed_fields": changed}
    return {"scope": "complete_group", "changed_fields": changed}


def stable_evidence(previous, current, *, append=False):
    """Return whether metadata preserves completed historical assignments."""
    return evidence_delta(previous, current, append=append)["scope"] != "complete_group"


def append_history_invalidation(previous, inputs):
    """Name accepted prior decisions that new rows can actually reconsider.

    A later suffix cannot establish an observation on an earlier unresolved
    session. Only overlapping new group observations may reconsider that prior
    scope. Changed mapping/evidence and incomplete partitions retain their
    separate owning reconciliation paths.
    """
    added = sorted(symbol for symbol, view in inputs.items() if any(rows for rows in view.views.values()))
    if not added:
        return None
    unresolved = sorted(
        symbol for symbol, prior in previous.items() if prior and prior.get("unresolved_rows", 0)
    )
    incomplete = sorted(
        symbol
        for symbol, prior in previous.items()
        if prior
        and (
            prior.get("row_partition_complete") is not True
            or prior.get("source_rows") != prior.get("correlated_rows", 0) + prior.get("unresolved_rows", 0)
        )
    )
    if unresolved and not incomplete:
        # The producer's all-view assignment bounds cover every accepted row,
        # including unresolved ones. Consume those bounds without reading old
        # assignment bodies or deriving historical session grids again.
        summaries = [previous[symbol].get("assignment_summary") for symbol in unresolved]
        ends = [
            view.get("last_session") for summary in summaries if summary is not None for view in summary.values()
        ]
        if all(summaries) and ends and all(ends):
            last = max(ends)
            added_bounds = [
                (
                    timestamp(rows[0][0]).astimezone(NEW_YORK).date().isoformat(),
                    timestamp(rows[-1][0]).astimezone(NEW_YORK).date().isoformat(),
                )
                for view in inputs.values()
                for rows in view.views.values()
                if rows
            ]
            # Previously observed future-dated actions may need new donor rows
            # before their occupancy evidence can resolve an earlier ambiguity.
            # Reaching such a date is a real changed observation dependency,
            # unlike an ordinary disjoint suffix with no dated event to supply.
            reached_action = any(
                isinstance(day, str) and last < day and any(first <= day <= end for first, end in added_bounds)
                for symbol in unresolved
                for action in previous[symbol]["evidence"].get("corporate_actions", ())
                for key in ("effective_date", "ex_date", "process_date", "payable_date")
                for day in (action.get(key),)
            )
            if not reached_action and all(first > last for first, _ in added_bounds):
                unresolved = []
    if not unresolved and not incomplete:
        return None
    return {"added_symbols": added, "unresolved_symbols": unresolved, "incomplete_symbols": incomplete}


def requires_lineage(
    previous,
    evidence,
    *,
    pricing_unchanged,
    refresh=False,
    append=False,
    completed_deltas=None,
    auxiliary_previous=None,
    auxiliary_current=None,
    classification_sources=None,
    empty_successors=(),
):
    """Unknown or changed inputs select the existing complete lineage pipeline."""
    if previous.keys() != evidence.keys() or (refresh or not pricing_unchanged) and completed_deltas is None:
        return True
    required = refresh or not pricing_unchanged
    empty_history = append and all(prior and prior.get("source_rows") == 0 for prior in previous.values())
    for symbol, prior in previous.items():
        if prior is None and symbol in empty_successors:
            continue
        if prior is None or not reusable_decision(prior):
            required = True
            continue
        if classification_sources is not None and symbol not in classification_sources:
            # Pricing already selected owning reconstruction for this source.
            # No retained choice or nonempty membership decision consumes a
            # metadata classification for it in this operation.
            continue
        delta = (
            {
                "scope": "added_sessions",
                "reason": "accepted_retired_label_prefix",
                "changed_fields": ["asset_lookups"],
            }
            if (append or completed_deltas is not None) and preserves_retired_prefix(prior, evidence[symbol])
            else evidence_delta(
                prior["evidence"],
                evidence[symbol],
                append=append or completed_deltas is not None,
                sole_owner_receipt=prior
                if len(previous) == 1 and not (auxiliary_current or auxiliary_previous)
                else None,
            )
        )
        if completed_deltas is not None:
            completed_deltas[symbol] = delta
        if empty_history and delta["scope"] == "complete_group":
            delta = {**delta, "scope": "added_sessions", "reason": "no_retained_rows"}
        event("lineage_evidence_delta", symbol=symbol, **delta)
        required |= delta["scope"] == "complete_group"
    if completed_deltas is not None:
        before, after = auxiliary_previous or {}, auxiliary_current or {}
        if before.keys() != after.keys():
            required = True
            for symbol in before.keys() ^ after.keys():
                completed_deltas[symbol] = {"scope": "complete_group", "reason": "auxiliary_membership_changed"}
        for symbol, current in after.items():
            if symbol not in before:
                continue
            delta = evidence_delta(before[symbol], current, append=True)
            completed_deltas[symbol] = delta
            required |= delta["scope"] == "complete_group"
    return required
