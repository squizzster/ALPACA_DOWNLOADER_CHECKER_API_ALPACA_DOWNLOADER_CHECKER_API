"""Resolve overlapping published history with the established source-selection rules."""

from typing import Any

from libs.alpaca_pricing_download.contract import COLUMNS as BAR_FIELD_NAMES

from .errors import EndOfDayReaderError


def _bar_value_key(candidate: dict[str, Any]) -> tuple[str, ...]:
    values = candidate["values"]
    return tuple(values[field_name] for field_name in BAR_FIELD_NAMES)


def _resolve_bar_candidates(
    candidates_by_timestamp: dict[str, list[dict[str, Any]]],
    selected_timestamps_by_source: dict[str, frozenset[str]],
    *,
    allow_strict_superset_selection: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    # The caller has already selected identity assignments and filtered published
    # superseded/conflicting observations. This helper only assembles remaining
    # numerical overlaps; it cannot restore a discarded source or infer identity.
    # Its coverage must describe the complete selected history (or an accepted
    # prefix plus suffix), never just a caller's requested display window.
    bars = []
    resolution_groups: dict[tuple[str, tuple[str, ...], tuple[str, ...]], dict[str, Any]] = {}
    resolved_differing_overlap_count = 0
    for timestamp in sorted(candidates_by_timestamp):
        candidates = candidates_by_timestamp[timestamp]
        keys = {_bar_value_key(candidate) for candidate in candidates}
        chosen_key = next(iter(keys)) if len(keys) == 1 else None
        if chosen_key is None:
            chosen_candidates = []
            selection_rule = None
            if allow_strict_superset_selection:
                maximal_candidates = []
                for candidate in candidates:
                    source_id = candidate["provenance"]["source_id"]
                    coverage = selected_timestamps_by_source[source_id]
                    if not any(
                        coverage < selected_timestamps_by_source[other["provenance"]["source_id"]]
                        for other in candidates
                    ):
                        maximal_candidates.append(candidate)
                maximal_keys = {_bar_value_key(candidate) for candidate in maximal_candidates}
                if len(maximal_keys) == 1:
                    chosen_candidates = maximal_candidates
                    selection_rule = "strict_superset_of_correlated_session_coverage"

            if not chosen_candidates:
                historical_ticker_candidates = [
                    candidate
                    for candidate in candidates
                    if candidate["provenance"].get("symbol") == candidate["provenance"].get("historical_symbol")
                ]
                historical_ticker_source_ids = {
                    candidate["provenance"]["source_id"] for candidate in historical_ticker_candidates
                }
                historical_ticker_keys = {
                    _bar_value_key(candidate) for candidate in historical_ticker_candidates
                }
                if len(historical_ticker_source_ids) == 1 and len(historical_ticker_keys) == 1:
                    chosen_candidates = historical_ticker_candidates
                    selection_rule = "requested_symbol_matches_historical_ticker"

            if not chosen_candidates or selection_rule is None:
                raise EndOfDayReaderError(
                    f"conflicting authenticated bars for security history at {timestamp}; "
                    "no source was preferred"
                )
            chosen_key = _bar_value_key(chosen_candidates[0])
            preferred_source_ids = tuple(
                sorted(
                    {
                        candidate["provenance"]["source_id"]
                        for candidate in chosen_candidates
                        if _bar_value_key(candidate) == chosen_key
                    }
                )
            )
            superseded_source_ids = tuple(
                sorted(
                    {
                        candidate["provenance"]["source_id"]
                        for candidate in candidates
                        if _bar_value_key(candidate) != chosen_key
                    }
                )
            )
            resolved_differing_overlap_count += 1
            resolution_key = (selection_rule, preferred_source_ids, superseded_source_ids)
            resolution = resolution_groups.setdefault(
                resolution_key,
                {
                    "selection_rule": selection_rule,
                    "preferred_source_ids": list(preferred_source_ids),
                    "superseded_source_ids": list(superseded_source_ids),
                    "first_timestamp": timestamp,
                    "last_timestamp": timestamp,
                    "timestamp_count": 0,
                },
            )
            resolution["last_timestamp"] = timestamp
            resolution["timestamp_count"] += 1

        chosen_candidates = [candidate for candidate in candidates if _bar_value_key(candidate) == chosen_key]
        values = chosen_candidates[0]["values"]
        bars.append(
            {
                **values,
                "integrity_error_ids": list(
                    dict.fromkeys(
                        error_id
                        for candidate in chosen_candidates
                        for error_id in candidate["integrity_error_ids"]
                    )
                ),
                "warning_ids": [],
                "provenance": [candidate["provenance"] for candidate in chosen_candidates],
            }
        )
    return bars, {
        "policy": (
            "strict_superset_then_historical_ticker" if allow_strict_superset_selection else "historical_ticker"
        ),
        "resolved_differing_overlap_count": resolved_differing_overlap_count,
        "resolutions": list(resolution_groups.values()),
    }
