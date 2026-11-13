from copy import deepcopy
from pathlib import Path
from typing import Any, Callable

from libs.alpaca_end_of_day_pipeline.pipeline_version import PIPELINE_VERSION
from libs.alpaca_security_lineage import (
    RECURRING_PIPELINE_RUN,
    recurring_pipeline_should_skip,
)

from .correlation_version import CORRELATION_VERSION, validate_correlation_change_log
from .errors import SecurityCorrelationError
from .evidence import ownership_interval_rule_order
from .evidence_digest import lineage_evidence_sha256
from .identity_partition import assess_one_session_distinct_uuid_transition
from .predecessor_prefix import (
    assess_bridged_initial_prefix,
    assess_independent_donor_prefixes,
    assess_independent_historical_intervals,
    assess_mapped_continuations,
    assess_predecessor_prefixes,
)
from .retired_continuation import assess_retired_continuation
from .row_correlation import (
    CorrelationCache,
    correlate_download_rows,
    row_correlation_rule_order,
)
from .settings import CORRELATED_BRANCH, UNCORRELATED_BRANCH
from .source_continuity import retired_history_lower_bound
from .terminal_episode import (
    assess_bridged_traded_episodes,
    assess_dated_departure_prefix,
    assess_historical_asof_gap,
    assess_terminal_traded_episode,
    assess_vacant_ticker_bridges,
    has_informative_trading,
    has_prolonged_carry_forward,
)
from .ticker_reuse_gap import AdjustmentSessionConsensus


def _source_context(authenticated_download) -> dict[str, Any]:
    manifest = authenticated_download.manifest
    return {
        "data_file": authenticated_download.path.name,
        "download_manifest": authenticated_download.path.name + ".manifest.json",
        "download_sha256": manifest["sha256"],
        "adjustment": manifest["adjustment"],
        "requested_symbol": manifest["symbol"],
        "asof": manifest["asof"],
        "request_from": manifest["request_from"],
        "request_to": manifest["request_to"],
        "feed": manifest["feed"],
        "timeframe": manifest["timeframe"],
        "pipeline_state": authenticated_download.pipeline_state,
        "validation_pipeline_version": PIPELINE_VERSION,
        "recorded_integrity_failure": authenticated_download.integrity_failure,
    }


def _interval_report_object(interval) -> dict[str, Any]:
    return {
        **interval.as_report_object(),
        "cusip": interval.cusip,
        "asset_id": interval.asset_id,
    }


def _without_reconciled_occupancy_actions(
    lineage: dict[str, Any], reconciliations: tuple[dict[str, Any], ...]
) -> dict[str, Any]:
    if not reconciliations:
        return lineage
    rejected = {item["action_id"] for item in reconciliations}
    interpreted = deepcopy(lineage)
    interpreted["corporate_actions"] = [
        action
        for action in interpreted.get("corporate_actions", ())
        if not (
            isinstance(action, dict)
            and action.get("action_type") == "cash_dividend"
            and action.get("id") in rejected
        )
    ]
    for field in ("symbol_occupancy_evidence", "input_symbol_occupants"):
        filtered = []
        for occupancy in interpreted.get(field, ()):
            if not isinstance(occupancy, dict) or not isinstance(occupancy.get("evidence"), list):
                filtered.append(occupancy)
                continue
            if not any(
                isinstance(item, dict) and item.get("action_id") in rejected for item in occupancy["evidence"]
            ):
                filtered.append(occupancy)
                continue
            remaining = [
                item
                for item in occupancy["evidence"]
                if not isinstance(item, dict) or item.get("action_id") not in rejected
            ]
            if remaining:
                filtered.append({**occupancy, "evidence": remaining})
        interpreted[field] = filtered
    return interpreted


def build_security_correlation_report(
    lineage_report: dict[str, Any],
    repository_root: Path,
    *,
    authenticated_download_reader: Callable[..., Any],
    verified_session_exclusions_reader: Callable[..., dict[str, Any]],
    authenticated_symbol_sessions: Callable[..., Any],
    correlation_loader: Callable[[str], Any] | None = None,
    source_rows_loader: Callable[[str, str], tuple[list[dict], str]] | None = None,
    cache: CorrelationCache | None = None,
    ownership_interval_rules=None,
    row_correlation_rules=None,
    occupancy_reconciliations: tuple[dict[str, Any], ...] = (),
    include_action_anchored_current_suffix: bool = False,
    established_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from libs.alpaca_security_lineage.inference_evidence import ProviderEvidence, accept_provider_evidence

    if not isinstance(lineage_report, dict):
        raise SecurityCorrelationError("lineage API returned a non-object report")
    if isinstance(lineage_report.get("lineage"), dict):
        lineage_report = {
            **lineage_report,
            "lineage": cache.provider_evidence(lineage_report["lineage"])
            if cache is not None
            else accept_provider_evidence(lineage_report["lineage"], where="fresh correlation evidence"),
        }
    validate_correlation_change_log()
    # TRUST BOUNDARY: the caller supplies selected input readers and one immutable
    # group cache. This owner calculates new assignments; it does not reacquire
    # provider evidence or authenticate the reader's upstream files. Keep the
    # cache bound to its input group and rule policy, not a later publication.
    cache = (
        cache
        if cache is not None
        else CorrelationCache(
            ownership_interval_rules=ownership_interval_rules,
            row_correlation_rules=row_correlation_rules,
        )
    )
    if ownership_interval_rules is None:
        ownership_interval_rules = cache.ownership_interval_rules
    elif cache.ownership_interval_rules is None or tuple(cache.ownership_interval_rules) != tuple(
        ownership_interval_rules
    ):
        raise SecurityCorrelationError("correlation cache ownership policy does not match requested rules")
    if row_correlation_rules is None:
        row_correlation_rules = cache.row_correlation_rules
    elif cache.row_correlation_rules is None or tuple(cache.row_correlation_rules) != tuple(
        row_correlation_rules
    ):
        raise SecurityCorrelationError("correlation cache row policy does not match requested rules")
    if not isinstance(lineage_report, dict):
        raise SecurityCorrelationError("lineage API returned a non-object report")
    symbol = lineage_report.get("symbol")
    local_report = lineage_report.get("local")
    supplied_lineage = lineage_report.get("lineage")
    if (
        not isinstance(symbol, str)
        or not isinstance(local_report, dict)
        or not isinstance(supplied_lineage, dict)
    ):
        raise SecurityCorrelationError("lineage API report is missing symbol, local, or lineage objects")
    lineage = _without_reconciled_occupancy_actions(supplied_lineage, occupancy_reconciliations)
    if lineage is not supplied_lineage:
        # The reconciliation owner constructs this provider-only projection.
        lineage = ProviderEvidence(lineage)

    intervals, evidence_issues, uuid_lineage_edges, uuid_lineage_issues = cache.lineage_analysis(
        lineage,
        include_action_anchored_current_suffix=include_action_anchored_current_suffix,
    )
    evidence_issues.extend(uuid_lineage_issues)
    evidence_issues.extend(deepcopy(occupancy_reconciliations))
    adopted_target_pairs = frozenset(
        (issue["target_asset_id"], issue["acquirer_asset_id"])
        for issue in uuid_lineage_issues
        if issue.get("reason_code") == "acquirer_adopted_target_symbol"
    )
    verified_session_exclusions = verified_session_exclusions_reader(
        repository_root,
        symbol,
    )
    correlated_segments = []
    uncorrelated_segments = []
    work_items = [
        {
            "scope": "lineage_evidence",
            **evidence_issue,
        }
        for evidence_issue in evidence_issues
    ]
    symbol_disposition = lineage.get("symbol_disposition")
    if not isinstance(symbol_disposition, dict):
        symbol_disposition = {
            "classification": "unresolved_symbol_disposition",
            "data_policy": "investigate",
            "recurring_pipeline_action": RECURRING_PIPELINE_RUN,
            "input_symbol_asset_lookup": {"outcome": "missing", "http_status": None},
            "resolved_alpaca_asset_ids": [],
            "local_market_data": None,
            "market_data_series_identity": None,
            "test_symbol_evidence": None,
        }
    if recurring_pipeline_should_skip(symbol_disposition):
        work_items.append(
            {
                "scope": "symbol_disposition",
                "symbol": symbol,
                "reason_code": symbol_disposition["classification"],
                "data_policy": symbol_disposition["data_policy"],
                "recurring_pipeline_action": symbol_disposition["recurring_pipeline_action"],
            }
        )
    source_downloads = []
    files = []
    symbol_directory = local_report.get("directory")
    local_downloads = local_report.get("downloads")
    if not isinstance(symbol_directory, str) or not isinstance(local_downloads, list):
        raise SecurityCorrelationError("lineage local report has no directory or downloads list")

    if not local_report.get("present") or not local_downloads:
        work_items.append(
            {
                "scope": "symbol",
                "symbol": symbol,
                "reason_code": "no_local_frozen_downloads",
            }
        )

    authenticated_work_items = []
    for local_download in sorted(
        (item for item in local_downloads if isinstance(item, dict)),
        key=lambda item: str(item.get("file")),
    ):
        authenticated_download = authenticated_download_reader(
            repository_root,
            symbol_directory,
            local_download,
        )
        source = _source_context(authenticated_download)
        authenticated_work_items.append(
            (
                authenticated_download,
                source,
                cache.session_dates(authenticated_download.rows),
            )
        )

    authenticated_work_items = cache.prepared_source(authenticated_work_items)
    accepted_current_source = (
        symbol in cache.accepted_current_source_ownership
        or symbol in cache.accepted_rename_sources
        or symbol in cache.accepted_common_sources
    )
    # The continuation owner already accepted the unchanged complete prefix and
    # the new grid/seam. Re-hashing that historical session set is not a new fact.
    session_consensus = (
        AdjustmentSessionConsensus(
            len(authenticated_work_items),
            tuple(f"{source['adjustment']}:{source['data_file']}" for _, source, _ in authenticated_work_items),
            True,
            None,
        )
        if accepted_current_source
        else cache.source_session_consensus(authenticated_work_items)
    )
    distinct_uuid_transition = assess_one_session_distinct_uuid_transition(
        lineage,
        evidence_issues=evidence_issues,
        authenticated_symbol_sessions=authenticated_symbol_sessions,
    )
    if distinct_uuid_transition is not None and symbol not in {
        distinct_uuid_transition.predecessor.symbol,
        distinct_uuid_transition.successor.symbol,
    }:
        distinct_uuid_transition = None
    # Missing pricing sessions do not revoke an accepted identity edge.

    # Incremental runs contain only the open boundary; they cannot establish a
    # whole-source prefix. Earlier published assignments remain trusted as-is.
    complete_source = not accepted_current_source and all(
        descriptor.get("rows", len(download.rows)) == len(download.rows)
        for download, _, _ in authenticated_work_items
        for descriptor in local_downloads
        if isinstance(descriptor, dict) and descriptor.get("file") == download.path.name
    )
    predecessor_prefixes = (
        ()
        if accepted_current_source
        else assess_predecessor_prefixes(
            symbol,
            lineage,
            intervals,
            uuid_lineage_edges,
            authenticated_work_items,
            correlation_loader=correlation_loader if complete_source else None,
            source_rows_loader=source_rows_loader,
        )
    )
    mapped_continuations = (
        ()
        if accepted_current_source
        else assess_mapped_continuations(
            symbol,
            lineage,
            intervals,
            uuid_lineage_edges,
            authenticated_work_items,
            correlation_loader=correlation_loader if complete_source else None,
            source_rows_loader=source_rows_loader,
            include_successor_prefix=cache.mapped_successor_prefixes,
            include_successor_split_chain=cache.mapped_successor_split_chains,
            include_active_successor_prefix=cache.active_successor_prefixes,
        )
    )
    independent_history = (
        ()
        if accepted_current_source
        else assess_independent_historical_intervals(
            symbol,
            lineage,
            intervals,
            uuid_lineage_edges,
            authenticated_work_items,
            correlation_loader=correlation_loader if complete_source else None,
            source_rows_loader=source_rows_loader,
        )
    )
    independent_history += (
        ()
        if accepted_current_source
        else assess_independent_donor_prefixes(
            symbol,
            lineage,
            intervals,
            uuid_lineage_edges,
            authenticated_work_items,
            correlation_loader=correlation_loader if complete_source else None,
            source_rows_loader=source_rows_loader,
        )
    )
    independent_history += (
        ()
        if accepted_current_source
        else assess_bridged_initial_prefix(
            symbol,
            lineage,
            intervals,
            uuid_lineage_edges,
            authenticated_work_items,
            correlation_loader=correlation_loader if complete_source else None,
            source_rows_loader=source_rows_loader,
        )
    )
    bridged_episodes = (
        assess_bridged_traded_episodes(symbol, lineage, intervals, authenticated_work_items)
        if complete_source
        else ()
    )
    vacant_ticker_bridges = (
        assess_vacant_ticker_bridges(
            symbol,
            lineage,
            intervals,
            uuid_lineage_edges,
            authenticated_work_items,
            evidence_issues=evidence_issues,
            correlation_loader=correlation_loader,
            source_rows_loader=source_rows_loader,
            include_action_anchored_arrivals=(cache.action_anchored_vacant_ticker_bridges),
        )
        if complete_source and cache.vacant_ticker_bridge_exclusions
        else ()
    )

    terminal_episode = (
        assess_terminal_traded_episode(symbol, lineage, intervals, authenticated_work_items)
        if complete_source
        else None
    )
    departure_prefix = (
        assess_dated_departure_prefix(symbol, lineage, intervals, authenticated_work_items)
        if complete_source and terminal_episode is not None
        else None
    )
    historical_asof_gap = (
        assess_historical_asof_gap(symbol, lineage, intervals, authenticated_work_items)
        if complete_source
        else None
    )
    if complete_source and authenticated_work_items.closed_history_evidence is None:
        authenticated_work_items.closed_history_evidence = any(
            source["adjustment"] == "raw" and has_informative_trading(download.rows)
            for download, source, _ in authenticated_work_items
        ) and all(
            not has_prolonged_carry_forward(rows)
            for rows in {
                id(download.rows): download.rows for download, _, _ in authenticated_work_items
            }.values()
        )
    closed_history_evidence = accepted_current_source or (
        complete_source and authenticated_work_items.closed_history_evidence
    )
    current_source_ownership = cache.current_source_ownership(
        symbol,
        supplied_lineage,
        intervals,
        uuid_lineage_edges,
        authenticated_work_items,
        session_consensus,
        complete=complete_source,
        informative=closed_history_evidence,
        evidence_issues=evidence_issues,
    )
    sourced_rename_ownership = cache.rename_source(
        symbol,
        supplied_lineage,
        authenticated_work_items,
        session_consensus,
        complete=complete_source,
        informative=closed_history_evidence,
    )
    common_ownership, common_histories = cache.common_source(
        symbol,
        supplied_lineage,
        authenticated_work_items,
        session_consensus,
        complete=complete_source,
        informative=closed_history_evidence,
    )
    retired_lower_bound = (
        None
        if accepted_current_source
        else retired_history_lower_bound(symbol, intervals, authenticated_work_items)
    )
    retired_continuation = (
        assess_retired_continuation(
            symbol,
            lineage,
            authenticated_work_items,
            correlation_loader=correlation_loader,
            source_rows_loader=source_rows_loader,
        )
        if complete_source
        else None
    )
    established_by_adjustment = {}
    if established_report is not None:
        for segment in established_report["correlated"]["segments"]:
            established_by_adjustment.setdefault(segment["adjustment"], []).append(segment)
        # The new fact is compatibility of newly produced intervals with the
        # frozen allocation, not authenticity of either accepted handoff.
        # Reject a contradictory fallback at its producer before reserving rows;
        # never conceal it by replacing a calculated result afterward.
        prior_intervals = established_report["ticker_ownership_intervals"]
        for interval in intervals:
            if _interval_report_object(interval) in prior_intervals or interval.historical_symbol != symbol:
                continue
            for _, source, sessions in authenticated_work_items:
                for segment in established_by_adjustment.get(source["adjustment"], ()):
                    if segment.get("accepted_row_ownership") is True:
                        # This source's input binding still applies. A newly
                        # constructed fallback can claim only unreserved rows;
                        # it cannot audit completed historical ownership again.
                        continue
                    owner = segment["canonical_security"]["alpaca_asset_id"]
                    if owner == interval.asset_id:
                        continue
                    if any(
                        interval.contains(day) for day in sessions[segment["row_start"] - 1 : segment["row_end"]]
                    ):
                        raise SecurityCorrelationError(
                            "fallback ownership conflicts with established row authority"
                        )
    allocations = {}
    for authenticated_download, source, _ in authenticated_work_items:
        # Alias views share immutable rows and every allocation input. Bind only
        # view provenance in the completed segments; do not traverse rows again.
        provenance = {"data_file", "download_manifest", "download_sha256", "adjustment"}
        key = (id(authenticated_download.rows), tuple((k, v) for k, v in source.items() if k not in provenance))
        established = tuple(established_by_adjustment.get(source["adjustment"], ()))
        accepted = allocations.get(key) if not established else None
        file_branches = (
            {
                branch: [
                    {
                        **segment,
                        **{k: source[k] for k in provenance},
                        "segment_id": f"{source['data_file']}:rows:{segment['row_start']}-{segment['row_end']}",
                    }
                    for segment in segments
                ]
                for branch, segments in accepted.items()
            }
            if accepted is not None
            else correlate_download_rows(
                authenticated_download.rows,
                source,
                lineage,
                intervals,
                adjustment_consensus=session_consensus,
                evidence_issues=evidence_issues,
                distinct_uuid_transition=distinct_uuid_transition,
                predecessor_prefixes=predecessor_prefixes,
                mapped_continuations=mapped_continuations,
                terminal_episode=terminal_episode,
                bridged_episodes=bridged_episodes,
                vacant_ticker_bridges=vacant_ticker_bridges,
                departure_prefix=departure_prefix,
                historical_asof_gap=historical_asof_gap,
                closed_history_evidence=closed_history_evidence,
                independent_history=independent_history,
                retired_lower_bound=retired_lower_bound,
                retired_continuation=retired_continuation,
                cache=cache,
                uuid_lineage_edges=uuid_lineage_edges,
                adopted_target_pairs=adopted_target_pairs,
                name_change_assessment=cache.name_change_assessment(
                    lineage,
                    include_action_anchored_current_suffix=include_action_anchored_current_suffix,
                ),
                row_rules=row_correlation_rules,
                established_segments=established,
                current_source_ownership=current_source_ownership,
                sourced_rename_histories=(*cache._rename_histories.get(symbol, ()), *common_histories),
                sourced_rename_mapping=cache._rename_mappings.get(symbol),
            )
        )
        if accepted is None and not established:
            allocations[key] = file_branches
        correlated_file_segments = file_branches[CORRELATED_BRANCH]
        uncorrelated_file_segments = file_branches[UNCORRELATED_BRANCH]
        correlated_segments.extend(correlated_file_segments)
        uncorrelated_segments.extend(uncorrelated_file_segments)

        correlated_row_count = sum(item["row_count"] for item in correlated_file_segments)
        uncorrelated_row_count = sum(item["row_count"] for item in uncorrelated_file_segments)
        source_row_count = len(authenticated_download.rows)
        if correlated_row_count + uncorrelated_row_count != source_row_count:
            raise SecurityCorrelationError(
                f"{authenticated_download.path}: correlation branches do not exhaust source rows"
            )

        source_download = {
            "data_file": authenticated_download.path.name,
            "download_manifest": authenticated_download.path.name + ".manifest.json",
            "download_sha256": authenticated_download.manifest["sha256"],
            "rows": source_row_count,
            "adjustment": authenticated_download.manifest["adjustment"],
            "feed": source["feed"],
            "timeframe": source["timeframe"],
            "pipeline_state": authenticated_download.pipeline_state,
            "validation_pipeline_version": PIPELINE_VERSION,
            "download_sha256_authenticated": True,
        }
        source_downloads.append(source_download)
        files.append(
            {
                **source_download,
                "correlated_segment_count": len(correlated_file_segments),
                "correlated_row_count": correlated_row_count,
                "uncorrelated_segment_count": len(uncorrelated_file_segments),
                "uncorrelated_row_count": uncorrelated_row_count,
                "row_partition_complete": True,
                "response_symbol_label_preserved": False,
            }
        )

    source_row_count = sum(item["rows"] for item in source_downloads)
    correlated_row_count = sum(item["row_count"] for item in correlated_segments)
    uncorrelated_row_count = sum(item["row_count"] for item in uncorrelated_segments)
    row_partition_complete = correlated_row_count + uncorrelated_row_count == source_row_count
    if not row_partition_complete:
        raise SecurityCorrelationError(
            "global row partition invariant failed: "
            f"{correlated_row_count} + {uncorrelated_row_count} != {source_row_count}"
        )

    return {
        "correlation_version": CORRELATION_VERSION,
        "current_source_ownership": current_source_ownership.to_report() if current_source_ownership else None,
        "sourced_rename_ownership": sourced_rename_ownership,
        "sourced_rename_membership": cache.rename_membership_report(supplied_lineage),
        "public_common_continuation_membership": cache.common_membership(supplied_lineage),
        "public_common_continuation_ownership": common_ownership,
        "security_relations": cache.relation_certificate(supplied_lineage),
        "symbol": symbol,
        "boundary": {
            "owner": "alpaca_end_of_day_security_correlation",
            "input": "authenticated_frozen_downloads_plus_lineage_evidence",
            "outputs": [CORRELATED_BRANCH, UNCORRELATED_BRANCH],
            "row_partition_is_exhaustive": True,
        },
        "lineage_evidence_sha256": lineage_evidence_sha256(lineage_report),
        "verified_session_exclusions": verified_session_exclusions,
        "symbol_disposition": symbol_disposition,
        "lineage_report": lineage_report,
        "source_downloads": source_downloads,
        "source_row_count": source_row_count,
        "ownership_interval_rule_order": ownership_interval_rule_order(rules=ownership_interval_rules),
        "row_correlation_rule_order": row_correlation_rule_order(rules=row_correlation_rules),
        "uuid_lineage_edges": [edge.as_report_object() for edge in uuid_lineage_edges],
        "ticker_ownership_intervals": [_interval_report_object(interval) for interval in intervals],
        "evidence_issues": evidence_issues,
        "predecessor_prefix_assessments": [item.as_report_object() for item in predecessor_prefixes],
        "files": files,
        "correlated": {
            "segment_count": len(correlated_segments),
            "row_count": correlated_row_count,
            "segments": correlated_segments,
        },
        "uncorrelated": {
            "segment_count": len(uncorrelated_segments),
            "row_count": uncorrelated_row_count,
            "segments": uncorrelated_segments,
            "work_item_count": len(work_items),
            "work_items": work_items,
        },
        "row_partition_complete": row_partition_complete,
        "warnings": [
            "Frozen CSV rows do not preserve Alpaca's returned symbol label; request and asof metadata are retained.",
            "Correlation applies the recorded ownership-interval and row-correlation rules in order.",
            "Uncorrelated is a durable pipeline branch, not an inferred assignment or a repair result.",
        ],
    }
