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
    assess_terminal_traded_episode,
    has_informative_trading,
    has_prolonged_carry_forward,
)
from .ticker_reuse_gap import adjustment_session_consensus
from .uuid_lineage import IDENTITY_PRESERVING_SPLIT_EDGE_TYPES


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
) -> dict[str, Any]:
    validate_correlation_change_log()
    cache = cache if cache is not None else CorrelationCache()
    if not isinstance(lineage_report, dict):
        raise SecurityCorrelationError("lineage API returned a non-object report")
    symbol = lineage_report.get("symbol")
    local_report = lineage_report.get("local")
    lineage = lineage_report.get("lineage")
    if not isinstance(symbol, str) or not isinstance(local_report, dict) or not isinstance(lineage, dict):
        raise SecurityCorrelationError("lineage API report is missing symbol, local, or lineage objects")

    intervals, evidence_issues, uuid_lineage_edges, uuid_lineage_issues = cache.lineage_analysis(lineage)
    evidence_issues.extend(uuid_lineage_issues)
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

    session_consensus = adjustment_session_consensus(
        (
            (
                f"{source['adjustment']}:{source['data_file']}",
                session_dates,
            )
            for _, source, session_dates in authenticated_work_items
        )
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
    complete_source = all(
        descriptor.get("rows", len(download.rows)) == len(download.rows)
        for download, _, _ in authenticated_work_items
        for descriptor in local_downloads
        if isinstance(descriptor, dict) and descriptor.get("file") == download.path.name
    )
    predecessor_prefixes = assess_predecessor_prefixes(
        symbol,
        lineage,
        intervals,
        uuid_lineage_edges,
        authenticated_work_items,
        correlation_loader=correlation_loader if complete_source else None,
        source_rows_loader=source_rows_loader,
    )
    independent_history = assess_independent_historical_intervals(
        symbol,
        lineage,
        intervals,
        uuid_lineage_edges,
        authenticated_work_items,
        correlation_loader=correlation_loader if complete_source else None,
        source_rows_loader=source_rows_loader,
    )
    independent_history += assess_independent_donor_prefixes(
        symbol,
        lineage,
        intervals,
        uuid_lineage_edges,
        authenticated_work_items,
        correlation_loader=correlation_loader if complete_source else None,
        source_rows_loader=source_rows_loader,
    )
    independent_history += assess_bridged_initial_prefix(
        symbol,
        lineage,
        intervals,
        uuid_lineage_edges,
        authenticated_work_items,
        correlation_loader=correlation_loader if complete_source else None,
        source_rows_loader=source_rows_loader,
    )
    bridged_episodes = (
        assess_bridged_traded_episodes(symbol, lineage, intervals, authenticated_work_items)
        if complete_source
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
    closed_history_evidence = (
        complete_source
        and (
            any(edge.edge_type in IDENTITY_PRESERVING_SPLIT_EDGE_TYPES for edge in uuid_lineage_edges)
            or (
                len({item.get("id") for item in lineage.get("identities", []) if item.get("symbol") == symbol})
                > 1
                and any(
                    action.get("action_type") in ("cash_merger", "stock_and_cash_merger", "stock_merger")
                    and action.get("acquiree_symbol") == symbol
                    for action in lineage.get("corporate_actions", [])
                )
            )
        )
        and any(
            source["adjustment"] == "raw" and has_informative_trading(download.rows)
            for download, source, _ in authenticated_work_items
        )
        and all(not has_prolonged_carry_forward(download.rows) for download, _, _ in authenticated_work_items)
    )
    retired_lower_bound = retired_history_lower_bound(symbol, intervals, authenticated_work_items)
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
    for authenticated_download, source, _ in authenticated_work_items:
        file_branches = correlate_download_rows(
            authenticated_download.rows,
            source,
            lineage,
            intervals,
            adjustment_consensus=session_consensus,
            evidence_issues=evidence_issues,
            distinct_uuid_transition=distinct_uuid_transition,
            predecessor_prefixes=predecessor_prefixes,
            terminal_episode=terminal_episode,
            bridged_episodes=bridged_episodes,
            departure_prefix=departure_prefix,
            closed_history_evidence=closed_history_evidence,
            independent_history=independent_history,
            retired_lower_bound=retired_lower_bound,
            retired_continuation=retired_continuation,
            cache=cache,
            uuid_lineage_edges=uuid_lineage_edges,
            adopted_target_pairs=adopted_target_pairs,
            name_change_assessment=cache.name_change_assessment(lineage),
        )
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
        "ownership_interval_rule_order": ownership_interval_rule_order(),
        "row_correlation_rule_order": row_correlation_rule_order(),
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
