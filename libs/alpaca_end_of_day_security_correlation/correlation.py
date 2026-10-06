from pathlib import Path
from typing import Any, Callable

from libs.alpaca_end_of_day_pipeline import PIPELINE_VERSION
from libs.alpaca_security_lineage import inspect_alpaca_security_lineage

from .correlation_version import CORRELATION_VERSION, validate_correlation_change_log
from .errors import SecurityCorrelationError
from .evidence import build_ticker_ownership_intervals, ownership_interval_rule_order
from .frozen_downloads import read_authenticated_frozen_download
from .publication import lineage_evidence_sha256, publish_security_correlation
from .row_correlation import correlate_download_rows, row_correlation_rule_order
from .settings import CORRELATED_BRANCH, SYMBOL_PATTERN, UNCORRELATED_BRANCH
from .uuid_lineage import build_uuid_lineage_edges


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
) -> dict[str, Any]:
    validate_correlation_change_log()
    if not isinstance(lineage_report, dict):
        raise SecurityCorrelationError("lineage API returned a non-object report")
    symbol = lineage_report.get("symbol")
    local_report = lineage_report.get("local")
    lineage = lineage_report.get("lineage")
    if not isinstance(symbol, str) or not isinstance(local_report, dict) or not isinstance(lineage, dict):
        raise SecurityCorrelationError("lineage API report is missing symbol, local, or lineage objects")

    intervals, evidence_issues = build_ticker_ownership_intervals(lineage)
    uuid_lineage_edges, uuid_lineage_issues = build_uuid_lineage_edges(lineage)
    evidence_issues.extend(uuid_lineage_issues)
    correlated_segments = []
    uncorrelated_segments = []
    work_items = [
        {
            "scope": "lineage_evidence",
            **evidence_issue,
        }
        for evidence_issue in evidence_issues
    ]
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

    for local_download in sorted(
        (item for item in local_downloads if isinstance(item, dict)),
        key=lambda item: str(item.get("file")),
    ):
        authenticated_download = read_authenticated_frozen_download(
            repository_root,
            symbol_directory,
            local_download,
        )
        source = _source_context(authenticated_download)
        file_branches = correlate_download_rows(
            authenticated_download.rows,
            source,
            lineage,
            intervals,
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
        "lineage_report": lineage_report,
        "source_downloads": source_downloads,
        "source_row_count": source_row_count,
        "ownership_interval_rule_order": ownership_interval_rule_order(),
        "row_correlation_rule_order": row_correlation_rule_order(),
        "uuid_lineage_edges": [edge.as_report_object() for edge in uuid_lineage_edges],
        "ticker_ownership_intervals": [_interval_report_object(interval) for interval in intervals],
        "evidence_issues": evidence_issues,
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


def correlate_and_publish_alpaca_end_of_day_security_segments(
    ticker: str,
    api_key_id: str,
    api_secret_key: str,
    *,
    repository_root: Path | None = None,
    lineage_inspector: Callable[..., dict] = inspect_alpaca_security_lineage,
) -> dict[str, Any]:
    symbol = ticker.upper()
    if not SYMBOL_PATTERN.fullmatch(symbol):
        raise ValueError(f"invalid ticker: {ticker!r}")
    correlation_repository_root = repository_root or Path.cwd()
    lineage_report = lineage_inspector(
        symbol,
        api_key_id,
        api_secret_key,
        repository_root=correlation_repository_root,
    )
    correlation_report = build_security_correlation_report(
        lineage_report,
        correlation_repository_root,
    )
    return publish_security_correlation(correlation_repository_root, correlation_report)
