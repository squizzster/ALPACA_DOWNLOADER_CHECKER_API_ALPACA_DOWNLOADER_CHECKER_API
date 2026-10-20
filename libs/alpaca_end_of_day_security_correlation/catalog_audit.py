"""Read-only, catalogue-wide authentication and integrity audit."""

import argparse
import json
import sys
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping
from datetime import date
from pathlib import Path
from typing import Any, TextIO
from uuid import UUID

from libs.download_alpaca_end_of_day_bars import ALPACA_ADJUSTMENT_MODES

from .security_master import AuthenticatedSecurityMaster, authenticate_security_master

_IDENTITY_STRING_LIST_FIELDS = (
    "tickers",
    "names",
    "cusips",
    "exchanges",
    "asset_classes",
    "statuses",
)

_Check = Callable[
    [Mapping[str, dict[str, Any]], Mapping[str, dict[str, Any]]],
    list[dict[str, Any]],
]


def _finding(
    check: str,
    code: str,
    message: str,
    *,
    severity: str = "error",
    **evidence: Any,
) -> dict[str, Any]:
    return {
        "check": check,
        "code": code,
        "severity": severity,
        "message": message,
        **evidence,
    }


def _canonical_string_list(value: object) -> bool:
    return (
        isinstance(value, list)
        and all(isinstance(item, str) and item for item in value)
        and value == sorted(set(value))
    )


def _canonical_objects(value: object) -> bool:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        return False
    serialized = [json.dumps(item, sort_keys=True, separators=(",", ":")) for item in value]
    return serialized == sorted(set(serialized))


def _canonical_date(value: object, *, nullable: bool = False) -> bool:
    if value is None:
        return nullable
    if not isinstance(value, str):
        return False
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _security_summary(security: dict[str, Any]) -> dict[str, Any]:
    return {
        "master_id": security.get("master_id"),
        "online": security.get("online"),
        "availability_status": security.get("availability_status"),
        "alpaca_asset_ids": security.get("alpaca_asset_ids"),
        "source_symbols": security.get("source_symbols"),
        "tickers": security.get("tickers"),
        "names": security.get("names"),
        "cusips": security.get("cusips"),
    }


def _audit_source_identity_closure(
    sources: Mapping[str, dict[str, Any]],
    securities: Mapping[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    check = "source_identity_closure"
    source_symbols_by_master_id: dict[str, set[str]] = defaultdict(set)
    for symbol, source in sources.items():
        for identity in source["identities"]:
            source_symbols_by_master_id[identity["master_id"]].add(symbol)

    findings = []
    for master_id in sorted(set(source_symbols_by_master_id) - set(securities)):
        findings.append(
            _finding(
                check,
                "source_identity_not_published",
                "source contributions name a master ID that is absent from the security inventory",
                master_id=master_id,
                source_symbols=sorted(source_symbols_by_master_id[master_id]),
            )
        )
    for master_id in sorted(set(securities) - set(source_symbols_by_master_id)):
        findings.append(
            _finding(
                check,
                "published_identity_without_source_contribution",
                "a published master ID has no source contribution",
                master_id=master_id,
                source_symbols=securities[master_id].get("source_symbols"),
            )
        )
    for master_id in sorted(set(source_symbols_by_master_id) & set(securities)):
        contributed = sorted(source_symbols_by_master_id[master_id])
        published = securities[master_id].get("source_symbols")
        if published != contributed:
            findings.append(
                _finding(
                    check,
                    "identity_source_symbols_mismatch",
                    "the identity's source-symbol projection does not match all source contributions",
                    master_id=master_id,
                    contributed_source_symbols=contributed,
                    published_source_symbols=published,
                )
            )
    return findings


def _audit_alpaca_asset_id_partition(
    _sources: Mapping[str, dict[str, Any]],
    securities: Mapping[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    check = "alpaca_asset_id_partition"
    master_ids_by_asset_id: dict[str, set[str]] = defaultdict(set)
    findings = []
    for master_id, security in securities.items():
        for asset_id in security["alpaca_asset_ids"]:
            try:
                canonical_asset_id = str(UUID(asset_id))
            except (AttributeError, TypeError, ValueError):
                canonical_asset_id = None
            if canonical_asset_id != asset_id:
                findings.append(
                    _finding(
                        check,
                        "invalid_alpaca_asset_id",
                        "master-ID membership contains a non-canonical Alpaca UUID",
                        master_id=master_id,
                        alpaca_asset_id=asset_id,
                    )
                )
            master_ids_by_asset_id[asset_id].add(master_id)

    for asset_id, master_ids in sorted(master_ids_by_asset_id.items()):
        if len(master_ids) < 2:
            continue
        ordered_master_ids = sorted(master_ids)
        findings.append(
            _finding(
                check,
                "alpaca_asset_id_multiple_master_ids",
                "one Alpaca asset UUID is a member of multiple public master IDs",
                alpaca_asset_id=asset_id,
                master_ids=ordered_master_ids,
                identities=[_security_summary(securities[master_id]) for master_id in ordered_master_ids],
            )
        )
    return findings


def _audit_identity_member_projection(
    _sources: Mapping[str, dict[str, Any]],
    securities: Mapping[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    check = "identity_member_projection"
    findings = []
    for master_id, security in securities.items():
        for field_name in _IDENTITY_STRING_LIST_FIELDS:
            if not _canonical_string_list(security.get(field_name)):
                findings.append(
                    _finding(
                        check,
                        "noncanonical_identity_metadata",
                        "top-level identity metadata is not a sorted unique list of non-empty strings",
                        master_id=master_id,
                        field=field_name,
                    )
                )

        member_records = security.get("alpaca_identities")
        if not isinstance(member_records, list) or not all(isinstance(item, dict) for item in member_records):
            findings.append(
                _finding(
                    check,
                    "invalid_member_identity_records",
                    "the identity does not contain a list of Alpaca member records",
                    master_id=master_id,
                )
            )
            continue

        projected_asset_ids = [record.get("alpaca_asset_id") for record in member_records]
        if projected_asset_ids != security["alpaca_asset_ids"]:
            findings.append(
                _finding(
                    check,
                    "member_identity_projection_mismatch",
                    "Alpaca member records do not exactly project master-ID membership",
                    master_id=master_id,
                    alpaca_asset_ids=security["alpaca_asset_ids"],
                    projected_alpaca_asset_ids=projected_asset_ids,
                )
            )

        for member_record in member_records:
            asset_id = member_record.get("alpaca_asset_id")
            for field_name in _IDENTITY_STRING_LIST_FIELDS:
                member_values = member_record.get(field_name)
                if not _canonical_string_list(member_values):
                    findings.append(
                        _finding(
                            check,
                            "noncanonical_member_metadata",
                            "member metadata is not a sorted unique list of non-empty strings",
                            master_id=master_id,
                            alpaca_asset_id=asset_id,
                            field=field_name,
                        )
                    )
                    continue
                published_values = security.get(field_name)
                if isinstance(published_values, list) and not set(member_values).issubset(published_values):
                    findings.append(
                        _finding(
                            check,
                            "member_metadata_missing_from_identity",
                            "member metadata is absent from the top-level identity projection",
                            master_id=master_id,
                            alpaca_asset_id=asset_id,
                            field=field_name,
                            missing_values=sorted(set(member_values) - set(published_values)),
                        )
                    )

            periods = member_record.get("ticker_ownership_periods")
            if not _canonical_objects(periods):
                findings.append(
                    _finding(
                        check,
                        "noncanonical_ticker_ownership_periods",
                        "member ticker-ownership periods are not canonical objects",
                        master_id=master_id,
                        alpaca_asset_id=asset_id,
                    )
                )
                continue
            for period in periods:
                ticker = period.get("ticker")
                start = period.get("start_date_inclusive")
                end = period.get("end_date_exclusive")
                if (
                    not isinstance(ticker, str)
                    or not ticker
                    or ticker not in member_record.get("tickers", ())
                    or not _canonical_date(start, nullable=True)
                    or not _canonical_date(end, nullable=True)
                    or (start is not None and end is not None and start >= end)
                ):
                    findings.append(
                        _finding(
                            check,
                            "invalid_ticker_ownership_period",
                            "a member ticker-ownership period is structurally inconsistent",
                            master_id=master_id,
                            alpaca_asset_id=asset_id,
                            period=period,
                        )
                    )
    return findings


def _audit_observed_history_projection(
    _sources: Mapping[str, dict[str, Any]],
    securities: Mapping[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    check = "observed_history_projection"
    findings = []
    allowed_adjustments = set(ALPACA_ADJUSTMENT_MODES)
    for master_id, security in securities.items():
        ranges = security.get("observed_history_ranges")
        if not isinstance(ranges, list) or not ranges or not all(isinstance(item, dict) for item in ranges):
            findings.append(
                _finding(
                    check,
                    "invalid_observed_history_ranges",
                    "the identity has no valid observed-history range list",
                    master_id=master_id,
                )
            )
            continue

        projected_source_symbols = set()
        projected_adjustments = set()
        first_dates = []
        last_dates = []
        range_keys = []
        for observed_range in ranges:
            source_symbol = observed_range.get("source_symbol")
            ticker = observed_range.get("ticker")
            first_date = observed_range.get("first_session_date")
            last_date = observed_range.get("last_session_date")
            adjustments = observed_range.get("adjustments")
            valid = (
                isinstance(source_symbol, str)
                and source_symbol
                and isinstance(ticker, str)
                and ticker
                and _canonical_date(first_date)
                and _canonical_date(last_date)
                and first_date <= last_date
                and _canonical_string_list(adjustments)
                and set(adjustments).issubset(allowed_adjustments)
            )
            if not valid:
                findings.append(
                    _finding(
                        check,
                        "invalid_observed_history_range",
                        "an observed-history range is structurally inconsistent",
                        master_id=master_id,
                        observed_history_range=observed_range,
                    )
                )
                continue
            projected_source_symbols.add(source_symbol)
            projected_adjustments.update(adjustments)
            first_dates.append(first_date)
            last_dates.append(last_date)
            range_keys.append((source_symbol, ticker, first_date, last_date))

        if range_keys != sorted(set(range_keys)):
            findings.append(
                _finding(
                    check,
                    "noncanonical_observed_history_ranges",
                    "observed-history ranges are not uniquely ordered",
                    master_id=master_id,
                )
            )
        if sorted(projected_source_symbols) != security.get("source_symbols"):
            findings.append(
                _finding(
                    check,
                    "observed_history_source_projection_mismatch",
                    "observed-history ranges do not project the identity's source symbols",
                    master_id=master_id,
                    projected_source_symbols=sorted(projected_source_symbols),
                    source_symbols=security.get("source_symbols"),
                )
            )
        if first_dates and (
            min(first_dates) != security.get("first_observed_session_date")
            or max(last_dates) != security.get("last_observed_session_date")
            or sorted(projected_adjustments) != security.get("available_adjustments")
        ):
            findings.append(
                _finding(
                    check,
                    "observed_history_summary_mismatch",
                    "identity coverage fields do not exactly summarize its observed-history ranges",
                    master_id=master_id,
                    projected_first_observed_session_date=min(first_dates),
                    projected_last_observed_session_date=max(last_dates),
                    projected_available_adjustments=sorted(projected_adjustments),
                    first_observed_session_date=security.get("first_observed_session_date"),
                    last_observed_session_date=security.get("last_observed_session_date"),
                    available_adjustments=security.get("available_adjustments"),
                )
            )
        if security.get("online") is True and projected_adjustments != allowed_adjustments:
            findings.append(
                _finding(
                    check,
                    "online_identity_missing_adjustment",
                    "an online identity does not advertise every supported adjustment",
                    master_id=master_id,
                    missing_adjustments=sorted(allowed_adjustments - projected_adjustments),
                )
            )
    return findings


def _audit_online_active_ticker_claims(
    _sources: Mapping[str, dict[str, Any]],
    securities: Mapping[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    check = "online_active_ticker_claims"
    claims_by_ticker: dict[str, dict[tuple[str, str], dict[str, Any]]] = defaultdict(dict)
    for master_id, security in securities.items():
        if security.get("online") is not True:
            continue
        member_records = security.get("alpaca_identities")
        if not isinstance(member_records, list):
            continue
        for member_record in member_records:
            if not isinstance(member_record, dict):
                continue
            if "active" not in member_record.get("statuses", ()):
                continue
            asset_id = member_record.get("alpaca_asset_id")
            periods = member_record.get("ticker_ownership_periods")
            if not isinstance(periods, list):
                continue
            for period in periods:
                if not isinstance(period, dict):
                    continue
                ticker = period.get("ticker")
                if not isinstance(ticker, str) or not ticker or period.get("end_date_exclusive") is not None:
                    continue
                claims_by_ticker[ticker][(master_id, asset_id)] = {
                    "master_id": master_id,
                    "alpaca_asset_id": asset_id,
                    "names": member_record.get("names"),
                    "cusips": member_record.get("cusips"),
                    "source_symbols": security.get("source_symbols"),
                }

    findings = []
    for ticker, claims in sorted(claims_by_ticker.items()):
        master_ids = sorted({master_id for master_id, _asset_id in claims})
        if len(master_ids) < 2:
            continue
        findings.append(
            _finding(
                check,
                "online_active_ticker_multiple_master_ids",
                "one current ticker is claimed by active members of multiple online public IDs",
                severity="warning",
                ticker=ticker,
                master_ids=master_ids,
                claims=[claims[key] for key in sorted(claims)],
            )
        )
    return findings


_CATALOG_CHECKS: tuple[tuple[str, _Check], ...] = (
    ("source_identity_closure", _audit_source_identity_closure),
    ("alpaca_asset_id_partition", _audit_alpaca_asset_id_partition),
    ("identity_member_projection", _audit_identity_member_projection),
    ("observed_history_projection", _audit_observed_history_projection),
    ("online_active_ticker_claims", _audit_online_active_ticker_claims),
)


def _check_result(name: str, findings: list[dict[str, Any]], **counts: int) -> dict[str, Any]:
    error_count = sum(finding["severity"] == "error" for finding in findings)
    warning_count = sum(finding["severity"] == "warning" for finding in findings)
    status = "failed" if error_count else "warning" if warning_count else "passed"
    return {
        "name": name,
        "status": status,
        "error_count": error_count,
        "warning_count": warning_count,
        **counts,
    }


def _materialize_authenticated_mapping(
    mapping: Mapping[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {key: mapping[key] for key in mapping}


def audit_security_master_catalogue(
    repository_root: Path | str,
    *,
    authenticator: Callable[[Path | str], AuthenticatedSecurityMaster] = authenticate_security_master,
) -> dict[str, Any]:
    """Authenticate every catalogue object and audit catalogue-wide invariants without writing."""
    root = Path(repository_root)
    authenticated = authenticator(root)
    sources = _materialize_authenticated_mapping(authenticated.sources_by_symbol)
    securities = _materialize_authenticated_mapping(authenticated.identities_by_master_id)

    findings: list[dict[str, Any]] = []
    checks = [
        _check_result(
            "catalog_authentication",
            [],
            authenticated_source_count=len(sources),
            authenticated_master_id_count=len(securities),
        )
    ]
    for check_name, check in _CATALOG_CHECKS:
        check_findings = check(sources, securities)
        findings.extend(check_findings)
        checks.append(_check_result(check_name, check_findings))

    current = authenticator(root)
    stable = current.master_sha256 == authenticated.master_sha256 and current.master.get(
        "security_master_generation_sha256"
    ) == authenticated.master.get("security_master_generation_sha256")
    stability_findings = []
    if not stable:
        stability_findings.append(
            _finding(
                "catalog_snapshot_stability",
                "catalog_changed_during_audit",
                "the published catalogue generation changed while it was being audited",
                initial_generation_sha256=authenticated.master.get("security_master_generation_sha256"),
                final_generation_sha256=current.master.get("security_master_generation_sha256"),
            )
        )
    findings.extend(stability_findings)
    checks.append(_check_result("catalog_snapshot_stability", stability_findings))

    error_count = sum(finding["severity"] == "error" for finding in findings)
    warning_count = sum(finding["severity"] == "warning" for finding in findings)
    check_statuses = Counter(check["status"] for check in checks)
    availability_counts = Counter(security["availability_status"] for security in securities.values())
    unique_asset_ids = {
        asset_id for security in securities.values() for asset_id in security["alpaca_asset_ids"]
    }
    status = "failed" if error_count else "passed_with_warnings" if warning_count else "passed"
    return {
        "report_type": "alpaca_end_of_day_catalog_audit",
        "read_only": True,
        "status": status,
        "catalog": {
            "security_master_version": authenticated.master.get("security_master_version"),
            "security_master_generation_sha256": authenticated.master.get("security_master_generation_sha256"),
            "created_at": authenticated.master.get("created_at"),
            "source_count": len(sources),
            "master_id_count": len(securities),
            "unique_alpaca_asset_id_count": len(unique_asset_ids),
            "online_master_id_count": sum(security["online"] is True for security in securities.values()),
            "availability_counts": {key: availability_counts[key] for key in sorted(availability_counts)},
        },
        "summary": {
            "check_count": len(checks),
            "passed_check_count": check_statuses["passed"],
            "warning_check_count": check_statuses["warning"],
            "failed_check_count": check_statuses["failed"],
            "error_count": error_count,
            "warning_count": warning_count,
        },
        "checks": checks,
        "findings": findings,
    }


def run_catalog_audit_command(
    cli_arguments: argparse.Namespace,
    *,
    output_stream: TextIO | None = None,
    auditor: Callable[[Path | str], dict[str, Any]] = audit_security_master_catalogue,
) -> dict[str, Any]:
    report = auditor(cli_arguments.repository_root)
    command_output = output_stream or sys.stdout
    json.dump(report, command_output, indent=2)
    command_output.write("\n")
    return report
