"""Operator evidence from the real lineage workflow, never a second resolver."""

import json
from datetime import timedelta
from pathlib import Path

from libs.alpaca_end_of_day_security_correlation.terminal_fallback import TERMINAL_FALLBACK_RULE_NAME
from libs.alpaca_pricing_download.contract import NEW_YORK, symbol_label, timestamp
from libs.alpaca_pricing_download.storage import load_source, published_path, read_update
from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    compare_observed_to_expected_sessions,
)

from . import storage
from .acquisition import unavailable_pricing
from .contract import LineageError, versions
from .pipeline import closure, run_lineage
from .pricing import load_pricing

EVIDENCE_VERSION = 3


def validate_evidence_version():
    path = Path(__file__).resolve().parents[2] / "resources/alpaca_lineage_evidence_change_log.json"
    entries = json.loads(path.read_bytes())
    if not isinstance(entries, list) or len(entries) != EVIDENCE_VERSION:
        raise LineageError("lineage evidence changelog/version mismatch")
    for entry in entries:
        if (
            not isinstance(entry, dict)
            or "version" in entry
            or not isinstance(entry.get("modifications"), list)
            or not entry["modifications"]
            or any(not isinstance(item, str) or not item for item in entry["modifications"])
        ):
            raise LineageError("invalid lineage evidence changelog")


def rule_path(segment, registry, *, fresh):
    """Explain first-match control flow from its recorded choice, not a replay.

    Individual failed predicates are not retained by the producer. Do not invent
    their reasons, or evaluate today's rules against an older publication.
    """
    selected = segment.get("correlation_rule", {}).get("name")
    row_rules = [item for item in registry if item["name"] != TERMINAL_FALLBACK_RULE_NAME]
    names = [item["name"] for item in row_rules]
    promoted = selected == TERMINAL_FALLBACK_RULE_NAME
    initial = "unresolved_uuid_candidates" if promoted else selected
    position = names.index(initial) if initial in names else None
    path = []
    for index, rule in enumerate(row_rules):
        outcome = "not_recorded"
        if position is not None:
            outcome = "fell_through" if index < position else "not_evaluated"
            if index == position:
                outcome = "selected_before_terminal_promotion" if promoted else "selected"
        path.append({**rule, "outcome": outcome})
    terminal = next((item for item in registry if item["name"] == TERMINAL_FALLBACK_RULE_NAME), None)
    if terminal is not None:
        evaluation = segment.get("terminal_correlation_fallback")
        path.append(
            {
                **terminal,
                "outcome": "selected"
                if promoted
                else "not_eligible"
                if evaluation is not None
                else "not_recorded",
                **({"evaluation": evaluation} if evaluation is not None else {}),
            }
        )
    return {
        "basis": "actual_run_selected_rule" if fresh else "published_selected_rule",
        "predicate_failure_reasons_recorded": False,
        "recorded_selection": segment.get("correlation_rule"),
        "steps": path,
    }


def pricing_context(view):
    """Session facts used by the cascade, with identical adjustment grids shared."""
    grids, indices, adjustments = [], {}, {}
    for adjustment, rows in view.views.items():
        timestamps = tuple(row[0] for row in rows)
        if timestamps not in indices:
            sessions = [timestamp(value).astimezone(NEW_YORK).date().isoformat() for value in timestamps]
            grid = {"timestamps": list(timestamps), "session_dates": sessions, "rows": len(rows)}
            if sessions:
                end = (timestamp(timestamps[-1]).astimezone(NEW_YORK).date() + timedelta(days=1)).isoformat()
                try:
                    expected, missing, unexpected = compare_observed_to_expected_sessions(
                        sessions, sessions[0], end
                    )
                    grid["calendar_between_observed_endpoints"] = {
                        "from": sessions[0],
                        "end_exclusive": end,
                        "expected_sessions": list(expected.session_dates),
                        "missing_sessions": list(missing),
                        "unexpected_sessions": list(unexpected),
                    }
                except (ValueError, MarketCalendarError) as error:
                    grid["calendar_between_observed_endpoints"] = {"error": str(error)}
            indices[timestamps] = len(grids)
            grids.append(grid)
        adjustments[adjustment] = indices[timestamps]
    return {
        "source_manifest": view.source,
        "source_sha256": view.source_sha256,
        "adapter_input": view.local,
        "adjustment_session_grids": adjustments,
        "session_grids": grids,
        "price_payloads_included": False,
        "note": "The identity cascade uses provider metadata, session facts and, for predecessor-prefix identity, exact cross-source prices. Fresh actual reports include compact qualifying comparisons, not duplicate price payloads. Published pricing is trusted, not re-authenticated.",
    }


def published_assignments(receipt, boundaries):
    result = []
    for kind in ("completed_years", "current_year", "current_month"):
        for entry in boundaries[kind]["adjustments"]:
            for segment in entry["segments"]:
                if kind == "current_year" and segment["from"][:7] >= receipt["anchor_month"]:
                    continue  # The current-month boundary replaces the year snapshot's overlap.
                result.append({"adjustment": entry["adjustment"], "pricing_boundary": kind, **segment})
    return result


class EvidenceCapture:
    def __init__(self):
        self.sources = {}

    def __call__(self, *, store, receipts, boundaries, inputs, reports, notices, resumed):
        # This callback runs under the pipeline's existing writer and ticker locks.
        # A full diagnostic may read closed session grids even when processing
        # reused their assignments. Those grids do NOT trigger another calculation.
        views = {
            name: inputs[name] if inputs is not None and not resumed else load_pricing(store, name)
            for name in receipts
        }
        self.capture(receipts, boundaries, views, reports, notices, resumed=resumed, offline=False)

    def capture(self, receipts, boundaries, views, reports, notices, *, resumed, offline):
        for symbol, receipt in receipts.items():
            view = views.get(symbol)
            if receipt is None:
                self.sources[symbol] = {
                    "status": "not_published",
                    "pricing": pricing_context(view) if view else None,
                    "pending_pricing_notice": notices.get(symbol),
                }
                continue
            report = reports.get(symbol) if reports else None
            registry = receipt["row_correlation_rule_order"]
            assignments = published_assignments(receipt, boundaries[symbol])
            for segment in assignments:
                fresh = report is not None and (not resumed or segment["pricing_boundary"] == "current_month")
                segment["decision_origin"] = "calculated" if fresh else "reused_publication"
                segment["rule_path"] = rule_path(segment, registry, fresh=fresh)
            changed_inputs = [
                name
                for name, expected in receipt["input_sources"].items()
                if name not in views or views[name].source_sha256 != expected
            ]
            changed_inputs.extend(name for name in unavailable_pricing(receipt) if name in views)
            self.sources[symbol] = {
                "status": "calculated_with_closed_reuse"
                if resumed
                else "calculated"
                if report
                else "reused_publication",
                "freshness": {
                    "matches_installed_versions": receipt["versions"] == versions(),
                    "changed_pricing_inputs": changed_inputs,
                    "pending_pricing_inputs": sorted(set(receipt["input_sources"]).intersection(notices))
                    if offline
                    else [],
                },
                "pricing": pricing_context(view) if view else None,
                "receipt": receipt,
                "assignments": assignments,
                "actual_calculation_scope": "current_month" if resumed else "full_history" if report else None,
                "actual_report": (
                    {key: value for key, value in report.items() if key != "lineage_report"} if report else None
                ),
                "actual_report_evidence": "receipt.evidence" if report else None,
                "calculation_paths": [
                    {"branch": branch, "segment_index": index, **rule_path(segment, registry, fresh=True)}
                    for branch in ("correlated", "uncorrelated")
                    for index, segment in enumerate(report[branch]["segments"])
                ]
                if report
                else [],
                "pending_pricing_notice": notices.get(symbol) if offline else None,
                "acknowledged_pricing_notice": notices.get(symbol) if not offline else None,
                "diagnostic_limit": None
                if report
                else "Detailed transient assessments were not retained; selected decisions and normalized evidence are published. No rules were replayed.",
            }


def inspect_offline(store, symbol, capture):
    """Observe existing publications under their locks; create or repair nothing."""

    def select(root):
        index = storage.read_index(root)
        group = closure(index, {symbol})
        receipts = {name: storage.read_receipt(root, name, index) for name in sorted(group)}
        if receipts[symbol] is None:
            raise LineageError(f"no published lineage for {symbol}; omit --offline to run the pipeline")
        # Include every recorded input, even if a later index changed dependency links.
        group.update(name for receipt in receipts.values() if receipt for name in receipt["input_sources"])
        return {name: index["symbols"].get(name) for name in sorted(group)}

    with storage.reader_snapshot(store, select) as snap:
        views, notices = {}, {}
        for name in sorted(snap["locked"]):
            pricing = published_path(store, f"end_of_day/pricing/{name}")
            if published_path(pricing, ".pending.json").exists():
                raise LineageError(f"{name}: pricing publication incomplete; resume downloader first")
            source = load_source(pricing, name)
            if source is not None:
                views[name] = load_pricing(store, name, source=source)
            notice = read_update(pricing)
            if notice is not None:
                notices[name] = notice
        capture.capture(snap["receipts"], snap["boundaries"], views, None, notices, resumed=False, offline=True)
    return {"results": [{"symbol": symbol, "status": "offline", "processed": []}], "failed": 0}


def output_evidence(store, symbol, *, offline=False, identity_client=None, pricing_client=None, refresh=False):
    validate_evidence_version()
    store, symbol = Path(store).absolute(), symbol_label(symbol)
    if offline and refresh:
        raise ValueError("--offline cannot refresh metadata")
    capture = EvidenceCapture()
    result = (
        inspect_offline(store, symbol, capture)
        if offline
        else run_lineage(
            store,
            [symbol],
            identity_client=identity_client,
            pricing_client=pricing_client,
            refresh=refresh,
            observer=capture,
        )
    )
    return {
        "schema_version": EVIDENCE_VERSION,
        "boundary": "alpaca_lineage_operator_evidence",
        "symbol": symbol,
        "mode": "offline" if offline else "pipeline",
        "installed_versions": versions(),
        "workflow": result,
        "sources": capture.sources,
        "notes": [
            "Internal operator evidence, not a public DataLibrary response.",
            "Public IDs describe pricing continuity; a ticker is a dated label, not an identity.",
            "Rule paths explain recorded first-match decisions; failed predicate reasons are not invented.",
            "Offline or reused publications retain their own versions; they are not recomputed by this command.",
        ],
    }
