"""Explain completed lineage metadata without opening prices or running rules."""

from collections import Counter
from pathlib import Path

from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_build.evidence import published_assignments, rule_path
from libs.alpaca_pricing_download.contract import symbol_label


def explain(store, symbol, *, session=None, adjustment="raw", maximum=100):
    symbol = symbol_label(symbol)
    with storage.reader_snapshot(
        Path(store), lambda root: storage.select_inputs(root, symbol), include_boundaries=False
    ) as snapshot:
        sources = {}
        candidates = []
        for name, receipt in sorted(snapshot["receipts"].items()):
            if receipt is None:
                sources[name] = {"status": "unavailable"}
                continue
            kinds = None
            if session is not None:
                anchor = receipt["anchor_month"]
                kinds = (
                    "completed_years"
                    if session[:4] < anchor[:4]
                    else "current_year"
                    if session[:7] < anchor
                    else "current_month",
                )
            boundaries = snapshot["boundaries"].get(name)
            if boundaries is None:
                boundaries = storage.read_boundaries(Path(store) / "end_of_day/lineage", receipt, kinds=kinds)
            # Empty unselected branches adapt this scoped selection to the
            # existing assignment iterator; they make no assertion about storage.
            scoped = {
                kind: boundaries.get(kind, {"adjustments": []})
                for kind in ("completed_years", "current_year", "current_month")
            }
            segments = [
                segment
                for segment in published_assignments(receipt, scoped)
                if segment["adjustment"] == adjustment
                and (session is None or segment["from"] <= session <= segment["to"])
            ]
            registry = receipt.get("row_correlation_rule_order", [])
            counts = Counter(
                segment.get("correlation_rule", {}).get("name", "not_recorded") for segment in segments
            )
            chosen = segments[:maximum]
            for segment in chosen:
                segment["rule_path"] = rule_path(segment, registry, fresh=False)
            sources[name] = {
                "status": "published",
                "selector": snapshot["entries"][name],
                "versions": receipt.get("versions"),
                "correlation_policy": receipt.get("correlation_policy"),
                "ownership_rule_order": receipt.get("ownership_interval_rule_order"),
                "row_rule_order": registry,
                "evidence": receipt.get("evidence"),
                "identity_memberships": receipt.get("identity_memberships"),
                "identity_intervals": receipt.get("identity_intervals"),
                "evidence_issues": receipt.get("evidence_issues"),
                "current_source_ownership": receipt.get("current_source_ownership"),
                "sourced_rename_ownership": receipt.get("sourced_rename_ownership"),
                "public_common_continuation_ownership": receipt.get("public_common_continuation_ownership"),
                "assignment_handoffs": receipt.get("assignment_handoffs"),
                "selected_boundary_kinds": list(kinds) if kinds else list(scoped),
                "observation_choice_capabilities": {
                    kind: {
                        "version": handoff.get("observation_choices_version"),
                        "explicit_choices_recorded": "observation_choices" in handoff,
                    }
                    for kind, handoff in receipt.get("assignment_handoffs", {}).items()
                },
                "serving_ownership": receipt.get("serving_ownership"),
                "matched_segments": len(segments),
                "omitted_segments": max(0, len(segments) - maximum),
                "rule_counts": dict(sorted(counts.items())),
                "segments": chosen,
            }
            for segment in segments:
                identity = segment.get("security_identity", {}).get("security_id")
                if segment.get("observation_selection") == "selected" and identity:
                    candidates.append({"source": name, "public_id": identity, **segment})
        overlaps = []
        for position, left in enumerate(candidates):
            for right in candidates[position + 1 :]:
                if left["source"] == right["source"] or left["public_id"] != right["public_id"]:
                    continue
                first, last = max(left["from"], right["from"]), min(left["to"], right["to"])
                if first <= last:
                    overlaps.append(
                        {
                            "public_id": left["public_id"],
                            "sources": [left["source"], right["source"]],
                            "from": first,
                            "to": last,
                            "same_first_timestamp": left.get("first_timestamp") is not None
                            and left.get("first_timestamp") == right.get("first_timestamp"),
                            "meaning": "overlapping selected segment bounds; exact shared sessions are not established by bounds alone",
                        }
                    )
        return {
            "status": "complete",
            "execution_complete": True,
            "mode": "published_metadata_only",
            "symbol": symbol,
            "group": sorted(snapshot["entries"]),
            "session": session,
            "adjustment": adjustment,
            "sources": sources,
            "selected_segment_overlaps": overlaps,
            "limits": [
                "No price payloads, provider requests, rule evaluation or publication.",
                "Recorded producer selections are reported as recorded, without authenticating them again.",
                "Earlier first-match rules fell through; their individual failed predicates were not recorded.",
                "Multiple ticker labels, multiple public identities and competing numerical observations are distinct facts.",
            ],
        }
