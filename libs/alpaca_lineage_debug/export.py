"""Inspect one completed CSV manifest's declared ticker instances, not CSV rows."""

import json
from collections import defaultdict
from pathlib import Path


def selected_path(root, relative):
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise ValueError("manifest paths must be nonempty relative paths")
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("manifest path escapes the export directory")
    return path


def instances(manifest, symbols, *, multiple_only=True):
    path = Path(manifest).absolute()
    value = json.loads(path.read_bytes())
    if value.get("schema_version") not in (1, 2) or value.get("format") != "alpaca_public_id_eod_csv":
        raise ValueError("a completed public-ID CSV manifest is required")
    identities = value.get("identities")
    if not isinstance(identities, dict) or not value.get("completed_at"):
        raise ValueError("manifest is incomplete")
    selected = set(symbols)
    tickers = defaultdict(list)
    missing = []
    for public_id, entry in sorted(identities.items()):
        digest = json.loads(selected_path(path.parent, entry["digest"]).read_bytes())
        if digest.get("public_id") != public_id or digest.get("catalogue_generation") != value.get(
            "catalogue", {}
        ).get("generation"):
            raise ValueError(f"{public_id}: digest does not belong to the selected manifest")
        security = digest.get("security", {})
        ranges = security.get("ticker_ranges")
        if not isinstance(ranges, list):
            missing.append(public_id)
            continue
        per_ticker = defaultdict(list)
        for interval in ranges:
            ticker = interval["ticker"]
            if selected and ticker not in selected:
                continue
            first, last = interval["first_session"], interval["last_session"]
            # The completed digest owns these dated labels. Use its endpoints
            # directly; do not replay the producer's range validation.
            per_ticker[ticker].append({"first_session": first, "last_session": last})
        for ticker, periods in per_ticker.items():
            tickers[ticker].append(
                {
                    "public_id": public_id,
                    "csv": entry["csv"],
                    "names": security.get("names", []),
                    "rows": entry.get("rows"),
                    "ranges": periods,
                }
            )
    reports = []
    for ticker, matches in sorted(tickers.items()):
        if multiple_only and len(matches) < 2:
            continue
        overlaps = []
        for position, left in enumerate(matches):
            for right in matches[position + 1 :]:
                for a in left["ranges"]:
                    for b in right["ranges"]:
                        first = max(a["first_session"], b["first_session"])
                        last = min(a["last_session"], b["last_session"])
                        if first <= last:
                            overlaps.append(
                                {
                                    "public_ids": [left["public_id"], right["public_id"]],
                                    "from": first,
                                    "to": last,
                                }
                            )
        reports.append(
            {
                "ticker": ticker,
                "instance_count": len(matches),
                "classification": "overlapping_dated_instances"
                if overlaps
                else "disjoint_dated_instances"
                if len(matches) > 1
                else "single_instance",
                "instances": matches,
                "overlaps": overlaps,
            }
        )
    return {
        "status": "complete" if not missing else "incomplete",
        "execution_complete": not missing,
        "manifest": str(path),
        "catalogue_generation": value["catalogue"]["generation"],
        "summary": {
            "identities": len(identities),
            "reported_tickers": len(reports),
            "multiple_instance_tickers": sum(item["instance_count"] > 1 for item in reports),
            "overlapping_tickers": sum(bool(item["overlaps"]) for item in reports),
        },
        "tickers": reports,
        "missing_dated_label_handoffs": missing,
        "limits": [
            "Only the named completed manifest and its named digests are consumed once; no CSV or source prices are opened.",
            "Inclusive recorded label bounds are reported; gaps between actual sessions are not reconstructed.",
            "Multiple public identities for a dated ticker are not automatically a lineage defect.",
            "An exported identity's presence does not establish the options or reader behavior of the earlier run.",
        ],
    }
