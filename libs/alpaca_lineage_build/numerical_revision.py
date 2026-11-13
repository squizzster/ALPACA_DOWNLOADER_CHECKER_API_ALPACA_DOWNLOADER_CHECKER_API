"""Construct a numerical-only revision from completed source-selection values."""

from libs.alpaca_pricing_download.contract import ADJUSTMENTS

from .contract import LineageError, document_digest


def build(previous, receipt_generations, completion, *, records, positions):
    """Carry accepted owners/labels and equality into serving; never read prices."""
    scopes = completion.scopes
    membership = {pid: {view: set() for view in ADJUSTMENTS} for pid in scopes}
    labels = {pid: {} for pid in scopes}
    for name, report in completion.reports.items():
        for segment in report["correlated"]["segments"]:
            if segment.get("observation_selection") != "selected":
                continue
            pid = segment["security_identity"]["security_id"]
            view = segment["adjustment"]
            for index in range(segment["row_start"] - 1, segment["row_end"]):
                original = positions[name][view][index]
                choice = completion.changes[name][view][original]
                labels[pid].setdefault(choice["day"], set()).update(choice["observation_tickers"])
    for pid, days in scopes.items():
        if pid not in records:
            raise LineageError("numerical-only completion requires its accepted serving checkpoint")
        record = records[pid]
        for day in days:
            old_labels = {
                item["ticker"]
                for item in record["ticker_ranges"]
                if item["first_session"] <= day <= item["last_session"]
            }
            if labels[pid].get(day) != old_labels:
                raise LineageError("numerical-only completion changed accepted dated labels")
    for name, receipt in previous.items():
        for view, accepted in receipt["serving_ownership"]["views"].items():
            counts = {}
            for run in accepted["ownership"]:
                pid = run["owner"][0]
                if pid in scopes:
                    counts[pid] = counts.get(pid, 0) + run["rows"]
            # Compact accepted runs cover unselected boundaries. Only newly
            # superseded positions reduce their contribution; no unrelated
            # assignment body or timestamp grid is needed to retain membership.
            changes = completion.changes.get(name, {}).get(view, {})
            runs, position, index = accepted["ownership"], 0, 0
            for offset, choice in sorted(changes.items()):
                while index < len(runs) and position + runs[index]["rows"] < offset:
                    position += runs[index]["rows"]
                    index += 1
                if index == len(runs):
                    raise LineageError("numerical revision exceeds accepted source positions")
                pid = runs[index]["owner"][0]
                if choice["observation_selection"] == "superseded" and pid in scopes:
                    counts[pid] -= 1
            for pid, count in counts.items():
                if count > 0:
                    membership[pid][view].add(name)
    revision = {
        "version": 2,
        "previous_receipt_generations": receipt_generations,
        "pricing_generations": {name: value["source_sha256"] for name, value in previous.items()},
        "sessions": scopes,
        "identity_episodes": {pid: [] for pid in scopes},
        "labels_unchanged": True,
        "selection_unchanged_outside_scope": True,
        "retained_session_grid": True,
        "numeric_reuse_sessions": {
            pid: [day for day in days if completion.equality_outcomes[pid].get(day) is True]
            for pid, days in scopes.items()
        },
        "source_membership": {
            pid: {view: sorted(names) for view, names in views.items()} for pid, views in membership.items()
        },
        "selected_sources": completion.selected_sources,
    }
    revision["id"] = document_digest(revision)
    return revision
