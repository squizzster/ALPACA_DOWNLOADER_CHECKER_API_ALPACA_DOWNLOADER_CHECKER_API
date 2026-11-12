"""Retain accepted numerical history around a dated source-choice correction."""

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, COLUMNS

from .contract import policy


def select_revisions(group, previous, inputs, offsets, completed):
    if not hasattr(group, "numerical_source_revisions"):
        return {}
    revisions = {item["id"]: item for item in group.numerical_source_revisions().values()}
    if not revisions:
        return {}
    from .reuse import project_dated_labels

    result = {}
    for prior in previous:
        if prior["policy"] != policy():
            continue
        history = prior["history_validation"]
        for record in history["prefix_records"]:
            pid = record["public_id"]
            if pid in completed or pid in result:
                continue
            matching = [
                item
                for item in revisions.values()
                if pid in item["sessions"]
                and item["previous_receipt_generations"] == prior["references"]["lineage"]
                and item["pricing_generations"] == prior["references"]["pricing"]
            ]
            if not matching:
                continue
            if len(matching) != 1:
                raise ValueError("source revision has competing accepted baseline bindings")
            revision = matching[0]
            if inputs.keys() != history["inputs"].keys() or any(
                not group.preserves_pricing_prefix(source, old, through=record["validated_through"])
                for source, old in history["inputs"].items()
            ):
                continue
            if record["price_reasons"]:
                raise ValueError(
                    "scoped source revision needs the legacy negative-outcome projection at its owner"
                )
            dates = record.get("session_dates")
            if dates is None:
                if record.get("session_projection_version") is not None:
                    raise ValueError("source revision lacks declared session dates; owning recovery required")
                dates = group.serving_session_dates(pid)
                if (
                    len(dates) != record["coverage"]["session_count"]
                    or not dates
                    or dates[0] != record["first_session"]
                    or dates[-1] != record["last_session"]
                ):
                    raise ValueError("source revision date assembly differs from accepted raw positions")
            if not set(revision["sessions"][pid]) <= set(dates):
                raise ValueError("source revision requires owning projection for a changed session grid")
            projected = project_dated_labels(record, revision["identity_episodes"][pid], dates)
            result[pid] = {
                "rows": {
                    source: {view: item["rows"] for view, item in old["views"].items()}
                    for source, old in history["inputs"].items()
                },
                "offsets": offsets,
                "coverage": {
                    view: {"version": 2, "sources": names}
                    for view, names in revision["source_membership"][pid].items()
                },
                "last_session": record["validated_through"],
                "record": projected,
                "refresh_gap_sources": False,
                "source_revision": revision,
            }
    return result


def finish_revision(group, public_id, prefix, work):
    """Validate only newly selected values; completed dates and prices stay closed."""
    revision = prefix.get("source_revision")
    if revision is None:
        return prefix
    from .policy import _bar_reasons

    required = set(revision["sessions"][public_id]) - set(revision["numeric_reuse_sessions"][public_id])
    if not required:
        return prefix
    observations = group.revised_observations(public_id, revision, days=required)
    record = prefix["record"]
    labels = {
        day: sorted(
            {
                item["ticker"]
                for item in record["ticker_ranges"]
                if item["first_session"] <= day <= item["last_session"]
            }
        )
        for day in required
    }
    reasons, before = set(), set()
    frames, checked = {}, {}
    if set(observations) != set(labels):
        raise ValueError("source revision observations do not cover its selected sessions")
    for day, views in observations.items():
        if set(views) != set(ADJUSTMENTS):
            raise ValueError("source revision lacks a complete selected bundle")
        for adjustment, value in views.items():
            if "bar" in value:
                bar = value["bar"]
            else:
                row = value["row"]
                key = id(row), day
                if key not in frames:
                    frames[key] = {**dict(zip(COLUMNS, row)), "session": day, "tickers": labels[day]}
                bar = frames[key]
            key = id(bar), adjustment in {"split", "split,dividend", "split,spin-off", "all"}
            if key not in checked:
                checked[key] = _bar_reasons(bar, adjustment)
            reasons.update(checked[key])
            if day < record["last_session"]:
                before.update(checked[key])
            work["checked_bars"] += 1
    updated = {**record, "price_reasons": sorted(reasons), "before_last_price_reasons": sorted(before)}
    return {**prefix, "record": updated}
