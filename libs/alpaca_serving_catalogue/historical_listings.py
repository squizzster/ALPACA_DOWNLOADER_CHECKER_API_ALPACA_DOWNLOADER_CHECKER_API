"""Public fields from completed, identity-scoped historical listing assessments."""

from .contract import CatalogueError

SORT_FIELDS = {
    "historical_listings": ("first_session", "last_session", "symbol"),
    "cross_ticker_listing_changes": ("from_session", "to_session", "from_symbol", "to_symbol"),
}
_CHANGE_FIELDS = (
    "from_session",
    "to_session",
    "from_tape",
    "to_tape",
    "from_exchange",
    "to_exchange",
    "effective_session",
)


def _fields(row, names):
    return {name: row[name] for name in names if name in row}


def project(envelope, online, *, rebuilt=False):
    """Assemble public facts, applying their scope to new assignments only on a rebuild."""
    from libs.alpaca_lineage_build.historical_listing_publication import covers_assessment, covers_change

    if envelope.get("version") != 1:
        raise CatalogueError("incompatible historical listing assessment")
    for owner, assessments in envelope["assessments"].items():
        if owner not in online:
            continue
        spans = online[owner]["ticker_ranges"] if rebuilt else None
        ranges = [
            {
                **_fields(row, ("symbol", "first_session", "last_session", "status", "limits")),
                "observations": [
                    _fields(item, ("session", "trade_at", "tape", "exchange")) for item in row["observations"]
                ],
                "changes": [_fields(item, _CHANGE_FIELDS) for item in row["changes"]],
            }
            for row in assessments
            if spans is None or covers_assessment(row, spans)
        ]
        # The journal owns transitions across separately assessed suffixes. An exact
        # handoff binds both endpoints. A new serving decision can reassign either.
        entries = {}
        for item in envelope.get("range_changes", {}).get(owner, ()):
            symbol = item["to_symbol"]
            first, last = item["from_session"], item["to_session"]
            if rebuilt and (
                not covers_change(item, spans)
                or any(
                    r["ticker"] == symbol
                    and (
                        r["first_session"] <= first <= r["last_session"]
                        or r["first_session"] <= last <= r["last_session"]
                    )
                    for peer, record in online.items()
                    if peer != owner
                    for r in record["ticker_ranges"]
                )
            ):
                continue
            entries[symbol, last] = _fields(item, _CHANGE_FIELDS)
        if entries:
            for row in ranges:
                change = entries.get((row["symbol"], row["first_session"]))
                if change:
                    row["entry_change"] = change
        boundaries = [
            _fields(item, (*_CHANGE_FIELDS, "from_symbol", "to_symbol"))
            for item in envelope.get("label_changes", {}).get(owner, ())
            if spans is None or covers_change(item, spans)
        ]
        yield owner, {"historical_listings": ranges, "cross_ticker_listing_changes": boundaries}
