"""Restore the closing unit basis of a retired security after a later label-level split."""

from .carried_rename import _traded
from .contract import ADJUSTMENTS
from .dated_rename import UnsupportedInterval, _fields
from .rename_split_basis import _SPLITS, _factor, scaled
from .shifted_split import _PAIRS, _PRICES, _scoped_rows


def factor(actions, symbol, review):
    if (
        len(actions) != 1
        or actions[0].get("symbol") != symbol
        or actions[0].get("action_type") not in _SPLITS
        or actions[0].get("ex_date") != review
    ):
        raise UnsupportedInterval("other unit actions compete with the proposed post-retirement share factor")
    return _factor(actions[0])


def assess(inputs):
    from decimal import Decimal

    info, views = inputs["plan"], inputs["views"]
    dates = tuple(row["session"] for row in views["raw"]["bars"])
    if not dates or dates[0] != info["first"] or dates[-1] != info["last"]:
        raise UnsupportedInterval("retired unit is missing its accepted dated price endpoints")
    rows, symbol = _scoped_rows(views, dates)
    if symbol != info["symbol"] or not _traded(rows["raw"][0]) or not _traded(rows["raw"][-1]):
        raise UnsupportedInterval("retired unit prices lack both accepted traded anchors")
    scale = Decimal(info["factor"])
    bundles = {mode: {} for mode in ADJUSTMENTS}
    for i, raw in enumerate(rows["raw"]):
        # These two tuples are the expected quote-only and post-retirement
        # split views. Both include actual VWAP, share volume and trade count.
        raw_fields = _fields(raw)
        split_fields = _fields(scaled(raw, scale))
        for mode, bars in rows.items():
            current = bars[i]
            if mode != "raw" and _fields(current) != (split_fields if mode in _PAIRS else raw_fields):
                raise UnsupportedInterval("retained views include an independent price or share adjustment")
            if mode in _PAIRS:
                base = rows[_PAIRS[mode]][i]
                current = {**current, **{field: base[field] for field in (*_PRICES, "volume")}}
            bundles[mode][raw["session"]] = current
    return bundles, dict(
        first=info["first"],
        cut=info.get("cut") or info["separation"],
        witness_end=info["review"],
        symbol=symbol,
        sessions=len(dates),
        witnesses=[info["last"], info["review"]],
        raw_revisions=[],
        repair_kind="retired_unit",
        retired_unit_scope={key: info[key] for key in ("symbol", "occupation_token", "terminal_action_id")},
        split_factor=info["factor"],
        reason="later_split_was_applied_to_an_extinguished_class",
    )
