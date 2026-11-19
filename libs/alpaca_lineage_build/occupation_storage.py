"""Completed scopes for displaced ticker occupations and their selected literal raw evidence."""

from datetime import date
from pathlib import Path
from uuid import UUID

from libs.alpaca_pricing_download.contract import symbol_label

from .acquisition import unavailable_pricing
from .contract import RecoveryRequired

FIELD = "dated_occupations"
TOKENS = "dated_occupation_tokens"
PIVOTS = "dated_occupation_pivots"
MARKER = "selected_occupation_repair"


def accept(receipt, entry):
    try:
        decisions = receipt.get(FIELD, {})
        for pid, item in decisions.items():
            dates = [item[k] for k in ("prior_first_trade", "last_prior_trade", "first", "current_asof")]
            members, unavailable, pricing = item["members"], item["unavailable"], item["source_pricing"]
            revisions = item["price_revision_dates"]
            if (
                item["version"] != 1
                or item["public_id"] != pid
                or len(pid) != 64
                or any(c not in "0123456789abcdef" for c in pid)
                or UUID(hex=item["token"]).hex != item["token"]
                or not all(date.fromisoformat(d).isoformat() == d for d in dates)
                or not dates[0] <= dates[1] < dates[2] <= dates[3]
                or item["prior_asof"] != dates[1]
                or any(symbol_label(n) != n for n in (*members, *unavailable, item["current_symbol"]))
                or members != sorted(set(members))
                or unavailable != sorted(set(unavailable))
                or set(members).intersection(unavailable)
                or set(pricing) != set(members)
                or any(len(v) != 64 or any(c not in "0123456789abcdef" for c in v) for v in pricing.values())
                or UUID(hex=item["source_decision"]).hex != item["source_decision"]
                or revisions != sorted(set(revisions))
                or any(d < dates[2] or d > dates[3] or date.fromisoformat(d).isoformat() != d for d in revisions)
                or symbol_label(item["symbol"]) != item["symbol"]
                or item["locator"] != min(item["members"])
                or receipt["symbol"] not in item["members"]
                or item["reason"]
                not in {
                    "current_route_excludes_previous_trades",
                    "historical_route_keeps_trading_against_different_later_tape",
                    "reissued_unit_and_distinct_dated_common",
                    "terminal_unit_separation_and_distinct_later_common",
                }
            ):
                raise ValueError
        # A selected export or acquisition snapshot holds the receipt generation alone.
        # A full index entry additionally binds the individual occupation tokens.
        if entry.keys() != {"sha256"} and entry.get(TOKENS, {}) != {
            pid: item["token"] for pid, item in decisions.items()
        }:
            raise ValueError
    except (ValueError, TypeError, KeyError, AttributeError) as err:
        raise RecoveryRequired("invalid selected dated ticker occupation") from err


def payload_name(item):
    """Use only the member name and token selected by the completed owner."""
    return f"{symbol_label(item['locator'])}/occupation.{UUID(hex=item['token']).hex}.json"


def earlier_rows(store, descriptor):
    from .storage import path_at, read_document

    if descriptor.get("retained_side") == "prior":
        raise RecoveryRequired("the retained earlier unit is not a displaced ticker occupation")
    document = read_document(path_at(Path(store).absolute() / "end_of_day/lineage", payload_name(descriptor)))
    if (
        document.get("token") != descriptor["token"]
        or document.get("boundary") != "alpaca_dated_occupation_literal_rows"
    ):
        raise RecoveryRequired("selected prior occupation observations are unavailable")
    return document["raw"]


def carry(previous, receipt, *, continued=False):
    old = (previous or {}).get(FIELD, {})
    if not old:
        return

    def closed(item):
        return item["first"][:4] < receipt["anchor_month"][:4]

    closed_same = previous["boundaries"]["completed_years"] == receipt["boundaries"]["completed_years"]
    same = (
        previous["boundaries"] == receipt["boundaries"] and previous["source_sha256"] == receipt["source_sha256"]
    )
    for item in old.values():
        if item.get("retained_side") == "prior" and (
            set(item["members"]) != receipt["input_sources"].keys()
            or set(item["unavailable"]) != set(unavailable_pricing(receipt))
        ):
            raise RecoveryRequired("unit retirement group changed; selected occupation recovery required")
    applied = receipt.pop("applied_unit_retirements", {})
    held = {
        pid: item
        for pid, item in old.items()
        if set(item["members"]) == receipt["input_sources"].keys()
        and set(item["unavailable"]) == set(unavailable_pricing(receipt))
        and (
            same
            or continued
            and closed(item)
            or closed_same
            and closed(item)
            or item.get("retained_side") == "prior"
            and applied.get(pid) == item["token"]
        )
    }
    if held:
        receipt[FIELD] = held
    else:
        receipt.pop(FIELD, None)


def index_fields(receipt, previous_entry):
    items = receipt.get(FIELD, {})
    fields = {TOKENS: {pid: item["token"] for pid, item in items.items()}} if items else {}
    pivots = {
        pid: item["last_prior_trade"]
        for pid, item in items.items()
        if receipt["symbol"] == item["locator"] and item.get("retained_side") != "prior"
    }
    if pivots or previous_entry and previous_entry.get(PIVOTS):
        fields[PIVOTS] = pivots
    return fields
