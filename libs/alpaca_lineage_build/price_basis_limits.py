"""Retain registered decisions about the first usable adjusted-price date independently of ownership."""

from datetime import date
from uuid import UUID

from libs.alpaca_pricing_download.product_quote import PROXY
from libs.alpaca_pricing_download.untraded_split import UNPRICED

from .contract import LineageError, RecoveryRequired
from .dated_price_publication import action_window, publish_selection
from .metadata import action_symbols

FIELD = "price_basis_limits"
TOKENS = "price_basis_limit_tokens"
RESTRICT = "unresolved_pre_event_adjusted_basis"
RESTORED = "price_basis_resolved"


def _valid(key, item):
    try:
        pid = item["public_id"]
        dates = [item[k] for k in ("witness_start", "previous_session", "cut", "witness_end")]
        return (
            key == pid + ":" + item["cut"]
            and len(pid) == 64
            and all(c in "0123456789abcdef" for c in pid)
            and UUID(hex=item["token"]).hex == item["token"]
            and type(item["schema_version"]) is int
            and item["schema_version"] == 1
            and (
                item.get("numerical_token") is None
                or UUID(hex=item["numerical_token"]).hex == item["numerical_token"]
            )
            and item["reason"] in (RESTRICT, RESTORED, PROXY, UNPRICED)
            and (
                "restored_review" not in item
                or (item["reason"] == RESTORED and item["restored_review"] in (PROXY, UNPRICED))
            )
            and (
                item["reason"] != UNPRICED
                and item.get("restored_review") != UNPRICED
                or (
                    isinstance(item["excluded_sessions"], list)
                    and item["excluded_sessions"]
                    and item["excluded_sessions"][0] == item["cut"]
                    and type(item["terminal"]) is bool
                    and (
                        item["excluded_sessions"][-1] == item["witness_end"]
                        if item["terminal"]
                        else item["excluded_sessions"][-1] < item["witness_end"]
                    )
                    and item["excluded_sessions"] == sorted(set(item["excluded_sessions"]))
                    and all(date.fromisoformat(d).isoformat() == d for d in item["excluded_sessions"])
                )
            )
            and type(item.get("pending", False)) is bool
            and all(date.fromisoformat(v).isoformat() == v for v in dates)
            and dates[0] <= dates[1] < dates[2] <= dates[3]
            and (
                dates[2] < dates[3]
                or UNPRICED in (item["reason"], item.get("restored_review"))
                and item["terminal"]
            )
            and date.fromisoformat(item.get("original_first_session", dates[0])).isoformat() <= dates[0]
            and isinstance(item["source_decision"], str)
            and bool(item["source_decision"])
            and isinstance(item["symbol"], str)
            and bool(item["symbol"])
            and all(isinstance(symbol, str) and symbol for symbol in (*item["members"], *item["unavailable"]))
            and item["members"] == sorted(set(item["members"]))
            and item["members"]
            and item["unavailable"] == sorted(set(item["unavailable"]))
            and not set(item["members"]).intersection(item["unavailable"])
        )
    except (TypeError, ValueError, KeyError, AttributeError):
        return False


def accept(receipt, entry):
    limits = receipt.get(FIELD, {})
    if not isinstance(limits, dict) or any(
        not _valid(k, v) or receipt["symbol"] not in v["members"] for k, v in limits.items()
    ):
        raise RecoveryRequired("invalid adjusted-price coverage selection")
    if TOKENS in entry and entry[TOKENS] != {k: v["token"] for k, v in limits.items()}:
        raise RecoveryRequired("missing declared adjusted-price coverage selection")


def publish(store, group, selection, **kwargs):
    refs = group["references"]
    item = {
        **selection,
        "schema_version": 1,
        "members": sorted(refs["pricing"]),
        "unavailable": sorted(set(refs["lineage"]) - set(refs["pricing"])),
    }
    key = item["public_id"] + ":" + item["cut"]
    if (
        not _valid(key, item)
        or item["source_decision"] != group["decision_generation"]
        or any(refs["lineage"][name] is not None for name in item["unavailable"])
    ):
        raise LineageError("invalid adjusted-price coverage decision")
    return publish_selection(store, group, item, key, FIELD, TOKENS, **kwargs)


def numerical_binding(interval, first, last):
    if interval and interval["first"] <= last and interval["cut"] > first:
        return interval["token"]
    return None


def group_limits(receipts, unavailable, intervals):
    if not receipts:
        return {}
    sample = next(iter(receipts.values())).get(FIELD, {})
    selected = {}
    for key, item in sample.items():
        if not (set(item["members"]) == receipts.keys() and set(item["unavailable"]) == set(unavailable)):
            continue
        current = [r.get(FIELD, {}).get(key) for r in receipts.values()]
        if all(v and v["token"] == item["token"] for v in current):
            selected.setdefault(item["public_id"], {})[item["cut"]] = {
                **item,
                "pending": any(v.get("pending") for v in current)
                or item.get("numerical_token")
                != numerical_binding(
                    intervals.get(item["public_id"]), item["witness_start"], item["witness_end"]
                ),
            }
    return selected


def continue_closed(previous, receipt, first_changed):
    limits = previous.get(FIELD)
    if limits:
        receipt[FIELD] = {
            k: v
            if first_changed is None
            or not (PROXY in (v["reason"], v.get("restored_review")) or v.get("terminal"))
            and first_changed > v["witness_end"]
            else {**v, "pending": True}
            for k, v in limits.items()
        }


def _actions(evidence, item):
    bounds = (
        (item["cut"], item["cut"])
        if item["reason"] == PROXY or item.get("restored_review") == PROXY
        else (item["cut"] if item["reason"] == UNPRICED else item["witness_start"], item["witness_end"])
    )
    actions = [
        a
        for a in action_window(evidence, *bounds)
        if {item["symbol"], item.get("published_symbol")} & action_symbols(a)
    ]
    return sorted(actions, key=lambda a: a.get("id") or repr(sorted(a.items())))


def retain(previous, receipt, *, continued=False):
    old = (previous or {}).get(FIELD, {})
    if not old:
        if receipt.get(FIELD):
            raise LineageError("price coverage decisions require their owning publication")
        return
    proposed = receipt.get(FIELD, {})
    if any(
        key not in old or item not in (old[key], {**old[key], "pending": True}) for key, item in proposed.items()
    ):
        raise LineageError("price coverage decisions require their owning publication")
    same = (
        previous["boundaries"] == receipt["boundaries"] and previous["source_sha256"] == receipt["source_sha256"]
    )
    limits = {}
    for key, item in old.items():
        if (
            set(item["members"]) != receipt["input_sources"].keys()
            or set(item["unavailable"]) != receipt.get("unavailable_pricing", {}).keys()
        ):
            continue
        selected = receipt.get(FIELD, {}).get(key, {}) if continued else {}
        pending = item.get("pending") or not same and (not continued or not selected or selected.get("pending"))
        if not pending and previous.get("evidence") is not receipt.get("evidence"):
            pending = _actions(previous["evidence"], item) != _actions(receipt["evidence"], item)
        if (
            not pending
            and PROXY in (item["reason"], item.get("restored_review"))
            and (
                previous.get("evidence") is not receipt.get("evidence")
                or previous.get("identity_memberships") != receipt.get("identity_memberships")
            )
        ):
            from libs.alpaca_pricing_download.product_quote import assets

            pending = assets(previous, item["public_id"], item["symbol"]) != assets(
                receipt, item["public_id"], item["symbol"]
            )
        limits[key] = {**item, "pending": True} if pending else item
    if limits:
        receipt[FIELD] = limits
    else:
        receipt.pop(FIELD, None)
