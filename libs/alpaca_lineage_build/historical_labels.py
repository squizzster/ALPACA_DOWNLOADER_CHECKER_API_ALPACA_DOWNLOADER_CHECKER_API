"""Project a former trading label selected from literal prices under frozen ownership."""

from datetime import date
from uuid import UUID

from . import price_basis_limits
from .contract import LineageError, RecoveryRequired
from .dated_price_publication import action_window, publish_selection
from .metadata import action_symbols

FIELD = "historical_label_intervals"
TOKENS = "historical_label_tokens"


def _valid(pid, item):
    try:
        change = item["change"]
        dates = [change[k] for k in ("prior_session", "first", "last", "next_session")] + [item["witness_end"]]
        return (
            item["schema_version"] == 1
            and item["public_id"] == pid
            and len(pid) == 64
            and all(c in "0123456789abcdef" for c in pid)
            and item["source_decision"]
            and UUID(hex=item["token"]).hex == item["token"]
            and UUID(hex=item["basis_token"]).hex == item["basis_token"]
            and (
                "occupation" not in item
                or isinstance(item["occupation"]["locator"], str)
                and item["occupation"]["locator"]
                and item["occupation"]["public_id"] != pid
                and len(item["occupation"]["public_id"]) == 64
                and all(c in "0123456789abcdef" for c in item["occupation"]["public_id"])
                and item["cut"] == change["next_session"]
            )
            and all(date.fromisoformat(v).isoformat() == v for v in dates)
            and dates[0] < dates[1] <= dates[2] < dates[3] < dates[4]
            and ("occupation" in item or dates[1] <= item["cut"] <= dates[2])
            and isinstance(change["new"], str)
            and change["new"]
            and isinstance(change["old"], str)
            and change["old"]
            and change["old"] != change["new"]
            and item["members"]
            and item["members"] == sorted(set(item["members"]))
            and item["unavailable"] == sorted(set(item["unavailable"]))
            and not set(item["members"]).intersection(item["unavailable"])
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def accept(receipt, entry):
    selected = receipt.get(FIELD, {})
    if (
        not isinstance(selected, dict)
        or any(not _valid(pid, x) or receipt["symbol"] not in x["members"] for pid, x in selected.items())
        or TOKENS in entry
        and entry[TOKENS] != {k: v["token"] for k, v in selected.items()}
    ):
        raise RecoveryRequired("invalid historical trading-label selection")


def _basis(receipt, item):
    value = receipt.get(price_basis_limits.FIELD, {}).get(item["public_id"] + ":" + item["cut"], {})
    interval = receipt.get("dated_price_intervals", {}).get(item["public_id"])
    return (
        "occupation" in item or value.get("token") == item["basis_token"] and not value.get("pending")
    ) and item.get("numerical_token") == price_basis_limits.numerical_binding(
        interval, item["change"]["first"], item["witness_end"]
    )


def selected(receipts, unavailable):
    result = {}
    if not receipts:
        return result
    for pid, item in next(iter(receipts.values())).get(FIELD, {}).items():
        if (
            set(item["members"]) == receipts.keys()
            and set(item["unavailable"]) == set(unavailable)
            and all(
                r.get(FIELD, {}).get(pid, {}).get("token") == item["token"] and _basis(r, item)
                for r in receipts.values()
            )
        ):
            result[pid] = item
    return result


def continue_closed(previous, receipt, first_changed):
    if previous.get(FIELD):
        receipt[FIELD] = {
            pid: i
            for pid, i in previous[FIELD].items()
            if first_changed is None or first_changed > i["witness_end"]
        }


def retain(previous, receipt, *, continued=False):
    old = (previous or {}).get(FIELD, {})
    carried = receipt.pop(FIELD, {})
    if any(old.get(pid) != item for pid, item in carried.items()):
        raise LineageError("historical label decisions require their owning publication")
    if not old:
        return
    same = (
        previous["boundaries"] == receipt["boundaries"] and previous["source_sha256"] == receipt["source_sha256"]
    )
    for pid, item in old.items():
        if not (same or continued and pid in carried) or not _basis(receipt, item):
            continue
        if (
            set(item["members"]) != receipt["input_sources"].keys()
            or set(item["unavailable"]) != receipt.get("unavailable_pricing", {}).keys()
        ):
            continue
        if previous["evidence"] is not receipt["evidence"]:
            changed = {item["change"]["old"], item["change"]["new"]}

            def relevant(e):
                return sorted(
                    (
                        a
                        for a in action_window(e, item["change"]["first"], item["witness_end"])
                        if action_symbols(a) & changed
                    ),
                    key=lambda a: a.get("id", ""),
                )

            if relevant(previous["evidence"]) != relevant(receipt["evidence"]):
                continue
        receipt.setdefault(FIELD, {})[pid] = item


class LabelledAssignments(dict):
    """A selected group whose raw ownership and dated display labels have separate producers."""

    def __init__(self, assignments, projection):
        super().__init__(assignments)
        self.historical_labels = projection


def with_labels(assignments, receipts, unavailable):
    labels = selected(receipts, unavailable)
    return LabelledAssignments(assignments, labels) if labels else assignments


def project_views(views, change):
    """Transfer only the selected label. Preserve genuine per-view ambiguity and aliases."""
    from bisect import bisect_left, bisect_right
    from operator import itemgetter

    replaced = {}
    session = itemgetter("session")
    for mode, view in list(views.items()):
        rows = view["bars"]
        key = id(rows)
        if key not in replaced:
            lo = bisect_left(rows, change["first"], key=session)
            hi = bisect_right(rows, change["last"], lo=lo, key=session)
            if lo == hi:
                replaced[key] = rows
            else:
                affected = [
                    {**b, "tickers": sorted({change["new"] if t == change["old"] else t for t in b["tickers"]})}
                    for b in rows[lo:hi]
                ]
                replaced[key] = rows[:lo] + affected + rows[hi:]
        views[mode] = {**view, "bars": replaced[key]}
    return views


def _review(store, pid, cut, data, client, _):
    from contextlib import ExitStack
    from uuid import uuid4

    from libs.alpaca_pricing_download import historical_labels as prices
    from libs.alpaca_pricing_download.price_basis_review import review_steps, selected_historical_route
    from libs.alpaca_serving_catalogue import export_inputs
    from libs.alpaca_serving_catalogue import storage as catalogue

    security = data.get_security(pid)
    record = data._records[pid]
    group = data._groups[record["group_key"]]
    refs = group["references"]
    pending = export_inputs.select(store, data.catalog_generation(), set(refs["lineage"]), recover_lineage=True)

    def result(status, reason):
        return None, dict(public_id=pid, cut=cut, status=status, reason=reason)

    established = record["public"].get("historical_label_interval")
    if established:
        if pending:
            raise RecoveryRequired("changed lineage requires owning label-publication recovery")
        return result(
            "already_projected" if established["cut"] == cut else "unsupported",
            "a dated label interval is already selected",
        )
    basis = record["public"].get(price_basis_limits.FIELD, {}).get(cut)
    if not basis or basis.get("pending"):
        return result(
            "unsupported", "the original historical ticker review is missing or requires owning recovery"
        )
    # A separate current owner can reuse a historical ticker. Bind its actual
    # catalogue selector during acquisition/recovery, without reading its prices.
    peer = catalogue.ticker_group_token(store, data.catalog_generation(), basis["symbol"], record["group_key"])
    bind = dict(
        **({"peer": peer} if peer else {}),
        kind="historical_label",
        version=1,
        store=str(store),
        public_id=pid,
        cut=cut,
        source_decision=group["decision_generation"],
        references=refs,
        basis_token=basis["token"],
    )
    borrowed = {}
    with ExitStack() as stack:
        steps = stack.enter_context(review_steps(store, bind, completed_only=bool(pending)))
        if not steps:
            raise RecoveryRequired("changed lineage requires owning label-publication recovery")
        get, put = steps
        verdict = get("decision")
        if pending and not verdict:
            raise RecoveryRequired("another lineage publication changed the selected owner")
        if verdict is None:
            route = get("route")
            if route is None:
                route = put("route", {"symbol": selected_historical_route(store, basis)})
            published = route["symbol"]
            if not published or published == basis["symbol"]:
                verdict = {
                    "outcome": "unsupported",
                    "reason": "a completed historical literal route is required",
                }
            else:
                snapshot = None
                io_cache = {}

                def load(lo, hi):
                    nonlocal snapshot
                    if snapshot is None:
                        snapshot = stack.enter_context(
                            data._reader._open_group(
                                group["locator"], expected=refs, export_selection="acquisition"
                            )
                        )
                        borrowed.update(snapshot._selected_receipts)
                    return snapshot.bounded_acquisition_views(pid, lo, hi, modes=("raw",), cache=io_cache)[
                        "raw"
                    ]["bars"]

                def other():
                    ranges = get("peer_ranges")
                    if ranges is None:
                        ranges = put(
                            "peer_ranges",
                            catalogue.other_ticker_ranges(
                                store, basis["symbol"], pid, record["group_key"], peer
                            ),
                        )
                    return ranges

                verdict = prices.assess(
                    basis, published, security.last_session.isoformat(), load, other, client, get, put
                )
            verdict = put("decision", {**verdict, "token": uuid4().hex})
    if verdict["outcome"] != "projected":
        return result("unsupported", verdict["reason"])
    item = dict(
        schema_version=1,
        public_id=pid,
        cut=cut,
        change=verdict["change"],
        witness_end=verdict["witness_end"],
        token=verdict["token"],
        basis_token=basis["token"],
        numerical_token=price_basis_limits.numerical_binding(
            record["public"].get("dated_price_interval"), verdict["change"]["first"], verdict["witness_end"]
        ),
        source_decision=group["decision_generation"],
        members=sorted(refs["pricing"]),
        unavailable=sorted(set(refs["lineage"]) - set(refs["pricing"])),
    )
    if not _valid(pid, item):
        raise LineageError("invalid historical label interval")
    publish_selection(store, group, item, pid, FIELD, TOKENS, previous=borrowed or None, pending_lineage=pending)
    return group["locator"], dict(public_id=pid, cut=cut, status="projected", assessment=item)


def review_many(store, public_ids, sessions, client):
    from .price_basis_review import selected_many

    return selected_many(store, public_ids, sessions, client, _review)
