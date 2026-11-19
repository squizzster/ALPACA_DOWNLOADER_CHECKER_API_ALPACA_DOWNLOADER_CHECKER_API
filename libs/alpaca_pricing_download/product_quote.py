"""Identify native NAV-relative quotes that cannot supply dollar-price OHLC."""

from decimal import Decimal

from libs.alpaca_security_lineage.inference_evidence import reject_assertions

from .carried_rename import _traded
from .dated_rename import UnsupportedInterval, _matches, provider_rows
from .rename_split_basis import _SPLITS, _factor

PROXY = "nav_relative_quote_not_a_dollar_price"


def assets(receipt, public_id, symbol):
    ids = {
        uid
        for item in receipt.get("identity_memberships", ())
        if item["security_id"] == public_id
        for uid in item["member_alpaca_asset_ids"]
    }
    observed = set()
    for lookup in receipt.get("evidence", {}).get("asset_lookups", ()):
        asset = lookup.get("asset") or {}
        if asset.get("id") not in ids or asset.get("symbol") != symbol:
            continue
        reject_assertions(lookup, where="product price convention")
        observed.add(
            (asset["id"], asset.get("name") or "", asset.get("class") or "", asset.get("exchange") or "")
        )
    return sorted(observed)


def names_proxy(assets_):
    return len(assets_) == 1 and assets_[0][1].casefold().split()[-1:] == ["nextshares"]


def candidate(assets_, actions, symbol, cut, *, recheck=False):
    if len(assets_) != 1:
        raise UnsupportedInterval("price convention lacks an exclusive asset observation")
    _, _, kind, exchange = assets_[0]
    if kind != "us_equity" or exchange != "NASDAQ" or (not recheck and not names_proxy(assets_)):
        raise UnsupportedInterval(
            "native asset type and product name do not identify a NAV-relative exchange listing"
        )
    if (
        len(actions) != 1
        or actions[0].get("action_type") not in _SPLITS
        or actions[0].get("symbol") != symbol
        or actions[0].get("ex_date") != cut
        or actions[0].get("new_symbol") not in (None, "", symbol)
    ):
        raise UnsupportedInterval("product convention lacks an exclusive same-label split")
    factor = _factor(actions[0])
    if Decimal("0.75") < factor < Decimal("1.25"):
        raise UnsupportedInterval("reported share change does not distinguish the quote convention")
    return factor


def plan(raw, split_pair, symbol, cut, factor, *, recheck=False, named_proxy=True):
    if not raw or any(row["tickers"] != [symbol] for row in raw):
        raise UnsupportedInterval("product quote history lacks an exclusive dated label")
    trades = [r for r in raw if _traded(r)]
    before = [r for r in trades if r["session"] < cut]
    after = [r for r in trades if r["session"] >= cut]
    if len(before) < 2 or len(after) < 2:
        raise UnsupportedInterval("insufficient eligible trades across the share change")
    proxy_history = all(
        abs(Decimal(r[field]) - 100) <= Decimal("0.50") for r in raw for field in ("low", "high", "close")
    )
    if proxy_history and not named_proxy:
        raise UnsupportedInterval("new instrument evidence no longer confirms the observed proxy convention")
    if not proxy_history:
        # A feed can replace proxy marks with completed dollar trades. Only
        # reinstate a full history when the old proxy regime has disappeared
        # and the independently reported change in share units is realized.
        away = all(Decimal(b["high"]) < Decimal("99.50") or Decimal(b["low"]) > Decimal("100.50") for b in raw)
        realized = all(
            Decimal("0.75")
            <= Decimal(after[0][field]) / Decimal(before[-1]["close"]) / factor
            <= Decimal("1.25")
            for field in ("close", "vwap")
        )
        if not recheck or not away or not realized:
            raise UnsupportedInterval(
                "history contains prices inconsistent with one resolved quotation convention"
            )
    if len({Decimal(r["close"]) for r in trades}) < 2:
        raise UnsupportedInterval("a constant mark does not establish a traded premium or discount")
    index = next((i for i, r in enumerate(raw) if r["session"] == cut), None)
    if not index or len(split_pair) != 2:
        raise UnsupportedInterval("split lacks both observed price bases")
    prev, pivot = raw[index - 1 : index + 1]
    if [r["session"] for r in split_pair] != [prev["session"], cut] or any(
        r["tickers"] != [symbol] for r in split_pair
    ):
        raise UnsupportedInterval("split basis is not on the selected instrument and dates")
    move = (Decimal(split_pair[0]["close"]) / Decimal(prev["close"])) / (
        Decimal(split_pair[1]["close"]) / Decimal(pivot["close"])
    )
    if not Decimal("0.98") <= move / factor <= Decimal("1.02"):
        raise UnsupportedInterval("the adjustment views do not show the reported split")
    return dict(
        outcome="check_product_tape",
        symbol=symbol,
        cut=cut,
        previous_session=prev["session"],
        witness_start=raw[0]["session"],
        witness_end=raw[-1]["session"],
        native_traded_sessions=len(trades),
        **({"restored_dollars": True} if not proxy_history else {}),
        factor=str(factor),
        witnesses=[*before[-2:], *after[:2]],
    )


def confirm(selected, client, get, put):
    for witness in selected["witnesses"]:
        day = witness["session"]
        key = "literal_product:" + day
        accepted = get(key)
        if accepted is None:
            observation = provider_rows(client, selected["symbol"], "-", "raw", day, day)
            accepted = put(key, day in observation and _matches(observation[day], witness))
        if not accepted:
            return {
                **selected,
                "outcome": "unsupported",
                "reason": "literal trades disagree with the proposed product convention",
            }
    if selected.get("restored_dollars"):
        from .price_basis_review import RESTORED

        return {**selected, "outcome": "restored", "reason": RESTORED}
    return {**selected, "outcome": "restricted", "reason": PROXY}
