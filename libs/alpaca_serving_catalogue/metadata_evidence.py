"""Identity-scoped public metadata from the group's completed native observations.

The caller supplies all online records and all selected receipt bodies for one
group. No storage reads, source reconstruction or new ownership decisions occur
here. A source ticker, dependency, or provider's mapped response label alone
cannot bind an observation to a security.
"""

import json
from collections import defaultdict

from libs.alpaca_data_api_solver import instant
from libs.alpaca_security_lineage.inference_evidence import ASSERTION_FIELDS


def _single(values):
    return next(iter(values)) if len(values) == 1 else None


def _snapshots(evidence, members):
    for lookup in evidence.get("asset_lookups", ()):
        asset = lookup.get("asset")
        if (
            not asset
            or lookup["lookup_kind"] != "symbol"
            or lookup["lookup"] != asset["symbol"]
            or lookup.get("source") not in (None, "asset_catalogue")
        ):
            continue
        owner = _single(members.get(asset["id"], ()))
        fields = {
            public: asset[key]
            for key, public in (
                ("class", "asset_class"),
                ("name", "name"),
                ("exchange", "exchange"),
                ("status", "status"),
            )
            if asset.get(key)
        }
        if isinstance(asset.get("tradable"), bool):
            fields["tradable"] = asset["tradable"]
        if owner and fields:
            observed = lookup.get("observed_at_utc")
            if observed is None and lookup.get("source") == "asset_catalogue":
                observed = evidence.get("catalogue_observed_at_utc")
            yield owner, {"symbol": asset["symbol"], "observed_at": observed, **fields}


def _literal_raw_owners(receipt, sample, session):
    """Use a selected raw row only within the literal price request that acquired it."""
    request = receipt.get("pricing_request") or {}
    symbol = sample["symbol"]
    if (
        request.get("asof") != "-"
        or request.get("symbol") != symbol
        or sample["returned_symbol"] != symbol
        or request.get("timeframe") != "1Day"
        or request.get("feed") != sample["feed"]
        or not request.get("start")
        or not request.get("end_exclusive")
    ):
        return set()
    start, end = instant(request["start"]), instant(request["end_exclusive"])
    if not start <= instant(sample["trade"]["t"]) < end:
        return set()
    # current_year can also contain the active month. Only the owning boundary
    # supplies the selected assignment for that session.
    anchor = receipt.get("anchor_month")
    if not anchor or session[:7] > anchor:
        return set()
    kind = (
        "completed_years"
        if session[:4] < anchor[:4]
        else ("current_month" if session[:7] == anchor else "current_year")
    )
    choices = receipt.get("assignment_handoffs", {}).get(kind, {}).get("observation_choices")
    if not choices:
        return set()
    owners = set()
    raw = choices["partitions"][choices["views"]["raw"]]
    for run in raw["runs"]:
        if run["state"] != "selected" or run["branch"] != "correlated" or symbol not in run["tickers"]:
            continue
        # Only endpoints name actual stored sessions. A date between endpoints
        # may be missing. The latest literal price request must also contain the
        # daily bar timestamp, not just a later intraday trade from that date.
        if session == run["from"]:
            stamp = run["first_timestamp"]
        elif session == run["to"]:
            stamp = run["last_timestamp"]
        else:
            continue
        if start <= instant(stamp) < end:
            owners.add(run["owner"])
    return owners


def _literal_owner(records, receipts, source_owners, sample, session):
    """Join the actual literal observation to a dated published label assignment."""
    symbol = sample["symbol"]
    dated, possible = set(), set()
    for pid, record in records.items():
        label_on_history = any(
            r["ticker"] == symbol and r["first_session"] <= session <= r["last_session"]
            for r in record["ticker_ranges"]
        )
        if label_on_history:
            possible.add(pid)
        for interval in record["ticker_ownership_intervals"]:
            first, end = interval["start_inclusive"], interval["end_exclusive"]
            if interval["ticker"] != symbol or first and session < first or end and session >= end:
                continue
            if first and end or label_on_history and (first or end):
                dated.add(pid)
                possible.add(pid)
    if len(possible) != 1:
        return None
    raw_owners = _literal_raw_owners(receipts.get(symbol, {}), sample, session)
    dated.update(raw_owners & possible)
    possible.update(raw_owners)
    if len(dated) != 1 or possible != dated:
        return None
    # Online selection alone would conceal a competing offline owner. Include
    # the source-local accepted contribution bounds for such rival labels.
    for source, receipt in receipts.items():
        owners = source_owners[source]
        if owners is None:
            return None
        offline = owners - records.keys()
        if offline and _offline_claim(receipt, offline, symbol, session):
            return None
    return _single(dated)


def _offline_claim(receipt, offline, symbol, session):
    if "identity_intervals" not in receipt:
        return True
    if not any(
        r["security_id"] in offline and r["from"] <= session <= r["to"] for r in receipt["identity_intervals"]
    ):
        return False
    episodes = (receipt.get("dated_market_projection") or {}).get("identities", {})
    for owner in offline:
        for period in episodes.get(owner, ()):
            end = period["end_exclusive"]
            if (
                period["symbol"] == symbol
                and period["start_inclusive"] <= session
                and (not end or session < end)
            ):
                return True
    # These are published label assignments, already held in the receipt.
    # Legacy bodies without this compact capability cannot exclude a rival.
    if not receipt.get("boundaries"):
        return True
    for kind in receipt["boundaries"]:
        handoff = receipt.get("assignment_handoffs", {}).get(kind, {})
        choices = handoff.get("observation_choices")
        if not choices:
            return True
        part = choices["partitions"][choices["views"]["raw"]]
        if any(
            run["owner"] in offline and run["from"] <= session <= run["to"] and symbol in run["tickers"]
            for run in part["runs"]
        ):
            return True
    return False


def _samples(evidence):
    """Yield the actual request that observed a trade, including range searches."""
    literal = evidence.get("historical_trade_tape", {})
    yield literal, None
    mapped = evidence.get("mapped_historical_trade_tape", {})
    asset_id = mapped.get("pricing_route_binding", {}).get("observed_symbol_asset_id")
    yield mapped, asset_id
    search = mapped.get("last_trade_search", {})
    if search:
        yield search.get("observation") or {}, asset_id
        if search["outcome"] == "observed":
            yield search.get("last_request") or {}, asset_id
    otc = evidence.get("otc_historical_trade_tape", {})
    yield otc, None
    following = otc.get("following_trade_search", {})
    if following.get("outcome") == "observed":
        yield following["last_request"], None
    retirement = evidence.get("terminal_cash_merger_trade", {})
    yield retirement, retirement.get("observed_symbol_asset_id")


def _facts(records, receipts, members, source_owners, complete_members):
    literal_owners = {}
    for receipt in receipts.values():
        evidence = receipt["evidence"]
        if complete_members:
            for owner, snapshot in _snapshots(evidence, members):
                yield owner, "asset_snapshots", snapshot
        # Retired authored envelopes predate the native tape and cash-retirement
        # producers. Their raw-shaped extra fields have no provider provenance.
        disposition = evidence.get("symbol_disposition") or {}
        if (
            "provider_corporate_actions" in evidence
            or any(evidence.get(field) for field in ASSERTION_FIELDS)
            or disposition.get("classification") == "exchange_test_symbol"
            or disposition.get("test_symbol_evidence")
        ):
            continue
        for sample, asset_id in _samples(evidence):
            if sample.get("outcome") != "observed":
                continue
            day = sample.get("observed_session") or sample["session"]
            if sample["asof"] == "-":
                key = sample["symbol"], day, sample["feed"], sample["trade"]["t"]
                if key not in literal_owners:
                    literal_owners[key] = _literal_owner(records, receipts, source_owners, sample, day)
                owner = literal_owners[key]
            else:
                # Mapped responses can relabel another period. The exact selected
                # source must already belong to the mapped UUID's identity here.
                owners = {
                    r["security_id"]
                    for r in receipt.get("identity_intervals", ())
                    if r["from"] <= day <= r["to"]
                }
                owner = _single(owners)
                if owner not in members.get(asset_id, ()):
                    continue
            if owner is not None:
                trade = sample["trade"]
                yield (
                    owner,
                    "trade_observations",
                    {
                        "session": day,
                        "trade_timestamp": trade["t"],
                        "feed": sample["feed"],
                        "request_symbol": sample["symbol"],
                        "asof": sample["asof"],
                        "returned_symbol": sample["returned_symbol"],
                        "tape_code": trade.get("z") or None,
                        "tape_network": sample.get("tape_network") or None,
                        "execution_venue": trade.get("x") or None,
                    },
                )
        retirement = evidence.get("terminal_cash_merger_trade")
        if complete_members and retirement and (terms := retirement.get("cash_merger")):
            owner = _single(members.get(retirement["observed_symbol_asset_id"], ()))
            if owner:
                yield (
                    owner,
                    "corporate_actions",
                    {
                        "action_type": "cash_merger",
                        **{
                            key: terms[key] for key in ("action_id", "role", "symbol", "cusip", "effective_date")
                        },
                        **({"process_date": terms["process_date"]} if terms.get("process_date") else {}),
                        "rate": str(terms["rate"]),
                    },
                )


_SORT_FIELDS = {
    "asset_snapshots": ("observed_at", "symbol"),
    "trade_observations": ("trade_timestamp", "request_symbol"),
    "corporate_actions": ("effective_date", "action_id"),
}


def project_group_evidence(records, receipts):
    """Project public facts for each online ID while retaining all competing memberships."""
    projected = {pid: {} for pid in records}
    if not records:
        return projected
    members = defaultdict(set)
    source_owners = {}
    known = set()
    for source, receipt in receipts.items():
        if "identity_memberships" not in receipt:
            # These older completed projections can establish an empty source or
            # refer to memberships already supplied by another source. They do
            # not authorize reconstructing a missing UUID set from asset lookups.
            if receipt.get("correlated_rows") == 0:
                source_owners[source] = set()
            elif "identity_intervals" in receipt:
                source_owners[source] = {r["security_id"] for r in receipt["identity_intervals"]}
            else:
                source_owners[source] = None
            continue
        source_owners[source] = set()
        for identity in receipt["identity_memberships"]:
            owner = identity["security_id"]
            source_owners[source].add(owner)
            known.add(owner)
            for asset in identity["member_alpaca_asset_ids"]:
                members[asset].add(owner)
    complete_members = all(owners is not None and owners <= known for owners in source_owners.values())
    facts = defaultdict(dict)
    for owner, section, fact in _facts(records, receipts, members, source_owners, complete_members):
        if owner in projected:
            encoded = json.dumps(fact, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            order = tuple(fact[key] or "" for key in _SORT_FIELDS[section])
            facts[owner, section][(*order, encoded)] = fact
    for (owner, section), entries in facts.items():
        projected[owner][section] = [entries[key] for key in sorted(entries)]
    return projected
