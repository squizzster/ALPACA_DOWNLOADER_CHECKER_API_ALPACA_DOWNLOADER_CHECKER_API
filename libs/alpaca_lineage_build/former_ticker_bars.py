"""Observe only selected literal daily market-data routes for fresh group inference."""

from copy import copy
from datetime import date, timedelta

from libs.alpaca_data_api_solver import instant, iso
from libs.alpaca_pricing_download.contract import FIELDS, BarsSymbolUnavailable, DownloadError, failure_outcome
from libs.alpaca_pricing_download.responses import fetch_rows

from .contract import LineageError


def observe_literal_bar(pricing, symbol, session):
    start = instant(session)
    end = instant((date.fromisoformat(session) + timedelta(days=1)).isoformat())
    request = {
        "symbol": symbol,
        "session": session,
        "asof": "-",
        "feed": "sip",
        "adjustment": "raw",
        "timeframe": "1Day",
        "start": iso(start),
        "end_exclusive": iso(end),
    }
    try:
        rows = fetch_rows(pricing, symbol, "raw", start, end, asof="-")
    except DownloadError as error:
        status = failure_outcome(error)["http_status"]
        if isinstance(error, BarsSymbolUnavailable) or status in (403, 404):
            return {**request, "outcome": "unavailable", "http_status": status}
        raise
    if not rows:
        return {**request, "outcome": "no_bars", "http_status": 200}
    if len(rows) != 1:
        raise LineageError(f"{symbol}: more than one daily raw bar for {session}")
    return {**request, "outcome": "observed", "http_status": 200, "bar": dict(zip(FIELDS, rows[0], strict=True))}


def complete_literal_edges(store, inputs, evidence, previous, pricing, provider, completion):
    """The source-generation group selects once and keeps completed days across peer failures."""
    from libs.alpaca_end_of_day_security_correlation.former_ticker import (
        complete_source_scope,
        literal_edge_requests,
    )

    if "former_ticker_literal_evidence" in completion:
        return completion["former_ticker_literal_evidence"]
    if "former_ticker_literal_plan" not in completion:
        completion["former_ticker_literal_plan"] = (
            literal_edge_requests(inputs, evidence)
            if all(previous.get(name) is None for name in inputs) and complete_source_scope(inputs)
            else {}
        )

    reports = {}
    for (former, terminal), sessions in completion["former_ticker_literal_plan"].items():
        generations = {name: getattr(inputs[name], "source_sha256", None) for name in (former, terminal)}
        asofs = {name: inputs[name].source["latest_request"].get("asof") for name in (former, terminal)}
        if not all(generations.values()) or not all(asofs.values()):
            continue
        former_binding = str(store), generations[former], asofs[former]
        observations = [
            provider.get_literal_bar_observation(pricing, former, day, binding=former_binding)
            for day in sessions
        ]
        if former not in reports:
            # Add group-bound observations to a new receipt view. Earlier
            # metadata and its accepted dividend selection remain reusable.
            reports[former] = copy(evidence[former])
            reports[former]["former_ticker_literal_bars"] = {}
        reports[former]["former_ticker_literal_bars"][terminal] = {
            "source_generations": generations,
            "source_asofs": asofs,
            "observations": observations,
        }
    completion["former_ticker_literal_evidence"] = reports
    return reports
