from hashlib import sha256
from typing import Any

from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_pricing_download.evidence import PublishedPricingEvidence

MARKET_DATA_SERIES_ID_SCHEME_VERSION = 1

ALPACA_ASSET_IDENTITY_AVAILABLE = "alpaca_asset_identity_available"
EXCHANGE_TEST_SYMBOL = "exchange_test_symbol"
HISTORICAL_MARKET_DATA_ONLY = "historical_market_data_only"
INACTIVE_EMPTY_ASSET_OUT_OF_SCOPE = "inactive_empty_asset_out_of_scope"
JUST_SKIP = "just_skip"
OTC_ASSET_OUT_OF_SCOPE = "otc_asset_out_of_scope"
UNSUPPORTED_EMPTY_SYMBOL = "unsupported_empty_symbol"
UNRESOLVED_SYMBOL_DISPOSITION = "unresolved_symbol_disposition"

RECURRING_PIPELINE_RUN = "run"
RECURRING_PIPELINE_SKIP = "skip"

_COMPLETE_ADJUSTMENT_SET = frozenset(
    {
        "all",
        "dividend",
        "dividend,spin-off",
        "raw",
        "spin-off",
        "split",
        "split,dividend",
        "split,spin-off",
    }
)


# This is deliberately an evidence-backed, exact-symbol snapshot rather than a
# pattern match. Exchange test symbols can share shapes with genuine tickers.
_NASDAQ_LISTED_TEST_SYMBOLS = frozenset(
    {"ZAZZT", "ZBZZT", "ZCZZT", "ZJZZT", "ZVZZT", "ZWZZT", "ZXYZ.A", "ZXZZT"}
)
_OTHER_EXCHANGE_LISTED_TEST_SYMBOLS = frozenset(
    {
        "ATEST",
        "ATEST.A",
        "ATEST.B",
        "ATEST.C",
        "CBO",
        "CBX",
        "CTEST",
        "CTEST.E",
        "CTEST.G",
        "CTEST.L",
        "CTEST.O",
        "CTEST.S",
        "CTEST.V",
        "IGZ",
        "MTEST",
        "NTEST",
        "NTEST.A",
        "NTEST.B",
        "NTEST.C",
        "ZBZX",
        "ZEXIT",
        "ZIEXT",
        "ZTEST",
        "ZVV",
        "ZXIET",
        "ZZZTA",
        "ZZZTE",
        "ZZZTT",
        "ZZZTX",
    }
)
KNOWN_EXCHANGE_TEST_SYMBOLS: dict[str, dict[str, Any]] = {
    **{
        symbol: {
            "source": "Nasdaq Symbol Directory: Nasdaq-Listed",
            "source_url": "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
            "verified_as_of": "2026-10-08",
            "test_issue": True,
        }
        for symbol in _NASDAQ_LISTED_TEST_SYMBOLS
    },
    **{
        symbol: {
            "source": "Nasdaq Symbol Directory: Other Exchange-Listed",
            "source_url": "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
            "verified_as_of": "2026-10-08",
            "test_issue": True,
        }
        for symbol in _OTHER_EXCHANGE_LISTED_TEST_SYMBOLS
    },
}


def market_data_series_identity(symbol: str) -> dict[str, Any]:
    """Identify an Alpaca symbol series without claiming a security identity."""
    canonical_input = f"alpaca\nus_equity\n{symbol}"
    return {
        "market_data_series_id": sha256(canonical_input.encode("utf-8")).hexdigest(),
        "market_data_series_id_scheme": {
            "kind": "sha256",
            "version": MARKET_DATA_SERIES_ID_SCHEME_VERSION,
            "canonical_input": "provider_newline_asset_class_newline_symbol",
        },
        "provider": "alpaca",
        "asset_class": "us_equity",
        "symbol": symbol,
        "is_security_identity": False,
    }


def _input_symbol_asset_lookup(
    symbol: str,
    asset_lookups: list[dict[str, Any]],
) -> dict[str, Any]:
    matches = [
        lookup
        for lookup in asset_lookups
        if isinstance(lookup, dict) and lookup.get("lookup_kind") == "symbol" and lookup.get("lookup") == symbol
    ]
    if len(matches) != 1:
        return {
            "outcome": "missing" if not matches else "ambiguous",
            "http_status": None,
        }
    lookup = matches[0]
    return {
        "outcome": lookup.get("outcome"),
        "http_status": lookup.get("http_status"),
    }


def _input_symbol_asset(
    symbol: str,
    asset_lookups: list[dict[str, Any]],
) -> dict[str, Any] | None:
    matches = [
        lookup.get("asset")
        for lookup in asset_lookups
        if isinstance(lookup, dict)
        and lookup.get("lookup_kind") == "symbol"
        and lookup.get("lookup") == symbol
        and lookup.get("outcome") == "found"
        and lookup.get("http_status") == 200
        and isinstance(lookup.get("asset"), dict)
    ]
    return matches[0] if len(matches) == 1 else None


def _local_market_data_evidence(local_report: dict[str, Any], symbol: str) -> dict[str, Any]:
    if "published_pricing" in local_report:
        try:
            published = PublishedPricingEvidence.from_report(local_report["published_pricing"])
            if published.symbol != symbol:
                raise ValueError("published pricing belongs to another symbol")
        except (ValueError, DownloadError):
            # An invalid current handoff cannot fall back to legacy filesystem flags.
            return _local_market_data_evidence({}, symbol)
        counts = dict(published.row_counts)
        return {
            "download_count": len(counts),
            "complete_row_count_evidence": True,
            "complete_request_scope_evidence": True,
            "complete_adjustment_coverage": True,
            "nonempty_download_count": sum(count > 0 for count in counts.values()),
            "maximum_rows": max(counts.values()),
            "feeds": [published.feed],
            "timeframes": [published.timeframe],
            "adjustments": sorted(counts),
        }
    downloads = local_report.get("downloads")
    if not isinstance(downloads, list):
        downloads = []
    row_counts = [
        download.get("rows")
        for download in downloads
        if isinstance(download, dict) and type(download.get("rows")) is int
    ]
    feeds = [
        download.get("feed")
        for download in downloads
        if isinstance(download, dict) and isinstance(download.get("feed"), str)
    ]
    timeframes = [
        download.get("timeframe")
        for download in downloads
        if isinstance(download, dict) and isinstance(download.get("timeframe"), str)
    ]
    adjustments = [
        download.get("adjustment")
        for download in downloads
        if isinstance(download, dict) and isinstance(download.get("adjustment"), str)
    ]
    complete_row_count_evidence = (
        bool(downloads)
        and len(row_counts) == len(downloads)
        and all(
            isinstance(download, dict)
            and download.get("sidecar_readable") is True
            and download.get("gzip_present") is True
            and download.get("stat_matches_sidecar") is True
            and download.get("pipeline_receipt_matches") is True
            for download in downloads
        )
    )
    return {
        "download_count": len(downloads),
        "complete_row_count_evidence": complete_row_count_evidence,
        "complete_request_scope_evidence": (
            bool(downloads) and len(feeds) == len(downloads) and len(timeframes) == len(downloads)
        ),
        "complete_adjustment_coverage": (
            len(adjustments) == len(downloads)
            and len(adjustments) == len(_COMPLETE_ADJUSTMENT_SET)
            and set(adjustments) == _COMPLETE_ADJUSTMENT_SET
        ),
        "nonempty_download_count": sum(row_count > 0 for row_count in row_counts),
        "maximum_rows": max(row_counts, default=None),
        "feeds": sorted(set(feeds)),
        "timeframes": sorted(set(timeframes)),
        "adjustments": sorted(set(adjustments)),
    }


def classify_symbol_disposition(
    symbol: str,
    local_report: dict[str, Any],
    asset_lookups: list[dict[str, Any]],
    identities: list[dict[str, Any]],
    input_symbol_occupants: list[dict[str, Any]],
) -> dict[str, Any]:
    """Classify recurring treatment without inventing missing asset identity."""
    lookup_evidence = _input_symbol_asset_lookup(symbol, asset_lookups)
    input_asset = _input_symbol_asset(symbol, asset_lookups)
    local_evidence = _local_market_data_evidence(local_report, symbol)
    usable_asset_ids = sorted(
        {
            identity["id"]
            for identity in identities
            if isinstance(identity, dict) and isinstance(identity.get("id"), str) and identity["id"]
        }
    )
    has_rows = bool(local_evidence["nonempty_download_count"])
    known_test_symbol_evidence = KNOWN_EXCHANGE_TEST_SYMBOLS.get(symbol)
    test_symbol_evidence = dict(known_test_symbol_evidence) if known_test_symbol_evidence is not None else None
    otc_scope_evidence = None
    if (
        isinstance(input_asset, dict)
        and input_asset.get("exchange") == "OTC"
        and isinstance(input_asset.get("id"), str)
        and input_asset["id"]
        and local_evidence["complete_row_count_evidence"]
        and local_evidence["complete_request_scope_evidence"]
        and local_evidence["feeds"] == ["sip"]
        and local_evidence["timeframes"] == ["1Day"]
        and not has_rows
    ):
        otc_scope_evidence = {
            "basis": "exact_alpaca_input_asset_exchange_and_authenticated_empty_sip_history",
            "alpaca_asset_id": input_asset["id"],
            "asset_class": input_asset.get("class"),
            "exchange": "OTC",
            "configured_feed": "sip",
            "timeframe": "1Day",
        }
    inactive_scope_evidence = None
    if (
        isinstance(input_asset, dict)
        and input_asset.get("class") == "us_equity"
        and input_asset.get("status") == "inactive"
        and input_asset.get("tradable") is False
        and isinstance(input_asset.get("id"), str)
        and input_asset["id"]
        and local_evidence["complete_row_count_evidence"]
        and local_evidence["complete_request_scope_evidence"]
        and local_evidence["complete_adjustment_coverage"]
        and local_evidence["feeds"] == ["sip"]
        and local_evidence["timeframes"] == ["1Day"]
        and not has_rows
    ):
        inactive_scope_evidence = {
            "basis": "exact_inactive_nontradable_alpaca_input_asset_and_authenticated_empty_sip_history",
            "alpaca_asset_id": input_asset["id"],
            "asset_class": input_asset.get("class"),
            "exchange": input_asset.get("exchange"),
            "status": "inactive",
            "tradable": False,
            "configured_feed": "sip",
            "timeframe": "1Day",
            "adjustments": sorted(_COMPLETE_ADJUSTMENT_SET),
        }
    just_skip_evidence = None
    if len(input_symbol_occupants) == 1:
        occupant = input_symbol_occupants[0]
        occupant_asset_id = occupant.get("asset_id") if isinstance(occupant, dict) else None
        occupant_evidence = occupant.get("evidence") if isinstance(occupant, dict) else None
        if (
            lookup_evidence == {"outcome": "not_found", "http_status": 404}
            and isinstance(occupant, dict)
            and occupant.get("symbol") == symbol
            and isinstance(occupant.get("cusip"), str)
            and occupant["cusip"]
            and isinstance(occupant_asset_id, str)
            and occupant_asset_id in usable_asset_ids
            and isinstance(occupant_evidence, list)
            and bool(occupant_evidence)
            and local_evidence["complete_row_count_evidence"]
            and local_evidence["complete_request_scope_evidence"]
            and local_evidence["complete_adjustment_coverage"]
            and local_evidence["feeds"] == ["sip"]
            and local_evidence["timeframes"] == ["1Day"]
            and not has_rows
        ):
            just_skip_evidence = {
                "basis": (
                    "exact_missing_input_asset_with_unambiguous_resolved_lineage_"
                    "and_authenticated_empty_sip_history"
                ),
                "symbol": symbol,
                "cusip": occupant["cusip"],
                "alpaca_asset_id": occupant_asset_id,
                "corporate_action_evidence_count": len(occupant_evidence),
                "configured_feed": "sip",
                "timeframe": "1Day",
                "adjustments": sorted(_COMPLETE_ADJUSTMENT_SET),
            }

    if test_symbol_evidence is not None:
        classification = EXCHANGE_TEST_SYMBOL
        data_policy = "exclude"
        recurring_pipeline_action = RECURRING_PIPELINE_SKIP
    elif otc_scope_evidence is not None:
        classification = OTC_ASSET_OUT_OF_SCOPE
        data_policy = "out_of_scope"
        recurring_pipeline_action = RECURRING_PIPELINE_SKIP
    elif inactive_scope_evidence is not None:
        classification = INACTIVE_EMPTY_ASSET_OUT_OF_SCOPE
        data_policy = "out_of_scope"
        recurring_pipeline_action = RECURRING_PIPELINE_SKIP
    elif just_skip_evidence is not None:
        classification = JUST_SKIP
        data_policy = "just_skip"
        recurring_pipeline_action = RECURRING_PIPELINE_SKIP
    elif usable_asset_ids:
        classification = ALPACA_ASSET_IDENTITY_AVAILABLE
        data_policy = "process"
        recurring_pipeline_action = RECURRING_PIPELINE_RUN
    elif (
        lookup_evidence == {"outcome": "not_found", "http_status": 404}
        and local_evidence["complete_row_count_evidence"]
        and has_rows
    ):
        classification = HISTORICAL_MARKET_DATA_ONLY
        data_policy = "retain"
        recurring_pipeline_action = RECURRING_PIPELINE_RUN
    elif (
        lookup_evidence == {"outcome": "not_found", "http_status": 404}
        and local_evidence["complete_row_count_evidence"]
        and not has_rows
    ):
        classification = UNSUPPORTED_EMPTY_SYMBOL
        data_policy = "quarantine"
        recurring_pipeline_action = RECURRING_PIPELINE_SKIP
    else:
        classification = UNRESOLVED_SYMBOL_DISPOSITION
        data_policy = "investigate"
        recurring_pipeline_action = RECURRING_PIPELINE_RUN

    series_identity = (
        market_data_series_identity(symbol)
        if has_rows and classification in {EXCHANGE_TEST_SYMBOL, HISTORICAL_MARKET_DATA_ONLY}
        else None
    )

    disposition = {
        "classification": classification,
        "data_policy": data_policy,
        "recurring_pipeline_action": recurring_pipeline_action,
        "input_symbol_asset_lookup": lookup_evidence,
        "resolved_alpaca_asset_ids": usable_asset_ids,
        "local_market_data": local_evidence,
        "market_data_series_identity": series_identity,
        "test_symbol_evidence": test_symbol_evidence,
    }
    if otc_scope_evidence is not None:
        disposition["out_of_scope_evidence"] = otc_scope_evidence
    elif inactive_scope_evidence is not None:
        disposition["out_of_scope_evidence"] = inactive_scope_evidence
    elif just_skip_evidence is not None:
        disposition["just_skip_evidence"] = just_skip_evidence
    return disposition


def recurring_pipeline_should_skip(disposition: object) -> bool:
    if not isinstance(disposition, dict):
        return False
    classification = disposition.get("classification")
    expected_policy = {
        EXCHANGE_TEST_SYMBOL: "exclude",
        INACTIVE_EMPTY_ASSET_OUT_OF_SCOPE: "out_of_scope",
        JUST_SKIP: "just_skip",
        OTC_ASSET_OUT_OF_SCOPE: "out_of_scope",
        UNSUPPORTED_EMPTY_SYMBOL: "quarantine",
    }.get(classification)
    if (
        expected_policy is None
        or disposition.get("data_policy") != expected_policy
        or disposition.get("recurring_pipeline_action") != RECURRING_PIPELINE_SKIP
    ):
        return False
    if classification == EXCHANGE_TEST_SYMBOL:
        test_evidence = disposition.get("test_symbol_evidence")
        return isinstance(test_evidence, dict) and test_evidence.get("test_issue") is True

    if classification == OTC_ASSET_OUT_OF_SCOPE:
        scope_evidence = disposition.get("out_of_scope_evidence")
        resolved_asset_ids = disposition.get("resolved_alpaca_asset_ids")
        if (
            disposition.get("input_symbol_asset_lookup") != {"outcome": "found", "http_status": 200}
            or not isinstance(scope_evidence, dict)
            or scope_evidence.get("basis")
            != "exact_alpaca_input_asset_exchange_and_authenticated_empty_sip_history"
            or scope_evidence.get("exchange") != "OTC"
            or scope_evidence.get("configured_feed") != "sip"
            or scope_evidence.get("timeframe") != "1Day"
            or not isinstance(scope_evidence.get("alpaca_asset_id"), str)
            or not isinstance(resolved_asset_ids, list)
            or scope_evidence["alpaca_asset_id"] not in resolved_asset_ids
        ):
            return False

    if classification == INACTIVE_EMPTY_ASSET_OUT_OF_SCOPE:
        scope_evidence = disposition.get("out_of_scope_evidence")
        resolved_asset_ids = disposition.get("resolved_alpaca_asset_ids")
        if (
            disposition.get("input_symbol_asset_lookup") != {"outcome": "found", "http_status": 200}
            or not isinstance(scope_evidence, dict)
            or scope_evidence.get("basis")
            != "exact_inactive_nontradable_alpaca_input_asset_and_authenticated_empty_sip_history"
            or scope_evidence.get("status") != "inactive"
            or scope_evidence.get("tradable") is not False
            or scope_evidence.get("configured_feed") != "sip"
            or scope_evidence.get("timeframe") != "1Day"
            or scope_evidence.get("asset_class") != "us_equity"
            or scope_evidence.get("adjustments") != sorted(_COMPLETE_ADJUSTMENT_SET)
            or not isinstance(scope_evidence.get("alpaca_asset_id"), str)
            or not isinstance(resolved_asset_ids, list)
            or scope_evidence["alpaca_asset_id"] not in resolved_asset_ids
        ):
            return False

    if classification == JUST_SKIP:
        skip_evidence = disposition.get("just_skip_evidence")
        resolved_asset_ids = disposition.get("resolved_alpaca_asset_ids")
        if (
            disposition.get("input_symbol_asset_lookup")
            != {
                "outcome": "not_found",
                "http_status": 404,
            }
            or not isinstance(skip_evidence, dict)
            or skip_evidence.get("basis")
            != (
                "exact_missing_input_asset_with_unambiguous_resolved_lineage_and_authenticated_empty_sip_history"
            )
            or not isinstance(skip_evidence.get("symbol"), str)
            or not isinstance(skip_evidence.get("cusip"), str)
            or not isinstance(skip_evidence.get("alpaca_asset_id"), str)
            or type(skip_evidence.get("corporate_action_evidence_count")) is not int
            or skip_evidence["corporate_action_evidence_count"] < 1
            or skip_evidence.get("configured_feed") != "sip"
            or skip_evidence.get("timeframe") != "1Day"
            or skip_evidence.get("adjustments") != sorted(_COMPLETE_ADJUSTMENT_SET)
            or not isinstance(resolved_asset_ids, list)
            or skip_evidence["alpaca_asset_id"] not in resolved_asset_ids
        ):
            return False

    if classification == UNSUPPORTED_EMPTY_SYMBOL:
        if (
            disposition.get("input_symbol_asset_lookup")
            != {
                "outcome": "not_found",
                "http_status": 404,
            }
            or disposition.get("resolved_alpaca_asset_ids") != []
        ):
            return False
    local_evidence = disposition.get("local_market_data")
    if not isinstance(local_evidence, dict) or local_evidence.get("complete_row_count_evidence") is not True:
        return False
    return (
        local_evidence.get("nonempty_download_count") == 0
        and local_evidence.get("maximum_rows") == 0
        and (
            classification not in {INACTIVE_EMPTY_ASSET_OUT_OF_SCOPE, JUST_SKIP, OTC_ASSET_OUT_OF_SCOPE}
            or (
                local_evidence.get("complete_request_scope_evidence") is True
                and local_evidence.get("feeds") == ["sip"]
                and local_evidence.get("timeframes") == ["1Day"]
                and (
                    classification not in {INACTIVE_EMPTY_ASSET_OUT_OF_SCOPE, JUST_SKIP}
                    or local_evidence.get("complete_adjustment_coverage") is True
                )
            )
        )
        and disposition.get("market_data_series_identity") is None
    )
