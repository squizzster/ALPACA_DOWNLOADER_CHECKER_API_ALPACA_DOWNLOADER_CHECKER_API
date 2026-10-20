"""One complete provider observation, retaining duplicate labels and uncertainty."""

from collections import defaultdict
from datetime import datetime, timezone

from libs.alpaca_data_api_solver import build_plan
from libs.alpaca_pricing_download.contract import symbol_label
from libs.alpaca_security_lineage.lineage import compact_asset

from .contract import LineageError, document_digest

DISCOVERY_EXCHANGES = ("OTC", "NYSE", "NASDAQ", "AMEX", "TXSE")


def observe_catalogue(client):
    plan = build_plan({"steps": [{"needs": [{"kind": "asset_catalogue", "why": "universe_discovery"}]}]})
    records = client.execute_plan(plan)[0][0]
    if not isinstance(records, list) or not records:
        raise LineageError("complete US-equity catalogue must be a nonempty array")
    for asset in records:
        if (
            not isinstance(asset, dict)
            or asset.get("class") != "us_equity"
            or any(
                not isinstance(asset.get(key), str) or not asset[key]
                for key in ("id", "symbol", "exchange", "status")
            )
            or not isinstance(asset.get("tradable"), bool)
        ):
            raise LineageError("invalid US-equity catalogue record; discovery is incomplete")
        # Inactive records may have no name. Retain them verbatim; the evidence
        # client already requires exact lookup for incomplete identity fields.
    return {
        "schema_version": 1,
        "catalogue_observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "request_plan": plan,
        "records": records,
        "records_sha256": document_digest({"records": records}),
    }


def by_symbol(observation):
    result = defaultdict(list)
    for asset in observation["records"]:
        result[asset["symbol"]].append(asset)
    return dict(result)


def discovery_labels(observation):
    result = set()
    for asset in observation["records"]:
        if asset["status"] == "active" and asset["exchange"] in DISCOVERY_EXCHANGES:
            try:
                if symbol_label(asset["symbol"]) == asset["symbol"]:
                    result.add(asset["symbol"])
            except (RuntimeError, ValueError):
                pass  # Preserve unsupported labels in the observation, never use them as paths.
    return result


def evidence_fingerprint(records):
    return document_digest(
        {
            "assets": sorted(
                (compact_asset(asset) for asset in records), key=lambda asset: (asset["id"], str(asset))
            )
        }
    )
