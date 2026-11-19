"""Select the provider's CUSIP lookup observations relevant to a split carry."""

from libs.alpaca_security_lineage.inference_evidence import reject_assertions


def endpoints(evidence, cusips):
    observed = set()
    for lookup in evidence.get("asset_lookups", ()):
        code = lookup.get("lookup")
        if lookup.get("lookup_kind") == "cusip" and code in cusips:
            reject_assertions(lookup, where="split carry CUSIP lookup")
            asset = lookup.get("asset") or {}
            observed.add((code, asset.get("symbol") or "", asset.get("id") or ""))
    return sorted(observed)
