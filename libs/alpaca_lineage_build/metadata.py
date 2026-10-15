"""Resume action acquisition at the previous active month; identity stays separate."""

from libs.alpaca_pricing_download.contract import DownloadError, symbol_label
from libs.alpaca_security_lineage.lineage import action_date, action_identity_key, build_identity_report

from .contract import LineageError


class EvidenceClient:
    """One run's coherent lookup cache. Conflicting action IDs fail before publication."""

    def __init__(self, client):
        self.client = client
        self.assets = {}
        self.actions = {}
        self.by_id = {}

    @property
    def request_count(self):
        return self.client.request_count

    def get_asset_lookup(self, identifier):
        if identifier not in self.assets:
            self.assets[identifier] = self.client.get_asset_lookup(identifier)
        return self.assets[identifier]

    def get_corporate_actions(self, symbol, start, end):
        key = (symbol, start, end)
        if key not in self.actions:
            actions = self.client.get_corporate_actions(symbol, start, end)
            for action in actions:
                identity = action_identity_key(action)
                previous = self.by_id.get(identity)
                if previous is not None and previous != action:
                    raise LineageError(f"conflicting corporate-action identity: {identity}")
                self.by_id[identity] = action
            self.actions[key] = actions
        return self.actions[key]


class WindowClient:
    def __init__(self, client, symbol, previous, refresh):
        self.client, self.symbol = client, symbol
        self.previous = previous
        self.start = None if refresh or previous is None else previous["anchor_month"] + "-01"

    @property
    def request_count(self):
        return self.client.request_count

    def get_asset_lookup(self, identifier):
        return self.client.get_asset_lookup(identifier)

    def get_corporate_actions(self, symbol, start, end):
        # Labels newly discovered in this run have no earlier acquisition checkpoint.
        known = self.previous["evidence"]["queried_symbols"] if self.previous else ()
        lower = max(start, self.start) if self.start and symbol in known else start
        current = self.client.get_corporate_actions(symbol, lower, end)
        historical = []
        if self.start and symbol == self.symbol:
            historical = [
                action
                for action in self.previous["evidence"]["corporate_actions"]
                if action_date(action) < self.start
            ]
        return historical + current


def acquire(view, client, previous=None, *, refresh=False):
    evidence = build_identity_report(
        view.symbol, view.local, WindowClient(client, view.symbol, previous, refresh)
    )
    evidence.pop("api_request_count", None)
    # The legacy inspector's warning describes its filesystem adapter, not this one.
    evidence["warnings"] = [
        warning
        for warning in evidence["warnings"]
        if not warning.startswith("Local gzip SHA-256 authentication was not performed;")
    ]
    return evidence


def dependencies(evidence: dict, symbol: str) -> list[str]:
    labels = set(evidence["queried_symbols"])
    for action in evidence["corporate_actions"]:
        labels.update(
            value for key, value in action.items() if key.endswith("symbol") and isinstance(value, str)
        )
    for identity in evidence["identities"]:
        labels.update(identity.get("lookup_symbols", ()))
        if isinstance(identity.get("symbol"), str):
            labels.add(identity["symbol"])
    result = set()
    for label in labels:
        try:
            result.add(symbol_label(label))
        except DownloadError:
            continue  # Invalid provider labels remain in evidence, never filesystem paths.
    return sorted(result - {symbol})
