"""Resume action acquisition at the previous active month; identity stays separate."""

from collections import defaultdict

from libs.alpaca_data_api_solver import VERSION as PLAN_VERSION
from libs.alpaca_data_api_solver import build_plan, split_action
from libs.alpaca_pricing_download.contract import DownloadError, symbol_label
from libs.alpaca_security_lineage.lineage import (
    action_date,
    action_identity_key,
    build_identity_report,
    compact_asset,
    extract_cusips,
)
from libs.alpaca_security_lineage.settings import ACTION_TYPE_BY_RESPONSE_KEY, MAX_NAME_CHANGE_SYMBOLS

from .contract import LineageError


def action_symbols(value):
    """Include recipients inside reorganization stock_movements, preserving the event."""
    if isinstance(value, list):
        return set().union(*(action_symbols(item) for item in value))
    if not isinstance(value, dict):
        return set()
    result = {
        item
        for key, item in value.items()
        if (key == "symbol" or key.endswith("_symbol")) and isinstance(item, str) and item
    }
    for item in value.values():
        if isinstance(item, (dict, list)):
            result.update(action_symbols(item))
    return result


def full_metadata(previous, notice, refresh):
    from .contract import reusable_metadata_versions

    return (
        refresh
        or previous is not None
        and (
            not reusable_metadata_versions(previous.get("versions"))
            or (notice or {}).get("from", previous["anchor_month"] + "-01") < previous["anchor_month"] + "-01"
        )
    )


def action_requests(symbol, start, end, previous=None, *, refresh=False):
    """Known labels only; new labels get the original full range when discovered."""
    window = WindowClient(None, symbol, previous, refresh)
    known = previous["evidence"]["queried_symbols"] if previous else ()
    return [(label, window.lower(label, start), end) for label in sorted({symbol, *known})]


class EvidenceClient:
    """One run's coherent lookup cache. Conflicting action IDs fail before publication."""

    # DO NOT turn cache hits into provider rechecks. Completed observations are
    # authoritative within this operation; failed requests remain failures, and
    # conflicting/new evidence is handled by its owning acquisition path below.

    def __init__(self, client, *, observation=None):
        self.client = client
        self.assets = {}
        self.actions = {}
        self.action_intervals = defaultdict(dict)
        self.by_id = {}
        self.catalogue = None
        self.catalogue_assets = {}
        self.catalogue_attempted = False
        self.action_conflicts = set()
        self.observation = observation
        self.work_policy = None
        self.restricted_identifiers = set()
        if observation is not None:
            self._load_catalogue(observation["records"])
            self.catalogue_attempted = True

    @property
    def supports_plans(self):
        return callable(getattr(self.client, "execute_plan", None))

    @property
    def request_count(self):
        return self.client.request_count

    def get_asset_lookup(self, identifier, *, completed_lookup=None):
        if self.work_policy is not None:
            self.restricted_identifiers.update(self.work_policy.restricted_identifiers)
        if identifier in self.restricted_identifiers:
            from .work_contract import WorkDeferred

            raise WorkDeferred(f"{identifier}: metadata request is deferred")
        if self.work_policy is not None:
            self.work_policy.check({identifier})
        if identifier not in self.assets:
            asset = (completed_lookup or {}).get("asset")
            if (
                asset
                and completed_lookup["outcome"] == "found"
                and self.catalogue_assets.get(asset["id"]) == asset
            ):
                # The receipt owns the CUSIP-to-ID mapping; the fresh catalogue
                # supplies matching current asset facts. Do not seed the exact
                # lookup cache: a new action may still require a fresh mapping.
                return {key: completed_lookup[key] for key in ("outcome", "http_status", "asset")}
            try:
                self.assets[identifier] = self.client.get_asset_lookup(identifier)
            except (RuntimeError, OSError, ValueError) as error:
                error.request_identifiers = [identifier]
                self.assets[identifier] = error
        return self._value(self.assets[identifier])

    def get_symbol_lookup(self, symbol):
        # A unique catalogue label can supply a symbol lookup. This permission
        # does not extend to ambiguous labels or exact CUSIP-to-ID mappings.
        if self.work_policy is not None:
            self.work_policy.check({symbol})
        if symbol not in self.assets and self.catalogue is not None and self.catalogue.get(symbol):
            return {
                "outcome": "found",
                "http_status": 200,
                "asset": self.catalogue[symbol],
                "source": "asset_catalogue",
            }
        return self.get_asset_lookup(symbol)

    @staticmethod
    def _value(value):
        if isinstance(value, Exception):
            raise value
        return value

    def _execute(self, action):
        return self.client.execute_plan({"schema_version": PLAN_VERSION, "steps": [{"actions": [action]}]})[0][0]

    def prefetch_symbols(self, symbols):
        """Only explicitly typed ticker needs may use the catalogue; CUSIPs never do."""
        if not self.supports_plans:
            return
        pending = sorted({s for s in symbols if s not in self.assets and not (self.catalogue or {}).get(s)})
        if self.work_policy is not None:
            pending = [s for s in pending if self.work_policy.allowed({s})]
        pending = [s for s in pending if s not in self.restricted_identifiers]
        if not pending:
            return
        need = {"kind": "asset_lookup", "why": "identity_lookup", "identifiers": pending}
        if not self.catalogue_attempted:
            need["lookup_kind"] = "symbol"
        plan = build_plan({"steps": [{"needs": [need]}]})
        for action in plan["steps"][0]["actions"]:
            if action["strategy"] == "asset_catalogue":
                self.catalogue_attempted = True
                try:
                    self._load_catalogue(self._execute(action))
                except (RuntimeError, OSError, ValueError):
                    # An optional optimization cannot manufacture a missing-asset outcome.
                    self.catalogue = None
            else:
                identifier = action["deliver"][0]["identifier"]
                try:
                    self.assets[identifier] = self._execute(action)
                except (RuntimeError, OSError, ValueError) as error:
                    error.request_identifiers = [identifier]
                    self.assets[identifier] = error
        if any(action["strategy"] == "asset_catalogue" for action in plan["steps"][0]["actions"]):
            self.prefetch_symbols(pending)  # Remaining needs are now exact lookups.

    def _load_catalogue(self, assets):
        if not isinstance(assets, list):
            raise LineageError("asset catalogue must be an array")
        catalogue, assets_by_id = {}, {}
        for asset in assets:
            if not isinstance(asset, dict) or not isinstance(asset.get("symbol"), str) or not asset["symbol"]:
                raise LineageError("invalid asset catalogue entry")
            symbol = asset["symbol"]
            if (
                any(
                    not isinstance(asset.get(key), str) or not asset[key]
                    for key in ("id", "class", "exchange", "name", "status")
                )
                or asset["class"] != "us_equity"
                or not isinstance(asset.get("tradable"), bool)
            ):
                catalogue[symbol] = None  # This label needs exact evidence; other valid entries remain useful.
                continue
            compact = compact_asset(asset)
            catalogue[symbol] = None if symbol in catalogue else compact
            identity = asset["id"]
            assets_by_id[identity] = (
                None if identity in assets_by_id and assets_by_id[identity] != compact else compact
            )
        self.catalogue = catalogue
        self.catalogue_assets = assets_by_id

    def prefetch_corporate_actions(self, requests):
        if not self.supports_plans:
            return
        pending = sorted(key for key in set(requests) if not self._covered(key))
        if self.work_policy is not None:
            self.restricted_identifiers.update(self.work_policy.restricted_identifiers)
            pending = [key for key in pending if self.work_policy.allowed({key[0]})]
        pending = [key for key in pending if key[0] not in self.restricted_identifiers]
        if not pending:
            return
        plan = build_plan(
            {
                "steps": [
                    {
                        "needs": [
                            {
                                "kind": "corporate_actions",
                                "why": "lineage_evidence",
                                "symbols": [symbol],
                                "start": start,
                                "end": end,
                            }
                            for symbol, start, end in pending
                        ]
                    }
                ]
            }
        )
        for action in plan["steps"][0]["actions"]:
            params = action["request"]["params"]
            keys = [(s, params["start"], params["end"]) for s in params["symbols"].split(",")]
            try:
                completed = {}
                self._acquire_action(action, completed)
            except (RuntimeError, OSError, ValueError) as error:
                # Other independent date/symbol batches still get their own result.
                error.request_identifiers = [key[0] for key in keys]
                self.actions.update((key, error) for key in keys)
                continue
            for key, rows in completed.items():
                try:
                    self._remember(key, rows)
                except (RuntimeError, OSError, ValueError) as error:
                    error.request_identifiers = [key[0]]
                    self.actions[key] = error

    def _acquire_action(self, action, completed):
        params = action["request"]["params"]
        symbols = params["symbols"].split(",")
        rows = self._execute(action)
        # Route every version before rejecting a conflict: unrelated symbols in
        # the same complete response still have usable evidence.
        self._record_actions(rows)
        routed = defaultdict(list)
        ambiguous = False
        for row in rows:
            if not params["start"] <= action_date(row) <= params["end"]:
                raise LineageError("corporate action outside requested dates")
            labels = action_symbols(row).intersection(symbols)
            if not labels or row.get("action_type") not in ACTION_TYPE_BY_RESPONSE_KEY.values():
                ambiguous = True
            for symbol in labels:
                routed[symbol].append(row)
        if ambiguous and len(symbols) > 1:
            children = []
            for part in split_action(action):
                children.extend(self._acquire_action(part, completed))
            if {action_identity_key(row): row for row in rows} != {
                action_identity_key(row): row for row in children
            }:
                raise LineageError("corporate actions changed during batch routing")
            return children
        for symbol in symbols:
            # Coverage is accepted only after the complete batch and any routing
            # comparisons succeed, including every recursively split response.
            completed[(symbol, params["start"], params["end"])] = rows if len(symbols) == 1 else routed[symbol]
        return rows

    def _remember(self, key, actions):
        symbol, start, end = key
        checked = self._checked_actions(actions)
        if any(not start <= action_date(action) <= end for action in checked):
            raise LineageError("corporate action outside requested dates")
        self.actions[key] = checked
        self.action_intervals[symbol][(start, end)] = checked

    def _covered(self, key):
        # A successfully completed containing action interval already proves
        # coverage of its subset. DO NOT request the subset again to confirm it.
        if key in self.actions:
            return True
        symbol, start, end = key
        for (lower, upper), actions in self.action_intervals.get(symbol, {}).items():
            if lower <= start and end <= upper:
                self.actions[key] = [action for action in actions if start <= action_date(action) <= end]
                return True
        return False

    def _record_actions(self, actions):
        for action in actions:
            identity = action_identity_key(action)
            previous = self.by_id.get(identity)
            if previous is not None and previous != action:
                self.action_conflicts.add(identity)
            else:
                self.by_id[identity] = action

    def _check_action_conflicts(self, actions):
        for action in actions:
            identity = action_identity_key(action)
            if identity in self.action_conflicts:
                raise LineageError(f"conflicting corporate-action identity: {identity}")

    def _checked_actions(self, actions):
        self._record_actions(actions)
        self._check_action_conflicts(actions)
        return list({action_identity_key(action): action for action in actions}.values())

    def get_corporate_actions(self, symbol, start, end):
        if self.work_policy is not None:
            self.restricted_identifiers.update(self.work_policy.restricted_identifiers)
            self.work_policy.check({symbol})
        if symbol in self.restricted_identifiers:
            from .work_contract import WorkDeferred

            raise WorkDeferred(f"{symbol}: metadata request is deferred")
        key = (symbol, start, end)
        if not self._covered(key):
            if self.supports_plans:
                self.prefetch_corporate_actions([key])
            else:
                try:
                    self._remember(key, self.client.get_corporate_actions(symbol, start, end))
                except (RuntimeError, OSError, ValueError) as error:
                    error.request_identifiers = [symbol]
                    self.actions[key] = error
        actions = self._value(self.actions[key])
        try:
            # A later response may contradict an action in this cached interval.
            # Check only the returned evidence, including any narrowed window.
            self._check_action_conflicts(actions)
        except LineageError as error:
            error.request_identifiers = [symbol]
            raise
        return actions


class _CompletedLookupChanged(RuntimeError):
    """Newly traversed evidence replaced a lookup consumed earlier in this report."""


class WindowClient:
    # Retained evidence owns the earlier completed window. Exact-lookup reuse also
    # requires a positive retained result and identical, unambiguous current asset
    # facts; negative/missing/changed facts or new actions can require acquisition.
    # Moving the cutoff alone does not invalidate all historical lookup evidence.
    def __init__(self, client, symbol, previous, refresh):
        self.client, self.symbol = client, symbol
        self.previous = previous
        self.start = None if refresh or previous is None else previous["anchor_month"] + "-01"
        self.lookup_sources = {}
        evidence = (previous or {}).get("evidence", {}) if client is not None else {}
        self.completed_actions = {
            action_identity_key(action): action for action in evidence.get("corporate_actions", ())
        }
        self.completed_cusip_lookups = {
            lookup["lookup"]: lookup
            for lookup in evidence.get("asset_lookups", ())
            if lookup["lookup_kind"] == "cusip"
        }
        self.changed_cusips = set()
        self.retained_lookups = {}

    @property
    def request_count(self):
        return self.client.request_count

    def get_asset_lookup(self, identifier):
        previous = (
            self.completed_cusip_lookups.get(identifier)
            if self.start and identifier not in self.changed_cusips
            else None
        )
        result = self.client.get_asset_lookup(identifier, completed_lookup=previous)
        if previous is not None and identifier not in self.client.assets:
            self.retained_lookups[identifier] = result
        return result

    def get_symbol_lookup(self, symbol):
        lookup = self.client.get_symbol_lookup(symbol)
        if "source" in lookup:
            self.lookup_sources[symbol] = lookup["source"]
        return lookup

    def lower(self, symbol, start):
        known = self.previous["evidence"]["queried_symbols"] if self.previous else ()
        return max(start, self.start) if self.start and symbol in known else start

    def prefetch_corporate_actions(self, symbols, start, end):
        self.client.prefetch_corporate_actions([(s, self.lower(s, start), end) for s in symbols])

    def get_corporate_actions(self, symbol, start, end):
        # Labels newly discovered in this run have no earlier acquisition checkpoint.
        current = self.client.get_corporate_actions(symbol, self.lower(symbol, start), end)
        for action in current:
            accepted = self.completed_actions.get(action_identity_key(action))
            if accepted != action:
                self.changed_cusips.update(extract_cusips([action, *([accepted] if accepted else [])]))
        replaced = False
        for identifier in sorted(self.changed_cusips & self.retained_lookups.keys()):
            accepted = self.retained_lookups.pop(identifier)
            fresh = self.client.get_asset_lookup(identifier)
            replaced |= any(fresh[key] != accepted[key] for key in ("outcome", "http_status", "asset"))
        if replaced:
            raise _CompletedLookupChanged("new action changed a retained CUSIP lookup")
        historical = []
        if self.start and symbol == self.symbol:
            historical = [
                action
                for action in self.previous["evidence"]["corporate_actions"]
                if action_date(action) < self.start
            ]
        return historical + current


def acquire(view, client, previous=None, *, refresh=False):
    for _ in range(MAX_NAME_CHANGE_SYMBOLS + 1):
        window = WindowClient(client, view.symbol, previous, refresh)
        try:
            # DO NOT REMOVE WITHOUT USER AUTH!#
            # The bar mapping date can be historical. Corporate-action discovery
            # still covers the complete accepted observation cutoff, including
            # the retirement that required that mapping in the first place.
            evidence = build_identity_report(
                view.symbol,
                view.local,
                window,
                query_dates=(
                    view.source["history_start"][:10],
                    view.source["latest_request"]["end_exclusive"][:10],
                ),
            )
        except _CompletedLookupChanged:
            continue  # The fresh lookup is now authoritative in this operation's cache.
        return _finish_evidence(evidence, window)
    raise LineageError("metadata lookup changes exceeded the name-change traversal limit")


def _finish_evidence(evidence, window):
    evidence.pop("api_request_count", None)
    observation = getattr(window.client, "observation", None)
    if observation is not None:
        evidence["catalogue_observed_at_utc"] = observation["catalogue_observed_at_utc"]
    for lookup in evidence["asset_lookups"]:
        if lookup["lookup_kind"] == "symbol" and lookup["lookup"] in window.lookup_sources:
            lookup["source"] = window.lookup_sources[lookup["lookup"]]
    # build_identity_report emits this filesystem warning for logical descriptors too.
    # Preserve the in-memory handoff; do not turn this wording into file authentication.
    evidence["warnings"] = [
        warning
        for warning in evidence["warnings"]
        if not warning.startswith("Local gzip SHA-256 authentication was not performed;")
    ]
    return evidence


def acquire_unpriced(symbol, request, client):
    """Query metadata independently of bars, without fabricating a pricing input."""
    local = {
        "present": False,
        "directory": None,
        "downloads": [],
    }
    window = WindowClient(client, symbol, None, True)
    evidence = build_identity_report(symbol, local, window, query_dates=(request["start"][:10], request["asof"]))
    return _finish_evidence(evidence, window)


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
