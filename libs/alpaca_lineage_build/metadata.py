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

DEPENDENCY_HANDOFF_CORRELATION_VERSION = 77


def mapping_dependencies(evidence, lookup):
    """The mapping's accepted endpoint baseline, not today's inferred owner."""
    old_symbols = {
        item["lookup"]: (item.get("asset") or {}).get("id")
        for item in evidence.get("asset_lookups", ())
        if item["lookup_kind"] == "symbol"
    }
    identifier = lookup["lookup"]

    def labels(value):
        if isinstance(value, list):
            return set().union(*(labels(item) for item in value))
        if not isinstance(value, dict):
            return set()
        result = {
            value.get(key[:-6] + "_symbol" if key.endswith("_cusip") else "symbol")
            for key, item in value.items()
            if (key == "cusip" or key.endswith("_cusip")) and item == identifier
        }
        for item in value.values():
            if isinstance(item, (dict, list)):
                result.update(labels(item))
        return result

    endpoints = labels(evidence.get("corporate_actions", ()))
    canonical = lookup.get("asset") or {}
    if canonical.get("symbol"):
        endpoints.add(canonical["symbol"])
        old_symbols.setdefault(canonical["symbol"], canonical.get("id"))
    result = {label: old_symbols[label] for label in endpoints if label in old_symbols}
    # These are endpoint observations made when the exact mapping was acquired,
    # not a decision about its UUID. They also preserve legitimate ticker reuse:
    # the fresh mapping may still identify the historical occupant.
    result.update(evidence.get("mapping_endpoint_observations", {}).get(identifier, {}))
    return result


def retained_symbol_observations(evidences):
    """Collect explicit completed observations without treating them as fresh HTTP."""
    observed = defaultdict(dict)
    for evidence in evidences:
        for lookup in evidence.get("asset_lookups", ()):
            if lookup["lookup_kind"] == "symbol":
                observed[lookup["lookup"]][(lookup.get("asset") or {}).get("id")] = lookup
    return observed


def changed_mapping_dependencies(evidence, lookup, observed):
    return {
        label
        for label, prior in mapping_dependencies(evidence, lookup).items()
        if (current := observed(label)) is not None and (current.get("asset") or {}).get("id") != prior
    }


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
        self.retained_symbols = {}
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
            if completed_lookup is not None:
                # The receipt owns this historical mapping, including a negative
                # result. A name/status change is not a changed CUSIP mapping.
                # Current facts may be joined by an already accepted UUID; they
                # must not be used to infer a different UUID from a ticker.
                current = self.catalogue_assets.get(asset["id"]) if asset else None
                return {
                    "outcome": completed_lookup["outcome"],
                    "http_status": completed_lookup["http_status"],
                    "asset": current if current is not None else asset,
                }
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
        if symbol in self.assets:
            return self.get_asset_lookup(symbol)
        observed = self.observed_symbol_lookup(symbol)
        return observed if observed is not None else self.get_asset_lookup(symbol)

    def observed_symbol_lookup(self, symbol):
        """Return only an already acquired observation, regardless of its route."""
        if symbol in self.assets:
            return self._value(self.assets[symbol])
        if self.catalogue is not None and self.catalogue.get(symbol):
            return {
                "outcome": "found",
                "http_status": 200,
                "asset": self.catalogue[symbol],
                "source": "asset_catalogue",
            }
        return None

    def evidence_current(self, evidence):
        """New producer reports must use this operation's acquired observations."""
        for lookup in evidence["asset_lookups"]:
            identifier = lookup["lookup"]
            observed = (
                self.observed_symbol_lookup(identifier)
                if lookup["lookup_kind"] == "symbol"
                else self._value(self.assets[identifier])
                if identifier in self.assets
                else None
            )
            if observed is not None and (
                lookup["outcome"] != observed["outcome"]
                or lookup["http_status"] != observed["http_status"]
                or lookup.get("asset") != (compact_asset(observed["asset"]) if observed.get("asset") else None)
            ):
                return False
            if lookup["lookup_kind"] == "cusip" and not self.mapping_current(evidence, lookup):
                return False
        return True

    def dependency_observation(self, symbol):
        observed = self.observed_symbol_lookup(symbol)
        return observed if observed is not None else self.retained_symbols.get(symbol)

    def mapping_current(self, evidence, lookup):
        # A fresh exact response is authoritative even when historical CUSIP and
        # current ticker legitimately identify different UUIDs. Only retained
        # mappings need invalidation after an actually observed endpoint change.
        return lookup["lookup"] in self.assets or not changed_mapping_dependencies(
            evidence, lookup, self.dependency_observation
        )

    def prepare_retained(self, evidences):
        """One pre-fix complete group's metadata-owner reconciliation."""
        for symbol, observations in retained_symbol_observations(evidences).items():
            if len(observations) == 1:
                self.retained_symbols[symbol] = next(iter(observations.values()))
            elif self.observed_symbol_lookup(symbol) is None:
                # Conflicting completed observations select this exact endpoint;
                # never choose a historical observation by iteration order.
                self.get_asset_lookup(symbol)
        mappings = defaultdict(set)
        for evidence in evidences:
            for lookup in evidence.get("asset_lookups", ()):
                if lookup["lookup_kind"] == "cusip":
                    mappings[lookup["lookup"]].add((lookup.get("asset") or {}).get("id"))
        for identifier, identities in mappings.items():
            if len(identities) > 1:
                self.get_asset_lookup(identifier)

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


class RetainedReportClient:
    """Reassemble accepted metadata after selected exact mappings change.

    Completed action coverage and symbol responses are historical inputs, kept
    separate from this operation's fresh cache. Only a genuinely new label or
    uncovered interval delegates acquisition to the ordinary metadata owner.
    """

    def __init__(self, client, evidence):
        self.client, self.evidence = client, evidence
        self.symbols = {
            lookup["lookup"]: lookup for lookup in evidence["asset_lookups"] if lookup["lookup_kind"] == "symbol"
        }

    def __getattr__(self, name):
        return getattr(self.client, name)

    def covered(self, symbol, start, end):
        query = self.evidence["query"]
        return (
            symbol in self.evidence["queried_symbols"]
            and query["corporate_actions_data_quality"] == "complete"
            and query["corporate_actions_start"] <= start <= end <= query["corporate_actions_end"]
        )

    def prefetch_corporate_actions(self, requests):
        self.client.prefetch_corporate_actions([request for request in requests if not self.covered(*request)])

    def get_corporate_actions(self, symbol, start, end):
        if self.covered(symbol, start, end):
            # The completed report owns this union across its queried labels.
            # Reassembly consumes that union; it is not a new provider response.
            return [
                action for action in self.evidence["corporate_actions"] if start <= action_date(action) <= end
            ]
        return self.client.get_corporate_actions(symbol, start, end)

    def get_symbol_lookup(self, symbol):
        observed = self.client.observed_symbol_lookup(symbol)
        if observed is not None:
            return observed
        if symbol in self.symbols:
            return self.symbols[symbol]
        return self.client.get_symbol_lookup(symbol)


class WindowClient:
    # Completed mappings remain authoritative until their endpoint/action inputs
    # change. A newly acquired endpoint has the same authority on every route.
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

    def completed_lookup(self, identifier):
        previous = (
            self.completed_cusip_lookups.get(identifier)
            if self.start and identifier not in self.changed_cusips
            else None
        )
        if previous is not None and not self.client.mapping_current(self.previous["evidence"], previous):
            previous = None
        return previous

    def get_asset_lookup(self, identifier):
        previous = self.completed_lookup(identifier)
        result = self.client.get_asset_lookup(identifier, completed_lookup=previous)
        if previous is not None and identifier not in self.client.assets:
            self.retained_lookups[identifier] = result
        return result

    def get_symbol_lookup(self, symbol):
        lookup = self.client.get_symbol_lookup(symbol)
        if "source" in lookup:
            self.lookup_sources[symbol] = lookup["source"]
        return lookup

    def evidence_current(self, evidence):
        return self.client.evidence_current(evidence) and all(
            identifier in self.client.assets or self.completed_lookup(identifier) is not None
            for identifier in self.retained_lookups
        )

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
        # Complete the producer's dependency baseline before checking coherence.
        # A retained historical mapping may intentionally differ from today's
        # ticker occupant; dropping its accepted baseline would reject the same
        # valid lookup on every retry. Actual endpoint/action changes still
        # invalidate the retained lookup through WindowClient.
        evidence = _finish_evidence(evidence, window)
        if not window.evidence_current(evidence):
            continue  # A later symbol lookup changed an earlier retained dependency.
        return evidence
    raise LineageError("metadata lookup changes exceeded the name-change traversal limit")


def _finish_evidence(evidence, window):
    evidence.pop("api_request_count", None)
    observation = getattr(window.client, "observation", None)
    if observation is not None:
        evidence["catalogue_observed_at_utc"] = observation["catalogue_observed_at_utc"]
    elif window.previous and "catalogue_observed_at_utc" in window.previous["evidence"]:
        evidence["catalogue_observed_at_utc"] = window.previous["evidence"]["catalogue_observed_at_utc"]
    for lookup in evidence["asset_lookups"]:
        if lookup["lookup_kind"] == "symbol" and lookup["lookup"] in window.lookup_sources:
            lookup["source"] = window.lookup_sources[lookup["lookup"]]
    mapping_observations = {}
    for lookup in evidence["asset_lookups"]:
        if lookup["lookup_kind"] != "cusip":
            continue
        identifier = lookup["lookup"]
        prior = window.completed_cusip_lookups.get(identifier)
        baseline = mapping_dependencies(window.previous["evidence"], prior) if prior else {}
        baseline.update(mapping_dependencies(evidence, lookup))
        if identifier in window.client.assets:
            for label in baseline:
                observed = window.client.dependency_observation(label)
                if observed is not None:
                    baseline[label] = (observed.get("asset") or {}).get("id")
        elif prior:
            baseline.update(
                window.previous["evidence"].get("mapping_endpoint_observations", {}).get(identifier, {})
            )
        if baseline:
            mapping_observations[identifier] = baseline
    if mapping_observations:
        evidence["mapping_endpoint_observations"] = mapping_observations
    # build_identity_report emits this filesystem warning for logical descriptors too.
    # Preserve the in-memory handoff; do not turn this wording into file authentication.
    evidence["warnings"] = [
        warning
        for warning in evidence["warnings"]
        if not warning.startswith("Local gzip SHA-256 authentication was not performed;")
    ]
    return evidence


def acquire_unpriced(symbol, request, client, *, previous=None):
    """Query metadata independently of bars, without fabricating a pricing input."""
    local = {
        "present": False,
        "directory": None,
        "downloads": [],
    }
    retained = {"anchor_month": request["end_exclusive"][:7], "evidence": previous} if previous else None
    if previous:
        client = RetainedReportClient(client, previous)
    window = WindowClient(client, symbol, retained, previous is None)
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
