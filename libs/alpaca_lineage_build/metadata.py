"""Resume action acquisition at the previous active month; identity stays separate."""

from collections import defaultdict
from datetime import date, timedelta
from secrets import token_hex
from threading import Condition
from uuid import UUID

from libs.alpaca_data_api_solver import VERSION as PLAN_VERSION
from libs.alpaca_data_api_solver import build_plan, split_action, valid_cusip
from libs.alpaca_end_of_day_security_correlation.former_ticker import anchor_cusip
from libs.alpaca_pricing_download.contract import NEW_YORK, DownloadError, symbol_label, timestamp
from libs.alpaca_security_lineage.cusip_discovery import accept_window, dated_matches, missing_intervals
from libs.alpaca_security_lineage.inference_evidence import (
    provider_actions,
    provider_evidence,
    reject_assertions,
)
from libs.alpaca_security_lineage.lineage import (
    action_date,
    action_identity_key,
    build_identity_report,
    compact_asset,
    extract_cusips,
)
from libs.alpaca_security_lineage.settings import ACTION_TYPE_BY_RESPONSE_KEY, MAX_NAME_CHANGE_SYMBOLS
from libs.common.debug_trace import event

from .contract import LineageError, document_digest
from .exchange_changes import dating_due, next_dating_time, record_exchange_changes
from .terminal_cash_trade import FIELD as TERMINAL_CASH_TRADE_FIELD
from .terminal_cash_trade import carry_terminal_cash_trade
from .trade_tape import record_samples

DEPENDENCY_HANDOFF_CORRELATION_VERSION = 77


def retired_uuid_anchor(symbol, lookup):
    """Only a positive observation of the requested label can anchor its retirement."""
    asset = (lookup or {}).get("asset") or {}
    if not (
        (lookup or {}).get("lookup_kind") == "symbol"
        and lookup.get("lookup") == symbol
        and lookup.get("outcome") == "found"
        and lookup.get("http_status") == 200
        and asset.get("class") == "us_equity"
        and asset.get("symbol") == symbol
        and isinstance(asset.get("id"), str)
    ):
        return None
    try:
        UUID(asset["id"])
    except ValueError:
        return None
    return asset["id"]


def retired_uuid_lookup_required(receipt):
    if not receipt or not receipt.get("last_observed_symbol_lookup"):
        return False
    symbol = receipt.get("symbol")
    identifier = retired_uuid_anchor(symbol, receipt.get("last_observed_symbol_lookup"))
    if identifier is None:
        return False
    lookups = receipt["evidence"]["asset_lookups"]
    return any(
        item.get("lookup_kind") == "symbol"
        and item.get("lookup") == symbol
        and item.get("outcome") == "not_found"
        and item.get("http_status") == 404
        for item in lookups
    ) and not any(item.get("lookup_kind") == "asset_id" and item["lookup"] == identifier for item in lookups)


def retired_successor_observations(metadata_cache, generations, new_sources):
    """Lend a discovered successor's UUID into the enlarged group's first load."""
    if not new_sources:
        return {}
    observed = {
        item["asset"]["symbol"]: item["lookup"]
        for symbol, generation in generations.items()
        for full in (False, True)
        if (evidence := metadata_cache.get((symbol, generation, full))) is not None
        for item in evidence["asset_lookups"]
        if item["lookup_kind"] == "asset_id" and item.get("asset") and item["asset"]["symbol"] in new_sources
    }
    return observed if new_sources == observed.keys() else {}


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

    # Pricing owns price changes. An old correction cannot invalidate completed
    # action acquisition or identifier mappings; their own changed facts do that.
    return refresh or previous is not None and not reusable_metadata_versions(previous.get("versions"))


def action_requests(symbol, start, end, previous=None, *, refresh=False):
    """Known labels only; new labels get the original full range when discovered."""
    window = WindowClient(None, symbol, previous, refresh)
    known = previous["evidence"]["queried_symbols"] if previous else ()
    return [request for label in sorted({symbol, *known}) for request in window.requests(label, start, end)]


class EvidenceClient:
    """One run's coherent lookup cache. Conflicting action IDs fail before publication."""

    # DO NOT turn cache hits into provider rechecks. Completed observations are
    # authoritative within this operation; failed requests remain failures, and
    # conflicting/new evidence is handled by its owning acquisition path below.

    def __init__(self, client, *, observation=None, event_supplements=()):
        if event_supplements:
            raise LineageError("research event supplements are inadmissible in lineage inference")
        self.client = client
        self.assets = {}
        self.actions = {}
        self.trade_tape_samples = {}
        self.literal_bar_observations = {}
        self.otc_status_queries = {}
        self.cusip_action_windows = {}
        self.cusip_action_cursors = getattr(client, "cusip_action_cursors", {})
        self.cusip_action_index = {"by_asset": {}, "by_symbol": {}, "matches": {}, "failures": {}}
        self.otc_trade_access = {}
        self.initialize_trade_transport()
        self.action_intervals = defaultdict(dict)
        self.by_id = {}
        self.catalogue = None
        self.catalogue_assets = {}
        self.catalogue_uuid_conflicts = {}
        self.exact_lookup_reasons = {}
        self.catalogue_attempted = False
        self.catalogue_error = None
        self.catalogue_records = None
        self.catalogue_plan = None
        self.action_conflicts = set()
        self.observation = observation
        self.work_policy = None
        self.restricted_identifiers = set()
        self.retained_symbols = {}
        self._provider_evidence_views = {}
        if observation is not None:
            self._load_catalogue(observation["records"])
            self.catalogue_attempted = True

    def observe_catalogue(self):
        """One completed observation per operation, including recovery and discovery.

        The producer validates it once. Later phases consume that exact observation
        and its exact-lookup cache; a phase transition is not an invalidation.
        An attempted failed observation cannot become permission to request it again.
        """
        from .catalogue import completed_catalogue, observe_catalogue

        if self.observation is None:
            if self.catalogue_error is not None:
                raise self.catalogue_error
            self.catalogue_attempted = True
            try:
                if self.catalogue_records is None:
                    observation = observe_catalogue(self.client)
                    self._load_catalogue(observation["records"])
                else:
                    # Optional lookup prefetch accepts a partial catalogue. The
                    # full discovery producer must establish its stronger schema,
                    # but must not request the completed response again.
                    observation = completed_catalogue(self.catalogue_records, self.catalogue_plan)
            except (RuntimeError, OSError, ValueError) as error:
                self.catalogue_error = error
                raise
            self.observation = observation
        return self.observation

    @property
    def supports_plans(self):
        return callable(getattr(self.client, "execute_plan", None))

    @property
    def request_count(self):
        return self.client.request_count

    def initialize_trade_transport(self):
        self._trade_condition = Condition()
        self._cusip_action_condition = Condition()
        if hasattr(self.client, "cusip_action_cursors"):
            self.client.cusip_action_cursors = self.cusip_action_cursors
        self._trade_pending = set()
        self._otc_probing = self._otc_authorized = False
        self._otc_denied_client = None

    def get_literal_bar_observation(self, pricing, symbol, session, *, binding):
        from .former_ticker_bars import observe_literal_bar

        key = symbol, session, binding
        with self._trade_condition:
            while key in self._trade_pending:
                self._trade_condition.wait()
            if key in self.literal_bar_observations:
                return self.literal_bar_observations[key]
            if self.work_policy is not None:
                self.work_policy.check({symbol})
            self._trade_pending.add(key)
        try:
            event("metadata_literal_bar", symbol=symbol, session=session)
            value = observe_literal_bar(pricing, symbol, session)
            with self._trade_condition:
                self.literal_bar_observations[key] = value
            return value
        finally:
            with self._trade_condition:
                self._trade_pending.remove(key)
                self._trade_condition.notify_all()

    def _otc_denied(self):
        state = self.otc_trade_access
        if not state:
            return False
        binding = getattr(self.client, "trade_entitlement_scope", None)
        if (
            binding
            and binding(state["nonce"]) == state["scope"]
            or not binding
            and self._otc_denied_client is self.client
        ):
            return True
        # The transport received different credentials. The recorded failed
        # request is retained by its source, but cannot deny this account access.
        self.trade_tape_samples.pop(state["request_key"], None)
        self.otc_trade_access = {}
        self._otc_authorized = False
        return False

    def get_trade_tape_sample(
        self, symbol, session, *, asof="-", mapping_asset_id=None, feed="sip", through_session=None, sort="asc"
    ):
        key = (symbol, session, asof, mapping_asset_id, feed)
        if through_session is not None:
            key = (*key, through_session)
        if sort != "asc":
            if sort != "desc" or feed != "sip" or asof == "-" or through_session is None or not mapping_asset_id:
                raise ValueError(
                    "descending trade evidence requires a bounded dated SIP route and selected asset"
                )
            key = (*key, sort)
        otc = feed == "otc"
        with self._trade_condition:
            while key in self._trade_pending or otc and self._otc_probing:
                self._trade_condition.wait()
            denied = otc and self._otc_denied()
            if key in self.trade_tape_samples:
                return self._value(self.trade_tape_samples[key])
            if denied:
                return None  # No request was made on this source/date.
            acquire_sample = getattr(self.client, "get_trade_tape_sample", None)
            if not callable(acquire_sample):
                return None  # This metadata-only adapter has no trade observation.
            if self.work_policy is not None:
                self.work_policy.check({symbol})
            self._trade_pending.add(key)
            probe = otc and not self._otc_authorized
            if probe:
                self._otc_probing = True
        try:
            try:
                interval = {"through_session": through_session} if through_session is not None else {}
                if sort != "asc":
                    interval["sort"] = sort
                event(
                    "metadata_trade_tape_sample",
                    symbol=symbol,
                    session=session,
                    asof=asof,
                    feed=feed,
                    **interval,
                )
                result = acquire_sample(symbol, session, asof=asof, feed=feed, **interval)
            except (RuntimeError, OSError, ValueError) as error:
                error.request_identifiers = [symbol]
                result = error
            with self._trade_condition:
                self.trade_tape_samples[key] = result
                if otc and isinstance(result, dict):
                    if result.get("http_status") == 403 and result.get("reason") == "otc_subscription_denied":
                        nonce = token_hex(16)
                        binding = getattr(self.client, "trade_entitlement_scope", None)
                        self.otc_trade_access = {
                            "nonce": nonce,
                            "scope": binding(nonce) if binding else None,
                            "request_key": key,
                        }
                        self._otc_denied_client = self.client
                        self._otc_authorized = False
                    elif result.get("http_status") == 200:
                        self._otc_authorized = True
                return self._value(result)
        finally:
            with self._trade_condition:
                self._trade_pending.remove(key)
                if probe:
                    self._otc_probing = False
                self._trade_condition.notify_all()

    def get_asset_lookup(
        self, identifier, *, completed_lookup=None, lookup_kind="cusip", reason="exact_mapping_required"
    ):
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
                event("metadata_exact_lookup", identifier=identifier, lookup_kind=lookup_kind, reason=reason)
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
        return (
            observed
            if observed is not None
            else self.get_asset_lookup(
                symbol, lookup_kind="symbol", reason=self.exact_lookup_reasons.get(symbol, "catalogue_absent")
            )
        )

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
        resume = self.unfinished_retirement(symbol)
        if resume is not None and symbol not in (self.catalogue or {}):
            return {"outcome": "not_found", "http_status": 404, "asset": None}
        return None

    def unfinished_retirement(self, symbol):
        policy = self.work_policy
        item = (getattr(policy, "work", None) or {}).get(symbol, {})
        saved = item.get("retired_uuid_observation")
        if saved and saved["cutoff"] == policy.end and item.get("status") != "complete":
            return saved["lookup"]
        return None

    def observed_uuid_lookup(self, identifier):
        if identifier in self.assets:
            return self._value(self.assets[identifier])
        asset = self.catalogue_assets.get(identifier)
        if asset is not None:
            return {"outcome": "found", "http_status": 200, "asset": asset, "source": "asset_catalogue"}
        return None

    def evidence_current(self, evidence):
        """New producer reports must use this operation's acquired observations."""
        if self.observation and dating_due(
            next_dating_time(evidence), self.observation["catalogue_observed_at_utc"]
        ):
            return False
        for lookup in evidence["asset_lookups"]:
            identifier = lookup["lookup"]
            if lookup["lookup_kind"] == "asset_id":
                conflict = self.catalogue_uuid_conflicts.get(identifier)
                if conflict and lookup.get("catalogue_conflict") != conflict:
                    return False
            observed = (
                self.observed_symbol_lookup(identifier)
                if lookup["lookup_kind"] == "symbol"
                else self.observed_uuid_lookup(identifier)
                if lookup["lookup_kind"] == "asset_id"
                else self._value(self.assets[identifier])
                if identifier in self.assets
                else None
            )
            # An optional asset field appearing on a new licence cannot by
            # itself reopen a completed legacy observation or start a backfill.
            saved = lookup.get("asset")
            snapshot_cusip = "cusip" in (saved or {})
            if (
                snapshot_cusip
                and observed is not None
                and not valid_cusip((observed.get("asset") or {}).get("cusip"))
            ):
                saved = {k: v for k, v in saved.items() if k != "cusip"}
                snapshot_cusip = False
            if observed is not None and (
                lookup["outcome"] != observed["outcome"]
                or lookup["http_status"] != observed["http_status"]
                or saved
                != (
                    compact_asset(observed["asset"], include_cusip=snapshot_cusip)
                    if observed.get("asset")
                    else None
                )
            ):
                return False
            if lookup["lookup_kind"] == "cusip" and not self.mapping_current(evidence, lookup):
                return False
        return True

    def provider_view(self, evidence, *, local_report=None):
        """One provider-only interpretation of selected retained metadata per owner."""
        cache = getattr(self, "_provider_evidence_views", None)
        if cache is None:
            cache = self._provider_evidence_views = {}
        key = id(evidence)
        if key not in cache:
            projected = provider_evidence(evidence, local_report=local_report)
            self.carry_cusip_labels(projected)
            cache[key] = (evidence, projected)
        return cache[key][1]

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
                self.get_asset_lookup(symbol, lookup_kind="symbol", reason="conflicting_retained_observations")
        mappings = defaultdict(set)
        for evidence in evidences:
            for lookup in evidence.get("asset_lookups", ()):
                if lookup["lookup_kind"] == "cusip":
                    mappings[lookup["lookup"]].add((lookup.get("asset") or {}).get("id"))
        for identifier, identities in mappings.items():
            if len(identities) > 1:
                self.get_asset_lookup(identifier, reason="conflicting_retained_mappings")

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
        pending = sorted({s for s in symbols if s not in self.assets and self.observed_symbol_lookup(s) is None})
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
                    self.catalogue_plan = plan
                    self._load_catalogue(self._execute(action))
                except (RuntimeError, OSError, ValueError) as error:
                    self.catalogue_error = error
                    # An optional optimization cannot manufacture a missing-asset outcome.
                    self.catalogue = None
            else:
                identifier = action["deliver"][0]["identifier"]
                try:
                    event(
                        "metadata_exact_lookup",
                        identifier=identifier,
                        lookup_kind="symbol",
                        reason=self.exact_lookup_reasons.get(identifier, "catalogue_absent"),
                    )
                    self.assets[identifier] = self._execute(action)
                except (RuntimeError, OSError, ValueError) as error:
                    error.request_identifiers = [identifier]
                    self.assets[identifier] = error
        if any(action["strategy"] == "asset_catalogue" for action in plan["steps"][0]["actions"]):
            self.prefetch_symbols(pending)  # Remaining needs are now exact lookups.

    def _load_catalogue(self, assets):
        if not isinstance(assets, list):
            raise LineageError("asset catalogue must be an array")
        catalogue, assets_by_id, conflicts = {}, {}, {}
        for asset in assets:
            if not isinstance(asset, dict) or not isinstance(asset.get("symbol"), str) or not asset["symbol"]:
                raise LineageError("invalid asset catalogue entry")
            symbol = asset["symbol"]
            complete = not (
                any(
                    not isinstance(asset.get(key), str) or not asset[key]
                    for key in ("id", "class", "exchange", "name", "status")
                )
                or asset["class"] != "us_equity"
                or not isinstance(asset.get("tradable"), bool)
            )
            identifier = asset.get("id")
            compact = compact_asset(asset, include_cusip=True)
            if isinstance(identifier, str) and identifier:
                prior = assets_by_id.get(identifier)
                if not complete or identifier in assets_by_id and prior != compact:
                    distinct = conflicts.setdefault(identifier, set())
                    if prior is not None:
                        distinct.add(document_digest(prior))
                    distinct.add(document_digest(compact))
                    assets_by_id[identifier] = None
                else:
                    assets_by_id[identifier] = compact
            if not complete:
                catalogue[symbol] = None  # This label needs exact evidence; other valid entries remain useful.
                self.exact_lookup_reasons[symbol] = "catalogue_incomplete"
                continue
            if symbol in catalogue:
                self.exact_lookup_reasons[symbol] = "catalogue_ambiguous"
            catalogue[symbol] = None if symbol in catalogue else compact
        self.catalogue = catalogue
        self.catalogue_assets = assets_by_id
        self.catalogue_uuid_conflicts = {identifier: sorted(values) for identifier, values in conflicts.items()}
        self.catalogue_records = assets

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
        # Select maximal requested intervals for each symbol. A nested need is
        # delivered from the completed containing response, including all pages;
        # it must not become a second request merely because both were pending
        # when this plan was compiled. Overlap alone establishes no coverage.
        ordered = sorted(sorted(pending, key=lambda key: key[2], reverse=True), key=lambda key: key[:2])
        pending, upper = [], {}
        for key in ordered:
            symbol, start, end = key
            if end > upper.get(symbol, ""):
                pending.append(key)
                upper[symbol] = end
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
            physical = getattr(self.client, "execute_corporate_action_parts", None)
            if physical is not None:

                def completed_part(parameters, rows):
                    part = {
                        **action,
                        "request": {**action["request"], "params": parameters},
                    }
                    self._remember_action(part, rows=rows)

                def failed_part(parameters, error):
                    if not isinstance(error, (RuntimeError, OSError, ValueError)):
                        raise error
                    symbols = parameters["symbols"].split(",")
                    error.request_identifiers = symbols
                    self.actions.update(
                        ((symbol, parameters["start"], parameters["end"]), error) for symbol in symbols
                    )

                physical(action, on_completed=completed_part, on_failure=failed_part)
                continue
            self._remember_action(action, keys=keys)

    def _remember_action(self, action, *, keys=None, rows=None):
        params = action["request"]["params"]
        keys = keys or [(s, params["start"], params["end"]) for s in params["symbols"].split(",")]
        completed = {}
        try:
            self._acquire_action(action, completed, rows=rows)
        except (RuntimeError, OSError, ValueError) as error:
            # Recursive routing may finish an independent child before another
            # fails. Keep that completed evidence, and fail only unfinished keys.
            remaining = set(keys) - completed.keys()
            error.request_identifiers = sorted({key[0] for key in remaining})
            self.actions.update((key, error) for key in remaining)
        for key, values in completed.items():
            try:
                self._remember(key, values)
            except (RuntimeError, OSError, ValueError) as error:
                error.request_identifiers = [key[0]]
                self.actions[key] = error

    def _acquire_action(self, action, completed, *, rows=None):
        params = action["request"]["params"]
        symbols = params["symbols"].split(",")
        rows = self._execute(action) if rows is None else rows
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
                # This is an observed contradiction in the newly assembled
                # routing proof, not an independent transport peer failure.
                for symbol in symbols:
                    completed.pop((symbol, params["start"], params["end"]), None)
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
        reject_assertions(actions, where="provider corporate actions")
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
        additional = self.cusip_action_index["by_symbol"].get(symbol)
        if not additional:
            return actions
        by_id = {action_identity_key(action): action for action in actions}
        for identity, action in additional.items():
            if start <= action_date(action) <= end:
                if identity in self.action_conflicts:
                    raise LineageError(f"conflicting corporate-action identity: {identity}")
                by_id.setdefault(identity, action)
        return list(by_id.values())

    def carry_cusip_labels(self, report):
        index = self.cusip_action_index
        for item in report.get("archive_cusip_action_endpoints", ()):
            facts = index["by_asset"].setdefault((item["asset_id"], item["cusip"]), {})
            for match in item["matches"]:
                key = (match["action_type"], match["action_id"], match["symbol_field"])
                facts[key] = match
        discovery = report.get("cusip_action_discovery")
        if discovery and discovery["windows"]:
            intervals = self.cusip_action_windows.setdefault(discovery["cusip"], {})
            for window in discovery["windows"]:
                key = window["start"], window["end"]
                if key not in intervals:
                    self._lend_cusip_window(discovery["cusip"], window)
                    intervals[key] = window

    def _lend_cusip_window(self, cusip, window):
        rows = [item["action"] for item in window["actions"]]
        self._record_actions(rows)
        self._check_action_conflicts(rows)
        accepted = defaultdict(list)
        for match in window["matches"]:
            accepted[cusip, match["action_type"], match["action_id"]].append(match)
        self.cusip_action_index["matches"].update(accepted)
        for item in window["actions"]:
            symbol, action = item["ticker"], item["action"]
            if symbol:
                self.cusip_action_index["by_symbol"].setdefault(symbol, {})[action_identity_key(action)] = action

    def known_cusip_labels(self, binding, start, end):
        facts = self.cusip_action_index["by_asset"].get((binding["asset_id"], binding["cusip"]), {})
        return {m["symbol"] for m in facts.values() if start <= m["process_date"] <= end}

    def dated_cusip_matches(self, action, cusip):
        key = cusip, *action_identity_key(action)
        matches = self.cusip_action_index["matches"]
        if key not in matches:
            matches[key] = dated_matches(action, cusip)
        return matches[key]

    def remember_cusip_labels(self, report):
        for lookup in report["asset_lookups"]:
            asset = lookup.get("asset") or {}
            cusip = lookup["lookup"]
            if (
                lookup["lookup_kind"] != "cusip"
                or lookup.get("outcome") != "found"
                or lookup.get("http_status") != 200
                or not valid_cusip(cusip)
                or asset.get("cusip") not in (None, cusip)
                or asset.get("class") != "us_equity"
                or not asset.get("id")
                or asset.get("status") != "inactive"
                and asset.get("exchange") != "OTC"
            ):
                continue
            matches = [
                m
                for action in report["corporate_actions"]
                for m in self.dated_cusip_matches(action, cusip)
                if m["symbol"] != asset.get("symbol")
            ]
            if matches:
                report.setdefault("archive_cusip_action_endpoints", []).append(
                    {"asset_id": asset["id"], "cusip": cusip, "matches": matches}
                )
        self.carry_cusip_labels(report)

    def cusip_discovery_windows(
        self, binding, start, end, *, retained=(), already_covered=(), covered_by_symbol=False
    ):
        """Use only uncovered intervals of the singleton provider-native action filter."""
        cusip = binding["cusip"]
        # An unchanged licensed endpoint can recur in peer-group work. Accept
        # each complete interval once, including a scoped empty response or denial.
        with self._cusip_action_condition:
            intervals = self.cusip_action_windows.setdefault(cusip, {})
            for window in retained:
                key = window["start"], window["end"]
                if key not in intervals:
                    self._lend_cusip_window(cusip, window)
                    intervals[key] = window
            get_actions = getattr(self.client, "get_cusip_actions", None)
            if callable(get_actions) and not covered_by_symbol:
                for lower, upper in missing_intervals(start, end, [*intervals, *already_covered]):
                    failures = self.cusip_action_index["failures"]
                    scope = cusip, lower, upper
                    if scope in failures:
                        raise failures[scope]
                    if self.work_policy is not None:
                        self.work_policy.check({binding["symbol"], cusip})
                    event(
                        "metadata_cusip_actions", symbol=binding["symbol"], cusip=cusip, start=lower, end=upper
                    )
                    response = get_actions(cusip, lower, upper)
                    try:
                        window = accept_window(response, cusip, lower, upper)
                        self._lend_cusip_window(cusip, window)
                    except (ValueError, LineageError) as error:
                        failures[scope] = error
                        raise
                    intervals[lower, upper] = window
            return [window for (a, b), window in sorted(intervals.items()) if a <= end and start <= b]


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
        self.invalidated_symbols = set()

    def invalidate_symbols(self, symbols):
        self.invalidated_symbols.update(symbols)

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
        if self.client._covered((symbol, start, end)):
            # Prefetch has completed this operation's fresh window. A retained
            # report cannot mask newly acquired actions at an unchanged cutoff.
            return self.client.get_corporate_actions(symbol, start, end)
        if self.covered(symbol, start, end):
            # The completed report owns this union across its queried labels.
            # Reassembly consumes that union; it is not a new provider response.
            return [action for action in provider_actions(self.evidence) if start <= action_date(action) <= end]
        return self.client.get_corporate_actions(symbol, start, end)

    def get_symbol_lookup(self, symbol):
        observed = self.client.observed_symbol_lookup(symbol)
        if observed is not None:
            return observed
        if symbol in self.symbols and symbol not in self.invalidated_symbols:
            saved = self.symbols[symbol]
            if (
                saved.get("source") == "asset_catalogue"
                and saved.get("asset")
                and "observed_at_utc" not in saved
            ):
                stamp = self.evidence.get("catalogue_observed_at_utc")
                if stamp:
                    return {**saved, "observed_at_utc": stamp}
            return saved
        return self.client.get_symbol_lookup(symbol)


class WindowClient:
    # Completed mappings remain authoritative until their endpoint/action inputs
    # change. A newly acquired endpoint has the same authority on every route.
    def __init__(self, client, symbol, previous, refresh, *, status_view=None, allow_status_search=False):
        self.client, self.symbol = client, symbol
        self.previous = previous
        self.status_view = status_view
        self.allow_status_search = allow_status_search
        self.early_mapped_samples = None
        self.start = None if refresh or previous is None else previous["anchor_month"] + "-01"
        evidence = (previous or {}).get("evidence", {}) if client is not None else {}
        self.retained_otc_search = (evidence.get("otc_historical_trade_tape") or {}).get(
            "following_trade_search"
        )
        self.otc_limits_changed = refresh
        self.completed_actions = {action_identity_key(action): action for action in provider_actions(evidence)}
        self.completed_cusip_lookups = {
            lookup["lookup"]: lookup
            for lookup in evidence.get("asset_lookups", ())
            if lookup["lookup_kind"] == "cusip"
        }
        self.changed_cusips = set()
        self.trade_tape_inputs_changed = False
        self.retained_lookups = {}
        self.consumed_symbols = {}

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
        prior = self.completed_cusip_lookups.get(identifier)
        if self.retained_otc_search and prior and not self.otc_limits_changed:
            if (prior.get("asset") or {}).get("id") != (result.get("asset") or {}).get("id"):
                self.otc_limits_changed = any(
                    item.get("cusip") == identifier
                    for item in self.previous["evidence"].get("input_symbol_occupants", ())
                )
        if prior and not self.trade_tape_inputs_changed:
            before, after = prior.get("asset") or {}, result.get("asset") or {}
            if any(before.get(k) != after.get(k) for k in ("id", "symbol", "class", "status", "exchange")):
                self.trade_tape_inputs_changed = any(
                    anchor_cusip(a, self.symbol) == identifier for a in self.completed_actions.values()
                )
        if previous is not None and identifier not in self.client.assets:
            self.retained_lookups[identifier] = result
        return result

    def get_symbol_lookup(self, symbol):
        lookup = self.client.get_symbol_lookup(symbol)
        self.consumed_symbols[symbol] = lookup
        return lookup

    def cusip_discovery_windows(self, binding, start, end, *, covered_by_symbol):
        previous = (self.previous or {}).get("evidence", {})
        saved = previous.get("cusip_action_discovery")
        retained, covered = (), ()
        if previous and (not saved or all(saved.get(k) == value for k, value in binding.items())):
            retained = saved["windows"] if saved else ()
            # The previous scope was already assessed. In particular, a
            # legacy selector did not request this route. Never backfill it.
            query = previous["query"]
            covered = [(query["corporate_actions_start"], query["corporate_actions_end"])]
        return self.client.cusip_discovery_windows(
            binding, start, end, retained=retained, already_covered=covered, covered_by_symbol=covered_by_symbol
        )

    def remember_cusip_labels(self, report):
        self.client.remember_cusip_labels(report)

    def otc_status_query(self, own, start, end):
        from .otc_status import discover

        return discover(self, own, start, end)

    def known_cusip_labels(self, binding, start, end):
        return self.client.known_cusip_labels(binding, start, end)

    def dated_cusip_matches(self, action, cusip):
        return self.client.dated_cusip_matches(action, cusip)

    def retired_asset_lookup(self):
        """Join the last positive own-label observation to its exact asset UUID."""
        from .projection import last_observed_symbol_lookup

        if self.previous is None or "symbol" not in self.previous:
            return None
        positive = last_observed_symbol_lookup(self.previous)
        identifier = retired_uuid_anchor(self.symbol, positive)
        if identifier is None:
            return None
        current = self.client.observed_uuid_lookup(identifier)
        conflict = self.client.catalogue_uuid_conflicts.get(identifier)
        unfinished = self.client.unfinished_retirement(self.symbol)
        if (
            unfinished is not None
            and unfinished.get("retained_symbol_lookup") == positive
            and current is None
            and (conflict is None or unfinished.get("catalogue_conflict") == conflict)
        ):
            return unfinished
        if current is None:
            prior = next(
                (
                    item
                    for item in self.previous["evidence"]["asset_lookups"]
                    if item["lookup_kind"] == "asset_id" and item["lookup"] == identifier
                ),
                None,
            )
            if (
                prior
                and prior.get("retained_symbol_lookup") == positive
                and (conflict is None or prior.get("catalogue_conflict") == conflict)
            ):
                if prior.get("source") == "asset_catalogue" and "observed_at_utc" not in prior:
                    stamp = self.previous["evidence"].get("catalogue_observed_at_utc")
                    return {**prior, "observed_at_utc": stamp}
                return prior
            current = self.client.get_asset_lookup(identifier, lookup_kind="asset_id", reason="retired_symbol")
        asset = current.get("asset")
        positive_asset = (
            current.get("outcome") == "found"
            and current.get("http_status") == 200
            and isinstance(asset, dict)
            and asset.get("id") == identifier
            and asset.get("class") == "us_equity"
            and isinstance(asset.get("symbol"), str)
            and asset["symbol"]
        )
        if not positive_asset and not (
            asset is None and current.get("outcome") == "not_found" and current.get("http_status") == 404
        ):
            error = LineageError(
                f"{self.symbol}: exact UUID lookup returned an incomplete or incompatible response"
            )
            error.request_identifiers = [identifier]
            raise error
        observed_at = current.get("observed_at_utc")
        if observed_at is None and current.get("source") == "asset_catalogue":
            observed_at = (self.client.observation or {}).get("catalogue_observed_at_utc")
        lookup = {
            "lookup_kind": "asset_id",
            "lookup": identifier,
            "outcome": current["outcome"],
            "http_status": current["http_status"],
            "asset": compact_asset(asset) if asset else None,
            "found": asset is not None,
            "source": current.get("source", "exact_uuid"),
            **({"observed_at_utc": observed_at} if observed_at else {}),
            "retained_symbol_lookup": positive,
            **({"catalogue_conflict": conflict} if conflict else {}),
        }
        policy = self.client.work_policy
        work = (getattr(policy, "work", None) or {}).get(self.symbol)
        if work and lookup != unfinished:
            from .universe_state import accept_retired_uuid

            saved = {"cutoff": policy.end, "lookup": lookup}
            accept_retired_uuid(policy.store, self.symbol, work["obligation_id"], saved)
            work["retired_uuid_observation"] = saved
        return lookup

    def evidence_current(self, evidence):
        return self.client.evidence_current(evidence) and all(
            identifier in self.client.assets or self.completed_lookup(identifier) is not None
            for identifier in self.retained_lookups
        )

    def requests(self, symbol, start, end):
        known = self.previous["evidence"]["queried_symbols"] if self.previous else ()
        if not self.start or symbol not in known:
            return [(symbol, start, end)]
        # The completed query owns historical coverage, independently of prices.
        # Extend only its uncovered prefix; never reacquire the accepted middle.
        accepted_start = self.previous["evidence"].get("query", {}).get("corporate_actions_start")
        if not accepted_start:
            raise LineageError("missing retained action coverage; run explicit metadata refresh")
        requests = []
        if start < accepted_start:
            prefix_end = min(end, (date.fromisoformat(accepted_start) - timedelta(days=1)).isoformat())
            requests.append((symbol, start, prefix_end))
        lower = max(start, self.start, accepted_start)
        if lower <= end:
            requests.append((symbol, lower, end))
        return requests

    def prefetch_corporate_actions(self, symbols, start, end):
        self.client.prefetch_corporate_actions(
            [request for symbol in symbols for request in self.requests(symbol, start, end)]
        )

    def get_corporate_actions(self, symbol, start, end):
        # Planning and execution consume the same disjoint windows.
        current = [
            action
            for request in self.requests(symbol, start, end)
            for action in self.client.get_corporate_actions(*request)
        ]
        for action in current:
            accepted = self.completed_actions.get(action_identity_key(action))
            if accepted != action:
                if self.retained_otc_search and not self.otc_limits_changed:
                    self.otc_limits_changed = self.symbol in action_symbols([action, accepted or {}])
                if not self.trade_tape_inputs_changed and any(
                    anchor_cusip(item, self.symbol) for item in (action, accepted or {})
                ):
                    self.trade_tape_inputs_changed = True
                self.changed_cusips.update(extract_cusips([action, *([accepted] if accepted else [])]))
                if isinstance(self.client, RetainedReportClient):
                    labels = action_symbols([action, *([accepted] if accepted else [])])
                    self.client.invalidate_symbols(labels)
                    for label in sorted(labels & self.consumed_symbols.keys()):
                        old = self.consumed_symbols[label]
                        fresh = self.client.get_symbol_lookup(label)
                        if any(fresh.get(key) != old.get(key) for key in ("outcome", "http_status", "asset")):
                            raise _CompletedLookupChanged("new action changed a retained symbol lookup")
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
                for action in provider_actions(self.previous["evidence"])
                if action_date(action) < self.start
            ]
        return historical + current


def retained_metadata(view, client, previous, *, refresh=False):
    """Select accepted metadata for bounded retained-input recovery.

    The pricing request bounds all possible observations. Its exclusive UTC
    endpoint can advance beyond the last inclusive action-query date without
    admitting a later market date. Keep the actual completed query unchanged;
    never manufacture fresh action coverage or ask for it again on recovery.
    """
    if refresh or previous is None:
        return None
    evidence = client.provider_view(previous["evidence"], local_report=view.local)
    previous = previous if evidence is previous["evidence"] else {**previous, "evidence": evidence}
    query = evidence["query"]
    if (
        query.get("corporate_actions_data_quality") != "complete"
        or view.source["history_start"][:10] < query["corporate_actions_start"]
        or timestamp(view.source["latest_request"]["end_exclusive"]).astimezone(NEW_YORK).date().isoformat()
        > query["corporate_actions_end"]
    ):
        return None
    if not retired_uuid_lookup_required(previous) and client.evidence_current(evidence):
        if TERMINAL_CASH_TRADE_FIELD not in evidence:
            return evidence
        return carry_terminal_cash_trade(view, evidence, previous_generation=previous.get("source_sha256"))
    # Changed descriptions or operator facts need their new metadata projection,
    # while the already completed provider action union keeps its exact scope.
    return acquire(
        view,
        RetainedReportClient(client, evidence),
        previous,
        query_dates=(query["corporate_actions_start"], query["corporate_actions_end"]),
        observe_new_tapes=False,
    )


def _exchange_predecessor(symbol, client, previous, auxiliary_evidence):
    if previous is None and auxiliary_evidence:
        return {"symbol": symbol, "evidence": client.provider_view(auxiliary_evidence)}
    return previous


def acquire(
    view,
    client,
    previous=None,
    *,
    refresh=False,
    query_dates=None,
    observe_new_tapes=True,
    previous_exchange_evidence=None,
):
    if previous:
        evidence = client.provider_view(previous["evidence"], local_report=view.local)
        if evidence is not previous["evidence"]:
            previous = {**previous, "evidence": evidence}
    for _ in range(MAX_NAME_CHANGE_SYMBOLS + 1):
        window = WindowClient(
            client, view.symbol, previous, refresh, status_view=view, allow_status_search=observe_new_tapes
        )
        try:
            # DO NOT REMOVE WITHOUT USER AUTH!#
            # The bar mapping date can be historical. Corporate-action discovery
            # still covers the complete accepted observation cutoff, including
            # the retirement that required that mapping in the first place.
            evidence = build_identity_report(
                view.symbol,
                view.local,
                window,
                query_dates=query_dates
                or (
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
        exchange_prior = _exchange_predecessor(view.symbol, client, previous, previous_exchange_evidence)
        record_exchange_changes(evidence, exchange_prior, window.client, observe_new_tapes=observe_new_tapes)
        if observe_new_tapes:
            record_samples(view, evidence, window, refresh=refresh)
        return evidence
    raise LineageError("metadata lookup changes exceeded the name-change traversal limit")


def _finish_evidence(evidence, window):
    evidence.pop("api_request_count", None)
    if window.previous:
        # Carry each observation with its original ticker, date, feed and mapping.
        for key in (
            "historical_trade_tape",
            "mapped_historical_trade_tape",
            "otc_historical_trade_tape",
            "former_ticker_literal_bars",
        ):
            if key in window.previous["evidence"]:
                evidence[key] = window.previous["evidence"][key]
        if window.status_view is not None and TERMINAL_CASH_TRADE_FIELD in window.previous["evidence"]:
            evidence[TERMINAL_CASH_TRADE_FIELD] = window.previous["evidence"][TERMINAL_CASH_TRADE_FIELD]
            evidence = carry_terminal_cash_trade(window.status_view, evidence, window)
    observation = getattr(window.client, "observation", None)
    if observation is not None:
        evidence["catalogue_observed_at_utc"] = observation["catalogue_observed_at_utc"]
    elif window.previous and "catalogue_observed_at_utc" in window.previous["evidence"]:
        evidence["catalogue_observed_at_utc"] = window.previous["evidence"]["catalogue_observed_at_utc"]
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


def acquire_unpriced(symbol, request, client, *, previous=None, previous_exchange_evidence=None):
    """Query metadata independently of bars, without fabricating a pricing input."""
    local = {
        "present": False,
        "directory": None,
        "downloads": [],
    }
    retained = {"anchor_month": request["end_exclusive"][:7], "evidence": previous} if previous else None
    if previous:
        previous = client.provider_view(previous, local_report=local)
        retained = {**retained, "evidence": previous}
        client = RetainedReportClient(client, previous)
    for _ in range(MAX_NAME_CHANGE_SYMBOLS + 1):
        window = WindowClient(client, symbol, retained, previous is None)
        try:
            evidence = build_identity_report(
                symbol,
                local,
                window,
                query_dates=(request["start"][:10], request["asof"]),
            )
        except _CompletedLookupChanged:
            continue
        evidence = _finish_evidence(evidence, window)
        if window.evidence_current(evidence):
            exchange_prior = _exchange_predecessor(symbol, client, retained, previous_exchange_evidence)
            record_exchange_changes(evidence, exchange_prior, window.client)
            return evidence
    raise LineageError("metadata lookup changes exceeded the name-change traversal limit")


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
