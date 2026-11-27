import hmac
import json
import math
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

from libs.alpaca_data_api_solver import (
    ACTION_URL,
    ASSET_URL,
    NEW_YORK,
    READABLE_ACTION_PLAN_VERSIONS,
    build_plan,
    cusip_action_request,
    instant,
    market_split_parameters,
    trade_tape_request,
    validate_version,
)
from libs.common.provider_http import (
    RETRY,
    fetch_symbol_batches,
    open_provider_request,
    paginated_pages,
    request_with_policy,
)

from .settings import (
    ACTION_TYPE_BY_RESPONSE_KEY,
    MIN_REQUEST_INTERVAL_SECONDS,
)


class AlpacaIdentityRequestRateLimiter:
    def __init__(self) -> None:
        self.next_request_monotonic = 0.0
        self._request_condition = threading.Condition()

    def wait_until_request_allowed(self) -> None:
        with self._request_condition:
            while True:
                delay_seconds = self.next_request_monotonic - time.monotonic()
                if delay_seconds <= 0:
                    self.next_request_monotonic = time.monotonic() + MIN_REQUEST_INTERVAL_SECONDS
                    return
                self._request_condition.wait(timeout=delay_seconds)

    def defer_requests_for_seconds(self, delay_seconds: float) -> None:
        with self._request_condition:
            self.next_request_monotonic = max(
                self.next_request_monotonic,
                time.monotonic() + delay_seconds,
            )
            self._request_condition.notify_all()


class IdentityHttpError(RuntimeError):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _nonfinite(_value):
    raise ValueError("non-finite JSON number")


def _finite_float(value):
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("non-finite JSON number")
    return result


def rate_limit_delay_seconds(response_headers, default_seconds: float = 60.0) -> float:
    if response_headers is None:
        return default_seconds

    retry_after = response_headers.get("Retry-After")
    if retry_after:
        try:
            return max(float(retry_after), 0.0) + 0.25
        except ValueError:
            pass

    reset_timestamp = response_headers.get("X-RateLimit-Reset")
    if reset_timestamp:
        try:
            return max(float(reset_timestamp) - time.time(), 0.0) + 0.25
        except ValueError:
            pass
    return default_seconds


class AlpacaIdentityClient:
    def __init__(
        self,
        api_key_id: str,
        api_secret_key: str,
        *,
        rate_limiter: AlpacaIdentityRequestRateLimiter | None = None,
    ) -> None:
        validate_version()
        self.headers = {
            "APCA-API-KEY-ID": api_key_id,
            "APCA-API-SECRET-KEY": api_secret_key,
        }
        self.rate_limiter = rate_limiter or AlpacaIdentityRequestRateLimiter()
        self.request_count = 0
        self._seeded_asset_lookups: dict[str, dict[str, Any]] = {}
        self.cusip_action_cursors = {}

    def trade_entitlement_scope(self, nonce):
        """Operation-salted credential binding for a transported OTC subscription denial."""
        material = f"alpaca-otc-entitlement\0{nonce}\0{self.headers['APCA-API-KEY-ID']}".encode()
        return hmac.digest(self.headers["APCA-API-SECRET-KEY"].encode(), material, "sha256").hex()

    def market_observation_handoff_token(self, digest):
        """Bind a preparation's closed physical responses to their original credentials."""
        material = f"alpaca-canonical-market-handoff\0{digest}\0{self.headers['APCA-API-KEY-ID']}".encode()
        return hmac.digest(self.headers["APCA-API-SECRET-KEY"].encode(), material, "sha256").hex()

    def seed_asset_lookup(
        self,
        identifier: str,
        *,
        asset: dict[str, Any] | None,
        http_status: int,
    ) -> None:
        """Consume a prior provider lookup as this client's identical lookup result."""
        # TRUST BOUNDARY: the caller supplies a completed exact provider lookup.
        # Validate its outcome here, then consume it once; DO NOT issue another
        # HTTP request merely to confirm the same observation.
        if identifier in self._seeded_asset_lookups:
            raise ValueError(f"asset lookup was seeded twice: {identifier}")
        if (asset is None and http_status != 404) or (asset is not None and http_status != 200):
            raise ValueError(f"asset lookup seed has an incoherent HTTP status: {identifier}")
        self._seeded_asset_lookups[identifier] = {
            "outcome": "not_found" if asset is None else "found",
            "http_status": http_status,
            "asset": asset,
        }
        self.request_count += 1

    def request_json(self, url: str, *, allow_not_found: bool = False, array: bool = False):
        http_request = urllib.request.Request(url, headers=self.headers)

        def before_attempt():
            self.rate_limiter.wait_until_request_allowed()
            self.request_count += 1

        def read_response(http_response):
            try:
                response_document = json.load(
                    http_response,
                    object_pairs_hook=_unique_object,
                    parse_constant=_nonfinite,
                    parse_float=_finite_float,
                )
            except (ValueError, UnicodeError) as json_error:
                raise RuntimeError(f"Alpaca returned invalid JSON for {url}") from json_error
            if http_response.headers.get("X-RateLimit-Remaining") == "0":
                self.rate_limiter.defer_requests_for_seconds(rate_limit_delay_seconds(http_response.headers))
            if not isinstance(response_document, list if array else dict):
                raise RuntimeError(f"Alpaca returned an invalid response shape for {url}")
            return response_document

        def on_error(error, attempt, last):
            if isinstance(error, urllib.error.HTTPError):
                response_body = error.read().decode("utf-8", "replace")
                if error.code == 404 and allow_not_found:
                    return None
                if error.code == 429 and not last:
                    self.rate_limiter.defer_requests_for_seconds(rate_limit_delay_seconds(error.headers))
                    return RETRY
                failure = IdentityHttpError(
                    error.code, f"Alpaca returned HTTP {error.code} for {url}: {response_body}"
                )
                if error.code == 403:
                    try:
                        reason = json.loads(response_body)
                    except ValueError:
                        reason = None
                    failure.otc_subscription_denied = isinstance(reason, dict) and (
                        reason.get("message") == "subscription does not permit querying OTC data"
                    )
                raise failure from error
            if isinstance(error, urllib.error.URLError):
                raise RuntimeError(f"Alpaca request failed for {url}: {error.reason}") from error
            raise error

        return request_with_policy(
            http_request,
            opener=open_provider_request,
            timeout=60,
            before_attempt=before_attempt,
            read_response=read_response,
            on_error=on_error,
        )

    def get_asset_lookup(self, identifier: str) -> dict[str, Any]:
        """Return an asset plus the explicit HTTP outcome used as evidence."""
        # Negative lookups are evidence too; keep the outcome and HTTP status with the asset.
        seeded = self._seeded_asset_lookups.pop(identifier, None)
        if seeded is not None:
            return seeded
        plan = build_plan(
            {
                "steps": [
                    {"needs": [{"kind": "asset_lookup", "why": "identity_lookup", "identifiers": [identifier]}]}
                ]
            }
        )
        return self.execute_plan(plan)[0][0]

    def get_corporate_actions(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        plan = build_plan(
            {
                "steps": [
                    {
                        "needs": [
                            {
                                "kind": "corporate_actions",
                                "why": "lineage_evidence",
                                "symbols": [symbol],
                                "start": start_date,
                                "end": end_date,
                            }
                        ]
                    }
                ]
            }
        )
        return self.execute_plan(plan)[0][0]

    def get_market_splits(self, session):
        plan = build_plan(
            {
                "steps": [
                    {
                        "needs": [
                            {"kind": "market_split_session", "why": "historical_price_basis", "session": session}
                        ]
                    }
                ]
            }
        )
        return self.execute_plan(plan)[0][0]

    def get_cusip_actions(self, cusip, start, end):
        need = {
            "kind": "corporate_actions_by_cusip",
            "why": "lineage_evidence",
            "cusip": cusip,
            "start": start,
            "end": end,
        }
        return self.execute_plan(build_plan({"steps": [{"needs": [need]}]}))[0][0]

    def get_trade_tape_sample(self, symbol, session, *, asof="-", feed="sip", through_session=None, sort="asc"):
        need = {
            "kind": "historical_trade_tape",
            "why": "dated_ticker_tape",
            "symbol": symbol,
            "session": session,
            "asof": asof,
            "feed": feed,
            **({"through_session": through_session} if through_session is not None else {}),
            **({"sort": sort} if sort != "asc" else {}),
        }
        return self.execute_plan(build_plan({"steps": [{"needs": [need]}]}))[0][0]

    def execute_plan(self, plan, *, metrics=None):
        """Execute ordered metadata actions; results retain step/action positions."""
        # The solver owns request construction, but this credential-bearing
        # executor owns allowed strategies/targets and complete pagination.
        # Plan validity is not permission to send credentials to an arbitrary URL.
        if plan.get("schema_version") not in READABLE_ACTION_PLAN_VERSIONS:
            raise RuntimeError("unsupported API action-plan version")
        for step in plan["steps"]:
            for action in step["actions"]:
                # Plans and executor endpoint checks share the solver's URL constants.
                strategy, url = action["strategy"], action["request"]["url"]
                if strategy == "corporate_actions_by_cusip":
                    expected = cusip_action_request(action.get("cusip"), action.get("start"), action.get("end"))
                    if action["request"] != expected or action.get("pagination") != {
                        "parameter": "page_token",
                        "response_field": "next_page_token",
                    }:
                        raise ValueError("CUSIP action plan exceeds its selected filter or dates")
                    continue
                if strategy == "historical_trade_tape":
                    if "through_session" in action and action["through_session"] is None:
                        raise ValueError("trade tape interval requires an explicit final session")
                    expected = trade_tape_request(
                        action.get("symbol"),
                        action.get("session"),
                        action.get("asof"),
                        action.get("feed"),
                        through_session=action.get("through_session"),
                        sort=action.get("sort", "asc"),
                    )
                    if action["request"] != expected or "pagination" in action:
                        raise ValueError("trade tape plan exceeds its selected date range or mapping")
                    continue
                if not (
                    strategy == "asset_lookup"
                    and url.startswith(ASSET_URL + "/")
                    or strategy in ("corporate_actions_batch", "market_split_session")
                    and url == ACTION_URL
                    or strategy == "asset_catalogue"
                    and url == ASSET_URL
                ):
                    raise RuntimeError("metadata executor received a different API strategy")
                if strategy == "market_split_session":
                    params = action["request"]["params"]
                    if params != market_split_parameters(params.get("start")) or action.get("pagination") != {
                        "parameter": "page_token",
                        "response_field": "next_page_token",
                    }:
                        raise ValueError("market split plan exceeds its single-session scope")
        results = []
        for step in plan["steps"]:
            values = []
            for action in step["actions"]:
                if action["strategy"] == "asset_lookup":
                    identifier = urllib.parse.unquote(action["request"]["url"][len(ASSET_URL) + 1 :])
                    seeded = self._seeded_asset_lookups.pop(identifier, None)
                    if seeded is not None:
                        values.append(seeded)
                        continue
                    asset = self.request_json(action["request"]["url"], allow_not_found=True)
                    values.append(
                        {
                            "outcome": "not_found" if asset is None else "found",
                            "http_status": 404 if asset is None else 200,
                            "asset": asset,
                            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                elif action["strategy"] == "historical_trade_tape":
                    values.append(self._execute_trade_tape(action))
                elif action["strategy"] == "market_split_session":
                    values.append(self._execute_corporate_action(action, metrics=metrics))
                elif action["strategy"] == "corporate_actions_by_cusip":
                    values.append(self._execute_cusip_actions(action, metrics=metrics))
                elif action["strategy"] == "asset_catalogue":
                    values.append(
                        self.request_json(
                            action["request"]["url"] + "?" + urllib.parse.urlencode(action["request"]["params"]),
                            array=True,
                        )
                    )
                else:
                    values.append(self._corporate_action_with_splitting(action, metrics=metrics))
            results.append(values)
        return results

    def _execute_trade_tape(self, action):
        request = action["request"]
        params = request["params"]
        sample = {
            "symbol": action["symbol"],
            "session": action["session"],
            "feed": params["feed"],
            "asof": params["asof"],
            "start": params["start"],
            "end": params["end"],
            "limit": params["limit"],
            **({"through_session": action["through_session"]} if "through_session" in action else {}),
            **({"sort": params["sort"]} if params["sort"] != "asc" else {}),
        }
        try:
            document = self.request_json(request["url"] + "?" + urllib.parse.urlencode(params))
        except IdentityHttpError as error:
            if error.status not in (403, 404):
                raise
            if (
                error.status == 403
                and params["feed"] == "otc"
                and getattr(error, "otc_subscription_denied", False)
            ):
                return {
                    **sample,
                    "http_status": 403,
                    "outcome": "unavailable",
                    "reason": "otc_subscription_denied",
                }
            return {**sample, "http_status": error.status, "outcome": "unavailable"}
        trades = document.get("trades")
        # A completed empty native page can explicitly contain "trades": null.
        if trades is None and "trades" in document:
            trades = []
        if document.get("symbol") != sample["symbol"] or not isinstance(trades, list) or len(trades) > 1:
            raise RuntimeError("Alpaca returned an invalid trade tape sample for the requested route")
        sample["returned_symbol"] = document["symbol"]
        if not trades:
            if document.get("next_page_token") not in (None, ""):
                raise RuntimeError("empty first trade page does not establish an empty interval")
            return {**sample, "http_status": 200, "outcome": "no_trades"}
        trade = trades[0]
        if not isinstance(trade, dict) or not isinstance(trade.get("t"), str) or len(trade["t"]) <= 10:
            raise RuntimeError("Alpaca returned a trade without a dated timestamp")
        try:
            observed = instant(trade["t"])
        except ValueError as error:
            raise RuntimeError("Alpaca returned an invalid trade timestamp") from error
        if not instant(params["start"]) <= observed <= instant(params["end"]):
            raise RuntimeError("Alpaca returned a trade outside the selected session")
        if any(trade.get(field) is not None and not isinstance(trade[field], str) for field in ("z", "x")):
            raise RuntimeError("Alpaca returned an invalid trade tape or execution venue field")
        for key, name in (("p", "price"), ("s", "size")):
            value = trade.get(key)
            if value is not None and (
                type(value) not in (int, float)
                or value < 0
                or isinstance(value, float)
                and not math.isfinite(value)
            ):
                raise RuntimeError(f"Alpaca returned an invalid trade {name}")
        conditions = trade.get("c")
        if conditions is not None and (
            not isinstance(conditions, list) or any(not isinstance(code, str) for code in conditions)
        ):
            raise RuntimeError("Alpaca returned invalid trade sale conditions")
        return {
            **sample,
            "http_status": 200,
            "outcome": "observed",
            "trade": {key: trade[key] for key in ("t", "z", "x", "p", "s", "c") if key in trade},
            **(
                {"observed_session": observed.astimezone(NEW_YORK).date().isoformat()}
                if "through_session" in action
                else {}
            ),
            "tape_network": (
                {"A": "CTA network A", "B": "CTA network B", "C": "Nasdaq UTP"}.get(trade.get("z"), "unknown")
                if params["feed"] == "sip"
                else "unknown"
            ),
        }

    def execute_corporate_action_parts(self, action, *, on_completed, on_failure):
        """Lend completed physical responses before independent subsets run."""
        if action["strategy"] != "corporate_actions_batch" or action["request"]["url"] != ACTION_URL:
            raise RuntimeError("metadata executor received a different API strategy")
        return self._corporate_action_with_splitting(action, on_completed=on_completed, on_failure=on_failure)

    def _corporate_action_with_splitting(self, action, *, metrics=None, on_completed=None, on_failure=None):
        return [
            row
            for batch in fetch_symbol_batches(
                action["request"]["params"],
                lambda params: self._execute_corporate_action(action, params, metrics=metrics),
                status_code=lambda error: error.status if isinstance(error, IdentityHttpError) else None,
                metrics=metrics,
                on_completed=on_completed,
                on_failure=on_failure,
            )
            for row in batch
        ]

    def _execute_corporate_action(self, api_action, parameters=None, *, metrics=None):
        normalized_actions = []

        def get_page(params):
            return self.request_json(f"{api_action['request']['url']}?{urllib.parse.urlencode(params)}")

        for response_document in paginated_pages(
            parameters if parameters is not None else api_action["request"]["params"],
            api_action["pagination"],
            get_page,
            error_type=RuntimeError,
            metrics=metrics,
        ):
            normalized_actions.extend(_normalize_actions(response_document))

        return normalized_actions

    def _execute_cusip_actions(self, action, *, metrics=None):
        key = (action["cusip"], action["start"], action["end"])
        saved = self.cusip_action_cursors.setdefault(key, {"actions": [], "tokens": [], "next": None})
        params = dict(action["request"]["params"])
        while True:
            if saved["next"] is not None:
                params["page_token"] = saved["next"]
            if metrics is not None:
                metrics["page_requests"] = metrics.get("page_requests", 0) + 1
            try:
                document = self.request_json(ACTION_URL + "?" + urllib.parse.urlencode(params))
            except IdentityHttpError as error:
                if error.status != 403:
                    raise
                # A denied interval supplies no positive or negative action evidence,
                # even if preceding pages succeeded.
                del self.cusip_action_cursors[key]
                return {"outcome": "denied", "http_status": 403, "actions": []}
            rows = _normalize_actions(document)
            token = document.get("next_page_token")
            if token is not None and (not isinstance(token, str) or not token or token in saved["tokens"]):
                raise RuntimeError("invalid or repeated CUSIP action next_page_token")
            saved["actions"].extend(rows)
            if token is None:
                del self.cusip_action_cursors[key]
                return {"outcome": "complete", "http_status": 200, "actions": saved["actions"]}
            saved["tokens"].append(token)
            saved["next"] = token


def _normalize_actions(response_document):
    if not isinstance(response_document, dict):
        raise RuntimeError("corporate-actions request unexpectedly returned no response")
    action_groups = response_document.get("corporate_actions")
    if not isinstance(action_groups, dict):
        raise RuntimeError("corporate-actions response has no corporate_actions object")
    normalized = []
    for response_key, actions in action_groups.items():
        if not isinstance(actions, list):
            raise RuntimeError(f"corporate-actions group {response_key!r} is not a list")
        action_type = ACTION_TYPE_BY_RESPONSE_KEY.get(response_key, response_key)
        for action in actions:
            if not isinstance(action, dict):
                raise RuntimeError(f"corporate-actions group {response_key!r} contains a non-object")
            normalized.append({**action, "action_type": action_type})
    return normalized
