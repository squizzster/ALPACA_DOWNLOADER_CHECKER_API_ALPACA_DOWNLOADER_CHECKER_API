import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from libs.alpaca_data_api_solver import ACTION_URL, ASSET_URL, build_plan, split_action, validate_version
from libs.alpaca_data_api_solver import VERSION as PLAN_VERSION

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

    def seed_asset_lookup(
        self,
        identifier: str,
        *,
        asset: dict[str, Any] | None,
        http_status: int,
    ) -> None:
        """Consume a prior provider lookup as this client's identical lookup result."""
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
        for retry_attempt in range(6):
            self.rate_limiter.wait_until_request_allowed()
            self.request_count += 1
            try:
                with urllib.request.urlopen(http_request, timeout=60) as http_response:
                    try:
                        response_document = json.load(http_response)
                    except (json.JSONDecodeError, UnicodeError) as json_error:
                        raise RuntimeError(f"Alpaca returned invalid JSON for {url}") from json_error
                    if http_response.headers.get("X-RateLimit-Remaining") == "0":
                        self.rate_limiter.defer_requests_for_seconds(
                            rate_limit_delay_seconds(http_response.headers)
                        )
                if not isinstance(response_document, list if array else dict):
                    raise RuntimeError(f"Alpaca returned an invalid response shape for {url}")
                return response_document
            except urllib.error.HTTPError as http_error:
                try:
                    response_body = http_error.read().decode("utf-8", "replace")
                    if http_error.code == 404 and allow_not_found:
                        return None
                    if http_error.code == 429 and retry_attempt < 5:
                        self.rate_limiter.defer_requests_for_seconds(
                            rate_limit_delay_seconds(http_error.headers)
                        )
                        continue
                    raise IdentityHttpError(
                        http_error.code, f"Alpaca returned HTTP {http_error.code} for {url}: {response_body}"
                    ) from http_error
                finally:
                    http_error.close()
            except urllib.error.URLError as request_error:
                raise RuntimeError(f"Alpaca request failed for {url}: {request_error.reason}") from request_error
        raise RuntimeError(f"Alpaca request retry limit reached for {url}")

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

    def execute_plan(self, plan):
        """Execute ordered metadata actions; results retain step/action positions."""
        if plan.get("schema_version") != PLAN_VERSION:
            raise RuntimeError("unsupported API action-plan version")
        for step in plan["steps"]:
            for action in step["actions"]:
                # Plans and executor endpoint checks share the solver's URL constants.
                strategy, url = action["strategy"], action["request"]["url"]
                if not (
                    strategy == "asset_lookup"
                    and url.startswith(ASSET_URL + "/")
                    or strategy == "corporate_actions_batch"
                    and url == ACTION_URL
                    or strategy == "asset_catalogue"
                    and url == ASSET_URL
                ):
                    raise RuntimeError("metadata executor received a different API strategy")
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
                        }
                    )
                elif action["strategy"] == "asset_catalogue":
                    values.append(
                        self.request_json(
                            action["request"]["url"] + "?" + urllib.parse.urlencode(action["request"]["params"]),
                            array=True,
                        )
                    )
                else:
                    values.append(self._corporate_action_with_splitting(action))
            results.append(values)
        return results

    def _corporate_action_with_splitting(self, action):
        try:
            return self._execute_corporate_action(action)
        except IdentityHttpError as error:
            if error.status != 414 or len(action["request"]["params"]["symbols"].split(",")) < 2:
                raise
            return [row for part in split_action(action) for row in self._corporate_action_with_splitting(part)]

    def _execute_corporate_action(self, api_action):
        page_token = None
        seen_page_tokens = set()
        normalized_actions = []
        while True:
            query_parameters = dict(api_action["request"]["params"])
            if page_token is not None:
                query_parameters[api_action["pagination"]["parameter"]] = page_token

            response_document = self.request_json(
                f"{api_action['request']['url']}?{urllib.parse.urlencode(query_parameters)}"
            )
            if response_document is None:
                raise RuntimeError("corporate-actions request unexpectedly returned no response")

            action_groups = response_document.get("corporate_actions")
            if not isinstance(action_groups, dict):
                raise RuntimeError("corporate-actions response has no corporate_actions object")
            for response_key, actions in action_groups.items():
                if not isinstance(actions, list):
                    raise RuntimeError(f"corporate-actions group {response_key!r} is not a list")
                action_type = ACTION_TYPE_BY_RESPONSE_KEY.get(response_key, response_key)
                for action in actions:
                    if not isinstance(action, dict):
                        raise RuntimeError(f"corporate-actions group {response_key!r} contains a non-object")
                    normalized_actions.append({**action, "action_type": action_type})

            next_page_token = response_document.get(api_action["pagination"]["response_field"])
            if not next_page_token:
                return normalized_actions
            if not isinstance(next_page_token, str):
                raise RuntimeError("corporate-actions response has a non-string next_page_token")
            if next_page_token in seen_page_tokens:
                raise RuntimeError("corporate-actions pagination repeated a page token")
            seen_page_tokens.add(next_page_token)
            page_token = next_page_token
