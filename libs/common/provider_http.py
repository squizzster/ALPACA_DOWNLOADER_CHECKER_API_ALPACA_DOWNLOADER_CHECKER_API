"""Open an exact provider request without extending its credential authority."""

import http.client
import urllib.error
import urllib.request
from collections import deque

SYMBOL_BATCH_LIMIT = 1000
RETRY = object()


def balanced_batches(values, limit=SYMBOL_BATCH_LIMIT):
    """Stable, balanced subdivisions, with smaller batches first."""
    if type(limit) is not int or limit < 1:
        raise ValueError("batch limit must be a positive integer")
    if not values:
        return []
    count = (len(values) + limit - 1) // limit
    size, extra = divmod(len(values), count)
    result, offset = [], 0
    for index in range(count):
        end = offset + size + (index >= count - extra)
        result.append(values[offset:end])
        offset = end
    return result


def request_with_policy(request, *, opener, timeout, before_attempt, read_response, on_error, attempts=6):
    """Shared attempt lifecycle; adapters own retry, rate and response policies.

    on_error returns RETRY, a completed endpoint outcome (e.g. an allowed 404),
    or raises the endpoint's error. Every retry re-enters before_attempt.
    """
    for attempt in range(attempts):
        before_attempt()
        try:
            with opener(request, timeout=timeout) as response:
                return read_response(response)
        except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.HTTPException) as error:
            try:
                result = on_error(error, attempt, attempt == attempts - 1)
            finally:
                if isinstance(error, urllib.error.HTTPError):
                    error.close()
            if result is not RETRY:
                return result
    raise RuntimeError("provider request exhausted retries")


def paginated_pages(parameters, pagination, get_page, *, error_type, metrics=None, on_error=None):
    """Exhaust one batch; a new batch always starts with its own token state."""
    parameters = dict(parameters)
    tokens = set()
    while True:
        if metrics is not None:
            metrics["page_requests"] = metrics.get("page_requests", 0) + 1
        try:
            document = get_page(dict(parameters))
        except Exception as error:
            if on_error is not None:
                on_error(error, bool(tokens))
            raise
        yield document
        token = document.get(pagination["response_field"])
        if token is None:
            return
        if not isinstance(token, str) or not token:
            raise error_type("invalid next_page_token")
        if token in tokens:
            raise error_type("pagination repeated a page token (next_page_token)")
        tokens.add(token)
        parameters[pagination["parameter"]] = token


def fetch_symbol_batches(parameters, fetch_batch, *, status_code, metrics=None):
    """Complete a logical request atomically, including any HTTP 414 fallback.

    Only explicit symbol rosters split. Results remain private until every batch
    completes; a later error discards earlier success. Adapters validate records.
    """
    metrics = metrics if metrics is not None else {}
    metrics.setdefault("batches", 0)
    metrics.setdefault("oversize_splits", 0)
    results = []

    pending = (
        deque(
            {**parameters, "symbols": ",".join(batch)}
            for batch in balanced_batches(parameters["symbols"].split(","))
        )
        if "symbols" in parameters
        else deque([dict(parameters)])
    )
    while pending:
        params = pending.popleft()
        metrics["batches"] += 1
        try:
            result = fetch_batch(params)
        except Exception as error:
            symbols = params.get("symbols", "").split(",")
            if status_code(error) != 414 or len(symbols) < 2:
                raise
            metrics["oversize_splits"] += 1
            pending.extendleft(
                reversed(
                    [
                        {**params, "symbols": ",".join(batch)}
                        for batch in balanced_batches(symbols, (len(symbols) + 1) // 2)
                    ]
                )
            )
        else:
            results.append(result)
    return results


class _NoProviderRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, newurl):
        # The executor accepted one endpoint, not a server-selected destination.
        # In particular urllib's default redirect copies custom API-key headers.
        raise urllib.error.HTTPError(request.full_url, code, "provider redirect rejected", headers, response)


def open_provider_request(request, *, timeout):
    return urllib.request.build_opener(_NoProviderRedirect()).open(request, timeout=timeout)
