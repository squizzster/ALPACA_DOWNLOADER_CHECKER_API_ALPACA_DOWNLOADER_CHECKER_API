"""Open an exact provider request without extending its credential authority."""

import urllib.error
import urllib.request


class _NoProviderRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, newurl):
        # The executor accepted one endpoint, not a server-selected destination.
        # In particular urllib's default redirect copies custom API-key headers.
        raise urllib.error.HTTPError(request.full_url, code, "provider redirect rejected", headers, response)


def open_provider_request(request, *, timeout):
    return urllib.request.build_opener(_NoProviderRedirect()).open(request, timeout=timeout)
