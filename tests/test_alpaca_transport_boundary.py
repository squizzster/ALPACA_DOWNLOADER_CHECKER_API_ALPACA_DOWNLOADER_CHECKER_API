"""Credential and complete-response guarantees are established by HTTP producers."""

import io
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock, patch

from libs.alpaca_pricing_download import http as pricing
from libs.alpaca_security_lineage import alpaca_http as metadata


class TransportBoundaryTests(unittest.TestCase):
    def identity_client(self):
        return metadata.AlpacaIdentityClient("test-key", "test-secret", rate_limiter=Mock())

    def test_malformed_false_pagination_tokens_never_complete_an_interval(self):
        for token in (False, 0, [], {}, ""):
            with self.subTest(token=token):
                client = self.identity_client()
                with (
                    patch.object(
                        client, "request_json", return_value={"corporate_actions": {}, "next_page_token": token}
                    ),
                    self.assertRaisesRegex(RuntimeError, "next_page_token"),
                ):
                    client.get_corporate_actions("GOOG", "2026-01-01", "2026-02-01")

    def test_metadata_rejects_duplicate_keys_and_nonfinite_numbers_at_ingress(self):
        for body in (
            b'{"id":"first","id":"second"}',
            b'{"value":NaN}',
            b'{"value":Infinity}',
            b'{"value":1e9999}',
        ):
            with self.subTest(body=body):
                response = io.BytesIO(body)
                response.headers = {}
                with (
                    patch(
                        "libs.alpaca_security_lineage.alpaca_http.open_provider_request", return_value=response
                    ),
                    self.assertRaisesRegex(RuntimeError, "invalid JSON"),
                ):
                    self.identity_client().request_json(metadata.ASSET_URL + "/GOOG")

    def test_actual_clients_never_forward_credentials_or_accept_redirected_responses(self):
        received = []

        class Redirect(BaseHTTPRequestHandler):
            def do_GET(self):
                received.append((self.path, dict(self.headers)))
                if self.path.startswith("/redirect/"):
                    self.send_response(int(self.path.split("/")[-1].split("?")[0]))
                    self.send_header("Location", f"http://localhost:{self.server.server_port}/target")
                else:
                    self.send_response(200)
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *_args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), Redirect) as server:
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                for code in (301, 302, 303, 307, 308):
                    for owner in ("pricing", "metadata"):
                        with self.subTest(code=code, owner=owner):
                            received.clear()
                            url = f"http://127.0.0.1:{server.server_port}/redirect/{code}"
                            if owner == "pricing":
                                with (
                                    patch.object(pricing, "BAR_URL", url),
                                    self.assertRaises(pricing.DownloadError),
                                ):
                                    pricing.BarsClient("test-key", "test-secret")._get({"symbols": "GOOG"})
                            else:
                                with self.assertRaises(metadata.IdentityHttpError):
                                    self.identity_client().request_json(url)
                            self.assertEqual(len(received), 1)
                            self.assertNotIn("/target", received[0][0])
            finally:
                server.shutdown()
                worker.join()
