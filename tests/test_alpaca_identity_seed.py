import unittest
from unittest.mock import Mock, patch

from libs.alpaca_security_lineage import AlpacaIdentityClient


class AlpacaIdentitySeedTests(unittest.TestCase):
    def test_seeded_asset_lookup_consumes_the_prior_provider_request_once(self) -> None:
        client = AlpacaIdentityClient("key", "secret", rate_limiter=Mock())
        asset = {
            "id": "00000000-0000-4000-8000-000000000001",
            "class": "us_equity",
            "symbol": "AAA",
        }
        client.seed_asset_lookup("AAA", asset=asset, http_status=200)

        with patch(
            "libs.alpaca_security_lineage.alpaca_http.urllib.request.urlopen",
            side_effect=AssertionError("seeded lookup must not be requested again"),
        ):
            lookup = client.get_asset_lookup("AAA")

        self.assertEqual("found", lookup["outcome"])
        self.assertEqual(asset, lookup["asset"])
        self.assertEqual(1, client.request_count)

    def test_not_found_seed_retains_the_original_http_evidence(self) -> None:
        client = AlpacaIdentityClient("key", "secret", rate_limiter=Mock())
        client.seed_asset_lookup("MISSING", asset=None, http_status=404)
        self.assertEqual(
            {"outcome": "not_found", "http_status": 404, "asset": None},
            client.get_asset_lookup("MISSING"),
        )
        self.assertEqual(1, client.request_count)


if __name__ == "__main__":
    unittest.main()
