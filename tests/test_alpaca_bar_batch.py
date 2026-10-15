import io
import json
import unittest
import urllib.parse
from unittest.mock import Mock, patch

from libs.download_alpaca_end_of_day_bars.alpaca_http import (
    request_alpaca_daily_bars_batch,
)


class _Response(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))
        self.headers = {}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class AlpacaBarBatchTests(unittest.TestCase):
    def test_batch_preserves_requested_symbols_and_explicit_empties(self) -> None:
        response = _Response(
            {
                "bars": {
                    "AAA": [{"t": "2026-10-13T04:00:00Z"}],
                    "AAC": [{"t": "2026-10-13T04:00:00Z"}],
                },
                "next_page_token": None,
            }
        )
        limiter = Mock()
        with patch(
            "libs.download_alpaca_end_of_day_bars.alpaca_http.urllib.request.urlopen",
            return_value=response,
        ) as urlopen:
            result = request_alpaca_daily_bars_batch(
                ("AAA", "AAB", "AAC"),
                "raw",
                "key",
                "secret",
                "2026-10-14T00:00:00Z",
                limiter,
            )

        self.assertEqual(["AAA", "AAB", "AAC"], list(result))
        self.assertEqual([], result["AAB"])
        limiter.wait_until_request_allowed.assert_called_once_with()
        query = urllib.parse.parse_qs(urllib.parse.urlparse(urlopen.call_args.args[0].full_url).query)
        self.assertEqual(["AAA,AAB,AAC"], query["symbols"])

    def test_batch_rejects_pagination_instead_of_truncating(self) -> None:
        response = _Response({"bars": {}, "next_page_token": "more"})
        with patch(
            "libs.download_alpaca_end_of_day_bars.alpaca_http.urllib.request.urlopen",
            return_value=response,
        ):
            with self.assertRaisesRegex(RuntimeError, "10,000-bar limit"):
                request_alpaca_daily_bars_batch(
                    ("AAA", "AAB", "AAC"),
                    "all",
                    "key",
                    "secret",
                    "2026-10-14T00:00:00Z",
                    Mock(),
                )


if __name__ == "__main__":
    unittest.main()
