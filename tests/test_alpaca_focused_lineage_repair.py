"""Selected evidence repair restores history without pricing or peer work."""

import copy
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset, informative_bars
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import DataLibrary
from libs.alpaca_end_of_day_security_correlation.security_identity import security_id_for_alpaca_asset_ids
from libs.alpaca_lineage_build import pipeline, storage
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue.producer import maintain
from libs.common.work_scope import selected_work


def source():
    return {
        "url": "https://exchange.example/identifier-substitution",
        "title": "Common-share identifier substitution",
        "authority": "exchange",
        "document_date": "2024-10-01",
    }


def succession(symbol):
    return {
        "kind": "holding_company_succession",
        "match": {"source_id": "direct-holding-substitution", "symbol": symbol},
        "facts": {
            "schema_version": 1,
            "fact_kind": "holding_company_succession",
            "old_cusip": "111111111",
            "new_cusip": "222222222",
            "legal_effective_date": "2024-10-01",
            "exchange_effective_date": "2024-10-02",
            "old_rate": 1,
            "new_rate": 1,
            "share_class": "common_stock",
            "automatic_exchange": True,
            "unchanged_rights": True,
        },
        "sources": [source()],
    }


class OnlyMissingMappings:
    def __init__(self, expected=None):
        self.expected = expected or {}
        self.calls = []
        self.request_count = 0

    def get_asset_lookup(self, identifier):
        if identifier not in self.expected or identifier in self.calls:
            raise AssertionError(f"unselected or repeated mapping lookup: {identifier}")
        self.calls.append(identifier)
        self.request_count += 1
        return {"outcome": "found", "http_status": 200, "asset": self.expected[identifier]}

    def get_corporate_actions(self, *args):
        raise AssertionError(f"accepted actions requested again: {args}")


class FocusedLineageRepairTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.root = self.store / "end_of_day/lineage"
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
        )
        self.rows = informative_bars("2023-01-03", "2026-11-07")

    def seed(self, symbol, *, split=False, current_dividend=False):
        old = {**asset(symbol, symbol + "-old"), "status": "inactive", "tradable": False}
        new = asset(symbol, symbol + "-new")
        actions = [
            {
                "id": symbol + "-old-dividend",
                "action_type": "cash_dividend",
                "symbol": symbol,
                "cusip": "111111111",
                "rate": 1,
                "ex_date": "2023-03-01",
                "process_date": "2023-03-01",
            }
        ]
        if split:
            actions.append(
                {
                    "id": symbol + "-split",
                    "action_type": "forward_split",
                    "symbol": symbol,
                    "cusip": "111111111",
                    "ex_date": "2024-10-03",
                    "process_date": "2024-10-03",
                    "old_rate": 1,
                    "new_rate": 4,
                }
            )
        if current_dividend:
            actions.append(
                {
                    "id": symbol + "-new-dividend",
                    "action_type": "cash_dividend",
                    "symbol": symbol,
                    "cusip": "222222222",
                    "rate": 1,
                    "ex_date": "2024-12-05",
                    "process_date": "2024-12-23",
                }
            )
        download_pricing(self.store, symbol, "2026-11-07", from_date="2023-01-03", client=FakeClient(self.rows))
        result = pipeline.run_lineage(
            self.store,
            [symbol],
            identity_client=IdentityClient({symbol: new, "111111111": old, "222222222": new}, actions),
        )
        self.assertEqual(0, result["failed"], result)
        if split:
            return {
                "kind": "split_endpoints",
                "match": copy.deepcopy(next(a for a in actions if a["action_type"] == "forward_split")),
                "facts": {
                    "old_cusip": "111111111",
                    "new_cusip": "222222222",
                    "effective_date": "2024-10-03",
                },
                "sources": [source()],
            }, new
        return succession(symbol), new

    def repair(self, symbol, fact, client, **kwargs):
        with (
            selected_work(self.store, [symbol], phase="focused_evidence_repair"),
            patch.object(pipeline, "download_pricing", side_effect=AssertionError("pricing acquisition")),
        ):
            result = pipeline.run_lineage(
                self.store, [symbol], identity_client=client, event_supplements=[fact], **kwargs
            )
        self.assertEqual(0, result["failed"], result)
        return result

    def assert_online_history(self, symbols):
        result = maintain(self.store, bootstrap=True)
        self.assertEqual(0, result["failed"], result)
        data = DataLibrary(store=self.store)
        for symbol in symbols:
            matches = data.resolve_ticker(symbol)
            self.assertEqual(1, len(matches), (symbol, matches))
            security = data.get_security(matches[0].public_id)
            self.assertEqual("2023-01-03", security.first_session.isoformat())
            self.assertEqual("2026-11-06", security.last_session.isoformat())
            self.assertEqual(len(self.rows), security.stored_session_count)
            self.assertEqual(
                security_id_for_alpaca_asset_ids([symbol + "-old", symbol + "-new"]), security.public_id
            )

    def test_split_shapes_restore_complete_history_with_only_missing_mapping(self):
        symbols = ("SPLITA", "SPLITB", "SPLITC")
        for symbol in symbols:
            with self.subTest(symbol=symbol):
                known = symbol == "SPLITA"
                fact, new = self.seed(symbol, split=True, current_dividend=known)
                client = OnlyMissingMappings({} if known else {"222222222": new})
                result = self.repair(symbol, fact, client)
                self.assertEqual([] if known else ["222222222"], client.calls)
                self.assertEqual(0, result["results"][0]["unresolved_rows"])
                with (
                    patch.object(pipeline, "load_pricing", side_effect=AssertionError("accepted prices")),
                    patch.object(pipeline, "correlate", side_effect=AssertionError("accepted calculation")),
                    patch.object(storage, "publish", side_effect=AssertionError("unchanged publication")),
                ):
                    repeat = pipeline.run_lineage(self.store, [symbol], identity_client=OnlyMissingMappings())
                self.assertEqual(0, repeat["failed"], repeat)
        self.assert_online_history(symbols)

    def test_succession_journal_recovery_preserves_completed_metadata_and_calculation(self):
        symbol = "PARENT"
        fact, _ = self.seed(symbol, current_dividend=True)
        client = OnlyMissingMappings()
        with patch.object(storage, "finish_publication", side_effect=RuntimeError("interrupted journal")):
            failed = pipeline.run_lineage(self.store, [symbol], identity_client=client, event_supplements=[fact])
        self.assertEqual(1, failed["failed"], failed)
        self.assertEqual([], client.calls)
        self.assertTrue((self.root / ".pending.json").exists())
        with (
            patch.object(pipeline, "acquire", side_effect=AssertionError("completed metadata")),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("completed prices")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("completed calculation")),
            patch.object(pipeline, "download_pricing", side_effect=AssertionError("pricing acquisition")),
        ):
            completed = pipeline.run_lineage(self.store, [symbol], identity_client=OnlyMissingMappings())
        self.assertEqual(0, completed["failed"], completed)
        self.assertFalse((self.root / ".pending.json").exists())
        self.assert_online_history([symbol])

    def test_succession_membership_and_retained_facts_survive_an_ordinary_append(self):
        symbol = "PARENT"
        fact, new = self.seed(symbol, current_dividend=True)
        self.repair(symbol, fact, OnlyMissingMappings())
        self.assert_online_history([symbol])
        prior = DataLibrary(store=self.store).resolve_ticker(symbol)[0].public_id
        added = informative_bars("2026-11-09", "2026-11-10")
        download_pricing(self.store, symbol, "2026-11-10", client=FakeClient([*self.rows, *added]))
        old = {**asset(symbol, symbol + "-old"), "status": "inactive", "tradable": False}
        result = pipeline.run_lineage(
            self.store,
            [symbol],
            identity_client=IdentityClient({symbol: new, "111111111": old, "222222222": new}),
        )
        self.assertEqual(0, result["failed"], result)
        self.assertEqual(0, result["results"][0]["unresolved_rows"])
        served = maintain(self.store, symbols=[symbol])
        self.assertEqual(0, served["failed"], served)
        data = DataLibrary(store=self.store)
        self.assertEqual(prior, data.resolve_ticker(symbol)[0].public_id)
        security = data.get_security(prior)
        self.assertEqual(len(self.rows) + 1, security.stored_session_count)
        self.assertEqual("2023-01-03", security.first_session.isoformat())
        self.assertEqual("2026-11-09", security.last_session.isoformat())


if __name__ == "__main__":
    unittest.main()
