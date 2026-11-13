"""Research cannot repair source gaps; completed provider work survives recovery and append."""

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

    def seed(self, symbol, *, split=False, current_dividend=False, provider_transition=False, interrupted=False):
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
        if provider_transition:
            actions.append(
                {
                    "id": symbol + "-provider-transition",
                    "action_type": "name_change",
                    "old_symbol": symbol,
                    "new_symbol": symbol,
                    "old_cusip": "111111111",
                    "new_cusip": "222222222",
                    "process_date": "2024-10-02",
                }
            )
        download_pricing(self.store, symbol, "2026-11-07", from_date="2023-01-03", client=FakeClient(self.rows))
        result = pipeline.run_lineage(
            self.store,
            [symbol],
            identity_client=IdentityClient({symbol: new, "111111111": old, "222222222": new}, actions),
        )
        self.assertEqual(1 if interrupted else 0, result["failed"], result)
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

    def test_missing_provider_endpoints_cannot_be_supplied_by_research(self):
        symbol = "SPLITFUTURE"
        fact, _ = self.seed(symbol, split=True)
        index = storage.read_index(self.root)
        before = storage.read_receipt(self.root, symbol, index)
        self.assertGreater(before["unresolved_rows"], 0)
        client = OnlyMissingMappings()
        with patch.object(
            pipeline, "accept_context", side_effect=AssertionError("store reopened before rejection")
        ):
            with self.assertRaisesRegex(Exception, "inadmissible"):
                self.repair(symbol, fact, client)
        self.assertEqual([], client.calls)
        self.assertEqual(index, storage.read_index(self.root))
        self.assertEqual(before, storage.read_receipt(self.root, symbol, index))
        # The unsupported result was processed and may be reused without another
        # historical owner calculation, publication or provider request.
        with (
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("accepted prices reopened")),
            patch.object(
                pipeline, "correlate", side_effect=AssertionError("accepted ownerless decision repeated")
            ),
            patch.object(storage, "publish", side_effect=AssertionError("unchanged publication")),
        ):
            repeated = pipeline.run_lineage(self.store, [symbol], identity_client=OnlyMissingMappings())
        self.assertEqual(0, repeated["failed"], repeated)
        self.assertEqual(before, storage.read_receipt(self.root, symbol, index))

    def test_provider_transition_journal_recovery_preserves_completed_work(self):
        symbol = "PARENT"
        with patch.object(storage, "finish_publication", side_effect=RuntimeError("interrupted journal")):
            self.seed(symbol, current_dividend=True, provider_transition=True, interrupted=True)
        self.assertTrue((self.root / ".pending.json").exists())
        with (
            patch.object(pipeline, "acquire", side_effect=AssertionError("completed metadata repeated")),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("completed prices reopened")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("completed calculation repeated")),
            patch.object(pipeline, "download_pricing", side_effect=AssertionError("pricing acquisition")),
        ):
            completed = pipeline.run_lineage(self.store, [symbol], identity_client=OnlyMissingMappings())
        self.assertEqual(0, completed["failed"], completed)
        self.assertFalse((self.root / ".pending.json").exists())
        self.assert_online_history([symbol])

    def test_provider_supported_membership_survives_ordinary_append(self):
        symbol = "PARENT"
        _, new = self.seed(symbol, current_dividend=True, provider_transition=True)
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
