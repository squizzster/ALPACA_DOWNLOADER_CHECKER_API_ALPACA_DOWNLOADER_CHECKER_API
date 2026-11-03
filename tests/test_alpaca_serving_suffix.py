"""Supported publication ranges survive uncertainty outside their dates."""

import csv
import io
import json
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase, valid_bars

from alpaca_end_of_day import DataLibrary, StandaloneReader
from libs.alpaca_lineage_build import storage as lineage
from libs.alpaca_serving_catalogue import storage
from libs.alpaca_serving_catalogue.contract import policy
from libs.alpaca_serving_catalogue.policy import _unresolved_ends
from libs.alpaca_serving_catalogue.producer import maintain


class ServingSuffixTests(NativeCase):
    def test_ownerless_old_label_after_closed_ownership_is_not_a_conflict(self):
        old = {
            "branch": "correlated",
            "from": "2026-09-28",
            "to": "2026-09-30",
            "security_identity": {"security_id": "public", "member_alpaca_asset_ids": ["owner"]},
            "ticker_ownership_interval": {
                "historical_symbol": "OLD",
                "start_date_inclusive": None,
                "end_date_exclusive": "2026-10-01",
                "cusip": "123456789",
            },
        }
        unresolved = {"branch": "uncorrelated", "from": "2026-10-01", "to": "2026-10-05"}
        group = SimpleNamespace(assignments={"OLD": [("raw", "history", old), ("raw", "history", unresolved)]})
        security = {"public_id": "public", "tickers": ["OLD", "NEW"]}
        args = group, security, {"owner"}, "2026-09-28", "2026-10-13"
        self.assertEqual([], _unresolved_ends(*args))
        unresolved["from"] = "2026-09-30"
        self.assertEqual(["2026-09-30"], _unresolved_ends(*args))
        self.assertEqual([], _unresolved_ends(group, security, {"owner"}, "2026-10-01", "2026-10-13"))
        unresolved["from"] = "2026-10-01"
        unresolved["candidate_securities"] = [{"asset_id": "owner"}]
        self.assertEqual(["2026-10-05"], _unresolved_ends(*args))

    def overlap(self, end="2026-10-06", candidate="TEST-id"):
        self.add_symbol("ALIAS", valid_bars("2026-10-01", end))
        entries = lineage.read_index(self.root)["symbols"]
        sources = {name: entry["source_sha256"] for name, entry in entries.items()}
        for symbol in ("TEST", "ALIAS"):

            def connect(receipt, boundaries, symbol=symbol):
                receipt["input_sources"] = sources
                receipt["dependencies"] = sorted(set(sources) - {symbol})
                if symbol == "ALIAS":
                    for boundary in boundaries.values():
                        for entry in boundary["adjustments"]:
                            for segment in entry["segments"]:
                                segment["branch"] = "uncorrelated"
                                segment.pop("security_identity", None)
                                segment["candidate_securities"] = [{"asset_id": candidate}]

            self.publish_change(connect, symbol)

    def test_recent_suffix_is_exact_across_metadata_readers_and_exports(self):
        self.overlap()
        data = self.bootstrap()
        (security,) = data.list_securities()
        self.assertEqual(date(2026, 10, 6), security.first_session)
        self.assertEqual(date(2026, 10, 13), security.last_session)
        self.assertFalse(data.resolve_ticker("TEST", on="2026-10-05"))
        self.assertTrue(data.resolve_ticker("TEST", on="2026-10-06"))
        histories = data.get_histories(security.public_id)
        expected = [date.fromisoformat(b["t"][:10]) for b in self.bars if b["t"][:10] > "2026-10-05"]
        for history in histories.values():
            self.assertEqual(expected, [bar.session for bar in history.bars])
        self.assertFalse(data.get_history(security.public_id, end="2026-10-05").bars)
        self.assertFalse(data.get_history(security.public_id, start="2026-10-14").bars)
        self.assertEqual(len(self.bars), len(StandaloneReader(self.store).get_history("TEST")["bars"]))
        selected = self.catalogue_groups("ALIAS")["ALIAS"]
        self.assertEqual("2026-10-05", selected["decisions"][0]["publication_start_after_unresolved"])
        self.assertEqual(len(expected), selected["securities"][0]["coverage"]["session_count"])
        self.assertEqual("2026-10-01", selected["history_validation"]["prefix_records"][0]["first_session"])
        stream = io.StringIO()
        data.write_flat_online_csv(stream)
        rows = list(csv.reader(io.StringIO(stream.getvalue())))
        self.assertEqual([d.isoformat() for d in expected], [r[0] for r in rows[1:]])
        output = self.directory / "csv"
        result = data.write_csv_export(output)
        output = self.directory / result["output"]
        digest = json.loads((output / security.public_id / f"{security.public_id}_digest.json").read_text())
        self.assertEqual(len(expected), digest["rows"])
        self.assertEqual("2026-10-06", digest["range"]["start"])
        with (output / security.public_id / f"{security.public_id}_eod.csv").open() as stream:
            rows = list(csv.reader(stream))
        self.assertEqual([d.isoformat() for d in expected], [r[0] for r in rows[1:]])

    def test_latest_uncertainty_still_blocks_and_unrelated_candidates_do_not_truncate(self):
        self.overlap(end="2026-10-14")
        self.assertEqual(0, self.bootstrap().online_count())
        self.assertEqual(
            ["unresolved_coverage"], self.catalogue_groups("ALIAS")["ALIAS"]["decisions"][0]["reasons"]
        )

    def test_unrelated_uncertainty_preserves_full_history(self):
        self.overlap(candidate="another-identity")
        (security,) = self.bootstrap().list_securities()
        self.assertEqual(date(2026, 10, 1), security.first_session)

    def test_changed_uncertainty_recalculates_range_and_unchanged_maintenance_reuses(self):
        self.overlap()
        self.bootstrap()
        with patch("libs.alpaca_serving_catalogue.producer.evaluate", side_effect=AssertionError("replay")):
            self.assertTrue(maintain(self.store)["complete"])
        self.publish_change(lambda r, _: r["evidence"].update(warnings=["new metadata"]), "TEST")
        self.assertTrue(maintain(self.store)["complete"])
        (security,) = DataLibrary(store=self.store).list_securities()
        self.assertEqual(date(2026, 10, 6), security.first_session)

    def test_policy_eight_checkpoint_can_rebuild_without_hiding_old_conflict(self):
        self.overlap()
        current = policy()
        with (
            patch("libs.alpaca_serving_catalogue.contract.policy", return_value={**current, "version": 8}),
            patch("libs.alpaca_serving_catalogue.storage.policy", return_value={**current, "version": 8}),
            patch("libs.alpaca_serving_catalogue.policy.policy", return_value={**current, "version": 8}),
            patch("libs.alpaca_serving_catalogue.producer.policy", return_value={**current, "version": 8}),
        ):
            # The stored numerical checkpoint covers the complete assigned history.
            self.assertTrue(maintain(self.store, bootstrap=True)["complete"])
        self.assertTrue(maintain(self.store, rebuild=True)["complete"])
        self.assertEqual(9, storage.selector_state(self.store)["policy"]["version"])
        (security,) = DataLibrary(store=self.store).list_securities()
        self.assertEqual(date(2026, 10, 6), security.first_session)
