"""Exchange changes through a daily CLI update, lineage recovery and public export."""

import gzip
import io
import json
import unittest
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import datetime
from functools import partial
from pathlib import Path
from unittest.mock import Mock, patch

from test_alpaca_durable_maintenance import fixture
from test_alpaca_end_of_day_api import valid_bars
from test_alpaca_lineage_build import asset
from test_alpaca_universe_update import CatalogueClient

from alpaca_end_of_day import DataLibrary
from libs.alpaca_cli import main
from libs.alpaca_lineage_build import storage, universe_update
from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient


class UpdateListingExchangeTests(unittest.TestCase):
    def setUp(self):
        self.case = self.enterContext(fixture())
        self.symbol = "NEW"
        self.provider_id = "new-instrument-id"
        self.case.records[:] = [asset("AAA"), {**asset(self.symbol, self.provider_id), "exchange": "NYSE"}]
        self.case.live[self.symbol] = valid_bars("2026-10-14", "2026-10-17")
        self.identity = CatalogueClient(self.case.records)
        self.http = AlpacaIdentityClient("test", "test", rate_limiter=Mock())
        self.identity.get_trade_tape_sample = Mock(side_effect=self.tape)
        self.enterContext(patch("libs.alpaca_cli.clients", return_value=(self.case.pricing, self.identity)))
        # Use the normal CLI/planner with a temporary recovery registry. Its
        # post-update Git synchronization must never target the repository.
        self.enterContext(
            patch.object(
                universe_update, "run_update", partial(universe_update.run_update, registry=self.case.registry)
            )
        )
        self.enterContext(patch("libs.alpaca_cli.commit_registry"))
        self.clock = self.enterContext(patch("libs.alpaca_lineage_build.catalogue.datetime"))
        self.first = "2026-10-16T21:00:00+00:00"
        self.last = "2026-10-19T21:00:00+00:00"
        self.clock.now.return_value = datetime.fromisoformat(self.first)
        self.update()
        data = DataLibrary(store=self.case.store)
        self.public_id = data.resolve_ticker(self.symbol)[0].public_id
        self.initial_prices = {
            view: history.bars for view, history in data.get_histories(self.public_id).items()
        }
        self.generation = self.receipt()["source_sha256"]
        # One stable baseline defines the normal price-boundary requests for this
        # frozen cutoff. The exchange event must never add a historical download.
        self.case.pages.clear()
        self.update()
        self.boundary_pages = list(self.case.pages)
        self.assertTrue(self.boundary_pages)
        self.assertTrue(all(page["adjustment"] == "all" for page in self.boundary_pages))
        self.identity.get_trade_tape_sample.assert_not_called()

    def update(self, through="2026-10-16"):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = main(["update", "--store", str(self.case.store), "--through", through, "--workers", "1"])
        self.assertEqual(code, 0, err.getvalue())
        report = json.loads(out.getvalue())
        self.assertTrue(report["complete"], report)
        return report

    def receipt(self):
        root = self.case.store / "end_of_day/lineage"
        index = storage.read_index(root, self.symbol)
        return storage.read_receipt(root, self.symbol, index)

    def tape(self, symbol, session, **scope):
        self.assertEqual(symbol, self.symbol)
        self.assertEqual((scope["asof"], scope["feed"]), ("-", "sip"))
        code = {"2026-10-16": "A", "2026-10-19": "C"}[session]
        trade = {"t": session + "T15:00:00Z", "z": code, "x": "P", "s": 10, "p": 5}
        with patch.object(self.http, "request_json", return_value={"symbol": symbol, "trades": [trade]}):
            return self.http.get_trade_tape_sample(symbol, session, **scope)

    @contextmanager
    def only_boundary_work(self, *, recovering=False):
        original = Path.open
        prices = self.case.store / "end_of_day/pricing"
        self.case.pages.clear()

        def open_file(path, *args, **kwargs):
            if path.is_relative_to(prices) and path.name.endswith(".csv.gz"):
                self.fail(f"exchange-only update reopened accepted pricing payload {path}")
            return original(path, *args, **kwargs)

        with patch.object(Path, "open", open_file):
            yield

        def requests_by_symbol(pages):
            return [{**page, "symbols": symbol} for page in pages for symbol in page["symbols"].split(",")]

        expected = requests_by_symbol(self.boundary_pages)
        if recovering:
            # The installed listing journal completes this security's accepted
            # work. The independent control still receives its normal daily check.
            expected = [page for page in expected if page["symbols"] != self.symbol]
        self.assertCountEqual(requests_by_symbol(self.case.pages), expected)

    def move(self):
        row = {**asset(self.symbol, self.provider_id), "exchange": "NASDAQ"}
        self.case.records[1] = self.identity.assets[self.symbol] = row
        self.clock.now.return_value = datetime.fromisoformat(self.last)

    def check_change(self):
        receipt = self.receipt()
        changes = receipt["evidence"]["listing_exchange_changes"]
        self.assertEqual(len(changes), 1, changes)
        event = changes[0]
        expected = {
            "asset_id": self.provider_id,
            "from_symbol": self.symbol,
            "to_symbol": self.symbol,
            "from_exchange": "NYSE",
            "to_exchange": "NASDAQ",
        }
        self.assertEqual({key: event[key] for key in expected}, expected)
        self.assertEqual(
            datetime.fromisoformat(event["from_observed_at_utc"]), datetime.fromisoformat(self.first)
        )
        self.assertEqual(datetime.fromisoformat(event["to_observed_at_utc"]), datetime.fromisoformat(self.last))
        self.assertEqual((event["prior_session"], event["effective_session"]), ("2026-10-16", "2026-10-19"))
        self.assertEqual(receipt["source_sha256"], self.generation)
        data = DataLibrary(store=self.case.store)
        self.assertEqual([match.public_id for match in data.resolve_ticker(self.symbol)], [self.public_id])
        self.assertEqual(
            {view: history.bars for view, history in data.get_histories(self.public_id).items()},
            self.initial_prices,
        )
        report = data.write_metadata_export(self.case.directory / "companion", public_ids=[self.public_id])
        record = json.loads(gzip.decompress(Path(report["records_file"]).read_bytes()))
        self.assertEqual(record["public_id"], self.public_id)
        self.assertEqual(len(record["listing_exchange_changes"]), 1)
        observed = record["listing_exchange_changes"][0]
        self.assertEqual((observed["from_exchange"], observed["to_exchange"]), ("NYSE", "NASDAQ"))
        self.assertEqual(
            (observed["prior_session"], observed["effective_session"]), ("2026-10-16", "2026-10-19")
        )
        self.assertNotIn("asset_id", observed)

    def test_exchange_only_update_and_stable_repeat(self):
        self.move()
        with self.only_boundary_work():
            report = self.update()
        result = next(
            row
            for group in report["stages"]["discovery_lineage"]
            for row in group["results"]
            if self.symbol in row.get("processed", ())
        )
        self.assertEqual(result["pricing_rows_processed"], 0)
        self.assertEqual(report["stages"]["serving_catalogue"]["checked_bars"], 0)
        self.assertCountEqual(
            [call.args[1] for call in self.identity.get_trade_tape_sample.call_args_list],
            ["2026-10-16", "2026-10-19"],
        )
        # A new catalogue observation of the same exchange cannot create a second
        # transition or select the earlier trade request again.
        self.assert_idle_update("2026-10-20T21:00:00+00:00")
        self.check_change()

    def assert_idle_update(self, observed_at):
        consumed = self.identity.request_count
        self.clock.now.return_value = datetime.fromisoformat(observed_at)
        with (
            self.only_boundary_work(),
            patch.object(
                self.identity, "get_trade_tape_sample", side_effect=AssertionError("repeated listing evidence")
            ),
            patch.object(storage, "read_receipt", side_effect=AssertionError("unchanged receipt reopened")),
        ):
            repeated = self.update()
        self.assertNotIn(self.symbol, repeated["plan"]["work"])
        self.assertEqual(self.identity.request_count - consumed, 1)

    def test_preopen_change_waits_for_one_completed_session_without_price_work(self):
        # Both observations and all retries use Friday's accepted price cutoff.
        self.last = "2026-10-19T12:00:00+00:00"
        self.move()
        with self.only_boundary_work():
            self.update()
        tape = self.identity.get_trade_tape_sample
        self.assertEqual([call.args[1] for call in tape.call_args_list], ["2026-10-16"])
        root = self.case.store / "end_of_day/lineage"

        def projection():
            return storage.read_index(root, self.symbol)["symbols"][self.symbol]["projection"]

        (pending,) = self.receipt()["evidence"]["listing_exchange_changes"]
        due = "2026-10-19T20:15:00+00:00"
        self.assertEqual(pending["date_after_utc"], due)
        self.assertEqual(pending["date_session"], "2026-10-19")
        self.assertEqual(pending["date_prior_session"], "2026-10-16")
        self.assertNotIn("effective_session", pending)
        self.assertEqual(projection()["listing_date_after_utc"], due)
        self.assert_idle_update("2026-10-19T12:30:00+00:00")

        followup = "2026-10-20T17:00:00+00:00"
        self.clock.now.return_value = datetime.fromisoformat(followup)
        with self.only_boundary_work():
            report = self.update()
        self.assertEqual(set(report["plan"]["work"]), {self.symbol})
        results = [
            row
            for group in report["stages"]["discovery_lineage"]
            for row in group["results"]
            if self.symbol in row.get("processed", ())
        ]
        self.assertEqual([row["pricing_rows_processed"] for row in results], [0])
        self.assertEqual(report["stages"]["serving_catalogue"]["checked_bars"], 0)
        self.assertEqual([call.args[1] for call in tape.call_args_list], ["2026-10-16", "2026-10-19"])
        (dated,) = self.receipt()["evidence"]["listing_exchange_changes"]
        self.assertEqual(dated["dating_observed_at_utc"], followup)
        self.assertFalse({"date_session", "date_prior_session", "date_after_utc"}.intersection(dated))
        self.assertNotIn("listing_date_after_utc", projection())
        self.assert_idle_update("2026-10-21T17:00:00+00:00")
        self.check_change()

    def test_recovery_reuses_published_listing_evidence(self):
        self.move()
        with (
            patch.object(storage, "_acknowledge", side_effect=KeyboardInterrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            self.update()
        self.assertTrue((self.case.store / "end_of_day/lineage/.pending.json").is_file())
        frozen = self.receipt()["evidence"]["listing_exchange_changes"]
        with (
            self.only_boundary_work(recovering=True),
            patch.object(
                self.identity,
                "get_trade_tape_sample",
                side_effect=AssertionError("reacquired frozen listing evidence"),
            ),
        ):
            report = self.update()
        self.assertTrue(report["stages"]["recovery"]["lineage_recovered"])
        self.assertEqual(self.receipt()["evidence"]["listing_exchange_changes"], frozen)
        self.check_change()

    def test_next_price_append_carries_the_observed_listing_change(self):
        self.move()
        self.update()
        changes = self.receipt()["evidence"]["listing_exchange_changes"]
        self.case.live[self.symbol].extend(valid_bars("2026-10-19", "2026-10-21"))
        self.clock.now.return_value = datetime.fromisoformat("2026-10-20T21:00:00+00:00")
        with patch.object(
            self.identity,
            "get_trade_tape_sample",
            side_effect=AssertionError("reacquired prior exchange transition"),
        ):
            self.update(through="2026-10-20")
        receipt = self.receipt()
        self.assertNotEqual(receipt["source_sha256"], self.generation)
        self.assertEqual(receipt["evidence"]["listing_exchange_changes"], changes)
        histories = DataLibrary(store=self.case.store).get_histories(self.public_id)
        for view, history in histories.items():
            self.assertEqual(history.bars[:-2], self.initial_prices[view])
            self.assertEqual([str(bar.session) for bar in history.bars[-2:]], ["2026-10-19", "2026-10-20"])
