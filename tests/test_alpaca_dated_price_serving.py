"""Serving eligibility and all catalogue consumers follow native numerical selection."""

import json
import sqlite3
import zlib
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase, valid_bars

from alpaca_end_of_day import ADJUSTMENTS, DataLibrary, StandaloneReader
from libs.alpaca_lineage_build import dated_price_publication
from libs.alpaca_pricing_download import dated_publication
from libs.alpaca_serving_catalogue import policy
from libs.alpaca_serving_catalogue.producer import maintain
from libs.alpaca_serving_catalogue.reuse import validate_history_validation
from libs.common.store_lock import operation_lock


class DatedPriceServingTests(NativeCase):
    def setUp(self):
        bars = valid_bars()
        # New York midnight written with a Pacific offset has the previous
        # calendar date in its timestamp. Keep it in a nonterminal segment.
        instant = datetime.fromisoformat(bars[3]["t"])
        bars[3]["t"] = instant.astimezone(timezone(timedelta(hours=-7))).isoformat()
        with patch("test_alpaca_end_of_day_api.valid_bars", return_value=bars):
            super().setUp()
        self.library = self.bootstrap()
        (security,) = self.library.list_securities()
        self.public_id = security.public_id
        self.prior = self.catalogue_groups("TEST")["TEST"]
        self.interval = {
            "schema_version": 1,
            "token": "a" * 32,
            "members": ["TEST"],
            "unavailable": [],
            "public_id": self.public_id,
            "first": "2026-10-01",
            "cut": "2026-10-05",
            "witness_end": "2026-10-08",
            "source_decision": self.prior["decision_generation"],
        }
        # Install the immutable result of a synthetic successful acquisition for
        # an independent consumer test. The pricing tests own admission of rows.
        with self.library.open_history_export(public_ids=[self.public_id]) as export:
            item = next(export.iter_histories())
            with closing(sqlite3.connect(dated_publication.database(self.store))) as conn, conn:
                conn.execute("CREATE TABLE dated_price_views (token TEXT, view TEXT,payload BLOB)")
                for mode, view in item.views.items():
                    rows = {}
                    for bar in view["bars"]:
                        if bar["session"] >= self.interval["cut"]:
                            continue
                        result = dict(bar)
                        if mode == "all":
                            for key in ("open", "high", "low", "close", "vwap"):
                                result[key] = str(Decimal(result[key]) * 3)
                        rows[bar["session"]] = result
                    conn.execute(
                        "INSERT INTO dated_price_views VALUES(?,?,?)",
                        (self.interval["token"], mode, zlib.compress(json.dumps(rows).encode())),
                    )

    def select(self):
        with operation_lock(self.store, exclusive=True):
            dated_price_publication.publish(self.store, self.prior, self.interval)

    def test_native_selection_reaches_standalone_public_history_and_export(self):
        old = self.library.get_history(self.public_id, "all")
        self.select()
        with (
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("completed prices rechecked")),
            patch.object(dated_publication, "assemble", side_effect=AssertionError("accepted payload reopened")),
        ):
            maintained = maintain(self.store)
        self.assertTrue(maintained["complete"], maintained)
        self.assertEqual(maintained["checked_bars"], 0)
        data = DataLibrary(store=self.store)
        records = self.catalogue_groups("TEST")["TEST"]["history_validation"]["prefix_records"]
        self.assertEqual(records[0]["dated_price_token"], self.interval["token"])
        self.assertEqual(data._records[self.public_id]["public"]["dated_price_interval"], self.interval)
        with data.open_history_export(public_ids=[self.public_id]) as opened:
            exported = next(opened.iter_histories()).views
        direct = StandaloneReader(self.store).get_histories("TEST", public_id=self.public_id)["views"]
        self.assertEqual(direct, exported)
        for mode, history in data.get_histories(self.public_id).items():
            self.assertEqual(
                [b.close for b in history.bars], [Decimal(b["close"]) for b in direct[mode]["bars"]]
            )
        expected = [b.close * 3 if b.session.isoformat() < self.interval["cut"] else b.close for b in old.bars]
        self.assertEqual(expected, [Decimal(b["close"]) for b in exported["all"]["bars"]])
        with patch.object(dated_publication, "database", side_effect=AssertionError("suffix opened interval")):
            self.assertEqual(
                data.get_history(self.public_id, "all", start=self.interval["cut"]).bars[0].close,
                old.bars[2].close,
            )
            late = old.bars[3]
            self.assertEqual(
                (late,),
                data.get_history(self.public_id, "all", start=late.session, end=late.session).bars,
            )

    def test_exact_interval_reuses_checks_and_revocation_reopens_the_selected_identity(self):
        self.select()
        reader = StandaloneReader(self.store)
        with reader._open_group("TEST", include_unresolved=True) as group:
            first = policy.evaluate(group, (self.prior,))
            self.assertEqual(first["validation_work"]["checked_bars"], 0)
            for field in ("dated_price_span", "dated_price_token"):
                invalid = deepcopy(first)
                del invalid["history_validation"]["prefix_records"][0][field]
                with self.assertRaisesRegex(ValueError, "invalid serving dated price checkpoint"):
                    validate_history_validation(invalid)
            with patch.object(
                policy, "_bar_reasons", side_effect=AssertionError("verified interval checked twice")
            ):
                second = policy.evaluate(group, (first,))
                self.assertEqual(second["validation_work"]["full_history_evaluations"], 0)
            mismatched = deepcopy(first)
            mismatched["history_validation"]["prefix_records"][0]["dated_price_token"] = "b" * 32
            # This new frame is bound to the initial catalogue decision, so it
            # cannot bypass numerical eligibility with a different old token.
            with (
                patch.object(policy, "_bar_reasons", wraps=policy._bar_reasons) as checks,
                patch.object(
                    dated_publication, "assemble", side_effect=AssertionError("reopened accepted prices")
                ),
            ):
                stale = policy.evaluate(group, (mismatched,))
                self.assertEqual(stale["validation_work"]["full_history_evaluations"], 1)
                dates = {call.args[0]["session"] for call in checks.call_args_list}
                self.assertTrue(dates)
                self.assertTrue(all(day >= self.interval["cut"] for day in dates))

        def relabel(receipt, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    for segment in view["segments"]:
                        segment["historical_symbol"] = "EARLIER"
                        segment["observation_tickers"] = ["EARLIER"]

        self.publish_change(relabel)
        with reader._open_group("TEST", include_unresolved=True) as group:
            revoked = policy.evaluate(group, (first,))
        self.assertEqual(revoked["validation_work"]["checked_bars"], len(ADJUSTMENTS) * len(self.bars))
        self.assertNotIn("dated_price_token", revoked["history_validation"]["prefix_records"][0])
        self.assertNotIn("dated_price_interval", revoked["securities"][0])
