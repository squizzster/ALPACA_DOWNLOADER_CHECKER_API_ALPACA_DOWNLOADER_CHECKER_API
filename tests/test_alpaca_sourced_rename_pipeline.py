"""Sourced same-security history keeps its decisions through actual publication."""

import copy
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset, informative_bars
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import DataLibrary
from libs.alpaca_end_of_day_security_correlation import (
    correlation,
    row_correlation,
    security_identity,
    sourced_rename,
)
from libs.alpaca_end_of_day_security_correlation.security_identity import security_id_for_alpaca_asset_ids
from libs.alpaca_lineage_build import pipeline, pricing, storage
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_pricing_download.storage import read_update
from libs.alpaca_serving_catalogue.producer import maintain


class SourcedRenamePipelineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.root = self.store / "end_of_day/lineage"
        self.coverage_end = "2024-06-21"
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
        )
        self.current = asset("NEXT", "current-id")
        self.prior = {**asset("PRIOR", "old-id"), "status": "inactive", "tradable": False}
        self.other = asset("PRIOR", "other-id")
        self.rival = asset("PRIOR", "rival-id")
        self.actions = [
            {
                "id": "equity-rename",
                "action_type": "name_change",
                "old_symbol": "PRIOR",
                "new_symbol": "NEXT",
                "old_cusip": "111111111",
                "new_cusip": "222222222",
                "process_date": "2024-06-17",
            },
            {
                "id": "other-distribution",
                "action_type": "cash_dividend",
                "symbol": "PRIOR",
                "cusip": "333333333",
                "ex_date": "2024-06-20",
                "record_date": "2024-06-20",
                "payable_date": "2024-06-21",
                "process_date": "2024-06-20",
                "rate": 1,
            },
            {
                "id": "rival-distribution",
                "action_type": "cash_dividend",
                "symbol": "PRIOR",
                "cusip": "444444444",
                "ex_date": "2024-06-18",
                "process_date": "2024-06-18",
                "rate": 1,
            },
        ]
        self.supplement = {
            "kind": "identity_preserving_rename",
            "match": dict(self.actions[0]),
            "facts": {
                "schema_version": 1,
                "share_class": "common_stock",
                "unchanged_security": True,
                "unchanged_rights": True,
                "no_share_exchange": True,
                "legal_effective_date": "2024-06-14",
                "exchange_effective_date": "2024-06-17",
                "prior_labels": [
                    {
                        "symbol": symbol,
                        "cusip": "111111111",
                        "start_inclusive": first,
                        "end_exclusive": last,
                    }
                    for symbol, first, last in (
                        ("EARLY", "2024-01-01", "2024-06-12"),
                        ("PRIOR", "2024-06-12", "2024-06-17"),
                    )
                ],
            },
            "sources": [
                {
                    "url": "https://issuer.example/common-security-rename",
                    "title": "Common security and historical symbol notice",
                    "document_date": "2024-06-17",
                    "authority": "issuer",
                }
            ],
        }
        self.rows = {
            "NEXT": informative_bars("2024-06-10", "2024-06-21"),
            "PRIOR": informative_bars("2024-06-18", "2024-06-21"),
        }
        # The peer has an uninformative row before its first dated action. New
        # peer rows may reconsider that ambiguity; NEXT's accepted rows stay shut.
        self.rows["PRIOR"][0].update(o=10, h=10, l=10, c=10, v=0, n=0, vw=0)
        for row in self.rows["NEXT"]:
            for field in ("o", "h", "l", "c", "vw"):
                row[field] += 100
        for symbol, rows in self.rows.items():
            download_pricing(self.store, symbol, "2024-06-21", from_date="2024-01-01", client=FakeClient(rows))

    def client(self):
        return IdentityClient(
            {
                "NEXT": self.current,
                "PRIOR": self.other,
                "111111111": self.prior,
                "222222222": self.current,
                "333333333": self.other,
                "444444444": self.rival,
            },
            self.actions,
        )

    def build(self, *, submit=False, **kwargs):
        client = self.client()
        with patch.object(
            pipeline, "download_pricing", side_effect=AssertionError("unexpected pricing provider")
        ):
            result = pipeline.run_lineage(
                self.store,
                ["NEXT"],
                identity_client=client,
                event_supplements=[self.supplement] if submit else (),
                **kwargs,
            )
        self.assertEqual(result["failed"], 0, result)
        self.assertNotIn("EARLY", {call[0] for call in client.calls})
        return result

    def receipt(self, symbol="NEXT"):
        return storage.read_receipt(self.root, symbol, storage.read_index(self.root))

    def append(self, end="2024-06-25"):
        for symbol, rows in self.rows.items():
            added = informative_bars(self.coverage_end, end)
            if symbol == "NEXT":
                for row in added:
                    for field in ("o", "h", "l", "c", "vw"):
                        row[field] += 100
            self.rows[symbol] = [*rows, *added]
            download_pricing(self.store, symbol, end, client=FakeClient(self.rows[symbol]))
        self.coverage_end = end

    @contextmanager
    def forbid_old_decisions(self, through="2024-06-20"):
        decide = row_correlation._decide_row
        activity = correlation.has_informative_trading
        load = pricing.load_published_boundary
        source_history = sourced_rename.source_history
        identify = row_correlation.security_identity_for_alpaca_assets

        def row_decision(context, **kwargs):
            if context.requested_symbol == "NEXT" and context.session_date.isoformat() <= through:
                raise AssertionError("accepted same-security row decision repeated")
            return decide(context, **kwargs)

        def informative(rows):
            if rows and rows[0]["t"] == self.rows["NEXT"][0]["t"] and rows[0]["c"] > 100:
                raise AssertionError("accepted same-security activity proof repeated")
            return activity(rows)

        def boundary(root, source, kind, **kwargs):
            if source["symbol"] == "NEXT" and kind != "current_month":
                raise AssertionError("accepted same-security cold pricing reopened")
            return load(root, source, kind, **kwargs)

        def history(symbol, *args, **kwargs):
            if symbol == "NEXT":
                raise AssertionError("accepted source history proof repeated")
            return source_history(symbol, *args, **kwargs)

        def identity(ids, *args, **kwargs):
            ids = tuple(ids)
            if set(ids) & {"old-id", "current-id"}:
                raise AssertionError("accepted public identity membership recalculated")
            return identify(ids, *args, **kwargs)

        with (
            patch.object(
                sourced_rename, "certify_group", side_effect=AssertionError("membership proof repeated")
            ),
            patch.object(sourced_rename, "source_history", side_effect=history),
            patch.object(row_correlation, "_decide_row", side_effect=row_decision),
            patch.object(correlation, "has_informative_trading", side_effect=informative),
            patch.object(pricing, "load_published_boundary", side_effect=boundary),
            patch.object(row_correlation, "security_identity_for_alpaca_assets", side_effect=identity),
        ):
            yield

    def test_complete_publication_unifies_identity_with_exact_dated_labels(self):
        self.build(submit=True)
        receipt = self.receipt()
        self.assertEqual(receipt["unresolved_rows"], 0)
        self.assertEqual(receipt["correlated_rows"], len(self.rows["NEXT"]) * 8)
        self.assertEqual(
            [member["security_id"] for member in receipt["identity_memberships"]],
            [security_id_for_alpaca_asset_ids(["old-id", "current-id"])],
        )
        self.assertGreater(self.receipt("PRIOR")["unresolved_rows"], 0)
        from libs.alpaca_lineage_build.reader import read_lineage

        assignments = read_lineage(self.store, "NEXT")["adjustments"]["raw"]
        self.assertEqual(
            [(item["from"], item["to"], item["historical_symbol"]) for item in assignments],
            [
                ("2024-06-10", "2024-06-11", "EARLY"),
                ("2024-06-12", "2024-06-14", "PRIOR"),
                ("2024-06-17", "2024-06-20", "NEXT"),
            ],
        )
        with (
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("unchanged prices reopened")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("unchanged identity recalculated")),
            patch.object(storage, "publish", side_effect=AssertionError("unchanged publication repeated")),
        ):
            self.build()

    def test_append_keeps_same_id_and_historical_decisions_despite_unresolved_peer(self):
        self.build(submit=True)
        before = self.receipt()
        self.append()
        with self.forbid_old_decisions():
            self.build()
        after = self.receipt()
        self.assertEqual(before["identity_memberships"], after["identity_memberships"])
        self.assertEqual(after["unresolved_rows"], 0)
        self.assertEqual(after["sourced_rename_ownership"]["last"], "2024-06-24")

    def test_new_facts_repair_existing_publication_without_changing_prices(self):
        self.build()
        before = self.receipt()
        self.assertGreater(before["unresolved_rows"], 0)
        self.build(submit=True)
        after = self.receipt()
        self.assertEqual(after["source_sha256"], before["source_sha256"])
        self.assertEqual(after["unresolved_rows"], 0)
        self.assertEqual(
            [item["security_id"] for item in after["identity_memberships"]],
            [security_id_for_alpaca_asset_ids(["old-id", "current-id"])],
        )

    def test_public_reader_keeps_one_continuing_id_and_separate_reused_label(self):
        self.build(submit=True)
        public_id = self.receipt()["identity_memberships"][0]["security_id"]
        with (
            patch.object(
                sourced_rename, "certify_group", side_effect=AssertionError("consumer membership proof")
            ),
            patch.object(sourced_rename, "source_history", side_effect=AssertionError("consumer source proof")),
            patch.object(
                security_identity,
                "security_id_for_alpaca_asset_ids",
                side_effect=AssertionError("consumer public ID recalculation"),
            ),
        ):
            self.assertTrue(maintain(self.store, rebuild=True)["complete"])
            data = DataLibrary(store=self.store)
            histories = data.get_histories(public_id)
            matches = {label: data.resolve_ticker(label) for label in ("EARLY", "PRIOR", "NEXT")}
        self.assertEqual(set(histories), set(pricing.ADJUSTMENTS))
        self.assertTrue(all(len(history.bars) == len(self.rows["NEXT"]) for history in histories.values()))
        for label, first, last in (
            ("EARLY", "2024-06-10", "2024-06-11"),
            ("PRIOR", "2024-06-12", "2024-06-14"),
            ("NEXT", "2024-06-17", "2024-06-20"),
        ):
            observed = {
                (item.public_id, str(item.first_session), str(item.last_session)) for item in matches[label]
            }
            self.assertIn((public_id, first, last), observed)
        self.assertEqual({item.public_id for item in matches["EARLY"] + matches["NEXT"]}, {public_id})
        self.assertTrue(any(item.public_id != public_id for item in matches["PRIOR"]))

    def test_journal_recovery_consumes_completed_identity_and_source_history(self):
        with patch.object(storage, "finish_publication", side_effect=RuntimeError("interrupted rename journal")):
            failed = pipeline.run_lineage(
                self.store, ["NEXT"], identity_client=self.client(), event_supplements=[self.supplement]
            )
        self.assertEqual(failed["failed"], 1, failed)
        self.assertTrue((self.root / ".pending.json").exists())
        with (
            patch.object(
                sourced_rename, "certify_group", side_effect=AssertionError("recovery repeated membership")
            ),
            patch.object(
                sourced_rename, "source_history", side_effect=AssertionError("recovery repeated history")
            ),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("recovery repeated prices")),
        ):
            self.build()
        self.assertEqual(self.receipt()["unresolved_rows"], 0)

    def test_weekend_rollover_uses_producers_actual_changed_session(self):
        self.build(submit=True)
        self.append("2024-06-29")
        with self.forbid_old_decisions():
            self.build()
        before = self.receipt()
        # June 29/30 have no sessions. Pricing's completed change notice starts
        # on July 1; the accepted June partition must never be reopened.
        self.append("2024-07-02")
        with self.forbid_old_decisions(through="2024-06-28"):
            self.build()
        after = self.receipt()
        self.assertEqual(before["identity_memberships"], after["identity_memberships"])
        self.assertEqual(after["sourced_rename_ownership"]["last"], "2024-07-01")

    def test_actual_new_prior_month_row_is_not_hidden_by_calendar_assumptions(self):
        self.build(submit=True)
        self.append("2024-06-29")
        self.build()
        before = self.receipt()
        unexpected = {**self.rows["NEXT"][-1], "t": "2024-06-30T04:00:00Z"}
        self.rows["NEXT"].append(unexpected)
        self.append("2024-07-02")
        root = self.store / "end_of_day/pricing/NEXT"
        source, notice = pricing.load_source(root, "NEXT"), read_update(root)
        self.assertEqual(notice["from"], "2024-06-30")
        load, selected = pricing.load_published_boundary, []

        def boundary(root, source, kind, **kwargs):
            selected.append(kind)
            return load(root, source, kind, **kwargs)

        with patch.object(pricing, "load_published_boundary", side_effect=boundary):
            view = pricing.load_pricing(
                self.store,
                "NEXT",
                source=source,
                previous=before,
                previous_boundaries={},
                resume_from=before["processed_through"][:10],
                selected_correction=None,
                selected_change_from=notice["from"],
            )
        self.assertEqual(selected, ["current_year", "current_month"])
        self.assertEqual([row[0][:10] for row in view.views["raw"]], ["2024-06-30", "2024-07-01"])

    def test_price_replacement_renews_source_proof_without_repeating_membership(self):
        self.build(submit=True)
        before = self.receipt()
        changed = copy.deepcopy(self.rows["NEXT"])
        for field in ("o", "h", "l", "c", "vw"):
            changed[0][field] += 1
        download_pricing(self.store, "NEXT", "2024-06-21", client=FakeClient(changed), refresh_all=True)
        with (
            patch.object(
                sourced_rename, "certify_group", side_effect=AssertionError("price change repeated membership")
            ),
            patch.object(sourced_rename, "source_history", wraps=sourced_rename.source_history) as proof,
        ):
            self.build()
        self.assertEqual(sum(call.args[0] == "NEXT" for call in proof.call_args_list), 1)
        after = self.receipt()
        self.assertNotEqual(before["source_sha256"], after["source_sha256"])
        self.assertEqual(before["identity_memberships"], after["identity_memberships"])
        self.assertEqual(after["unresolved_rows"], 0)

    def test_changed_mapping_cannot_inherit_source_ownership(self):
        self.build(submit=True)
        download_pricing(
            self.store,
            "NEXT",
            "2024-06-21",
            client=FakeClient(self.rows["NEXT"]),
            refresh_all=True,
            mapping_asof="2024-06-14",
        )
        with patch.object(sourced_rename, "source_history", wraps=sourced_rename.source_history) as proof:
            self.build()
        self.assertIsNone(self.receipt()["sourced_rename_ownership"])
        self.assertEqual(sum(call.args[0] == "NEXT" for call in proof.call_args_list), 1)

    def test_empty_holiday_advance_does_not_reopen_completed_work(self):
        self.store = self.store.parent / "holiday-store"
        self.root = self.store / "end_of_day/lineage"
        for action in self.actions:
            if action["id"] == "other-distribution":
                for key in ("process_date", "ex_date", "record_date"):
                    action[key] = "2024-06-18"
        self.rows = {
            name: [row for row in rows if row["t"][:10] < "2024-06-19"] for name, rows in self.rows.items()
        }
        for symbol, rows in self.rows.items():
            download_pricing(self.store, symbol, "2024-06-19", from_date="2024-01-01", client=FakeClient(rows))
        self.build(submit=True)
        before = self.receipt()
        for symbol, rows in self.rows.items():
            download_pricing(self.store, symbol, "2024-06-20", client=FakeClient(rows))
        with (
            patch.object(
                pipeline, "correlate", side_effect=AssertionError("empty advance recalculated identity")
            ),
            patch.object(
                pricing, "load_published_boundary", side_effect=AssertionError("empty advance read prices")
            ),
            patch.object(
                sourced_rename, "certify_group", side_effect=AssertionError("empty advance repeated membership")
            ),
            patch.object(
                sourced_rename, "source_history", side_effect=AssertionError("empty advance repeated history")
            ),
        ):
            self.build()
        after = self.receipt()
        self.assertEqual(before["identity_memberships"], after["identity_memberships"])
        self.assertEqual(before["sourced_rename_ownership"], after["sourced_rename_ownership"])
