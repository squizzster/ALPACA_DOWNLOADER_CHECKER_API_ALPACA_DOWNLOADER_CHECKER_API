"""A selected split window trusts provider observations and survives a retry."""

from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch

from test_alpaca_shifted_split import DAYS, fixture

from libs.alpaca_lineage_build import dated_price_publication as lineage
from libs.alpaca_lineage_build.contract import RecoveryRequired
from libs.alpaca_pricing_download import dated_publication as pricing
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval


class TimingHandoffTests(TestCase):
    def test_restart_uses_bound_window_and_completed_price_decision(self):
        views, actions = fixture("NEWSEC")
        pid = "1" * 64
        with TemporaryDirectory() as folder:
            store = Path(folder)
            (store / "end_of_day/pricing").mkdir(parents=True)
            binding = {
                "public_id": pid,
                "cut": DAYS[1],
                "decision_generation": "g",
                "repair_kind": "split_timing",
            }
            with patch("libs.alpaca_pricing_download.shifted_split.assess", side_effect=OSError("interrupted")):
                with self.assertRaises(OSError):
                    pricing.acquire_selected(store, binding, lambda: {"views": views, "actions": actions}, None)
            forbidden = Mock(side_effect=AssertionError("held inputs reopened"))
            result = pricing.acquire_selected(store, binding, forbidden, None)
            self.assertEqual(result["repair_kind"], "split_timing")
            with patch(
                "libs.alpaca_pricing_download.shifted_split.assess",
                side_effect=AssertionError("prices recomputed"),
            ):
                self.assertEqual(pricing.acquire_selected(store, binding, forbidden, None), result)
            rejected = {**binding, "decision_generation": "changed"}
            with self.assertRaises(UnsupportedInterval):
                pricing.acquire_selected(store, rejected, lambda: {"views": views, "actions": []}, None)
            with self.assertRaises(UnsupportedInterval):
                pricing.acquire_selected(store, rejected, forbidden, None)

    def test_changed_relevant_action_revokes_only_bound_correction(self):
        _, actions = fixture("NEWSEC")
        pid = "1" * 64
        interval = {
            "repair_kind": "split_timing",
            "symbol": "NEWSEC",
            "first": DAYS[1],
            "cut": DAYS[2],
            "witness_start": DAYS[0],
            "witness_end": DAYS[2],
            "members": ["NEWSEC"],
            "unavailable": [],
        }
        old = {
            "source_sha256": "g",
            "boundaries": {},
            "input_sources": {"NEWSEC": "g"},
            "evidence": {"corporate_actions": actions},
            lineage.FIELD: {pid: interval},
        }
        same = dict(old)
        with patch.object(lineage, "action_window", side_effect=AssertionError("reopened accepted metadata")):
            lineage.retain(old, same)
        for changed in (False, True):
            new = deepcopy(old)
            new["evidence"]["corporate_actions"].append(
                {"action_type": "cash_dividend", "symbol": "NEWSEC", "ex_date": "2030-01-01"}
            )
            if changed:
                new["evidence"]["corporate_actions"][0]["old_rate"] = 4
            lineage.retain(old, new)
            self.assertEqual(lineage.FIELD in new, not changed)

    def test_research_overlay_cannot_be_used_as_a_provider_action(self):
        _, actions = fixture("NEWSEC")
        fake = {"corporate_actions": actions, "event_supplements": [{"issuer": "research"}]}
        with self.assertRaises(ValueError):
            list(lineage.action_window(fake, DAYS[0], DAYS[2]))
        fake["provider_corporate_actions"] = []
        self.assertEqual(list(lineage.action_window(fake, DAYS[0], DAYS[2])), [])


class RebuiltSplitTimingTests(TestCase):
    def setUp(self):
        self.pid = "1" * 64
        self.item = dict(
            repair_kind="split_timing",
            symbol="NEWSEC",
            token="bound",
            first=DAYS[1],
            witness_start=DAYS[0],
            witness_end=DAYS[2],
            members=["NEWSEC"],
            unavailable=[],
        )
        self.run = dict(owner=self.pid, tickers=["NEWSEC"], state="selected", **{"from": DAYS[0], "to": DAYS[2]})
        self.choices = {"views": dict.fromkeys(ADJUSTMENTS, 0), "partitions": [{"runs": [self.run]}]}
        request = {"asof": "2026-11-18", "end_exclusive": "2026-11-18", "feed": "sip"}
        self.source = {
            "latest_request": request,
            "history_changes": [{"source_sha256": "old", "changed_from": "2026-11-18"}],
        }
        self.old = {
            "source_sha256": "old",
            "boundaries": {"current_year": {"sha256": "a"}},
            "anchor_month": "2026-11",
            "pricing_request": {**request, "history_start": None},
            "evidence": {"corporate_actions": []},
            lineage.FIELD: {self.pid: self.item},
        }
        self.new = {
            **self.old,
            "source_sha256": "new",
            "boundaries": {"current_year": {"sha256": "b"}},
            "input_sources": {"NEWSEC": "new"},
            "assignment_handoffs": {"current_year": {"observation_choices": self.choices}},
        }
        self.new.pop(lineage.FIELD)

    def rebuild(self):
        with (
            patch(
                "libs.alpaca_pricing_download.shifted_split.assess",
                side_effect=AssertionError("repeated numerical decision"),
            ),
            patch.object(pricing, "assemble", side_effect=AssertionError("opened historical price payload")),
            patch.object(lineage.storage, "read_boundaries", side_effect=AssertionError("reopened assignments")),
        ):
            lineage.retain(self.old, self.new, full_materialization=True, source=self.source)

    def test_legacy_single_source_accepts_completed_view_choices_across_rollover(self):
        self.new["anchor_month"] = "2027-01"
        self.new["boundaries"] = {"completed_years": {"sha256": "rebuilt"}}
        self.new["assignment_handoffs"]["completed_years"] = self.new["assignment_handoffs"].pop("current_year")
        self.rebuild()
        self.assertIs(self.new[lineage.FIELD][self.pid], self.item)

    def test_multi_source_rebuild_keeps_the_original_contributors(self):
        self.item["members"].append("OTHER")
        self.new["input_sources"]["OTHER"] = "peer"
        self.old["assignment_handoffs"] = deepcopy(self.new["assignment_handoffs"])
        self.rebuild()
        self.assertIs(self.new[lineage.FIELD][self.pid], self.item)
        self.choices["views"]["all"] = 1
        self.choices["partitions"].append({"runs": [{**self.run, "state": "superseded"}]})
        with self.assertRaises(RecoveryRequired):
            self.rebuild()

    def test_changed_or_missing_witness_fails_before_publication(self):
        for case in (
            "price",
            "ancestry",
            "mapping",
            "owner",
            "label",
            "choice",
            "missing_view_handoff",
            "action",
            "member",
            "unavailable",
            "legacy_peer",
        ):
            with self.subTest(case=case):
                self.setUp()
                if case == "price":
                    self.source["history_changes"][0]["changed_from"] = DAYS[2]
                elif case == "ancestry":
                    self.source["history_changes"].clear()
                elif case == "mapping":
                    self.source["latest_request"]["asof"] = "2020-01-01"
                elif case == "owner":
                    self.run["owner"] = "different-owner"
                elif case == "label":
                    self.run["tickers"] = ["OTHER"]
                elif case == "choice":
                    self.run["state"] = "conflict"
                elif case == "missing_view_handoff":
                    self.new["assignment_handoffs"].clear()
                elif case == "action":
                    self.new["evidence"] = {
                        "corporate_actions": [
                            {"action_type": "reverse_split", "symbol": "NEWSEC", "ex_date": DAYS[1]}
                        ]
                    }
                elif case == "unavailable":
                    self.new["unavailable_pricing"] = {"OTHER": {}}
                else:
                    self.new["input_sources"]["OTHER"] = "peer"
                    if case == "legacy_peer":
                        self.item["members"].append("OTHER")
                with self.assertRaises(RecoveryRequired):
                    self.rebuild()
                self.assertNotIn(lineage.FIELD, self.new)

    def test_equal_assignment_generation_uses_no_choice_projection(self):
        self.new["boundaries"] = self.old["boundaries"]
        with patch(
            "libs.alpaca_lineage_build.split_timing._choices",
            side_effect=AssertionError("rechecked equal assignments"),
        ):
            self.rebuild()
        self.assertIs(self.new[lineage.FIELD][self.pid], self.item)


class NativeTimingRebuildTests(TestCase):
    def test_later_append_and_full_refresh_preserve_corrected_public_price(self):
        from datetime import date
        from decimal import Decimal

        from test_alpaca_lineage_build import IdentityClient, asset
        from test_alpaca_pricing_download import FakeClient, direct_adjustment_observations

        from alpaca_end_of_day import DataLibrary
        from libs.alpaca_lineage_build.dated_price_repair import repair_many
        from libs.alpaca_lineage_build.pipeline import run_lineage
        from libs.alpaca_pricing_download.download import download_pricing
        from libs.alpaca_serving_catalogue.producer import maintain
        from libs.common.store_lock import operation_lock

        name = "EXAMPLE"
        views, actions = fixture(name)
        identity = IdentityClient({name: asset(name)}, [{**actions[0], "id": "example-action"}])
        fields = {
            "t": "timestamp",
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
            "vw": "vwap",
            "v": "volume",
            "n": "trade_count",
        }
        clients = {
            mode: FakeClient(
                [
                    {key: row[field] if key == "t" else Decimal(row[field]) for key, field in fields.items()}
                    for row in view["bars"]
                ]
            )
            for mode, view in views.items()
        }

        class Bars:
            def fetch(self, symbol, adjustment, start, end, *, asof=None):
                return clients[adjustment].fetch(symbol, adjustment, start, end, asof=asof)

        with TemporaryDirectory() as folder, direct_adjustment_observations():
            store = Path(folder) / "store"
            bars = Bars()
            download_pricing(store, name, "2026-03-20", client=bars)
            self.assertFalse(run_lineage(store, [name], identity_client=identity)["failed"])
            self.assertTrue(maintain(store, bootstrap=True)["complete"])
            (security,) = DataLibrary(store=store).list_securities()
            pid = security.public_id
            with operation_lock(store, exclusive=True):
                result = repair_many(store, [pid], [DAYS[1]], None, repair_kind="split_timing")
            self.assertEqual(result["outcomes"][0]["status"], "published", result)
            for client in clients.values():
                client.bars.append({**client.bars[-1], "t": "2026-03-20T04:00:00Z"})
            updated = download_pricing(store, name, "2026-03-21", client=bars)
            with patch(
                "libs.alpaca_pricing_download.shifted_split.assess",
                side_effect=AssertionError("repeated correction"),
            ):
                rebuilt = run_lineage(
                    store, [name], identity_client=identity, refresh=True, pricing_handoff={name: updated}
                )
                self.assertFalse(rebuilt["failed"], rebuilt)
                self.assertFalse(rebuilt["results"][0]["reused_closed_boundaries"])
                self.assertTrue(maintain(store, symbols=[name])["complete"])
            day = date.fromisoformat(DAYS[1])
            price = DataLibrary(store=store).get_histories(pid, ("raw", "all"), start=day, end=day)
            self.assertEqual(
                (price["raw"].bars[0].close, price["all"].bars[0].close), (Decimal("9.05"), Decimal("27.15"))
            )
