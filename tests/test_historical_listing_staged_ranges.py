"""Bounded reuse of completed assessment stages when an unpublished ticker range grows."""

import tempfile
from contextlib import closing
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

import test_historical_listing_backfill as migration_fixture
from test_alpaca_end_of_day_api import NativeCase, valid_bars

from libs.alpaca_lineage_build import listing_backfill, listing_staged_ranges
from libs.alpaca_lineage_build.listing_observations import Observations
from libs.alpaca_serving_catalogue.producer import maintain


class StagedAssessmentRangesTests(TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.store = Path(temp.name)
        (self.store / "end_of_day/lineage").mkdir(parents=True)
        self.client = Mock(side_effect=AssertionError("selector asked the provider"))

    def save(self, *scopes, version=1):
        with closing(Observations(self.store, self.client)) as cache:
            for scope in scopes:
                cache.put("assessment", cache.assessment_key(version, scope), {"stage": list(scope)})

    @staticmethod
    def scope(a, b, *, owner="owner", symbol="LABEL", competing=False):
        return owner, symbol, f"2026-10-{a:02d}", f"2026-10-{b:02d}", competing

    def test_direct_completed_scope_wins_without_reading_its_body_or_checking_smaller_bindings(self):
        whole = self.scope(2, 13)
        self.save(self.scope(2, 5), self.scope(6, 9), whole)
        group = {"listing_work_scopes": [whole]}
        with (
            closing(Observations(self.store, self.client, readonly=True)) as cache,
            patch.object(cache, "get", side_effect=AssertionError("opened staged assessment")),
            patch.object(cache, "assessment_key", wraps=cache.assessment_key) as binding,
            patch.object(
                listing_staged_ranges,
                "market_session_window",
                side_effect=AssertionError("resliced accepted range"),
            ),
        ):
            self.assertEqual(
                listing_staged_ranges.select_staged_ranges([(group, [whole])], cache, 1), [(group, [whole])]
            )
            binding.assert_called_once_with(1, whole)
        self.assertEqual(group["staged_listing_assessment_keys"], {whole: [1, *whole]})

    def test_scope_owner_rule_class_and_retained_observation_binding_select_only_eligible_stages(self):
        whole = self.scope(2, 13)
        left, overlap, right = self.scope(2, 5), self.scope(5, 7), self.scope(9, 12)
        # The pure staged selector must never promote a partial intersection,
        # another rule, changed input, different owner, ticker or conflict class.
        self.save(
            left,
            overlap,
            right,
            self.scope(1, 4),
            self.scope(12, 14),
            self.scope(6, 6),
            self.scope(8, 8),
            self.scope(13, 13, owner="other"),
            self.scope(13, 13, symbol="PEER"),
            self.scope(13, 13, competing=True),
        )
        self.save(self.scope(13, 13), version=2)
        carried = self.scope(15, 15)
        self.save(carried)
        prior = {"owner": [["LABEL", "2026-10-02", "2026-10-13"]]}
        group = {
            "superseded_listing_scopes": prior,
            "carried_listing_assessments": {carried: {"status": "unresolved"}},
        }
        with (
            closing(Observations(self.store, self.client, readonly=True)) as cache,
            patch.object(cache, "get", side_effect=AssertionError("opened stage bodies during planning")),
            patch.object(cache, "assessment_key", wraps=cache.assessment_key) as binding,
        ):
            cache.seed(
                {
                    "group": {
                        "owner": {
                            "trade_observations": [
                                {
                                    "request_symbol": "LABEL",
                                    "returned_symbol": "LABEL",
                                    "feed": "sip",
                                    "asof": "-",
                                    "session": "2026-10-08",
                                    "trade_timestamp": "2026-10-08T15:00:00Z",
                                    "tape_code": "C",
                                }
                            ]
                        }
                    }
                }
            )
            _, work = listing_staged_ranges.select_staged_ranges([(group, [whole, carried])], cache, 1)[0]
            self.assertNotIn(self.scope(6, 6), [call.args[1] for call in binding.call_args_list])
        self.assertEqual(work, [left, overlap, self.scope(8, 8), right, self.scope(13, 13), carried])
        self.assertEqual(group["listing_work_scopes"], work)
        self.assertEqual(set(group["staged_listing_assessment_keys"]), {left, overlap, right})
        self.assertIs(group["superseded_listing_scopes"], prior)
        self.assertIn(carried, group["carried_listing_assessments"])


class StagedExtensionIntegrationTests(NativeCase):
    def test_dry_run_staged_prefix_survives_normal_update_without_repeating_assessment(self):
        self.bootstrap()
        args = {"symbols": ["TEST"], "workers": 1}

        def client():
            return migration_fixture.ListingMigrationCommandTests.client(self)

        first = listing_backfill.run(self.store, client_factory=client, **args)
        (pid,) = first["assessments"]
        (prefix,) = first["assessments"][pid]
        self.assertEqual(prefix["last_session"], "2026-10-13")
        self.add_symbol("TEST", valid_bars(end="2026-10-15"), end="2026-10-15")
        self.assertTrue(maintain(self.store)["complete"])
        offline = Mock(side_effect=AssertionError("planning or staged publication requested the provider"))
        with patch(
            "libs.alpaca_lineage_build.storage.load_source",
            side_effect=AssertionError("reopened historical prices"),
        ):
            plan = listing_backfill.run(self.store, plan=True, client_factory=offline, **args)
            self.assertEqual(
                (plan["ranges_selected"], plan["staged_ranges"], plan["missing_assessments"]), (2, 1, 1)
            )
            with patch.object(
                listing_backfill, "_stage_body", side_effect=AssertionError("opened incomplete group's stage")
            ):
                partial = listing_backfill.run(self.store, publish_staged=True, client_factory=offline, **args)
            self.assertEqual((partial["groups_published"], partial["groups_waiting_for_assessment"]), (0, 1))
            with (
                patch.object(listing_backfill, "assess_range", wraps=listing_backfill.assess_range) as assess,
                patch.object(listing_backfill, "_stage_body", wraps=listing_backfill._stage_body) as read_stage,
            ):
                applied = listing_backfill.run(self.store, apply=True, client_factory=client, **args)
            self.assertEqual(read_stage.call_count, 1)
            self.assertEqual(assess.call_count, 1)
            self.assertEqual(assess.call_args.args[:3], ("TEST", "2026-10-14", "2026-10-14"))
            self.assertEqual(applied["groups_published"], 1)
            self.assertEqual(applied["assessments"][pid][0], prefix)
            self.assertEqual(applied["assessments"][pid][1]["first_session"], "2026-10-14")
            repeated = listing_backfill.run(self.store, apply=True, client_factory=offline, **args)
            self.assertEqual((repeated["groups_selected"], repeated["provider_requests"]), (0, 0))
        offline.assert_not_called()
