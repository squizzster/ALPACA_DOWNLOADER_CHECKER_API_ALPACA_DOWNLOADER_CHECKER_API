"""A retired symbol lookup and current UUID are separate provider observations."""

from copy import deepcopy
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import test_alpaca_universe_update as fixtures
from test_alpaca_end_of_day_api import valid_bars
from test_alpaca_end_of_day_security_correlation import correlation_source
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import DataLibrary
from libs.alpaca_end_of_day_security_correlation import row_correlation as rows
from libs.alpaca_lineage_build import pipeline, storage, universe_state
from libs.alpaca_lineage_build.catalogue import completed_catalogue
from libs.alpaca_lineage_build.check_summary import rebuild
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.metadata import (
    EvidenceClient,
    RetainedReportClient,
    WindowClient,
    acquire,
    dependencies,
)
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.universe_update import rebuild_universe
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue.producer import maintain

IDENTIFIER = "0498c539-18bd-4e1d-a302-9e3c09592267"
OLD, NEW = "FORMER", "CURRENT"


def observation(records):
    return {**completed_catalogue(records, {}), "catalogue_observed_at_utc": "2026-10-20T06:00:00+00:00"}


class RetiredLabelTests(TestCase):
    def setUp(self):
        self.old, self.new = asset(OLD, IDENTIFIER), asset(NEW, IDENTIFIER)
        self.view = SimpleNamespace(
            symbol=OLD,
            local={"present": True, "downloads": []},
            source={"history_start": "2026-10-01", "latest_request": {"end_exclusive": "2026-10-22"}},
        )
        self.prior = {
            "symbol": OLD,
            "anchor_month": "2026-10",
            "evidence": acquire(
                self.view, EvidenceClient(IdentityClient({}), observation=observation([self.old]))
            ),
        }

    def test_exact_observations_are_cached_and_keep_distinct_labels(self):
        for records, expected in (
            ([self.new], [OLD]),
            ([asset("PEER")], [OLD, IDENTIFIER]),
            ([self.new, {**self.new, "symbol": "OTHER"}], [OLD, IDENTIFIER]),
            ([{**self.new, "symbol": "OTHER", "name": ""}, self.new], [OLD, IDENTIFIER]),
        ):
            client = IdentityClient({IDENTIFIER: self.new})
            with (
                self.subTest(records=records),
                patch.object(client, "get_asset_lookup", wraps=client.get_asset_lookup) as lookup,
            ):
                evidence_client = EvidenceClient(client, observation=observation(records))
                first = acquire(self.view, evidence_client, self.prior)
                self.assertEqual([c.args[0] for c in lookup.call_args_list], expected)
                self.assertEqual(dependencies(first, OLD), [NEW])
                own, uuid = first["asset_lookups"]
                self.assertEqual((own["lookup"], own["asset"], own["http_status"]), (OLD, None, 404))
                self.assertEqual((uuid["lookup"], uuid["asset"]["symbol"]), (IDENTIFIER, NEW))
                self.assertEqual(uuid["retained_symbol_lookup"]["asset"]["symbol"], OLD)
                self.assertEqual(first["symbol_changes"], [])
                self.assertEqual(first["identities"][0]["lookup_symbols"], [])
                self.assertEqual(first, acquire(self.view, evidence_client, self.prior))
                self.assertEqual([c.args[0] for c in lookup.call_args_list], expected)

    def test_exact_failure_stays_pending_and_wrong_uuid_is_rejected(self):
        for failure in ("mismatched", "timeout"):
            with self.subTest(failure=failure):
                bare = IdentityClient({IDENTIFIER: asset(NEW, "100cdb75-66e6-4aca-848a-64957ed68cba")})
                client = EvidenceClient(bare)
                calls = []
                get = bare.get_asset_lookup

                def lookup(identifier):
                    calls.append(identifier)
                    if identifier == IDENTIFIER and failure == "timeout":
                        raise TimeoutError("connection lost")
                    return get(identifier)

                with patch.object(bare, "get_asset_lookup", side_effect=lookup):
                    for _ in range(2):
                        with self.assertRaisesRegex((TimeoutError, LineageError), "lost|incompatible"):
                            acquire(self.view, client, self.prior)
                self.assertEqual(calls, [OLD, IDENTIFIER])

    def test_reassembling_an_old_positive_does_not_extend_its_observation_date(self):
        next_catalogue = {**observation([self.new]), "catalogue_observed_at_utc": "2026-10-22T08:00:00+00:00"}
        bare = IdentityClient({})
        client = EvidenceClient(bare, observation=next_catalogue)
        saved = RetainedReportClient(client, self.prior["evidence"])
        retained = acquire(self.view, saved, self.prior)
        self.assertEqual(retained["catalogue_observed_at_utc"], next_catalogue["catalogue_observed_at_utc"])
        facts = acquire(self.view, client, {**self.prior, "evidence": retained})
        self.assertEqual(
            facts["asset_lookups"][1]["retained_symbol_lookup"]["catalogue_observed_at_utc"],
            self.prior["evidence"]["catalogue_observed_at_utc"],
        )
        report = rows.correlate_download_rows(
            valid_bars("2026-10-19", "2026-10-22"), correlation_source(OLD, asof="2026-10-22"), facts, []
        )
        self.assertEqual(report["uncorrelated"][0]["first_session_date"], "2026-10-21")

    def test_observations_bound_supported_dates_and_keep_conflicts_explicit(self):
        facts = acquire(
            self.view, EvidenceClient(IdentityClient({}), observation=observation([self.new])), self.prior
        )
        bars = valid_bars("2026-10-19", "2026-10-22")
        source = correlation_source(OLD, asof="2026-10-22")
        with patch.object(rows, "_retired_label_observation", wraps=rows._retired_label_observation) as assess:
            cache = rows.CorrelationCache()
            for _ in range(2):
                result = rows.correlate_download_rows(bars, source, facts, [], cache=cache)
            assess.assert_called_once()
        self.assertEqual(
            [(s["first_session_date"], s["last_session_date"]) for s in result["correlated"]],
            [("2026-10-19", "2026-10-20")],
        )
        self.assertEqual(result["uncorrelated"][0]["first_session_date"], "2026-10-21")
        self.assertIn("effective_rename_date_unknown", result["correlated"][0]["warnings"])
        self.assertIsNone(result["correlated"][0]["ticker_ownership_interval"])
        historical = rows.correlate_download_rows(
            bars, source, facts, [], cache=rows.CorrelationCache(retired_label_observations=False)
        )
        self.assertFalse(historical["correlated"])
        conflicting = deepcopy(facts)
        conflicting["corporate_actions"].append(
            {
                "action_type": "name_change",
                "id": "other",
                "old_symbol": OLD,
                "new_symbol": "DIFFERENT",
                "process_date": "2026-10-19",
            }
        )
        self.assertFalse(rows.correlate_download_rows(bars, source, conflicting, [])["correlated"])
        nested = deepcopy(facts)
        nested["corporate_actions"].append(
            {"action_type": "reorganization", "stock_movements": [{"symbol": OLD}]}
        )
        self.assertFalse(rows.correlate_download_rows(bars, source, nested, [])["correlated"])
        for stamp in ("unknown", "2026-10-20", 42):
            unknown_time = deepcopy(facts)
            unknown_time["asset_lookups"][1]["retained_symbol_lookup"]["catalogue_observed_at_utc"] = stamp
            self.assertFalse(rows.correlate_download_rows(bars, source, unknown_time, [])["correlated"])

    def test_reused_label_negative_uuid_and_missing_anchor(self):
        fresh = asset(OLD, "4ca133ff-74a6-4c33-9a54-0619969cc725")
        client = IdentityClient({})
        with patch.object(client, "get_asset_lookup", wraps=client.get_asset_lookup) as lookup:
            new_occupant = acquire(
                self.view, EvidenceClient(client, observation=observation([fresh])), self.prior
            )
            self.assertEqual(new_occupant["asset_lookups"][0]["asset"], fresh)
            self.assertEqual(len(new_occupant["asset_lookups"]), 1)
            lookup.assert_not_called()
            without_anchor = acquire(self.view, EvidenceClient(client, observation=observation([self.new])))
            self.assertEqual(len(without_anchor["asset_lookups"]), 1)
            self.assertEqual([c.args[0] for c in lookup.call_args_list], [OLD])
            lookup.reset_mock()
            negative = acquire(
                self.view, EvidenceClient(client, observation=observation([asset("UNRELATED")])), self.prior
            )
            self.assertEqual(negative["asset_lookups"][1]["http_status"], 404)
            self.assertFalse(negative["identities"])
            self.assertEqual([c.args[0] for c in lookup.call_args_list], [OLD, IDENTIFIER])


class RetiredLabelUpdateTests(TestCase):
    def setUp(self):
        f = fixtures.UniverseUpdateTests()
        f.setUp()
        self.addCleanup(f.doCleanups)
        prices = valid_bars("2026-10-01", "2026-10-20")
        old = asset(OLD, IDENTIFIER)
        initial = EvidenceClient(IdentityClient({OLD: old}), observation=observation([old]))
        download_pricing(f.store, OLD, "2026-10-20", client=FakeClient(prices))
        self.assertEqual(
            run_lineage(f.store, [OLD], evidence_client=initial, identity_client=initial.client)["failed"], 0
        )
        rebuild(f.store)
        rebuild_universe(f.store)
        maintain(f.store)
        self.f = f
        self.before = DataLibrary(store=f.store)
        self.prior = self.before.resolve_ticker(OLD, on="2026-10-19")[0]
        f.live[OLD] = valid_bars("2026-10-01", "2026-10-21")
        f.live[NEW] = []

    def test_advance_retains_prefix_and_serves_the_supported_day_under_prior_id(self):
        f, prior = self.f, self.prior
        previous = self.before.get_history(prior.public_id).bars
        f.records[:] = [asset("AAA"), asset(NEW, IDENTIFIER)]
        f.identity = fixtures.CatalogueClient(f.records)
        with (
            patch.object(f.identity, "get_asset_lookup", wraps=f.identity.get_asset_lookup) as lookup,
            patch.object(pipeline, "load_pricing", wraps=pipeline.load_pricing) as loads,
        ):
            result = f.update("2026-10-21")
        self.assertEqual(
            [call.kwargs.get("resume_from") for call in loads.call_args_list if call.args[1] == OLD],
            ["2026-10-20"],
        )
        self.assertTrue(result["complete"], result)
        self.assertNotIn(IDENTIFIER, [item.args[0] for item in lookup.call_args_list])
        current = DataLibrary(store=f.store)
        self.assertEqual(current.get_history(prior.public_id).bars[:-1], previous)
        self.assertEqual(current.resolve_ticker(OLD, on="2026-10-20")[0].public_id, prior.public_id)
        self.assertEqual(current.resolve_ticker(NEW, on="2026-10-19"), ())
        root = f.store / "end_of_day/lineage"
        index = storage.read_index(root)
        self.assertEqual(index["symbols"][OLD]["group_members"], sorted([OLD, NEW]))
        self.assertTrue(index["symbols"][OLD]["projection"]["retired_uuid_assessed"])

    def test_same_cutoff_adopts_legacy_absence_then_reopens_only_on_observed_successor(self):
        f = self.f
        cutoff = "2026-10-20"
        last_day = "2026-10-19"
        accepted_history = self.before.get_history(self.prior.public_id).bars
        # Reproduce a legacy publication with an exact symbol 404 and no UUID
        # lookup. All subsequent work goes through an ordinary current update.
        with patch.object(WindowClient, "retired_asset_lookup", return_value=None):
            legacy = run_lineage(f.store, [OLD], identity_client=IdentityClient({}), refresh=True)
        self.assertGreater(legacy["results"][0]["unresolved_rows"], 0)
        self.assertTrue(maintain(f.store)["complete"])
        self.assertEqual(DataLibrary(store=f.store).resolve_ticker(OLD, on=last_day), ())
        f.live[OLD] = valid_bars("2026-10-01", cutoff)
        f.records[:] = [asset("AAA")]
        f.identity = fixtures.CatalogueClient(f.records)
        with (
            patch.object(f.identity, "get_asset_lookup", wraps=f.identity.get_asset_lookup) as lookup,
            patch.object(pipeline, "load_pricing", wraps=pipeline.load_pricing) as loads,
        ):
            empty = f.update(cutoff)
            self.assertTrue(empty["complete"], empty)
            self.assertEqual([call.args[0] for call in lookup.call_args_list], [IDENTIFIER])
            self.assertEqual(
                [call.kwargs["resume_from"] for call in loads.call_args_list if call.args[1] == OLD],
                [cutoff],
            )
            self.assertEqual(DataLibrary(store=f.store).resolve_ticker(OLD, on=last_day), ())
            lookup.reset_mock()
            loads.reset_mock()
            unchanged = f.update(cutoff)
            self.assertEqual(unchanged["stages"]["serving_catalogue"]["published_components"], 0)
            lookup.assert_not_called()
            loads.assert_not_called()
            f.records.append(asset(NEW, IDENTIFIER))
            found = f.update(cutoff)
            self.assertTrue(found["complete"], found)
            lookup.assert_not_called()
        current = DataLibrary(store=f.store)
        self.assertEqual(current.resolve_ticker(OLD, on=last_day)[0].public_id, self.prior.public_id)
        self.assertEqual(current.get_history(self.prior.public_id).bars, accepted_history)
        self.assertEqual(current.resolve_ticker(NEW, on=last_day), ())

    def test_restart_consumes_exact_uuid_checkpoint_without_http_replay(self):
        f = self.f
        f.records[:] = [asset("AAA")]
        new = asset(NEW, IDENTIFIER)
        f.identity = fixtures.CatalogueClient(f.records, exact={IDENTIFIER: new, NEW: new})
        accept = universe_state.accept_retired_uuid

        def interrupted(*args):
            accept(*args)
            raise KeyboardInterrupt

        with patch.object(f.identity, "get_asset_lookup", wraps=f.identity.get_asset_lookup) as lookup:
            with (
                patch.object(universe_state, "accept_retired_uuid", side_effect=interrupted),
                self.assertRaises(KeyboardInterrupt),
            ):
                f.update("2026-10-21")
            self.assertIn(IDENTIFIER, [call.args[0] for call in lookup.call_args_list])
            accepted = universe_state.read_state(f.store)["work"][OLD]["retired_uuid_observation"]
            self.assertEqual(accepted["lookup"]["asset"]["symbol"], NEW)
            lookup.reset_mock()
            result = f.update("2026-10-21")
            self.assertTrue(result["complete"], result)
            attempted = [call.args[0] for call in lookup.call_args_list]
            self.assertNotIn(IDENTIFIER, attempted)
            self.assertNotIn(OLD, attempted)
        self.assertEqual(
            DataLibrary(store=f.store).resolve_ticker(OLD, on="2026-10-20")[0].public_id, self.prior.public_id
        )
        self.assertNotIn("retired_uuid_observation", universe_state.read_state(f.store)["work"][OLD])

    def test_changed_uuid_conflict_selects_one_exact_resolution_then_reuses_it(self):
        f = self.f
        cutoff = "2026-10-21"
        current = asset(NEW, IDENTIFIER)
        f.records[:] = [asset("AAA"), current]
        f.identity = fixtures.CatalogueClient(f.records, exact={IDENTIFIER: current})
        root = f.store / "end_of_day/lineage"
        with patch.object(f.identity, "get_asset_lookup", wraps=f.identity.get_asset_lookup) as lookup:
            self.assertTrue(f.update(cutoff)["complete"])
            self.assertNotIn(IDENTIFIER, [item.args[0] for item in lookup.call_args_list])
            for contender in ("OTHER", "ANOTHER"):
                f.records.append(asset(contender, IDENTIFIER))
                lookup.reset_mock()
                if contender == "ANOTHER":
                    accept = universe_state.accept_retired_uuid

                    def interrupted(*args):
                        accept(*args)
                        raise KeyboardInterrupt

                    with (
                        patch.object(universe_state, "accept_retired_uuid", side_effect=interrupted),
                        self.assertRaises(KeyboardInterrupt),
                    ):
                        f.update(cutoff)
                disputed = f.update(cutoff)
                self.assertTrue(disputed["complete"], disputed)
                self.assertEqual([item.args[0] for item in lookup.call_args_list].count(IDENTIFIER), 1)
                completed = storage.read_index(root, OLD)["symbols"][OLD]["projection"]["retired_uuid_lookup"]
                self.assertEqual(completed["asset"]["symbol"], NEW)
                self.assertEqual(len(completed["catalogue_conflict"]), len(f.records) - 1)
                lookup.reset_mock()
                f.records.reverse()
                stable = f.update(cutoff)
                self.assertTrue(stable["complete"], stable)
                self.assertEqual(stable["stages"]["serving_catalogue"]["published_components"], 0)
                self.assertNotIn(IDENTIFIER, [item.args[0] for item in lookup.call_args_list])
