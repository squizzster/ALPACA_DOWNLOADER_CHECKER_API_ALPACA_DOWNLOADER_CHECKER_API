"""Legacy closed-source envelopes and bounded normal-update recovery."""

import copy
import json
import unittest
from contextlib import ExitStack
from unittest.mock import patch

import test_alpaca_public_common_pipeline as common_fixture
from test_alpaca_closed_departure_compatibility import prior_decision, prior_policy
from test_alpaca_lineage_build import informative_bars
from test_alpaca_universe_update import CatalogueClient

from alpaca_end_of_day import DataLibrary, standalone
from libs.alpaca_end_of_day_security_correlation import correlation, public_common_continuation
from libs.alpaca_lineage_build import (
    assignment_handoff,
    closed_source_recovery,
    pipeline,
    pricing,
    storage,
    universe_update,
)
from libs.alpaca_lineage_build.check_summary import rebuild
from libs.alpaca_lineage_build.continuation import validate_handoff
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_pricing_download import check_summary as pricing_summary
from libs.alpaca_pricing_download import update_checks
from libs.alpaca_pricing_download.contract import timestamp
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_serving_catalogue import work as serving_work
from libs.alpaca_serving_catalogue.producer import maintain


class ClosedSourceRecoveryTests(unittest.TestCase):
    def setUp(self):
        common_fixture.PublicCommonPipelineTests.setUp(self)

    def _daily_inputs(self):
        self.registry = self.store.parent / "ticker.csv"
        self.registry.write_text("BUSINESS,SPAC\n")
        # Install an already completed generic component result. No authored
        # facts, scoped applications or legacy JSON import enter fresh inference.
        completed = common_fixture.install_completed_common(self)
        self.assertEqual(0, completed["failed"], completed)
        self.assertEqual(0, maintain(self.store)["failed"])
        # These two completed sources are the entire synthetic universe. Lend
        # its completed scheduling boundary without an unrelated fallback scan.
        checks = update_checks.begin_checks(self.store)
        update_checks.complete_checks(self.store, checks["epoch"])
        rebuild(self.store)
        universe_update.rebuild_universe(self.store)
        self.identity = CatalogueClient([self.assets["SPAC"], self.assets["BUSINESS"]], exact=self.assets)
        self.client = BarsClient("fake", "fake")
        rows = {
            "SPAC": informative_bars("2024-01-02", "2024-01-05"),
            "BUSINESS": informative_bars("2024-01-05", "2024-01-09"),
        }

        def response(params):
            return {
                "bars": {
                    name: [
                        row
                        for row in rows[name]
                        if timestamp(params["start"]) <= timestamp(row["t"]) <= timestamp(params["end"])
                    ]
                    for name in params["symbols"].split(",")
                }
            }

        self.enterContext(patch.object(self.client, "_get", side_effect=response))

    def _daily(self):
        return universe_update.run_update(
            self.store,
            "2024-01-09",
            pricing_client=self.client,
            identity_client=self.identity,
            registry=self.registry,
        )

    def _receipt(self, symbol="SPAC"):
        return storage.read_receipt(self.root, symbol, storage.read_index(self.root))

    def _forbid_replay(self, *, invalidation=False):
        stack = ExitStack()
        for module, name in (
            (pipeline, "load_pricing"),
            (pricing, "load_published_boundary"),
            (pricing, "load_published_views"),
            (public_common_continuation, "certify_group"),
            (public_common_continuation, "source_history"),
            (correlation, "correlate_download_rows"),
            (standalone, "_rows"),
        ):
            stack.enter_context(patch.object(module, name, side_effect=AssertionError("replayed " + name)))
        if invalidation:
            stack.enter_context(
                patch.object(pricing_summary, "invalidate", side_effect=AssertionError("pricing invalidated"))
            )
        return stack

    def _publish_defect(self, mutate=None):
        from libs.alpaca_lineage_build.correlation_policy import load_policy
        from libs.alpaca_lineage_build.correlation_policy_contract import RETAINED_V82_SEMANTIC_SHA256

        index = storage.read_index(self.root)
        before = storage.read_receipt(self.root, "SPAC", index)
        peer = storage.read_receipt(self.root, "BUSINESS", index)
        broken = copy.deepcopy(before)
        # This recovery owns the known v82 lift omission. A new producer's
        # missing required proof must not be accepted as that legacy defect.
        broken["versions"] = {
            **broken["versions"],
            "correlation": 82,
            "correlation_policy_sha256": RETAINED_V82_SEMANTIC_SHA256,
        }
        broken["correlation_policy"] = load_policy(
            "resources/alpaca_security_correlation_v82_legacy_policy.yaml"
        ).provenance()
        for field in ("current_source_ownership", "sourced_rename_ownership", "closed_source_handoff_version"):
            broken.pop(field, None)
        if mutate:
            mutate(broken)
        frozen_peer = {
            **peer,
            "versions": broken["versions"],
            "correlation_policy": broken["correlation_policy"],
        }
        with storage.writer_lock(self.store), storage.pricing_locks(self.store, {"SPAC", "BUSINESS"}):
            storage.publish(
                self.store,
                self.root,
                index,
                {"SPAC": broken, "BUSINESS": frozen_peer},
                {"SPAC": {}, "BUSINESS": {}},
                {},
                previous={"SPAC": before, "BUSINESS": peer},
            )
        with self.assertRaises(LineageError):
            validate_handoff(broken)
        return broken

    def _recover(self):
        with storage.writer_lock(self.store):
            return closed_source_recovery.recover(
                self.store,
                self.root,
                storage.read_index(self.root),
                serving_work.read(self.store),
            )

    def test_legacy_closed_source_lift_emits_null_envelopes_and_serves(self):
        index = storage.read_index(self.root)
        receipt = storage.read_receipt(self.root, "SPAC", index)
        frozen = prior_decision(assets=receipt["asset_ids"])
        frozen.pop("source_rows")
        frozen.pop("unresolved_rows")
        receipt.update(frozen)
        receipt["schema_version"] = 34
        for field in (
            "current_source_ownership",
            "sourced_rename_ownership",
            "sourced_rename_membership",
            "closed_source_handoff_version",
        ):
            receipt.pop(field, None)
        storage._install(
            self.root,
            {
                "schema_version": 34,
                "correlation_policy": prior_policy(),
                "retire": [],
                "documents": {"SPAC/lineage.manifest.json": receipt},
                "index_updates": {
                    "SPAC": {**index["symbols"]["SPAC"], **frozen, "sha256": storage.document_digest(receipt)}
                },
            },
        )
        self.assertEqual(0, maintain(self.store)["failed"])
        self._daily_inputs()
        with patch.object(pipeline, "load_pricing", side_effect=AssertionError("legacy prices reopened")):
            result = self._daily()
        self.assertTrue(result["complete"], result)
        current = self._receipt()
        self.assertEqual(37, current["schema_version"])
        self.assertEqual(1, current["closed_source_handoff_version"])
        self.assertIsNone(current["current_source_ownership"])
        self.assertIsNone(current["sourced_rename_ownership"])
        validate_handoff(current)
        self.assertEqual(1, DataLibrary(store=self.store).online_count())

    def test_normal_update_recovers_published_envelope_without_replaying_lineage(self):
        self._daily_inputs()
        self.assertTrue(self._daily()["complete"])
        broken = self._publish_defect()
        accept = closed_source_recovery.accept_observation_choices
        validate = assignment_handoff._valid_run
        ingress = False

        def accept_once(receipt):
            nonlocal ingress
            ingress = True
            try:
                return accept(receipt)
            finally:
                ingress = False

        def only_at_ingress(run):
            self.assertTrue(ingress, "completed recovery choices validated again downstream")
            return validate(run)

        with (
            self._forbid_replay(),
            patch.object(closed_source_recovery, "accept_observation_choices", side_effect=accept_once),
            patch.object(assignment_handoff, "_valid_run", side_effect=only_at_ingress) as validation,
        ):
            result = self._daily()
        self.assertGreater(validation.call_count, 0)
        self.assertTrue(result["complete"], result)
        self.assertEqual(["SPAC"], result["stages"]["closed_source_recovery"]["symbols"])
        repaired = self._receipt()
        self.assertEqual(
            {
                **broken,
                "current_source_ownership": None,
                "sourced_rename_ownership": None,
                "closed_source_handoff_version": 1,
            },
            repaired,
        )
        self.assertEqual(1, DataLibrary(store=self.store).online_count())

    def test_malformed_choice_stops_envelope_recovery_before_publication(self):
        self._daily_inputs()
        self.assertTrue(self._daily()["complete"])

        def malformed(receipt):
            handoff = receipt["assignment_handoffs"]["current_month"]
            choices = json.loads(json.dumps(handoff["observation_choices"]))
            choices["partitions"][0]["runs"] = [{}]
            handoff["observation_choices"] = choices

        self._publish_defect(malformed)
        with (
            self._forbid_replay(),
            patch.object(storage, "publish", side_effect=AssertionError("malformed choices published")),
            self.assertRaisesRegex(LineageError, "observation-choice run"),
        ):
            self._recover()

    def test_recovery_journal_retry_preserves_assignments_and_donor_references(self):
        self._daily_inputs()
        self.assertTrue(self._daily()["complete"])
        broken = self._publish_defect()

        class Interrupted(BaseException):
            pass

        with self._forbid_replay(invalidation=True):
            with patch.object(storage, "_install", side_effect=Interrupted()), self.assertRaises(Interrupted):
                self._recover()
            self.assertTrue((self.root / ".pending.json").exists())
            self.assertTrue(storage.recover(self.store, self.root))
            self.assertFalse((self.root / ".pending.json").exists())
            with (
                patch.object(
                    closed_source_recovery,
                    "selected_document",
                    side_effect=AssertionError("completed peer reopened"),
                ),
                patch.object(
                    closed_source_recovery,
                    "accept_observation_choices",
                    side_effect=AssertionError("completed choices repeated"),
                ),
            ):
                self.assertEqual([], self._recover())
            self.assertEqual(0, maintain(self.store)["failed"])
        repaired = self._receipt()
        for field in (
            "boundaries",
            "pricing_boundaries",
            "membership_only_rebind",
            "source_sha256",
            "identity_memberships",
        ):
            self.assertEqual(broken[field], repaired[field], field)

    def test_malformed_present_certificate_is_not_repaired(self):
        self._daily_inputs()
        self.assertTrue(self._daily()["complete"])
        self._publish_defect(lambda receipt: receipt.update(current_source_ownership={"version": 1}))
        with self._forbid_replay(), self.assertRaises(LineageError):
            self._recover()
        self.assertFalse((self.root / ".pending.json").exists())

    def test_compact_selection_opens_only_unknown_legacy_pending_groups(self):
        def entry(name, correlation=82, projection=None):
            return {
                "sha256": name + "-generation",
                "versions": {"lineage": 37, "correlation": correlation},
                "group_members": [name],
                "projection": projection or {},
            }

        index = {
            "symbols": {
                "LEGACY": entry("LEGACY"),
                "MODERN": entry("MODERN", correlation=91),
                "COMPLETE": entry("COMPLETE", projection={"source_ownership_envelopes_complete": True}),
                "UNSELECTED": entry("UNSELECTED"),
            }
        }
        pending = {name: index["symbols"][name]["sha256"] for name in ("LEGACY", "MODERN", "COMPLETE")}
        with (
            patch.object(closed_source_recovery, "_recover_group", return_value=[]) as recover,
            patch("pathlib.Path.open", side_effect=AssertionError("selection touched storage")),
        ):
            self.assertEqual([], closed_source_recovery.recover(self.store, self.root, index, pending))
        self.assertEqual(1, recover.call_count)
        self.assertEqual({"LEGACY"}, recover.call_args.args[-1])

    def test_changed_pending_generation_and_missing_group_fail_before_storage(self):
        entry = {
            "sha256": "current",
            "versions": {"lineage": 37, "correlation": 82},
            "group_members": ["LEGACY"],
            "projection": {},
        }
        for change in ("generation", "membership"):
            value = copy.deepcopy(entry)
            pending = {"LEGACY": "changed" if change == "generation" else "current"}
            if change == "membership":
                value.pop("group_members")
            with (
                self.subTest(change=change),
                patch("pathlib.Path.open", side_effect=AssertionError("invalid selection touched storage")),
                patch.object(
                    closed_source_recovery,
                    "_recover_group",
                    side_effect=AssertionError("invalid group recovered"),
                ),
                self.assertRaises(LineageError),
            ):
                closed_source_recovery.recover(self.store, self.root, {"symbols": {"LEGACY": value}}, pending)

    def test_missing_proof_on_newly_mapped_source_is_not_repaired(self):
        self._daily_inputs()
        self.assertTrue(self._daily()["complete"])
        self._publish_defect(
            lambda receipt: receipt.update(
                public_common_continuation_ownership={
                    "version": 1,
                    "symbol": "SPAC",
                    "source_fact_id": "public-holder-transition",
                    "route": [],
                    "first": "2024-01-02",
                    "last": "2024-01-04",
                }
            )
        )
        with self._forbid_replay(), self.assertRaisesRegex(LineageError, "outside legacy-lift recovery"):
            self._recover()
        self.assertFalse((self.root / ".pending.json").exists())
