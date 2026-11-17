"""Legacy mapping adoption constructs dates without reopening accepted decisions."""

import json
import sqlite3
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_numerical_mapping import fixture
from test_alpaca_pricing_download import FakeClient, bar

from libs.alpaca_lineage_build import numerical_mapping, numerical_mapping_acquisition, pricing
from libs.alpaca_lineage_build.contract import RecoveryRequired
from libs.alpaca_lineage_build.recovery_handoffs import _source_selector
from libs.alpaca_pricing_download import download, handoff
from libs.alpaca_serving_catalogue import session_projection
from libs.alpaca_serving_catalogue.contract import CatalogueError


class LegacyProjectionTests(TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = Path(self.directory.name)
        self.days = ("2020-01-02", "2020-01-03", "2020-01-06")
        result = download.download_pricing(
            self.store, "ORBIT", "2026-01-01", client=FakeClient([bar(day) for day in self.days])
        )
        self.root = self.store / "end_of_day/pricing/ORBIT"
        self.source = download.load_source(self.root, "ORBIT")
        self.selections = {
            "ORBIT": {
                "source": self.source,
                "generation": result["source_sha256"],
                "selector": _source_selector(self.store, "ORBIT"),
            }
        }
        self.record = {
            "public_id": "owner",
            "first_session": self.days[0],
            "last_session": self.days[-1],
            "coverage": {"session_count": 2},
        }
        self.checkpoint = {
            "history_validation": {
                "inputs": {
                    "ORBIT": {
                        "pricing_generation": result["source_sha256"],
                        "views": {
                            "raw": {
                                "rows": 3,
                                "ownership": [
                                    {"rows": 1, "owner": ["owner", ["ORBIT"]]},
                                    {"rows": 1, "owner": [None, []]},
                                    {"rows": 1, "owner": ["owner", ["ORBIT"]]},
                                ],
                            }
                        },
                    }
                }
            }
        }

    def test_raw_positions_supply_only_owned_dates_and_lend_rows_and_manifest_to_lineage(self):
        partitions = {}
        with patch.object(
            handoff, "load_published_views", side_effect=AssertionError("historical views audited")
        ):
            dates = session_projection.legacy_dates(
                self.store, self.checkpoint, self.record, self.selections, partitions
            )
        self.assertEqual((self.days[0], self.days[-1]), dates)
        key = str(self.root), "completed_years", self.source["boundaries"]["completed_years"]["sha256"]
        held_rows, held_document = partitions[("selected_raw", *key)]
        cold_manifest = self.root / self.source["boundaries"]["completed_years"]["manifest"]
        original_read = handoff.read_document

        def selected_document(path, **options):
            self.assertNotEqual(cold_manifest, path, "completed raw manifest reread")
            return original_read(path, **options)

        with (
            patch.object(handoff, "read_document", side_effect=selected_document),
            patch.object(
                handoff, "read_artifact", side_effect=AssertionError("completed raw/alias payload reread")
            ),
        ):
            view = pricing.load_pricing(
                self.store,
                "ORBIT",
                source=self.source,
                source_sha256=self.selections["ORBIT"]["generation"],
                partitions=partitions,
            )
        self.assertIs(view.views["raw"][0], held_rows[0])
        self.assertNotIn(("selected_raw", *key), partitions)
        self.assertIs(partitions[key]["all"], held_rows)
        self.assertEqual("ORBIT", held_document["symbol"])

    def test_changed_pricing_selector_rejects_before_raw_payloads(self):
        with (
            patch.object(session_projection, "_source_selector", return_value=("changed",)),
            patch.object(session_projection, "_rows", side_effect=AssertionError("stale source rows read")),
            self.assertRaisesRegex(CatalogueError, "selector changed"),
        ):
            session_projection.legacy_dates(self.store, self.checkpoint, self.record, self.selections, {})

    def test_declared_missing_dates_reject_without_legacy_assembly(self):
        lineage = {"ORBIT": "accepted-receipt"}
        generations = {"ORBIT": self.selections["ORBIT"]["generation"]}
        catalogue = {
            "state": {"generation": "selected-catalogue"},
            "members": {"ORBIT": "group"},
            "groups": {"group": {"references": {"lineage": lineage, "pricing": generations}}},
        }
        context = SimpleNamespace(
            store=self.store,
            index={"symbols": {"ORBIT": {"sha256": lineage["ORBIT"]}}},
            select_serving=lambda: catalogue,
        )
        record = {**self.record, "session_projection_version": 1}
        checkpoint = {"history_validation": {"prefix_records": [record]}}
        from libs.alpaca_serving_catalogue import storage

        with (
            patch.object(storage, "read_groups", return_value={"group": checkpoint}),
            patch.object(
                session_projection, "legacy_dates", side_effect=AssertionError("declared capability rebuilt")
            ),
            self.assertRaises(RecoveryRequired),
        ):
            numerical_mapping.checkpoint_sessions(context, generations, "owner", {}, partitions={})

    def test_legacy_checkpoint_is_projected_once_and_keeps_accepted_outcomes(self):
        from libs.alpaca_serving_catalogue import storage

        lineage = {"ORBIT": "accepted-receipt"}
        generations = {"ORBIT": self.selections["ORBIT"]["generation"]}
        catalogue = {
            "state": {"generation": "selected-catalogue"},
            "members": {"ORBIT": "group"},
            "groups": {"group": {"references": {"lineage": lineage, "pricing": generations}}},
        }
        context = SimpleNamespace(
            store=self.store,
            index={"symbols": {"ORBIT": {"sha256": lineage["ORBIT"]}}},
            select_serving=lambda: catalogue,
        )
        checkpoint = deepcopy(self.checkpoint)
        checkpoint["history_validation"]["prefix_records"] = [self.record]
        original = deepcopy(checkpoint)
        cache, partitions = {"source_selections": self.selections}, {}
        with patch.object(storage, "read_groups", return_value={"group": checkpoint}) as read:
            first = numerical_mapping.checkpoint_sessions(
                context, generations, "owner", cache, partitions=partitions
            )
            with patch.object(
                session_projection, "legacy_dates", side_effect=AssertionError("legacy projection repeated")
            ):
                second = numerical_mapping.checkpoint_sessions(
                    context, generations, "owner", cache, partitions=partitions
                )
        self.assertIs(first, second)
        read.assert_called_once()
        self.assertEqual(original, checkpoint)


class LegacyMappingIntegrationTests(TestCase):
    def test_projected_dates_survive_interruption_before_acquisition_construction(self):
        from libs.alpaca_pricing_download import mapping
        from libs.alpaca_pricing_download.responses import CompletedBars

        with TemporaryDirectory() as directory:
            store = Path(directory)
            request = numerical_mapping.select(store, *fixture(), selected=True)["NOVA27"]
            calls = []

            def witness(symbol, adjustment, start, end, *, asof):
                calls.append("witness")
                row = (start.isoformat(), *(["1"] * 7))
                return CompletedBars(symbol, adjustment, start, end, asof, (row,))

            options = dict(
                store=store,
                root=store / "end_of_day/lineage",
                symbol="NOVA27",
                request=request,
                client=SimpleNamespace(fetch=witness),
                context=None,
                required_sessions=None,
            )
            with patch.object(
                mapping, "MappingAcquisition", side_effect=RuntimeError("constructor interrupted")
            ):
                with self.assertRaisesRegex(RuntimeError, "constructor interrupted"):
                    numerical_mapping_acquisition.execute(
                        **options,
                        cache={},
                        download=lambda *a, **k: self.fail("download before handoff"),
                        grid_provider=lambda: ("2016-01-04",),
                    )
            options["client"] = SimpleNamespace(fetch=lambda *a, **k: self.fail("witness repeated"))
            result = numerical_mapping_acquisition.execute(
                **options,
                cache={},
                download=lambda *a, **k: {"source_sha256": "completed-generation", "changed": True},
                grid_provider=lambda: self.fail("completed date projection repeated"),
            )
            self.assertEqual(["witness"], calls)
            self.assertEqual("mapped_history_acquired", result["mapping_assessment"]["outcome"])

    def test_real_group_adopts_unmarked_legacy_checkpoint_without_repeating_witness_on_retry(self):
        from test_alpaca_lineage_build import IdentityClient, LineageBuildTests, asset

        from libs.alpaca_lineage_build import storage, universe_state
        from libs.alpaca_lineage_build.work_contract import WorkPolicy
        from libs.alpaca_serving_catalogue import producer
        from libs.alpaca_serving_catalogue import storage as serving_storage

        helper = LineageBuildTests("runTest")
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        days = ("2020-01-03", "2020-01-06", "2020-01-07")
        helper.download("FUTURE.A", end="2021-01-01", bars=[bar(days[0])])
        helper.download("NOVA27", end="2021-01-01", bars=[bar(day) for day in days[1:]])
        continuing = {**asset("NOVA27", "provider-uuid"), "status": "inactive"}
        actions = fixture()[3]["NOVA27"]["corporate_actions"]
        actions[1]["process_date"] = actions[1]["ex_date"]
        identities = IdentityClient({"NOVA27": continuing, "000000001": continuing}, actions)
        helper.run_build(["NOVA27"], client=identities)
        producer.maintain(helper.store, bootstrap=True)
        with sqlite3.connect(serving_storage.path(helper.store)) as connection:
            key, body = connection.execute("SELECT group_key, body FROM groups").fetchone()
            checkpoint = json.loads(body)
            for record in checkpoint["history_validation"]["prefix_records"]:
                record.pop("session_dates", None)
                record.pop("session_projection_version", None)
            connection.execute("UPDATE groups SET body=? WHERE group_key=?", (json.dumps(checkpoint), key))
        universe_state.initialize(helper.store)
        policy = WorkPolicy(helper.store, storage.read_index(helper.root), {}, [], "2021-01-01")
        client = FakeClient([bar(day) for day in days])
        result = helper.run_build(
            ["NOVA27"], client=identities, pricing_client=client, refresh=True, work_policy=policy
        )
        self.assertEqual(0, result["failed"], result)
        decisions = helper.receipt("NOVA27")["numerical_mapping_assessments"]["decisions"]
        self.assertEqual("mapped_history_acquired", decisions[-1]["outcome"])
        calls = len(client.calls)
        result = helper.run_build(["NOVA27"], client=identities, pricing_client=client, work_policy=policy)
        self.assertEqual(0, result["failed"], result)
        self.assertEqual(calls, len(client.calls))
