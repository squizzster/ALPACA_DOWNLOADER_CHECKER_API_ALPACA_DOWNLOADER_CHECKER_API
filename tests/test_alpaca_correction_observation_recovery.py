"""New provider evidence may supersede stale values, never completed scope."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from test_alpaca_pricing_download import FakeClient, bar, single_session_request

from libs.alpaca_lineage_build import universe_state
from libs.alpaca_lineage_build.work_contract import WorkPolicy
from libs.alpaca_pricing_download import download, handoff, observation_recovery, refresh, storage
from libs.alpaca_pricing_download.contract import timestamp, transform
from libs.alpaca_pricing_download.endpoints import compare_history
from libs.alpaca_pricing_download.http import BarsClient


class CorrectionObservationRecoveryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.root = self.store / "end_of_day/pricing/AAA"
        self.end = "2026-01-13"
        self.enterContext(
            patch.object(download, "utc_now", return_value=datetime(2030, 1, 1, tzinfo=timezone.utc))
        )
        self.old = [bar("2026-01-05", "10"), bar("2026-01-12", "10")]
        self.output = [bar("2026-01-05", "12"), self.old[-1]]
        self.confirmation = self.output[:1]
        download.download_pricing(self.store, "AAA", self.end, client=FakeClient(self.old))
        self.before = (self.root / "source.manifest.json").read_bytes()
        start, end = timestamp("2016-01-01T00:00:00Z"), timestamp(self.end + "T00:00:00Z")
        previous = transform(self.old, start, end)
        witness = transform([bar("2026-01-05", "11"), self.old[-1]], start, end)
        decision = compare_history(previous[0], previous, witness)
        self.prepared = refresh.prepare_refresh(self.store, ["AAA"], comparisons={"AAA": decision})
        universe_state.initialize(self.store)
        self.work = WorkPolicy(self.store, {"symbols": {}}, {}, [], self.end)
        self.work.require_correction(self.prepared)
        self.pending = universe_state.read_state(self.store)["work"]["AAA"]
        self.client, self.calls = BarsClient("test", "test"), []
        self.enterContext(patch.object(self.client, "_get", side_effect=self.page))
        self.enterContext(
            patch.object(refresh, "fetch_history", side_effect=AssertionError("completed comparison repeated"))
        )
        self.enterContext(
            patch.object(download, "fetch_history", side_effect=AssertionError("completed comparison repeated"))
        )
        self.enterContext(
            patch.object(download, "read_boundary", side_effect=AssertionError("retained payload read"))
        )
        self.enterContext(
            patch.object(handoff, "read_artifact", side_effect=AssertionError("retained payload decoded"))
        )

    def page(self, params):
        self.calls.append(dict(params))
        first, last = timestamp(params["start"]), timestamp(params["end"])
        rows = (
            self.confirmation
            if params["adjustment"] == "all" and single_session_request(params)
            else self.output
        )
        return {"bars": {"AAA": [row for row in rows if first <= timestamp(row["t"]) <= last]}}

    def prepared_recovery(self):
        return [
            refresh.check_retained_source(self.store, "AAA", self.end, client=self.client, pending=self.pending)
        ]

    def exact_calls(self):
        return [call for call in self.calls if call["adjustment"] == "all" and single_session_request(call)]

    def test_newer_same_mapping_observation_publishes_with_original_scope_and_evidence(self):
        result = refresh.replace_prepared(self.store, self.prepared_recovery(), client=self.client)
        self.assertEqual(0, result["failed"], result)
        self.assertEqual(1, len(self.exact_calls()))
        self.assertEqual(self.end, self.exact_calls()[0]["asof"])
        self.assertEqual(4, len(self.calls))
        source = storage.load_source(self.root, "AAA")
        evidence = source["history_correction_observation_resolution"]
        self.assertEqual("11", evidence["resolutions"][0]["previous_observation"][4])
        self.assertEqual("12", evidence["resolutions"][0]["current_observation"][4])
        self.assertEqual(self.pending["correction_id"], source["history_correction_id"])
        self.assertEqual("full_history", source["history_correction_scope"])
        self.assertEqual(self.pending, universe_state.read_state(self.store)["work"]["AAA"])
        self.calls.clear()
        with patch.object(
            self.client, "_get", side_effect=AssertionError("completed correction requested again")
        ):
            recovered = self.prepared_recovery()
        self.assertTrue(recovered[0]["matched"])

    def test_durable_new_witness_survives_interruption_before_price_publication(self):
        class Interrupted(BaseException):
            pass

        commit = observation_recovery._write

        def interrupt(root, receipt):
            commit(root, receipt)
            raise Interrupted

        with patch.object(observation_recovery, "_write", side_effect=interrupt):
            with self.assertRaises(Interrupted):
                refresh.replace_prepared(self.store, self.prepared_recovery(), client=self.client)
        self.assertEqual(self.before, (self.root / "source.manifest.json").read_bytes())
        self.assertEqual(1, len(self.exact_calls()))
        saved = json.loads((self.root / "correction.observations.json").read_bytes())
        self.assertEqual("12", saved["resolutions"][0]["current_observation"][4])
        self.calls.clear()
        fetch = self.page

        def no_repeat(params):
            if params["adjustment"] == "all" and single_session_request(params):
                self.fail("completed newer witness requested again")
            return fetch(params)

        with patch.object(self.client, "_get", side_effect=no_repeat):
            result = refresh.replace_prepared(self.store, self.prepared_recovery(), client=self.client)
        self.assertEqual(0, result["failed"], result)
        self.assertEqual([], self.exact_calls())
        self.assertEqual(
            saved, storage.load_source(self.root, "AAA")["history_correction_observation_resolution"]
        )

    def test_fresh_old_third_missing_and_ambiguous_responses_leave_publication_unchanged(self):
        for rows in ([bar("2026-01-05", "11")], [bar("2026-01-05", "13")], [], self.output[:1] * 2):
            with self.subTest(rows=rows):
                self.confirmation = rows
                self.calls.clear()
                result = refresh.replace_prepared(self.store, self.prepared_recovery(), client=self.client)
                self.assertEqual(1, result["failed"], result)
                self.assertEqual(self.before, (self.root / "source.manifest.json").read_bytes())
                self.assertFalse((self.root / "correction.observations.json").exists())
                self.assertEqual(1, len(self.exact_calls()))

    def test_changed_resolution_binding_does_not_approve_new_request(self):
        class Interrupted(BaseException):
            pass

        commit = observation_recovery._write

        def interrupt(root, receipt):
            commit(root, receipt)
            raise Interrupted

        with patch.object(observation_recovery, "_write", side_effect=interrupt):
            with self.assertRaises(Interrupted):
                refresh.replace_prepared(self.store, self.prepared_recovery(), client=self.client)
        saved = json.loads((self.root / "correction.observations.json").read_bytes())
        for changed in ("source_sha256", "correction_id", "request"):
            with self.subTest(changed=changed):
                binding = {**saved["binding"], changed: "different"}
                self.assertEqual([], observation_recovery._read(self.root, binding)["resolutions"])
