"""Selected observation transport preserves completed physical aliases."""

import json
from copy import deepcopy
from unittest import TestCase

from test_alpaca_end_of_day_api import valid_bars

from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.source_choice_handoff import accept, produce
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, FIELDS


class SourceChoiceHandoffTests(TestCase):
    def setUp(self):
        row = valid_bars("2026-10-02", "2026-10-03")[0]
        self.row = tuple(str(row[field]) for field in FIELDS)
        self.revision = {
            "id": "a" * 64,
            "sessions": {"owner": ["2026-10-02"]},
            "selected_sources": {"owner": {"2026-10-02": "SOURCE"}},
        }

    def produced(self):
        return produce(
            {"owner": {"2026-10-02": {view: {"source": "SOURCE", "row": self.row} for view in ADJUSTMENTS}}},
            self.revision,
        )

    def test_round_trip_keeps_alias_identity_without_comparing_values_again(self):
        value = self.produced()
        self.assertIs(value, deepcopy(value))
        self.assertIs(value, accept(value, self.revision))
        self.assertEqual(1, len(value["identities"]["owner"]["2026-10-02"]["partitions"]))
        incoming = accept(json.loads(json.dumps(value)), self.revision)
        views = incoming.observations["owner"]["2026-10-02"]
        self.assertTrue(all(entry["row"] is views["raw"]["row"] for entry in views.values()))
        with self.assertRaises(TypeError):
            views["raw"]["source"] = "WRONG"

    def test_changed_source_slot_or_revision_fails_without_reconstruction(self):
        for field in ("source", "slot", "revision"):
            value = json.loads(json.dumps(self.produced()))
            if field == "source":
                value["identities"]["owner"]["2026-10-02"]["partitions"][0]["source"] = "WRONG"
            elif field == "slot":
                value["identities"]["owner"]["2026-10-02"]["views"]["all"] = 9
            else:
                value["revision_id"] = "b" * 64
            with self.subTest(field=field), self.assertRaises(LineageError):
                accept(value, self.revision)
