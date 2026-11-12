"""Count-only latest corrections carry accepted ownership, not old price values."""

import copy
import json
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_lineage_build import correction_binding, storage, transform
from libs.alpaca_lineage_build.assignment_handoff import AcceptedObservationChoices
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_pipeline.codec import Decoder, Encoder
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


def fixture(rule="asof_selected_uuid_lineage_interval"):
    store, name, peer = "/synthetic", "CURRENT", "PEER"
    old_generation, generation = "a" * 64, "b" * 64
    source_generations = {name: old_generation, peer: "c" * 64}
    receipt_generations = {name: "d" * 64, peer: "e" * 64}
    session, end, day, pid = "2026-11-11T05:00:00Z", "2026-11-12T00:00:00Z", "2026-11-11", "f" * 64
    before = (session, "10", "11", "9", "10", "100", "10", "10.1")
    after = (*before[:5], "101", "11", before[7])
    request = {
        "symbol": name,
        "start": "2016-01-01T00:00:00Z",
        "end_exclusive": end,
        "asof": "2026-11-12",
        "feed": "sip",
        "currency": "USD",
        "timeframe": "1Day",
        "history_start": "2016-01-01T00:00:00Z",
    }
    run = {
        "from": day,
        "to": day,
        "owner": pid,
        "state": "selected",
        "historical_symbol": name,
        "tickers": [name],
    }
    choices = AcceptedObservationChoices(dict.fromkeys(ADJUSTMENTS, 0), [{"rows": 1, "runs": [run]}], "1" * 64)
    old = {
        "symbol": name,
        "source_sha256": old_generation,
        "input_sources": source_generations,
        "pricing_request": request,
        "pricing_boundaries": {"current_month": {"sha256": "2" * 64}},
        "anchor_month": "2026-11",
        "processed_through": end,
        "row_partition_complete": True,
        "unresolved_rows": 0,
        "source_rows": 8,
        "correlated_rows": 8,
        "identity_memberships": [{"security_id": pid}],
        "assignment_summary": {
            view: {"rows": 1, "last_timestamp": session, "last_session": day} for view in ADJUSTMENTS
        },
        "assignment_handoffs": {"current_month": {"observation_choices": choices}},
        "boundaries": {"current_month": {"manifest": "current.json", "sha256": "1" * 64}},
        "serving_ownership": {"accepted": "ownership"},
        "observation_routes": {pid: [session, {}]},
        "evidence": {"accepted": "metadata"},
        "history_correction_id": None,
    }
    other = {
        **old,
        "symbol": peer,
        "source_sha256": source_generations[peer],
        "identity_memberships": [{"security_id": "0" * 64}],
    }
    source = {
        "symbol": name,
        "anchor_month": "2026-11",
        "history_start": request["history_start"],
        "latest_request": {k: v for k, v in request.items() if k != "history_start"},
        "boundaries": {"current_month": {"sha256": "3" * 64}},
        "history_correction_id": "correction",
        "latest_session_correction": {
            "session": session,
            "source_sha256": old_generation,
            "preserves_timestamps": True,
        },
    }
    segment = {
        "from": day,
        "to": day,
        "branch": "correlated",
        "observation_selection": "selected",
        "observation_tickers": [name],
        "correlation_rule": {"name": rule},
        "security_identity": {"security_id": pid},
    }
    assignment = {
        "receipt_generation": receipt_generations[name],
        "kind": "current_month",
        "generation": "1" * 64,
        "document": {
            "symbol": name,
            "anchor_month": "2026-11",
            "adjustments": [{"adjustment": view, "segments": [segment]} for view in ADJUSTMENTS],
        },
    }
    contract = {
        "store": store,
        "symbol": name,
        "scope": "latest_session",
        "source_sha256": old_generation,
        "asof": request["asof"],
        "end_exclusive": end,
        "selection": {
            "generation": old_generation,
            "comparison": {"decision": "latest_session"},
            "points": [[view, [before]] for view in ADJUSTMENTS],
        },
    }
    return store, {
        "previous": {name: old, peer: other},
        "receipt_generations": receipt_generations,
        "assignments": {name: assignment},
        "sources": {name: {"source": source, "generation": generation}},
        "obligations": {name: {"correction_id": "correction", "pricing_correction": contract}},
        "partitions": {
            ("pricing_partition", (name, "current_month", "3" * 64)): dict.fromkeys(ADJUSTMENTS, (after,))
        },
    }


class CorrectionBindingTests(TestCase):
    def test_supported_rules_preserve_exact_decisions_without_storage_or_rule_calls(self):
        for rule in ("asof_selected_uuid_lineage_interval", "stable_continuous_ticker_identity_set"):
            with self.subTest(rule=rule):
                store, inputs = fixture(rule)
                before = copy.deepcopy(inputs)
                with (
                    patch.object(storage, "read_receipt", side_effect=AssertionError("receipt read")),
                    patch.object(storage, "read_boundaries", side_effect=AssertionError("assignment reread")),
                    patch.object(transform, "correlate", side_effect=AssertionError("rule replay")),
                    patch(
                        "libs.alpaca_lineage_build.observations.select_observations",
                        side_effect=AssertionError("numerical replay"),
                    ),
                ):
                    result = correction_binding.prepare(store, **inputs)
                old, current = inputs["previous"]["CURRENT"], result["receipts"]["CURRENT"]
                self.assertEqual(inputs, before)
                self.assertIs(current["boundaries"], old["boundaries"])
                self.assertIs(current["assignment_handoffs"], old["assignment_handoffs"])
                self.assertIs(current["serving_ownership"], old["serving_ownership"])
                self.assertEqual(current["source_sha256"], "b" * 64)
                self.assertEqual(result["receipts"]["PEER"]["input_sources"]["CURRENT"], "b" * 64)
                self.assertEqual(result["boundaries"], {"CURRENT": {}, "PEER": {}})
                self.assertEqual(result["serving_invalidated_sessions"], {"CURRENT": ["2026-11-11"]})
                self.assertNotIn("numerical_source_revision", current)

    def test_actual_changed_price_inertness_timestamp_and_mapping_fail_closed(self):
        for changed in (
            "price",
            "vwap",
            "zero_volume",
            "zero_trades",
            "time",
            "asof",
            "metadata",
            "cause",
            "scope",
            "rule",
        ):
            store, inputs = fixture()
            source = inputs["sources"]["CURRENT"]["source"]
            views = inputs["partitions"][("pricing_partition", ("CURRENT", "current_month", "3" * 64))]
            row = list(views["raw"][-1])
            if changed in ("price", "vwap", "zero_volume", "zero_trades", "time"):
                position = {"price": 1, "vwap": 7, "zero_volume": 5, "zero_trades": 6, "time": 0}[changed]
                row[position] = (
                    "2026-11-10T05:00:00Z" if changed == "time" else "0" if changed.startswith("zero") else "12"
                )
                views["raw"] = (tuple(row),)
            elif changed == "asof":
                source["latest_request"]["asof"] = "2026-11-11"
            elif changed == "metadata":
                source["acquisition_evidence_sha256"] = "changed"
            elif changed == "cause":
                source["latest_session_correction"]["source_sha256"] = "changed"
            elif changed == "scope":
                inputs["obligations"]["CURRENT"]["pricing_correction"]["scope"] = "full_history"
            else:
                inputs["assignments"]["CURRENT"]["document"]["adjustments"][0]["segments"][0][
                    "correlation_rule"
                ]["name"] = "price_dependent_predecessor"
            with self.subTest(changed=changed), self.assertRaises(LineageError):
                correction_binding.prepare(store, **inputs)

    def test_competing_or_incompletely_selected_group_does_not_borrow_authority(self):
        for changed in ("competitor", "missing_peer", "receipt", "assignment", "missing_rows"):
            store, inputs = fixture()
            if changed == "competitor":
                inputs["previous"]["PEER"]["identity_memberships"] = inputs["previous"]["CURRENT"][
                    "identity_memberships"
                ]
            elif changed == "missing_peer":
                inputs["previous"].pop("PEER")
                inputs["receipt_generations"].pop("PEER")
            elif changed == "receipt":
                inputs["assignments"]["CURRENT"]["receipt_generation"] = "changed"
            elif changed == "assignment":
                inputs["assignments"]["CURRENT"]["generation"] = "changed"
            else:
                inputs["partitions"].clear()
            with self.subTest(changed=changed), self.assertRaises(LineageError):
                correction_binding.prepare(store, **inputs)

    def test_value_bound_capabilities_are_not_dropped_or_relabelled(self):
        for capability in (
            "numerical_source_revision",
            "selected_observation_bundles",
            "numerical_source_equivalence",
            "dated_market_projection",
            "membership_only_rebind",
        ):
            store, inputs = fixture()
            marker = {"accepted": "old values", "pricing_generation": "a" * 64}
            inputs["previous"]["CURRENT"][capability] = marker
            with (
                self.subTest(capability=capability),
                self.assertRaisesRegex(LineageError, "value-bound capability"),
            ):
                correction_binding.prepare(store, **inputs)
            self.assertIs(inputs["previous"]["CURRENT"][capability], marker)

    def test_completed_plan_crosses_json_and_only_named_applicability_is_checked(self):
        store, inputs = fixture()
        plan = correction_binding.prepare(store, **inputs)
        encoder = Encoder()
        value = encoder.encode(plan)
        objects, value = json.loads(json.dumps([encoder.objects, value]))
        restored = Decoder(objects).decode(value)
        with (
            patch.object(
                correction_binding, "prepare", side_effect=AssertionError("completed preparation repeated")
            ),
            patch.object(
                correction_binding, "_only_count_revision", side_effect=AssertionError("invariance repeated")
            ),
        ):
            correction_binding.assert_current(
                restored,
                store,
                receipt_generations=restored["receipt_generations"],
                pricing_generations=restored["pricing_generations"],
            )
            for kind in ("receipt", "pricing"):
                receipt_generations = dict(restored["receipt_generations"])
                pricing_generations = dict(restored["pricing_generations"])
                (receipt_generations if kind == "receipt" else pricing_generations)["CURRENT"] = "changed"
                with self.subTest(kind=kind), self.assertRaises(LineageError):
                    correction_binding.assert_current(
                        restored,
                        store,
                        receipt_generations=receipt_generations,
                        pricing_generations=pricing_generations,
                    )
