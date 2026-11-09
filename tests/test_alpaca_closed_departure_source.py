"""Current-source ownership after a closed departure, across the complete group."""

import copy
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset, informative_bars
from test_alpaca_name_change_evidence import bars, inputs_for, raw_segments, rename
from test_alpaca_pricing_download import FakeClient

from libs.alpaca_end_of_day_security_correlation import closed_departure, correlation, row_correlation
from libs.alpaca_end_of_day_security_correlation.correlation_version import validate_correlation_change_log
from libs.alpaca_lineage_build import pipeline, policy_engine, pricing, storage
from libs.alpaca_lineage_build.continuation import validate_handoff
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy
from libs.alpaca_lineage_build.evidence import EvidenceCapture
from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_pricing_download.download import download_pricing
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage


def case():
    evidence = rename(same_uuid=True, same_cusip=True, old="REUS", new="KEEP", when="2024-04-08")
    old = {**asset("KEEP", "old-id"), "lookup_cusips": ["old-cusip"]}
    current = {**asset("REUS", "current-id"), "lookup_cusips": []}
    evidence.update(
        identities=[old, current],
        asset_lookups=[
            {
                "lookup_kind": kind,
                "lookup": value,
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "asset": item,
            }
            for kind, value, item in (
                ("symbol", "REUS", current),
                ("symbol", "KEEP", old),
                ("cusip", "old-cusip", old),
            )
        ],
        query={
            "input_symbol": "REUS",
            "corporate_actions_start": "2024-01-01",
            "corporate_actions_end": "2024-06-19",
            "corporate_actions_data_quality": "complete",
        },
        queried_symbols=["REUS", "KEEP"],
        unresolved_cusips=[],
    )
    histories = {
        "REUS": bars(("2024-06-12", "2024-06-13", "2024-06-14", "2024-06-17", "2024-06-18"), 100),
        "KEEP": bars(expected_market_session_coverage("2024-04-05", "2024-06-19").session_dates, 20),
    }
    inputs = inputs_for(histories, asofs=dict.fromkeys(histories, "2024-06-19"))
    for view in inputs.values():
        view.source["latest_request"]["end_exclusive"] = "2024-06-19"
        for download in view.downloads.values():
            download.manifest["request_to"] = "2024-06-19"
    return evidence, inputs


class ClosedDepartureSourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.policy = load_authorized_policy()
        validate_correlation_change_log()

    def evaluate(self, evidence, inputs, policy=None, *, store=Path("/unused"), **scope):
        with patch.object(Path, "open", side_effect=AssertionError("unexpected storage read")):
            return correlate(
                store, inputs, {name: evidence for name in inputs}, policy=policy or self.policy, **scope
            )

    def test_complete_current_source_resolves_without_current_cusip_or_action(self):
        evidence, inputs = case()
        with patch.object(
            row_correlation, "assess_current_source", wraps=closed_departure.assess_current_source
        ) as assess:
            reports = self.evaluate(evidence, inputs)
        self.assertEqual(2, assess.call_count)  # one result, including negative KEEP, per complete source
        self.assertEqual(40, reports["REUS"]["correlated"]["row_count"])
        self.assertEqual(0, reports["REUS"]["uncorrelated"]["row_count"])
        self.assertEqual("current-id", raw_segments(reports["REUS"])[0]["canonical_security"]["alpaca_asset_id"])
        certificate = reports["REUS"]["current_source_ownership"]
        self.assertEqual(("2024-06-12", "2024-06-18"), (certificate["first"], certificate["last"]))
        self.assertEqual(
            [],
            reports["REUS"]["uuid_lineage_edges"]
            if not reports["REUS"]["uuid_lineage_edges"]
            else [edge for edge in reports["REUS"]["uuid_lineage_edges"] if "current-id" in str(edge)],
        )
        previous = self.evaluate(
            evidence,
            inputs,
            replace(self.policy, implementation_correlation_version=79, implementation_lineage_version=34),
        )
        self.assertEqual(40, previous["REUS"]["uncorrelated"]["row_count"])
        self.assertEqual(previous["KEEP"]["correlated"], reports["KEEP"]["correlated"])
        self.assertEqual(previous["KEEP"]["uncorrelated"], reports["KEEP"]["uncorrelated"])

    def test_nearest_vetoes_leave_the_new_source_unassigned(self):
        def change_sources(inputs, key, value):
            for download in inputs["REUS"].downloads.values():
                download.manifest[key] = value

        def remove_row(inputs, adjustment=None):
            for name, download in inputs["REUS"].downloads.items():
                if adjustment is None or download.manifest["adjustment"] == adjustment:
                    download.rows.pop(1)
                    download.manifest["rows"] -= 1
                    next(item for item in inputs["REUS"].local["downloads"] if item["file"] == name)["rows"] -= 1

        mutations = {
            "historical mapping": lambda e, i: change_sources(i, "asof", "2024-04-05"),
            "disabled mapping": lambda e, i: change_sources(i, "asof", "-"),
            "cropped request": lambda e, i: change_sources(i, "request_from", "2024-06-12"),
            "missing session": lambda e, i: remove_row(i),
            "incomplete view": lambda e, i: remove_row(i, "all"),
            "partial source": lambda e, i: i["REUS"].local["downloads"][0].update(rows=6),
            "unqueried destination": lambda e, i: e.update(queried_symbols=["REUS"]),
            "partial actions": lambda e, i: e["query"].update(corporate_actions_data_quality="partial"),
            "unknown CUSIP": lambda e, i: e.update(unresolved_cusips=["unknown"]),
            "third occupant": lambda e, i: e["identities"].append(asset("REUS", "third-id")),
            "third successor occupant": lambda e, i: e["identities"].append(asset("KEEP", "third-id")),
            "missing complete counterpart": lambda e, i: i.pop("KEEP"),
            "historically mapped counterpart": lambda e, i: [
                download.manifest.update(asof="2024-04-05") for download in i["KEEP"].downloads.values()
            ],
            "duplicate current lookup": lambda e, i: e["asset_lookups"].append(
                {**e["asset_lookups"][0], "asset": {**e["asset_lookups"][0]["asset"], "status": "inactive"}}
            ),
            "failed exact lookup": lambda e, i: e["asset_lookups"][0].update(http_status=503),
            "inactive current": lambda e, i: e["identities"][-1].update(status="inactive"),
            "current not tradable": lambda e, i: e["identities"][-1].update(tradable=False),
            "third structural event": lambda e, i: e["corporate_actions"].append(
                {
                    "id": "unknown-event",
                    "action_type": "reorganization",
                    "process_date": "2024-06-13",
                    "recipients": [{"symbol": "REUS"}],
                }
            ),
            "continued old occupancy": lambda e, i: e["symbol_occupancy_evidence"].append(
                {
                    "symbol": "REUS",
                    "cusip": "old-cusip",
                    "asset_id": "old-id",
                    "evidence": [
                        {
                            "action_id": "late-dividend",
                            "action_type": "cash_dividend",
                            "process_date": "2024-06-13",
                        }
                    ],
                }
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                evidence, inputs = case()
                mutate(evidence, inputs)
                reports = self.evaluate(evidence, inputs)
                self.assertIsNone(reports["REUS"]["current_source_ownership"])
                self.assertFalse(
                    any(
                        segment.get("correlation_method") == closed_departure.METHOD
                        for segment in reports["REUS"]["correlated"]["segments"]
                    )
                )

    def test_exact_successor_copy_cannot_claim_current_identity(self):
        evidence, inputs = case()
        for name, download in inputs["REUS"].downloads.items():
            donor = {row["t"]: row for row in inputs["KEEP"].downloads[name].rows}
            download.rows[:] = [copy.deepcopy(donor[row["t"]]) for row in download.rows]
        report = self.evaluate(evidence, inputs)["REUS"]
        self.assertIsNone(report["current_source_ownership"])
        self.assertEqual(40, report["uncorrelated"]["row_count"])

    def test_completed_certificate_survives_peer_failure_and_phase_transition(self):
        evidence, inputs = case()
        from libs.alpaca_end_of_day_security_correlation.correlation import build_security_correlation_report
        from libs.alpaca_lineage_build.policy_engine import _correlation_cache, _register_sources

        cache = _correlation_cache(self.policy)
        _register_sources(cache, inputs)
        view = inputs["REUS"]
        kwargs = dict(
            authenticated_download_reader=view.reader,
            verified_session_exclusions_reader=lambda *_: {},
            authenticated_symbol_sessions=lambda _: None,
            cache=cache,
        )
        with patch.object(
            row_correlation, "assess_current_source", wraps=closed_departure.assess_current_source
        ) as assess:
            first = build_security_correlation_report(
                {"symbol": "REUS", "local": view.local, "lineage": evidence}, Path("/unused"), **kwargs
            )
            # A later peer failure must not erase this completed source authority.
            later = _correlation_cache(self.policy)
            _register_sources(later, inputs)
            later.inherit_continuity(cache)
            second = build_security_correlation_report(
                {"symbol": "REUS", "local": view.local, "lineage": evidence},
                Path("/unused"),
                **{**kwargs, "cache": later},
            )
        self.assertEqual(1, assess.call_count)
        self.assertEqual(first["current_source_ownership"], second["current_source_ownership"])

    def test_missing_required_receipt_handoff_fails_closed(self):
        with self.assertRaisesRegex(Exception, "owning lineage recovery"):
            validate_handoff({"schema_version": 35, "symbol": "REUS"})
        evidence, inputs = case()
        certificate = self.evaluate(evidence, inputs)["REUS"]["current_source_ownership"]
        for key, value in (("version", True), ("first", "nonsense"), ("asset_id", "old-id")):
            with self.subTest(key=key), self.assertRaisesRegex(Exception, "owning lineage recovery"):
                validate_handoff(
                    {
                        "schema_version": 35,
                        "symbol": "REUS",
                        "current_source_ownership": {**certificate, key: value},
                    }
                )

    def test_actual_evaluator_retry_retains_completed_proof_until_inputs_change(self):
        evidence, inputs = case()
        original = policy_engine.build_security_correlation_report

        def fail_peer(report, *args, **kwargs):
            result = original(report, *args, **kwargs)
            if report["symbol"] == "REUS":
                self.assertIsNotNone(result["current_source_ownership"])
                raise RuntimeError("interrupted peer")
            return result

        with patch.object(
            row_correlation, "assess_current_source", wraps=closed_departure.assess_current_source
        ) as assess:
            with (
                patch.object(policy_engine, "build_security_correlation_report", side_effect=fail_peer),
                self.assertRaisesRegex(RuntimeError, "interrupted peer"),
            ):
                self.evaluate(evidence, inputs)
            retried = self.evaluate(evidence, inputs)
            self.assertEqual(40, retried["REUS"]["correlated"]["row_count"])
            self.assertEqual(1, sum(call.args[0] == "REUS" for call in assess.call_args_list))
            inputs["REUS"].source_sha256 = "changed-generation"
            self.evaluate(evidence, inputs)
            self.assertEqual(2, sum(call.args[0] == "REUS" for call in assess.call_args_list))

    def test_cache_is_bound_to_store_generation_and_complete_selected_scope(self):
        evidence, inputs = case()
        owner = inputs[min(inputs)]
        with patch.object(
            row_correlation, "assess_current_source", wraps=closed_departure.assess_current_source
        ) as assess:
            self.evaluate(evidence, inputs)
            first = owner._completed_correlation_producers[3]
            self.evaluate(evidence, inputs)
            self.assertIs(first, owner._completed_correlation_producers[3])
            self.assertEqual(1, sum(call.args[0] == "REUS" for call in assess.call_args_list))
            self.evaluate(evidence, inputs, store=Path("/another-unused-store"))
            second = owner._completed_correlation_producers[3]
            self.assertIsNot(first, second)
            self.assertEqual(2, sum(call.args[0] == "REUS" for call in assess.call_args_list))
            scope = dict(store=Path("/another-unused-store"), selected_group=["REUS", "KEEP", "MISSING"])
            incomplete = self.evaluate(evidence, inputs, **scope)
            third = owner._completed_correlation_producers[3]
            self.assertIsNot(second, third)
            self.assertIsNone(incomplete["REUS"]["current_source_ownership"])
            unavailable = self.evaluate(evidence, inputs, unavailable_pricing=["MISSING"], **scope)
            self.assertIsNot(third, owner._completed_correlation_producers[3])
            self.assertIsNone(unavailable["REUS"]["current_source_ownership"])

    def test_unbound_legacy_adapter_does_not_retain_cross_call_cache(self):
        evidence, inputs = case()
        inputs["REUS"].source_sha256 = None
        facts = {name: evidence for name in inputs}
        arguments = dict(selected_group=sorted(inputs), unavailable_pricing=[])
        first = policy_engine._selected_group_cache(Path("/unused"), self.policy, inputs, facts, **arguments)
        second = policy_engine._selected_group_cache(Path("/unused"), self.policy, inputs, facts, **arguments)
        self.assertIsNot(first, second)


class ClosedDeparturePipelineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.root = self.store / "end_of_day/lineage"
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
        )
        self.current = asset("REUS", "current-id")
        self.old = asset("KEEP", "old-id")
        self.actions = [
            {
                "id": "departure",
                "action_type": "name_change",
                "old_symbol": "REUS",
                "new_symbol": "KEEP",
                "old_cusip": "111111111",
                "new_cusip": "111111111",
                "process_date": "2024-04-08",
            }
        ]
        self.rows = {
            "REUS": informative_bars("2024-06-12", "2024-06-19"),
            "KEEP": informative_bars("2024-04-05", "2024-06-19"),
        }
        for row in self.rows["REUS"]:
            for field in ("o", "h", "l", "c", "vw"):
                row[field] += 100
        for symbol, rows in self.rows.items():
            download_pricing(self.store, symbol, "2024-06-19", from_date="2024-01-01", client=FakeClient(rows))

    def run_lineage(self, **kwargs):
        with patch.object(pipeline, "download_pricing", side_effect=AssertionError("provider pricing")):
            result = pipeline.run_lineage(
                self.store,
                ["REUS"],
                identity_client=IdentityClient(
                    {"REUS": self.current, "KEEP": self.old, "111111111": self.old}, self.actions
                ),
                **kwargs,
            )
        self.assertEqual(0, result["failed"], result)
        return result

    def receipt(self, symbol="REUS"):
        return storage.read_receipt(self.root, symbol, storage.read_index(self.root))

    def test_actual_pipeline_keeps_proof_when_unresolved_peer_reopens_group(self):
        self.run_lineage()
        prior = self.receipt()
        self.assertEqual(0, prior["unresolved_rows"])
        self.assertGreater(self.receipt("KEEP")["unresolved_rows"], 0)
        for symbol, rows in self.rows.items():
            added = informative_bars("2024-06-20", "2024-06-21")
            download_pricing(self.store, symbol, "2024-06-21", client=FakeClient([*rows, *added]))
        original = row_correlation.assess_current_source
        decide = row_correlation._decide_row
        informative = correlation.has_informative_trading

        def reject_reproof(symbol, *args, **kwargs):
            if symbol == "REUS":
                raise AssertionError("completed current-source certificate was repeated")
            return original(symbol, *args, **kwargs)

        def reject_old_decision(context, **kwargs):
            if context.requested_symbol == "REUS" and context.session_date.isoformat() <= "2024-06-18":
                raise AssertionError("accepted current-source row decision was repeated")
            return decide(context, **kwargs)

        def reject_old_activity(rows):
            if rows and rows[0]["t"] == self.rows["REUS"][0]["t"] and rows[0]["c"] > 100:
                raise AssertionError("accepted historical activity was repeated")
            return informative(rows)

        with (
            patch.object(row_correlation, "assess_current_source", side_effect=reject_reproof),
            patch.object(row_correlation, "_decide_row", side_effect=reject_old_decision),
            patch.object(correlation, "has_informative_trading", side_effect=reject_old_activity),
        ):
            result = self.run_lineage()
        self.assertEqual(40, result["results"][0]["historical_rows_reused"])
        after = self.receipt()
        self.assertEqual(0, after["unresolved_rows"])
        self.assertEqual("2024-06-20", after["current_source_ownership"]["last"])
        self.assertEqual(prior["current_source_ownership"]["first"], after["current_source_ownership"]["first"])
        with (
            patch.object(pipeline, "correlate", side_effect=AssertionError("unchanged calculation")),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("unchanged payloads")),
        ):
            self.run_lineage()

    def test_peer_fallback_does_not_open_certified_sources_closed_pricing(self):
        for symbol, start in (("REUS", "2024-06-12"), ("KEEP", "2024-04-05")):
            self.rows[symbol] = informative_bars(start, "2024-07-01")
            if symbol == "REUS":
                for row in self.rows[symbol]:
                    for field in ("o", "h", "l", "c", "vw"):
                        row[field] += 100
            download_pricing(
                self.store, symbol, "2024-07-01", client=FakeClient(self.rows[symbol]), refresh_all=True
            )
        self.run_lineage()
        prior = self.receipt()
        for symbol, rows in self.rows.items():
            download_pricing(
                self.store,
                symbol,
                "2024-07-02",
                client=FakeClient([*rows, *informative_bars("2024-07-01", "2024-07-02")]),
            )
        load = pricing.load_published_boundary

        def reject_closed_source(root, source, kind, **kwargs):
            if source["symbol"] == "REUS" and kind != "current_month":
                raise AssertionError("certified source's closed pricing was reopened")
            return load(root, source, kind, **kwargs)

        with patch.object(pricing, "load_published_boundary", side_effect=reject_closed_source):
            self.run_lineage()
        after = self.receipt()
        self.assertEqual(prior["correlated_rows"] + 8, after["correlated_rows"])
        self.assertEqual("2024-07-01", after["current_source_ownership"]["last"])
        self.assertEqual(prior["identity_memberships"], after["identity_memberships"])

    def test_publication_interruption_recovers_completed_proof(self):
        with patch.object(storage, "finish_publication", side_effect=RuntimeError("interrupted journal")):
            failed = pipeline.run_lineage(
                self.store,
                ["REUS"],
                identity_client=IdentityClient(
                    {"REUS": self.current, "KEEP": self.old, "111111111": self.old}, self.actions
                ),
            )
        self.assertEqual(1, failed["failed"], failed)
        with (
            patch.object(
                row_correlation, "assess_current_source", side_effect=AssertionError("journal proof repeated")
            ),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("journal prices repeated")),
        ):
            self.run_lineage()
        self.assertEqual(0, self.receipt()["unresolved_rows"])

    def test_diagnostic_marks_mixed_source_reuse_without_replaying_decisions(self):
        self.run_lineage()
        for symbol, rows in self.rows.items():
            download_pricing(
                self.store,
                symbol,
                "2024-06-21",
                client=FakeClient([*rows, *informative_bars("2024-06-20", "2024-06-21")]),
            )
        capture = EvidenceCapture()
        self.run_lineage(observer=capture)
        current, old = capture.sources["REUS"], capture.sources["KEEP"]
        self.assertEqual("added_sessions", current["actual_calculation_scope"])
        self.assertEqual("calculated_with_closed_reuse", current["status"])
        self.assertEqual("full_history", old["actual_calculation_scope"])
        self.assertEqual("calculated", old["status"])
        self.assertTrue(any(item["decision_origin"] == "reused_publication" for item in current["assignments"]))
        self.assertEqual(6, len(current["pricing"]["session_grids"][0]["session_dates"]))

    def test_normal_append_and_empty_advance_consume_completed_ownership(self):
        self.rows["KEEP"] = informative_bars("2024-04-08", "2024-06-19")
        download_pricing(
            self.store, "KEEP", "2024-06-19", client=FakeClient(self.rows["KEEP"]), refresh_all=True
        )
        self.run_lineage()
        self.assertEqual(0, self.receipt("KEEP")["unresolved_rows"])
        # June 19 is an exchange holiday: coverage advances with no new row.
        for symbol, rows in self.rows.items():
            download_pricing(self.store, symbol, "2024-06-20", client=FakeClient(rows))
        with (
            patch.object(pipeline, "correlate", side_effect=AssertionError("empty append calculation")),
            patch.object(pricing, "load_published_boundary", side_effect=AssertionError("empty append prices")),
        ):
            self.run_lineage()
        for symbol, rows in self.rows.items():
            added = informative_bars("2024-06-20", "2024-06-21")
            download_pricing(self.store, symbol, "2024-06-21", client=FakeClient([*rows, *added]))
        original = row_correlation.assess_current_source

        def reject_reproof(symbol, *args, **kwargs):
            if symbol == "REUS":
                raise AssertionError("append repeated source certificate")
            return original(symbol, *args, **kwargs)

        with patch.object(row_correlation, "assess_current_source", side_effect=reject_reproof):
            result = self.run_lineage()
        self.assertEqual(16, result["results"][0]["pricing_rows_processed"])
        self.assertEqual("2024-06-20", self.receipt()["current_source_ownership"]["last"])

    def test_changed_mapping_cannot_keep_current_source_ownership(self):
        self.run_lineage()
        download_pricing(
            self.store,
            "REUS",
            "2024-06-19",
            client=FakeClient(self.rows["REUS"]),
            refresh_all=True,
            mapping_asof="2024-04-05",
        )
        with patch.object(
            row_correlation, "assess_current_source", wraps=closed_departure.assess_current_source
        ) as assess:
            self.run_lineage()
        self.assertTrue(any(call.args[0] == "REUS" for call in assess.call_args_list))
        after = self.receipt()
        self.assertIsNone(after["current_source_ownership"])
        self.assertFalse(
            any("current-id" in item["member_alpaca_asset_ids"] for item in after["identity_memberships"])
        )

    def test_actual_changed_history_selects_a_new_source_certificate(self):
        self.run_lineage()
        replacement = copy.deepcopy(self.rows["REUS"])
        for field in ("o", "h", "l", "c", "vw"):
            replacement[1][field] += 1
        download_pricing(self.store, "REUS", "2024-06-19", client=FakeClient(replacement), refresh_all=True)
        with patch.object(
            row_correlation, "assess_current_source", wraps=closed_departure.assess_current_source
        ) as assess:
            self.run_lineage()
        self.assertEqual(1, sum(call.args[0] == "REUS" for call in assess.call_args_list))
        self.assertEqual(0, self.receipt()["unresolved_rows"])


if __name__ == "__main__":
    unittest.main()
