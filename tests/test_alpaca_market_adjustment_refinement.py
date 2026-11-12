"""Changed adjustment evidence consumes completed labels and original choices."""

from copy import deepcopy
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_dated_market_occupancy import continuous_case

from libs.alpaca_end_of_day_security_correlation import market_occupancy
from libs.alpaca_lineage_build import market_assignments, source_work
from libs.alpaca_lineage_build.contract import document_digest, versions
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy, load_policy
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


class MarketAdjustmentRefinementTests(TestCase):
    def prepared(self):
        inputs, reports, evidence = continuous_case()
        documents, prior = {}, {}
        for name, report in reports.items():
            # The fixture's first temporary observation is already owned here;
            # this test selects only the invalidated numerical decision.
            for segment in report["uncorrelated"]["segments"]:
                template = next(
                    s for s in report["correlated"]["segments"] if s["adjustment"] == segment["adjustment"]
                )
                report["correlated"]["segments"].append(
                    {**template, **{k: v for k, v in segment.items() if k != "reason_codes"}}
                )
            report["uncorrelated"]["segments"] = []
            for segment in report["correlated"]["segments"]:
                segment.update(from_=segment["first_timestamp"][:10])
                segment["from"] = segment.pop("from_")
                segment["to"] = segment["last_timestamp"][:10]
                segment["row_count"] = segment["row_end"] - segment["row_start"] + 1
                segment["observation_selection"] = "selected" if name == "MAIN" else "superseded"
            doc = {
                "schema_version": 37,
                "boundary": "alpaca_lineage_current_year",
                "anchor_month": "2026-11",
                "adjustments": [],
            }
            for view in ADJUSTMENTS:
                segments = sorted(
                    (
                        {**s, "branch": "correlated"}
                        for s in report["correlated"]["segments"]
                        if s["adjustment"] == view
                    ),
                    key=lambda s: s["row_start"],
                )
                for segment in segments:
                    segment.pop("adjustment")
                doc["adjustments"].append(
                    {"adjustment": view, "rows": len(inputs[name].views[view]), "segments": segments}
                )
            documents[name] = {"current_year": doc}
            prior[name] = {
                "versions": versions(),
                "correlation_policy": load_authorized_policy().provenance(),
                "source_sha256": name,
                "anchor_month": "2026-11",
                "evidence": {},
                "identity_memberships": [report["correlated"]["segments"][0]["security_identity"]],
                "boundaries": {"current_year": {"sha256": document_digest(doc)}},
                "unresolved_rows": 0,
                "observation_routes": {"public": [inputs[name].views["raw"][-1][0], {}]}
                if name == "MAIN"
                else {},
            }
        return inputs, evidence, documents, prior

    def test_refinement_reuses_labels_and_compares_against_published_states(self):
        inputs, evidence, documents, prior = self.prepared()
        generations = {name: "a" * 64 for name in prior}
        old_policy = load_policy("resources/alpaca_security_correlation_v83_legacy_policy.yaml")
        old_out = {}
        first, first_documents = market_assignments.project_group(
            prior,
            evidence,
            generations,
            documents,
            old_policy,
            date_rows=lambda name, _kind: inputs[name].views,
            temporary_source_provider=lambda name: inputs[name].source,
            selection_output=old_out,
        )
        self.assertTrue(old_out["revision"])
        completed = market_assignments.CompletedMarketProjection(first, first_documents)
        selected = {}
        with patch.object(market_occupancy, "project", side_effect=AssertionError("completed labels repeated")):
            revised, changed = market_assignments.project_group(
                prior,
                evidence,
                generations,
                documents,
                load_authorized_policy(),
                date_rows=lambda name, _kind: inputs[name].views,
                temporary_source_provider=lambda name: inputs[name].source,
                completed_labels=completed,
                selection_output=selected,
            )
        self.assertIsNone(selected["revision"])
        for name, receipt in revised.items():
            self.assertTrue(receipt["dated_market_projection"]["selection_unchanged"])
            self.assertNotIn("numerical_source_revision", receipt)
            for doc in changed[name].values():
                for entry in doc["adjustments"]:
                    self.assertTrue(
                        all(
                            s["observation_selection"] == ("selected" if name == "MAIN" else "superseded")
                            for s in entry["segments"]
                        )
                    )
                    self.assertTrue(any(s["historical_symbol"] == "ALT" for s in entry["segments"]))
        controls = {
            name: SimpleNamespace(previous={**receipt, "evidence": evidence[name]}, accepted_reuse=(True,))
            for name, receipt in prior.items()
        }
        # Installing adjustment scope onto accepted dated labels selects the same
        # adapter on the ordinary metadata path, without rebuilding those labels.
        old_evidence = deepcopy(evidence)
        for value in old_evidence.values():
            fact = value["sourced_market_occupancy"]["facts"][0]
            value["sourced_market_occupancy"]["facts"] = [
                {
                    **fact,
                    "facts": {k: v for k, v in fact["facts"].items() if k != "adjustment_basis"}
                    | {"schema_version": 1},
                }
            ]
        for name, control in controls.items():
            control.previous["evidence"] = old_evidence[name]
        plan = source_work._regular_market_plan(controls, evidence, lambda _name: True)
        self.assertTrue(plan.market_labels_complete)
