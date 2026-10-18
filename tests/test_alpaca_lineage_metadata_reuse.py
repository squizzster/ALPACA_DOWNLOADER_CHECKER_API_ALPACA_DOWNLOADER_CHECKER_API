"""Changed correlation rules consume approved metadata without reacquiring it."""

import unittest
from unittest.mock import patch

import test_alpaca_lineage_build as fixtures
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import bar, snapshot

from libs.alpaca_lineage_build import contract, pipeline


class MetadataReuseTests(unittest.TestCase):
    setUp = fixtures.LineageBuildTests.setUp
    download = fixtures.LineageBuildTests.download
    run_build = fixtures.LineageBuildTests.run_build
    receipt = fixtures.LineageBuildTests.receipt

    def changed_rules(self):
        return patch.object(contract, "CORRELATION_VERSION", contract.CORRELATION_VERSION + 1)

    def test_rule_only_change_recalculates_once_with_stored_metadata(self):
        self.download()
        self.run_build()
        previous = self.receipt()
        pricing = snapshot(self.store / "end_of_day/pricing")
        client = IdentityClient()
        with (
            self.changed_rules(),
            patch.object(pipeline, "acquire", side_effect=AssertionError("metadata reacquisition")),
            patch.object(pipeline, "download_pricing", side_effect=AssertionError("pricing acquisition")),
            patch.object(pipeline, "correlate", wraps=pipeline.correlate) as calculate,
        ):
            result = self.run_build(client=client)
            self.assertEqual(result["results"][0]["status"], "processed")
            self.assertFalse(result["results"][0]["reused_closed_boundaries"])
            self.assertEqual(calculate.call_count, 1)
            current = self.receipt()
            self.assertEqual(current["evidence"], previous["evidence"])
            self.assertEqual(current["versions"], contract.versions())
            published = snapshot(self.store)
            repeated = self.run_build(client=client)
            self.assertEqual(repeated["results"], [])
            self.assertEqual(snapshot(self.store), published)
            self.assertEqual(calculate.call_count, 1)
        self.assertEqual(client.request_count, 0)
        self.assertEqual(snapshot(self.store / "end_of_day/pricing"), pricing)

    def test_rule_only_change_recalculates_affected_links_with_stored_evidence(self):
        self.download("GOOG")
        self.download("ALIAS")
        identities = IdentityClient({name: asset(name, "shared-id") for name in ("GOOG", "ALIAS")})
        self.run_build(client=identities)
        previous = {name: self.receipt(name) for name in ("GOOG", "ALIAS")}
        self.assertEqual(set(previous["GOOG"]["input_sources"]), {"GOOG", "ALIAS"})
        client = IdentityClient()
        with (
            self.changed_rules(),
            patch.object(pipeline, "acquire", side_effect=AssertionError("metadata reacquisition")),
            patch.object(pipeline, "correlate", wraps=pipeline.correlate) as calculate,
        ):
            result = self.run_build(["GOOG"], client=client)
            self.assertEqual(result["results"][0]["processed"], ["ALIAS", "GOOG"])
            self.assertEqual(calculate.call_count, 1)
            for name, prior in previous.items():
                self.assertEqual(self.receipt(name)["evidence"], prior["evidence"])
                self.assertEqual(self.receipt(name)["versions"], contract.versions())
        self.assertEqual(client.request_count, 0)

    def test_changed_pricing_with_new_rules_still_acquires_metadata(self):
        self.download()
        self.run_build()
        self.download(end="2026-10-15", bars=self.bars + [bar("2026-10-14")])
        client = IdentityClient()
        with self.changed_rules():
            self.run_build(client=client)
        self.assertEqual(client.calls, [("GOOG", "2016-01-01", "2026-10-15")])

    def test_identical_pricing_notice_prevents_metadata_reuse(self):
        self.download()
        self.run_build()
        source = self.receipt()["source_sha256"]
        self.download(refresh_all=True)
        client = IdentityClient()
        with self.changed_rules():
            self.run_build(client=client)
        self.assertEqual(self.receipt()["source_sha256"], source)
        self.assertEqual(client.calls, [("GOOG", "2016-01-01", "2026-10-14")])

    def test_explicit_refresh_with_new_rules_still_acquires_metadata(self):
        self.download()
        self.run_build()
        prior = self.receipt()["evidence"]
        client = IdentityClient({"GOOG": asset("GOOG", "corrected-id")})
        with self.changed_rules():
            self.run_build(["GOOG"], client=client, refresh=True)
        self.assertEqual(client.calls, [("GOOG", "2016-01-01", "2026-10-14")])
        self.assertNotEqual(self.receipt()["evidence"], prior)

    def test_changed_metadata_contract_still_acquires_metadata(self):
        self.download()
        self.run_build()
        client = IdentityClient()
        with (
            self.changed_rules(),
            patch.object(contract, "REPORT_VERSION", contract.REPORT_VERSION + 1),
        ):
            self.run_build(client=client)
        self.assertEqual(client.calls, [("GOOG", "2016-01-01", "2026-10-14")])

    def test_new_linked_input_prevents_metadata_reuse(self):
        identities = IdentityClient({name: asset(name, "shared-id") for name in ("GOOG", "ALIAS")})
        self.download("GOOG")
        self.run_build(client=identities)
        self.download("ALIAS")
        client = IdentityClient(identities.assets)
        with self.changed_rules(), patch.object(pipeline, "acquire", wraps=pipeline.acquire) as acquire:
            result = self.run_build(["ALIAS"], client=client)
        self.assertEqual(result["results"][0]["processed"], ["ALIAS", "GOOG"])
        self.assertEqual({call.args[0].symbol for call in acquire.call_args_list}, {"ALIAS", "GOOG"})
        self.assertGreater(client.request_count, 0)

    def test_metadata_version_gate_ignores_only_valid_correlation_versions(self):
        current = contract.versions()
        prior = {**current, "correlation": current["correlation"] - 1}
        for version in (2, 3, 4, 5, contract.VERSION):
            self.assertTrue(contract.reusable_metadata_versions({**prior, "lineage": version}))
        for value in (
            None,
            {key: value for key, value in prior.items() if key != "correlation"},
            {**prior, "correlation": None},
            {**prior, "correlation": True},
            {**prior, "correlation": 0},
            {**prior, "lineage": 1},
            {**prior, "identity_report": current["identity_report"] + 1},
            {**prior, "calendar_sha256": "0" * 64},
            {**prior, "unknown": 1},
        ):
            with self.subTest(value=value):
                self.assertFalse(contract.reusable_metadata_versions(value))


if __name__ == "__main__":
    unittest.main()
