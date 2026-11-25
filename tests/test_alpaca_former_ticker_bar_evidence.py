"""Fresh group observation boundaries for ambiguous former historical symbols."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch

from test_alpaca_pipeline_evidence import NoRequests, transport
from test_alpaca_pricing_download import FakeClient, bar

from libs.alpaca_data_api_solver import instant
from libs.alpaca_end_of_day_security_correlation import former_ticker
from libs.alpaca_lineage_build import group_engine
from libs.alpaca_lineage_build.former_ticker_bars import complete_literal_edges
from libs.alpaca_lineage_build.metadata import EvidenceClient, RetainedReportClient
from libs.alpaca_pricing_download.contract import BarsSymbolUnavailable, DownloadError
from libs.alpaca_security_lineage.inference_evidence import ProviderEvidence


def view(name, asof, scope="complete_history"):
    return SimpleNamespace(
        source_sha256=name + "-sha",
        source={"latest_request": {"asof": asof}},
        materialization_scope=scope,
    )


class LiteralBarEvidenceTests(TestCase):
    def setUp(self):
        self.inputs = {"FORMER": view("former", "2019-02-08"), "TERMINAL": view("terminal", "2026-06-01")}
        self.facts = {name: ProviderEvidence() for name in self.inputs}
        self.facts["FORMER"]._former_ticker_anchors = {"FORMER": ()}
        self.provider = EvidenceClient(NoRequests())
        self.days = ("2019-02-04", "2019-02-08")
        self.plan = {("FORMER", "TERMINAL"): self.days}
        self.store = "selected-store"
        self.binding = self.store, "former-sha", "2019-02-08"

    def complete(self, pricing, completion, provider=None, previous=None):
        return complete_literal_edges(
            self.store, self.inputs, self.facts, previous or {}, pricing, provider or self.provider, completion
        )

    def test_first_observation_and_absence_survive_peer_failure_and_stage_transition(self):
        market = FakeClient([bar(self.days[0])])
        completion = {}
        with patch.object(former_ticker, "literal_edge_requests", return_value=self.plan) as select:
            first = market.fetch

            def fail_peer(symbol, adjustment, start, end, *, asof):
                if start == instant(self.days[1]):
                    raise DownloadError("peer transport failure")
                return first(symbol, adjustment, start, end, asof=asof)

            with patch.object(market, "fetch", fail_peer), self.assertRaisesRegex(DownloadError, "peer"):
                self.complete(market, completion)
            self.assertEqual(
                set(self.provider.literal_bar_observations), {("FORMER", self.days[0], self.binding)}
            )
            self.assertNotIn("former_ticker_literal_bars", self.facts["FORMER"])
            restored, recovered = transport(self.provider, extra=completion)
            reports = self.complete(market, recovered, restored)
            select.assert_called_once_with(self.inputs, self.facts)
            self.assertEqual(market.asofs, ["-", "-"])
            self.assertEqual(
                market.calls,
                [
                    ("FORMER", "raw", instant(day), instant(end))
                    for day, end in zip(self.days, ("2019-02-05", "2019-02-09"), strict=True)
                ],
            )
            report = reports["FORMER"]
            self.assertIsNot(report, self.facts["FORMER"])
            self.assertIs(report._former_ticker_anchors, self.facts["FORMER"]._former_ticker_anchors)
            bundle = report["former_ticker_literal_bars"]["TERMINAL"]
            self.assertEqual(bundle["source_generations"], {"FORMER": "former-sha", "TERMINAL": "terminal-sha"})
            self.assertEqual(bundle["source_asofs"], {"FORMER": "2019-02-08", "TERMINAL": "2026-06-01"})
            observed, absent = bundle["observations"]
            self.assertEqual(
                observed,
                {
                    "symbol": "FORMER",
                    "session": self.days[0],
                    "feed": "sip",
                    "asof": "-",
                    "adjustment": "raw",
                    "timeframe": "1Day",
                    "start": "2019-02-04T05:00:00Z",
                    "end_exclusive": "2019-02-05T05:00:00Z",
                    "http_status": 200,
                    "outcome": "observed",
                    "bar": {k: str(v) for k, v in bar(self.days[0]).items()},
                },
            )
            self.assertEqual((absent["outcome"], absent["http_status"]), ("no_bars", 200))
            self.assertNotIn("bar", absent)
            with patch.object(market, "fetch", side_effect=AssertionError("repeated price request")):
                self.assertIs(self.complete(market, recovered, restored), reports)
                another_group = {}
                self.assertEqual(self.complete(market, another_group, restored), reports)
            self.assertEqual(select.call_count, 2)

    def test_explicitly_rejected_symbol_is_unavailable_without_inventing_empty_tape(self):
        market = FakeClient([])
        with (
            patch.object(
                former_ticker, "literal_edge_requests", return_value={("FORMER", "TERMINAL"): self.days[:1]}
            ),
            patch.object(market, "fetch", side_effect=BarsSymbolUnavailable("FORMER", 400)) as fetch,
        ):
            first = self.complete(market, {})
            restored, _ = transport(self.provider)
            self.assertEqual(self.complete(market, {}, restored), first)
        fetch.assert_called_once()
        point = first["FORMER"]["former_ticker_literal_bars"]["TERMINAL"]["observations"][0]
        self.assertEqual((point["http_status"], point["outcome"]), (400, "unavailable"))
        self.assertNotIn("bar", point)

    def test_previously_assigned_and_suffix_sources_never_select_a_literal_request(self):
        self.inputs["PEER"] = view("peer", "2026-06-01")
        with patch.object(
            former_ticker, "literal_edge_requests", side_effect=AssertionError("reopened sources")
        ):
            self.assertFalse(self.complete(None, {}, previous={"PEER": {"accepted": True}}))
            self.inputs["PEER"].accepted_report = {"accepted": True}
            self.assertFalse(self.complete(None, {}))
            self.inputs["PEER"].accepted_report = None
            self.inputs["PEER"].retained_split = {"assigned": True}
            self.assertFalse(self.complete(None, {}))
            self.inputs["PEER"].retained_split = None
            self.inputs["PEER"].materialization_scope = "suffix"
            self.assertFalse(self.complete(None, {}))

    def test_changed_former_generation_mapping_or_store_invalidates_only_its_raw_observation(self):
        market = FakeClient([bar(self.days[0], "1")])
        plan = {("FORMER", "TERMINAL"): self.days[:1]}
        with patch.object(former_ticker, "literal_edge_requests", return_value=plan):

            def observed():
                return self.complete(market, {})["FORMER"]["former_ticker_literal_bars"]["TERMINAL"]

            first = observed()
            self.inputs["TERMINAL"].source_sha256 = "updated-archive"
            self.inputs["TERMINAL"].source["latest_request"]["asof"] = "2026-07-01"
            with patch.object(market, "fetch", side_effect=AssertionError("unchanged former route repeated")):
                archive = observed()
            self.assertIs(archive["observations"][0], first["observations"][0])
            self.assertEqual(archive["source_generations"]["TERMINAL"], "updated-archive")
            self.assertEqual(archive["source_asofs"]["TERMINAL"], "2026-07-01")
            market.bars = [bar(self.days[0], "2")]
            self.inputs["FORMER"].source_sha256 = "corrected-former"
            self.assertEqual(observed()["observations"][0]["bar"]["c"], "2")
            self.inputs["FORMER"].source["latest_request"]["asof"] = "2019-02-11"
            market.bars = []
            self.assertEqual(observed()["observations"][0]["outcome"], "no_bars")
            self.store = "another-store"
            market.bars = [bar(self.days[0], "3")]
            self.assertEqual(observed()["observations"][0]["bar"]["c"], "3")
        self.assertEqual(len(market.calls), 4)

    def test_an_unbound_source_cannot_acquire_or_lend_a_literal_bar(self):
        market = FakeClient([])
        with (
            patch.object(former_ticker, "literal_edge_requests", return_value=self.plan),
            patch.object(market, "fetch", side_effect=AssertionError("unbound observation")),
        ):
            self.inputs["FORMER"].source_sha256 = None
            self.assertEqual(self.complete(market, {}), {})
            self.inputs["FORMER"].source_sha256 = "former-sha"
            self.inputs["TERMINAL"].source["latest_request"].pop("asof")
            self.assertEqual(self.complete(market, {}), {})
        self.assertFalse(self.provider.literal_bar_observations)

    def test_only_complete_fresh_components_request_literal_bars(self):
        names = ("FORMER", "TERMINAL", "MIX", "MIXPEER", "CLOSED", "PART", "PARTPEER")
        for name in names:
            self.inputs.setdefault(name, view(name, "2026-06-01"))
            self.facts[name] = ProviderEvidence(identities=[])
        deps = {
            "FORMER": ["TERMINAL"],
            "TERMINAL": [],
            "MIX": ["MIXPEER", "CLOSED"],
            "MIXPEER": [],
            "CLOSED": [],
            "PART": ["PARTPEER", "NO-INPUT"],
            "PARTPEER": [],
        }
        market = FakeClient([])
        services = SimpleNamespace(
            event=Mock(), correlate=lambda store, inputs, *args, **kwargs: dict.fromkeys(inputs, {})
        )
        select = Mock(return_value={("FORMER", "TERMINAL"): self.days[:1]})
        with patch.object(former_ticker, "literal_edge_requests", select):
            group_engine.correlate_components(
                self.store,
                self.inputs,
                self.facts,
                {"CLOSED": {"accepted": True}},
                deps,
                {},
                policy=object(),
                retain=False,
                services=services,
                literal_clients=(market, self.provider),
            )
        select.assert_called_once()
        self.assertEqual(set(select.call_args.args[0]), {"FORMER", "TERMINAL"})
        self.assertEqual(len(market.calls), 1)
        self.assertEqual([n for n in names if "former_ticker_literal_bars" in self.facts[n]], ["FORMER"])

    def test_retained_undated_404_does_not_take_the_catalogue_observation_date(self):
        missing = {
            "lookup": "FORMER",
            "lookup_kind": "symbol",
            "asset": None,
            "http_status": 404,
            "outcome": "not_found",
        }
        legacy = {"asset_lookups": [missing], "catalogue_observed_at_utc": "2026-06-01T16:00:00Z"}
        retained = RetainedReportClient(self.provider, legacy)
        self.assertIs(retained.get_symbol_lookup("FORMER"), missing)
        self.assertNotIn("observed_at_utc", missing)

    def test_terminal_exchange_snapshot_keeps_its_observation_source(self):
        from libs.alpaca_lineage_build.projection import last_observed_symbol_lookup

        lookup = {
            "lookup": "ARCHIVE",
            "lookup_kind": "symbol",
            "asset": {"symbol": "ARCHIVE", "class": "us_equity", "exchange": "OTC"},
        }
        evidence = {"asset_lookups": [lookup], "catalogue_observed_at_utc": "2026-08-01T01:00:00Z"}
        receipt = {"symbol": "ARCHIVE", "evidence": evidence}
        client = RetainedReportClient(self.provider, evidence)
        self.assertIs(client.get_symbol_lookup("ARCHIVE"), lookup)
        self.assertIsNone(last_observed_symbol_lookup(receipt)["catalogue_observed_at_utc"])
        lookup["source"] = "asset_catalogue"
        self.assertEqual(
            client.get_symbol_lookup("ARCHIVE")["observed_at_utc"], evidence["catalogue_observed_at_utc"]
        )
        self.assertEqual(
            last_observed_symbol_lookup(receipt)["catalogue_observed_at_utc"],
            evidence["catalogue_observed_at_utc"],
        )
        lookup["observed_at_utc"] = "2025-10-13T13:00:00Z"
        self.assertIs(client.get_symbol_lookup("ARCHIVE"), lookup)
        self.assertEqual(
            last_observed_symbol_lookup(receipt)["catalogue_observed_at_utc"], lookup["observed_at_utc"]
        )

    def test_concurrent_peers_share_the_first_successful_literal_observation(self):
        market = FakeClient([])
        started, release = Event(), Event()

        def fetch(*args, **kwargs):
            started.set()
            if not release.wait(5):
                raise AssertionError("daily bar request was not released")
            return []

        with patch.object(market, "fetch", side_effect=fetch) as request, ThreadPoolExecutor(2) as workers:
            first = workers.submit(
                self.provider.get_literal_bar_observation, market, "FORMER", self.days[0], binding=self.binding
            )
            self.assertTrue(started.wait(5))
            peer = workers.submit(
                self.provider.get_literal_bar_observation, market, "FORMER", self.days[0], binding=self.binding
            )
            release.set()
            self.assertIs(first.result(), peer.result())
        request.assert_called_once()
