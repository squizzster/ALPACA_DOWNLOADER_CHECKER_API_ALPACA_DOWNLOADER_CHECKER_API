import copy
import tempfile
import unittest
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, unquote, urlparse

from test_alpaca_lineage_build import asset
from test_alpaca_pricing_download import FakeClient, bar

from libs.alpaca_data_api_solver import ACTION_URL, build_plan
from libs.alpaca_lineage_build.contract import LineageError, reusable_versions, versions
from libs.alpaca_lineage_build.metadata import (
    EvidenceClient,
    WindowClient,
    acquire_unpriced,
    action_requests,
    action_symbols,
    full_metadata,
)
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.resume import stable_evidence
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient, IdentityHttpError
from libs.alpaca_security_lineage.settings import ACTION_TYPE_BY_RESPONSE_KEY

START, END = "2016-01-01", "2026-10-15"


def event(symbol="A", identity="event", **fields):
    return {
        "action_type": "cash_dividend",
        "id": identity,
        "symbol": symbol,
        "process_date": "2026-10-01",
        **fields,
    }


class Provider:
    """Exercise the real solver/executor against deterministic provider responses."""

    def __init__(self, symbols=(), actions=(), catalogue=None, page_size=1000):
        self.assets = {s: asset(s) for s in symbols}
        self.catalogue = list(self.assets.values()) if catalogue is None else catalogue
        self.actions = list(actions)
        self.page_size = page_size
        self.calls = []
        self.client = AlpacaIdentityClient("test", "test")
        self.client.request_json = self.request

    def request(self, url, *, allow_not_found=False, array=False):
        self.client.request_count += 1
        query = {k: v[0] for k, v in parse_qs(urlparse(url).query).items()}
        if url.startswith(ACTION_URL):
            self.calls.append(("actions", query))
            requested = set(query["symbols"].split(","))
            rows = [
                a
                for a in self.actions
                if requested & action_symbols(a) and query["start"] <= a["process_date"] <= query["end"]
            ]
            offset = int(query.get("page_token", 0))
            groups = defaultdict(list)
            response_keys = {v: k for k, v in ACTION_TYPE_BY_RESPONSE_KEY.items()}
            for row in rows[offset : offset + self.page_size]:
                groups[response_keys.get(row["action_type"], row["action_type"])].append(copy.deepcopy(row))
            return {
                "corporate_actions": dict(groups),
                "next_page_token": str(offset + self.page_size) if offset + self.page_size < len(rows) else None,
            }
        if urlparse(url).path == "/v2/assets":
            self.calls.append(("catalogue", query))
            if not array:
                raise AssertionError("catalogue transport must accept arrays")
            return copy.deepcopy(self.catalogue)
        identifier = unquote(urlparse(url).path.rsplit("/", 1)[-1])
        self.calls.append(("asset", identifier))
        if not allow_not_found:
            raise AssertionError("exact lookup must preserve 404")
        return copy.deepcopy(self.assets.get(identifier))


class LineageBatchingTests(unittest.TestCase):
    def test_transport_provenance_alone_does_not_reopen_cold_history(self):
        fields = (
            "identities",
            "corporate_actions",
            "symbol_occupancy_evidence",
            "input_symbol_occupants",
            "unresolved_cusips",
            "queried_symbols",
        )
        previous = {field: [] for field in fields}
        previous["asset_lookups"] = [
            {"lookup": "A", "lookup_kind": "symbol", "outcome": "found", "http_status": 200, "asset": asset("A")}
        ]
        current = copy.deepcopy(previous)
        current["asset_lookups"][0]["source"] = "asset_catalogue"
        self.assertTrue(stable_evidence(previous, current))
        current["asset_lookups"][0]["asset"]["id"] = "changed"
        self.assertFalse(stable_evidence(previous, current))

    def test_thousand_symbols_share_request_and_nested_events_keep_all_roles(self):
        symbols = [f"S{i:04}" for i in range(1000)]
        nested = event(
            "OLD",
            "nested",
            action_type="reorganization",
            stock_movements=[{"symbol": symbols[1]}, {"symbol": symbols[2]}],
        )
        merger = event(
            symbols[-1],
            "merger",
            action_type="stock_merger",
            acquirer_symbol=symbols[-1],
            acquiree_symbol="EXTERNAL",
        )
        provider = Provider(actions=[event(symbols[0]), nested, merger])
        client = EvidenceClient(provider.client)
        client.prefetch_corporate_actions([(s, START, END) for s in symbols])
        result = {s: client.get_corporate_actions(s, START, END) for s in symbols}
        self.assertEqual(len(provider.calls), 1)
        self.assertEqual(result[symbols[1]], [nested])
        self.assertEqual(result[symbols[2]], [nested])
        self.assertEqual(result[symbols[-1]], [merger])
        self.assertEqual(result[symbols[3]], [])

    def test_pagination_and_exact_windows_are_not_confused(self):
        provider = Provider(
            actions=[event("A", "old", process_date="2016-02-01"), event("A", "new"), event("B", "b")],
            page_size=1,
        )
        client = EvidenceClient(provider.client)
        client.prefetch_corporate_actions([("A", START, END), ("B", START, END)])
        self.assertEqual(len(provider.calls), 3)
        self.assertEqual(len(client.get_corporate_actions("A", START, END)), 2)
        self.assertEqual([a["id"] for a in client.get_corporate_actions("A", "2026-10-01", END)], ["new"])
        self.assertEqual(len(provider.calls), 3)

    def test_empty_first_page_does_not_finish_acquisition(self):
        client = AlpacaIdentityClient("test", "test")
        with patch.object(
            client,
            "request_json",
            side_effect=[
                {"corporate_actions": {}, "next_page_token": "next"},
                {"corporate_actions": {"cash_dividends": [event()]}},
            ],
        ) as request:
            result = EvidenceClient(client).get_corporate_actions("A", START, END)
        self.assertEqual(result, [event()])
        self.assertEqual(request.call_count, 2)

    def test_catalogue_uses_unique_entries_and_exact_fallbacks_without_inventing_404(self):
        symbols = [f"S{i}" for i in range(100)]
        catalogue = [asset(s) for s in symbols[:-1]] + [asset("S98", "other-id")]
        provider = Provider(symbols, catalogue=catalogue)
        client = EvidenceClient(provider.client)
        client.prefetch_symbols(symbols)
        self.assertEqual(
            provider.calls, [("catalogue", {"asset_class": "us_equity"}), ("asset", "S98"), ("asset", "S99")]
        )
        self.assertEqual(client.get_symbol_lookup("S0")["source"], "asset_catalogue")
        self.assertEqual(client.get_symbol_lookup("S99")["http_status"], 200)
        self.assertNotIn("source", client.get_symbol_lookup("S98"))
        # Even an identifier equal to a catalogue label must go through the exact CUSIP/ID path.
        provider.assets["S0"] = asset("S0", "historical-id")
        self.assertEqual(client.get_asset_lookup("S0")["asset"]["id"], "historical-id")
        client.prefetch_symbols(symbols)
        self.assertEqual(sum(k == "catalogue" for k, v in provider.calls), 1)

    def test_invalid_entry_is_not_a_positive_match_and_catalogue_failure_falls_back(self):
        symbols = [f"S{i}" for i in range(100)]
        provider = Provider(symbols, catalogue=[asset(s) for s in symbols])
        provider.catalogue[0]["id"] = ""
        client = EvidenceClient(provider.client)
        client.prefetch_symbols(symbols)
        self.assertEqual(provider.calls[-1], ("asset", "S0"))
        self.assertEqual(len(provider.calls), 2)
        provider = Provider(symbols, catalogue={"invalid": "array"})
        client = EvidenceClient(provider.client)
        client.prefetch_symbols(symbols)
        self.assertEqual(len(provider.calls), 101)
        self.assertTrue(all(client.get_symbol_lookup(s)["http_status"] == 200 for s in symbols))

    def test_provenance_is_preserved_only_for_catalogue_symbol_matches(self):
        symbols = [f"S{i}" for i in range(100)]
        provider = Provider(symbols, actions=[event("S0", cusip="OLD-CUSIP")])
        provider.assets["OLD-CUSIP"] = asset("S0", "old-identity")
        client = EvidenceClient(provider.client)
        client.prefetch_symbols(symbols)
        evidence = acquire_unpriced("S0", {"start": START, "asof": END}, client)
        lookup = {a["lookup_kind"]: a for a in evidence["asset_lookups"]}
        self.assertEqual(lookup["symbol"]["source"], "asset_catalogue")
        self.assertNotIn("source", lookup["cusip"])
        self.assertNotEqual(lookup["symbol"]["asset"]["id"], lookup["cusip"]["asset"]["id"])

    def test_unknown_action_routing_splits_and_checks_union(self):
        opaque = event(action_type="future_action")
        opaque.pop("symbol")
        client = AlpacaIdentityClient("test", "test")
        for leaf_rows, succeeds in (([opaque], True), ([], False)):
            with (
                self.subTest(succeeds=succeeds),
                patch.object(
                    client,
                    "request_json",
                    side_effect=[
                        {"corporate_actions": {"future_action": [opaque]}},
                        {"corporate_actions": {"future_action": leaf_rows}},
                        {"corporate_actions": {}},
                    ],
                ) as request,
            ):
                evidence = EvidenceClient(client)
                evidence.prefetch_corporate_actions([("A", START, END), ("B", START, END)])
                if succeeds:
                    self.assertEqual(evidence.get_corporate_actions("A", START, END), [opaque])
                    self.assertEqual(evidence.get_corporate_actions("B", START, END), [])
                else:
                    with self.assertRaisesRegex(LineageError, "changed during batch routing"):
                        evidence.get_corporate_actions("A", START, END)
                self.assertEqual(request.call_count, 3)
                if not succeeds:
                    with patch.object(client, "request_json", return_value={"corporate_actions": {}}) as retry:
                        self.assertEqual(evidence.get_corporate_actions("A", "2026-10-01", END), [])
                    self.assertEqual(retry.call_count, 1)

    def test_conflicting_identity_and_failed_pagination_never_publish_partial_evidence(self):
        provider = Provider(actions=[event(rate=1), event(rate=2)])
        client = EvidenceClient(provider.client)
        with self.assertRaisesRegex(LineageError, "conflicting corporate-action"):
            client.get_corporate_actions("A", START, END)
        client = AlpacaIdentityClient("test", "test")
        with patch.object(
            client,
            "request_json",
            side_effect=[
                {"corporate_actions": {"cash_dividends": [event()]}, "next_page_token": "again"},
                {"corporate_actions": {}, "next_page_token": "again"},
                {"corporate_actions": {}},
            ],
        ):
            evidence = EvidenceClient(client)
            evidence.prefetch_corporate_actions([("A", START, END), ("B", "2026-10-01", END)])
            with self.assertRaisesRegex(RuntimeError, "repeated a page token"):
                evidence.get_corporate_actions("A", START, END)
            self.assertEqual(evidence.get_corporate_actions("B", "2026-10-01", END), [])

    def test_oversized_request_splits_but_other_http_errors_do_not(self):
        plan = build_plan(
            {
                "steps": [
                    {
                        "needs": [
                            {
                                "kind": "corporate_actions",
                                "why": "lineage_evidence",
                                "symbols": ["A", "B"],
                                "start": START,
                                "end": END,
                            }
                        ]
                    }
                ]
            }
        )
        client = AlpacaIdentityClient("test", "test")
        with patch.object(
            client,
            "request_json",
            side_effect=[
                IdentityHttpError(414, "large"),
                {"corporate_actions": {"cash_dividends": [event()]}},
                {"corporate_actions": {}},
            ],
        ) as request:
            self.assertEqual(client.execute_plan(plan)[0][0], [event()])
            self.assertEqual(request.call_count, 3)
        with patch.object(client, "request_json", side_effect=IdentityHttpError(400, "invalid")) as request:
            with self.assertRaises(IdentityHttpError):
                client.execute_plan(plan)
            self.assertEqual(request.call_count, 1)

    def test_resume_preserves_old_evidence_and_new_labels_require_full_history(self):
        old = event("A", "old", process_date="2016-02-01")
        previous = {
            "anchor_month": "2026-10",
            "versions": {**versions(), "lineage": 8},
            "evidence": {"queried_symbols": ["A"], "corporate_actions": [old]},
        }
        self.assertTrue(reusable_versions(previous["versions"]))
        self.assertEqual(action_requests("A", START, END, previous), [("A", "2026-10-01", END)])
        self.assertEqual(action_requests("A", START, END, previous, refresh=True), [("A", START, END)])
        self.assertTrue(full_metadata(previous, {"from": "2016-01-01"}, False))
        provider = Provider(actions=[old, event("A", "new"), event("B", "b", process_date="2016-03-01")])
        window = WindowClient(EvidenceClient(provider.client), "A", previous, False)
        window.prefetch_corporate_actions(["A", "B"], START, END)
        self.assertEqual(window.get_corporate_actions("A", START, END), [old, event("A", "new")])
        self.assertEqual([a["id"] for a in window.get_corporate_actions("B", START, END)], ["b"])
        self.assertEqual({p["start"] for kind, p in provider.calls}, {START, "2026-10-01"})

    def test_newly_discovered_labels_share_next_wave(self):
        actions = [
            event("A", "ab", action_type="name_change", old_symbol="A", new_symbol="B"),
            event("A", "ac", action_type="name_change", old_symbol="A", new_symbol="C"),
        ]
        provider = Provider(["A", "B", "C"], actions=actions)
        evidence = acquire_unpriced("A", {"start": START, "asof": END}, EvidenceClient(provider.client))
        self.assertEqual(evidence["queried_symbols"], ["A", "B", "C"])
        self.assertEqual([p["symbols"] for kind, p in provider.calls if kind == "actions"], ["A", "B,C"])

    def test_pipeline_batches_independent_groups_and_unchanged_run_makes_no_requests(self):
        with (
            tempfile.TemporaryDirectory() as temporary,
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, 12, tzinfo=timezone.utc),
            ),
        ):
            store = Path(temporary) / "store"
            for symbol in ["A", "B"]:
                download_pricing(store, symbol, END, client=FakeClient([bar("2026-10-01")]))
            provider = Provider(["A", "B"])
            result = run_lineage(store, ["A", "B"], identity_client=provider.client)
            self.assertEqual(result["failed"], 0, result)
            self.assertEqual([p["symbols"] for kind, p in provider.calls if kind == "actions"], ["A,B"])
            self.assertEqual(len(provider.calls), 3)
            provider.calls.clear()
            result = run_lineage(store, ["A", "B"], identity_client=provider.client)
            self.assertEqual(result["failed"], 0, result)
            self.assertEqual(provider.calls, [])


if __name__ == "__main__":
    unittest.main()
