import copy
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace

from libs.alpaca_end_of_day_security_correlation.retired_continuation import (
    RETIRED_CONTINUATION_RULE,
    assess_retired_continuation,
    retired_rename_path,
)
from libs.alpaca_end_of_day_security_correlation.row_correlation import download_session_dates
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy
from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_pricing_download.contract import ADJUSTMENTS as ALPACA_ADJUSTMENT_MODES
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage
from tests.test_alpaca_end_of_day_security_correlation import asset, correlation_source

PRODUCTION_POLICY = load_authorized_policy()


def history(start, end):
    return [
        dict(
            t=day + "T12:00:00Z",
            o=str(10 + i),
            h=str(10 + i),
            l=str(10 + i),
            c=str(10 + i),
            vw=str(10 + i),
            v="100",
            n="10",
        )
        for i, day in enumerate(expected_market_session_coverage(start, end).session_dates)
    ]


def lookup(kind, value, entity=None):
    return dict(
        lookup_kind=kind,
        lookup=value,
        found=entity is not None,
        outcome="found" if entity else "not_found",
        http_status=200 if entity else 404,
        asset=entity,
    )


class RetiredContinuationTests(unittest.TestCase):
    def setUp(self):
        current = asset("known", "NEXT", "Example warrants")
        self.lineage = {
            "query": {"corporate_actions_data_quality": "complete"},
            "identities": [current],
            "corporate_actions": [
                dict(
                    id="rename",
                    action_type="name_change",
                    old_symbol="OLD",
                    old_cusip="old-cusip",
                    new_symbol="NEXT",
                    new_cusip="new-cusip",
                    process_date="2024-08-20",
                )
            ],
            "asset_lookups": [
                lookup("symbol", "OLD"),
                lookup("cusip", "old-cusip"),
                lookup("symbol", "NEXT", current),
                lookup("cusip", "new-cusip", current),
            ],
            "symbol_occupancy_evidence": [
                dict(symbol="OLD", cusip="old-cusip", asset_id=None),
                dict(symbol="NEXT", cusip="new-cusip", asset_id="known"),
            ],
        }
        self.old = history("2024-08-01", "2024-08-16")
        self.new = history("2024-08-16", "2024-09-01")
        self.refresh()

    def refresh(self):
        self.work = [
            (
                SimpleNamespace(rows=copy.deepcopy(self.old)),
                {**correlation_source("OLD", asof="2026-10-14"), "adjustment": mode},
                download_session_dates(self.old),
            )
            for mode in ALPACA_ADJUSTMENT_MODES
        ]
        self.descriptors = {
            mode: dict(
                data_file=mode, adjustment=mode, download_sha256="generation", feed="sip", timeframe="1Day"
            )
            for mode in ALPACA_ADJUSTMENT_MODES
        }
        self.other = {mode: copy.deepcopy(self.new) for mode in ALPACA_ADJUSTMENT_MODES}

    def assess(self):
        return assess_retired_continuation(
            "OLD",
            self.lineage,
            self.work,
            correlation_loader=lambda _: SimpleNamespace(sources_by_filename=self.descriptors),
            source_rows_loader=lambda _, name: (self.other[name], "generation"),
        )

    def test_known_successor_identifies_continuity_not_missing_historical_uuid(self):
        result = self.assess()
        self.assertEqual(
            (date(2024, 8, 1), date(2024, 8, 15), "known"), (result.first, result.last, result.anchor_asset_id)
        )
        self.assertIsNone(self.lineage["symbol_occupancy_evidence"][0]["asset_id"])

    def test_real_three_pass_pipeline_recovers_without_fabricating_uuid_or_edges(self):
        inputs = {}
        for symbol, rows in (("OLD", self.old), ("NEXT", self.new)):
            downloads, descriptors = {}, []
            for mode in ALPACA_ADJUSTMENT_MODES:
                filename = mode + ".csv.gz"
                downloads[filename] = SimpleNamespace(
                    rows=copy.deepcopy(rows),
                    path=Path(filename),
                    manifest=dict(
                        symbol=symbol,
                        adjustment=mode,
                        sha256=symbol + mode,
                        asof="2026-10-14",
                        request_from="2016-01-01",
                        request_to="2026-10-15",
                        feed="sip",
                        timeframe="1Day",
                        rows=len(rows),
                    ),
                    pipeline_state="source_published",
                    integrity_failure=None,
                )
                descriptors.append(dict(file=filename, rows=len(rows)))
            inputs[symbol] = SimpleNamespace(
                downloads=downloads,
                local=dict(directory=symbol, downloads=descriptors, present=True),
                reader=lambda _root, _directory, descriptor, items=downloads: items[descriptor["file"]],
            )
        reports = correlate(Path("."), inputs, {s: self.lineage for s in inputs}, policy=PRODUCTION_POLICY)
        self.assertEqual(len(self.old) * 8, reports["OLD"]["correlated"]["row_count"])
        self.assertEqual([], reports["OLD"]["uuid_lineage_edges"])
        for segment in reports["OLD"]["correlated"]["segments"]:
            self.assertEqual(RETIRED_CONTINUATION_RULE, segment["correlation_method"])
            self.assertIsNone(segment["canonical_security"]["alpaca_asset_id"])
            self.assertIsNone(segment["canonical_security"]["cusip"])
            self.assertEqual(["known"], segment["security_identity"]["member_alpaca_asset_ids"])
        for descriptor in inputs["OLD"].local["downloads"]:
            descriptor["rows"] += 100
        partial = correlate(Path("."), inputs, {s: self.lineage for s in inputs}, policy=PRODUCTION_POLICY)
        self.assertEqual(0, partial["OLD"]["correlated"]["row_count"])

    def test_later_reuse_and_shared_inert_bridge_are_not_published(self):
        bridge = history("2024-08-16", "2025-03-01")
        for row in bridge:
            row.update(dict.fromkeys(("o", "h", "l", "c", "vw"), self.old[-1]["c"]))
            row.update(v="0", n="0")
        self.old += bridge + history("2025-03-03", "2025-04-01")
        self.refresh()
        result = self.assess()
        self.assertEqual(date(2024, 8, 15), result.last)

    def test_same_base_symbol_is_not_warrant_class_evidence(self):
        self.lineage["asset_lookups"][-1]["asset"] = asset("other", "NEXT.WS", "Different warrant series")
        self.assertIsNone(self.assess())
        self.setUp()
        self.lineage["asset_lookups"][-1]["asset"] = None
        self.assertIsNone(self.assess())

    def test_nearby_unlinked_series_and_unresolved_endpoint_cannot_supply_identity(self):
        self.lineage["corporate_actions"].clear()
        self.assertIsNone(self.assess())
        self.setUp()
        self.lineage["asset_lookups"][-1] = lookup("cusip", "new-cusip")
        self.assertIsNone(self.assess())

    def test_conflicting_occupancy_corrective_fork_and_recurrence_are_not_guessed(self):
        saved = copy.deepcopy(self.lineage)
        changes = [
            lambda: self.lineage["symbol_occupancy_evidence"].append(
                dict(symbol="OLD", cusip="foreign", asset_id=None)
            ),
            lambda: self.lineage["symbol_occupancy_evidence"].extend(
                [
                    dict(symbol="OLD", cusip="old-cusip", asset_id="foreign"),
                    dict(symbol="OLD", cusip="old-cusip", asset_id="another"),
                ]
            ),
            lambda: self.lineage["corporate_actions"].append(
                {**self.lineage["corporate_actions"][0], "id": "correction", "new_symbol": "OTHER"}
            ),
            lambda: self.lineage["corporate_actions"].append(
                dict(
                    id="return",
                    action_type="name_change",
                    old_symbol="NEXT",
                    old_cusip="new-cusip",
                    new_symbol="OLD",
                    new_cusip="old-cusip",
                    process_date="2025-01-01",
                )
            ),
        ]
        for change in changes:
            self.lineage = copy.deepcopy(saved)
            change()
            self.assertIsNone(self.assess())

    def test_overlap_gap_noninformative_and_incomplete_grids_fail_closed(self):
        self.new = history("2024-08-15", "2024-09-01")
        self.refresh()
        self.assertIsNone(self.assess())
        self.new = history("2024-08-26", "2024-09-01")
        self.refresh()
        self.assertIsNone(self.assess())
        self.setUp()
        self.work.pop()
        self.assertIsNone(self.assess())
        self.setUp()
        self.descriptors["raw"]["feed"] = "iex"
        self.assertIsNone(self.assess())
        self.setUp()
        for download, _, _ in self.work:
            for row in download.rows:
                row.update(v="0", n="0")
        self.assertIsNone(self.assess())

    def test_zero_day_gap_and_four_missing_sessions_have_precise_boundaries(self):
        for end in ("2024-08-22", "2024-08-23"):
            self.new = history(end, "2024-09-01")
            self.refresh()
            self.assertEqual(end == "2024-08-22", self.assess() is not None)

    def test_split_terms_do_not_veto_but_competing_replacement_does(self):
        self.lineage["corporate_actions"].append(
            dict(action_type="reverse_split", symbol="OLD", ex_date="2024-08-08")
        )
        self.assertIsNotNone(self.assess())
        self.lineage["corporate_actions"][-1].update(
            action_type="stock_merger", acquiree_symbol="OLD", effective_date="2024-08-08"
        )
        self.assertIsNone(self.assess())

    def test_multi_hop_and_otc_endpoint_are_evidence_not_availability(self):
        a = self.lineage["corporate_actions"][0]
        self.lineage["corporate_actions"].append(
            dict(
                id="next",
                action_type="name_change",
                old_symbol="MID",
                old_cusip="new-cusip",
                new_symbol="NEXT",
                new_cusip="new-cusip",
                process_date="2025-01-01",
            )
        )
        a["new_symbol"] = "MID"
        for item in self.lineage["asset_lookups"]:
            if item.get("asset"):
                item["asset"]["exchange"] = "OTC"
        self.lineage["symbol_occupancy_evidence"].append(dict(symbol="MID", cusip="new-cusip", asset_id="known"))
        self.assertIsNotNone(self.assess())
        self.lineage["corporate_actions"].append(
            dict(
                id="intermediate-replacement",
                action_type="cash_merger",
                acquiree_symbol="MID",
                acquirer_symbol="FOREIGN",
                effective_date="2024-10-01",
            )
        )
        self.assertIsNone(self.assess())
        self.lineage["corporate_actions"].pop()
        self.lineage["corporate_actions"][-1]["process_date"] = "2024-01-01"
        self.assertIsNone(retired_rename_path("OLD", self.lineage))
