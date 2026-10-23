import copy
import unittest
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace

from libs.alpaca_end_of_day_security_correlation.evidence import build_ticker_ownership_intervals
from libs.alpaca_end_of_day_security_correlation.predecessor_prefix import (
    PREDECESSOR_PREFIX_RULE_NAME,
    assess_predecessor_prefixes,
)
from libs.alpaca_end_of_day_security_correlation.row_correlation import (
    CorrelationCache,
    correlate_download_rows,
    download_session_dates,
)
from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges
from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_pricing_download.contract import ADJUSTMENTS as ALPACA_ADJUSTMENT_MODES
from tests.test_alpaca_end_of_day_security_correlation import (
    add_name_change_edge,
    asset,
    complete_market_session_rows,
    correlation_source,
)


def lineage():
    result = {"corporate_actions": [], "symbol_occupancy_evidence": [], "identities": []}
    for suffix, successor, when in (("1", "NEXT", "2024-08-16"), ("2", "OTHER", "2025-10-30")):
        add_name_change_edge(
            result,
            action_id="rename" + suffix,
            process_date=when,
            predecessor=("old" + suffix, "OLD", "old-cusip" + suffix, "Predecessor"),
            successor=("new" + suffix, successor, "new-cusip" + suffix, "Successor"),
        )
    return result


def rows():
    return [
        {
            "t": session + "T04:00:00Z",
            "o": price,
            "h": price,
            "l": price,
            "c": price,
            "v": "100",
            "n": "10",
            "vw": price,
        }
        for session, price in (("2024-08-14", "12"), ("2024-08-15", "13"), ("2024-08-16", "14"))
    ]


class PredecessorPrefixTests(unittest.TestCase):
    def setUp(self):
        self.lineage = lineage()
        self.intervals, _ = build_ticker_ownership_intervals(self.lineage)
        # These unit tests assess candidates against an independently supplied
        # donor report; candidate resolution itself no longer accepts an edge.
        self.edges, _ = build_uuid_lineage_edges(self.lineage, candidates_only=True)
        self.work = []
        self.donor_rows = {}
        self.descriptors = {}
        self.segments = []
        for adjustment in ALPACA_ADJUSTMENT_MODES:
            filename = adjustment + ".csv.gz"
            source = {
                **correlation_source("OLD", asof="2026-10-14"),
                "adjustment": adjustment,
                "data_file": filename,
            }
            self.work.append((SimpleNamespace(rows=rows()), source, download_session_dates(rows())))
            self.donor_rows[filename] = rows()
            self.descriptors[filename] = {
                "data_file": filename,
                "adjustment": adjustment,
                "download_sha256": "generation",
            }
            self.segments.append(
                {
                    "data_file": filename,
                    "row_start": 1,
                    "row_end": 2,
                    "correlation_rule": {"name": "asof_selected_uuid_lineage_interval"},
                    "historical_symbol": "OLD",
                    "canonical_security": {"alpaca_asset_id": "old1", "cusip": "old-cusip1"},
                    "feed": "sip",
                    "timeframe": "1Day",
                    "download_sha256": "generation",
                }
            )

    def assess(self):
        def report(symbol):
            return SimpleNamespace(
                correlated={"segments": self.segments if symbol == "NEXT" else []},
                sources_by_filename=self.descriptors,
            )

        return assess_predecessor_prefixes(
            "OLD",
            self.lineage,
            self.intervals,
            self.edges,
            self.work,
            correlation_loader=report,
            source_rows_loader=lambda _symbol, filename: (self.donor_rows[filename], "generation"),
        )

    def test_only_exact_prefix_resolves_without_changing_membership_or_later_rows(self):
        result = self.assess()
        self.assertEqual(1, len(result))
        self.assertEqual(date(2024, 8, 14), result[0].first_session)
        source = self.work[0][1]
        before = correlate_download_rows(rows(), source, self.lineage, self.intervals)
        after = correlate_download_rows(
            rows(), source, self.lineage, self.intervals, predecessor_prefixes=result
        )
        cache = CorrelationCache()
        self.assertEqual(
            before, correlate_download_rows(rows(), source, self.lineage, self.intervals, cache=cache)
        )
        self.assertEqual(
            after,
            correlate_download_rows(
                rows(), source, self.lineage, self.intervals, predecessor_prefixes=result, cache=cache
            ),
        )
        self.assertEqual([], before["correlated"])
        self.assertEqual(2, after["correlated"][0]["row_count"])
        self.assertEqual(PREDECESSOR_PREFIX_RULE_NAME, after["correlated"][0]["correlation_rule"]["name"])
        self.assertEqual("old1", after["correlated"][0]["canonical_security"]["alpaca_asset_id"])
        self.assertEqual(["old1"], after["correlated"][0]["security_identity"]["member_alpaca_asset_ids"])
        self.assertEqual(1, after["uncorrelated"][0]["row_count"])
        for field in ("row_start", "row_end", "first_session_date", "last_session_date", "reason_codes"):
            self.assertEqual(before["uncorrelated"][-1][field], after["uncorrelated"][-1][field])

    def test_one_view_one_price_or_one_session_difference_disqualifies(self):
        for change in ("price", "missing", "extra", "timestamp"):
            with self.subTest(change=change):
                saved = copy.deepcopy(self.donor_rows)
                selected = self.donor_rows[next(iter(self.donor_rows))]
                if change == "price":
                    selected[0]["vw"] = "12.000001"
                elif change == "missing":
                    selected.pop(0)
                elif change == "extra":
                    selected.insert(1, {**selected[0], "t": "2024-08-14T05:00:00Z"})
                else:
                    selected[0]["t"] = "2024-08-14T05:00:00Z"
                self.assertEqual((), self.assess())
                self.donor_rows = saved

    def test_all_views_required_and_generation_must_match(self):
        removed = self.work.pop()
        self.assertEqual((), self.assess())
        self.work.append(removed)
        self.descriptors[next(iter(self.descriptors))]["download_sha256"] = "stale"
        self.assertEqual((), self.assess())

    def test_later_adjustments_do_not_change_raw_transaction_ownership(self):
        for adjustment in ALPACA_ADJUSTMENT_MODES:
            if adjustment == "raw":
                continue
            for row in self.donor_rows[adjustment + ".csv.gz"]:
                for field in ("o", "h", "l", "c", "vw"):
                    row[field] = str(float(row[field]) * 10)
                row["v"] = "10"
        self.assertEqual(1, len(self.assess()))

    def test_adjustment_session_difference_still_prevents_cross_view_assignment(self):
        download, source, _ = self.work[-1]
        download.rows.pop(0)
        self.work[-1] = (download, source, download_session_dates(download.rows))
        self.assertEqual((), self.assess())

    def test_unresolved_wrong_identity_recursive_or_different_feed_is_not_a_donor(self):
        for field, value in (
            ("correlation_rule", {"name": PREDECESSOR_PREFIX_RULE_NAME}),
            ("canonical_security", {"alpaca_asset_id": "old2", "cusip": "old-cusip2"}),
            ("feed", "iex"),
            ("row_end", 1),
            ("historical_symbol", "OTHER"),
        ):
            with self.subTest(field=field):
                saved = copy.deepcopy(self.segments)
                self.segments[0][field] = value
                self.assertEqual((), self.assess())
                self.segments = saved

    def test_constant_and_zero_trade_series_are_not_identity(self):
        for kind in ("constant", "volume", "trades"):
            with self.subTest(kind=kind):
                saved_work, saved_donor = copy.deepcopy(self.work), copy.deepcopy(self.donor_rows)
                for values in [item[0].rows for item in self.work] + list(self.donor_rows.values()):
                    for row in values:
                        if kind == "constant":
                            for field in ("o", "h", "l", "c", "vw"):
                                row[field] = "10"
                        else:
                            row["v" if kind == "volume" else "n"] = "0"
                self.assertEqual((), self.assess())
                self.work, self.donor_rows = saved_work, saved_donor

    def test_matching_middle_island_is_not_a_complete_source_prefix(self):
        for download, _, _ in self.work:
            download.rows.insert(0, {**download.rows[0], "t": "2024-08-13T04:00:00Z"})
        self.work = [(item, source, download_session_dates(item.rows)) for item, source, _ in self.work]
        self.assertEqual((), self.assess())

    def test_prefix_must_reach_the_handoff(self):
        for download, _, _ in self.work:
            download.rows[1]["t"] = "2024-08-13T04:00:00Z"
            download.rows.sort(key=lambda row: row["t"])
        self.work = [(item, source, download_session_dates(item.rows)) for item, source, _ in self.work]
        self.assertEqual((), self.assess())

    def test_earlier_asof_selection_retains_precedence(self):
        source = {**self.work[0][1], "requested_symbol": "NEXT"}
        result = correlate_download_rows(
            rows(),
            source,
            self.lineage,
            self.intervals,
            predecessor_prefixes=self.assess(),
            uuid_lineage_edges=self.edges,
        )
        self.assertTrue(all(segment["correlation_rule"]["order"] == 9 for segment in result["correlated"]))

    def test_matching_donors_with_different_owners_do_not_choose_by_order(self):
        edges = tuple(replace(edge, effective_date=date(2024, 8, 16)) for edge in self.edges)
        intervals = [
            replace(interval, end_date_exclusive=date(2024, 8, 16))
            if interval.historical_symbol == "OLD"
            else interval
            for interval in self.intervals
        ]

        def report(symbol):
            segments = copy.deepcopy(self.segments)
            if symbol == "OTHER":
                for segment in segments:
                    segment["canonical_security"] = {"alpaca_asset_id": "old2", "cusip": "old-cusip2"}
            return SimpleNamespace(correlated={"segments": segments}, sources_by_filename=self.descriptors)

        for ordered in (edges, tuple(reversed(edges))):
            self.assertEqual(
                (),
                assess_predecessor_prefixes(
                    "OLD",
                    self.lineage,
                    intervals,
                    ordered,
                    self.work,
                    correlation_loader=report,
                    source_rows_loader=lambda _symbol, filename: (self.donor_rows[filename], "generation"),
                ),
            )

    def test_shared_cascade_cannot_bootstrap_a_certificate_from_its_own_action(self):
        inputs = {}
        for symbol in ("OLD", "NEXT", "OTHER"):
            downloads, descriptors = {}, []
            for adjustment in ALPACA_ADJUSTMENT_MODES:
                filename = adjustment + ".csv.gz"
                manifest = {
                    "symbol": symbol,
                    "adjustment": adjustment,
                    "sha256": symbol + adjustment,
                    "asof": "2026-10-14",
                    "request_from": "2016-01-01",
                    "request_to": "2026-10-15",
                    "feed": "sip",
                    "timeframe": "1Day",
                    "rows": 3,
                }
                downloads[filename] = SimpleNamespace(
                    rows=rows(),
                    manifest=manifest,
                    path=Path(filename),
                    pipeline_state="source_published",
                    integrity_failure=None,
                )
                descriptors.append({"file": filename, "rows": 3})
            inputs[symbol] = SimpleNamespace(
                downloads=downloads,
                local={"directory": symbol, "downloads": descriptors, "present": True},
                reader=lambda _root, _directory, descriptor, items=downloads: items[descriptor["file"]],
            )
        reports = correlate(Path("."), inputs, {symbol: self.lineage for symbol in inputs})
        self.assertEqual(0, reports["OLD"]["correlated"]["row_count"])
        self.assertEqual(24, reports["OLD"]["uncorrelated"]["row_count"])
        self.assertEqual([], reports["OLD"]["predecessor_prefix_assessments"])
        self.assertEqual([], reports["NEXT"]["uuid_lineage_edges"])
        # An incremental open-month input is not the complete on-disk prefix.
        for descriptor in inputs["OLD"].local["downloads"]:
            descriptor["rows"] = 100
        reports = correlate(Path("."), inputs, {symbol: self.lineage for symbol in inputs})
        self.assertEqual(0, reports["OLD"]["correlated"]["row_count"])

    def reused_source(self):
        current = asset("reused", "OLD", "Unrelated new issuer")
        self.lineage["identities"].append(current)
        self.lineage.setdefault("asset_lookups", []).append(
            {
                "lookup": "OLD",
                "lookup_kind": "symbol",
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "asset": current,
            }
        )
        suffix = complete_market_session_rows("2026-10-01", "2026-10-07")
        return rows() + suffix

    def test_certified_prefix_survives_later_reuse_gap_with_unsupported_remainder(self):
        proofs = self.assess()
        source_rows = self.reused_source()
        before = correlate_download_rows(source_rows, self.work[0][1], self.lineage, self.intervals)
        self.assertEqual(3, before["uncorrelated"][0]["row_count"])
        after = correlate_download_rows(
            source_rows,
            self.work[0][1],
            self.lineage,
            self.intervals,
            predecessor_prefixes=proofs,
        )
        self.assertEqual([2, 4], [item["row_count"] for item in after["correlated"]])
        self.assertEqual(1, after["uncorrelated"][0]["row_count"])
        self.assertEqual(
            ["old1", "reused"], [item["canonical_security"]["alpaca_asset_id"] for item in after["correlated"]]
        )
        rival = replace(proofs[0], interval=replace(proofs[0].interval, asset_id="old2"))
        conflict = correlate_download_rows(
            source_rows,
            self.work[0][1],
            self.lineage,
            self.intervals,
            predecessor_prefixes=(*proofs, rival),
        )
        self.assertEqual(3, conflict["uncorrelated"][0]["row_count"])

    def test_metadata_only_reused_occupant_can_trigger_certification_screen(self):
        self.reused_source()
        self.lineage["identities"] = [item for item in self.lineage["identities"] if item["id"] != "old2"]
        self.lineage["symbol_occupancy_evidence"] = [
            item for item in self.lineage["symbol_occupancy_evidence"] if item["asset_id"] != "old2"
        ]
        self.intervals = [item for item in self.intervals if item.asset_id != "old2"]
        self.assertEqual(1, len(self.assess()))

    def test_certificate_does_not_release_gap_rows_to_conflicting_asof_owner(self):
        proofs = self.assess()
        source_rows = self.reused_source()
        rival = replace(
            proofs[0].interval, asset_id="rival", start_date_inclusive=date(2024, 8, 1), end_date_exclusive=None
        )
        after = correlate_download_rows(
            source_rows,
            self.work[0][1],
            self.lineage,
            [*self.intervals, rival],
            predecessor_prefixes=proofs,
        )
        self.assertEqual(3, after["uncorrelated"][0]["row_count"])


if __name__ == "__main__":
    unittest.main()
