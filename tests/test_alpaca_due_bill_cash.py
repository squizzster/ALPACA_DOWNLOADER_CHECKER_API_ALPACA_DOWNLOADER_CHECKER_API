"""A native special distribution can explain trading beyond a reported cash merger boundary."""

from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_former_ticker import lookup
from test_alpaca_lineage_build import IdentityClient, informative_bars
from test_alpaca_name_change_evidence import PRODUCTION_POLICY, bars, inputs_for, raw_segments, rename
from test_alpaca_pricing_download import FakeClient

from libs.alpaca_end_of_day_security_correlation import due_bill_cash as cash
from libs.alpaca_lineage_build import pipeline, policy_engine, storage
from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.download import download_pricing
from libs.common.us_equity_market_session_calendar import market_sessions_between


def case(symbol="MAPLE", replacement="RIGHTS", amount="8.75", *, last_observed="2024-01-16"):
    evidence = rename(old=symbol, new=replacement, when="2024-01-17")
    for asset in evidence["identities"]:
        asset.update(status="inactive", tradable=False)
        evidence["asset_lookups"].append(lookup(asset["lookup_cusips"][0], "cusip", asset))
    evidence["query"] = {
        "input_symbol": symbol,
        "corporate_actions_data_quality": "complete",
        "corporate_actions_start": "2024-01-01",
        "corporate_actions_end": "2024-01-31",
    }
    evidence["corporate_actions"].extend(
        (
            {
                "id": "cash",
                "action_type": "cash_merger",
                "acquiree_symbol": symbol,
                "acquiree_cusip": "old-cusip",
                "effective_date": "2024-01-08",
                "process_date": "2024-01-08",
                "rate": amount,
            },
            {
                "id": "special",
                "action_type": "cash_dividend",
                "symbol": symbol,
                "cusip": "old-cusip",
                "rate": amount,
                "special": True,
                "record_date": "2024-01-03",
                "payable_date": "2024-01-05",
                "process_date": "2024-01-05",
                "due_bill_on_date": "2024-01-03",
                "due_bill_off_date": "2024-01-08",
                "ex_date": "2024-01-08",
            },
        )
    )
    rows = bars(market_sessions_between("2024-01-02", last_observed))
    views = inputs_for({symbol: rows}, asofs={symbol: "2024-01-31"})
    view = views[symbol]
    view.materialization_scope = "complete_history"
    for download in view.downloads.values():
        download.manifest["request_to"] = "2024-01-31"
    view.source["latest_request"]["end_exclusive"] = "2024-01-31"
    return evidence, views


# An isolated evaluator selects the two executable versions through the same cache factory.
EARLIER = replace(PRODUCTION_POLICY, implementation_correlation_version=105)
CANDIDATE = replace(PRODUCTION_POLICY, implementation_correlation_version=106)


def evaluate(evidence, views, policy=CANDIDATE):
    symbol = evidence["query"]["input_symbol"]
    return correlate(Path("."), views, {symbol: evidence}, policy=policy)[symbol]


class DueBillCashTests(TestCase):
    def test_same_old_equity_owns_only_traded_dates_between_distribution_and_replacement(self):
        for old, new, amount in (("MAPLE", "RIGHTS", "8.75"), ("FEN", "ESCROW", 0.625)):
            with self.subTest(old=old):
                evidence, views = case(old, new, amount)
                original = deepcopy(evidence["corporate_actions"])
                prior = evaluate(evidence, views, EARLIER)
                with (
                    patch.object(cash, "due_bill_cash_terms", wraps=cash.due_bill_cash_terms) as resolve,
                    patch.object(cash, "positive_activity", wraps=cash.positive_activity) as active,
                ):
                    result = evaluate(evidence, views)
                    evaluate(evidence, views)
                resolve.assert_called_once()
                self.assertEqual(active.call_count, 6)
                self.assertEqual(original, evidence["corporate_actions"])
                self.assertEqual(0, result["uncorrelated"]["row_count"])
                self.assertEqual(6 * len(ADJUSTMENTS), prior["uncorrelated"]["row_count"])
                for adjustment in ADJUSTMENTS:
                    selected = [
                        seg for seg in result["correlated"]["segments"] if seg["adjustment"] == adjustment
                    ]
                    previous = [
                        seg for seg in prior["correlated"]["segments"] if seg["adjustment"] == adjustment
                    ]
                    self.assertEqual(previous, selected[:-1])
                    advanced = selected[-1]
                    self.assertEqual(
                        (advanced["first_session_date"], advanced["last_session_date"]),
                        ("2024-01-08", "2024-01-16"),
                    )
                    self.assertEqual(cash.METHOD, advanced["correlation_method"])
                    self.assertEqual(["old-id"], advanced["security_identity"]["member_alpaca_asset_ids"])
                    self.assertEqual(advanced["historical_symbol"], old)
                    self.assertEqual(
                        set(advanced["ticker_ownership_interval"]["supporting_corporate_action_ids"]),
                        {"cash", "special", "transition"},
                    )

    def test_special_payment_schedule_remains_evidence_when_due_bill_fields_are_missing(self):
        for missing in (
            ("due_bill_on_date",),
            ("due_bill_off_date",),
            ("due_bill_on_date", "due_bill_off_date"),
        ):
            evidence, _ = case()
            special = evidence["corporate_actions"][-1]
            for key in missing:
                del special[key]
            with self.subTest(missing=missing):
                terms = cash.due_bill_cash_terms(evidence, "MAPLE")
                self.assertEqual(terms.timing_basis, "special_distribution_payable_before_ex_date")
                self.assertNotEqual(terms.asset_id, terms.replacement_asset_id)
                special["payable_date"] = "2024-01-08"
                self.assertIsNone(cash.due_bill_cash_terms(evidence, "MAPLE"))

    def test_ordinary_merger_competing_endpoints_and_conflicting_dates_preserve_baseline(self):
        for problem in (
            "ordinary cash",
            "different rate",
            "different ex date",
            "different off date",
            "different old CUSIP",
            "same replacement UUID",
            "same replacement label",
            "missing new lookup",
            "different old ticker lookup",
            "contrary departure dates",
            "visible rival",
            "unresolved rival",
            "contrary event",
            "late action coverage",
            "two special distributions",
            "amount not finite",
            "same issue after name change",
            "foreign cash currency",
        ):
            with self.subTest(problem=problem):
                evidence, views = case()
                departure, merger, special = evidence["corporate_actions"]
                if problem == "ordinary cash":
                    evidence["corporate_actions"].remove(special)
                elif problem == "different rate":
                    special["rate"] = "50"
                elif problem == "different ex date":
                    special["ex_date"] = "2024-01-09"
                elif problem == "different off date":
                    special["due_bill_off_date"] = "2024-01-11"
                elif problem == "different old CUSIP":
                    departure["old_cusip"] = "another-cusip"
                elif problem == "same replacement UUID":
                    replacement = next(
                        item for item in evidence["asset_lookups"] if item["lookup"] == "new-cusip"
                    )
                    replacement["asset"] = {**replacement["asset"], "id": "old-id"}
                elif problem == "same replacement label":
                    departure["new_symbol"] = "MAPLE"
                elif problem == "missing new lookup":
                    evidence["asset_lookups"] = [
                        item for item in evidence["asset_lookups"] if item["lookup"] != "new-cusip"
                    ]
                elif problem == "different old ticker lookup":
                    original = next(item for item in evidence["asset_lookups"] if item["lookup"] == "MAPLE")
                    original["asset"] = {**original["asset"], "id": "other-id"}
                elif problem == "contrary departure dates":
                    departure["effective_date"] = "2024-01-19"
                elif problem in {"visible rival", "unresolved rival"}:
                    evidence["symbol_occupancy_evidence"].append(
                        {
                            "symbol": "MAPLE",
                            "cusip": "rival-cusip",
                            "asset_id": "rival-id" if problem == "visible rival" else None,
                            "evidence": [
                                {"id": "rival", "action_type": "cash_dividend", "process_date": "2024-01-10"}
                            ],
                        }
                    )
                elif problem == "contrary event":
                    evidence["corporate_actions"].append(
                        {
                            "id": "removal",
                            "action_type": "worthless_removal",
                            "symbol": "MAPLE",
                            "process_date": "2024-01-12",
                        }
                    )
                elif problem == "late action coverage":
                    evidence["query"]["corporate_actions_end"] = "2024-01-16"
                elif problem == "two special distributions":
                    evidence["corporate_actions"].append({**special, "id": "second-special", "rate": "20"})
                elif problem == "amount not finite":
                    special["rate"] = "NaN"
                elif problem == "same issue after name change":
                    departure["new_cusip"] = departure["old_cusip"]
                elif problem == "foreign cash currency":
                    merger["currency"], special["currency"] = "USD", "EUR"
                self.assertIsNone(cash.due_bill_cash_terms(evidence, "MAPLE"))
                with patch.object(
                    cash, "positive_activity", side_effect=AssertionError("read an inapplicable session")
                ):
                    prior = evaluate(evidence, views, EARLIER)
                    result = evaluate(evidence, views)
                for branch in ("correlated", "uncorrelated"):
                    self.assertEqual(prior[branch], result[branch])

    def test_inert_and_missing_sessions_do_not_gain_ownership_or_interpolate_dates(self):
        evidence, views = case()
        for download in views["MAPLE"].downloads.values():
            for row in download.rows:
                day = row["t"][:10]
                if day in ("2024-01-09", "2024-01-16"):
                    row.update(v="0", n="0")
                if day == "2024-01-11":
                    row.update(n="0")
        result = evaluate(evidence, views)
        gained = [part for part in raw_segments(result) if part["correlation_method"] == cash.METHOD]
        self.assertEqual(
            [(p["first_session_date"], p["last_session_date"]) for p in gained],
            [("2024-01-08", "2024-01-08"), ("2024-01-10", "2024-01-10"), ("2024-01-12", "2024-01-12")],
        )
        self.assertEqual(3 * len(ADJUSTMENTS), result["uncorrelated"]["row_count"])

        for mode in ("inert", "only merger date traded"):
            with self.subTest(mode=mode):
                evidence, views = case()
                for download in views["MAPLE"].downloads.values():
                    for row in download.rows:
                        if row["t"][:10] > "2024-01-08" or mode == "inert":
                            row.update(v="0", n="0")
                result = evaluate(evidence, views)
                self.assertFalse(any(seg["correlation_method"] == cash.METHOD for seg in raw_segments(result)))

    def test_activity_is_examined_once_on_only_bounded_raw_rows_with_their_own_route(self):
        evidence, views = case()
        original = cash.traded_intervals
        reads = []

        class ScopedRaw:
            def __init__(self, raw):
                self.raw = raw

            def __iter__(self):
                raise AssertionError("traversed retained source")

            def __getitem__(self, i):
                row = self.raw[i]
                day = row["t"][:10]
                if not "2024-01-08" <= day < "2024-01-17" or day in reads:
                    raise AssertionError(f"unexpected raw row {day}")
                reads.append(day)
                return {"v": row["v"], "n": row["n"]}

        class Forbidden:
            @property
            def rows(self):
                raise AssertionError("reopened historical adjusted prices")

        def selected(symbol, lineage, intervals, edges, items, consensus, **kwargs):
            held = [
                (
                    SimpleNamespace(rows=ScopedRaw(download.rows))
                    if source["adjustment"] == "raw"
                    else Forbidden(),
                    source,
                    dates,
                )
                for download, source, dates in items
            ]
            return original(symbol, lineage, intervals, edges, held, consensus, **kwargs)

        with patch.object(cash, "traded_intervals", side_effect=selected) as owner:
            evaluate(evidence, views)
            evaluate(evidence, views)
        self.assertEqual(reads, list(market_sessions_between("2024-01-08", "2024-01-16")))
        owner.assert_called_once()

        for change in (
            "older mapping",
            "different view mapping",
            "OTC feed",
            "accepted rows",
            "partial materialization",
            "one row grid differs",
        ):
            with self.subTest(change=change):
                evidence, views = case()
                view = views["MAPLE"]
                if change == "older mapping":
                    for download in view.downloads.values():
                        download.manifest["asof"] = "2023-12-31"
                elif change == "different view mapping":
                    view.downloads["split.csv.gz"].manifest["asof"] = "2023-12-31"
                elif change == "OTC feed":
                    for download in view.downloads.values():
                        download.manifest["feed"] = "otc"
                elif change == "partial materialization":
                    view.materialization_scope = "retained_suffix"
                elif change == "accepted rows":
                    # The group cache's decision does not inspect an already accepted body.
                    view.accepted_report = {"unchanged": True}
                    with patch.object(
                        cash, "due_bill_cash_terms", side_effect=AssertionError("old decision reopened")
                    ):
                        cache = policy_engine._selected_group_cache(
                            Path("."),
                            CANDIDATE,
                            views,
                            {"MAPLE": evidence},
                            selected_group=["MAPLE"],
                            unavailable_pricing=[],
                        )
                        self.assertFalse(cache.cash_due_bill_continuations)
                    continue
                else:
                    view.downloads["split.csv.gz"].rows.pop()
                with patch.object(
                    cash, "positive_activity", side_effect=AssertionError("examined inapplicable prices")
                ):
                    result = evaluate(evidence, views)
                self.assertFalse(any(seg["correlation_method"] == cash.METHOD for seg in raw_segments(result)))

    def test_later_replacement_never_owns_or_absorbs_prior_trades(self):
        evidence, views = case(last_observed="2024-01-19")
        report = evaluate(evidence, views)
        frozen = evaluate(evidence, views, EARLIER)
        for branch in ("correlated", "uncorrelated"):

            def after(value):
                return [
                    (seg["last_session_date"], seg.get("security_identity"), seg.get("reason_codes"))
                    for seg in raw_segments(value, branch)
                    if seg["last_session_date"] >= "2024-01-17"
                ]

            self.assertEqual(after(report), after(frozen))
        self.assertEqual(
            len([seg for seg in raw_segments(report) if seg["correlation_method"] == cash.METHOD]), 1
        )
        self.assertFalse(
            any(
                set(seg["security_identity"]["member_alpaca_asset_ids"]) >= {"old-id", "new-id"}
                for seg in raw_segments(report)
            )
        )

    def test_completed_selected_assessment_survives_peer_interruption_and_checks_new_conflicts(self):
        evidence, views = case()
        auxiliary = inputs_for({"ZED": bars(("2024-01-04", "2024-01-05"))})
        auxiliary["ZED"].materialization_scope = "complete_history"
        views.update(auxiliary)
        peer = {
            "query": {**evidence["query"], "input_symbol": "ZED"},
            "corporate_actions": [],
            "asset_lookups": [],
            "identities": [],
        }
        held = {"MAPLE": evidence, "ZED": peer}
        build = policy_engine.build_security_correlation_report

        def interrupt(report, *args, **kwargs):
            if report["symbol"] == "ZED":
                raise RuntimeError("unrelated peer interrupted")
            return build(report, *args, **kwargs)

        def run():
            return correlate(Path("."), views, held, policy=CANDIDATE)

        with patch.object(cash, "positive_activity", wraps=cash.positive_activity) as active:
            with (
                patch.object(policy_engine, "build_security_correlation_report", side_effect=interrupt),
                self.assertRaisesRegex(RuntimeError, "peer interrupted"),
            ):
                run()
            self.assertEqual(active.call_count, 6)
            complete = run()
            self.assertEqual(active.call_count, 6)
        self.assertEqual(complete["MAPLE"]["uncorrelated"]["row_count"], 0)
        held["ZED"] = {
            **peer,
            "asset_lookups": [
                lookup("MAPLE", "symbol", {"id": "competing", "class": "us_equity", "symbol": "MAPLE"})
            ],
        }
        with patch.object(
            cash, "positive_activity", side_effect=AssertionError("read beyond changed native scope")
        ):
            report = run()["MAPLE"]
        self.assertEqual(report["uncorrelated"]["row_count"], 6 * len(ADJUSTMENTS))
        held["ZED"] = peer
        views["MAPLE"].source_sha256 = "changed-pricing-generation"
        raw = views["MAPLE"].downloads["raw.csv.gz"]
        for row in raw.rows:
            if row["t"][:10] >= "2024-01-08":
                row.update(v="0", n="0")
        with patch.object(cash, "positive_activity", wraps=cash.positive_activity) as active:
            report = run()["MAPLE"]
        self.assertEqual(active.call_count, 6)
        self.assertFalse(any(seg["correlation_method"] == cash.METHOD for seg in raw_segments(report)))

    def test_cold_pipeline_admits_native_post_payment_rows_and_reuses_accepted_generation(self):
        symbol, replacement = "FENCORP", "FENNOTE"
        evidence, _ = case(symbol, replacement, "0.65")

        class Prices(FakeClient):
            def fetch(self, label, adjustment, start, end, *, asof=None):
                if label == replacement:
                    return []
                return super().fetch(label, adjustment, start, end, asof=asof)

        for enabled in (False, True):
            with self.subTest(enabled=enabled), TemporaryDirectory() as temporary:
                store = Path(temporary) / "store"
                prices = Prices(informative_bars("2024-01-02", "2024-01-17"))
                identity = IdentityClient(
                    {item["lookup"]: item["asset"] for item in evidence["asset_lookups"]},
                    evidence["corporate_actions"],
                )
                download_pricing(store, symbol, "2024-01-31", client=prices)
                # Exercise both candidate choices on the identical provider history through ordinary group acquisition.
                threshold = PRODUCTION_POLICY.implementation_correlation_version + int(not enabled)
                with patch.object(policy_engine, "DUE_BILL_CASH_VERSION", threshold):

                    def run():
                        report = pipeline.run_lineage(
                            store, [symbol], identity_client=identity, pricing_client=prices
                        )
                        self.assertEqual(report["failed"], 0, report)
                        return report["results"][0]

                    first = run()
                    self.assertEqual(first["unresolved_rows"], 0 if enabled else 6 * len(ADJUSTMENTS))
                    lineage = store / "end_of_day/lineage"
                    index = storage.read_index(lineage)
                    receipt = storage.read_receipt(lineage, symbol, index)
                    self.assertEqual(
                        {a["id"] for a in receipt["evidence"]["corporate_actions"]},
                        {"transition", "special", "cash"},
                    )
                    calls = (identity.request_count, len(prices.calls))
                    with (
                        patch.object(
                            cash, "due_bill_cash_terms", side_effect=AssertionError("repeated native decision")
                        ),
                        patch.object(
                            pipeline, "load_pricing", side_effect=AssertionError("read accepted prices")
                        ),
                        patch.object(
                            pipeline, "correlate", side_effect=AssertionError("replayed accepted rows")
                        ),
                    ):
                        repeated = run()
                    self.assertTrue(repeated["reused_decision"])
                    self.assertEqual(calls, (identity.request_count, len(prices.calls)))
                    self.assertEqual(
                        index["symbols"][symbol]["sha256"],
                        storage.read_index(lineage)["symbols"][symbol]["sha256"],
                    )

    def test_true_cash_merger_with_final_inert_bar_preserves_the_original_decision(self):
        evidence, views = case(last_observed="2024-01-08")
        _, merger, _ = evidence["corporate_actions"]
        evidence["corporate_actions"] = [merger]
        evidence["symbol_occupancy_evidence"] = []
        evidence["identities"] = [item for item in evidence["identities"] if item["id"] == "old-id"]
        evidence["asset_lookups"] = [
            item for item in evidence["asset_lookups"] if item["asset"]["id"] == "old-id"
        ]
        for download in views["MAPLE"].downloads.values():
            close = download.rows[-2]["c"]
            download.rows[-1].update(o=close, h=close, l=close, c=close, v=0, n=0, vw=0)
        frozen = evaluate(evidence, views, EARLIER)
        with (
            patch.object(cash, "due_bill_cash_terms", side_effect=AssertionError("no independent departure")),
            patch.object(cash, "positive_activity", side_effect=AssertionError("cash payout is not a print")),
        ):
            updated = evaluate(evidence, views)
        for branch in ("correlated", "uncorrelated"):
            self.assertEqual(updated[branch], frozen[branch])

    def test_conflicting_replacement_lookup_in_peer_vetoes_the_observation(self):
        evidence, _ = case()
        wrong = {
            "id": "old-id",
            "symbol": "RIGHTS",
            "class": "us_equity",
            "status": "inactive",
            "exchange": "NYSE",
        }
        peer = {"asset_lookups": [lookup("new-cusip", "cusip", wrong)], "corporate_actions": []}
        self.assertIsNone(cash.due_bill_cash_terms(evidence, "MAPLE", peers=(peer,)))
        peer["asset_lookups"] = [lookup("new-cusip", "cusip", None)]
        self.assertIsNotNone(cash.due_bill_cash_terms(evidence, "MAPLE", peers=(peer,)))
