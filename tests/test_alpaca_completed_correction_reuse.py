"""Published and unchanged-price corrections retain their original recovery scope."""

import copy
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from test_alpaca_durable_maintenance import ProcessInterrupted, fixture
from test_alpaca_lineage_build import asset
from test_alpaca_pricing_download import FakeClient, bar
from test_alpaca_universe_update import CatalogueClient
from test_alpaca_update_obligation_recovery import catalogue, source

from libs.alpaca_cli import build_parser, dispatch
from libs.alpaca_lineage_build import storage, universe_state
from libs.alpaca_lineage_build.completed_corrections import applicable
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.dependency_suffixes import accept_empty_suffixes
from libs.alpaca_lineage_build.registration import command_policy
from libs.alpaca_pricing_download import append as pricing_append
from libs.alpaca_pricing_download import correction, refresh
from libs.alpaca_pricing_download.acquire import acquire_selected
from libs.alpaca_pricing_download.contract import VALIDATOR_FILE, timestamp
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_serving_catalogue.producer import maintain


class CompletedCorrectionReuseTests(unittest.TestCase):
    def interrupt_changed_correction(self, case, *, before_lineage=False):
        catalogue(case, asset("AAA"))
        case.live["AAA"] = [bar("2016-01-04", "50"), bar("2026-10-16", "50")]
        boundary = (
            "libs.alpaca_lineage_build.refresh.run_lineage"
            if before_lineage
            else "libs.alpaca_lineage_build.universe_state.record_outcome"
        )
        with patch(boundary, side_effect=ProcessInterrupted), self.assertRaises(ProcessInterrupted):
            case.update()
        work = universe_state.read_state(case.store)["work"]["AAA"]
        self.assertIsNone(work["correction_completion"])
        self.assertEqual(work["correction_id"], source(case)["history_correction_id"])
        case.pages.clear()
        return work["correction_id"]

    def test_same_cutoff_uses_published_correction_once_without_pricing_or_lineage_replay(self):
        with fixture() as case:
            correction_id = self.interrupt_changed_correction(case)
            self.assertTrue(maintain(case.store)["complete"])
            reads = []
            read = Path.read_bytes

            def record_read(path):
                if path.name == "source.manifest.json":
                    reads.append(path)
                if path.suffix == ".gz" or path.name == VALIDATOR_FILE:
                    self.fail(f"accepted price history reopened: {path}")
                return read(path)

            with (
                patch.object(Path, "read_bytes", record_read),
                patch.object(storage, "publish", side_effect=AssertionError("repeated lineage publication")),
                patch.object(case.pricing, "_get", side_effect=AssertionError("repeated provider request")),
            ):
                retry = case.update(retry_blocked=True)
            self.assertTrue(retry["complete"], retry)
            self.assertEqual(reads, [case.store / "end_of_day/pricing/AAA/source.manifest.json"])
            self.assertEqual(source(case)["history_correction_id"], correction_id)

    def test_later_cutoff_checks_published_correction_then_advances_selected_history(self):
        for changed in (True, False):
            for before_lineage in (True, False):
                with self.subTest(changed=changed, before_lineage=before_lineage), fixture() as case:
                    correction_id = self.interrupt_changed_correction(case, before_lineage=before_lineage)
                    first_close = "55" if changed else "50"
                    case.live["AAA"][0] = bar("2016-01-04", first_close)
                    case.live["AAA"].append(bar("2026-10-19", "60"))
                    result = case.update("2026-10-20", retry_blocked=True)
                    self.assertTrue(result["complete"], result)
                    expected = [
                        ("all", "2016-01-04", "2016-01-05"),
                        ("all", "2026-10-16", "2026-10-16"),
                        ("raw", "2026-10-17", "2026-10-19"),
                        ("all", "2026-10-17", "2026-10-19"),
                    ]
                    if changed:
                        expected.extend(
                            [
                                ("raw", "2016-01-01", "2026-10-16"),
                                ("all", "2016-01-01", "2026-10-16"),
                                ("raw", "2016-01-04", "2016-01-05"),
                            ]
                        )
                    self.assertCountEqual(
                        [(p["adjustment"], p["start"][:10], p["end"][:10]) for p in case.pages], expected
                    )
                    self.assertEqual(source(case)["history_correction_id"] != correction_id, changed)
                    for rows in read_pricing_views(case.store, "AAA").values():
                        self.assertEqual([row[4] for row in rows], [first_close, "50", "60"])

    def test_interrupted_new_difference_keeps_its_own_correction_id_and_comparison(self):
        with fixture() as case:
            old_id = self.interrupt_changed_correction(case)
            case.live["AAA"][0] = bar("2016-01-04", "55")
            case.live["AAA"].append(bar("2026-10-19", "60"))
            with (
                patch("libs.alpaca_lineage_build.refresh.replace_prepared", side_effect=ProcessInterrupted),
                self.assertRaises(ProcessInterrupted),
            ):
                case.update("2026-10-20", retry_blocked=True)
            pending = universe_state.read_state(case.store)["work"]["AAA"]
            self.assertNotEqual(pending["correction_id"], old_id)
            self.assertEqual(source(case)["history_correction_id"], old_id)
            case.pages.clear()
            result = case.update("2026-10-20", retry_blocked=True)
            self.assertTrue(result["complete"], result)
            self.assertFalse(
                any(
                    p["adjustment"] == "all" and p["start"][:10] in ("2016-01-04", "2026-10-16")
                    for p in case.pages
                ),
                case.pages,
            )
            self.assertEqual(source(case)["history_correction_id"], pending["correction_id"])
            self.assertEqual(
                [row[4] for row in read_pricing_views(case.store, "AAA")["all"]], ["55", "50", "60"]
            )

    def test_replacement_published_before_append_only_approves_its_selected_advance(self):
        for change in ("none", "later_cutoff", "different_generation", "different_mapping"):
            with self.subTest(change=change), fixture() as case:
                self.interrupt_changed_correction(case)
                case.live["AAA"][0] = bar("2016-01-04", "55")
                case.live["AAA"].append(bar("2026-10-19", "60"))
                with (
                    patch("libs.alpaca_lineage_build.refresh.run_lineage", side_effect=ProcessInterrupted),
                    self.assertRaises(ProcessInterrupted),
                ):
                    case.update("2026-10-20", retry_blocked=True)
                pending = universe_state.read_state(case.store)["work"]["AAA"]
                contract = pending["pricing_correction"]
                self.assertEqual(contract["advance_end_exclusive"], "2026-10-20")
                self.assertEqual(contract["comparison_end_exclusive"][:10], "2026-10-17")
                self.assertEqual(pending["correction_id"], source(case)["history_correction_id"])
                cutoff = "2026-10-20"
                if change == "later_cutoff":
                    cutoff = "2026-10-21"
                elif change in ("different_generation", "different_mapping"):
                    download_pricing(
                        case.store,
                        "AAA",
                        "2026-10-17",
                        client=FakeClient([bar("2016-01-04", "56"), case.live["AAA"][1]]),
                        refresh_all=True,
                        mapping_asof="2026-10-15" if change == "different_mapping" else None,
                    )
                    self.assertEqual(pending["correction_id"], source(case)["history_correction_id"])
                if change != "none":
                    case.live["AAA"][0] = bar("2016-01-04", "57")
                case.pages.clear()
                resumed = case.update(cutoff, retry_blocked=True)
                self.assertTrue(resumed["complete"], resumed)
                if change == "none":
                    self.assertCountEqual(
                        [(p["adjustment"], p["start"][:10], p["end"][:10]) for p in case.pages],
                        [("raw", "2026-10-17", "2026-10-19"), ("all", "2026-10-17", "2026-10-19")],
                    )
                else:
                    self.assertEqual(
                        sum(p["adjustment"] == "all" and p["start"][:10] == "2016-01-04" for p in case.pages),
                        1,
                        case.pages,
                    )
                self.assertEqual(
                    [row[4] for row in read_pricing_views(case.store, "AAA")["all"]],
                    ["55" if change == "none" else "57", "50", "60"],
                )

    def test_inactive_retry_compares_history_only_for_independently_required_advance(self):
        for advance in (False, True):
            with self.subTest(advance=advance), fixture() as case:
                self.interrupt_changed_correction(case)
                catalogue(case, {**asset("AAA"), "status": "inactive"})
                if advance:
                    policy = command_policy(
                        case.store, "2026-10-20", retry_blocked=True, context=OperationContext(case.store)
                    )
                    policy.require(
                        "AAA", "2026-10-20", reason="advance_retained_coverage", acquire=True, history_check=True
                    )
                case.live["AAA"][0] = bar("2016-01-04", "55")
                case.live["AAA"].append(bar("2026-10-19", "60"))
                result = case.update("2026-10-20", retry_blocked=True)
                self.assertTrue(result["complete"], result)
                rows = read_pricing_views(case.store, "AAA")["all"]
                if advance:
                    self.assertEqual([row[4] for row in rows], ["55", "50", "60"])
                    self.assertEqual(
                        sum(p["adjustment"] == "all" and p["start"][:10] == "2016-01-04" for p in case.pages), 1
                    )
                else:
                    self.assertEqual([row[4] for row in rows], ["50", "50"])
                    self.assertEqual(case.pages, [])

    def test_inactive_correction_finishes_its_selected_coverage_with_the_completed_suffix(self):
        target = "2026-10-20"
        for observed_empty, focused in ((True, True), (True, False), (False, False)):
            with self.subTest(observed_empty=observed_empty, focused=focused), fixture() as case:
                catalogue(case, asset("AAA"))
                case.live["AAA"] = [bar("2016-01-04", "55"), case.old[-1]]
                with (
                    patch("libs.alpaca_lineage_build.refresh.run_lineage", side_effect=ProcessInterrupted),
                    self.assertRaises(ProcessInterrupted),
                ):
                    case.update(target)
                policy = command_policy(case.store, target, context=OperationContext(case.store))
                current = policy.work["AAA"]
                self.assertEqual(current["pricing_correction"]["advance_end_exclusive"], target)
                self.assertEqual(source(case)["history_correction_id"], current["correction_id"])
                if observed_empty:
                    probed = acquire_selected(
                        case.store,
                        ["AAA"],
                        target,
                        client=case.pricing,
                        select_append_work=True,
                        on_empty_suffixes=lambda rows: accept_empty_suffixes(policy, {"AAA": {"AAA"}}, rows),
                    )
                    self.assertEqual(probed["results"][0]["status"], "not_selected")
                else:
                    case.live["AAA"].append(bar("2026-10-19", "60"))
                catalogue(case, {**asset("AAA"), "status": "inactive"})
                case.pages.clear()
                page = case.pricing._get

                def permitted(params):
                    if observed_empty or params["start"][:10] < "2026-10-17":
                        self.fail(f"repeated completed pricing work: {params}")
                    return page(params)

                with (
                    patch.object(case.pricing, "_get", side_effect=permitted),
                    patch(
                        "libs.alpaca_pricing_download.acquire.acquire_retained",
                        side_effect=AssertionError("completed retained comparison"),
                    ),
                ):
                    if focused:
                        args = build_parser().parse_args(
                            [
                                "lineage",
                                "update",
                                "--symbol",
                                "AAA",
                                "--store",
                                str(case.store),
                                "--through",
                                "2026-10-19",
                            ]
                        )
                        with (
                            patch("libs.alpaca_cli.clients", return_value=(case.pricing, case.identity)),
                            patch("libs.alpaca_cli.DEFAULT_REGISTRY", case.registry),
                            patch.object(
                                pricing_append,
                                "load_published_boundary",
                                side_effect=AssertionError("empty same-month append reopened accepted prices"),
                            ),
                            patch.object(
                                pricing_append,
                                "validate_entry",
                                side_effect=AssertionError("published endpoints were numerically revalidated"),
                            ),
                        ):
                            append = pricing_append.publish_observed_empty_append

                            def after_publication(*args, **kwargs):
                                append(*args, **kwargs)
                                raise ProcessInterrupted("after advancing the corrected pricing generation")

                            with (
                                patch.object(pricing_append, "publish_observed_empty_append", after_publication),
                                self.assertRaises(ProcessInterrupted),
                            ):
                                dispatch(args)
                            resumed = dispatch(args)
                    else:
                        resumed = case.update(target, retry_blocked=True)
                self.assertTrue(resumed["complete"], resumed)
                self.assertFalse(resumed["pending_work"])
                installed = source(case)
                self.assertEqual(installed["latest_request"]["end_exclusive"][:10], target)
                self.assertEqual(installed["history_correction_id"], current["correction_id"])
                entry = storage.read_index(case.store / "end_of_day/lineage")["symbols"]["AAA"]
                self.assertEqual(entry["projection"]["processed_through"][:10], target)
                self.assertEqual(universe_state.read_state(case.store)["work"]["AAA"]["status"], "complete")
                if focused:
                    for change in ("mapping", "target", "ancestor", "changed_prefix"):
                        with self.subTest(stale=change):
                            other = copy.deepcopy(installed)
                            if change == "mapping":
                                other["latest_request"]["asof"] = "2026-10-16"
                            elif change == "target":
                                other["latest_request"]["end_exclusive"] = "2026-10-21T00:00:00Z"
                            elif change == "ancestor":
                                other["history_changes"][1]["source_sha256"] = "b" * 64
                            else:
                                other["history_changes"][0]["changed_from"] = "2016-01-04"
                            policy.history_approved.clear()
                            policy.recover_continued_advances(
                                {"AAA": other}, {"AAA": storage.document_digest(other)}
                            )
                            self.assertNotIn("AAA", policy.history_approved)

    def test_source_replacement_invalidates_the_selected_non_summary_advance(self):
        target = "2026-10-20"
        for mapping in (None, "2026-10-15"):
            with self.subTest(mapping=mapping), fixture() as case:
                catalogue(case, asset("AAA"))
                case.live["AAA"] = [bar("2016-01-04", "55"), case.old[-1]]
                with (
                    patch("libs.alpaca_lineage_build.refresh.run_lineage", side_effect=ProcessInterrupted),
                    self.assertRaises(ProcessInterrupted),
                ):
                    case.update(target)
                pending = universe_state.read_state(case.store)["work"]["AAA"]
                self.assertEqual(pending["pricing_correction"]["advance_end_exclusive"], target)
                download_pricing(
                    case.store,
                    "AAA",
                    "2026-10-17",
                    client=FakeClient([bar("2016-01-04", "56"), case.old[-1]]),
                    refresh_all=True,
                    mapping_asof=mapping,
                )
                self.assertEqual(source(case)["history_correction_id"], pending["correction_id"])
                catalogue(case, {**asset("AAA"), "status": "inactive"})
                case.live["AAA"][0] = bar("2016-01-04", "57")
                case.live["AAA"].append(bar("2026-10-19", "60"))
                case.pages.clear()
                result = case.update(target, retry_blocked=True)
                self.assertTrue(result["complete"], result)
                self.assertEqual(
                    sum(p["adjustment"] == "all" and p["start"][:10] == "2016-01-04" for p in case.pages),
                    1,
                    case.pages,
                )
                self.assertEqual(
                    [row[4] for row in read_pricing_views(case.store, "AAA")["all"]],
                    ["57", str(case.old[-1]["c"]), "60"],
                )

    def test_completed_old_correction_does_not_hide_a_change_when_advancing(self):
        with fixture() as case:
            case.records[:] = [asset("AAA")]
            case.identity = CatalogueClient(case.records)
            page = case.pricing._get

            def context_difference(params):
                values = copy.deepcopy(page(params))
                if params["adjustment"] == "all" and timestamp(params["end"]) - timestamp(
                    params["start"]
                ) < timedelta(days=2):
                    for rows in values["bars"].values():
                        for row in rows:
                            if row["t"] == case.old[0]["t"]:
                                row["v"] = 12345
                return values

            with patch.object(case.pricing, "_get", side_effect=context_difference):
                self.assertTrue(case.update("2026-10-17")["complete"])
            prior = universe_state.read_state(case.store)["work"]["AAA"]
            self.assertTrue(prior["correction_completion"])
            case.live["AAA"] = [case.old[0], {**case.old[-1], "v": 98765}, bar("2026-10-19")]
            acquire = refresh.acquire_full

            def localized(needs, *args, **kwargs):
                self.assertTrue(
                    all(window["start"][:10] >= "2026-10-16" for need in needs for window in need["ranges"]),
                    "latest correction widened to completed history",
                )
                return acquire(needs, *args, **kwargs)

            with patch.object(refresh, "acquire_full", side_effect=localized):
                advanced = case.update("2026-10-20")
            self.assertTrue(advanced["complete"], advanced)
            for rows in read_pricing_views(case.store, "AAA").values():
                self.assertEqual(rows[-2][5], "98765", "old correction hid a new selected-row change")
                self.assertEqual(rows[-1][0][:10], "2026-10-19")

    def test_later_cutoff_compares_after_equal_price_receipt_preceded_workflow_acknowledgement(self):
        with fixture() as case:
            catalogue(case, asset("AAA"))
            policy = command_policy(case.store, "2026-10-17", context=OperationContext(case.store))
            policy.require_correction(refresh.prepare_refresh(case.store, ["AAA"]))
            old_id = policy.work["AAA"]["correction_id"]
            with (
                patch.object(universe_state, "accept_correction_completion", side_effect=ProcessInterrupted),
                self.assertRaises(ProcessInterrupted),
            ):
                case.update()
            pending = universe_state.read_state(case.store)["work"]["AAA"]
            self.assertIsNone(pending["correction_completion"])
            self.assertTrue((case.store / "end_of_day/pricing/AAA/correction.complete.json").is_file())
            case.live["AAA"] = [bar("2016-01-04", "55"), case.old[-1], bar("2026-10-19", "60")]
            case.pages.clear()
            result = case.update("2026-10-20", retry_blocked=True)
            self.assertTrue(result["complete"], result)
            self.assertNotEqual(source(case)["history_correction_id"], old_id)
            self.assertEqual(
                sum(p["adjustment"] == "all" and p["start"][:10] == "2016-01-04" for p in case.pages), 1
            )
            rows = read_pricing_views(case.store, "AAA")["all"]
            self.assertEqual((rows[0][4], rows[-1][4]), ("55", "60"))

    def test_same_witness_reuses_completed_disposition_after_json_without_history_io(self):
        with fixture() as case:
            case.records[:] = [asset("AAA")]
            case.identity = CatalogueClient(case.records)
            case.live["AAA"] = case.old
            page = case.pricing._get

            def context_difference(params):
                values = copy.deepcopy(page(params))
                if params["adjustment"] == "all" and timestamp(params["end"]) - timestamp(
                    params["start"]
                ) < timedelta(days=2):
                    for rows in values["bars"].values():
                        for row in rows:
                            if row["t"] == case.old[0]["t"]:
                                row.update({k: bar("2016-01-04", "50")[k] for k in ("o", "h", "l", "c", "vw")})
                return values

            with patch.object(case.pricing, "_get", side_effect=context_difference):
                first = case.update("2026-10-17")
                self.assertTrue(first["complete"], first)
                state = universe_state.read_state(case.store)
                accepted = state["work"]["AAA"]
                self.assertEqual(accepted["status"], "complete")
                self.assertEqual(
                    accepted["pricing_correction"]["source_sha256"],
                    accepted["correction_completion"]["source_sha256"],
                )
                cid = accepted["correction_id"]
                read = Path.read_bytes

                def no_history(path):
                    if path.suffix == ".gz" or path.name == VALIDATOR_FILE:
                        raise AssertionError("accepted history reopened: " + str(path))
                    return read(path)

                with (
                    patch.object(
                        refresh,
                        "acquire_full",
                        side_effect=AssertionError("completed full acquisition repeated"),
                    ),
                    patch.object(Path, "read_bytes", no_history),
                    patch.object(
                        correction,
                        "validate_durable_observations",
                        side_effect=AssertionError("completed evidence revalidated"),
                    ),
                ):
                    second = case.update("2026-10-17")
                self.assertTrue(second["complete"], second)
                self.assertEqual(second["stages"]["maintenance"]["stages"]["refresh"]["pricing"]["results"], [])
                case.live["AAA"] = case.old + [bar("2026-10-19")]
                with patch.object(
                    refresh, "acquire_full", side_effect=AssertionError("accepted prefix reacquired")
                ):

                    class Interrupted(BaseException):
                        pass

                    record = universe_state.record_outcome

                    def interrupt(store, symbol, **kwargs):
                        if kwargs.get("status") == "complete":
                            raise Interrupted()
                        return record(store, symbol, **kwargs)

                    with patch.object(universe_state, "record_outcome", side_effect=interrupt):
                        with self.assertRaises(Interrupted):
                            case.update("2026-10-20")
                    with patch.object(
                        correction,
                        "validate_durable_observations",
                        side_effect=AssertionError("accepted correction replayed after interruption"),
                    ):
                        advanced = case.update("2026-10-20")
                    self.assertTrue(advanced["complete"], advanced)
                    again = case.update("2026-10-20")
                    self.assertTrue(again["complete"], again)
                self.assertEqual(again["stages"]["maintenance"]["stages"]["refresh"]["pricing"]["results"], [])
            self.assertEqual(universe_state.read_state(case.store)["work"]["AAA"]["correction_id"], cid)

    def test_actual_observation_generation_mapping_or_scope_change_revokes_disposition(self):
        entry = {"symbol": "AAA", "asof": "2026-10-17", "end_exclusive": "2026-10-17T00:00:00Z"}
        row = ("2026-10-16T04:00:00Z", "1")
        item = {
            "correction_id": "id",
            "pricing_correction": {
                "schema_version": 3,
                "scope": "full_history",
                "cause": "selected_all_rows_changed",
                "store": "/store",
                "symbol": "AAA",
                "source_sha256": "generation",
                "asof": entry["asof"],
                "end_exclusive": entry["end_exclusive"],
                "observations": {"all": [list(row)]},
            },
            "correction_completion": {"correction_id": "id", "source_sha256": "generation"},
        }
        args = dict(store="/store", entry=entry, generation="generation", live_all=(row,))
        self.assertTrue(applicable(item, **args))
        for changed in (
            dict(args, generation="new"),
            dict(args, store="/other"),
            dict(args, live_all=(("different", "1"),)),
            dict(args, entry={**entry, "asof": "2026-10-18"}),
            dict(args, invalidated=True),
        ):
            self.assertFalse(applicable(item, **changed))
        carried = copy.deepcopy(item)
        carried["correction_completion"]["source_sha256"] = "appended"
        extended = {**entry, "asof": "2026-10-20", "end_exclusive": "2026-10-20T00:00:00Z"}
        composed = dict(
            store="/store",
            entry=extended,
            generation="appended",
            live_all=(row, ("2026-10-19T04:00:00Z", "2")),
            changed_sessions=(row[0],),
        )
        self.assertTrue(applicable(carried, **composed))
        self.assertFalse(applicable(carried, **{**composed, "added_sessions": ("2026-10-18T04:00:00Z",)}))
        self.assertFalse(applicable(carried, **{**composed, "changed_sessions": ("2026-10-19T04:00:00Z",)}))
        new = copy.deepcopy(item)
        new["correction_completion"]["source_sha256"] = "changed"
        self.assertFalse(applicable(new, **args))
