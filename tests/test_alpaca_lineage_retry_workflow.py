"""Recovery of a retained peer whose lineage group owes a publication."""

import json
import sqlite3
from contextlib import closing, contextmanager
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_durable_maintenance import (
    ActionCatalogueClient,
    ProcessInterrupted,
    fixture,
    rename,
    seed_group,
)
from test_alpaca_lineage_build import asset
from test_alpaca_pricing_download import bar

from libs.alpaca_lineage_build import pipeline, storage, universe_state
from libs.alpaca_lineage_build.context import accept_context
from libs.alpaca_lineage_build.contract import RecoveryRequired
from libs.alpaca_lineage_build.registration import command_policy
from libs.alpaca_lineage_build.universe_update import run_update
from libs.alpaca_pricing_download import acquire
from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_pricing_download.download import read_pricing_views
from libs.alpaca_pricing_download.storage import load_source
from libs.alpaca_serving_catalogue import storage as serving_storage
from libs.alpaca_serving_catalogue.producer import maintain
from libs.common.store_lock import operation_lock

END = "2026-10-20"
GROUP = {"AAA", "BBB"}


@contextmanager
def advancing_group():
    with fixture() as case:
        store = seed_group(case, inactive_second=True)
        assets = {"AAA": asset("AAA"), "BBB": {**asset("BBB"), "status": "inactive"}}
        identity = ActionCatalogueClient(
            list(assets.values()), [rename()], exact={**assets, "111111111": assets["BBB"]}
        )
        case.live = {"AAA": [*case.old, bar("2026-10-19")], "BBB": list(case.old)}

        def update(end=END, **options):
            return run_update(
                store,
                end,
                pricing_client=case.pricing,
                identity_client=identity,
                registry=case.registry,
                **options,
            )

        opened = update()
        assert opened["complete"], opened["failures"]
        assert [(p["adjustment"], p["start"][:10]) for p in case.pages if p["symbols"] == "BBB"] == [
            ("raw", "2026-10-17"),
            ("all", "2026-10-17"),
        ]
        case.pages.clear()
        case.live["AAA"][0] = bar("2016-01-04", "50")
        yield case, store, identity, update


def stop_group_publication(update):
    publish = storage.publish

    def fail_once_corrected(*args, **kwargs):
        if kwargs.get("full_materialization") and kwargs["sources"]["AAA"].get("history_correction_id"):
            raise RecoveryRequired("synthetic failure after pricing replacement")
        return publish(*args, **kwargs)

    with patch.object(storage, "publish", side_effect=fail_once_corrected):
        return update()


def peer_source(store):
    return load_source(store / "end_of_day/pricing/BBB", "BBB")


class LineageRetryWorkflowTests(TestCase):
    def test_group_failure_keeps_empty_dependency_without_acquiring_or_extending_peer(self):
        with advancing_group() as (case, store, _, update):
            before = peer_source(store)
            failed = stop_group_publication(update)
            self.assertFalse(failed["complete"])
            self.assertEqual({"AAA", "BBB"}, {row["symbol"] for row in failed["failures"]})
            work = universe_state.read_state(store)["work"]
            self.assertEqual("blocked", work["AAA"]["status"])
            self.assertFalse(work["BBB"]["acquire"])
            self.assertEqual(["unfinished_lineage"], work["BBB"]["reasons"])
            self.assertEqual(["raw", "all"], [p["adjustment"] for p in case.pages if p["symbols"] == "BBB"])
            with (
                patch.object(case.pricing, "_get", side_effect=AssertionError("repeated accepted pricing")),
                patch.object(storage, "publish", wraps=storage.publish) as published,
                patch.object(serving_storage, "commit", wraps=serving_storage.commit) as served,
            ):
                default = update()
                self.assertFalse(default["complete"])
                published.assert_not_called()
                served.assert_not_called()
                result = update(retry_blocked=True)
            self.assertTrue(result["complete"], result["failures"])
            self.assertEqual(0, result["pending_work"])
            self.assertFalse(result["plan"]["work"].get("BBB", {}).get("acquire"))
            self.assertEqual([], result["stages"]["acquisition"]["results"])
            self.assertEqual(before, peer_source(store))
            self.assertEqual(1, published.call_count)
            self.assertEqual(1, served.call_count)

    def test_interruption_after_empty_selection_resumes_from_producer_outcome(self):
        with advancing_group() as (case, store, _, update):
            before = peer_source(store)
            catch_up = pipeline._catch_up_published_dependencies

            def interrupt_after_selection(*args, **kwargs):
                result = catch_up(*args, **kwargs)
                if any(row["status"] == "not_selected" for row in result["results"]):
                    raise ProcessInterrupted
                return result

            with patch.object(
                pipeline, "_catch_up_published_dependencies", side_effect=interrupt_after_selection
            ):
                with self.assertRaises(ProcessInterrupted):
                    update()
            self.assertEqual(["raw", "all"], [p["adjustment"] for p in case.pages if p["symbols"] == "BBB"])
            with patch.object(case.pricing, "_get", side_effect=AssertionError("repeated accepted pricing")):
                resumed = update(retry_blocked=True)
            self.assertTrue(resumed["complete"], resumed["failures"])
            self.assertEqual(before, peer_source(store))

    def test_advancing_retry_rechecks_changed_history_and_uses_new_peer_suffix(self):
        with advancing_group() as (case, store, _, update):
            self.assertFalse(stop_group_publication(update)["complete"])
            case.live["AAA"][0] = bar("2016-01-04", "55")
            case.live["AAA"].append(bar("2026-10-20", "60"))
            case.live["BBB"].append(bar("2026-10-20", "17"))
            case.pages.clear()
            result = update("2026-10-21", retry_blocked=True)
            self.assertTrue(result["complete"], result["failures"])
            self.assertEqual(0, result["pending_work"])
            self.assertCountEqual(
                [("raw", "2026-10-17"), ("all", "2026-10-17"), ("all", "2016-01-04"), ("all", "2026-10-16")],
                [(p["adjustment"], p["start"][:10]) for p in case.pages if p["symbols"] == "BBB"],
            )
            self.assertEqual({"2026-10-21"}, {p["asof"] for p in case.pages if p["symbols"] == "BBB"})
            values = read_pricing_views(store, "AAA")["all"]
            self.assertEqual(("55", "60"), (values[0][4], values[-1][4]))
            self.assertEqual("17", read_pricing_views(store, "BBB")["all"][-1][4])

    def test_previously_published_group_can_acknowledge_without_publication(self):
        with advancing_group() as (case, store, identity, update):
            self.assertFalse(stop_group_publication(update)["complete"])
            before = peer_source(store)
            with operation_lock(store, exclusive=True), universe_state.update_lock(store):
                context = accept_context(store)
                policy = command_policy(store, END, context=context, retry_blocked=True)
                # The empty suffix was accepted before the failed group publication.
                recovered = pipeline.run_lineage(
                    store, ["AAA"], end_date=END, identity_client=identity, context=context, work_policy=policy
                )
                self.assertFalse(recovered["failed"], recovered["results"])
                self.assertNotEqual("waiting_for_pricing", recovered["results"][0]["status"])
                policy.acknowledge()
                self.assertTrue(maintain(store, symbols=GROUP, context=context)["complete"])
            with (
                patch.object(case.pricing, "_get", side_effect=AssertionError("repeated accepted pricing")),
                patch.object(storage, "publish", side_effect=AssertionError("repeated lineage publication")),
                patch.object(
                    serving_storage, "commit", side_effect=AssertionError("repeated serving publication")
                ),
            ):
                final = update(retry_blocked=True)
            self.assertTrue(final["complete"], final["failures"])
            self.assertEqual(0, final["pending_work"])
            self.assertEqual(before, peer_source(store))

    def test_pre_upgrade_retry_selects_only_missing_suffix_and_preserves_real_failures(self):
        for outcome in ("empty", "positive", "incomplete"):
            with self.subTest(outcome=outcome), advancing_group() as (case, store, identity, update):
                self.assertFalse(stop_group_publication(update)["complete"])
                failed_work = universe_state.read_state(store)["work"]
                before = peer_source(store)
                with operation_lock(store, exclusive=True), universe_state.update_lock(store):
                    context = accept_context(store)
                    policy = command_policy(store, END, context=context, retry_blocked=True)
                    recovered = pipeline.run_lineage(
                        store,
                        ["AAA"],
                        end_date=END,
                        identity_client=identity,
                        pricing_client=case.pricing,
                        context=context,
                        work_policy=policy,
                    )
                    self.assertFalse(recovered["failed"], recovered["results"])
                    self.assertTrue(maintain(store, symbols=GROUP, context=context)["complete"])

                # Recreate the workflow contract older releases left beside those
                # completed publications. Its aggregate report is outside the handoff.
                legacy_fields = {
                    "symbol",
                    "obligation_id",
                    "status",
                    "attempts",
                    "outcome",
                    "last_failure",
                    "reasons",
                    "end_exclusive",
                    "pricing_start",
                    "pricing_end_exclusive",
                    "evidence_fingerprint",
                    "acquisition_evidence_fingerprint",
                    "required_lookups",
                    "required_group",
                    "required_history_check",
                    "correction_id",
                    "pricing_correction",
                    "correction_completion",
                    "invalidate_pricing",
                    "acquire",
                    "refresh_metadata",
                }
                work = {
                    label: {k: v for k, v in row.items() if k in legacy_fields}
                    for label, row in failed_work.items()
                }
                work["BBB"].update(
                    reasons=["dependency_reconciliation", "lineage_evidence", "unfinished_lineage"],
                    acquire=True,
                    pricing_start="2016-01-01",
                    pricing_end_exclusive=END,
                    required_group=sorted(GROUP),
                    refresh_metadata=False,
                )
                with closing(sqlite3.connect(store / universe_state.STATE_FILE)) as conn, conn:
                    conn.executemany(
                        "UPDATE work SET body=? WHERE symbol=?",
                        [(json.dumps(row), label) for label, row in work.items()],
                    )
                    conn.execute("UPDATE state SET revision=revision+1 WHERE id=1")
                case.pages.clear()
                if outcome == "positive":
                    case.live["BBB"].append(bar("2026-10-19", "17"))

                def request(params):
                    self.assertEqual("BBB", params["symbols"])
                    if outcome != "positive":
                        self.assertEqual("2026-10-17", params["start"][:10])
                    if outcome == "incomplete":
                        case.pages.append(dict(params))
                        if params.get("page_token"):
                            raise DownloadError("suffix pagination interrupted")
                        return {"bars": {}, "next_page_token": "next"}
                    return case.page(params)

                read_bytes = Path.read_bytes
                selected_manifests = []

                def read_selected(path):
                    if path.name == "source.manifest.json":
                        selected_manifests.append(path.parent.name)
                    if outcome != "positive" and path.suffix == ".gz":
                        self.fail(f"opened historical payload for a suffix selection: {path}")
                    return read_bytes(path)

                with (
                    patch.object(Path, "read_bytes", read_selected),
                    patch.object(case.pricing, "_get", side_effect=request),
                    patch.object(storage, "publish", wraps=storage.publish) as published,
                    patch.object(serving_storage, "commit", wraps=serving_storage.commit) as served,
                    patch.object(acquire, "read_validator", wraps=acquire.read_validator) as read_validator,
                    patch.object(storage, "read_receipt", wraps=storage.read_receipt) as read_receipt,
                ):
                    result = update(retry_blocked=True)
                if outcome == "incomplete":
                    self.assertFalse(result["complete"])
                    self.assertGreater(result["pending_work"], 0)
                    self.assertTrue(any(p.get("page_token") == "next" for p in case.pages))
                    self.assertEqual(before, peer_source(store))
                    published.assert_not_called()
                    served.assert_not_called()
                    case.pages.clear()
                    with (
                        patch.object(
                            storage, "publish", side_effect=AssertionError("repeated lineage publication")
                        ),
                        patch.object(
                            serving_storage, "commit", side_effect=AssertionError("repeated serving publication")
                        ),
                        patch.object(Path, "read_bytes", read_selected),
                    ):
                        repaired = update(retry_blocked=True)
                    self.assertTrue(repaired["complete"], repaired["failures"])
                    self.assertEqual(
                        [("BBB", "raw", "2026-10-17"), ("BBB", "all", "2026-10-17")],
                        [(p["symbols"], p["adjustment"], p["start"][:10]) for p in case.pages],
                    )
                    self.assertEqual(before, peer_source(store))
                    continue
                self.assertTrue(result["complete"], result["failures"])
                self.assertEqual(0, result["pending_work"])
                if outcome == "empty":
                    self.assertEqual(["raw", "all"], [p["adjustment"] for p in case.pages])
                    self.assertEqual(before, peer_source(store))
                    read_validator.assert_not_called()
                    read_receipt.assert_not_called()
                    self.assertCountEqual(["AAA", "BBB"], selected_manifests)
                    published.assert_not_called()
                    served.assert_not_called()
                else:
                    self.assertCountEqual(
                        [
                            ("raw", "2026-10-17"),
                            ("all", "2026-10-17"),
                            ("all", "2016-01-04"),
                            ("all", "2026-10-16"),
                        ],
                        [(p["adjustment"], p["start"][:10]) for p in case.pages],
                    )
                    self.assertEqual("17", read_pricing_views(store, "BBB")["all"][-1][4])
                    self.assertEqual(1, published.call_count)
                    self.assertEqual(1, served.call_count)
