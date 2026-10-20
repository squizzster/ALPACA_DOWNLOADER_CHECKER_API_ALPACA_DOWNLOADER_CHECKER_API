"""Cross-command evidence coherence and restrictions during group expansion."""

import copy
import json
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from test_alpaca_durable_maintenance import ProcessInterrupted, fixture, seed_group
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_phase4_finalization import dependency_identity, invoke
from test_alpaca_pricing_download import FakeClient, bar
from test_alpaca_universe_update import CatalogueClient

from libs.alpaca_lineage_build import storage, universe_state
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views


def interrupted_replacement(case):
    accepted = asset("AAA", "accepted-replacement-id")
    case.identity = CatalogueClient([accepted])
    case.live["AAA"] = [bar("2016-01-04", "75"), bar("2026-10-16", "100")]
    with patch("libs.alpaca_lineage_build.universe_update.run_lineage", side_effect=ProcessInterrupted()):
        try:
            case.update()
        except ProcessInterrupted:
            pass
        else:
            raise AssertionError("replacement did not reach the interruption")
    return accepted, copy.deepcopy(universe_state.read_state(case.store)["work"]["AAA"])


def source(case, symbol="AAA"):
    return storage.read_document(case.store / f"end_of_day/pricing/{symbol}/source.manifest.json")


class EvidenceWorkPolicyTests(unittest.TestCase):
    def assert_finished(self, case, current):
        work = universe_state.read_state(case.store)["work"]["AAA"]
        entry = storage.read_index(case.store / "end_of_day/lineage")["symbols"]["AAA"]
        lookup = next(x for x in entry["projection"]["symbol_lookups"] if x["lookup"] == "AAA")
        self.assertEqual(lookup["asset"]["id"], current["id"])
        self.assertEqual(work["status"], "complete")
        self.assertEqual(entry["projection"]["update_obligation_id"], work["obligation_id"])
        self.assertEqual(source(case)["acquisition_evidence_sha256"], work["acquisition_evidence_fingerprint"])
        self.assertEqual(
            [row[4] for row in read_pricing_views(case.store, "AAA")["raw"]],
            [str(row["c"]) for row in case.live["AAA"]],
        )

    def test_changed_identity_is_repriced_by_each_lineage_entrypoint(self):
        for command in (("lineage", "update"), ("lineage", "refresh"), ("pricing", "refresh")):
            with self.subTest(command=command), fixture() as case:
                interrupted_replacement(case)
                current = asset("AAA", "newer-replacement-id")
                case.live["AAA"] = [bar("2016-01-04", "50"), bar("2026-10-16", "100")]
                if command[0] == "pricing":
                    # A differing endpoint sends pricing refresh through lineage.
                    case.live["AAA"][-1] = bar("2026-10-16", "80")
                case.pages.clear()
                args = [*command]
                if command[0] == "lineage":
                    args += ["--symbol", "AAA", "--through", "2026-10-16"]
                code, result, errors = invoke(case, *args, identity=IdentityClient({"AAA": current}))
                self.assertEqual(code, 0, (result, errors))
                self.assert_finished(case, current)
                case.identity = CatalogueClient([current])
                case.pages.clear()
                result = case.update()
                self.assertTrue(result["complete"], result)
                self.assertFalse(result["plan"]["work"])
                self.assertTrue(all(p["start"][:10] >= "2026-10-16" for p in case.pages))

    def test_unchanged_identity_and_metadata_only_change_reuse_completed_prices(self):
        for metadata_change in (False, True):
            with self.subTest(metadata_change=metadata_change), fixture() as case:
                accepted, _ = interrupted_replacement(case)
                current = {**accepted, **({"exchange": "TXSE"} if metadata_change else {})}
                before = source(case)
                with patch.object(case.pricing, "_get", side_effect=AssertionError("repeated replacement")):
                    code, result, errors = invoke(
                        case,
                        "lineage",
                        "update",
                        "--symbol",
                        "AAA",
                        "--through",
                        "2026-10-16",
                        identity=IdentityClient({"AAA": current}),
                    )
                self.assertEqual(code, 0, (result, errors))
                self.assertEqual(source(case), before)
                self.assert_finished(case, current)

    def test_changed_identity_stays_pending_when_acquisition_is_disabled(self):
        with fixture() as case:
            _, previous = interrupted_replacement(case)
            current = asset("AAA", "newer-replacement-id")
            case.live["AAA"] = [bar("2016-01-04", "50"), bar("2026-10-16", "100")]
            before = source(case)
            code, result, errors = invoke(
                case,
                "lineage",
                "update",
                "--symbol",
                "AAA",
                "--through",
                "2026-10-16",
                "--no-pricing-acquisition",
                identity=IdentityClient({"AAA": current}),
            )
            self.assertEqual(code, 1, (result, errors))
            self.assertFalse(result["complete"])
            work = universe_state.read_state(case.store)["work"]["AAA"]
            self.assertEqual(work["status"], "pending")
            self.assertNotEqual(work["obligation_id"], previous["obligation_id"])
            self.assertEqual(work["required_lookups"]["AAA"]["asset"]["id"], current["id"])
            self.assertEqual(source(case), before)
            # Catalogue absence cannot discard the newly accepted evidence.
            case.identity = CatalogueClient([{**asset("OTHER"), "status": "inactive"}])
            self.assertTrue(case.update()["complete"])
            self.assert_finished(case, current)

    def test_changed_evidence_survives_each_publication_interruption(self):
        for boundary in ("acceptance", "pricing", "lineage", "acknowledgement"):
            with self.subTest(boundary=boundary), fixture() as case:
                interrupted_replacement(case)
                current = asset("AAA", "newer-replacement-id")
                case.live["AAA"] = [bar("2016-01-04", "50"), bar("2026-10-16", "100")]
                if boundary == "acceptance":
                    original = universe_state.put_work_group

                    def interrupted(*args, **kwargs):
                        value = original(*args, **kwargs)
                        if any(
                            w.get("required_lookups", {}).get("AAA", {}).get("asset", {}).get("id")
                            == current["id"]
                            for w in value.values()
                        ):
                            raise ProcessInterrupted()
                        return value

                    target, action = "libs.alpaca_lineage_build.universe_state.put_work_group", interrupted
                elif boundary == "pricing":
                    target, action = "libs.alpaca_lineage_build.pipeline.download_pricing", ProcessInterrupted()
                elif boundary == "lineage":
                    target, action = "libs.alpaca_lineage_build.storage.publish", ProcessInterrupted()
                else:
                    target, action = (
                        "libs.alpaca_lineage_build.universe_state.record_outcome",
                        ProcessInterrupted(),
                    )
                with patch(target, side_effect=action), self.assertRaises(ProcessInterrupted):
                    invoke(
                        case,
                        "lineage",
                        "update",
                        "--symbol",
                        "AAA",
                        "--through",
                        "2026-10-16",
                        identity=IdentityClient({"AAA": current}),
                    )
                case.pages.clear()
                code, result, errors = invoke(
                    case,
                    "lineage",
                    "update",
                    "--symbol",
                    "AAA",
                    "--through",
                    "2026-10-16",
                    identity=IdentityClient({"AAA": current}),
                )
                self.assertEqual(code, 0, (result, errors))
                self.assert_finished(case, current)
                if boundary in {"lineage", "acknowledgement"}:
                    self.assertFalse(case.pages)

    def test_failed_group_replacement_cannot_publish_through_sibling(self):
        with fixture() as case:
            case.store = seed_group(case)
            old_index = storage.read_index(case.store / "end_of_day/lineage")
            current = asset("AAA", "replacement-id")
            identities = IdentityClient({"AAA": current, "BBB": asset("BBB")})
            case.live["AAA"] = case.live["BBB"] = [bar("2016-01-04", "50"), bar("2026-10-16", "100")]
            requests = []

            def denied(params):
                if "BBB" in params["symbols"].split(","):
                    requests.append(dict(params))
                    raise HTTPError("https://invalid", 403, "denied", {}, None)
                return case.page(params)

            with patch.object(case.pricing, "_get", side_effect=denied):
                code, result, errors = invoke(
                    case,
                    "lineage",
                    "refresh",
                    "--symbol",
                    "AAA",
                    "--symbol",
                    "BBB",
                    "--through",
                    "2026-10-16",
                    identity=identities,
                )
                self.assertEqual(code, 1, (result, errors))
                self.assertEqual(storage.read_index(case.store / "end_of_day/lineage"), old_index)
                work = universe_state.read_state(case.store)["work"]
                self.assertTrue(all(work[s]["invalidate_pricing"] for s in ("AAA", "BBB")))
                self.assertTrue(all(work[s]["status"] == "blocked" for s in ("AAA", "BBB")))
                count = len(requests)
                invoke(
                    case, "lineage", "update", "--symbol", "AAA", "--through", "2026-10-16", identity=identities
                )
                self.assertEqual(len(requests), count)
            case.identity = CatalogueClient([current, asset("BBB")])
            result = case.update(retry_blocked=True)
            self.assertTrue(result["complete"], result)
            self.assert_finished(case, current)

    def test_dependency_identity_survives_interruption_after_initial_pricing(self):
        with fixture() as case:
            original = download_pricing
            old_identity = dependency_identity()
            pricing = FakeClient([bar("2016-01-04", "75"), bar("2026-10-16", "100")])

            def interrupted(store, symbol, *args, **kwargs):
                result = original(store, symbol, *args, **kwargs)
                if symbol == "DEP":
                    raise ProcessInterrupted()
                return result

            with (
                patch("libs.alpaca_lineage_build.pipeline.download_pricing", side_effect=interrupted),
                self.assertRaises(ProcessInterrupted),
            ):
                invoke(
                    case,
                    "lineage",
                    "refresh",
                    "--symbol",
                    "AAA",
                    "--through",
                    "2026-10-16",
                    pricing=pricing,
                    identity=old_identity,
                )
            newer = {**asset("DEP", "replacement-dependency-id"), "exchange": "BATS"}
            current_identity = dependency_identity()
            current_identity.assets.update({"DEP": newer, "111111111": newer})
            pricing.bars = [bar("2016-01-04", "50"), bar("2026-10-16", "100")]
            code, result, errors = invoke(
                case,
                "lineage",
                "update",
                "--symbol",
                "AAA",
                "--through",
                "2026-10-16",
                pricing=pricing,
                identity=current_identity,
            )
            self.assertEqual(code, 0, (result, errors))
            self.assertEqual([row[4] for row in read_pricing_views(case.store, "DEP")["raw"]], ["50", "100"])

    def test_group_evidence_acceptance_rolls_back_as_one_transaction(self):
        with fixture() as case:
            case.store = seed_group(case)
            identities = IdentityClient({"AAA": asset("AAA", "replacement-id"), "BBB": asset("BBB")})
            connect = universe_state.connect_sqlite
            writes = []

            class InterruptedConnection:
                def __init__(self, *args, **kwargs):
                    self.connection = connect(*args, **kwargs)

                def __enter__(self):
                    self.connection.__enter__()
                    return self

                def __exit__(self, *args):
                    return self.connection.__exit__(*args)

                def __getattr__(self, name):
                    return getattr(self.connection, name)

                def execute(self, sql, parameters=()):
                    if sql.startswith("INSERT OR REPLACE INTO work") and json.loads(parameters[1]).get(
                        "invalidate_pricing"
                    ):
                        writes.append(parameters[0])
                        if len(writes) == 2:
                            raise ProcessInterrupted()
                    return self.connection.execute(sql, parameters)

            with (
                patch.object(universe_state, "connect_sqlite", InterruptedConnection),
                self.assertRaises(ProcessInterrupted),
            ):
                invoke(
                    case, "lineage", "refresh", "--symbol", "AAA", "--through", "2026-10-16", identity=identities
                )
            self.assertEqual(writes, ["AAA", "BBB"])
            work = universe_state.read_state(case.store)["work"]
            self.assertFalse(any(w.get("invalidate_pricing") for w in work.values()))
            case.live["BBB"] = list(case.old)
            code, result, errors = invoke(
                case, "lineage", "refresh", "--symbol", "AAA", "--through", "2026-10-16", identity=identities
            )
            self.assertEqual(code, 0, (result, errors))
            work = universe_state.read_state(case.store)["work"]
            self.assertTrue(all(w["status"] == "complete" for w in work.values()))

    def test_identity_replacement_cannot_overwrite_a_concurrent_source(self):
        with fixture() as case:
            interrupted_replacement(case)
            current = asset("AAA", "newer-replacement-id")
            before = storage.read_index(case.store / "end_of_day/lineage")
            original = download_pricing
            published = []

            def concurrent(store, symbol, end, **kwargs):
                if not published:
                    published.append(
                        original(
                            store,
                            symbol,
                            end,
                            client=FakeClient([bar("2016-01-04", "66"), bar("2026-10-16", "100")]),
                            refresh_all=True,
                        )
                    )
                return original(store, symbol, end, **kwargs)

            with patch("libs.alpaca_lineage_build.pipeline.download_pricing", side_effect=concurrent):
                code, result, errors = invoke(
                    case,
                    "lineage",
                    "update",
                    "--symbol",
                    "AAA",
                    "--through",
                    "2026-10-16",
                    identity=IdentityClient({"AAA": current}),
                )
            self.assertEqual(code, 1, (result, errors))
            self.assertIn("pricing source changed", result["results"][0]["error"])
            self.assertEqual(storage.document_digest(source(case)), published[0]["source_sha256"])
            self.assertEqual([row[4] for row in read_pricing_views(case.store, "AAA")["raw"]], ["66", "100"])
            self.assertEqual(storage.read_index(case.store / "end_of_day/lineage"), before)
            self.assertEqual(universe_state.read_state(case.store)["work"]["AAA"]["status"], "retry")

    def test_expanding_a_failed_group_preserves_restrictions_across_commands(self):
        for failure in (403, 503):
            with self.subTest(failure=failure), fixture() as case:
                download_pricing(case.store, "NEW", "2026-10-17", client=FakeClient(case.old))
                self.assertEqual(
                    run_lineage(case.store, ["NEW"], identity_client=IdentityClient({"NEW": asset("NEW")}))[
                        "failed"
                    ],
                    0,
                )
                dep = {**asset("DEP"), "exchange": "BATS"}
                identity = IdentityClient({"AAA": dep, "NEW": dep, "DEP": dep})
                pricing = FakeClient(case.old)
                fetch = pricing.fetch
                attempts = []

                def deny(symbol, *args, **kwargs):
                    if symbol == "DEP":
                        attempts.append(symbol)
                        raise HTTPError("https://invalid", failure, "denied", {}, None)
                    return fetch(symbol, *args, **kwargs)

                with patch.object(pricing, "fetch", side_effect=deny):
                    code, result, errors = invoke(
                        case,
                        "lineage",
                        "refresh",
                        "--symbol",
                        "AAA",
                        "--symbol",
                        "NEW",
                        "--through",
                        "2026-10-16",
                        pricing=pricing,
                        identity=identity,
                    )
                    self.assertEqual(code, 1, (result, errors))
                    work = universe_state.read_state(case.store)["work"]["DEP"]
                    self.assertEqual(work["status"], "retry" if failure == 503 else "blocked")
                    self.assertEqual(work["attempts"], 1)
                    self.assertEqual(set(work["required_group"]), {"AAA", "DEP", "NEW"})
                    self.assertEqual(bool(work["retry_after_utc"]), failure == 503)
                    count = len(attempts)
                    invoke(
                        case, "lineage", "update", "--through", "2026-10-16", pricing=pricing, identity=identity
                    )
                    self.assertEqual(len(attempts), count)
                case.identity = CatalogueClient([asset("AAA"), asset("NEW")])
                case.live["NEW"] = case.live["DEP"] = list(case.old)
                case.pages.clear()
                self.assertFalse(case.update()["complete"])
                self.assertFalse(case.pages)
                if failure == 503:
                    with patch.object(universe_state, "now_utc", return_value="2099-01-01"):
                        result = case.update()
                else:
                    result = case.update(retry_blocked=True)
                self.assertTrue(result["complete"], result)


if __name__ == "__main__":
    unittest.main()
