import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from libs.alpaca_end_of_day_clean_room.generation import (
    CLEAN_ROOM_RUN_VERSION,
    CleanRoomError,
    activate_clean_room_generation,
    load_universe,
    run_clean_room_generation,
)
from libs.alpaca_end_of_day_v6.common import PUBLICATION_PENDING, read_json_object
from libs.alpaca_end_of_day_v6.pipeline import run_v6_pipeline
from libs.alpaca_end_of_day_v6.source import initialize_v6_store


def _resolution(submitted: str, canonical: str) -> dict:
    return {
        "submitted_symbol": submitted,
        "authoritative_symbol": canonical,
        "changed": submitted != canonical,
        "basis": "alpaca_asset_lookup",
        "http_status": 200,
        "asset": {
            "id": f"asset-{canonical}",
            "class": "us_equity",
            "symbol": canonical,
        },
    }


def _pipeline_result(canonical: str, *, admitted: bool) -> dict:
    result = {
        "admitted": admitted,
        "rejection": None if admitted else {"reason": "pricing_validation_failed", "symbols": [canonical]},
        "source": {canonical: {"source_generation_sha256": "s" * 64}},
        "validation": {canonical: {"validation_generation_sha256": "v" * 64}},
        "lineage": {},
        "correlation": {},
        "warnings": {},
        "identity_candidate": None,
        "availability": None,
        "catalogue": None,
    }
    if admitted:
        result.update(
            {
                "lineage": {canonical: {"lineage_generation_sha256": "l" * 64}},
                "correlation": {canonical: {"correlation_generation_sha256": "c" * 64}},
                "warnings": {canonical: {"warning_pipeline_generation_sha256": "w" * 64}},
                "identity_candidate": {"candidate_generation_sha256": "i" * 64},
                "availability": {"availability_generation_sha256": "a" * 64},
                "catalogue": {
                    "catalogue_generation_sha256": "g" * 64,
                    "security_master": {"generation_sha256": "m" * 64},
                },
            }
        )
    return result


class CleanRoomGenerationTests(unittest.TestCase):
    def _activation_store(
        self,
        root: Path,
        name: str,
        *,
        run_id: str,
        generation: str,
        online_count: int,
    ) -> Path:
        store = root / name
        initialize_v6_store(store)
        state = store / "clean_room_run"
        state.mkdir()
        (store / "identity.txt").write_text(name, encoding="utf-8")
        (state / "manifest.json").write_text(
            json.dumps({"run_id": run_id}),
            encoding="utf-8",
        )
        audit = {
            "status": "passed",
            "run_id": run_id,
            "catalogue_generation_sha256": generation,
            "counts": {"online_identities": online_count},
        }
        (state / "audit.json").write_text(json.dumps(audit), encoding="utf-8")
        from libs.alpaca_end_of_day_v6.common import sha256_file

        completion = {
            "schema_version": CLEAN_ROOM_RUN_VERSION,
            "boundary": "alpaca_end_of_day_clean_room_completion",
            "run_id": run_id,
            "status": "passed",
            "audit_sha256": sha256_file(state / "audit.json"),
            "catalogue_generation_sha256": generation,
        }
        (state / "completion.json").write_text(json.dumps(completion), encoding="utf-8")
        return store

    def test_change_log_matches_clean_room_run_version(self) -> None:
        path = Path("resources/alpaca_clean_room_run_change_log.json")
        entries = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(CLEAN_ROOM_RUN_VERSION, len(entries))
        self.assertTrue(all("version" not in entry for entry in entries))

    def test_universe_is_one_sorted_csv_record(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "universe.csv"
            path.write_text("AAA,AAB,AAC\n", encoding="utf-8")
            self.assertEqual(("AAA", "AAB", "AAC"), load_universe(path))
            path.write_text("AAA\nAAB\n", encoding="utf-8")
            with self.assertRaisesRegex(CleanRoomError, "exactly one"):
                load_universe(path)

    def test_run_checkpoints_each_label_deduplicates_canonical_work_and_resumes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            universe = root / "universe.csv"
            store = root / "clean"
            universe.write_text("AAA,AAB,AAC\n", encoding="utf-8")
            resolution_calls = []
            pipeline_calls = []

            def resolver(label, *_args, **_kwargs):
                resolution_calls.append(label)
                return _resolution(label, "AAA" if label == "AAB" else label)

            def pipeline_runner(_symbols, **kwargs):
                canonical = kwargs["symbol_resolutions"][0]["authoritative_symbol"]
                pipeline_calls.append(canonical)
                return _pipeline_result(canonical, admitted=canonical == "AAA")

            def auditor(audit_store, symbols, *, output):
                manifest = read_json_object(
                    Path(audit_store) / "clean_room_run" / "manifest.json",
                    "manifest",
                )
                self.assertEqual(("AAA", "AAB", "AAC"), tuple(symbols))
                self.assertIsInstance(output, io.StringIO)
                return {
                    "status": "passed",
                    "run_id": manifest["run_id"],
                    "catalogue_generation_sha256": "m" * 64,
                }

            output = io.StringIO()
            completion = run_clean_room_generation(
                universe,
                store,
                request_end_timestamp="2026-10-14T00:00:00Z",
                environment={"ALPACA_API_KEY_ID": "key", "ALPACA_API_SECRET_KEY": "secret"},
                output=output,
                resolver=resolver,
                pipeline_runner=pipeline_runner,
                final_auditor=auditor,
            )
            self.assertEqual("passed", completion["status"])
            self.assertEqual(["AAA", "AAB", "AAC"], resolution_calls)
            self.assertEqual(["AAA", "AAC"], pipeline_calls)
            outcomes = [
                read_json_object(
                    store / "clean_room_run" / "outcomes" / f"{position:06d}.json",
                    "outcome",
                )
                for position in range(3)
            ]
            self.assertEqual(
                ["published", "canonical_reused", "rejected_validation"],
                [outcome["status"] for outcome in outcomes],
            )
            self.assertEqual(0, outcomes[1]["canonical_owner_position"])

            run_clean_room_generation(
                universe,
                store,
                request_end_timestamp="2026-10-14T00:00:00Z",
                environment={"ALPACA_API_KEY_ID": "key", "ALPACA_API_SECRET_KEY": "secret"},
                output=io.StringIO(),
                resolver=Mock(side_effect=AssertionError("completed run must not resolve again")),
                pipeline_runner=Mock(side_effect=AssertionError("completed run must not calculate again")),
                final_auditor=Mock(side_effect=AssertionError("completed run must not audit again")),
            )

    def test_snapshot_survives_registry_growth_and_interrupted_manifest_publication(self):
        from libs.alpaca_end_of_day_clean_room import generation
        from libs.alpaca_recovery_registry import publish_union

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry, store = root / "ticker.csv", root / "build"
            registry.write_text("AAA\n")
            with patch.object(generation, "replace_json", side_effect=OSError("interrupted")):
                with self.assertRaisesRegex(OSError, "interrupted"):
                    generation._initialize_run(store, registry, "2026-10-17T00:00:00Z")
            manifest, _ = generation._initialize_run(store, registry, "2026-10-17T00:00:00Z")
            before = (store / "clean_room_run/manifest.json").read_bytes()
            publish_union(["NEW"], registry)
            pinned = store / "clean_room_run/universe.snapshot.csv"
            resumed, _ = generation._initialize_run(store, pinned, "2026-10-17T00:00:00Z")
            self.assertEqual(manifest["run_id"], resumed["run_id"])
            self.assertEqual(before, (store / "clean_room_run/manifest.json").read_bytes())
            self.assertEqual(load_universe(pinned), ("AAA",))

    def test_pin_older_universe_preserves_provenance_without_upgrading_contracts(self):
        from libs.alpaca_end_of_day_clean_room import generation
        from libs.alpaca_end_of_day_v6.common import canonical_sha256

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry, store = root / "ticker.csv", root / "build"
            registry.write_text("AAA\n")
            manifest, _ = generation._initialize_run(store, registry, "2026-10-17T00:00:00Z")
            manifest["schema_version"] = 1
            manifest["contracts"]["source"] -= 1
            manifest["run_id"] = canonical_sha256(
                {k: v for k, v in manifest.items() if k not in {"run_id", "created_at"}}
            )
            path = store / "clean_room_run/manifest.json"
            path.write_text(json.dumps(manifest))
            (store / "clean_room_run/universe.snapshot.csv").unlink()
            before = path.read_bytes()
            generation.pin_universe(store, registry)
            self.assertEqual(before, path.read_bytes())
            with self.assertRaisesRegex(CleanRoomError, "immutable identity"):
                generation._initialize_run(store, registry, "2026-10-17T00:00:00Z")

    def test_run_identity_refuses_a_different_request_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            universe = root / "universe.csv"
            store = root / "clean"
            universe.write_text("AAA\n", encoding="utf-8")

            def auditor(audit_store, _symbols, *, output):
                del output
                manifest = read_json_object(
                    Path(audit_store) / "clean_room_run" / "manifest.json",
                    "manifest",
                )
                return {
                    "status": "passed",
                    "run_id": manifest["run_id"],
                    "catalogue_generation_sha256": "m" * 64,
                }

            run_clean_room_generation(
                universe,
                store,
                request_end_timestamp="2026-10-14T00:00:00Z",
                environment={"ALPACA_API_KEY_ID": "key", "ALPACA_API_SECRET_KEY": "secret"},
                resolver=lambda label, *_args, **_kwargs: _resolution(label, label),
                pipeline_runner=lambda _symbols, **_kwargs: _pipeline_result("AAA", admitted=True),
                final_auditor=auditor,
            )
            with self.assertRaisesRegex(CleanRoomError, "different immutable identity"):
                run_clean_room_generation(
                    universe,
                    store,
                    request_end_timestamp="2026-10-15T00:00:00Z",
                    environment={"ALPACA_API_KEY_ID": "key", "ALPACA_API_SECRET_KEY": "secret"},
                )

    def test_new_run_refuses_a_prepopulated_monthly_store(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            universe = root / "universe.csv"
            store = root / "clean"
            universe.write_text("AAA\n", encoding="utf-8")
            initialize_v6_store(store)
            (store / "end_of_day" / "OLD").mkdir()
            with self.assertRaisesRegex(CleanRoomError, "virgin monthly store"):
                run_clean_room_generation(
                    universe,
                    store,
                    request_end_timestamp="2026-10-14T00:00:00Z",
                    environment={"ALPACA_API_KEY_ID": "key", "ALPACA_API_SECRET_KEY": "secret"},
                )

    def test_resolution_failure_is_retried_before_pipeline_work(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            universe = root / "universe.csv"
            store = root / "clean"
            universe.write_text("AAA\n", encoding="utf-8")
            calls = []

            def resolver(label, *_args, **_kwargs):
                calls.append(label)
                if len(calls) == 1:
                    raise RuntimeError("temporary provider failure")
                return _resolution(label, label)

            def auditor(audit_store, _symbols, *, output):
                del output
                manifest = read_json_object(
                    Path(audit_store) / "clean_room_run" / "manifest.json",
                    "manifest",
                )
                return {
                    "status": "passed",
                    "run_id": manifest["run_id"],
                    "catalogue_generation_sha256": "m" * 64,
                }

            with patch("libs.alpaca_end_of_day_clean_room.generation.time.sleep") as sleep:
                run_clean_room_generation(
                    universe,
                    store,
                    request_end_timestamp="2026-10-14T00:00:00Z",
                    environment={"ALPACA_API_KEY_ID": "key", "ALPACA_API_SECRET_KEY": "secret"},
                    resolver=resolver,
                    pipeline_runner=lambda _symbols, **_kwargs: _pipeline_result("AAA", admitted=True),
                    final_auditor=auditor,
                )
            self.assertEqual(["AAA", "AAA"], calls)
            sleep.assert_called_once_with(1)
            attempt = read_json_object(
                store / "clean_room_run" / "attempts" / "000000.001.json",
                "attempt",
            )
            self.assertEqual("resolution", attempt["stage"])

    def test_validation_rejection_finishes_and_is_resumed_without_network(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = Path(directory) / "clean"
            kwargs = {
                "store_root": store,
                "environment": {"ALPACA_API_KEY_ID": "key", "ALPACA_API_SECRET_KEY": "secret"},
                "request_start_timestamp": "2016-01-01T00:00:00Z",
                "request_end_timestamp": "2026-10-14T00:00:00Z",
                "symbol_resolutions": [_resolution("EMPTY", "EMPTY")],
                "accept_failed_validation": True,
                "reuse_acquired_boundaries": True,
            }
            with patch(
                "libs.alpaca_end_of_day_v6.pipeline.request_alpaca_daily_bars",
                return_value=[],
            ) as request_bars:
                result = run_v6_pipeline(["EMPTY"], **kwargs)
            self.assertFalse(result["admitted"])
            self.assertEqual(8, request_bars.call_count)
            self.assertFalse((store / PUBLICATION_PENDING).exists())
            self.assertTrue((store / "end_of_day" / "EMPTY" / "source.manifest.json").is_file())

            with patch(
                "libs.alpaca_end_of_day_v6.pipeline.request_alpaca_daily_bars",
                side_effect=AssertionError("authenticated source must be reused"),
            ):
                resumed = run_v6_pipeline(["EMPTY"], **kwargs)
            self.assertFalse(resumed["admitted"])
            self.assertTrue(resumed["source"]["EMPTY"]["reused_authenticated_boundary"])

    def test_activation_retains_the_previous_store_and_checks_public_selection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            initialize_v6_store(root / "data_vtest")
            (root / "data_vtest" / "identity.txt").write_text("old", encoding="utf-8")
            run_id = "r" * 64
            generation = "g" * 64
            self._activation_store(
                root,
                "data_clean_room",
                run_id=run_id,
                generation=generation,
                online_count=7,
            )

            from types import SimpleNamespace

            selected = SimpleNamespace(
                receipt={"security_master_generation_sha256": generation},
                identities_by_master_id={str(i): {"online": True} for i in range(7)},
            )
            with patch("libs.alpaca_end_of_day_clean_room.generation.load_catalogue", return_value=selected):
                activation = activate_clean_room_generation(root, "data_clean_room")

            self.assertEqual("active", activation["status"])
            self.assertEqual("data_clean_room", (root / "data_vtest" / "identity.txt").read_text())
            backup = root / activation["previous_store"]
            self.assertEqual("old", (backup / "identity.txt").read_text())
            self.assertTrue((root / "alpaca_run_logs" / f"clean_room_activation_{run_id}.json").is_file())


if __name__ == "__main__":
    unittest.main()
