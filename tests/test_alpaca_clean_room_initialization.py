"""Coherent immutable inputs across registry growth, concurrent creation and interruption."""

import fcntl
import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from unittest.mock import patch

from libs.alpaca_end_of_day_clean_room import generation
from libs.alpaca_end_of_day_v6.common import canonical_sha256, sha256_bytes
from libs.alpaca_end_of_day_v6.source import initialize_v6_store
from libs.alpaca_recovery_registry import publish_union


class CleanRoomInitializationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.registry = self.root / "ticker.csv"
        self.original_bytes = b'"AAA"\r\n'
        self.registry.write_bytes(self.original_bytes)
        self.store = self.root / "build"
        self.end = "2026-10-17T00:00:00Z"

    def initialize(self, *, store=None, end=None):
        return generation._initialize_run(store or self.store, self.registry, end or self.end)

    def assert_snapshot(self, manifest, content, *, store=None):
        state = (store or self.store) / generation.RUN_STATE_DIRECTORY
        self.assertEqual((state / generation.UNIVERSE_SNAPSHOT).read_bytes(), content)
        self.assertEqual(manifest["universe"]["source_sha256"], sha256_bytes(content))
        labels = generation.load_universe(state / generation.UNIVERSE_SNAPSHOT)
        self.assertEqual(manifest["universe"]["label_count"], len(labels))
        self.assertEqual(manifest["universe"]["ordered_labels_sha256"], canonical_sha256(list(labels)))
        self.assertFalse((state / generation.INITIALIZATION_PENDING).exists())

    def test_registry_growth_after_capture_cannot_tear_initialization(self):
        capture = generation._capture_universe
        reads = []

        def grow_after_capture(path):
            content = capture(path)
            reads.append(path)
            if path == self.registry:
                publish_union(["NEW"], self.registry)
            return content

        with patch.object(generation, "_capture_universe", side_effect=grow_after_capture):
            manifest, symbols = self.initialize()
        self.assertEqual(reads, [self.registry])
        self.assertEqual(symbols, ("AAA",))
        self.assertEqual(generation.load_universe(self.registry), ("AAA", "NEW"))
        self.assert_snapshot(manifest, self.original_bytes)
        resumed, symbols = self.initialize()
        self.assertEqual(resumed, manifest)
        self.assertEqual(symbols, ("AAA",))

    def legacy_manifest(self, version=1, *, incompatible=True):
        manifest, _ = self.initialize()
        manifest["schema_version"] = version
        if incompatible:
            manifest["contracts"]["correlation_transform"] -= 1
        manifest["run_id"] = canonical_sha256(
            {key: value for key, value in manifest.items() if key not in {"run_id", "created_at"}}
        )
        state = self.store / generation.RUN_STATE_DIRECTORY
        path = state / generation.RUN_MANIFEST
        path.write_text(json.dumps(manifest))
        (state / generation.UNIVERSE_SNAPSHOT).unlink()
        return manifest, path.read_bytes()

    def test_legacy_pinning_captures_once_and_preserves_original_provenance(self):
        manifest, original_manifest = self.legacy_manifest()
        capture = generation._capture_universe

        def grow_after_capture(path):
            result = capture(path)
            if path == self.registry:
                publish_union(["NEW"], self.registry)
            return result

        with patch.object(generation, "_capture_universe", side_effect=grow_after_capture):
            result = generation.pin_universe(self.store, self.registry)
        self.assertEqual(result["label_count"], 1)
        self.assert_snapshot(manifest, self.original_bytes)
        self.assertEqual(
            (self.store / generation.RUN_STATE_DIRECTORY / generation.RUN_MANIFEST).read_bytes(),
            original_manifest,
        )
        with self.assertRaisesRegex(generation.CleanRoomError, "immutable identity"):
            self.initialize()

    def test_concurrent_initializers_share_one_pinned_identity(self):
        initialize_v6_store(self.store)
        snapshot_installed, release_first = Event(), Event()
        original = generation.atomic_bytes

        def pause_after_snapshot(path, content):
            original(path, content)
            if path.name == generation.UNIVERSE_SNAPSHOT:
                snapshot_installed.set()
                if not release_first.wait(5):
                    raise AssertionError("initializer was not released")

        with patch.object(generation, "atomic_bytes", side_effect=pause_after_snapshot):
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(self.initialize)
                try:
                    self.assertTrue(snapshot_installed.wait(5))
                    with (self.store / ".publication.lock").open("r+b") as lock:
                        with self.assertRaises(BlockingIOError):
                            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    publish_union(["NEW"], self.registry)
                    second = pool.submit(self.initialize)
                finally:
                    release_first.set()
                manifest, first_labels = first.result(timeout=5)
                other_manifest, second_labels = second.result(timeout=5)
        self.assertEqual(manifest, other_manifest)
        self.assertEqual(first_labels, second_labels)
        self.assertEqual(first_labels, ("AAA",))
        self.assert_snapshot(manifest, self.original_bytes)

    def test_concurrent_incompatible_target_cannot_replace_first_identity(self):
        initialize_v6_store(self.store)
        snapshot_installed, release_first = Event(), Event()
        original = generation.atomic_bytes

        def pause_after_snapshot(path, content):
            original(path, content)
            if path.name == generation.UNIVERSE_SNAPSHOT:
                snapshot_installed.set()
                if not release_first.wait(5):
                    raise AssertionError("initializer was not released")

        with patch.object(generation, "atomic_bytes", side_effect=pause_after_snapshot):
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(self.initialize)
                try:
                    self.assertTrue(snapshot_installed.wait(5))
                    publish_union(["NEW"], self.registry)
                    second = pool.submit(self.initialize, end="2026-10-20T00:00:00Z")
                finally:
                    release_first.set()
                manifest, _ = first.result(timeout=5)
                with self.assertRaisesRegex(generation.CleanRoomError, "immutable identity"):
                    second.result(timeout=5)
        self.assertEqual(manifest["request"]["to"], self.end)
        self.assert_snapshot(manifest, self.original_bytes)

    def test_interruption_at_each_initialization_publication_boundary(self):
        original_json, original_bytes, original_unlink = (
            generation.replace_json,
            generation.atomic_bytes,
            Path.unlink,
        )
        for boundary in (
            "before_intent",
            "after_intent",
            "before_snapshot",
            "after_snapshot",
            "before_manifest",
            "after_manifest",
            "before_clear",
        ):
            with self.subTest(boundary=boundary):
                store = self.root / boundary
                self.registry.write_bytes(self.original_bytes)

                def write_json(path, document):
                    label = "intent" if path.name == generation.INITIALIZATION_PENDING else "manifest"
                    if boundary == "before_" + label:
                        raise OSError(boundary)
                    result = original_json(path, document)
                    if boundary == "after_" + label:
                        raise OSError(boundary)
                    return result

                def write_snapshot(path, content):
                    if boundary == "before_snapshot":
                        raise OSError(boundary)
                    original_bytes(path, content)
                    if boundary == "after_snapshot":
                        raise OSError(boundary)

                def unlink(path, *args, **kwargs):
                    if path.name == generation.INITIALIZATION_PENDING and boundary == "before_clear":
                        raise OSError(boundary)
                    return original_unlink(path, *args, **kwargs)

                # Store initialization belongs to the monthly-store owner; fault
                # injection here starts at the clean-room input transaction.
                initialize_v6_store(store)
                with (
                    patch.object(generation, "replace_json", side_effect=write_json),
                    patch.object(generation, "atomic_bytes", side_effect=write_snapshot),
                    patch.object(Path, "unlink", unlink),
                ):
                    with self.assertRaisesRegex(OSError, boundary):
                        self.initialize(store=store)
                publish_union(["NEW"], self.registry)
                manifest, labels = self.initialize(store=store)
                if boundary == "before_intent":
                    self.assertEqual(labels, ("AAA", "NEW"))
                    self.assert_snapshot(manifest, self.registry.read_bytes(), store=store)
                else:
                    self.assertEqual(labels, ("AAA",))
                    self.assert_snapshot(manifest, self.original_bytes, store=store)
                self.assertEqual(manifest["request"]["to"], self.end)

    def test_recovery_finishes_original_target_before_rejecting_another_request(self):
        with patch.object(generation, "atomic_bytes", side_effect=OSError("interrupted")):
            with self.assertRaisesRegex(OSError, "interrupted"):
                self.initialize()
        state = self.store / generation.RUN_STATE_DIRECTORY
        intent = json.loads((state / generation.INITIALIZATION_PENDING).read_text())
        publish_union(["NEW"], self.registry)
        with self.assertRaisesRegex(generation.CleanRoomError, "immutable identity"):
            self.initialize(end="2026-10-16T00:00:00Z")
        manifest = json.loads((state / generation.RUN_MANIFEST).read_text())
        self.assertEqual(manifest, intent["manifest"])
        self.assert_snapshot(manifest, self.original_bytes)

    def test_recovery_preserves_original_contracts_and_resume_target(self):
        with patch.object(generation, "atomic_bytes", side_effect=OSError("interrupted")):
            with self.assertRaisesRegex(OSError, "interrupted"):
                self.initialize()
        state = self.store / generation.RUN_STATE_DIRECTORY
        intent = json.loads((state / generation.INITIALIZATION_PENDING).read_text())
        different_contracts = {**intent["manifest"]["contracts"], "source": 999}
        with patch.object(generation, "_contract_versions", return_value=different_contracts):
            with self.assertRaisesRegex(generation.CleanRoomError, "immutable identity"):
                self.initialize()
        self.assertEqual(json.loads((state / generation.RUN_MANIFEST).read_text()), intent["manifest"])
        self.assert_snapshot(intent["manifest"], self.original_bytes)
        with patch.object(generation, "run_clean_room_generation", return_value={}) as run:
            generation.resume_clean_room_generation(self.store)
        self.assertEqual(run.call_args.args[0], state / generation.UNIVERSE_SNAPSHOT)
        self.assertEqual(run.call_args.kwargs["request_end_timestamp"], self.end)

    def test_interrupted_legacy_pin_recovers_captured_bytes_without_changing_manifest(self):
        manifest, original_manifest = self.legacy_manifest()
        with patch.object(generation, "atomic_bytes", side_effect=OSError("interrupted pin")):
            with self.assertRaisesRegex(OSError, "interrupted pin"):
                generation.pin_universe(self.store, self.registry)
        publish_union(["NEW"], self.registry)
        generation.pin_universe(self.store, self.registry)
        self.assert_snapshot(manifest, self.original_bytes)
        path = self.store / generation.RUN_STATE_DIRECTORY / generation.RUN_MANIFEST
        self.assertEqual(path.read_bytes(), original_manifest)

    def test_compatible_prior_run_versions_preserve_identity_and_manifest_bytes(self):
        for version in (1, 2):
            with self.subTest(version=version):
                self.store = self.root / f"version{version}"
                manifest, original_manifest = self.legacy_manifest(version, incompatible=False)
                restored, labels = self.initialize()
                self.assertEqual(restored, manifest)
                self.assertEqual(labels, ("AAA",))
                self.assert_snapshot(manifest, self.original_bytes)
                path = self.store / generation.RUN_STATE_DIRECTORY / generation.RUN_MANIFEST
                self.assertEqual(path.read_bytes(), original_manifest)

    def test_only_owned_abandoned_initialization_staging_is_removed(self):
        initialize_v6_store(self.store)
        state = self.store / generation.RUN_STATE_DIRECTORY
        state.mkdir()
        for name in (generation.INITIALIZATION_PENDING, generation.UNIVERSE_SNAPSHOT, generation.RUN_MANIFEST):
            (state / f".{name}.tmp.abandoned").write_bytes(b"partial")
        manifest, _ = self.initialize()
        self.assert_snapshot(manifest, self.original_bytes)
        self.assertFalse(list(state.glob(".*.tmp.*")))

    def test_inconsistent_published_snapshot_is_rejected_without_repair(self):
        self.initialize()
        state = self.store / generation.RUN_STATE_DIRECTORY
        (state / generation.UNIVERSE_SNAPSHOT).write_bytes(b"AAA,NEW\n")
        before = {path.name: path.read_bytes() for path in state.iterdir() if path.is_file()}
        with self.assertRaisesRegex(generation.CleanRoomError, "immutable identity"):
            self.initialize()
        self.assertEqual(before, {path.name: path.read_bytes() for path in state.iterdir() if path.is_file()})


if __name__ == "__main__":
    unittest.main()
