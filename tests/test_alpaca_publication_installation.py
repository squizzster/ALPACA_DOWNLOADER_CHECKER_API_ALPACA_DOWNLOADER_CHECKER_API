"""Completed staging acceptance survives installation and process interruption."""

import json
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_pricing_download import FakeClient, bar

from libs.alpaca_pricing_download import storage
from libs.alpaca_pricing_download.contract import DownloadError, json_bytes
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views


class Interrupted(BaseException):
    pass


class PublicationInstallationTests(TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.root = self.store / "end_of_day/pricing/SAME"
        self.bars = [bar("2024-01-02"), bar("2024-01-03"), bar("2024-01-04")]
        self.cutoff = "2024-01-05"
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
        )

    def download(self):
        client = FakeClient(self.bars)
        result = download_pricing(self.store, "SAME", self.cutoff, from_date="2024-01-01", client=client)
        return client, result

    def no_publication_reads(self, path, *, purpose, **kwargs):
        self.assertNotIn(purpose, {"publication_staged_validation", "publication_installed_validation"})
        self.assertNotEqual(".gz", path.suffix, "accepted publication payload reopened")
        return self.read_artifact(path, purpose=purpose, **kwargs)

    def test_normal_install_consumes_producer_digests_without_file_reads_or_rehashing(self):
        self.read_artifact = storage.read_artifact
        original = storage.digest
        hashes = Counter()

        def first_digest(data):
            self.assertFalse(data.startswith(b"\x1f\x8b"), "producer gzip digest repeated")
            value = json.loads(data)
            self.assertIn(
                value.get("boundary"),
                (None, "alpaca_standalone_pricing_source"),
                "producer boundary/validator digest repeated",
            )
            hashes[data] += 1
            self.assertEqual(1, hashes[data], "completed publication digest repeated")
            return original(data)

        with (
            patch.object(storage, "read_artifact", side_effect=self.no_publication_reads),
            patch.object(storage, "digest", side_effect=first_digest),
        ):
            _, result = self.download()
        self.assertTrue(result["changed"])
        self.assertEqual({3}, {len(rows) for rows in read_pricing_views(self.store, "SAME").values()})

    def test_interrupted_payload_selector_and_notice_renames_retain_acceptance(self):
        for boundary in ("payload", "source", "update"):
            with self.subTest(boundary=boundary):
                # Give each interruption its own producer scope and stable lock.
                with tempfile.TemporaryDirectory() as directory:
                    self.store = Path(directory) / "store"
                    self.root = self.store / "end_of_day/pricing/SAME"
                    original = storage.os.replace

                    def interrupted(source, target):
                        original(source, target)
                        selected = (
                            boundary == "payload"
                            and Path(target).suffix == ".gz"
                            or boundary == "source"
                            and Path(target).name == "source.manifest.json"
                            or boundary == "update"
                            and Path(target).name == "update.json"
                        )
                        if selected:
                            raise Interrupted()

                    with (
                        patch.object(storage.os, "replace", side_effect=interrupted),
                        self.assertRaises(Interrupted),
                    ):
                        self.download()
                    pending = storage.read_document(self.root / ".pending.json")
                    self.assertEqual(1, pending["installation_handoff_version"])
                    self.read_artifact = storage.read_artifact
                    with (
                        patch.object(storage, "read_artifact", side_effect=self.no_publication_reads),
                        patch.object(storage, "digest", side_effect=AssertionError("completed output rehashed")),
                    ):
                        with storage.ticker_lock(self.root, exclusive=True):
                            self.assertTrue(storage.recover(self.root))
                    self.assertFalse((self.root / ".pending.json").exists())
                    self.assertEqual(
                        {3}, {len(rows) for rows in read_pricing_views(self.store, "SAME").values()}
                    )

    def leave_uninstalled_journal(self):
        with (
            patch.object(storage, "_finish_publication", side_effect=Interrupted()),
            self.assertRaises(Interrupted),
        ):
            self.download()
        return storage.read_document(self.root / ".pending.json")

    def test_unmarked_legacy_journal_accepts_each_selected_output_once(self):
        journal = self.leave_uninstalled_journal()
        journal.pop("installation_handoff_version")
        (self.root / ".pending.json").write_bytes(json_bytes(journal))
        original = storage.read_artifact
        output_reads = Counter()

        def once(path, *, purpose, output_path=None, **kwargs):
            if purpose.startswith("publication_"):
                self.assertEqual("publication_staged_validation", purpose, "just-installed output reread")
                output_reads[str(output_path)] += 1
                self.assertEqual(1, output_reads[str(output_path)])
            elif path.parent.name == ".staging":
                self.fail("legacy update was parsed by rereading its staged file")
            return original(path, purpose=purpose, output_path=output_path, **kwargs)

        with patch.object(storage, "read_artifact", side_effect=once):
            with storage.ticker_lock(self.root, exclusive=True):
                self.assertTrue(storage.recover(self.root))
        self.assertEqual(len(journal["writes"]) + (journal["update"] is not None), len(output_reads))

    def test_unknown_installation_handoff_fails_before_payload_access(self):
        journal = self.leave_uninstalled_journal()
        journal["installation_handoff_version"] = 2
        (self.root / ".pending.json").write_bytes(json_bytes(journal))
        original = storage.read_artifact

        def journal_only(path, **kwargs):
            self.assertEqual(".pending.json", path.name)
            return original(path, **kwargs)

        with patch.object(storage, "read_artifact", side_effect=journal_only):
            with storage.ticker_lock(self.root, exclusive=True):
                with self.assertRaisesRegex(DownloadError, "installation handoff"):
                    storage.recover(self.root)

    def test_declared_missing_summary_handoff_cannot_reopen_installed_metadata(self):
        journal = self.leave_uninstalled_journal()
        journal.pop("summary_handoff")
        (self.root / ".pending.json").write_bytes(json_bytes(journal))
        original = storage.read_artifact

        def journal_only(path, **kwargs):
            self.assertEqual(".pending.json", path.name)
            return original(path, **kwargs)

        with patch.object(storage, "read_artifact", side_effect=journal_only):
            with storage.ticker_lock(self.root, exclusive=True):
                with self.assertRaisesRegex(DownloadError, "summary handoff"):
                    storage.recover(self.root)

    def test_summary_recovery_consumes_held_rows_and_committed_generation_without_replay(self):
        from libs.alpaca_pricing_download import check_summary as summary

        for completed_summary in (False, True):
            with self.subTest(completed_summary=completed_summary), tempfile.TemporaryDirectory() as directory:
                self.store = Path(directory) / "store"
                self.root = self.store / "end_of_day/pricing/SAME"
                self.bars = [bar("2024-01-02"), bar("2024-01-03"), bar("2024-01-04")]
                self.cutoff = "2024-01-05"
                self.download()
                source = storage.load_source(self.root, "SAME")
                endpoints = storage.read_validator(self.root, source)
                token = summary.begin_rebuild(self.store)
                summary.finish_rebuild(
                    self.store, token, [summary.price_entry(self.root, source, endpoints=endpoints)]
                )
                self.cutoff = "2024-01-06"
                self.bars.append(bar("2024-01-05"))
                replace, unlink = storage.os.replace, Path.unlink

                def stop_selector(staged, target):
                    replace(staged, target)
                    if not completed_summary and Path(target).name == "source.manifest.json":
                        raise Interrupted()

                def stop_acknowledgement(path, *args, **kwargs):
                    if completed_summary and path == self.root / ".pending.json":
                        raise Interrupted()
                    return unlink(path, *args, **kwargs)

                with (
                    patch.object(storage.os, "replace", side_effect=stop_selector),
                    patch.object(Path, "unlink", new=stop_acknowledgement),
                    self.assertRaises(Interrupted),
                ):
                    self.download()
                read = storage.read_artifact
                price_entry = summary.price_entry
                calls = []

                def journal_only(path, **kwargs):
                    self.assertEqual(".pending.json", path.name, "accepted installed metadata reopened")
                    return read(path, **kwargs)

                def first_summary(*args, **kwargs):
                    self.assertFalse(completed_summary, "committed summary calculation repeated")
                    calls.append(True)
                    return price_entry(*args, **kwargs)

                with (
                    patch.object(storage, "read_artifact", side_effect=journal_only),
                    patch.object(
                        storage, "read_validator", side_effect=AssertionError("accepted validator reread")
                    ),
                    patch.object(storage, "load_source", side_effect=AssertionError("accepted source reread")),
                    patch.object(summary, "price_entry", side_effect=first_summary),
                ):
                    with storage.ticker_lock(self.root, exclusive=True):
                        self.assertTrue(storage.recover(self.root))
                self.assertEqual(0 if completed_summary else 1, len(calls))
                self.assertEqual(4, len(read_pricing_views(self.store, "SAME")["all"]))

    def test_unaccepted_legacy_staging_mismatch_still_requires_owning_recovery(self):
        journal = self.leave_uninstalled_journal()
        journal.pop("installation_handoff_version")
        (self.root / ".pending.json").write_bytes(json_bytes(journal))
        (self.root / ".staging/write-000").write_bytes(b"not the unaccepted legacy output")
        with storage.ticker_lock(self.root, exclusive=True):
            with self.assertRaisesRegex(DownloadError, "staged pricing output"):
                storage.recover(self.root)
        self.assertFalse((self.root / "source.manifest.json").exists())
