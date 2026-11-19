"""A dated numerical selection uses the existing lineage publication boundary."""

from unittest.mock import Mock, patch

import test_alpaca_lineage_build as fixture
from test_alpaca_end_of_day_api import NativeCase
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import DataLibrary
from alpaca_end_of_day._standalone_group import ExportGroup
from libs.alpaca_lineage_build import dated_price_publication as dated
from libs.alpaca_lineage_build import dated_price_repair as repair
from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_build.contract import RecoveryRequired
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download import dated_rename, refresh
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue import export_inputs, work
from libs.alpaca_serving_catalogue.contract import CatalogueError
from libs.alpaca_serving_catalogue.producer import maintain
from libs.common.store_lock import operation_lock


class DatedPublicationTests(NativeCase):
    def setUp(self):
        super().setUp()
        data = self.bootstrap()
        self.data = data
        (security,) = data.list_securities()
        self.group = data._groups["TEST"]
        self.descriptor = {
            "token": "a" * 32,
            "public_id": security.public_id,
            "source_decision": self.group["decision_generation"],
            "first": "2026-10-01",
            "cut": "2026-10-05",
            "witness_end": "2026-10-07",
        }
        self.enterContext(operation_lock(self.store, exclusive=True))

    def _select(self):
        return repair._select(
            self.store, self.descriptor["public_id"], self.descriptor["cut"], Mock(), self.data
        )

    def test_changed_pricing_invalidates_partial_acquisition_before_any_stage_read(self):
        def interrupt(*args, checkpoint, **kwargs):
            checkpoint("scope", {"producer": "completed"})
            raise OSError("interrupted")

        with (
            patch.object(
                ExportGroup, "dated_acquisition_inputs", return_value={mode: {} for mode in ADJUSTMENTS}
            ),
            patch.object(dated_rename, "acquire", side_effect=interrupt),
            self.assertRaisesRegex(OSError, "interrupted"),
        ):
            self._select()
        export_inputs.invalidate(self.store, "pricing", {"TEST": "new-source"})
        with (
            patch.object(
                repair, "acquire_selected", side_effect=AssertionError("reopened stale pricing stages")
            ),
            patch.object(repair, "publish", side_effect=AssertionError("published stale result")),
            self.assertRaisesRegex(CatalogueError, "inputs changed"),
        ):
            self._select()

    def test_only_own_selected_lineage_generation_can_resume(self):
        output = ({mode: {} for mode in ADJUSTMENTS}, self.descriptor.copy())
        with (
            patch.object(
                ExportGroup, "dated_acquisition_inputs", return_value={mode: {} for mode in ADJUSTMENTS}
            ),
            patch.object(dated_rename, "acquire", return_value=output),
            patch.object(storage, "_update_index", side_effect=OSError("interrupted")),
            self.assertRaisesRegex(OSError, "interrupted"),
        ):
            self._select()
        pending = export_inputs.pending_lineage(self.store)
        self.assertIn("TEST", pending)
        export_inputs.invalidate(self.store, "lineage", {"TEST": "another-owner"})
        with (
            patch.object(dated_rename, "acquire", side_effect=AssertionError("reopened pricing")),
            patch.object(storage, "recover_journal", side_effect=AssertionError("installed disputed journal")),
            self.assertRaisesRegex(RecoveryRequired, "another lineage"),
        ):
            self._select()
        export_inputs.invalidate(self.store, "lineage", pending)
        with patch.object(dated_rename, "acquire", side_effect=AssertionError("reopened pricing")):
            with patch.object(storage, "read_index", side_effect=AssertionError("reread installed index")):
                first = self._select()
            second = self._select()
        self.assertEqual(first, second)
        self.assertFalse((self.root / ".pending.json").exists())
        export_inputs.invalidate(self.store, "pricing", {"TEST": "new-source"})
        with (
            patch.object(repair, "acquire_selected", side_effect=AssertionError("consumed obsolete descriptor")),
            self.assertRaisesRegex(CatalogueError, "inputs changed"),
        ):
            self._select()

    def test_frozen_metadata_interruption_and_repeat(self):
        index = storage.read_index(self.root, "TEST")
        previous = {"TEST": storage.read_receipt(self.root, "TEST", index)}
        with (
            patch.object(storage, "read_receipt", side_effect=AssertionError("reopened held receipt")),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("reopened assignments")),
            patch.object(storage, "_acknowledge", side_effect=AssertionError("unrelated upstream work")),
            patch.object(storage, "_recovery_inputs", side_effect=AssertionError("reopened pricing")),
        ):
            with patch.object(storage, "_update_index", side_effect=RuntimeError("interrupt")):
                with self.assertRaisesRegex(RuntimeError, "interrupt"):
                    dated.publish(self.store, self.group, self.descriptor, previous=previous)
            self.assertTrue((self.root / ".pending.json").exists())
            output = dated.publish(self.store, self.group, self.descriptor, previous=previous)
            self.assertEqual(dated.publish(self.store, self.group, self.descriptor), output)
            self.assertFalse((self.root / ".pending.json").exists())
        new_index = storage.read_index(self.root, "TEST")
        self.assertEqual(new_index["symbols"]["TEST"]["sha256"], output["TEST"])
        new = storage.read_receipt(self.root, "TEST", new_index)
        interval = new[dated.FIELD][self.descriptor["public_id"]]
        self.assertEqual(interval["members"], ["TEST"])
        self.assertEqual(interval["unavailable"], [])
        self.assertEqual(interval["token"], self.descriptor["token"])
        self.assertEqual(previous["TEST"]["boundaries"], new["boundaries"])
        self.assertEqual(export_inputs.pending_lineage(self.store), output)
        self.assertEqual(work.read(self.store), output)

    def test_malformed_declared_intervals_fail_closed(self):
        pid = self.descriptor["public_id"]
        descriptor = dated.bound_descriptor(self.group, self.descriptor)
        entry = {dated.TOKENS: {pid: descriptor["token"]}}
        for interval in ({**descriptor, "token": "../source"}, {**descriptor, "members": ["OTHER"]}):
            with self.assertRaisesRegex(RecoveryRequired, "invalid selected"):
                dated.accept({"symbol": "TEST", dated.FIELD: {pid: interval}}, entry)
        with self.assertRaisesRegex(RecoveryRequired, "missing or changed"):
            dated.accept({"symbol": "TEST"}, entry)

    def test_generation_change_rejected_before_receipt_or_publication(self):
        refs = self.group["references"]
        with patch.object(storage, "read_receipt", side_effect=AssertionError("opened stale receipt")):
            for kind in ("lineage", "pricing"):
                group = {**self.group, "references": {**refs, kind: {"TEST": "0" * 64}}}
                with self.subTest(kind=kind), self.assertRaisesRegex(RecoveryRequired, "changed"):
                    dated.publish(self.store, group, self.descriptor)
        self.assertFalse((self.root / ".pending.json").exists())

    def test_scope_carry_and_nearest_invalidation(self):
        pid = self.descriptor["public_id"]
        old = {
            "source_sha256": "old",
            "boundaries": {"a": 1},
            dated.FIELD: {pid: dated.bound_descriptor(self.group, self.descriptor)},
        }
        same = {"source_sha256": "old", "boundaries": {"a": 1}, "input_sources": {"TEST": "old"}}
        dated.retain(old, same)
        self.assertEqual(same[dated.FIELD], old[dated.FIELD])
        changed = {**same, "source_sha256": "new"}
        dated.retain(old, changed)
        self.assertNotIn(dated.FIELD, changed)
        dated.continue_closed(old, changed, "2026-10-07")
        dated.retain(old, changed, continued=True)
        self.assertNotIn(dated.FIELD, changed)
        dated.continue_closed(old, changed, "2026-10-08")
        dated.retain(old, changed, continued=True)
        self.assertEqual(changed[dated.FIELD], old[dated.FIELD])
        changed["unavailable_pricing"] = {"NEW": {}}
        dated.retain(old, changed, continued=True)
        self.assertNotIn(dated.FIELD, changed)

    def test_native_continuation_past_short_retired_peer_then_latest_correction(self):
        case = fixture.LineageBuildTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        names = ("AAA", "BBB")
        identity = fixture.IdentityClient({n: fixture.asset(n, "shared-id") for n in names})
        earlier = fixture.informative_bars("2026-10-01", "2026-10-06")
        current = fixture.informative_bars("2026-10-01", "2026-10-14")
        case.download("AAA", bars=earlier)
        case.download("BBB", bars=current)
        case.run_build(symbols=names, client=identity)
        self.assertTrue(maintain(case.store, bootstrap=True)["complete"])
        data = DataLibrary(store=case.store)
        (security,) = data.list_securities()
        group = data._groups["AAA"]
        desc = {
            **self.descriptor,
            "public_id": security.public_id,
            "source_decision": group["decision_generation"],
        }
        with operation_lock(case.store, exclusive=True):
            dated.publish(case.store, group, desc)
        later = [*current, *fixture.informative_bars("2026-10-14", "2026-10-15")]
        additions = {n: case.download(n, end="2026-10-15", bars=earlier if n == "AAA" else later) for n in names}
        result = case.run_build(symbols=names, client=identity, pricing_handoff=additions)
        self.assertTrue(result["results"][0]["reused_closed_boundaries"])
        for name in names:
            self.assertEqual(
                storage.read_index(case.root, name)["symbols"][name][dated.TOKENS],
                {security.public_id: desc["token"]},
            )

        changed = FakeClient([*later[:-1], {**later[-1], "v": 23456}])
        selection = refresh.check_retained_source(case.store, "BBB", "2026-10-15", client=changed)
        publication = download_pricing(
            case.store, "BBB", "2026-10-15", client=changed, endpoint_selection=selection["endpoint_selection"]
        )
        result = case.run_build(symbols=names, client=identity, pricing_handoff={"BBB": publication})
        self.assertTrue(result["results"][0]["reused_closed_boundaries"])
        for name in names:
            self.assertEqual(
                storage.read_index(case.root, name)["symbols"][name][dated.TOKENS],
                {security.public_id: desc["token"]},
            )

    def test_correction_on_last_witness_invalidates_the_native_interval(self):
        desc = {**self.descriptor, "witness_end": "2026-10-13"}
        dated.publish(self.store, self.group, desc)
        client = FakeClient([*self.bars[:-1], {**self.bars[-1], "v": 23456}])
        selection = refresh.check_retained_source(self.store, "TEST", "2026-10-14", client=client)
        publication = download_pricing(
            self.store, "TEST", "2026-10-14", client=client, endpoint_selection=selection["endpoint_selection"]
        )
        result = run_lineage(
            self.store,
            ["TEST"],
            identity_client=fixture.IdentityClient({"TEST": fixture.asset("TEST")}),
            pricing_handoff={"TEST": publication},
        )
        self.assertTrue(result["results"][0]["reused_closed_boundaries"])
        index = storage.read_index(self.root, "TEST")
        self.assertNotIn(dated.TOKENS, index["symbols"]["TEST"])
        self.assertNotIn(dated.FIELD, storage.read_receipt(self.root, "TEST", index))
