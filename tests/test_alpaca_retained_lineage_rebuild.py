"""A computation revision rebuilds retained decisions, never accepted pricing or evidence."""

import unittest
from unittest.mock import patch

import test_alpaca_end_of_day_security_correlation as correlation_fixture
import test_alpaca_lineage_acquisition as auxiliary_fixture
import test_alpaca_lineage_build as fixture

from libs.alpaca_lineage_build import pipeline, storage
from libs.alpaca_lineage_build.contract import COMPUTATION_REVISION, reusable_decision


class RetainedLineageRebuildTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.LineageBuildTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def legacy(self, names, *, remove_continuation=False):
        case = self.case
        index = storage.read_index(case.root)
        entries = {}
        for name in names:
            receipt = case.receipt(name)
            receipt.pop("computation_revision", None)
            if remove_continuation:
                for field in (
                    "continuation_handoff_version",
                    "identity_memberships",
                    "identity_intervals",
                    "observation_routes",
                ):
                    receipt.pop(field, None)
            storage.atomic_json(case.root / name / "lineage.manifest.json", receipt)
            entry = {**index["symbols"][name], "sha256": storage.document_digest(receipt)}
            entry.pop("computation_revision", None)
            entry["projection"] = {**entry["projection"], "computation_revision": 0}
            entries[name] = entry
        with storage.publication_lock(case.root, exclusive=True):
            storage._update_index(case.root, entries)

    def fun(self):
        case = self.case
        facts = correlation_fixture.fun_like_lineage()
        assets = {item["lookup"]: item["asset"] for item in facts["asset_lookups"]}
        for identity in facts["identities"]:
            for cusip in identity.get("lookup_cusips", ()):
                assets[cusip] = identity
        assets["SIX"] = assets["83001A102"] = None
        facts["corporate_actions"].append(
            {
                "action_type": "cash_dividend",
                "id": "current-dividend",
                "process_date": "2024-07-02",
                "ex_date": "2024-07-02",
                "symbol": "FUN",
                "cusip": "83001C108",
                "rate": "0.1",
            }
        )
        client = fixture.IdentityClient(assets, facts["corporate_actions"])
        bars = fixture.informative_bars("2024-06-01", "2024-07-03")
        for name in ("FUN", "SIX"):
            case.download(name, end="2024-07-03", bars=bars if name == "FUN" else [], from_date="2024-06-01")
        case.run_build(symbols=["FUN"], client=client)
        accepted = case.assignments("FUN")[0]["security_identity"]
        publications = {
            name: case.download(
                name,
                end="2024-07-04",
                bars=[*bars, *fixture.informative_bars("2024-07-03", "2024-07-04")] if name == "FUN" else [],
            )
            for name in ("FUN", "SIX")
        }
        # Produce the actual old append failure through the real publication
        # path, then install its unmarked legacy envelope as a synthetic fixture.
        with patch.object(
            pipeline, "scoped_identity_memberships", side_effect=lambda previous, _: dict.fromkeys(previous, {})
        ):
            case.run_build(symbols=["FUN"], client=client, pricing_handoff=publications)
        self.assertNotEqual(
            case.assignments("FUN")[-1]["security_identity"]["security_id"], accepted["security_id"]
        )
        self.legacy(("FUN", "SIX"), remove_continuation=True)
        return client, accepted

    def test_published_membership_loss_is_rebuilt_from_retained_inputs_then_unchanged(self):
        case = self.case
        client, accepted = self.fun()
        before = {name: case.receipt(name)["source_sha256"] for name in ("FUN", "SIX")}
        with (
            patch.object(client, "get_asset_lookup", side_effect=AssertionError("accepted lookup repeated")),
            patch.object(
                client, "get_corporate_actions", side_effect=AssertionError("accepted actions repeated")
            ),
            patch.object(
                pipeline, "download_pricing", side_effect=AssertionError("accepted pricing reacquired")
            ),
            patch.object(pipeline, "acquire", side_effect=AssertionError("accepted metadata reassembled")),
        ):
            result = pipeline.run_lineage(
                case.store, ["FUN", "SIX"], identity_client=client, retained_rebuild=True
            )
        self.assertEqual(result["failed"], 0, result)
        self.assertFalse(result["results"][0]["reused_decision"])
        self.assertEqual(
            {s["security_identity"]["security_id"] for s in case.assignments("FUN")}, {accepted["security_id"]}
        )
        index = storage.read_index(case.root)
        for name in ("FUN", "SIX"):
            receipt = case.receipt(name)
            self.assertEqual(receipt["source_sha256"], before[name])
            self.assertEqual(receipt["computation_revision"], COMPUTATION_REVISION)
            self.assertTrue(reusable_decision(index["symbols"][name]))
        with (
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("completed prices reopened")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("completed computation repeated")),
            patch.object(
                storage, "read_boundaries", side_effect=AssertionError("completed assignments reopened")
            ),
        ):
            repeated = pipeline.run_lineage(
                case.store, ["FUN", "SIX"], identity_client=client, retained_rebuild=True
            )
        self.assertEqual(repeated["failed"], 0, repeated)
        self.assertTrue(repeated["results"][0]["reused_decision"])

    def test_rebuild_recovers_new_revision_without_repeating_computation(self):
        class Interrupted(BaseException):
            pass

        case = self.case
        client, accepted = self.fun()
        with (
            patch.object(storage, "finish_publication", side_effect=Interrupted()),
            self.assertRaises(Interrupted),
        ):
            pipeline.run_lineage(case.store, ["FUN", "SIX"], identity_client=client, retained_rebuild=True)
        with (
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("journal prices replayed")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("journal computation replayed")),
            patch.object(pipeline, "acquire", side_effect=AssertionError("journal metadata replayed")),
        ):
            result = pipeline.run_lineage(
                case.store, ["FUN", "SIX"], identity_client=client, retained_rebuild=True
            )
        self.assertTrue(result["recovered"])
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(
            case.assignments("FUN")[-1]["security_identity"]["security_id"], accepted["security_id"]
        )

    def test_retained_group_preserves_distinct_requests_and_unavailable_mapping(self):
        case = auxiliary_fixture.AuxiliaryAcquisitionTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        identity = case.prepare_entitlement()
        rejection, _ = case.rejected_client()
        seeded = pipeline.run_lineage(case.store, ["BUYER"], identity_client=identity, pricing_client=rejection)
        self.assertEqual(seeded["failed"], 0, seeded)
        self.case = case
        names = tuple(case.receipt("BUYER")["input_sources"])
        # An accepted empty observation can advance one source's request
        # envelope without changing any of the group's historical rows.
        case.download("TARGET", end="2026-10-16", bars=case.bars[:3])
        index = storage.read_index(case.root)
        previous = {name: case.receipt(name) for name in names}
        outputs = {name: case.receipt(name) for name in names}
        with storage.pricing_locks(case.store, names):
            sources = {
                name: storage.load_source(case.store / "end_of_day/pricing" / name, name) for name in names
            }
            selected = {name: storage.document_digest(source) for name, source in sources.items()}
            for name, receipt in outputs.items():
                source = sources[name]
                receipt.update(
                    input_sources=selected,
                    source_sha256=selected[name],
                    pricing_request={**source["latest_request"], "history_start": source["history_start"]},
                    pricing_boundaries=source["boundaries"],
                    processed_through=source["latest_request"]["end_exclusive"],
                )
            storage.publish(
                case.store,
                case.root,
                index,
                outputs,
                {name: {} for name in names},
                {},
                previous=previous,
                sources=sources,
            )
        self.legacy(names)
        before = {name: case.receipt(name) for name in names}
        self.assertNotEqual(
            before["BUYER"]["pricing_request"]["end_exclusive"],
            before["TARGET"]["pricing_request"]["end_exclusive"],
        )
        # A completed unavailable request can retain a pinned mapping distinct
        # from its coverage end. Rebuild consumes it rather than reselecting it.
        index = storage.read_index(case.root)
        updates = {}
        for name in names:
            receipt = case.receipt(name)
            receipt["unavailable_pricing"]["ENTITLE"]["request"]["asof"] = "2024-02-29"
            storage.atomic_json(case.root / name / "lineage.manifest.json", receipt)
            updates[name] = {**index["symbols"][name], "sha256": storage.document_digest(receipt)}
        with storage.publication_lock(case.root, exclusive=True):
            storage._update_index(case.root, updates)
        with (
            patch.object(
                identity, "get_asset_lookup", side_effect=AssertionError("retained mapping reacquired")
            ),
            patch.object(
                identity, "get_corporate_actions", side_effect=AssertionError("retained actions reacquired")
            ),
            patch.object(pipeline, "download_pricing", side_effect=AssertionError("retained auxiliary retried")),
            patch.object(
                pipeline, "acquire_unpriced", side_effect=AssertionError("retained absence reassembled")
            ),
        ):
            result = pipeline.run_lineage(
                case.store, list(names), identity_client=identity, retained_rebuild=True
            )
        self.assertEqual(result["failed"], 0, result)
        for name in names:
            receipt = case.receipt(name)
            self.assertEqual(receipt["pricing_request"], before[name]["pricing_request"])
            self.assertEqual(receipt["unavailable_pricing"]["ENTITLE"]["request"]["asof"], "2024-02-29")

    def test_incompatible_metadata_or_changed_source_blocks_before_payload_or_provider_work(self):
        for changed_source in (False, True):
            with self.subTest(changed_source=changed_source):
                case = fixture.LineageBuildTests()
                case.setUp()
                self.addCleanup(case.doCleanups)
                case.download()
                case.run_build(symbols=["GOOG"])
                self.case = case
                self.legacy(["GOOG"])
                if changed_source:
                    bars = [*case.bars[:-1], {**case.bars[-1], "v": 12345}]
                    publication = case.download(bars=bars, refresh_all=True)
                    self.assertTrue(publication["changed"])
                else:
                    receipt = case.receipt()
                    receipt["versions"]["identity_report"] += 1
                    index = storage.read_index(case.root)
                    storage.atomic_json(case.root / "GOOG/lineage.manifest.json", receipt)
                    with storage.publication_lock(case.root, exclusive=True):
                        storage._update_index(
                            case.root,
                            {
                                "GOOG": {
                                    **index["symbols"]["GOOG"],
                                    "versions": receipt["versions"],
                                    "sha256": storage.document_digest(receipt),
                                }
                            },
                        )
                identity = fixture.IdentityClient()
                with (
                    patch.object(
                        pipeline, "load_pricing", side_effect=AssertionError("unapproved payload opened")
                    ),
                    patch.object(
                        pipeline,
                        "acquire",
                        side_effect=AssertionError("metadata acquired during retained rebuild"),
                    ),
                    patch.object(
                        identity, "get_asset_lookup", side_effect=AssertionError("provider lookup attempted")
                    ),
                    patch.object(
                        identity,
                        "get_corporate_actions",
                        side_effect=AssertionError("provider actions attempted"),
                    ),
                ):
                    result = pipeline.run_lineage(
                        case.store, ["GOOG"], identity_client=identity, retained_rebuild=True
                    )
                self.assertEqual(result["failed"], 1, result)
                self.assertIn("finish owning recovery first", result["results"][0]["error"])
