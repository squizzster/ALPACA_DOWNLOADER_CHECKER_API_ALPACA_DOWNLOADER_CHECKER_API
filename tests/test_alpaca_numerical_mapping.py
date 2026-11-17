"""Generic dated mapping work preserves closed owners and completed observations."""

from copy import deepcopy
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_lineage_build import numerical_mapping as mapping
from libs.alpaca_lineage_build import numerical_mapping_acquisition as acquisition
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.pricing import PricingInput
from libs.alpaca_lineage_build.work_contract import WorkPolicy, numerical_satisfied
from libs.alpaca_pricing_download.responses import CompletedBars


def fixture(old="FUTURE.A", new="NOVA27"):
    owner = {"security_id": "1" * 64, "member_alpaca_asset_ids": ["provider-uuid"]}
    rename = {
        "id": "rename-1",
        "action_type": "name_change",
        "old_symbol": old,
        "new_symbol": new,
        "old_cusip": "000000001",
        "new_cusip": "000000001",
        "process_date": "2020-01-06",
    }
    dividend = {
        "id": "distribution-1",
        "action_type": "cash_dividend",
        "symbol": new,
        "ex_date": "2020-01-06",
        "rate": "0.1",
    }
    facts = {
        "query": {
            "corporate_actions_start": "2016-01-01",
            "corporate_actions_end": "2026-01-01",
            "corporate_actions_data_quality": "complete",
        },
        "queried_symbols": [old, new],
        "corporate_actions": [rename, dividend],
        "asset_lookups": [],
        "identities": [{"id": "provider-uuid"}],
        "unresolved_cusips": [],
    }
    reports, evidence, sources, generations = {}, {}, {}, {}
    for name, first, last, lower, upper in (
        (old, "2016-01-04", "2020-01-03", "2016-01-04", "2020-01-06"),
        (new, "2020-01-06", "2020-01-07", "2020-01-06", None),
    ):
        interval = {
            "historical_symbol": name,
            "asset_id": "provider-uuid",
            "cusip": "000000001",
            "start_date_inclusive": lower,
            "end_date_exclusive": upper,
            "supporting_corporate_action_ids": ["rename-1"],
        }
        reports[name] = {
            "identity_memberships": [deepcopy(owner)],
            "observation_routes": {owner["security_id"]: [last + "T05:00:00Z", interval]},
            "assignment_summary": {
                "all": {
                    "rows": 2,
                    "first_timestamp": first + "T05:00:00Z",
                    "last_timestamp": last + "T05:00:00Z",
                }
            },
            "source_rows": 16,
            "correlated_rows": 16,
            "unresolved_rows": 0,
            "evidence_issues": [],
        }
        sources[name] = {
            "history_start": "2016-01-01T00:00:00Z",
            "latest_request": {"asof": "2026-01-01", "end_exclusive": "2026-01-01T00:00:00Z"},
        }
        evidence[name] = deepcopy(facts)
        generations[name] = ("a" if name == old else "b") * 64
    return sources, generations, reports, evidence


def select(values=None, **options):
    return mapping.select("synthetic-store", *(values or fixture()), selected=True, **options)


class NumericalMappingTests(TestCase):
    def test_fresh_route_is_not_admitted_after_materialization(self):
        sources, generations, reports, evidence = fixture()
        self.assertEqual(
            {},
            mapping.prepare("synthetic-store", sources, generations, reports, evidence, {}, selected=True),
        )

    def test_source_work_selects_retained_need_before_any_materialization(self):
        from libs.alpaca_lineage_build.source_work import _decide_source_work

        sources, generations, reports, evidence = fixture()
        controls = {
            name: SimpleNamespace(source=source, source_sha256=generations[name], previous=reports[name])
            for name, source in sources.items()
        }
        for name in sources:
            reports[name]["source_sha256"] = generations[name]
        with patch(
            "libs.alpaca_lineage_build.source_work._prepare_source_decisions",
            side_effect=AssertionError("mapping must precede source payload reads/correlation"),
        ):
            result = _decide_source_work(
                "synthetic-store",
                None,
                controls,
                evidence,
                {},
                SimpleNamespace(dispositions={name: "fresh" for name in sources}),
                None,
                {},
                loader=None,
                handoff={},
            )
        self.assertIsInstance(result, mapping.PendingNumericalWork)
        self.assertEqual(["NOVA27"], list(result.required))

    def test_pending_obligation_requires_versioned_completed_assessment(self):
        sources, generations, _, _ = values = fixture()
        request = select(values)["NOVA27"]
        item = {"required_numerical_mapping": {"version": 1, "request": request}}
        self.assertFalse(numerical_satisfied(item, {}))
        capability = {"version": 1, "candidate_ids": [request["numerical_mapping"]["candidate_id"]]}
        self.assertTrue(numerical_satisfied(item, {"numerical_mapping_completed": capability}))
        policy = WorkPolicy("synthetic-store", {"symbols": {}}, {"NOVA27": item}, [], "2026-01-01")
        self.assertEqual({"NOVA27": request}, policy.pending_mappings(sources, sources, generations))
        decision = {"result_generation": generations["NOVA27"]}
        completed = {request["numerical_mapping"]["candidate_id"]: decision}
        self.assertEqual({}, policy.pending_mappings(sources, sources, generations, completed=completed))
        changed = {**generations, "FUTURE.A": "changed"}
        with self.assertRaisesRegex(LineageError, "changed source binding"):
            policy.pending_mappings(sources, sources, changed, completed=completed)

    def test_mapping_work_selection_is_bounded_and_survives_missing_acknowledgement(self):
        import json

        from libs.alpaca_lineage_build import universe_state

        sources, generations, _, _ = values = fixture()
        request = select(values)["NOVA27"]
        with TemporaryDirectory() as directory:
            store = Path(directory)
            universe_state.initialize(store)
            policy = WorkPolicy(store, {"symbols": {}}, {}, [], "2026-01-01")
            policy.require_dependencies(sources, "2026-01-01", {"NOVA27"})
            policy.require_mappings({"NOVA27": request})
            # An unrelated pending body is opaque to the selected ingress.
            universe_state.put_work_group(
                store, {"UNRELATED": {"status": "pending", "pricing_correction": "invalid"}}
            )
            state = universe_state.read_mapping_work(store)
            self.assertEqual(["NOVA27"], list(state["work"]))
            restored = WorkPolicy(store, {"symbols": {}}, state["work"], [], "2026-01-01")
            self.assertEqual({"NOVA27": request}, restored.pending_mappings(sources, sources, generations))
            installed = deepcopy(sources)
            installed["NOVA27"]["history_correction_id"] = request["numerical_mapping"]["candidate_id"]
            installed["NOVA27"]["latest_request"]["asof"] = request["mapping_asof"]
            advanced = {**generations, "NOVA27": "installed-generation"}
            self.assertEqual({"NOVA27": request}, restored.pending_mappings(sources, installed, advanced))
            bad = json.dumps({"required_numerical_mapping": {"version": 99}})
            with self.assertRaisesRegex(LineageError, "required numerical mapping"):
                universe_state._read_work(bad)

    def test_daily_preparation_resumes_selected_mapping_before_ordinary_pricing(self):
        from test_alpaca_universe_update import UniverseUpdateTests

        from libs.alpaca_lineage_build import storage, universe_state, universe_update
        from libs.alpaca_lineage_build.context import accept_context

        helper = UniverseUpdateTests("runTest")
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        index = storage.read_index(helper.store / "end_of_day/lineage")
        policy = WorkPolicy(helper.store, index, {}, [], "2026-10-17")
        policy.require("AAA", "2026-10-17", reason="selected_mapping", group=["AAA"])
        request = select(fixture("PREVIOUS", "AAA"))["AAA"]
        policy.require_mappings({"AAA": request})
        selected = universe_state.read_mapping_work(helper.store)
        self.assertEqual(["AAA"], list(selected["work"]))

        class SelectedOwnerReached(BaseException):
            pass

        def resume(store, symbols, **kwargs):
            self.assertEqual(["AAA"], symbols)
            self.assertEqual([], helper.pages, "ordinary pricing ran before selected mapping owner")
            raise SelectedOwnerReached

        with (
            patch.object(universe_update, "run_lineage", side_effect=resume),
            patch.object(
                universe_update, "acquire_selected", side_effect=AssertionError("ordinary acquisition first")
            ),
            self.assertRaises(SelectedOwnerReached),
        ):
            universe_update._prepare_locked(
                helper.store,
                "2026-10-17",
                pricing_client=helper.pricing,
                identity_client=helper.identity,
                registry=helper.registry,
                workers=1,
                progress=None,
                retry_blocked=False,
                coalesce_late_bars=True,
                context=accept_context(helper.store),
                result={"stages": {}, "failed": 0},
            )

    def test_future_names_choose_continuing_observed_date_not_predecessor_or_cutoff(self):
        values = fixture("ORBIT9", "AFTER.PRA")
        required = select(values)
        self.assertEqual(list(required), ["AFTER.PRA"])
        need = required["AFTER.PRA"]
        self.assertEqual(need["mapping_asof"], "2020-01-07")
        self.assertEqual(need["numerical_mapping"]["binding"]["witness_session"], "2016-01-04")
        self.assertIsNone(values[2]["AFTER.PRA"]["observation_routes"]["1" * 64][1]["end_date_exclusive"])

    def test_closed_work_does_not_inspect_provider_or_reports(self):
        self.assertEqual({}, mapping.select(None, None, None, None, None, selected=False))

    def test_real_group_entry_trials_only_retained_route_before_payload_work_and_carries_negative(self):
        from test_alpaca_lineage_build import IdentityClient, LineageBuildTests, asset
        from test_alpaca_pricing_download import bar

        from libs.alpaca_lineage_build import pipeline, storage, universe_state

        helper = LineageBuildTests("runTest")
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        helper.download("FUTURE.A", end="2021-01-01", bars=[bar("2020-01-03")])
        helper.download("NOVA27", end="2021-01-01", bars=[bar("2020-01-06"), bar("2020-01-07")])
        continuing = {**asset("NOVA27", "provider-uuid"), "status": "inactive"}
        actions = fixture()[3]["NOVA27"]["corporate_actions"]
        actions[1]["process_date"] = actions[1]["ex_date"]
        identities = IdentityClient({"NOVA27": continuing, "000000001": continuing}, actions)
        forbidden = SimpleNamespace(fetch=lambda *args, **kwargs: self.fail("fresh/closed group tried mapping"))
        helper.run_build(["NOVA27"], client=identities, pricing_client=forbidden)
        helper.run_build(["NOVA27"], client=identities, pricing_client=forbidden)
        universe_state.initialize(helper.store)
        policy = WorkPolicy(helper.store, storage.read_index(helper.root), {}, [], "2021-01-01")
        events = []
        original = pipeline.load_pricing

        def load(*args, **kwargs):
            events.append("load")
            return original(*args, **kwargs)

        def fetch(symbol, adjustment, start, end, *, asof):
            self.assertEqual([], events, "retained request must precede historical materialization")
            events.append("witness")
            self.assertEqual(("NOVA27", "all", "2020-01-07"), (symbol, adjustment, asof))
            return CompletedBars(symbol, adjustment, start, end, asof, ())

        with patch.object(pipeline, "load_pricing", side_effect=load):
            helper.run_build(
                ["NOVA27"],
                client=identities,
                pricing_client=SimpleNamespace(fetch=fetch),
                refresh=True,
                work_policy=policy,
            )
        self.assertEqual("witness", events[0])
        receipt = helper.receipt("NOVA27")
        self.assertEqual("provider_mapping_unavailable", receipt[mapping.FIELD]["decisions"][0]["outcome"])
        # A later metadata publication carries the completed bounded result;
        # stamping changed receipt bytes cannot renew the same provider trial.
        helper.run_build(
            ["NOVA27"], client=identities, pricing_client=forbidden, refresh=True, work_policy=policy
        )
        self.assertEqual(receipt[mapping.FIELD], helper.receipt("NOVA27")[mapping.FIELD])

    def test_no_facts_cannot_be_replaced_by_retirement(self):
        values = fixture()
        for facts in values[3].values():
            facts["corporate_actions"] = []
            facts["identities"][0]["status"] = "inactive"
        self.assertEqual({}, select(values))

    def test_cash_exdate_in_gap_or_on_first_successor_session_is_not_a_factor_gate(self):
        values = fixture()
        for facts in values[3].values():
            facts["corporate_actions"][1]["ex_date"] = "2020-01-06"
        self.assertTrue(select(values))
        for facts in values[3].values():
            facts["corporate_actions"][1]["rate"] = "1000000"
        self.assertTrue(select(values), "rates are provider observations, never runtime price equations")

    def test_competing_owner_branch_or_unresolved_prefix_prevents_source_wide_replacement(self):
        for mutate in (
            lambda values: values[2]["NOVA27"]["identity_memberships"][0].update(
                member_alpaca_asset_ids=["rival"]
            ),
            lambda values: values[2]["NOVA27"].update(unresolved_rows=1),
            lambda values: values[2]["NOVA27"].update(correlated_rows=15),
            lambda values: values[3]["NOVA27"]["corporate_actions"].append(
                {
                    "id": "other-rename",
                    "action_type": "name_change",
                    "old_symbol": "FUTURE.A",
                    "new_symbol": "RIVAL",
                }
            ),
        ):
            values = fixture()
            mutate(values)
            self.assertEqual({}, select(values))

    def test_different_source_starts_extend_replacement_to_predecessor_scope(self):
        values = fixture()
        values[0]["NOVA27"]["history_start"] = "2020-01-01T00:00:00Z"
        self.assertEqual("2016-01-01", select(values)["NOVA27"]["from"])

    def test_negative_receipt_stamp_does_not_invalidate_its_own_binding(self):
        values = fixture()
        request = select(values)["NOVA27"]
        need = request["numerical_mapping"]
        decision = {
            "candidate_id": need["candidate_id"],
            "binding": need["binding"],
            "outcome": "provider_mapping_unavailable",
            "witness": {},
            "result_generation": values[1]["NOVA27"],
            "scope": {"kind": "one_session_all_witness"},
        }
        mapping.stamp(values[2], {decision["candidate_id"]: decision})
        for report in values[2].values():
            mapping.accept_assessments(report)
            report["finished_at"] = "new publication bookkeeping"
            report["sha256"] = "new receipt identity"
        with patch.object(mapping, "document_digest", wraps=mapping.document_digest) as digest:
            self.assertEqual({}, select(values))
            self.assertEqual(
                digest.call_count, 1, "only the new applicability projection, no receipt authentication"
            )
        changed = deepcopy(values)
        changed[1]["NOVA27"] = "changed-generation"
        self.assertTrue(select(changed))

    def test_assessment_schema_is_accepted_once_and_carried_without_revalidation(self):
        receipt = {mapping.FIELD: {"version": 999, "decisions": []}}
        with self.assertRaises(LineageError):
            mapping.accept_assessments(receipt)
        receipt[mapping.FIELD] = {"version": 1, "decisions": []}
        accepted = mapping.accept_assessments(receipt)
        other = {}
        mapping.carry(receipt, other)
        self.assertIs(accepted, mapping.accept_assessments(other))

    def test_real_pricing_input_complete_history_supplies_exact_owned_dates(self):
        row = ("2020-01-02T05:00:00Z", "1", "1", "1", "1", "100", "1", "1")
        view = PricingInput("FUTURE", {}, "generation", {"raw": (row,)}, {}, {}, "complete_history")
        report = {"correlated": {"segments": [{"adjustment": "raw", "row_start": 1, "row_end": 1}]}}
        self.assertEqual(("2020-01-02",), mapping.required_sessions({"FUTURE": view}, {"FUTURE": report}))
        view.materialization_scope = "retained_suffix"
        self.assertIsNone(mapping.required_sessions({"FUTURE": view}, {"FUTURE": report}))


class NumericalMappingAcquisitionTests(TestCase):
    def execute(self, store, request, client, cache, *, required_sessions=()):
        root = store / "end_of_day/lineage"
        return acquisition.execute(
            store,
            root,
            "NOVA27",
            request,
            client=client,
            download=lambda *args, **kwargs: self.fail("negative witness must not start full acquisition"),
            context=None,
            cache=cache,
            required_sessions=required_sessions,
        )

    def test_empty_response_is_processed_and_survives_restart_without_request(self):
        with TemporaryDirectory() as directory:
            store = Path(directory)
            values = fixture()
            request = mapping.select(store, *values, selected=True)["NOVA27"]
            calls = []

            def fetch(symbol, adjustment, start, end, *, asof):
                calls.append((symbol, adjustment, start, end, asof))
                return CompletedBars(symbol, adjustment, start, end, asof, ())

            result = self.execute(store, request, SimpleNamespace(fetch=fetch), {})
            self.assertEqual("provider_mapping_unavailable", result["mapping_assessment"]["outcome"])
            self.assertEqual(1, len(calls))
            forbidden = SimpleNamespace(fetch=lambda *args, **kwargs: self.fail("completed witness repeated"))
            second = self.execute(store, request, forbidden, {})
            self.assertEqual(result["mapping_assessment"], second["mapping_assessment"])

    def test_missing_internal_grid_fails_closed_but_keeps_the_positive_witness(self):
        with TemporaryDirectory() as directory:
            store = Path(directory)
            request = mapping.select(store, *fixture(), selected=True)["NOVA27"]

            def fetch(symbol, adjustment, start, end, *, asof):
                row = (start.isoformat(), "1", "1", "1", "1", "100", "1", "1")
                return CompletedBars(symbol, adjustment, start, end, asof, (row,))

            with self.assertRaisesRegex(LineageError, "contribution grid"):
                self.execute(store, request, SimpleNamespace(fetch=fetch), {})
            forbidden = SimpleNamespace(
                fetch=lambda *args, **kwargs: self.fail("accepted positive witness repeated")
            )
            with self.assertRaisesRegex(LineageError, "contribution grid"):
                self.execute(store, request, forbidden, {})

    def test_transport_failure_never_becomes_a_provider_negative(self):
        with TemporaryDirectory() as directory:
            store = Path(directory)
            request = mapping.select(store, *fixture(), selected=True)["NOVA27"]
            client = SimpleNamespace(
                fetch=lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("unfinished"))
            )
            cache = {}
            with self.assertRaisesRegex(RuntimeError, "unfinished"):
                self.execute(store, request, client, cache)
            self.assertFalse(cache.get("completed"))

    def test_installed_mapping_resumes_after_assessment_interruption_without_replaying_producers(self):
        from test_alpaca_pricing_download import FakeClient, bar

        from libs.alpaca_pricing_download import adaptive, download, responses
        from libs.alpaca_pricing_download.storage import load_source

        with TemporaryDirectory() as directory:
            store = Path(directory)
            days = ("2016-01-04", "2020-01-03", "2020-01-06", "2020-01-07")
            with patch.object(
                download, "utc_now", return_value=datetime.fromisoformat("2030-01-01T12:00:00+00:00")
            ):
                old = download.download_pricing(
                    store, "NOVA27", "2026-01-01", client=FakeClient([bar(day) for day in days[2:]])
                )
                sources, generations, reports, evidence = fixture()
                root = store / "end_of_day/pricing/NOVA27"
                sources["NOVA27"] = load_source(root, "NOVA27")
                generations["NOVA27"] = old["source_sha256"]
                request = mapping.select(store, sources, generations, reports, evidence, selected=True)["NOVA27"]
                identifier = request["numerical_mapping"]["candidate_id"]
                journal = acquisition.storage.atomic_json

                def interrupt(path, value, **options):
                    if "assessment" in value:
                        raise RuntimeError("installed before assessment acknowledgement")
                    return journal(path, value, **options)

                with patch.object(acquisition.storage, "atomic_json", side_effect=interrupt):
                    with self.assertRaisesRegex(RuntimeError, "before assessment"):
                        acquisition.execute(
                            store,
                            store / "end_of_day/lineage",
                            "NOVA27",
                            request,
                            client=FakeClient([bar(day) for day in days]),
                            download=download.download_pricing,
                            context=None,
                            cache={},
                            required_sessions=days,
                        )
                installed = load_source(root, "NOVA27")
                self.assertEqual(identifier, installed["history_correction_id"])
                forbidden = AssertionError("completed provider/price producer was replayed")
                with (
                    patch.object(responses, "transform", side_effect=forbidden),
                    patch.object(adaptive, "digest", side_effect=forbidden),
                    patch.object(download, "read_validator", side_effect=forbidden),
                    patch.object(download, "read_boundary", side_effect=forbidden),
                    patch.object(mapping, "required_sessions", side_effect=forbidden),
                ):
                    result = acquisition.execute(
                        store,
                        store / "end_of_day/lineage",
                        "NOVA27",
                        request,
                        client=SimpleNamespace(
                            fetch=lambda *args, **kwargs: self.fail("completed request repeated")
                        ),
                        download=download.download_pricing,
                        context=None,
                        cache={},
                        required_sessions=None,
                        grid_provider=lambda: self.fail("completed contribution grid reselected"),
                    )
                self.assertEqual("mapped_history_acquired", result["mapping_assessment"]["outcome"])
                self.assertEqual("completed_correction", result["consistency_check"])

    def test_checkpoint_dates_are_borrowed_once_under_exact_selected_references(self):
        from libs.alpaca_serving_catalogue import storage as serving_storage

        generations = {"ORBIT9": "old-price", "AFTER": "new-price"}
        lineage = {"ORBIT9": "old-lineage", "AFTER": "new-lineage"}
        references = {"pricing": dict(generations), "lineage": dict(lineage)}
        catalogue = {
            "state": {"generation": "catalogue-selected"},
            "members": {name: "group" for name in generations},
            "groups": {"group": {"references": references}},
        }
        body = {
            "history_validation": {
                "prefix_records": [{"public_id": "identity", "session_dates": ["2020-01-03", "2020-01-06"]}]
            }
        }
        context = SimpleNamespace(
            store=Path("/tmp/synthetic-checkpoint"),
            select_serving=lambda: catalogue,
            index={"symbols": {name: {"sha256": value} for name, value in lineage.items()}},
        )
        cache = {}
        with patch.object(serving_storage, "read_groups", return_value={"group": body}) as read:
            self.assertEqual(
                ("2020-01-03", "2020-01-06"),
                mapping.checkpoint_sessions(context, generations, "identity", cache),
            )
            self.assertEqual(
                ("2020-01-03", "2020-01-06"),
                mapping.checkpoint_sessions(context, generations, "identity", cache),
            )
            read.assert_called_once_with(context.store, {"group"}, generation="catalogue-selected")
            references["pricing"]["AFTER"] = "changed"
            self.assertIsNone(mapping.checkpoint_sessions(context, generations, "identity", cache))
            references["pricing"] = dict(generations)
            references["lineage"]["ORBIT9"] = "changed"
            self.assertIsNone(mapping.checkpoint_sessions(context, generations, "identity", cache))
            self.assertEqual(1, read.call_count)
