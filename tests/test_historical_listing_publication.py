"""Metadata-only lineage promotion, bounded retries and accepted output isolation."""

import json
from contextlib import ExitStack, closing, contextmanager
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase, valid_bars

from libs.alpaca_lineage_build import historical_listing_publication as publication
from libs.alpaca_lineage_build import listing_ranges
from libs.alpaca_lineage_build import storage as lineage
from libs.alpaca_lineage_build.contract import LineageError, RecoveryRequired
from libs.alpaca_serving_catalogue import export_inputs, listing_selection, metadata_projection, storage, work
from libs.alpaca_serving_catalogue.contract import CatalogueError
from libs.alpaca_serving_catalogue.producer import maintain
from libs.common.sqlite_connections import connect_sqlite
from libs.common.store_lock import operation_lock


def assessed(symbol, first, last):
    return {
        "symbol": symbol,
        "first_session": first,
        "last_session": last,
        "status": "unresolved",
        "observations": [],
        "changes": [],
        "limits": ["no_recognized_trade_tape"],
        "diagnostics": {"trade_sessions_sampled": 1},
    }


class HistoricalListingPublicationTests(NativeCase):
    def setUp(self):
        super().setUp()
        self.add_symbol("SECOND")
        self.bootstrap()
        _, groups, _, _ = listing_selection.select(self.store, None, None, True)
        self.groups = groups
        self.items = []
        for label, group in groups.items():
            (pid,) = group["ticker_ranges"]
            self.items.append((group, {pid: [assessed(label, "2026-10-02", "2026-10-05")]}))
        self.enterContext(operation_lock(self.store, exclusive=True))

    @contextmanager
    def forbid_revalidation(self, *, receipt=False):
        with ExitStack() as stack:
            for module, names in (
                (
                    lineage,
                    [
                        "read_index",
                        "read_boundaries",
                        "read_receipt",
                        "_recovery_inputs",
                        "_acknowledge",
                        "load_source",
                    ],
                ),
                (metadata_projection, ["selected_metadata_receipts"]),
                (storage, ["read_groups", "commit"]),
            ):
                for name in names:
                    stack.enter_context(patch.object(module, name, side_effect=AssertionError(name)))
            if receipt:
                stack.enter_context(
                    patch.object(lineage, "selected_document", side_effect=AssertionError("accepted receipt"))
                )
            yield

    def test_batch_atomic_metadata_retarget_and_completed_unknown_reuse(self):
        old_ids = {key: g["decision_generation"] for key, g in self.groups.items()}
        with (
            self.forbid_revalidation(),
            patch.object(lineage, "_update_index", wraps=lineage._update_index) as update,
            patch.object(storage, "_write_state", wraps=storage._write_state) as publish,
        ):
            result = publication.publish_many(self.store, self.items)
        self.assertEqual(result, {"published": 2, "already_published": 0})
        self.assertEqual((update.call_count, publish.call_count), (1, 1))
        self.assertFalse(work.read(self.store))
        self.assertFalse(export_inputs.pending_lineage(self.store))
        _, groups, _, facts = listing_selection.select(self.store, None, None, True)
        for g, assessments in self.items:
            key = g["group_key"]
            (pid,) = assessments
            self.assertEqual(groups[key]["decision_generation"], old_ids[key])
            self.assertEqual(groups[key]["references"]["pricing"], g["references"]["pricing"])
            stored = lineage.read_receipt(self.root, key, lineage.read_index(self.root, key))
            self.assertEqual(stored[publication.FIELD]["assessments"], assessments)
            self.assertEqual(facts[key][pid]["historical_listings"][0]["limits"], ["no_recognized_trade_tape"])
        with (
            self.forbid_revalidation(receipt=True),
            patch.object(storage, "_write_state", side_effect=AssertionError("repeat")),
        ):
            self.assertEqual(publication.publish_many(self.store, self.items)["already_published"], 2)
            self.assertEqual(
                publication.publish_many(self.store, [(groups[g["group_key"]], a) for g, a in self.items])[
                    "already_published"
                ],
                2,
            )

    def test_journal_install_and_serving_recovery_only_consume_frozen_outputs(self):
        with (
            self.forbid_revalidation(),
            patch.object(lineage, "_update_index", side_effect=OSError("before lineage selector")),
        ):
            with self.assertRaisesRegex(OSError, "before lineage"):
                publication.publish_many(self.store, self.items)
        self.assertTrue((self.root / ".pending.json").exists())
        with (
            self.forbid_revalidation(receipt=True),
            patch.object(storage, "_write_state", side_effect=OSError("before serving commit")),
        ):
            with self.assertRaisesRegex(OSError, "before serving"):
                publication.publish_many(self.store, self.items)
        self.assertFalse((self.root / ".pending.json").exists())
        self.assertEqual(len(work.read(self.store)), 2)
        with (
            self.forbid_revalidation(receipt=True),
            patch.object(work, "acknowledge", side_effect=OSError("after serving")),
        ):
            with self.assertRaisesRegex(OSError, "after serving"):
                publication.publish_many(self.store, self.items)
        with (
            self.forbid_revalidation(receipt=True),
            patch.object(storage, "_write_state", side_effect=AssertionError("repeat commit")),
        ):
            self.assertEqual(publication.publish_many(self.store, self.items)["already_published"], 2)
        self.assertFalse(work.read(self.store))

    def test_stale_decision_price_label_and_other_lineage_fail_before_receipts(self):
        selection, assessments = self.items[0]
        (pid,) = assessments
        key = selection["group_key"]
        refs = selection["references"]
        invalid = [
            ({**selection, "decision_generation": "another-decision"}, assessments),
            ({**selection, "references": {**refs, "lineage": {key: "a" * 64}}}, assessments),
            ({**selection, "references": {**refs, "pricing": {key: "a" * 64}}}, assessments),
            (selection, {pid: [assessed("ANOTHER", "2026-10-02", "2026-10-05")]}),
            (selection, {"another owner": assessments[pid]}),
            (selection, {pid: [assessed(key, "2000-01-03", "2026-10-05")]}),
        ]
        with self.forbid_revalidation(receipt=True):
            for group, incoming in invalid:
                with (
                    self.subTest(group=group["group_key"], incoming=incoming),
                    self.assertRaises((LineageError, RecoveryRequired)),
                ):
                    publication.publish_historical_listings(self.store, group, incoming)
            export_inputs.invalidate(self.store, "pricing", {key: "changed-price-source"})
            with self.assertRaisesRegex(CatalogueError, "inputs changed"):
                publication.publish_historical_listings(self.store, selection, assessments)

    def test_new_suffix_keeps_completed_earlier_scope_using_one_anchor_only(self):
        selection, incoming = self.items[0]
        key = selection["group_key"]
        (pid,) = incoming
        with self.forbid_revalidation():
            publication.publish_historical_listings(self.store, selection, incoming)

        _, groups, _, _ = listing_selection.select(self.store, [key], None, False)
        added = assessed(key, "2026-10-06", "2026-10-07")
        with (
            self.forbid_revalidation(),
            patch.object(lineage, "selected_document", wraps=lineage.selected_document) as read,
        ):
            publication.publish_historical_listings(self.store, groups[key], {pid: [added]})
        self.assertEqual(read.call_count, 1)
        self.assertEqual(
            lineage.read_receipt(self.root, key, lineage.read_index(self.root, key))[publication.FIELD][
                "assessments"
            ][pid],
            [*incoming[pid], added],
        )
        with (
            self.forbid_revalidation(receipt=True),
            self.assertRaisesRegex(RecoveryRequired, "selection changed"),
        ):
            publication.publish_historical_listings(self.store, selection, incoming)

    def test_interrupted_commit_before_journal_retirement_installs_no_receipt_twice(self):
        from pathlib import Path

        remove = Path.unlink

        def interrupt(path, *args, **kwargs):
            if path == self.root / ".pending.json":
                raise OSError("before journal retirement")
            return remove(path, *args, **kwargs)

        with (
            patch.object(Path, "unlink", interrupt),
            self.assertRaisesRegex(OSError, "before journal retirement"),
        ):
            publication.publish_many(self.store, self.items)
        with (
            self.forbid_revalidation(receipt=True),
            patch.object(lineage, "_install", side_effect=AssertionError("reinstalled committed journal")),
            patch.object(
                publication, "assess_label_boundaries", side_effect=AssertionError("repeated inference")
            ),
        ):
            recovered = publication.recover_pending(self.store, self.groups)
        self.assertEqual(set(recovered["retargeted"]), set(self.groups))
        self.assertEqual(recovered["acknowledged"], 2)
        self.assertEqual(recovered["catalogue_generation"], storage.selector_state(self.store)["generation"])
        self.assertFalse(work.read(self.store))

    def test_selective_acknowledgement_after_serving_commit_needs_no_receipts_or_publication(self):
        with (
            patch.object(work, "acknowledge", side_effect=OSError("ack interrupted")),
            self.assertRaises(OSError),
        ):
            publication.publish_many(self.store, self.items)
        first, other = self.groups
        with (
            self.forbid_revalidation(receipt=True),
            patch.object(storage, "_write_state", side_effect=AssertionError("repeated serving")),
        ):
            recovered = publication.recover_pending(self.store, {first: self.groups[first]})
            self.assertEqual(recovered["retargeted"], {})
            self.assertEqual(recovered["acknowledged"], 1)
            self.assertEqual(
                recovered["affected_public_ids"], {first: list(self.groups[first]["ticker_ranges"])}
            )
            self.assertEqual(set(work.read(self.store)), {other})
            recovered = publication.recover_pending(self.store, self.groups)
            self.assertEqual(recovered["acknowledged"], 1)
            self.assertEqual(publication.recover_pending(self.store, self.groups)["acknowledged"], 0)

    def test_identical_dated_scope_changes_competition_and_can_become_unique_again(self):
        group = self.groups["TEST"]
        (pid,) = group["ticker_ranges"]
        (span,) = group["ticker_ranges"][pid]
        first, last = span["first_session"], span["last_session"]
        row = {
            **assessed("TEST", first, last),
            "status": "sampled",
            "limits": [],
            "observations": [
                {"session": first, "trade_at": first + "T15:00:00Z", "tape": "C", "exchange": "NASDAQ"}
            ],
        }
        with self.forbid_revalidation():
            publication.publish_historical_listings(self.store, group, {pid: [row]})
        (rival,) = self.groups["SECOND"]["ticker_ranges"]
        # The accepted public selector now contains a second eligible owner of the literal label.
        with closing(connect_sqlite(storage.path(self.store))) as db, db:
            (old,) = db.execute("SELECT body FROM securities WHERE public_id=?", (rival,)).fetchone()
            db.execute(
                "UPDATE securities SET body=? WHERE public_id=?",
                (json.dumps({**json.loads(old), "ticker_ranges": [span]}), rival),
            )

        def plan():
            state, groups, owners, facts = listing_selection.select(self.store, None, [pid], False)
            state = publication.bind_catalogue(self.store, state, groups, owners)
            ((g, work_scopes),) = listing_ranges._work(groups, owners, facts, [pid])
            self.assertEqual(g["superseded_listing_scopes"][pid], [("TEST", first, last)])
            return state, groups, g, work_scopes, facts

        state, groups, g, scopes, _ = plan()
        self.assertEqual(scopes, [(pid, "TEST", first, last, True)])
        unknown = {**assessed("TEST", first, last), "limits": ["competing_published_owners"]}
        with (
            self.forbid_revalidation(receipt=True),
            self.assertRaisesRegex(RecoveryRequired, "current selected scope"),
        ):
            publication.publish_historical_listings(self.store, dict(g), {pid: [unknown]})
        with (
            patch.object(storage, "retarget_historical_decisions", side_effect=OSError("interruption")),
            self.assertRaises(OSError),
        ):
            publication.publish_historical_listings(self.store, g, {pid: [unknown]}, accepted_state=state)
        with self.forbid_revalidation(receipt=True):
            recovered = publication.recover_pending(self.store, groups, accepted_state=state)
            self.assertEqual(recovered["affected_public_ids"], {"TEST": [pid]})
            self.assertEqual(recovered["retargeted"]["TEST"][publication.FIELD]["assessments"][pid], [unknown])
            self.assertEqual(
                publication.publish_historical_listings(self.store, g, {pid: [unknown]}, accepted_state=state)[
                    "already_published"
                ],
                1,
            )
        with closing(connect_sqlite(storage.path(self.store))) as db, db:
            db.execute("UPDATE securities SET body=? WHERE public_id=?", (old, rival))
        state, _, g, scopes, facts = plan()
        self.assertEqual(scopes, [(pid, "TEST", first, last, False)])
        self.assertEqual(facts["TEST"][pid]["historical_listings"][0]["observations"], [])
        with self.forbid_revalidation():
            self.assertEqual(
                publication.publish_historical_listings(self.store, g, {pid: [row]}, accepted_state=state)[
                    "published"
                ],
                1,
            )
        receipt = lineage.read_receipt(self.root, "TEST", lineage.read_index(self.root, "TEST"))
        self.assertEqual(receipt[publication.FIELD]["assessments"][pid], [row])

    def test_former_group_anchor_releases_stale_claim_and_cross_label_change(self):
        pid = "owner"
        tags = {"OLD": ("2026-10-02", "A"), "NEW": ("2026-10-05", "C")}
        ranges, rows, sources, entries = [], {}, {}, {}
        for name, (day, tape) in tags.items():
            ranges.append({"ticker": name, "first_session": day, "last_session": day})
            rows[name] = {
                **assessed(name, day, day),
                "status": "sampled",
                "limits": [],
                "observations": [{"session": day, "tape": tape, "trade_at": day + "T15:00:00Z"}],
            }
            sources[name] = {
                publication.FIELD: {"version": 1, "assessments": {pid: [rows[name]]}},
                "unrelated": name,
            }
            entries[name] = ({publication.RANGES: {pid: [[name, day, day]]}}, None)
        old_change = publication.assess_label_boundaries(list(rows.values()))
        self.assertEqual(len(old_change), 1)
        sources["OLD"][publication.FIELD]["label_changes"] = {pid: old_change}
        group = {
            "group_key": "group",
            "decision_generation": "changed",
            "references": {"pricing": {"OLD": "first", "NEW": "second"}},
            "ticker_ranges": {pid: ranges},
        }
        claim = {"superseded": {pid: [["OLD", "2026-10-02", "2026-10-02"]]}}
        changed = {**assessed("OLD", "2026-10-02", "2026-10-02"), "limits": ["competing_published_owners"]}
        with patch.object(
            publication, "_receipt", side_effect=lambda store, root, symbol, entry: sources[symbol]
        ):
            outputs = publication._merge(self.store, self.root, group, {pid: [changed]}, entries, "NEW", claim)
        self.assertEqual(outputs["OLD"], ({"unrelated": "OLD"}, {}))
        anchor, _ = outputs["NEW"]
        self.assertEqual(anchor["unrelated"], "NEW")
        self.assertNotIn("label_changes", anchor[publication.FIELD])
        self.assertCountEqual(anchor[publication.FIELD]["assessments"][pid], [changed, rows["NEW"]])

    def test_daily_suffix_joins_same_label_native_endpoints_and_recovery_carries_once(self):
        group = self.groups["TEST"]
        (pid,) = group["ticker_ranges"]
        (span,) = group["ticker_ranges"][pid]

        def sample(first, last, tape):
            return {
                **assessed("TEST", first, last),
                "status": "sampled",
                "limits": [],
                "observations": [{"session": last, "tape": tape, "trade_at": last + "T15:00:00Z"}],
            }

        old = sample(span["first_session"], span["last_session"], "A")
        self.assertEqual(old["last_session"], "2026-10-13")
        publication.publish_historical_listings(self.store, group, {pid: [old]})
        initial = lineage.read_receipt(self.root, "TEST", lineage.read_index(self.root, "TEST"))[
            publication.FIELD
        ]
        self.assertNotIn("range_changes", initial)
        for day, end in (("2026-10-14", "2026-10-15"), ("2026-10-15", "2026-10-16")):
            self.add_symbol("TEST", valid_bars(end=end), end=end)
            self.assertTrue(maintain(self.store)["complete"])
            state, groups, owners, facts = listing_selection.select(self.store, None, [pid], False)
            ((g, scopes),) = listing_ranges._work(groups, owners, facts, [pid])
            self.assertEqual(scopes, [(pid, "TEST", day, day, False)])
            new = sample(day, day, "C")
            with (
                self.forbid_revalidation(),
                patch.object(
                    publication, "assess_label_boundaries", wraps=publication.assess_label_boundaries
                ) as detect,
            ):
                if day == "2026-10-14":
                    with (
                        patch.object(
                            storage, "retarget_historical_decisions", side_effect=OSError("interrupted")
                        ),
                        self.assertRaises(OSError),
                    ):
                        publication.publish_historical_listings(
                            self.store, g, {pid: [new]}, accepted_state=state
                        )
                    with patch.object(
                        lineage, "selected_document", side_effect=AssertionError("reopened accepted source")
                    ):
                        publication.recover_pending(self.store, groups, accepted_state=state)
                else:
                    publication.publish_historical_listings(self.store, g, {pid: [new]}, accepted_state=state)
                self.assertEqual(detect.call_count, 1)
                self.assertEqual(detect.call_args.kwargs["windows"], {(day, day)})
                self.assertTrue(detect.call_args.kwargs["include_same_symbol"])
                self.assertEqual(
                    publication.publish_historical_listings(self.store, g, {pid: [new]}, accepted_state=state)[
                        "already_published"
                    ],
                    1,
                )
                self.assertEqual(detect.call_count, 1)

        receipt = lineage.read_receipt(self.root, "TEST", lineage.read_index(self.root, "TEST"))
        envelope = receipt[publication.FIELD]
        self.assertNotIn("label_changes", envelope)
        (bridge,) = envelope["range_changes"][pid]
        self.assertEqual(
            (bridge["from_session"], bridge["to_session"], bridge["effective_session"]),
            ("2026-10-13", "2026-10-14", "2026-10-14"),
        )
        self.assertEqual(
            (bridge["from_symbol"], bridge["to_symbol"], bridge["from_tape"], bridge["to_tape"]),
            ("TEST", "TEST", "A", "C"),
        )
        self.assertTrue(all(not row["changes"] for row in envelope["assessments"][pid]))
