"""Scope, recovery and completion of the legacy dated membership selector."""

import json
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_lineage_build import retained_membership, storage
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_serving_catalogue import membership_selection


class LegacyMembershipSelectionTests(TestCase):
    def test_dated_overlap_includes_ineligible_checkpoint_and_excludes_same_owner_or_separate_dates(self):
        def ranges(start, end):
            return [{"ticker": "OLD", "first_session": start, "last_session": end}]

        left = ranges("2024-01-02", "2024-01-03")
        peer = ranges("2024-01-03", "2024-01-04")
        self.assertFalse(membership_selection.overlapping_labels([("A", left + peer)]))
        self.assertFalse(
            membership_selection.overlapping_labels([("A", left), ("B", ranges("2024-01-04", "2024-01-06"))])
        )
        with tempfile.TemporaryDirectory() as temp:
            store = Path(temp)
            path = membership_selection.path(store)
            path.parent.mkdir(parents=True)
            with closing(sqlite3.connect(path)) as conn, conn:
                conn.execute(
                    "CREATE TABLE groups(group_key TEXT PRIMARY KEY, decision_generation TEXT, body TEXT)"
                )
                conn.execute("CREATE TABLE securities(group_key TEXT, public_id TEXT PRIMARY KEY, body TEXT)")
                conn.execute(
                    "INSERT INTO groups VALUES('GROUP','generation',?)",
                    (
                        json.dumps(
                            {
                                "history_validation": {
                                    "prefix_records": [{"public_id": "X", "ticker_ranges": peer}]
                                }
                            }
                        ),
                    ),
                )
                conn.execute(
                    "INSERT INTO securities VALUES('GROUP','A',?)", (json.dumps({"ticker_ranges": left}),)
                )
            selection = {"GROUP": {"decision_generation": "generation", "ineligible_identities": 1}}
            self.assertEqual(membership_selection.read_legacy_overlaps(store, selection), {"GROUP": True})

    def test_changed_peer_and_competing_completed_screen_are_not_overwritten(self):
        refs = {"A": "a", "B": "b", "NO_PRICES": None}
        marker = {"lineage": refs, "serving": "old", "overlap": True}
        for changed in ("peer", "absence", "completed", "screen", "unrelated"):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                state = {"schema_version": 1, "revision": 1, "complete": True}
                index = {"symbols": {s: {"sha256": v} for s, v in refs.items() if v}, "projection_state": state}
                with closing(sqlite3.connect(root / "index.sqlite3")) as conn, conn:
                    conn.execute(
                        "CREATE TABLE projection_state(id INTEGER PRIMARY KEY,schema_version INTEGER,revision INTEGER,complete INTEGER)"
                    )
                    conn.execute("INSERT INTO projection_state VALUES(1,1,2,1)")
                    conn.execute("CREATE TABLE symbols(symbol TEXT PRIMARY KEY,receipt TEXT)")
                    current = {s: dict(v) for s, v in index["symbols"].items()}
                    if changed == "peer":
                        current["B"]["sha256"] = "new"
                    elif changed == "absence":
                        current["NO_PRICES"] = {"sha256": "new"}
                    elif changed == "completed":
                        current["A"][retained_membership.FIELD] = {
                            "lineage": refs,
                            "assessed": "retained_ownership",
                        }
                    elif changed == "screen":
                        current["A"][retained_membership.FIELD] = {**marker, "serving": "new"}
                    conn.executemany(
                        "INSERT INTO symbols VALUES(?,?)", [(s, json.dumps(v)) for s, v in current.items()]
                    )
                if changed == "unrelated":
                    storage.publish_membership_screens(root, index, {"A": marker})
                    self.assertEqual(index["symbols"]["A"][retained_membership.FIELD], marker)
                    self.assertEqual(index["projection_state"], state)
                else:
                    with self.assertRaisesRegex(LineageError, "selected lineage changed"):
                        storage.publish_membership_screens(root, index, {"A": marker})
                    with closing(sqlite3.connect(root / "index.sqlite3")) as conn:
                        self.assertEqual(
                            json.loads(
                                conn.execute("SELECT receipt FROM symbols WHERE symbol='A'").fetchone()[0]
                            ),
                            current["A"],
                        )

    def test_bound_absent_peer_and_serving_scope_select_work_only_when_due(self):
        refs = {"A": "a", "B": "b", "ABSENT": None}
        group = {
            "decision_generation": "one",
            "online_identities": 1,
            "ineligible_identities": 1,
            "references": {"lineage": refs},
        }
        index = {"symbols": {s: {"sha256": v, "versions": {"correlation": 93}} for s, v in refs.items() if v}}

        def select(changed_rules=()):
            return retained_membership.select_legacy_groups(
                Path("."), index, {"groups": {"GROUP": group}}, changed_rules=set(changed_rules)
            )

        with patch.object(membership_selection, "read_legacy_overlaps", return_value={"GROUP": True}) as read:
            index["symbols"]["A"]["versions"]["correlation"] = 92
            index["symbols"]["B"]["versions"]["correlation"] = 94
            self.assertEqual(select(changed_rules={"A"}), {})
            self.assertEqual(select(), {"A": refs})
            index["symbols"]["A"][retained_membership.FIELD] = {
                "lineage": refs,
                "serving": "one",
                "overlap": False,
            }
            self.assertEqual(select(), {})
            self.assertEqual(read.call_count, 1)
            group["decision_generation"] = "two"
            self.assertEqual(select(), {"A": refs})
            self.assertEqual(read.call_count, 2)
            index["symbols"]["A"][retained_membership.FIELD] = {
                "lineage": refs,
                "assessed": "retained_ownership",
            }
            self.assertEqual(select(), {})
            index["symbols"]["ABSENT"] = {"sha256": "new", "versions": {"correlation": 94}}
            self.assertEqual(select(), {})  # Publication remains behind lineage.
            group["references"]["lineage"] = {**refs, "ABSENT": "new"}
            self.assertIn("A", select())
            self.assertEqual(read.call_count, 3)
