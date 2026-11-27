"""Ownership-change invalidation of accepted historical listing scope."""

from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_lineage_build.listing_ranges import COMPETING, _work


def row(first="2026-10-02", last="2026-10-09", *, competing=False):
    samples = (
        []
        if competing
        else [
            {"session": d, "trade_at": d + "T17:00:00Z", "tape": t}
            for d, t in (("2026-10-02", "A"), ("2026-10-05", "B"), ("2026-10-07", "B"), ("2026-10-09", "C"))
            if first <= d <= last
        ]
    )
    changes = [
        {
            "from_session": a["session"],
            "to_session": b["session"],
            "from_tape": a["tape"],
            "to_tape": b["tape"],
            **({"effective_session": b["session"]} if b["session"] == "2026-10-05" else {}),
        }
        for a, b in zip(samples, samples[1:])
        if a["tape"] != b["tape"]
    ]
    return {
        "symbol": "T",
        "first_session": first,
        "last_session": last,
        "status": "changes_observed" if changes else "sampled" if samples else "unresolved",
        "observations": samples,
        "changes": changes,
        "limits": [COMPETING]
        if competing
        else ["sampled_sessions_only", "multiple_listings_on_tape_b", "change_date_bounded"],
    }


class ListingRangeTests(TestCase):
    def setUp(self):
        self.a, self.b = "a" * 64, "b" * 64
        first, last = "2026-10-02", "2026-10-09"
        spans = [{"ticker": "T", "first_session": first, "last_session": last}]
        self.group = {"ticker_ranges": {self.a: spans}}
        self.groups = {"g": self.group}
        self.owners = {"T": [(self.a, first, last)]}
        self.facts = {"g": {self.a: {"historical_listings": [row()]}}}

    def work(self, ids=None):
        return list(_work(self.groups, self.owners, self.facts, ids))

    def test_later_overlap_retires_old_claim_preserving_points_and_wholly_contained_transitions(self):
        self.owners["T"].append((self.b, "2026-10-07", "2026-10-07"))
        ((selected, scopes),) = self.work()
        self.assertIs(selected, self.group)
        expected = [
            (self.a, "T", "2026-10-02", "2026-10-06", False),
            (self.a, "T", "2026-10-07", "2026-10-07", True),
            (self.a, "T", "2026-10-08", "2026-10-09", False),
        ]
        self.assertEqual(scopes, expected)
        self.assertEqual(selected["listing_work_scopes"], scopes)
        self.assertEqual(selected["superseded_listing_scopes"], {self.a: [("T", "2026-10-02", "2026-10-09")]})
        carried = selected["carried_listing_assessments"]
        self.assertEqual(set(carried), {expected[0], expected[2]})
        before, after = carried[expected[0]], carried[expected[2]]
        self.assertEqual([p["session"] for p in before["observations"]], ["2026-10-02", "2026-10-05"])
        self.assertEqual([c["effective_session"] for c in before["changes"]], ["2026-10-05"])
        self.assertNotIn("change_date_bounded", before["limits"])
        self.assertEqual(after["changes"], [])
        self.assertNotIn("multiple_listings_on_tape_b", after["limits"])
        self.assertEqual(before["diagnostics"]["retained_from"][0]["last_session"], "2026-10-09")
        self.facts["g"][self.a]["historical_listings"] = [before, row(*expected[1][2:4], competing=True), after]
        self.assertEqual(self.work(), [])
        self.assertEqual(self.group["superseded_listing_scopes"], {})

    def test_retired_competitor_reopens_only_sessions_which_have_become_unambiguous(self):
        self.facts["g"][self.a]["historical_listings"] = [row(competing=True)]
        self.owners["T"].extend([(self.b, "2026-10-05", "2026-10-06"), ("c" * 64, "2026-10-06", "2026-10-07")])
        ((group, scopes),) = self.work()
        self.assertEqual(
            [s[2:] for s in scopes],
            [
                ("2026-10-02", "2026-10-02", False),
                ("2026-10-05", "2026-10-07", True),
                ("2026-10-08", "2026-10-09", False),
            ],
        )
        carried = group["carried_listing_assessments"]
        self.assertEqual(list(carried), [scopes[1]])
        self.assertEqual(carried[scopes[1]]["limits"], [COMPETING])
        self.owners["T"] = self.owners["T"][:1]
        ((group, scopes),) = self.work()
        self.assertEqual(scopes, [(self.a, "T", "2026-10-02", "2026-10-09", False)])
        self.assertEqual(group["carried_listing_assessments"], {})

    def test_public_id_bounds_work_with_siblings_retained_as_competitors(self):
        self.group["ticker_ranges"][self.b] = [
            {"ticker": "T", "first_session": "2026-10-06", "last_session": "2026-10-07"}
        ]
        self.facts["g"][self.b] = {}
        self.owners["T"].append((self.b, "2026-10-06", "2026-10-07"))
        ((group, scopes),) = self.work([self.a.upper()])
        self.assertEqual({s[0] for s in scopes}, {self.a})
        self.assertEqual(set(group["ticker_ranges"]), {self.a, self.b})
        self.assertTrue(any(s[-1] for s in scopes))
        self.assertEqual({s[0] for s in self.work()[0][1]}, {self.a, self.b})

    def test_recovered_sibling_replaces_invalidated_assessments_only(self):
        self.group["ticker_ranges"][self.b] = [
            {"ticker": "T", "first_session": "2026-10-06", "last_session": "2026-10-07"},
            {"ticker": "T", "first_session": "2026-11-02", "last_session": "2026-11-05"},
            {"ticker": "NEW", "first_session": "2026-11-06", "last_session": "2026-11-09"},
        ]
        for span in self.group["ticker_ranges"][self.b]:
            self.owners.setdefault(span["ticker"], []).append(
                (self.b, span["first_session"], span["last_session"])
            )
        self.facts["g"][self.b] = {"historical_listings": [row("2026-10-06", "2026-10-07")]}
        ((group, scopes),) = _work(self.groups, self.owners, self.facts, [self.a], {"g": {self.a, self.b}})
        self.assertEqual(
            [s for s in scopes if s[0] == self.b], [(self.b, "T", "2026-10-06", "2026-10-07", True)]
        )
        self.assertEqual(group["superseded_listing_scopes"][self.b], [("T", "2026-10-06", "2026-10-07")])

    def test_unchanged_scope_never_enumerates_sessions_and_weekend_overlap_keeps_accepted_scope(self):
        with patch(
            "libs.alpaca_lineage_build.listing_ranges.market_sessions_between",
            side_effect=AssertionError("enumerated accepted sessions"),
        ):
            self.assertEqual(self.work(), [])
        self.owners["T"].append((self.b, "2026-10-03", "2026-10-04"))
        self.assertEqual(self.work(), [])
