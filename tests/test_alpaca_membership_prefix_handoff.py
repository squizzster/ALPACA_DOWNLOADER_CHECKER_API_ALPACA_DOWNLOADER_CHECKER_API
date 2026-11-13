"""Completed membership donors survive assignment subdivision within their scope."""

from copy import deepcopy
from unittest import TestCase

from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.membership_transfer import carry_prefix


class MembershipPrefixHandoffTests(TestCase):
    def fixture(self, *, rollover=False):
        original = {
            "source": "AAA",
            "adjustment": "raw",
            "boundary": "current_month",
            "row_start": 1,
            "row_end": 2,
            "from": "2024-01-30",
            "to": "2024-01-31",
            "old_public_id": "original",
            "new_public_id": "accepted",
            "labels": ["AAA"],
        }
        donor = {
            "version": 1,
            "source": "AAA",
            "previous_receipt_generation": "original-receipt",
            "previous_pricing_generation": "original-prices",
            "pricing_generation": "accepted-prices",
            "previous_boundaries": {"original": "completed"},
            "identities": {"original": {"accepted": True}},
            "runs": [original],
        }
        previous = {"anchor_month": "2024-01", "membership_only_rebind": donor}
        fragments = [
            {
                "row_start": position,
                "row_end": position,
                "from": day,
                "to": day,
                "owner": "accepted",
                "state": "selected",
                "tickers": ["AAA"],
            }
            for position, day in enumerate(("2024-01-30", "2024-01-31"), 1)
        ]
        current = {
            "anchor_month": "2024-02" if rollover else "2024-01",
            "source_sha256": "appended-prices",
            "assignment_handoffs": {},
        }
        for kind in ("completed_years", "current_year", "current_month"):
            runs = fragments if kind == ("current_year" if rollover else "current_month") else []
            current["assignment_handoffs"][kind] = {
                "ownership": {"raw": {"rows": len(runs)}},
                "observation_choices": {"partitions": {"raw": {"runs": runs}}, "views": {"raw": "raw"}},
            }
        return previous, current, {"raw": {"rows": 2}}

    def test_same_boundary_keeps_the_original_complete_donor_run(self):
        previous, current, prefix = self.fixture()
        result = carry_prefix(previous, current, prefix)
        self.assertEqual({**previous["membership_only_rebind"], "pricing_generation": "appended-prices"}, result)
        self.assertIs(previous["membership_only_rebind"]["runs"][0], result["runs"][0])

    def test_changed_owner_or_label_requires_owning_recovery(self):
        for field, value in (("owner", "different"), ("tickers", ["OTHER"])):
            previous, current, prefix = self.fixture()
            current["assignment_handoffs"]["current_month"]["observation_choices"]["partitions"]["raw"]["runs"][
                0
            ][field] = value
            with (
                self.subTest(field=field),
                self.assertRaisesRegex(LineageError, "changed membership-transfer selection"),
            ):
                carry_prefix(previous, current, prefix)

    def test_rollover_relocates_positions_without_rebinding_the_donor(self):
        previous, current, prefix = self.fixture(rollover=True)
        result = carry_prefix(previous, current, prefix)
        self.assertEqual(2, len(result["runs"]))
        self.assertEqual({"current_year"}, {run["boundary"] for run in result["runs"]})
        self.assertEqual("original-receipt", result["previous_receipt_generation"])
        self.assertEqual("original-prices", result["previous_pricing_generation"])
        changed = deepcopy(current)
        changed["assignment_handoffs"]["current_year"]["observation_choices"]["partitions"]["raw"]["runs"][1][
            "owner"
        ] = "other"
        with self.assertRaisesRegex(LineageError, "changed membership-transfer selection"):
            carry_prefix(previous, changed, prefix)

    def test_changed_prefix_extent_rejects_before_opening_current_assignments(self):
        previous, _, _ = self.fixture()
        with self.assertRaisesRegex(LineageError, "changed membership-transfer donor positions"):
            carry_prefix(previous, {}, {"raw": {"rows": 1}})
