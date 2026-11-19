"""A changed contributor can invalidate the physical seam of a screened legacy range."""

from copy import deepcopy
from unittest.mock import patch

import test_alpaca_gap_suffix_adoption as adoption
from test_alpaca_end_of_day_api import NativeCase, valid_bars

from alpaca_end_of_day import DataLibrary
from alpaca_end_of_day._standalone_group import GroupSnapshot
from libs.alpaca_lineage_build import storage as lineage
from libs.alpaca_serving_catalogue import gap_suffix, policy, producer, storage


class LegacyGapApplicabilityTests(NativeCase):
    def add_contributor_after_screen(self, *, has_gap_bar):
        day, later = adoption.LegacyGapSuffixTests.legacy_groups(self)
        with patch.object(producer, "evaluate", side_effect=OSError("interrupted after screening")):
            self.assertFalse(producer.maintain(self.store)["complete"])
        reviews = storage.selector_state(self.store)["gap_suffix_reviews"]
        (public_id,) = reviews["RECENT"]
        self.assertEqual(reviews["RECENT"][public_id], later)
        # The peer supplies one new session after the old publication. Its raw
        # seam observation, when present, belongs to its separate identity.
        tail = valid_bars(end="2026-10-15")[-1]
        self.add_symbol("PEER", [*([self.bars[2]] if has_gap_bar else []), tail], end="2026-10-15")
        index = lineage.read_index(self.root)
        sources = {name: index["symbols"][name]["source_sha256"] for name in ("RECENT", "PEER")}
        receipt = lineage.read_receipt(self.root, "RECENT", index)
        donor = next(
            segment
            for boundary in lineage.read_boundaries(self.root, receipt).values()
            for view in boundary["adjustments"]
            for segment in view["segments"]
            if segment["historical_symbol"] == "RECENT"
        )
        for symbol in sources:

            def join(member, boundaries, symbol=symbol):
                member.update(input_sources=sources, dependencies=sorted(sources.keys() - {symbol}))
                if symbol == "RECENT":
                    return
                for boundary in boundaries.values():
                    for view in boundary["adjustments"]:
                        if not view["segments"]:
                            continue
                        (whole,) = view["segments"]
                        suffix = {
                            **deepcopy(donor),
                            "row_start": whole["row_end"],
                            "row_end": whole["row_end"],
                            "row_count": 1,
                            "from": tail["t"][:10],
                            "to": tail["t"][:10],
                            "first_timestamp": tail["t"],
                            "last_timestamp": tail["t"],
                        }
                        view["segments"] = [suffix]
                        if has_gap_bar:
                            view["segments"].insert(
                                0,
                                {
                                    **whole,
                                    "row_end": 1,
                                    "row_count": 1,
                                    "to": day,
                                    "last_timestamp": self.bars[2]["t"],
                                },
                            )

            self.publish_change(join, symbol)
        check = policy._bar_reasons

        def new_only(bar, view):
            self.assertIn(bar["session"], {tail["t"][:10], day})
            return check(bar, view)

        with (
            patch.object(gap_suffix, "legacy_start", side_effect=AssertionError("repeated old screening")),
            patch.object(policy, "_bar_reasons", side_effect=new_only),
            patch.object(
                GroupSnapshot,
                "source_raw_presence",
                side_effect=AssertionError("reopened completed raw presence"),
            ),
        ):
            result = producer.maintain(self.store)
        self.assertTrue(result["complete"], result.get("failures"))
        self.assertEqual(storage.selector_state(self.store)["gap_suffix_reviews"], {})
        return public_id, day, later

    def test_raw_bar_on_new_contributor_with_other_owner_expires_screened_absence(self):
        public_id, day, _ = self.add_contributor_after_screen(has_gap_bar=True)
        self.assertFalse(DataLibrary(store=self.store).resolve_ticker("RECENT"))
        group = self.catalogue_groups("PEER")["PEER"]
        decision = next(d for d in group["decisions"] if d["public_id"] == public_id)
        self.assertEqual(decision["reasons"], ["unexplained_missing_sessions"])
        self.assertEqual(group["history_validation"]["raw_gap_presence"]["PEER"]["present_sessions"], [day])

    def test_new_contributor_with_no_raw_bar_preserves_screened_range(self):
        public_id, _, later = self.add_contributor_after_screen(has_gap_bar=False)
        self.assertEqual(DataLibrary(store=self.store).get_security(public_id).first_session.isoformat(), later)

    def test_unassigned_insert_on_existing_contributor_consumes_fresh_observation(self):
        day, later = adoption.LegacyGapSuffixTests.legacy_groups(self)
        group = self.catalogue_groups("RECENT")["RECENT"]
        history = group["history_validation"]
        (record,) = history["prefix_records"]
        public_id = record["public_id"]
        group["_legacy_gap_starts"] = {public_id: later}
        start, carried = gap_suffix.retained_range(group, record, day, record["last_session"])
        self.assertEqual((start, carried), (later, None))
        # The pricing owner delivered its insertion, but lineage left this
        # session unassigned. The completed fresh source presence is decisive.
        inputs = {"RECENT": {"pricing_generation": "new"}}
        self.assertFalse(
            gap_suffix.applicable_absence(
                None,
                [group],
                group,
                record,
                start,
                {day},
                history["identities"][public_id],
                inputs,
                {"RECENT": {day: True}},
            )
        )
