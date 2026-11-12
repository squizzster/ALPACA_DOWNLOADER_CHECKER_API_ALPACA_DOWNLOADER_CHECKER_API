"""A selected manifest crosses its reader boundary once, including aliases."""

from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_durable_maintenance import fixture
from test_alpaca_pricing_download import FakeClient, bar

from alpaca_end_of_day import standalone
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_pricing_download.storage import load_source


class SelectedRowHandoffTests(TestCase):
    def test_manifest_acceptance_is_not_repeated_for_segments_or_alias_views(self):
        with fixture() as case:
            root = case.store / "end_of_day/pricing/AAA"
            source = load_source(root, "AAA")
            read = standalone.read_document
            accepted = []

            class OnceAcceptedManifest(dict):
                def get(self, key, *args):
                    if key in {"schema_version", "symbol", "boundary"}:
                        self.seen[key] = self.seen.get(key, 0) + 1
                        if self.seen[key] > 1:
                            raise AssertionError("selected manifest schema checked again")
                    return super().get(key, *args)

            def incoming(path):
                value = OnceAcceptedManifest(read(path))
                value.seen = {}
                accepted.append(value)
                return value

            cache = {}
            with patch.object(standalone, "read_document", side_effect=incoming):
                first = standalone._rows(root, source, "completed_years", "raw", cache)
                for view in ADJUSTMENTS:
                    standalone._rows(root, source, "completed_years", view, cache)
                self.assertIs(first, standalone._rows(root, source, "completed_years", "raw", cache))
            self.assertEqual(1, len(accepted))
            self.assertEqual({"schema_version": 1, "symbol": 1, "boundary": 1}, accepted[0].seen)

    def test_real_new_generation_does_not_reuse_old_rows_at_the_same_path(self):
        with fixture() as case:
            root = case.store / "end_of_day/pricing/AAA"
            before = load_source(root, "AAA")
            cache = {}
            old = standalone._rows(root, before, "completed_years", "raw", cache)
            saved = deepcopy(old)
            download_pricing(
                case.store,
                "AAA",
                "2026-10-17",
                refresh_all=True,
                client=FakeClient([bar("2016-01-04", "23"), bar("2026-10-16", "25")]),
            )
            after = load_source(root, "AAA")
            self.assertNotEqual(before["boundaries"]["completed_years"], after["boundaries"]["completed_years"])
            new = standalone._rows(root, after, "completed_years", "raw", cache)
            self.assertNotEqual(old, new)
            self.assertEqual("23", new[0][4])
            self.assertEqual(saved, old)
