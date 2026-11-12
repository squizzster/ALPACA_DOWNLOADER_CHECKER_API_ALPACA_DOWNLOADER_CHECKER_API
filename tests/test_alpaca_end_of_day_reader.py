"""Consumers cannot replace a missing coherent producer choice."""

import unittest

from libs.alpaca_end_of_day_reader import EndOfDayReaderError
from libs.alpaca_end_of_day_reader.history import require_unique_observation


class EndOfDayReaderTests(unittest.TestCase):
    def test_competing_selected_observations_require_owning_recovery(self):
        with self.assertRaisesRegex(EndOfDayReaderError, "generation-bound lineage recover"):
            require_unique_observation("2024-01-02T05:00:00Z", 2)

    def test_completed_unique_choice_is_consumed(self):
        require_unique_observation("2024-01-02T05:00:00Z", 1)
