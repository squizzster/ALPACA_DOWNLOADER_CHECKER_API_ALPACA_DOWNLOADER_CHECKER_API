"""The established pipeline version is still emitted by correlation reports."""

import unittest

from libs.alpaca_end_of_day_pipeline import PIPELINE_VERSION, validate_pipeline_change_log


class PipelineVersionTests(unittest.TestCase):
    def test_version_changelog_matches_pipeline(self):
        self.assertEqual(PIPELINE_VERSION, 19)
        self.assertEqual(len(validate_pipeline_change_log()), PIPELINE_VERSION)


if __name__ == "__main__":
    unittest.main()
