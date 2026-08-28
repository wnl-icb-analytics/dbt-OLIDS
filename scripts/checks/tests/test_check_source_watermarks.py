"""Tests for source watermark checks."""

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


CHECKS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CHECKS_DIR))

from check_source_watermarks import (  # noqa: E402
    find_unadvanced,
    matches_previous,
)


class SourceWatermarkTests(unittest.TestCase):
    def test_find_unadvanced_requires_strictly_newer_watermarks(self):
        previous = {
            'ADVANCED': datetime(2026, 8, 27, tzinfo=timezone.utc),
            'UNCHANGED': datetime(2026, 8, 27, tzinfo=timezone.utc),
            'REGRESSED': datetime(2026, 8, 27, tzinfo=timezone.utc),
        }
        current = {
            'ADVANCED': datetime(2026, 8, 28, tzinfo=timezone.utc),
            'UNCHANGED': datetime(2026, 8, 27, tzinfo=timezone.utc),
            'REGRESSED': datetime(2026, 8, 26, tzinfo=timezone.utc),
            'NEW_TABLE': datetime(2026, 8, 28, tzinfo=timezone.utc),
        }

        unadvanced = find_unadvanced(current, previous)

        self.assertEqual(set(unadvanced), {'UNCHANGED', 'REGRESSED'})

    def test_matches_previous_requires_the_full_watermark_set(self):
        watermark = datetime(2026, 8, 28, tzinfo=timezone.utc)

        self.assertTrue(
            matches_previous({'PERSON': watermark}, {'PERSON': watermark})
        )
        self.assertFalse(
            matches_previous(
                {'PERSON': watermark, 'NEW_TABLE': watermark},
                {'PERSON': watermark},
            )
        )


if __name__ == '__main__':
    unittest.main()
