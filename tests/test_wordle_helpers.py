import importlib
import os
import unittest
from datetime import date


os.environ.setdefault("DISCORD_TOKEN", "test-token")

poopbot = importlib.import_module("poopbot")


class WordleHelperTests(unittest.TestCase):
    def test_parse_wordle_summary_text_extracts_scores_and_crowns(self):
        text = (
            "Wordle: Your group is on a 15 day streak! Here are yesterday's results:\n"
            f"{poopbot.WORDLE_CROWN_EMOJI} 3/6: <@101>\n"
            "4/6: <@202>\n"
            "5/6: <@303> <@404>"
        )

        parsed = poopbot.parse_wordle_summary_text(
            text,
            date(2026, 5, 3),
            {101: "Abe", 202: "Pair", 303: "Howlie", 404: "Gokias"},
        )

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.result_date, date(2026, 5, 3))
        entries = {entry.user_id: entry for entry in parsed.entries}
        self.assertEqual(entries[101].score, 3)
        self.assertTrue(entries[101].crowned)
        self.assertEqual(entries[404].score, 5)
        self.assertFalse(entries[404].crowned)

    def test_parse_wordle_summary_text_maps_x_and_over_six_to_fail_bucket(self):
        text = (
            "Wordle: Your group is on a 1 day streak! Here are yesterday's results:\n"
            "x/6: <@101>\n"
            "7/6: <@202>"
        )

        parsed = poopbot.parse_wordle_summary_text(text, date(2026, 5, 3))

        self.assertIsNotNone(parsed)
        entries = {entry.user_id: entry for entry in parsed.entries}
        self.assertEqual(entries[101].score, poopbot.WORDLE_FAIL_SCORE)
        self.assertEqual(entries[202].score, poopbot.WORDLE_FAIL_SCORE)

    def test_parse_wordle_summary_text_uses_results_phrase_as_marker(self):
        text = (
            "Your group is on a 15 day streak! Here are yesterday's results:\n"
            f"{poopbot.WORDLE_CROWN_EMOJI} 3/6: <@101>"
        )

        parsed = poopbot.parse_wordle_summary_text(text, date(2026, 5, 3))

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.entries[0].user_id, 101)

    def test_compute_wordle_streaks_counts_successive_non_fail_days(self):
        rows = [
            {"result_date": "2026-05-01", "score": 4, "crowned": 0},
            {"result_date": "2026-05-02", "score": poopbot.WORDLE_FAIL_SCORE, "crowned": 0},
            {"result_date": "2026-05-03", "score": 5, "crowned": 0},
            {"result_date": "2026-05-04", "score": 3, "crowned": 1},
        ]

        self.assertEqual(
            poopbot.compute_wordle_streaks(rows, date(2026, 5, 4)),
            (2, 2),
        )
        self.assertEqual(
            poopbot.compute_wordle_streaks(rows, date(2026, 5, 5)),
            (0, 2),
        )


if __name__ == "__main__":
    unittest.main()
