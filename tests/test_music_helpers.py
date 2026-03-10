import importlib
import os
import unittest


os.environ.setdefault("DISCORD_TOKEN", "test-token")

poopbot = importlib.import_module("poopbot")


class MusicHelperTests(unittest.TestCase):
    def test_normalize_audio_source_preserves_http_url(self):
        source = "https://soundcloud.com/example/song"
        self.assertEqual(poopbot.normalize_audio_source(source), source)

    def test_normalize_audio_source_uses_search_for_plain_text(self):
        self.assertEqual(
            poopbot.normalize_audio_source("never gonna give you up"),
            "ytsearch1:never gonna give you up",
        )

    def test_is_playlist_url_detects_watch_url_with_list(self):
        self.assertTrue(
            poopbot.is_playlist_url("https://www.youtube.com/watch?v=abc123&list=PL1234567890")
        )

    def test_parse_tracks_from_info_caches_stream_url_for_single_entry_result(self):
        info = {
            "entries": [
                {
                    "id": "abc123",
                    "title": "Example Track",
                    "duration": 187,
                    "webpage_url": "https://www.youtube.com/watch?v=abc123",
                    "url": "https://stream.example/audio.webm",
                    "vcodec": "none",
                    "acodec": "opus",
                    "extractor_key": "Youtube",
                }
            ]
        }

        tracks = poopbot.parse_tracks_from_info(info, "ytsearch1:example track")

        self.assertEqual(len(tracks), 1)
        self.assertEqual(tracks[0].source_url, "https://www.youtube.com/watch?v=abc123")
        self.assertEqual(tracks[0].stream_url, "https://stream.example/audio.webm")
        self.assertEqual(tracks[0].audio_codec, "opus")

    def test_parse_tracks_from_flat_playlist_entry_uses_watch_url(self):
        info = {
            "entries": [
                {
                    "id": "abc123",
                    "title": "Playlist Track",
                    "duration": 42,
                    "extractor_key": "Youtube",
                }
            ]
        }

        tracks = poopbot.parse_tracks_from_info(
            info,
            "https://www.youtube.com/playlist?list=PL1234567890",
        )

        self.assertEqual(len(tracks), 1)
        self.assertEqual(tracks[0].source_url, "https://www.youtube.com/watch?v=abc123")
        self.assertIsNone(tracks[0].stream_url)
        self.assertIsNone(tracks[0].audio_codec)


if __name__ == "__main__":
    unittest.main()
