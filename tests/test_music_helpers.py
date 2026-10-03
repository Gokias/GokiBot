import importlib
import os
import shlex
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
                    "http_headers": {"User-Agent": "extractor-agent"},
                    "extractor_key": "Youtube",
                }
            ]
        }

        tracks = poopbot.parse_tracks_from_info(info, "ytsearch1:example track")

        self.assertEqual(len(tracks), 1)
        self.assertEqual(tracks[0].source_url, "https://www.youtube.com/watch?v=abc123")
        self.assertEqual(tracks[0].stream_url, "https://stream.example/audio.webm")
        self.assertEqual(tracks[0].audio_codec, "opus")
        self.assertEqual(tracks[0].http_headers, {"User-Agent": "extractor-agent"})

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

    def test_flat_url_entries_are_resolved_before_streaming(self):
        watch_url = "https://www.youtube.com/watch?v=abc123"
        for result_type in ("url", "url_transparent"):
            with self.subTest(result_type=result_type):
                info = {
                    "entries": [
                        {
                            "_type": result_type,
                            "id": "abc123",
                            "title": "Playlist Track",
                            "duration": 42,
                            "url": watch_url,
                            "ie_key": "Youtube",
                        }
                    ]
                }

                tracks = poopbot.parse_tracks_from_info(
                    info,
                    "https://www.youtube.com/playlist?list=PL1234567890",
                )

                self.assertEqual(tracks[0].source_url, watch_url)
                self.assertIsNone(tracks[0].stream_url)
                self.assertIsNone(tracks[0].audio_codec)
                self.assertEqual(tracks[0].http_headers, {})

    def test_requested_audio_format_preserves_its_headers(self):
        info = {
            "http_headers": {"User-Agent": "parent-agent"},
            "requested_formats": [
                {
                    "url": "https://stream.example/video.mp4",
                    "vcodec": "avc1",
                    "acodec": "none",
                    "http_headers": {"Referer": "https://wrong.example/"},
                },
                {
                    "url": "https://stream.example/audio.webm",
                    "vcodec": "none",
                    "acodec": "opus",
                    "http_headers": {"Referer": "https://audio.example/"},
                },
            ],
        }

        stream = poopbot.extract_stream_selection(info)

        self.assertEqual(stream.url, "https://stream.example/audio.webm")
        self.assertEqual(
            stream.http_headers,
            {"User-Agent": "parent-agent", "Referer": "https://audio.example/"},
        )

    def test_direct_stream_uses_matching_format_header_precedence(self):
        audio_url = "https://stream.example/audio.webm"
        info = {
            "url": audio_url,
            "vcodec": "none",
            "acodec": "opus",
            "http_headers": {
                "User-Agent": "parent-agent",
                "Referer": "https://parent.example/",
                "X-Parent": "inherited",
            },
            "formats": [
                {
                    "url": "https://stream.example/other.webm",
                    "vcodec": "none",
                    "acodec": "opus",
                    "http_headers": {"User-Agent": "wrong-agent"},
                },
                {
                    "url": audio_url,
                    "vcodec": "none",
                    "acodec": "opus",
                    "http_headers": {
                        "user-agent": "selected-agent",
                        "referer": "https://selected.example/",
                    },
                },
            ],
        }

        stream = poopbot.extract_stream_selection(info)

        self.assertEqual(stream.url, audio_url)
        self.assertEqual(
            {name.lower(): value for name, value in stream.http_headers.items()},
            {
                "user-agent": "selected-agent",
                "referer": "https://selected.example/",
                "x-parent": "inherited",
            },
        )
        self.assertEqual(len(stream.http_headers), 3)

    def test_stream_selection_prefers_hls_audio_to_video_without_audio(self):
        info = {
            "formats": [
                {
                    "url": "https://stream.example/video.mp4",
                    "vcodec": "avc1",
                    "acodec": "none",
                    "protocol": "https",
                },
                {
                    "url": "https://stream.example/audio.m3u8",
                    "vcodec": "none",
                    "acodec": "mp4a.40.2",
                    "protocol": "m3u8_native",
                    "abr": 128,
                    "http_headers": {"Referer": "https://audio.example/"},
                },
            ]
        }

        stream = poopbot.extract_stream_selection(info)

        self.assertEqual(stream.url, "https://stream.example/audio.m3u8")
        self.assertEqual(stream.audio_codec, "mp4a.40.2")
        self.assertEqual(stream.http_headers, {"Referer": "https://audio.example/"})

    def test_hls_audio_with_unknown_codec_remains_playable(self):
        audio_url = "https://stream.example/audio.m3u8"
        info = {
            "formats": [
                {
                    "url": audio_url,
                    "vcodec": "none",
                    "protocol": "m3u8_native",
                }
            ]
        }

        stream = poopbot.extract_stream_selection(info)

        self.assertEqual(stream.url, audio_url)
        self.assertIsNone(stream.audio_codec)

    def test_storyboard_is_skipped_for_playable_hls_audio(self):
        storyboard = {
            "url": "https://i.ytimg.com/sb/abc123/storyboard3_L2/M$M.jpg",
            "vcodec": "none",
            "acodec": "none",
            "protocol": "mhtml",
            "format_id": "sb0",
        }
        audio = {
            "url": "https://stream.example/audio.m3u8",
            "vcodec": "none",
            "acodec": "mp4a.40.2",
            "protocol": "m3u8_native",
        }

        for direct_storyboard in (False, True):
            for requested_storyboard in (False, True):
                with self.subTest(
                    direct_storyboard=direct_storyboard,
                    requested_storyboard=requested_storyboard,
                ):
                    info = {"formats": [storyboard, audio]}
                    if direct_storyboard:
                        info.update(storyboard)
                    if requested_storyboard:
                        info["requested_formats"] = [storyboard]

                    stream = poopbot.extract_stream_selection(info)

                    self.assertEqual(stream.url, audio["url"])
                    self.assertEqual(stream.audio_codec, "mp4a.40.2")

    def test_storyboard_without_audio_is_not_playable(self):
        storyboard = {
            "url": "https://i.ytimg.com/sb/abc123/storyboard3_L2/M$M.jpg",
            "vcodec": "none",
            "acodec": "none",
            "protocol": "mhtml",
        }
        for info in (
            storyboard,
            {"requested_formats": [storyboard]},
            {"formats": [storyboard]},
        ):
            with self.subTest(info=info):
                with self.assertRaises(RuntimeError):
                    poopbot.extract_stream_selection(info)

    def test_direct_generic_stream_with_unknown_codec_remains_playable(self):
        audio_url = "https://stream.example/generic-audio.mp3"

        stream = poopbot.extract_stream_selection({"url": audio_url})

        self.assertEqual(stream.url, audio_url)
        self.assertIsNone(stream.audio_codec)

    def test_ffmpeg_headers_preserve_quotes_and_ignore_invalid_newlines(self):
        user_agent = "artist's \"music\" client"
        referer = "https://audio.example/song?title=artist's-song"
        track = poopbot.QueueTrack(
            title="Example Track",
            source_url="https://soundcloud.com/example/song",
            duration_seconds=42,
            requested_by=1,
            http_headers={
                "User-Agent": user_agent,
                "Referer": referer,
                "X-Bad-Value": "injected\r\nX-Injected: yes",
                "Bad\nName": "invalid-name",
            },
        )

        arguments = shlex.split(
            poopbot.build_ffmpeg_before_options(track.source_url, track.http_headers)
        )
        header_text = arguments[arguments.index("-headers") + 1]

        self.assertEqual(
            header_text,
            f"User-Agent: {user_agent}\r\nReferer: {referer}\r\n",
        )


if __name__ == "__main__":
    unittest.main()
