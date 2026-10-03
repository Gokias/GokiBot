import asyncio
import importlib
import os
from types import SimpleNamespace
import unittest
from unittest import mock


os.environ.setdefault("DISCORD_TOKEN", "test-token")

poopbot = importlib.import_module("poopbot")


class FakeVoiceClient:
    def __init__(self):
        self.connected = True
        self.playing = False
        self.played_sources = []
        self.disconnect_calls = 0
        self.fail_next_play = False

    def is_connected(self):
        return self.connected

    def is_playing(self):
        return self.playing

    def is_paused(self):
        return False

    def play(self, source, *, after):
        if self.fail_next_play:
            self.fail_next_play = False
            raise RuntimeError("Voice client rejected playback")
        if self.playing:
            raise RuntimeError("Already playing audio")
        self.playing = True
        self.played_sources.append(source)
        self.after = after

    async def disconnect(self, *, force):
        self.disconnect_calls += 1
        self.connected = False
        self.playing = False


class MusicQueueTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.guild_id = 987654321
        poopbot.music_states.pop(self.guild_id, None)
        self.voice = FakeVoiceClient()
        self.guild = SimpleNamespace(id=self.guild_id, voice_client=self.voice)
        self.state = poopbot.get_music_state(self.guild_id)
        self.sources = []

    async def asyncTearDown(self):
        poopbot.music_states.pop(self.guild_id, None)

    def track(self, title, *, cached=True):
        return poopbot.QueueTrack(
            title=title,
            source_url=f"https://example.com/{title}",
            duration_seconds=180,
            requested_by=1,
            stream_url=f"https://stream.example/{title}" if cached else None,
        )

    def make_source(self, track, stream_url):
        source = SimpleNamespace(
            track=track,
            stream_url=stream_url,
            audio_packets_read=0,
            cleanup=mock.Mock(),
        )
        self.sources.append(source)
        return source

    async def finish(self, track, source, error=None):
        self.voice.playing = False
        await poopbot._finish_music_track(self.guild, self.voice, track, source, error)

    async def test_concurrent_advance_preserves_tracks_during_stream_resolution(self):
        first = self.track("first", cached=False)
        second = self.track("second")
        self.state.queue.extend([first, second])
        resolving = asyncio.Event()
        release_resolution = asyncio.Event()

        async def resolve(source_url):
            resolving.set()
            await release_resolution.wait()
            return poopbot.StreamSelection("https://stream.example/first", "opus")

        with mock.patch.object(poopbot, "resolve_stream_selection", side_effect=resolve), \
                mock.patch.object(poopbot, "build_discord_audio_source", side_effect=self.make_source):
            starting = asyncio.create_task(poopbot.play_next_track(self.guild))
            await asyncio.wait_for(resolving.wait(), timeout=1)
            concurrent = asyncio.create_task(poopbot.play_next_track(self.guild))
            await asyncio.sleep(0)

            self.assertFalse(concurrent.done())
            self.assertEqual(list(self.state.queue), [second])
            self.assertIs(self.state.current_track, first)
            release_resolution.set()
            await asyncio.wait_for(asyncio.gather(starting, concurrent), timeout=1)

            self.assertEqual([source.track for source in self.voice.played_sources], [first])
            self.sources[0].audio_packets_read = 1
            await self.finish(first, self.sources[0])
            self.assertEqual([source.track for source in self.voice.played_sources], [first, second])
            self.assertIs(self.state.current_track, second)

    async def test_rejected_play_cleans_audio_source_and_advances(self):
        first = self.track("first")
        second = self.track("second")
        self.state.queue.extend([first, second])
        self.voice.fail_next_play = True

        with mock.patch.object(poopbot, "build_discord_audio_source", side_effect=self.make_source):
            await poopbot.play_next_track(self.guild)

        self.sources[0].cleanup.assert_called_once_with()
        self.sources[1].cleanup.assert_not_called()
        self.assertEqual([source.track for source in self.voice.played_sources], [second])
        self.assertIs(self.state.current_track, second)
        self.assertFalse(self.state.queue)

    async def test_zero_audio_error_refreshes_once_then_advances_and_disconnects(self):
        first = self.track("first")
        second = self.track("second")
        self.state.queue.extend([first, second])
        resolver = mock.AsyncMock(return_value=poopbot.StreamSelection("https://fresh.example/first", "opus"))

        with mock.patch.object(poopbot, "resolve_stream_selection", resolver), \
                mock.patch.object(poopbot, "build_discord_audio_source", side_effect=self.make_source):
            await poopbot.play_next_track(self.guild)
            await self.finish(first, self.sources[0], RuntimeError("Expired stream"))
            await self.finish(first, self.sources[1], RuntimeError("Fresh stream also failed"))
            self.assertIs(self.state.current_track, second)
            self.sources[2].audio_packets_read = 1
            await self.finish(second, self.sources[2])

        resolver.assert_awaited_once_with(first.source_url)
        self.assertEqual(first.stream_url_refresh_attempts, 1)
        self.assertEqual(
            [source.stream_url for source in self.voice.played_sources],
            ["https://stream.example/first", "https://fresh.example/first", "https://stream.example/second"],
        )
        self.assertEqual(self.voice.disconnect_calls, 1)
        self.assertIsNone(self.state.current_track)

    async def assert_completion_does_not_retry(self, packets_read, error=None):
        track = self.track("first")
        self.state.queue.append(track)
        resolver = mock.AsyncMock()
        with mock.patch.object(poopbot, "resolve_stream_selection", resolver), \
                mock.patch.object(poopbot, "build_discord_audio_source", side_effect=self.make_source):
            await poopbot.play_next_track(self.guild)
            self.sources[0].audio_packets_read = packets_read
            await self.finish(track, self.sources[0], error)
        resolver.assert_not_awaited()
        self.assertEqual(track.stream_url_refresh_attempts, 0)
        self.assertEqual(len(self.voice.played_sources), 1)
        self.assertEqual(self.voice.disconnect_calls, 1)

    async def test_deliberate_stop_before_audio_does_not_retry(self):
        await self.assert_completion_does_not_retry(packets_read=0)

    async def test_normal_completion_does_not_retry(self):
        await self.assert_completion_does_not_retry(packets_read=10)

    async def test_error_after_audio_started_does_not_restart_song(self):
        await self.assert_completion_does_not_retry(packets_read=10, error=RuntimeError("Connection lost"))

    async def test_callback_for_old_track_does_not_advance_queue(self):
        old_track = self.track("old")
        current = self.track("current")
        queued = self.track("queued")
        self.state.current_track = current
        self.state.queue.append(queued)

        await poopbot._finish_music_track(
            self.guild, self.voice, old_track, self.make_source(old_track, old_track.stream_url), None,
        )

        self.assertIs(self.state.current_track, current)
        self.assertEqual(list(self.state.queue), [queued])
        self.assertFalse(self.voice.played_sources)
        self.assertEqual(self.voice.disconnect_calls, 0)

    async def test_callback_for_old_connection_does_not_advance_queue(self):
        track = self.track("first")
        queued = self.track("queued")
        self.state.current_track = track
        self.state.queue.append(queued)
        old_voice = FakeVoiceClient()

        await poopbot._finish_music_track(
            self.guild, old_voice, track, self.make_source(track, track.stream_url), None,
        )

        self.assertIs(self.state.current_track, track)
        self.assertEqual(list(self.state.queue), [queued])
        self.assertFalse(self.voice.played_sources)
        self.assertEqual(self.voice.disconnect_calls, 0)

    async def test_delayed_playlist_expansion_resumes_after_first_track_finishes(self):
        first = self.track("first")
        self.state.queue.append(first)
        expanding = asyncio.Event()
        release_expansion = asyncio.Event()

        async def extract(source, **kwargs):
            expanding.set()
            await release_expansion.wait()
            return {
                "entries": [{
                    "title": "second",
                    "webpage_url": "https://example.com/second",
                    "url": "https://stream.example/second",
                    "vcodec": "none",
                    "acodec": "opus",
                    "duration": 180,
                }],
            }

        with mock.patch.object(poopbot.bot, "get_guild", return_value=self.guild), \
                mock.patch.object(poopbot, "extract_info", side_effect=extract), \
                mock.patch.object(poopbot, "build_discord_audio_source", side_effect=self.make_source):
            await poopbot.play_next_track(self.guild)
            expansion = asyncio.create_task(poopbot.expand_remaining_playlist(
                self.guild_id, "https://example.com/playlist", 7, self.voice,
            ))
            self.state.playlist_tasks.add(expansion)
            await asyncio.wait_for(expanding.wait(), timeout=1)
            self.sources[0].audio_packets_read = 1
            await self.finish(first, self.sources[0])
            self.assertTrue(self.voice.connected)
            self.assertEqual(self.voice.disconnect_calls, 0)
            self.assertIsNone(self.state.current_track)

            release_expansion.set()
            await asyncio.wait_for(expansion, timeout=1)

        self.assertFalse(self.state.playlist_tasks)
        self.assertEqual([source.track.title for source in self.voice.played_sources], ["first", "second"])
        self.assertEqual(self.state.current_track.requested_by, 7)
        self.assertTrue(self.voice.playing)

    async def test_failed_or_empty_playlist_expansion_disconnects_idle_voice(self):
        for result in (RuntimeError("Playlist unavailable"), {"entries": []}):
            with self.subTest(result=result):
                self.voice = FakeVoiceClient()
                self.guild.voice_client = self.voice
                self.state.playlist_tasks.add(asyncio.current_task())
                await poopbot.play_next_track(self.guild)
                self.assertEqual(self.voice.disconnect_calls, 0)
                extractor = mock.AsyncMock()
                if isinstance(result, RuntimeError):
                    extractor.side_effect = result
                else:
                    extractor.return_value = result

                with mock.patch.object(poopbot.bot, "get_guild", return_value=self.guild), \
                        mock.patch.object(poopbot, "extract_info", extractor):
                    await poopbot.expand_remaining_playlist(
                        self.guild_id, "https://example.com/playlist", 7, self.voice,
                    )

                self.assertFalse(self.state.playlist_tasks)
                self.assertEqual(self.voice.disconnect_calls, 1)
                self.assertFalse(self.voice.connected)
                self.assertIsNone(self.state.current_track)


if __name__ == "__main__":
    unittest.main()
