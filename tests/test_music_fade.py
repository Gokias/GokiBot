import asyncio
import importlib
import os
import struct
from types import SimpleNamespace
import unittest
from unittest import mock

import discord


os.environ.setdefault("DISCORD_TOKEN", "test-token")
poopbot = importlib.import_module("poopbot")


class ConstantPCMSource(discord.AudioSource):
    def __init__(self):
        self.frame = struct.pack("<1920h", *([12000] * 1920))
        self.cleanup_calls = 0

    def read(self):
        return self.frame

    def cleanup(self):
        self.cleanup_calls += 1


class FakeVoiceClient:
    def __init__(self, source, channel):
        self.source = source
        self.channel = channel
        self.connected = True
        self.playing = True
        self.paused = False
        self.stopped_sources = []
        self.volumes_at_stop = []
        self.on_stop = None

    def is_connected(self):
        return self.connected

    def is_playing(self):
        return self.playing and not self.paused

    def is_paused(self):
        return self.paused

    def stop(self):
        self.stopped_sources.append(self.source)
        self.volumes_at_stop.append(self.source.volume)
        self.source = None
        self.playing = False
        self.paused = False
        if self.on_stop is not None:
            self.on_stop()


class MusicSourceVolumeTests(unittest.TestCase):
    def test_pcm_volume_changes_actual_sample_amplitude(self):
        original = ConstantPCMSource()
        source = poopbot.MusicAudioSource(original)
        self.addCleanup(source.cleanup)

        self.assertFalse(source.is_opus())
        self.assertEqual(len(source.read()), 3840)
        source.volume = 0.5
        self.assertEqual(set(struct.unpack("<1920h", source.read())), {6000})
        source.volume = 0.0
        self.assertEqual(source.read(), bytes(3840))
        self.assertEqual(source.audio_packets_read, 3)

    def test_wrapped_ffmpeg_failure_is_not_hidden(self):
        original = ConstantPCMSource()
        original.read = mock.Mock(side_effect=[original.frame, b""])
        source = poopbot.MusicAudioSource(original)
        self.addCleanup(source.cleanup)
        source.read()
        original._current_error = RuntimeError("FFmpeg stream failed")

        with self.assertRaisesRegex(RuntimeError, "FFmpeg stream failed"):
            source.read()


class MusicFadeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.guild_id = 654321987
        poopbot.music_states.pop(self.guild_id, None)
        self.state = poopbot.get_music_state(self.guild_id)
        self.channel = SimpleNamespace(id=123, mention="<#123>")
        self.source = self.make_source()
        self.voice = FakeVoiceClient(self.source, self.channel)
        self.guild = SimpleNamespace(id=self.guild_id, voice_client=self.voice)
        self.state.current_track = poopbot.QueueTrack(
            title="First track",
            source_url="https://example.com/first",
            duration_seconds=180,
            requested_by=1,
        )
        self.tasks = []

    async def asyncTearDown(self):
        fade_task = self.state.fade_task
        if fade_task is not None:
            self.tasks.append(fade_task)
        for task in self.tasks:
            if not task.done():
                task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        poopbot.music_states.pop(self.guild_id, None)

    def make_source(self):
        source = poopbot.MusicAudioSource(ConstantPCMSource())
        self.addCleanup(source.cleanup)
        return source

    def interaction(self, *, guild=True):
        return SimpleNamespace(
            guild=self.guild if guild else None,
            user=SimpleNamespace(id=1),
            response=SimpleNamespace(send_message=mock.AsyncMock()),
        )

    def start_fade(self, *, duration_seconds=0.06):
        task = asyncio.create_task(poopbot.fade_out_track(
            self.guild, self.voice, self.source,
            duration_seconds=duration_seconds,
        ))
        self.state.fade_task = task
        self.state.fade_source = self.source
        self.tasks.append(task)
        return task

    async def test_fade_reaches_silence_before_stopping_original(self):
        self.source.volume = 0.65
        task = self.start_fade()
        observed_volumes = []
        while not task.done():
            observed_volumes.append(self.source.volume)
            await asyncio.sleep(0.005)
        await task

        self.assertEqual(self.voice.stopped_sources, [self.source])
        self.assertEqual(self.voice.volumes_at_stop, [0.0])
        self.assertTrue(any(0 < volume < 0.65 for volume in observed_volumes))
        self.assertTrue(all(a >= b for a, b in zip(observed_volumes, observed_volumes[1:])))
        self.assertIsNone(self.state.fade_task)
        self.assertIsNone(self.state.fade_source)

    async def test_natural_end_does_not_fade_or_skip_successor(self):
        task = self.start_fade()
        await asyncio.sleep(0)
        successor = self.make_source()
        self.voice.source = successor
        await task

        self.assertFalse(self.voice.stopped_sources)
        self.assertEqual(successor.volume, 1.0)

    async def test_old_connection_cannot_stop_new_connection(self):
        task = self.start_fade()
        await asyncio.sleep(0)
        replacement = FakeVoiceClient(self.source, self.channel)
        self.guild.voice_client = replacement
        await task

        self.assertFalse(self.voice.stopped_sources)
        self.assertFalse(replacement.stopped_sources)

    async def test_disconnect_ends_fade_without_stopping(self):
        task = self.start_fade()
        await asyncio.sleep(0)
        self.voice.connected = False
        await task

        self.assertFalse(self.voice.stopped_sources)
        self.assertIsNone(self.state.fade_task)

    async def test_duplicate_fade_requests_share_one_skip(self):
        real_fade = poopbot.fade_out_track

        async def short_fade(guild, voice, source):
            await real_fade(guild, voice, source, duration_seconds=0.06)

        first = self.interaction()
        duplicate = self.interaction()
        with mock.patch.object(poopbot, "ensure_voice_channel", return_value=self.channel), \
                mock.patch.object(poopbot, "fade_out_track", side_effect=short_fade):
            await poopbot.gskipfade.callback(first)
            task = self.state.fade_task
            self.assertIsNotNone(task)
            self.tasks.append(task)
            await poopbot.gskipfade.callback(duplicate)
            self.assertIs(self.state.fade_task, task)
            await task

        self.assertEqual(self.voice.stopped_sources, [self.source])
        self.assertTrue(self.state.current_track.skip_requested)
        first.response.send_message.assert_awaited_once()
        duplicate.response.send_message.assert_awaited_once()

    async def test_immediate_skip_cancels_fade_and_preserves_next_song(self):
        real_fade = poopbot.fade_out_track

        async def short_fade(guild, voice, source):
            await real_fade(guild, voice, source, duration_seconds=0.1)

        successor = self.make_source()

        def start_successor():
            self.voice.source = successor
            self.voice.playing = True

        self.voice.on_stop = start_successor
        with mock.patch.object(poopbot, "ensure_voice_channel", return_value=self.channel), \
                mock.patch.object(poopbot, "fade_out_track", side_effect=short_fade):
            await poopbot.gskipfade.callback(self.interaction())
            task = self.state.fade_task
            self.tasks.append(task)
            await asyncio.sleep(0)
            await poopbot.gskip.callback(self.interaction())
            await asyncio.gather(task, return_exceptions=True)

        self.assertEqual(self.voice.stopped_sources, [self.source])
        self.assertIs(self.voice.source, successor)
        self.assertEqual(successor.volume, 1.0)
        self.assertTrue(self.state.current_track.skip_requested)
        self.assertIsNone(self.state.fade_task)

    async def test_paused_track_is_skipped_without_resuming(self):
        self.voice.paused = True
        with mock.patch.object(poopbot, "ensure_voice_channel", return_value=self.channel):
            await poopbot.gskipfade.callback(self.interaction())

        self.assertEqual(self.voice.stopped_sources, [self.source])
        self.assertEqual(self.voice.volumes_at_stop, [1.0])
        self.assertIsNone(self.state.fade_task)
        self.assertTrue(self.state.current_track.skip_requested)

    async def test_skip_commands_enforce_server_and_voice_channel(self):
        for command in (poopbot.gskip, poopbot.gskipfade):
            for scenario in ("no server", "no user voice", "other voice", "idle", "disconnected"):
                with self.subTest(command=command.name, scenario=scenario):
                    self.voice.playing = scenario != "idle"
                    self.voice.connected = scenario != "disconnected"
                    user_channel = self.channel
                    if scenario == "no user voice":
                        user_channel = None
                    elif scenario == "other voice":
                        user_channel = SimpleNamespace(id=456, mention="<#456>")
                    interaction = self.interaction(guild=scenario != "no server")
                    with mock.patch.object(poopbot, "ensure_voice_channel", return_value=user_channel):
                        await command.callback(interaction)

                    self.assertFalse(self.voice.stopped_sources)
                    self.assertIsNone(self.state.fade_task)
                    interaction.response.send_message.assert_awaited_once()

    async def test_help_lists_fade_skip_and_keeps_dev_commands_private(self):
        for is_dev in (False, True):
            with self.subTest(is_dev=is_dev):
                interaction = self.interaction()
                with mock.patch.object(poopbot, "is_dev_user", return_value=is_dev):
                    await poopbot.gokibothelp.callback(interaction)
                message = interaction.response.send_message.await_args.args[0]

                self.assertIn("`/gskipfade`", message)
                self.assertIn("`/gskip`", message)
                self.assertEqual("`/rebuildpoopdb`" in message, is_dev)
                self.assertEqual("`/diagnostics`" in message, is_dev)


if __name__ == "__main__":
    unittest.main()
