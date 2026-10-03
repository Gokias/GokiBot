import importlib
import os
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import unittest


os.environ.setdefault("DISCORD_TOKEN", "test-token")

poopbot = importlib.import_module("poopbot")


def opus_packet_duration_ms(packet: bytes) -> float:
    # RFC 6716, section 3.1: the TOC encodes frame duration and frame count.
    config = packet[0] >> 3
    if config >= 16:
        frame_ms = (2.5, 5, 10, 20)[config & 3]
    elif config >= 12:
        frame_ms = (10, 20)[config & 1]
    else:
        frame_ms = (10, 20, 40, 60)[config & 3]

    count_code = packet[0] & 3
    frame_count = (1, 2, 2)[count_code] if count_code < 3 else packet[1] & 63
    return frame_ms * frame_count


class QuietHTTPHandler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass


@unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg is required for audio integration tests")
class MusicPlaybackTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.audio_directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.audio_directory.cleanup)
        common_args = [
            shutil.which("ffmpeg"),
            "-hide_banner",
            "-loglevel", "error",
            "-nostdin",
            "-f", "lavfi",
            "-i", "sine=frequency=440:sample_rate=48000:duration=0.3",
            "-ac", "2",
        ]
        cls.aac_file = Path(cls.audio_directory.name) / "tone.aac"
        cls.opus_file = Path(cls.audio_directory.name) / "tone.opus"
        cls.empty_opus_file = Path(cls.audio_directory.name) / "empty.opus"
        subprocess.run(
            common_args + ["-c:a", "aac", "-f", "adts", str(cls.aac_file)],
            check=True,
            capture_output=True,
            timeout=15,
        )
        subprocess.run(
            common_args + ["-c:a", "libopus", "-frame_duration", "60", str(cls.opus_file)],
            check=True,
            capture_output=True,
            timeout=15,
        )
        subprocess.run(
            common_args + ["-t", "0", "-c:a", "libopus", str(cls.empty_opus_file)],
            check=True,
            capture_output=True,
            timeout=15,
        )

        handler = partial(QuietHTTPHandler, directory=cls.audio_directory.name)
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        cls.addClassCleanup(cls.server.server_close)
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.addClassCleanup(cls.server_thread.join, 5)
        cls.addClassCleanup(cls.server.shutdown)
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    def make_source(self, filename, audio_codec):
        url = f"{self.base_url}/{filename}"
        track = poopbot.QueueTrack(
            title="Synthetic audio",
            source_url=url,
            duration_seconds=1,
            requested_by=1,
            audio_codec=audio_codec,
        )
        source = poopbot.build_discord_audio_source(track, url)
        self.addCleanup(source.cleanup)
        return source

    def read_packets(self, source):
        packets = []
        while packet := source.read():
            packets.append(packet)
        self.assertGreater(len(packets), 2, "FFmpeg must produce playable audio")
        self.assertTrue(source.is_opus())
        self.assertEqual({opus_packet_duration_ms(packet) for packet in packets}, {20})
        return packets

    def test_http_aac_is_transcoded_to_20ms_opus_packets(self):
        source = self.make_source("tone.aac", "aac")
        self.read_packets(source)

    def test_http_60ms_opus_is_transcoded_to_20ms_packets(self):
        source = self.make_source("tone.opus", "opus")
        self.read_packets(source)

    def test_missing_http_audio_raises_instead_of_silent_completion(self):
        source = self.make_source("missing.aac", "aac")
        with self.assertRaises(RuntimeError):
            source.read()

    def test_header_only_http_opus_is_not_reported_as_audio(self):
        source = self.make_source("empty.opus", "opus")
        with self.assertRaises(RuntimeError):
            source.read()


if __name__ == "__main__":
    unittest.main()
