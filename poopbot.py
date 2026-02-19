from dotenv import load_dotenv
import os
import math
import uuid
import random
import sqlite3
import asyncio
import urllib.request
from urllib.parse import urlparse
import json
import tempfile
import shutil
import importlib.util
import ctypes.util
import xml.etree.ElementTree as ET
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone, date, time as dtime
from pathlib import Path
import time
import discord
from discord.ext import commands, tasks
try:
    from zoneinfo import ZoneInfo
    from zoneinfo import ZoneInfoNotFoundError
except ImportError:
    raise RuntimeError("Python 3.9+ required for zoneinfo")
# Ensure Opus is available for voice/recording on Linux
_OPUS_LOAD_ERROR: str | None = None
if not discord.opus.is_loaded():
    for opus_lib_name in ("libopus.so.0", "libopus.so", ctypes.util.find_library("opus")):
        if not opus_lib_name:
            continue
        try:
            discord.opus.load_opus(opus_lib_name)
            break
        except Exception as e:
            _OPUS_LOAD_ERROR = str(e)
if not discord.opus.is_loaded() and _OPUS_LOAD_ERROR:
    print(f"[voice] Failed to load Opus: {_OPUS_LOAD_ERROR}")

# =========================
# CONFIG
# =========================
load_dotenv()  # loads variables from .env into the process environment
TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN not found. Check your .env file and WorkingDirectory.")
DB_DIR = "db"
CONFIG_DB_PATH = os.path.join(DB_DIR, "poopbot_config.db")
CLEANUP_DB_PATH = os.path.join(DB_DIR, "poopbot_cleanup.db")
POOP_EMOJI = "💩"
UNDO_EMOJI = "🧻"
# Deletes ANY non-bot message posted in this channel
CLEANUP_CHANNEL_ID = 1419130398683959398
# Ticketing configuration
TICKET_DEV_USER_ID = os.getenv("TICKET_DEV_USER_ID")
TICKET_ARCHIVE_CHANNEL_ID = os.getenv("TICKET_ARCHIVE_CHANNEL_ID")
# Daily post time (12:00am Pacific)
TZ_NAME = "America/Los_Angeles"
# Rotate button message every N poops per guild
ROTATE_EVERY = 10
WESROTH_HANDLE_URL = "https://www.youtube.com/@WesRoth"
WESROTH_CHANNEL_ID = os.getenv("WESROTH_CHANNEL_ID")
WESROTH_ALERT_CHANNEL_ID = 1350269523902857369
WESROTH_POLL_MINUTES = 30
WESROTH_CAPTIONS = [
    "IT BEGINS.",
    "WE’RE DONE.",
    "PACK IT UP.",
    "GAME OVER.",
    "END OF THE LINE.",
    "CURTAINS.",
    "NO TURNING BACK.",
    "THIS AIN’T GOOD.",
    "THE END IS NIGH.",
    "POINT OF NO RETURN.",
    "HERE WE GO.",
    "ITS OVER.",
    "UH OH.",
    "THIS IS IT.",
]
FETCH_TRACK_INFO_TIMEOUT_SECONDS = 25
FETCH_TRACK_INFO_TIMEOUT_MESSAGE = (
    "Timed out while fetching track info from YouTube. Please try again in a moment."
)
# =========================
# MESSAGES
# =========================
CONGRATS = [
    "Delivery confirmed, {user}.",
    "Another successful drop, {user}.",
    "System pressure normalized, {user}.",
    "Pipeline cleared efficiently, {user}.",
    "That went exactly as planned, {user}.",
    "Another smooth release, {user}.",
    "Containment breach resolved, {user}.",
    "Stability restored, {user}.",
    "Another clean transfer, {user}.",
    "Gravity assisted as expected, {user}.",
    "Pressure differential resolved, {user}.",
    "End-to-end process completed, {user}.",
    "Everything moved downstream nicely, {user}.",
    "All valves closed behind you, {user}.",
    "Efficiency rating: excellent, {user}.",
    "Discharge complete, {user}.",
    "System output logged successfully, {user}.",
    "Clean handoff to the porcelain throne, {user}.",
    "Release sequence executed perfectly, {user}.",
    "System equilibrium restored, {user}.",
    "Exit strategy performed without issue, {user}.",
    "Nothing backed up this time, {user}.",
    "Flow rate acceptable, {user}.",
    "Mission accomplished, {user}. 💩",
    "Nature called and {user} answered with vigor.",
    "A job well done, {user}. Flush with pride.",
    "Textbook execution, {user}.",
    "Excellent form, {user}.",
    "Another one in the books, {user}.",
    "Poop status: logged. Nice work, {user}.",
    "Handled with precision, {user}.",
    "Clean break. Strong showing, {user}.",
    "That’s how it’s done, {user}.",
    "Well timed and well placed, {user}.",
    "Successful payload delivery, {user}.",
    "Poop confirmed. Solid work, {user}.",
    "Gravity remains undefeated, {user}.",
    "Another victory for biology, {user}.",
    "Another log logged, {user}.",
    "One small dump for man, one giant relief for {user}.",
    "Poop cycle complete. Well done, {user}.",
    "Balance restored. Nice work, {user}.",
    "All systems go. Nice dump, {user}.",
    "Another successful evacuation, {user}.",
    "Release valve opened flawlessly, {user}.",
    "Nothing left behind. Strong work, {user}.",
    "Clean operation from start to finish, {user}.",
    "System integrity maintained. Nice one, {user}.",
    "Successful offload confirmed, {user}.",
    "Another blockage cleared, {user}.",
    "Process completed without incident, {user}.",
    "That’s one for the records, {user}.",
    "Well executed exit strategy, {user}.",
    "All clear. Nice work, {user}.",
    "System reset complete, {user}.",
    "Poop event recorded. Excellent, {user}.",
    "Mission accomplished, but at what cost, {user}.",
    "Payload delivered… with collateral damage, {user}.",
    "Successful deployment; splash radius exceeded expectations, {user}.",
    "Package received, but the landing was rough, {user}.",
    "Delivery confirmed. The aftermath was… extensive, {user}.",
    "Release achieved; turbulence reported on final approach, {user}.",
    "Transfer completed with unexpected turbulence, {user}.",
    "Process completed; residue levels remain concerning, {user}.",
    "Outcome delivered, but the wipe budget doubled, {user}.",
    "Release completed—-friction heat noted, {user}.",
    "Primary objective met; secondary objectives failed, {user}.",
    "Release finished; splashback event recorded, {user}.",
    "Event completed; flush count higher than planned, {user}.",
    "Operation successful. Multiple passes required, {user}.",
    "Release confirmed. That was a high-friction exit, {user}.",
    "Output achieved; the situation got spicy, {user}.",
    "Task finished. The final seconds were a gamble, {user}.",
    "All done, {user}. Nobody’s calling that a clean run.",
    # Dota 2
    "Space created, {user}.",
    
    # Arc Raiders
    "Extraction successful, {user}.",
    
    # Factorio
    "Bottleneck resolved, {user}.",
    
    # Battlegrounds
    "Top four secured, {user}.",
    "Clean pivot, {user}.",
    
    # WoW
    "{user} pulled too much, wipe it up.",
    
    # PoE
    "{user} dropped an ancient orb.",
    
    # Dwarf Fortress
    
    # Minecraft
    
    # FFXIV
    "Limit break well timed, {user}.",
    "Duty complete, {user}.",
    
    # Diablo
    "Rift cleared efficiently, {user}.",
    
    # OSRS
    "XP waste minimized, {user}.",
    "Tick-perfect execution, {user}.",
    
    # General
    "Another small win logged, {user}.",
    "Consistency pays off, {user}.",
    "{user} — OVERPOOP!",
    "{user} — POOPTACULAR!",
    "{user} — POOPTROCITY!",
    "{user} — POOPIMANJARO!",
    "{user} — POOPTASTROPHE!",
    "{user} — POOPOCALYPSE!",
    "{user} — POOPIONAIRE!",
]
UNDO_MSGS = [
    "Rollback complete, {user}.",
    "Okay {user}, I removed your last poop.",
    "Wiped from history, {user}.",
    "Deleted one (1) poop from the timeline, {user}.",
]
WESROTH_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "yt": "http://www.youtube.com/xml/schemas/2015",
    "media": "http://search.yahoo.com/mrss/",
}
# =========================
# TIMEZONE
# =========================
try:
    LOCAL_TZ = ZoneInfo(TZ_NAME)
except ZoneInfoNotFoundError as e:
    raise RuntimeError(
        f"ZoneInfo timezone '{TZ_NAME}' not found. On Windows, install tzdata:\n"
        f"  python -m pip install tzdata\n"
        f"Then restart."
    ) from e
def current_year_local() -> int:
    return datetime.now(LOCAL_TZ).year
# =========================
# DISCORD SETUP
# =========================
intents = discord.Intents.default()
intents.reactions = True
intents.message_content = True  # needed for cleanup logging
bot = commands.Bot(command_prefix="!", intents=intents)
# serialize DB writes to avoid sqlite "database is locked"
db_write_lock = asyncio.Lock()
@dataclass
class QueueTrack:
    title: str
    source_url: str
    duration_seconds: int
    requested_by: int
class GuildMusicState:
    def __init__(self):
        self.queue: deque[QueueTrack] = deque()
        self.current_track: QueueTrack | None = None
        self.track_started_at: datetime | None = None
        self.lock = asyncio.Lock()
music_states: dict[int, GuildMusicState] = {}
class GuildTranscriptionSession:
    def __init__(self, guild_id: int, voice_channel_id: int):
        self.guild_id = guild_id
        self.voice_channel_id = voice_channel_id
        self.started_at = datetime.now(timezone.utc)
        self.temp_dir = Path(tempfile.mkdtemp(prefix=f"gokibot_transcribe_{guild_id}_"))
        self.voice_paths_by_user: dict[int, Path] = {}
        self.aliases_by_user: dict[int, str] = {}
transcription_sessions: dict[int, GuildTranscriptionSession] = {}
def get_music_state(guild_id: int) -> GuildMusicState:
    state = music_states.get(guild_id)
    if state is None:
        state = GuildMusicState()
        music_states[guild_id] = state
    return state
def get_transcription_session(guild_id: int) -> GuildTranscriptionSession | None:
    return transcription_sessions.get(guild_id)
def remove_transcription_session(guild_id: int):
    session = transcription_sessions.pop(guild_id, None)
    if session is None:
        return
    shutil.rmtree(session.temp_dir, ignore_errors=True)
def resolve_display_name(guild: discord.Guild | None, user_id: int, aliases_by_user: dict[int, str]) -> str:
    alias = aliases_by_user.get(user_id)
    if alias:
        return alias
    if guild is not None:
        member = guild.get_member(user_id)
        if member is not None:
            return member.display_name
    return str(user_id)
def can_record_voice() -> tuple[bool, str]:
    if not hasattr(discord, "sinks") or not hasattr(discord.sinks, "WaveSink"):
        return False, "This runtime is missing discord voice sinks support."
    if importlib.util.find_spec("nacl") is None:
        return False, "PyNaCl is not installed in this Python environment."
    if not discord.opus.is_loaded():
        return False, "Opus is not loaded (missing libopus on host)."
    return True, ""
def get_whisper_transcriber() -> tuple[str | None, object | None]:
    if importlib.util.find_spec("faster_whisper") is not None:
        from faster_whisper import WhisperModel
        model_name = os.getenv("WHISPER_MODEL", "base")
        model = WhisperModel(model_name, device="cpu", compute_type="int8")
        return "faster_whisper", model
    if importlib.util.find_spec("whisper") is not None:
        import whisper
        model_name = os.getenv("WHISPER_MODEL", "base")
        model = whisper.load_model(model_name)
        return "whisper", model
    return None, None
def transcribe_audio_file(engine_name: str, engine: object, file_path: Path) -> str:
    if engine_name == "faster_whisper":
        segments, _ = engine.transcribe(str(file_path), vad_filter=True)
        return " ".join(seg.text.strip() for seg in segments if seg.text.strip())
    if engine_name == "whisper":
        result = engine.transcribe(str(file_path), fp16=False)
        return str(result.get("text") or "").strip()
    return ""
def copy_recorded_audio_to_session(
    sink: object,
    guild: discord.Guild,
    session: GuildTranscriptionSession,
) -> dict[int, Path]:
    copied_files: dict[int, Path] = {}
    sink_audio_data = getattr(sink, "audio_data", None)
    if not isinstance(sink_audio_data, dict):
        return copied_files
    for user_id, audio_obj in sink_audio_data.items():
        if not isinstance(user_id, int):
            continue
        file_path = getattr(audio_obj, "file", None)
        if file_path is None:
            continue
        temp_output = session.temp_dir / f"{user_id}.wav"
        try:
            if hasattr(file_path, "seek"):
                file_path.seek(0)
            if hasattr(file_path, "read"):
                temp_output.write_bytes(file_path.read())
            else:
                shutil.copy(str(file_path), temp_output)
        except OSError:
            continue
        copied_files[user_id] = temp_output
    session.voice_paths_by_user = copied_files
    return copied_files
async def finalize_transcription_session(
    interaction: discord.Interaction,
    session: GuildTranscriptionSession,
) -> tuple[Path | None, str]:
    guild = interaction.guild
    if guild is None:
        return None, "This command only works in a server."
    if not session.voice_paths_by_user:
        return None, "No captured audio was found for this session."
    engine_name, engine = get_whisper_transcriber()
    if engine is None or engine_name is None:
        return None, (
            "No local transcription engine was found. Install `faster-whisper` (recommended) "
            "or `openai-whisper` on the host."
        )
    transcript_path = session.temp_dir / f"transcript-{guild.id}-{int(time.time())}.txt"
    lines = [
        f"GokiBot transcription session for guild {guild.id}",
        f"Started UTC: {session.started_at.isoformat()}",
        f"Ended UTC: {datetime.now(timezone.utc).isoformat()}",
        "",
    ]
    for user_id, audio_path in sorted(session.voice_paths_by_user.items(), key=lambda item: item[0]):
        speaker_name = resolve_display_name(guild, user_id, session.aliases_by_user)
        transcript = transcribe_audio_file(engine_name, engine, audio_path)
        if not transcript:
            transcript = "[No speech detected]"
        lines.append(f"[{speaker_name} | {user_id}]")
        lines.append(transcript)
        lines.append("")
    transcript_path.write_text("\n".join(lines), encoding="utf-8")
    return transcript_path, ""
def format_duration(duration_seconds: int) -> str:
    mins, secs = divmod(max(duration_seconds, 0), 60)
    hours, mins = divmod(mins, 60)
    if hours:
        return f"{hours:d}:{mins:02d}:{secs:02d}"
    return f"{mins:d}:{secs:02d}"
def parse_duration_seconds(value: object) -> int:
    if isinstance(value, (int, float)):
        return max(int(value), 0)
    if not isinstance(value, str):
        return 0
    text = value.strip()
    if not text:
        return 0
    if text.isdigit():
        return int(text)
    parts = text.split(":")
    if not all(part.isdigit() for part in parts):
        return 0
    total = 0
    for part in parts:
        total = (total * 60) + int(part)
    return total
def log_music_timing(step: str, phase: str, started_at: float, **fields: object):
    elapsed = time.perf_counter() - started_at
    details = " ".join(f"{key}={value!r}" for key, value in fields.items())
    suffix = f" {details}" if details else ""
    print(f"[music] {step} {phase} elapsed={elapsed:.2f}s{suffix}")
def pick_track_info(info: dict[str, object]) -> dict[str, object]:
    entries = info.get("entries")
    if isinstance(entries, list):
        for entry in entries:
            if isinstance(entry, dict):
                return entry
        raise RuntimeError("No playable track found for that query.")
    return info
def extract_stream_url(info: dict[str, object]) -> str:
    direct_url = str(info.get("url") or "").strip()
    if direct_url:
        return direct_url
    requested_formats = info.get("requested_formats")
    if isinstance(requested_formats, list):
        for fmt in requested_formats:
            if not isinstance(fmt, dict):
                continue
            format_url = str(fmt.get("url") or "").strip()
            if not format_url:
                continue
            if str(fmt.get("vcodec") or "") == "none":
                return format_url
    formats = info.get("formats")
    if not isinstance(formats, list):
        raise RuntimeError("yt-dlp did not provide an audio stream URL.")
    def _is_hls_protocol(fmt: dict[str, object]) -> bool:
        protocol = str(fmt.get("protocol") or "").lower()
        return "m3u8" in protocol or protocol == "http_dash_segments"
    best_audio_url = ""
    best_audio_score = -1.0
    best_hls_audio_url = ""
    best_hls_audio_score = -1.0
    fallback_url = ""
    fallback_non_hls_url = ""
    for fmt in formats:
        if not isinstance(fmt, dict):
            continue
        format_url = str(fmt.get("url") or "").strip()
        if not format_url:
            continue
        if not fallback_url:
            fallback_url = format_url
        is_hls = _is_hls_protocol(fmt)
        if not is_hls and not fallback_non_hls_url:
            fallback_non_hls_url = format_url
        is_audio_only = str(fmt.get("vcodec") or "") == "none"
        if not is_audio_only:
            continue
        bitrate = fmt.get("abr") or fmt.get("tbr") or 0
        try:
            score = float(bitrate)
        except (TypeError, ValueError):
            score = 0.0
        if is_hls:
            if score >= best_hls_audio_score:
                best_hls_audio_score = score
                best_hls_audio_url = format_url
        else:
            if score >= best_audio_score:
                best_audio_score = score
                best_audio_url = format_url
    if best_audio_url:
        return best_audio_url
    if fallback_non_hls_url:
        return fallback_non_hls_url
    if best_hls_audio_url:
        return best_hls_audio_url
    if fallback_url:
        return fallback_url
    raise RuntimeError("yt-dlp returned an empty stream URL.")
def parse_tracks_from_info(info: dict[str, object], source: str) -> list[QueueTrack]:
    entries = info.get("entries")
    if isinstance(entries, list):
        tracks: list[QueueTrack] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            title = str(entry.get("title") or "Unknown title")
            duration_seconds = parse_duration_seconds(entry.get("duration"))
            if duration_seconds <= 0:
                duration_seconds = parse_duration_seconds(entry.get("duration_string"))
            webpage_url = str(entry.get("webpage_url") or entry.get("url") or "").strip()
            if not webpage_url:
                entry_id = str(entry.get("id") or "").strip()
                if entry_id:
                    webpage_url = f"https://www.youtube.com/watch?v={entry_id}"
                else:
                    webpage_url = source
            tracks.append(
                QueueTrack(
                    title=title,
                    source_url=webpage_url,
                    duration_seconds=duration_seconds,
                    requested_by=0,
                )
            )
        if tracks:
            return tracks
        raise RuntimeError("No playable tracks found for that playlist.")
    track_info = pick_track_info(info)
    title = str(track_info.get("title") or "Unknown title")
    duration_seconds = parse_duration_seconds(track_info.get("duration"))
    if duration_seconds <= 0:
        duration_seconds = parse_duration_seconds(track_info.get("duration_string"))
    if duration_seconds <= 0:
        duration_seconds = parse_duration_seconds(info.get("duration_string"))
    webpage_url = str(track_info.get("webpage_url") or info.get("webpage_url") or source)
    return [
        QueueTrack(
            title=title,
            source_url=webpage_url,
            duration_seconds=duration_seconds,
            requested_by=0,
        )
    ]
async def fetch_tracks(source: str) -> list[QueueTrack]:
    info_proc = await asyncio.create_subprocess_exec(
        "yt-dlp",
        "-f",
        "bestaudio/best",
        "--dump-single-json",
        source,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    info_stdout, info_stderr = await info_proc.communicate()
    if info_proc.returncode != 0:
        err = info_stderr.decode("utf-8", errors="ignore").strip() or "yt-dlp failed"
        raise RuntimeError(err)
    try:
        info = json.loads(info_stdout.decode("utf-8", errors="ignore"))
    except Exception as exc:
        raise RuntimeError("Unable to read track metadata.") from exc
    return parse_tracks_from_info(info, source)
async def resolve_stream_url(source_url: str) -> str:
    stream_proc = await asyncio.create_subprocess_exec(
        "yt-dlp",
        "-f",
        "bestaudio/best",
        "--dump-single-json",
        "--no-playlist",
        source_url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stream_stdout, stream_stderr = await stream_proc.communicate()
    if stream_proc.returncode != 0:
        err = stream_stderr.decode("utf-8", errors="ignore").strip() or "yt-dlp failed"
        raise RuntimeError(err)
    try:
        info = json.loads(stream_stdout.decode("utf-8", errors="ignore"))
    except Exception as exc:
        raise RuntimeError("Unable to read playback stream URL.") from exc
    return extract_stream_url(pick_track_info(info))
def is_youtube_url(value: str) -> bool:
    try:
        parsed = urlparse(value)
    except ValueError:
        return False
    if parsed.scheme not in {"http", "https"}:
        return False
    hostname = (parsed.hostname or "").lower()
    youtube_hosts = {
        "youtube.com",
        "www.youtube.com",
        "m.youtube.com",
        "music.youtube.com",
        "youtu.be",
        "www.youtu.be",
    }
    return hostname in youtube_hosts
async def ensure_voice_channel(interaction: discord.Interaction) -> discord.VoiceChannel | None:
    if interaction.guild is None:
        return None
    member = interaction.guild.get_member(interaction.user.id)
    if member is None or member.voice is None or member.voice.channel is None:
        return None
    if not isinstance(member.voice.channel, discord.VoiceChannel):
        return None
    return member.voice.channel
async def play_next_track(guild: discord.Guild):
    voice_client = guild.voice_client
    if voice_client is None:
        return
    state = get_music_state(guild.id)
    async with state.lock:
        if voice_client.is_playing() or voice_client.is_paused():
            return
        if not state.queue:
            state.current_track = None
            state.track_started_at = None
            await voice_client.disconnect(force=True)
            return
        next_track = state.queue.popleft()
        state.current_track = next_track
        state.track_started_at = datetime.now(timezone.utc)
    try:
        stream_url = await resolve_stream_url(next_track.source_url)
    except RuntimeError as exc:
        print(f"Failed to resolve stream URL for '{next_track.title}': {exc}")
        await play_next_track(guild)
        return
    ffmpeg_source = discord.FFmpegPCMAudio(
        stream_url,
        before_options=(
            "-nostdin -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 "
            "-http_persistent 0 "
            "-user_agent 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36' "
            "-headers 'Referer: https://www.youtube.com/\r\nOrigin: https://www.youtube.com\r\n'"
        ),
        options="-vn -loglevel warning -af aresample=async=1:min_hard_comp=0.100:first_pts=0",
    )
    def _after_playback(play_error: Exception | None):
        if play_error:
            print(f"Playback error: {play_error}")
        fut = asyncio.run_coroutine_threadsafe(play_next_track(guild), bot.loop)
        try:
            fut.result()
        except Exception as exc:
            print(f"Failed to start next track: {exc}")
    print(f"[music] voice_client.play start track='{next_track.title}'")
    voice_client.play(ffmpeg_source, after=_after_playback)
# =========================
# DATABASE HELPERS
# =========================
def _apply_sqlite_pragmas(conn: sqlite3.Connection):
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=10000;")  # ms
def db_config() -> sqlite3.Connection:
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(CONFIG_DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    _apply_sqlite_pragmas(conn)
    return conn
def db_cleanup() -> sqlite3.Connection:
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(CLEANUP_DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    _apply_sqlite_pragmas(conn)
    return conn
def db_path_for_year(year: int) -> str:
    os.makedirs(DB_DIR, exist_ok=True)
    return os.path.join(DB_DIR, f"poopbot_{year}.db")
def db_year(year: int) -> sqlite3.Connection:
    path = db_path_for_year(year)
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    _apply_sqlite_pragmas(conn)
    return conn
def init_config_db():
    with db_config() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS guild_config (
            guild_id INTEGER PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles'
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS guild_state (
            guild_id INTEGER NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            PRIMARY KEY (guild_id, key)
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS tickets (
            ticket_id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            requester_id INTEGER NOT NULL,
            requester_name TEXT NOT NULL,
            channel_id INTEGER,
            thread_id INTEGER,
            archive_thread_id INTEGER,
            created_at_utc TEXT NOT NULL,
            closed_at_utc TEXT,
            status TEXT NOT NULL DEFAULT 'open'
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS ticket_collaborators (
            ticket_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            added_by_id INTEGER NOT NULL,
            added_at_utc TEXT NOT NULL,
            PRIMARY KEY (ticket_id, user_id)
        );
        """)
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(tickets)").fetchall()
        }
        if "archive_thread_id" not in columns:
            conn.execute("ALTER TABLE tickets ADD COLUMN archive_thread_id INTEGER;")
        if "closed_at_utc" not in columns:
            conn.execute("ALTER TABLE tickets ADD COLUMN closed_at_utc TEXT;")
def init_cleanup_db():
    with db_cleanup() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS cleanup_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER,
            channel_id INTEGER,
            guild_id INTEGER,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at_utc TEXT NOT NULL
        );
        """)
def init_year_db(year: int):
    with db_year(year) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,                 -- 'POOP' or 'UNDO'
            timestamp_utc TEXT NOT NULL,
            timestamp_local TEXT NOT NULL,
            date_local TEXT NOT NULL,                 -- YYYY-MM-DD (Pacific)
            time_local TEXT NOT NULL,                 -- HH:MM:SS (Pacific)
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            guild_id INTEGER,
            channel_id INTEGER,
            message_id INTEGER,
            target_event_id TEXT,                     -- for UNDO
            note TEXT
        );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_user_time ON events(user_id, timestamp_utc);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);")
# ---- config/state (persistent) ----
def gset(guild_id: int, key: str, value: str):
    with db_config() as conn:
        conn.execute("""
            INSERT INTO guild_state(guild_id, key, value) VALUES(?, ?, ?)
            ON CONFLICT(guild_id, key) DO UPDATE SET value=excluded.value
        """, (guild_id, key, value))
def gget(guild_id: int, key: str) -> str | None:
    with db_config() as conn:
        row = conn.execute("""
            SELECT value FROM guild_state WHERE guild_id=? AND key=?
        """, (guild_id, key)).fetchone()
        return row["value"] if row else None
def gget_int(guild_id: int, key: str, default: int = 0) -> int:
    v = gget(guild_id, key)
    try:
        return int(v) if v is not None else default
    except ValueError:
        return default
def gset_int(guild_id: int, key: str, value: int):
    gset(guild_id, key, str(value))
def set_ticket_target(guild_id: int, user_id: int, channel_id: int):
    gset(guild_id, f"ticket_target_{user_id}", str(channel_id))
def get_ticket_target(guild_id: int, user_id: int) -> int | None:
    value = gget(guild_id, f"ticket_target_{user_id}")
    try:
        return int(value) if value else None
    except ValueError:
        return None
def get_ticket_dev_user_id() -> int | None:
    if not TICKET_DEV_USER_ID:
        return None
    try:
        return int(TICKET_DEV_USER_ID)
    except ValueError:
        return None
def get_ticket_archive_channel_id() -> int | None:
    if not TICKET_ARCHIVE_CHANNEL_ID:
        return None
    try:
        return int(TICKET_ARCHIVE_CHANNEL_ID)
    except ValueError:
        return None
async def create_ticket_request(guild_id: int, requester_id: int, requester_name: str) -> int:
    created_at = datetime.now(timezone.utc).isoformat()
    async with db_write_lock:
        with db_config() as conn:
            cur = conn.execute("""
                INSERT INTO tickets(
                    guild_id, requester_id, requester_name,
                    channel_id, thread_id, created_at_utc, status
                )
                VALUES (?, ?, ?, NULL, NULL, ?, 'open')
            """, (guild_id, requester_id, requester_name, created_at))
            return int(cur.lastrowid)
async def update_ticket_request(ticket_id: int, channel_id: int, thread_id: int):
    async with db_write_lock:
        with db_config() as conn:
            conn.execute("""
                UPDATE tickets
                SET channel_id=?, thread_id=?
                WHERE ticket_id=?
            """, (channel_id, thread_id, ticket_id))
async def close_ticket_request(ticket_id: int, archive_thread_id: int):
    closed_at = datetime.now(timezone.utc).isoformat()
    async with db_write_lock:
        with db_config() as conn:
            conn.execute("""
                UPDATE tickets
                SET archive_thread_id=?, closed_at_utc=?, status='closed'
                WHERE ticket_id=?
            """, (archive_thread_id, closed_at, ticket_id))
async def add_ticket_collaborator(ticket_id: int, user_id: int, added_by_id: int):
    added_at = datetime.now(timezone.utc).isoformat()
    async with db_write_lock:
        with db_config() as conn:
            conn.execute("""
                INSERT INTO ticket_collaborators(
                    ticket_id, user_id, added_by_id, added_at_utc
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(ticket_id, user_id) DO UPDATE SET
                    added_by_id=excluded.added_by_id,
                    added_at_utc=excluded.added_at_utc
            """, (ticket_id, user_id, added_by_id, added_at))
def set_guild_channel(guild_id: int, channel_id: int):
    with db_config() as conn:
        conn.execute("""
            INSERT INTO guild_config(guild_id, channel_id, enabled)
            VALUES(?, ?, 1)
            ON CONFLICT(guild_id) DO UPDATE SET channel_id=excluded.channel_id, enabled=1
        """, (guild_id, channel_id))
def disable_guild(guild_id: int):
    with db_config() as conn:
        conn.execute("UPDATE guild_config SET enabled=0 WHERE guild_id=?", (guild_id,))
def get_enabled_guilds():
    with db_config() as conn:
        return conn.execute("""
            SELECT guild_id, channel_id, timezone
            FROM guild_config
            WHERE enabled=1
        """).fetchall()
def get_guild_config(guild_id: int):
    with db_config() as conn:
        return conn.execute("""
            SELECT guild_id, channel_id, timezone
            FROM guild_config
            WHERE enabled=1 AND guild_id=?
        """, (guild_id,)).fetchone()
def get_ticket_by_thread_id(thread_id: int):
    with db_config() as conn:
        return conn.execute("""
            SELECT ticket_id, guild_id, requester_id, requester_name,
                   channel_id, thread_id, archive_thread_id, status
            FROM tickets
            WHERE thread_id=?
        """, (thread_id,)).fetchone()
# =========================
# EVENT LOGGING (yearly)
# =========================
def now_utc_local():
    utc = datetime.now(timezone.utc)
    local = utc.astimezone(LOCAL_TZ)
    return utc, local
def _fetch_url_text(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        }
    )
    with urllib.request.urlopen(req, timeout=20) as response:
        return response.read().decode("utf-8")
def _extract_channel_id_from_handle_page(html: str) -> str | None:
    marker = '"channelId":"'
    idx = html.find(marker)
    if idx == -1:
        return None
    start = idx + len(marker)
    end = html.find('"', start)
    if end == -1:
        return None
    return html[start:end]
async def resolve_wesroth_channel_id() -> str | None:
    if WESROTH_CHANNEL_ID:
        return WESROTH_CHANNEL_ID
    try:
        html = await asyncio.to_thread(_fetch_url_text, WESROTH_HANDLE_URL)
    except OSError as exc:
        print(f"Failed to fetch WesRoth channel page: {exc}")
        return None
    channel_id = _extract_channel_id_from_handle_page(html)
    if not channel_id:
        print("Failed to parse WesRoth channel id from handle page.")
    return channel_id
def _parse_wesroth_feed(xml_text: str) -> list[dict]:
    root = ET.fromstring(xml_text)
    entries = []
    for entry in root.findall("atom:entry", WESROTH_NS):
        video_id = entry.findtext("yt:videoId", default=None, namespaces=WESROTH_NS)
        published = entry.findtext("atom:published", default=None, namespaces=WESROTH_NS)
        if not video_id or not published:
            continue
        link = None
        for link_elem in entry.findall("atom:link", WESROTH_NS):
            if link_elem.get("rel") == "alternate":
                link = link_elem.get("href")
                break
        duration_elem = entry.find("media:group/yt:duration", WESROTH_NS)
        duration_seconds = None
        if duration_elem is not None:
            duration_raw = duration_elem.get("seconds")
            if duration_raw and duration_raw.isdigit():
                duration_seconds = int(duration_raw)
        entries.append({
            "video_id": video_id,
            "published": published,
            "link": link or f"https://www.youtube.com/watch?v={video_id}",
            "duration_seconds": duration_seconds,
        })
    return entries
async def fetch_wesroth_latest_today() -> dict | None:
    channel_id = await resolve_wesroth_channel_id()
    if not channel_id:
        return None
    feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    try:
        xml_text = await asyncio.to_thread(_fetch_url_text, feed_url)
    except OSError as exc:
        print(f"Failed to fetch WesRoth feed: {exc}")
        return None
    entries = _parse_wesroth_feed(xml_text)
    if not entries:
        return None
    today_local = datetime.now(LOCAL_TZ).date()
    todays_entries = []
    for entry in entries:
        try:
            published_dt = datetime.fromisoformat(entry["published"].replace("Z", "+00:00"))
        except ValueError:
            continue
        if published_dt.astimezone(LOCAL_TZ).date() != today_local:
            continue
        duration_seconds = entry.get("duration_seconds")
        if duration_seconds is not None and duration_seconds <= 60:
            continue
        entry["published_dt"] = published_dt
        todays_entries.append(entry)
    if not todays_entries:
        return None
    return max(todays_entries, key=lambda item: item["published_dt"])
async def log_event(
    event_type: str,
    user_id: int,
    username: str,
    guild_id: int | None,
    channel_id: int | None,
    message_id: int | None,
    target_event_id: str | None = None,
    note: str | None = None
) -> str:
    utc, local = now_utc_local()
    event_year = local.year
    init_year_db(event_year)
    event_id = str(uuid.uuid4())
    async with db_write_lock:
        with db_year(event_year) as conn:
            conn.execute("""
                INSERT INTO events(
                    event_id, event_type,
                    timestamp_utc, timestamp_local,
                    date_local, time_local,
                    user_id, username,
                    guild_id, channel_id, message_id,
                    target_event_id, note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event_id, event_type,
                utc.isoformat(), local.isoformat(),
                local.date().isoformat(), local.time().replace(microsecond=0).isoformat(),
                user_id, username,
                guild_id, channel_id, message_id,
                target_event_id, note
            ))
    return event_id
async def log_cleanup_message(message: discord.Message):
    created_at = message.created_at.astimezone(timezone.utc)
    async with db_write_lock:
        with db_cleanup() as conn:
            conn.execute("""
                INSERT INTO cleanup_messages(
                    message_id, channel_id, guild_id,
                    user_id, username, content, created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                message.id,
                message.channel.id,
                message.guild.id if message.guild else None,
                message.author.id,
                str(message.author),
                message.content,
                created_at.isoformat()
            ))
    print(
        "Cleanup message stored:",
        f"user={message.author} ({message.author.id})",
        f"channel={message.channel.id}",
        f"time={created_at.isoformat()}",
        f"text={message.content!r}"
    )
def find_last_active_poop_event_id(user_id: int, year: int) -> str | None:
    """Most recent POOP in the given year that has NOT been undone by that same user."""
    init_year_db(year)
    start_local = datetime(year, 1, 1, 0, 0, 0, tzinfo=LOCAL_TZ)
    end_local = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=LOCAL_TZ)
    with db_year(year) as conn:
        poops = conn.execute("""
            SELECT event_id
            FROM events
            WHERE event_type='POOP'
              AND user_id=?
              AND timestamp_local >= ?
              AND timestamp_local < ?
            ORDER BY timestamp_local DESC
            LIMIT 500
        """, (user_id, start_local.isoformat(), end_local.isoformat())).fetchall()
        if not poops:
            return None
        poop_ids = [r["event_id"] for r in poops]
        undone = conn.execute(f"""
            SELECT target_event_id
            FROM events
            WHERE event_type='UNDO'
              AND user_id=?
              AND target_event_id IN ({",".join("?" * len(poop_ids))})
        """, (user_id, *poop_ids)).fetchall()
        undone_set = {r["target_event_id"] for r in undone}
        for pid in poop_ids:
            if pid not in undone_set:
                return pid
    return None
# =========================
# BUTTON POSTING (per guild)
# =========================
async def post_button_for_guild(guild_id: int, channel_id: int):
    channel = await bot.fetch_channel(channel_id)
    # delete previous active button message
    old_message_id = gget(guild_id, "active_message_id")
    if old_message_id:
        try:
            old_msg = await channel.fetch_message(int(old_message_id))
            if old_msg.author.id == bot.user.id:
                await old_msg.delete()
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass
    local_now = datetime.now(LOCAL_TZ)
    msg = await channel.send(
        f"💩 **Click here to log a poop** — {local_now.strftime('%Y-%m-%d')} (Pacific)\n"
        f"React {POOP_EMOJI} to log.\n"
        f"React {UNDO_EMOJI} to undo your most recent log.\n"
        "Want to see a new feature for the bot? (It doesn't have to be poop-logging related) "
        "/featurerequest to get started"
    )
    await msg.add_reaction(POOP_EMOJI)
    await msg.add_reaction(UNDO_EMOJI)
    gset(guild_id, "active_message_id", str(msg.id))
    gset(guild_id, "active_date_local", local_now.date().isoformat())
    gset_int(guild_id, "poops_since_post", 0)
@tasks.loop(time=dtime(hour=0, minute=0, tzinfo=LOCAL_TZ))
async def daily_midnight_pacific():
    today_local = datetime.now(LOCAL_TZ).date().isoformat()
    for row in get_enabled_guilds():
        gid = int(row["guild_id"])
        cid = int(row["channel_id"])
        try:
            # only post if we haven't posted today for that guild
            last_date = gget(gid, "active_date_local")
            if last_date != today_local:
                await post_button_for_guild(gid, cid)
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            continue
@tasks.loop(minutes=WESROTH_POLL_MINUTES)
async def wesroth_upload_watch():
    latest = await fetch_wesroth_latest_today()
    if not latest:
        return
    last_video_id = gget(0, "wesroth_last_video_id")
    if last_video_id == latest["video_id"]:
        return
    channel = bot.get_channel(WESROTH_ALERT_CHANNEL_ID)
    if channel is None:
        try:
            channel = await bot.fetch_channel(WESROTH_ALERT_CHANNEL_ID)
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            return
    caption = random.choice(WESROTH_CAPTIONS)
    await channel.send(f"{caption}\n{latest['link']}")
    gset(0, "wesroth_last_video_id", latest["video_id"])
    gset(0, "wesroth_last_post_date_local", datetime.now(LOCAL_TZ).date().isoformat())
# =========================
# MESSAGE CLEANUP CHANNEL
# =========================
@bot.event
async def on_message(message: discord.Message):
    # delete any non-bot message in the cleanup channel
    if message.channel.id == CLEANUP_CHANNEL_ID and not message.author.bot:
        try:
            await log_cleanup_message(message)
            await message.delete()
        except (discord.Forbidden, discord.HTTPException):
            pass
        # still allow commands processing elsewhere; this message is gone anyway
        return
    await bot.process_commands(message)
# =========================
# REACTIONS
# =========================
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.user_id == bot.user.id:
        return
    if payload.guild_id is None:
        return
    active_message_id = gget(payload.guild_id, "active_message_id")
    if not active_message_id or str(payload.message_id) != active_message_id:
        return
    emoji = str(payload.emoji)
    channel = await bot.fetch_channel(payload.channel_id)
    message = await channel.fetch_message(payload.message_id)
    user = await bot.fetch_user(payload.user_id)
    mention = f"<@{payload.user_id}>"
    if emoji == POOP_EMOJI:
        await log_event(
            event_type="POOP",
            user_id=payload.user_id,
            username=str(user),
            guild_id=payload.guild_id,
            channel_id=payload.channel_id,
            message_id=payload.message_id
        )
        await channel.send(random.choice(CONGRATS).format(user=mention))
        # remove reaction so they can click again
        try:
            await message.remove_reaction(payload.emoji, user)
        except discord.Forbidden:
            pass
        count = gget_int(payload.guild_id, "poops_since_post", 0) + 1
        gset_int(payload.guild_id, "poops_since_post", count)
        if count >= ROTATE_EVERY:
            cfg = get_guild_config(payload.guild_id)
            if cfg:
                await post_button_for_guild(payload.guild_id, int(cfg["channel_id"]))
        return
    if emoji == UNDO_EMOJI:
        year = current_year_local()
        target = find_last_active_poop_event_id(payload.user_id, year)
        if not target:
            await channel.send(f"{mention} you don’t have a poop to undo.")
        else:
            await log_event(
                event_type="UNDO",
                user_id=payload.user_id,
                username=str(user),
                guild_id=payload.guild_id,
                channel_id=payload.channel_id,
                message_id=payload.message_id,
                target_event_id=target
            )
            await channel.send(random.choice(UNDO_MSGS).format(user=mention))
        try:
            await message.remove_reaction(payload.emoji, user)
        except discord.Forbidden:
            pass
        return
# =========================
# STATS HELPERS
# =========================
def circular_mean_time(minutes_list: list[float]) -> float | None:
    if not minutes_list:
        return None
    angles = [m * 2 * math.pi / 1440.0 for m in minutes_list]
    s = sum(math.sin(a) for a in angles)
    c = sum(math.cos(a) for a in angles)
    mean_angle = math.atan2(s, c)
    if mean_angle < 0:
        mean_angle += 2 * math.pi
    return mean_angle * 1440.0 / (2 * math.pi)
def fmt_minutes_as_time(mins: float) -> str:
    mins = mins % 1440.0
    h = int(mins // 60)
    m = int(mins % 60)
    return f"{h:02d}:{m:02d}"
def _net_poop_rows_for_year(user_id: int, year: int):
    """Return list of (event_id, date_local, time_local, timestamp_local) for POOP not undone."""
    init_year_db(year)
    start_local = datetime(year, 1, 1, 0, 0, 0, tzinfo=LOCAL_TZ)
    end_local = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=LOCAL_TZ)
    with db_year(year) as conn:
        poops = conn.execute("""
            SELECT event_id, date_local, time_local, timestamp_local
            FROM events
            WHERE event_type='POOP'
              AND user_id=?
              AND timestamp_local >= ?
              AND timestamp_local < ?
        """, (user_id, start_local.isoformat(), end_local.isoformat())).fetchall()
        if not poops:
            return []
        poop_ids = [r["event_id"] for r in poops]
        undone = conn.execute(f"""
            SELECT target_event_id
            FROM events
            WHERE event_type='UNDO'
              AND user_id=?
              AND target_event_id IN ({",".join("?" * len(poop_ids))})
        """, (user_id, *poop_ids)).fetchall()
        undone_set = {r["target_event_id"] for r in undone}
        net = [r for r in poops if r["event_id"] not in undone_set]
        return net
def get_user_year_stats(user_id: int, year: int):
    net = _net_poop_rows_for_year(user_id, year)
    if not net:
        return 0, [], 0
    active_count = len(net)
    first_active_date = min(date.fromisoformat(r["date_local"]) for r in net)
    today_local = datetime.now(LOCAL_TZ).date()
    days_elapsed = (today_local - first_active_date).days + 1  # inclusive
    times = []
    for r in net:
        hh, mm, ss = r["time_local"].split(":")
        minutes = int(hh) * 60 + int(mm) + (int(ss) / 60.0)
        times.append(minutes)
    return active_count, times, days_elapsed
def get_latest_poop(user_id: int, year: int) -> str | None:
    net = _net_poop_rows_for_year(user_id, year)
    if not net:
        return None
    latest = max(net, key=lambda r: r["timestamp_local"])
    return latest["timestamp_local"]
def get_max_poops_in_one_day(user_id: int, year: int) -> tuple[int, str | None]:
    net = _net_poop_rows_for_year(user_id, year)
    if not net:
        return 0, None
    counts: dict[str, int] = {}
    for r in net:
        d = r["date_local"]
        counts[d] = counts.get(d, 0) + 1
    best_date = max(counts, key=lambda d: counts[d])
    return counts[best_date], best_date
# =========================
# COMMANDS (slash)
# =========================
@bot.slash_command(name="setpoopchannel", description="Set the poop logging channel for this server.")
@discord.default_permissions(administrator=True)
@discord.guild_only()
async def setpoopchannel(interaction: discord.Interaction):
    if interaction.guild is None or interaction.channel is None:
        return
    set_guild_channel(interaction.guild.id, interaction.channel.id)
    await interaction.response.send_message(
        f"✅ Poop channel set to {interaction.channel.mention} for this server."
    )
@bot.slash_command(name="disablepoop", description="Disable poop posting for this server.")
@discord.default_permissions(administrator=True)
@discord.guild_only()
async def disablepoop(interaction: discord.Interaction):
    if interaction.guild is None:
        return
    disable_guild(interaction.guild.id)
    await interaction.response.send_message("🛑 Poop posting disabled for this server.")
@bot.slash_command(name="debugpoop", description="Force-create a new poop button message.")
@discord.default_permissions(administrator=True)
@discord.guild_only()
async def debugpoop(interaction: discord.Interaction):
    """Force-create a new poop button message in this guild's configured channel."""
    if interaction.guild is None:
        return
    cfg = get_guild_config(interaction.guild.id)
    if not cfg:
        await interaction.response.send_message(
            "Run /setpoopchannel in the channel you want first."
        )
        return
    await post_button_for_guild(interaction.guild.id, int(cfg["channel_id"]))
    await interaction.response.send_message("🧪 Debug: recreated poop button.")
@bot.slash_command(name="poopstats", description="Show your poop stats for the current year.")
@discord.guild_only()
async def poopstats(interaction: discord.Interaction):
    user_id = interaction.user.id
    year = current_year_local()
    total, times, days_elapsed = get_user_year_stats(user_id, year)
    avg_per_day = (total / days_elapsed) if days_elapsed else 0.0
    mean_minutes = circular_mean_time(times)
    mean_time_str = fmt_minutes_as_time(mean_minutes) if mean_minutes is not None else "N/A"
    latest = get_latest_poop(user_id, year)
    max_day_count, max_day_date = get_max_poops_in_one_day(user_id, year)
    latest_str = (
        datetime.fromisoformat(latest).strftime("%Y-%m-%d %H:%M")
        if latest else "N/A"
    )
    max_day_str = f"{max_day_count} on {max_day_date}" if max_day_date else "N/A"
    await interaction.response.send_message(
        f"**{interaction.user.mention} — {year} Poop Stats**\n"
        f"- Total poops: **{total}**\n"
        f"- Avg poops/day ({year}, since first logged): **{avg_per_day:.3f}**\n"
        f"- Avg local time: **{mean_time_str} Pacific**\n"
        f"- Latest poop: **{latest_str}**\n"
        f"- Most poops in one day: **{max_day_str}**"
    )
@bot.slash_command(name="featurerequest", description="Start a feature request ticket.")
@discord.guild_only()
async def featurerequest(interaction: discord.Interaction):
    if interaction.guild is None or interaction.channel is None:
        await interaction.response.send_message(
            "Feature requests can only be created in a server.",
            ephemeral=True
        )
        return
    dev_user_id = get_ticket_dev_user_id()
    dev_member = interaction.guild.get_member(dev_user_id) if dev_user_id else None
    ticket_id = await create_ticket_request(
        guild_id=interaction.guild.id,
        requester_id=interaction.user.id,
        requester_name=str(interaction.user)
    )
    thread_name = f"ticket-{ticket_id}-{interaction.user.name}".lower().replace(" ", "-")
    ticket_target = await interaction.channel.create_thread(
        name=thread_name,
        type=discord.ChannelType.private_thread
    )
    await ticket_target.add_user(interaction.user)
    if dev_member:
        await ticket_target.add_user(dev_member)
    await update_ticket_request(ticket_id, interaction.channel.id, ticket_target.id)
    set_ticket_target(interaction.guild.id, interaction.user.id, ticket_target.id)
    dev_mention = dev_member.mention if dev_member else ""
    mention_line = " ".join(part for part in [interaction.user.mention, dev_mention] if part)
    prompt_lines = [
        f"{mention_line} **(Ticket #{ticket_id})**",
        "**Feature request intake**",
        "- **What is the feature?**",
        "- **How do you want to use it?**",
        "- **Give an example of how it is triggered, what happens, etc.**",
        "",
        "Want to bring someone else in? `/collab @user` in this chat."
    ]
    await ticket_target.send("\n".join(prompt_lines))
    await interaction.response.send_message(
        f"✅ Created ticket #{ticket_id} in {ticket_target.mention}.",
        ephemeral=True
    )
@bot.slash_command(name="collab", description="Add a collaborator to the current ticket thread.")
@discord.guild_only()

async def collab(interaction: discord.Interaction, user: discord.Option(discord.Member, "User to add to the ticket thread.")):
    if interaction.guild is None:
        await interaction.response.send_message(
            "This command can only be used in a server.",
            ephemeral=True
        )
        return
    if not isinstance(interaction.channel, discord.Thread):
        await interaction.response.send_message(
            "Please use this command inside a ticket thread.",
            ephemeral=True
        )
        return
    ticket = get_ticket_by_thread_id(interaction.channel.id)
    if not ticket:
        await interaction.response.send_message(
            "No ticket is associated with this thread.",
            ephemeral=True
        )
        return
    try:
        await interaction.channel.add_user(user)
    except (discord.Forbidden, discord.HTTPException):
        await interaction.response.send_message(
            "I couldn't add that user to this thread.",
            ephemeral=True
        )
        return
    await add_ticket_collaborator(ticket["ticket_id"], user.id, interaction.user.id)
    await interaction.response.send_message(
        f"✅ Added {user.mention} to this ticket thread."
    )
@bot.slash_command(name="closeticket", description="Close the current ticket thread.")
@discord.guild_only()
async def closeticket(interaction: discord.Interaction):
    if interaction.guild is None:
        return
    dev_user_id = get_ticket_dev_user_id()
    if dev_user_id is None or interaction.user.id != dev_user_id:
        return
    ticket = get_ticket_by_thread_id(interaction.channel.id)
    if not ticket:
        await interaction.response.send_message(
            "No ticket is associated with this channel.",
            ephemeral=True
        )
        return
    if ticket["status"] != "open":
        await interaction.response.send_message(
            f"Ticket #{ticket['ticket_id']} is already closed.",
            ephemeral=True
        )
        return
    archive_channel_id = get_ticket_archive_channel_id()
    if archive_channel_id is None:
        await interaction.response.send_message(
            "Archive channel is not configured.",
            ephemeral=True
        )
        return
    await interaction.response.send_message(
        "🔒 This ticket has been closed and will be archived and deleted in 24h."
    )
    archive_channel = await bot.fetch_channel(archive_channel_id)
    thread_name = f"ticket-{ticket['ticket_id']}-archive"
    archive_thread = await archive_channel.create_thread(
        name=thread_name,
        type=discord.ChannelType.private_thread
    )
    dev_member = archive_channel.guild.get_member(dev_user_id)
    if dev_member:
        await archive_thread.add_user(dev_member)
    await archive_thread.send(f"**Ticket #{ticket['ticket_id']} archive**")
    allowed_ids = {ticket["requester_id"], dev_user_id}
    async for message in interaction.channel.history(oldest_first=True, limit=None):
        if message.author.id not in allowed_ids:
            continue
        content_parts = []
        if message.content:
            content_parts.append(message.content)
        if message.attachments:
            content_parts.extend(att.url for att in message.attachments)
        content = "\n".join(content_parts).strip()
        if not content:
            continue
        await archive_thread.send(f"**{message.author.display_name}:** {content}")
    await close_ticket_request(ticket["ticket_id"], archive_thread.id)
def is_dev_user(user_id: int) -> bool:
    dev_user_id = get_ticket_dev_user_id()
    return bool(dev_user_id and user_id == dev_user_id)
@discord.guild_only()
@bot.slash_command(name="gplay", description="Queue and play audio from a YouTube link or search term.")

async def gplay(interaction: discord.Interaction, youtube_link: discord.Option(str, "A YouTube URL or search text.")):
    if interaction.guild is None:
        await interaction.response.send_message("This command only works in a server.", ephemeral=True)
        return
    voice_channel = await ensure_voice_channel(interaction)
    if voice_channel is None:
        await interaction.response.send_message(
            "You must be in a voice channel to use this command.",
            ephemeral=True
        )
        return
    await interaction.response.defer(ephemeral=True)
    await interaction.followup.send("Working…", ephemeral=True)
    if interaction.guild.voice_client and interaction.guild.voice_client.channel != voice_channel:
        await interaction.followup.send(
            f"You must be in {interaction.guild.voice_client.channel.mention} to control playback.",
            ephemeral=True
        )
        return
    source = youtube_link.strip()
    if not source:
        await interaction.followup.send("Please provide a YouTube link or search query.", ephemeral=True)
        return
    if not is_youtube_url(source):
        source = f"ytsearch1:{source}"
    if interaction.guild.voice_client is None:
        try:
            await voice_channel.connect()
        except discord.DiscordException as exc:
            await interaction.followup.send(f"Could not join voice channel: {exc}", ephemeral=True)
            return
    fetch_started_at = time.perf_counter()
    try:
        tracks = await asyncio.wait_for(
            fetch_tracks(source),
            timeout=FETCH_TRACK_INFO_TIMEOUT_SECONDS,
        )
        log_music_timing("fetch_track_info", "end", fetch_started_at, source=source)
    except asyncio.TimeoutError:
        log_music_timing("fetch_track_info", "timeout", fetch_started_at, source=source)
        await interaction.followup.send(FETCH_TRACK_INFO_TIMEOUT_MESSAGE, ephemeral=True)
        return
    except RuntimeError as exc:
        await interaction.followup.send(f"Could not fetch audio: {exc}", ephemeral=True)
        return
    for track in tracks:
        track.requested_by = interaction.user.id
    state = get_music_state(interaction.guild.id)
    async with state.lock:
        starting_queue_size = len(state.queue)
        state.queue.extend(tracks)
        first_queue_position = starting_queue_size + 1
    await play_next_track(interaction.guild)
    if len(tracks) == 1:
        track = tracks[0]
        await interaction.followup.send(
            (
                f"Queued **{track.title}** ({format_duration(track.duration_seconds)}). "
                f"Position in queue: **{first_queue_position}**. [YouTube link]({track.source_url})"
            ),
            ephemeral=True
        )
        return
    total_seconds = sum(track.duration_seconds for track in tracks)
    first_track = tracks[0]
    await interaction.followup.send(
        (
            f"Queued **{len(tracks)}** tracks from playlist/search results "
            f"(total {format_duration(total_seconds)}). "
            f"First position: **{first_queue_position}**. "
            f"Starts with: **{first_track.title}** ([YouTube link]({first_track.source_url}))."
        ),
        ephemeral=True
    )
@discord.guild_only()
@bot.slash_command(name="gqueue", description="Show the current playback queue.")
async def gqueue(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("This command only works in a server.", ephemeral=True)
        return
    voice_channel = await ensure_voice_channel(interaction)
    if voice_channel is None:
        await interaction.response.send_message(
            "You must be in a voice channel to use this command.",
            ephemeral=True
        )
        return
    vc = interaction.guild.voice_client
    if vc is not None and vc.channel != voice_channel:
        await interaction.response.send_message(
            f"You must be in {vc.channel.mention} to view this queue.",
            ephemeral=True
        )
        return
    state = get_music_state(interaction.guild.id)
    async with state.lock:
        current_track = state.current_track
        started_at = state.track_started_at
        queued_tracks = list(state.queue)
    lines = ["**Goki Queue**"]
    if current_track:
        elapsed = 0
        if started_at is not None:
            elapsed = int((datetime.now(timezone.utc) - started_at).total_seconds())
        lines.append(
            (
                f"Now playing: **{current_track.title}** "
                f"[{format_duration(elapsed)} / {format_duration(current_track.duration_seconds)}] "
                f"([YouTube]({current_track.source_url}))"
            )
        )
    else:
        lines.append("Now playing: *(nothing)*")
    if queued_tracks:
        lines.append("\n**Up next:**")
        for i, track in enumerate(queued_tracks, start=1):
            lines.append(
                f"{i}. {track.title} ({format_duration(track.duration_seconds)}) ([YouTube]({track.source_url}))"
            )
    else:
        lines.append("\nQueue is empty.")
    await interaction.response.send_message("\n".join(lines), ephemeral=True)
@discord.guild_only()
@bot.slash_command(name="gskip", description="Skip the currently playing track.")
async def gskip(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("This command only works in a server.", ephemeral=True)
        return
    voice_channel = await ensure_voice_channel(interaction)
    if voice_channel is None:
        await interaction.response.send_message(
            "You must be in a voice channel to use this command.",
            ephemeral=True
        )
        return
    vc = interaction.guild.voice_client
    if vc is None or not vc.is_connected():
        await interaction.response.send_message("Nothing is playing right now.", ephemeral=True)
        return
    if vc.channel != voice_channel:
        await interaction.response.send_message(
            f"You must be in {vc.channel.mention} to skip tracks.",
            ephemeral=True
        )
        return
    if not vc.is_playing() and not vc.is_paused():
        await interaction.response.send_message("Nothing is currently playing.", ephemeral=True)
        return
    vc.stop()
    await interaction.response.send_message("⏭️ Skipped current track.", ephemeral=True)
@discord.guild_only()
@bot.slash_command(name="gtranscribe", description="Join your voice channel and start recording speakers for transcription.")
async def gtranscribe(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("This command only works in a server.", ephemeral=True)
        return
    can_record, record_error = can_record_voice()
    if not can_record:
        await interaction.response.send_message(
            f"Voice recording is unavailable: {record_error} Install dependencies with `pip install -r requirements.txt` and ensure system Opus is installed (for Debian/Ubuntu: `sudo apt install libopus0`).",
            ephemeral=True,
        )
        return
    voice_channel = await ensure_voice_channel(interaction)
    if voice_channel is None:
        await interaction.response.send_message(
            "You must be in a voice channel to start transcription.",
            ephemeral=True,
        )
        return
    existing = get_transcription_session(interaction.guild.id)
    if existing is not None:
        await interaction.response.send_message(
            "A transcription session is already active in this server. Use `/gendsession` first.",
            ephemeral=True,
        )
        return
    await interaction.response.defer(ephemeral=True)
    await interaction.followup.send("Working…", ephemeral=True)
    vc = interaction.guild.voice_client
    connected_here = False
    if vc is not None and vc.channel != voice_channel:
        await interaction.followup.send(
            f"I am already connected to {vc.channel.mention}. Disconnect or move me first.",
            ephemeral=True,
        )
        return
    if vc is None or not vc.is_connected():
        try:
            vc = await voice_channel.connect(timeout=15.0, reconnect=True)
            connected_here = True
        except (discord.ClientException, discord.HTTPException, asyncio.TimeoutError) as exc:
            print(f"[transcribe] voice connect failed: {type(exc).__name__}: {exc}")
            await interaction.followup.send(
                "I couldn't join your voice channel. Confirm I have **Connect/Speak** permissions and that PyNaCl is installed (`pip install -r requirements.txt`).",
                ephemeral=True,
            )
            return
    if vc is None:
        await interaction.followup.send("I couldn't initialize a voice client.", ephemeral=True)
        return
    if getattr(vc, "recording", False):
        await interaction.followup.send("I am already recording in this server.", ephemeral=True)
        return
    session = GuildTranscriptionSession(interaction.guild.id, voice_channel.id)
    def _recording_finished(sink: object, channel: object, *_: object):
        guild = interaction.guild
        if guild is None:
            return
        active_session = get_transcription_session(guild.id)
        if active_session is None:
            return
        copy_recorded_audio_to_session(sink, guild, active_session)
    try:
        sink = discord.sinks.WaveSink()
        vc.start_recording(
            sink,
            _recording_finished,
            interaction.channel,
        )
    except Exception as exc:
        print(f"[transcribe] start_recording failed: {type(exc).__name__}: {exc}")
        shutil.rmtree(session.temp_dir, ignore_errors=True)
        if connected_here:
            try:
                await vc.disconnect(force=True)
            except (discord.HTTPException, discord.ClientException):
                pass
        await interaction.followup.send(
            f"I joined voice but couldn't start recording: `{type(exc).__name__}`. Ensure `PyNaCl` is installed in the bot venv and system Opus is available (`libopus0`/`libopus.so`).",
            ephemeral=True,
        )
        return
    transcription_sessions[interaction.guild.id] = session
    await interaction.followup.send(
        f"🎙️ Started transcription capture in {voice_channel.mention}. Use `/gendsession` when you're done.",
        ephemeral=True,
    )
@discord.guild_only()
@bot.slash_command(name="gsetuser", description="Set a display alias for a Discord user id in the active transcription session.")

async def gsetuser(
    interaction: discord.Interaction,
    user: discord.Option(discord.Member, "User to alias"),
    name: discord.Option(str, "Alias to write in the transcript"),
):
    if interaction.guild is None:
        await interaction.response.send_message("This command only works in a server.", ephemeral=True)
        return
    session = get_transcription_session(interaction.guild.id)
    if session is None:
        await interaction.response.send_message(
            "No active transcription session. Start one with `/gtranscribe`.",
            ephemeral=True,
        )
        return
    alias = name.strip()
    if not alias:
        await interaction.response.send_message("Alias cannot be empty.", ephemeral=True)
        return
    session.aliases_by_user[user.id] = alias
    await interaction.response.send_message(
        f"✅ Transcript alias set: `{user.id}` → **{alias}**",
        ephemeral=True,
    )
@discord.guild_only()
@bot.slash_command(name="gendsession", description="Stop recording and export the transcript text file.")
async def gendsession(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("This command only works in a server.", ephemeral=True)
        return
    session = get_transcription_session(interaction.guild.id)
    if session is None:
        await interaction.response.send_message(
            "No active transcription session in this server.",
            ephemeral=True,
        )
        return
    vc = interaction.guild.voice_client
    if vc is None or not vc.is_connected():
        remove_transcription_session(interaction.guild.id)
        await interaction.response.send_message(
            "The voice session was already disconnected, so no recording could be finalized.",
            ephemeral=True,
        )
        return
    await interaction.response.defer(ephemeral=True)
    await interaction.followup.send("Working…", ephemeral=True)
    if getattr(vc, "recording", False):
        vc.stop_recording()
        await asyncio.sleep(1)
    transcript_path, error_message = await finalize_transcription_session(interaction, session)
    try:
        await vc.disconnect(force=True)
    except (discord.HTTPException, discord.ClientException):
        pass
    if transcript_path is None:
        remove_transcription_session(interaction.guild.id)
        await interaction.followup.send(f"⚠️ Session ended, but transcript export failed: {error_message}", ephemeral=True)
        return
    transcript_file = discord.File(str(transcript_path), filename=transcript_path.name)
    await interaction.followup.send(
        content="📝 Transcription session ended. Here is your transcript file.",
        file=transcript_file,
        ephemeral=True,
    )
    remove_transcription_session(interaction.guild.id)
@bot.slash_command(name="gokibothelp", description="Show all available GokiBot commands.")
async def gokibothelp(interaction: discord.Interaction):
    command_lines = [
        "**GokiBot Commands**",
        "- `/poopstats` — Show your poop stats for the year.",
        "- `/featurerequest` — Start a feature request ticket.",
        "- `/collab` — Add someone to the current ticket thread.",
        "- `/gplay <youtube_link_or_search>` — Queue and play YouTube audio.",
        "- `/gqueue` — Show the current playback queue.",
        "- `/gskip` — Skip the currently playing track.",
        "- `/gtranscribe` — Start recording and isolate speakers for transcription.",
        "- `/gsetuser <user> <name>` — Alias a Discord user in transcript output.",
        "- `/gendsession` — Stop recording and export transcript text.",
        "- `/gokibothelp` — Show this help message."
    ]
    if is_dev_user(interaction.user.id):
        command_lines.extend([
            "",
            "**Dev/Admin Commands**",
            "- `/setpoopchannel` — Set the poop logging channel (admin only).",
            "- `/disablepoop` — Disable poop posting (admin only).",
            "- `/debugpoop` — Force-create a new poop button (admin only).",
            "- `/closeticket` — Close the current ticket thread (dev only)."
        ])
    await interaction.response.send_message("\n".join(command_lines), ephemeral=True)
# =========================
# STARTUP
# =========================
@bot.event
async def on_ready():
    init_config_db()
    init_year_db(current_year_local())
    init_cleanup_db()
    try:
        await bot.sync_commands()
    except (discord.HTTPException, discord.Forbidden):
        pass
    if not daily_midnight_pacific.is_running():
        daily_midnight_pacific.start()
    if not wesroth_upload_watch.is_running():
        wesroth_upload_watch.start()
    # If configured guilds haven't posted today, post immediately
    today_local = datetime.now(LOCAL_TZ).date().isoformat()
    for row in get_enabled_guilds():
        gid = int(row["guild_id"])
        cid = int(row["channel_id"])
        last_date = gget(gid, "active_date_local")
        if last_date != today_local:
            try:
                await post_button_for_guild(gid, cid)
            except (discord.Forbidden, discord.NotFound, discord.HTTPException):
                continue
    print(f"Logged in as {bot.user} (id={bot.user.id})")
if not TOKEN or TOKEN == "PUT_TOKEN_HERE_FOR_TESTING":
    raise RuntimeError("Set DISCORD_TOKEN_POOPBOT env var or paste token into TOKEN.")
bot.run(TOKEN)
