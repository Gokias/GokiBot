from dotenv import load_dotenv
import os
import math
import uuid
import random
import sqlite3
import asyncio
import logging
import logging.handlers
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
from datetime import datetime, timezone, date, time as dtime, timedelta
from pathlib import Path
import time
import discord
from discord.ext import commands, tasks

load_dotenv()  # loads variables from .env into the process environment

logger = logging.getLogger("gokibot")


def configure_logging() -> None:
    log_level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO"
    log_level = getattr(logging, log_level_name, logging.INFO)
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(formatter)
    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / "gokibot.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(log_level)
    root_logger.addHandler(stdout_handler)
    root_logger.addHandler(file_handler)
    logger.info("logging_configured level=%s", logging.getLevelName(log_level))


def interaction_log_context(interaction: discord.Interaction) -> dict[str, object]:
    return {
        "guild_id": getattr(interaction.guild, "id", None),
        "channel_id": getattr(interaction.channel, "id", None),
        "user_id": getattr(interaction.user, "id", None),
        "interaction": getattr(getattr(interaction, "command", None), "qualified_name", None),
    }


def register_loop_exception_handler(loop: asyncio.AbstractEventLoop) -> None:
    if getattr(loop, "_gokibot_exception_handler_installed", False):
        return
    default_handler = loop.get_exception_handler()

    def _loop_exception_handler(active_loop: asyncio.AbstractEventLoop, context: dict[str, object]) -> None:
        message = context.get("message", "Unhandled asyncio loop exception")
        exception = context.get("exception")
        if exception is not None:
            logger.exception("loop_exception message=%s context=%r", message, context, exc_info=exception)
        else:
            logger.error("loop_exception message=%s context=%r", message, context)
        if default_handler is not None:
            default_handler(active_loop, context)
        else:
            active_loop.default_exception_handler(context)

    loop.set_exception_handler(_loop_exception_handler)
    setattr(loop, "_gokibot_exception_handler_installed", True)
    logger.info("loop_exception_handler_registered")


configure_logging()
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
    logger.warning("voice opus_load_failed error=%s", _OPUS_LOAD_ERROR)

# =========================
# CONFIG
# =========================
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
TRANSCRIBE_SLICE_SECONDS = max(int(os.getenv("TRANSCRIBE_SLICE_SECONDS", "12")), 5)
TRANSCRIBE_MAX_FAILURES = max(int(os.getenv("TRANSCRIBE_MAX_FAILURES", "3")), 1)
TRANSCRIBE_CONSENT_VALID_DAYS = 180
TRANSCRIBE_CONSENT_EMOJI = "✅"
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
intents.voice_states = True  # needed to track voice-channel joins/leaves during transcription
intents.members = True  # needed to discover/add members to transcript thread and DM consent prompts
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
    def __init__(self, guild_id: int, voice_channel_id: int, transcript_thread_id: int):
        self.guild_id = guild_id
        self.voice_channel_id = voice_channel_id
        self.transcript_thread_id = transcript_thread_id
        self.started_at = datetime.now(timezone.utc)
        self.temp_dir = Path(tempfile.mkdtemp(prefix=f"gokibot_transcribe_{guild_id}_"))
        self.aliases_by_user: dict[int, str] = {}
        self.consented_user_ids: set[int] = set()
        self.prompted_user_ids: set[int] = set()
        self.slice_number = 0
        self.closed = False
        self.loop_task: asyncio.Task | None = None
        self.active_sink: object | None = None
        self.active_slice_done = asyncio.Event()
        self.recording_failure_count = 0


transcription_sessions: dict[int, GuildTranscriptionSession] = {}
transcription_consent_prompts: dict[int, tuple[int, int]] = {}
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
    stale_prompt_ids = [
        message_id
        for message_id, (prompt_guild_id, prompt_thread_id) in transcription_consent_prompts.items()
        if prompt_guild_id == guild_id and prompt_thread_id == session.transcript_thread_id
    ]
    for message_id in stale_prompt_ids:
        transcription_consent_prompts.pop(message_id, None)
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
def transcribe_audio_file(engine_name: str, engine: object, file_path: Path) -> list[dict[str, object]]:
    utterances: list[dict[str, object]] = []
    if engine_name == "faster_whisper":
        segments, _ = engine.transcribe(str(file_path), vad_filter=True)
        for seg in segments:
            phrase = (seg.text or "").strip()
            if not phrase:
                continue
            utterances.append({
                "start": float(getattr(seg, "start", 0.0) or 0.0),
                "text": phrase,
            })
        return utterances
    if engine_name == "whisper":
        result = engine.transcribe(str(file_path), fp16=False)
        segments = result.get("segments") if isinstance(result, dict) else None
        if isinstance(segments, list):
            for seg in segments:
                if not isinstance(seg, dict):
                    continue
                phrase = str(seg.get("text") or "").strip()
                if not phrase:
                    continue
                utterances.append({
                    "start": float(seg.get("start") or 0.0),
                    "text": phrase,
                })
        else:
            text_value = str((result or {}).get("text") or "").strip() if isinstance(result, dict) else ""
            if text_value:
                utterances.append({"start": 0.0, "text": text_value})
    return utterances


def normalize_transcript_display_name(name: str) -> str:
    compact = " ".join(name.split()).strip()
    return compact[:64]


def transcription_consent_is_active(consented_at: str | None, expires_at: str | None) -> bool:
    if not consented_at or not expires_at:
        return False
    try:
        consented_dt = datetime.fromisoformat(consented_at)
        expires_dt = datetime.fromisoformat(expires_at)
    except ValueError:
        return False
    if consented_dt.tzinfo is None:
        consented_dt = consented_dt.replace(tzinfo=timezone.utc)
    if expires_dt.tzinfo is None:
        expires_dt = expires_dt.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) <= expires_dt


def get_active_transcription_consent(guild_id: int, user_id: int) -> tuple[str | None, str | None, str | None]:
    with db_config() as conn:
        row = conn.execute(
            """
            SELECT display_name, consented_at_utc, expires_at_utc
            FROM transcription_consent
            WHERE guild_id=? AND user_id=?
            """,
            (guild_id, user_id),
        ).fetchone()
    if not row:
        return None, None, None
    display_name = normalize_transcript_display_name(str(row["display_name"] or ""))
    consented_at = row["consented_at_utc"]
    expires_at = row["expires_at_utc"]
    if not transcription_consent_is_active(consented_at, expires_at):
        return None, consented_at, expires_at
    if not display_name:
        return None, consented_at, expires_at
    return display_name, consented_at, expires_at


async def upsert_transcription_consent(guild_id: int, user_id: int, display_name: str):
    clean_name = normalize_transcript_display_name(display_name)
    now_utc = datetime.now(timezone.utc)
    expires = now_utc + timedelta(days=TRANSCRIBE_CONSENT_VALID_DAYS)
    async with db_write_lock:
        with db_config() as conn:
            conn.execute(
                """
                INSERT INTO transcription_consent(
                    guild_id, user_id, display_name, consented_at_utc, expires_at_utc
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(guild_id, user_id) DO UPDATE SET
                    display_name=excluded.display_name,
                    consented_at_utc=excluded.consented_at_utc,
                    expires_at_utc=excluded.expires_at_utc
                """,
                (
                    guild_id,
                    user_id,
                    clean_name,
                    now_utc.isoformat(),
                    expires.isoformat(),
                ),
            )


async def mark_transcription_consent_prompt_sent(guild_id: int, user_id: int):
    now_utc = datetime.now(timezone.utc)
    async with db_write_lock:
        with db_config() as conn:
            conn.execute(
                """
                INSERT INTO transcription_consent_prompts_sent(
                    guild_id, user_id, prompted_at_utc
                ) VALUES (?, ?, ?)
                ON CONFLICT(guild_id, user_id) DO UPDATE SET
                    prompted_at_utc=excluded.prompted_at_utc
                """,
                (
                    guild_id,
                    user_id,
                    now_utc.isoformat(),
                ),
            )


async def send_transcription_consent_dm(guild: discord.Guild, member: discord.Member):
    dm_channel = member.dm_channel or await member.create_dm()
    await dm_channel.send(
        f"🎙️ Live transcription is active in **{guild.name}**. "
        f"React with {TRANSCRIBE_CONSENT_EMOJI} in the consent thread message to opt in. "
        "To change your display name, run `/gsetname <display name>` in the server."
    )
    logger.info("transcribe_consent_dm_sent guild_id=%s user_id=%s", guild.id, member.id)


def find_active_transcription_thread(guild: discord.Guild, session: GuildTranscriptionSession) -> discord.Thread | None:
    thread = guild.get_thread(session.transcript_thread_id)
    if thread is not None:
        return thread
    fetched = guild.get_channel(session.transcript_thread_id)
    if isinstance(fetched, discord.Thread):
        return fetched
    return None


async def prompt_transcription_consent(
    guild: discord.Guild,
    session: GuildTranscriptionSession,
    transcript_thread: discord.Thread,
    members: list[discord.Member],
):
    non_consented: list[discord.Member] = []
    for member in members:
        if member.id in session.consented_user_ids:
            logger.info(
                "transcribe_consent_prompt_skip guild_id=%s thread_id=%s user_id=%s prompt_reason=already_consented",
                guild.id,
                transcript_thread.id,
                member.id,
            )
            continue
        if member.id in session.prompted_user_ids:
            logger.info(
                "transcribe_consent_prompt_skip guild_id=%s thread_id=%s user_id=%s prompt_reason=session_already_prompted",
                guild.id,
                transcript_thread.id,
                member.id,
            )
            continue
        non_consented.append(member)
    if not non_consented:
        logger.info(
            "transcribe_consent_prompt_none guild_id=%s thread_id=%s prompt_reason=session_no_eligible_members",
            guild.id,
            transcript_thread.id,
        )
        return
    mentions = " ".join(member.mention for member in non_consented)
    consent_message = await transcript_thread.send(
        (
            f"{mentions}\n"
            f"React with {TRANSCRIBE_CONSENT_EMOJI} to opt into transcription for this session. "
            "Only consented users will be transcribed."
        )
    )
    await consent_message.add_reaction(TRANSCRIBE_CONSENT_EMOJI)
    transcription_consent_prompts[consent_message.id] = (guild.id, session.transcript_thread_id)
    for member in non_consented:
        session.prompted_user_ids.add(member.id)
        logger.info(
            "transcribe_consent_prompt_thread_sent guild_id=%s thread_id=%s user_id=%s prompt_reason=session_new",
            guild.id,
            transcript_thread.id,
            member.id,
        )
        await mark_transcription_consent_prompt_sent(guild.id, member.id)


async def sync_voice_channel_members_for_transcription(
    guild: discord.Guild,
    voice_channel: discord.VoiceChannel,
    session: GuildTranscriptionSession,
    transcript_thread: discord.Thread,
):
    members: list[discord.Member] = []
    logger.info(
        "transcribe_sync_members_start guild_id=%s voice_channel_id=%s channel_member_count=%s",
        guild.id,
        voice_channel.id,
        len(getattr(voice_channel, "members", []) or []),
    )
    for member in voice_channel.members:
        if member.bot:
            continue
        members.append(member)
        try:
            await transcript_thread.add_user(member)
            logger.info("transcribe_thread_add_user_ok guild_id=%s thread_id=%s user_id=%s", guild.id, transcript_thread.id, member.id)
        except (discord.Forbidden, discord.HTTPException):
            logger.exception("transcribe_thread_add_user_failed guild_id=%s thread_id=%s user_id=%s", guild.id, transcript_thread.id, member.id)
        display_name, _, _ = get_active_transcription_consent(guild.id, member.id)
        if display_name:
            session.consented_user_ids.add(member.id)
            session.aliases_by_user[member.id] = display_name
            logger.info("transcribe_member_has_active_consent guild_id=%s user_id=%s display_name=%r", guild.id, member.id, display_name)
            continue
        if member.id in session.prompted_user_ids:
            logger.info(
                "transcribe_consent_dm_skip guild_id=%s user_id=%s prompt_reason=session_already_prompted",
                guild.id,
                member.id,
            )
            continue
        try:
            await send_transcription_consent_dm(guild, member)
            session.prompted_user_ids.add(member.id)
            logger.info(
                "transcribe_consent_dm_sent guild_id=%s user_id=%s prompt_reason=session_new",
                guild.id,
                member.id,
            )
            await mark_transcription_consent_prompt_sent(guild.id, member.id)
        except (discord.Forbidden, discord.HTTPException):
            logger.exception("transcribe_consent_dm_failed guild_id=%s user_id=%s prompt_reason=send_failed", guild.id, member.id)
    logger.info(
        "transcribe_sync_members_done guild_id=%s discovered_members=%s consented_members=%s",
        guild.id,
        [member.id for member in members],
        sorted(session.consented_user_ids),
    )
    await prompt_transcription_consent(guild, session, transcript_thread, members)


def slice_timestamp_label(started_at: datetime, seconds_offset: float) -> str:
    ts = started_at + timedelta(seconds=max(seconds_offset, 0.0))
    return ts.astimezone(LOCAL_TZ).strftime("%H:%M:%S")


def copy_recorded_audio_slice(sink: object, session: GuildTranscriptionSession) -> dict[int, Path]:
    copied_files: dict[int, Path] = {}
    sink_audio_data = getattr(sink, "audio_data", None)
    if not isinstance(sink_audio_data, dict):
        return copied_files
    session.slice_number += 1
    slice_dir = session.temp_dir / f"slice_{session.slice_number:05d}"
    slice_dir.mkdir(parents=True, exist_ok=True)
    for user_id, audio_obj in sink_audio_data.items():
        if not isinstance(user_id, int) or user_id not in session.consented_user_ids:
            continue
        file_path = getattr(audio_obj, "file", None)
        if file_path is None:
            continue
        output_file = slice_dir / f"{user_id}.wav"
        try:
            if hasattr(file_path, "seek"):
                file_path.seek(0)
            if hasattr(file_path, "read"):
                output_file.write_bytes(file_path.read())
            else:
                shutil.copy(str(file_path), output_file)
        except OSError:
            continue
        copied_files[user_id] = output_file
    logger.info(
        "transcribe_slice_copied session_guild_id=%s slice=%s sink_users=%s consented_users=%s copied_users=%s",
        session.guild_id,
        session.slice_number,
        sorted([uid for uid in sink_audio_data.keys() if isinstance(uid, int)]),
        sorted(session.consented_user_ids),
        sorted(copied_files.keys()),
    )
    return copied_files


async def post_transcription_slice_lines(guild: discord.Guild, session: GuildTranscriptionSession, copied_files: dict[int, Path]):
    if not copied_files:
        logger.info("transcribe_slice_skip_empty guild_id=%s slice=%s", guild.id, session.slice_number)
        return
    engine_name, engine = get_whisper_transcriber()
    if engine is None or engine_name is None:
        logger.warning("transcribe_engine_unavailable guild_id=%s slice=%s", guild.id, session.slice_number)
        return
    thread = find_active_transcription_thread(guild, session)
    if thread is None:
        logger.warning("transcribe_thread_missing guild_id=%s thread_id=%s", guild.id, session.transcript_thread_id)
        return
    ordered_lines: list[tuple[float, str]] = []
    for user_id, audio_path in copied_files.items():
        speaker_name = resolve_display_name(guild, user_id, session.aliases_by_user)
        utterances = transcribe_audio_file(engine_name, engine, audio_path)
        for utterance in utterances:
            phrase = str(utterance.get("text") or "").strip()
            if not phrase:
                continue
            start_sec = float(utterance.get("start") or 0.0)
            stamp = slice_timestamp_label(session.started_at, ((session.slice_number - 1) * TRANSCRIBE_SLICE_SECONDS) + start_sec)
            ordered_lines.append((start_sec, f"[{stamp}] [{speaker_name}] {phrase}"))
    ordered_lines.sort(key=lambda item: item[0])
    if not ordered_lines:
        logger.info("transcribe_slice_no_utterances guild_id=%s slice=%s copied_users=%s", guild.id, session.slice_number, sorted(copied_files.keys()))
        return
    logger.info("transcribe_slice_posting guild_id=%s slice=%s lines=%s", guild.id, session.slice_number, len(ordered_lines))
    for _, line in ordered_lines:
        await thread.send(line)


async def finalize_recording_slice(vc: discord.VoiceClient, guild: discord.Guild, session: GuildTranscriptionSession):
    if not getattr(vc, "recording", False):
        logger.warning(
            "transcribe_finalize_skipped_not_recording guild_id=%s voice_connected=%s",
            guild.id,
            vc.is_connected() if vc is not None else None,
        )
        return
    logger.info(
        "transcribe_finalize_start guild_id=%s slice=%s voice_connected=%s",
        guild.id,
        session.slice_number,
        vc.is_connected(),
    )
    done_event = asyncio.Event()
    session.active_slice_done = done_event

    async def _slice_finished(sink: object, channel: object, *_: object):
        logger.info(
            "transcribe_finalize_callback_start guild_id=%s slice=%s sink_type=%s",
            guild.id,
            session.slice_number,
            type(sink).__name__,
        )
        copied = copy_recorded_audio_slice(sink, session)
        try:
            await asyncio.wait_for(post_transcription_slice_lines(guild, session, copied), timeout=90)
        except Exception:
            logger.exception("transcribe_slice_post_failed guild_id=%s", guild.id)
        logger.info(
            "transcribe_finalize_callback_done guild_id=%s slice=%s copied_users=%s",
            guild.id,
            session.slice_number,
            sorted(copied.keys()),
        )
        done_event.set()

    try:
        vc.stop_recording()
        logger.info("transcribe_finalize_stop_recording_called guild_id=%s slice=%s", guild.id, session.slice_number)
    except Exception:
        logger.exception("transcribe_slice_stop_failed guild_id=%s", guild.id)
        done_event.set()
    await asyncio.wait_for(done_event.wait(), timeout=120)
    logger.info("transcribe_finalize_done_event_received guild_id=%s slice=%s", guild.id, session.slice_number)
    if session.closed:
        logger.info("transcribe_finalize_exit_session_closed guild_id=%s slice=%s", guild.id, session.slice_number)
        return
    try:
        new_sink = discord.sinks.WaveSink()
        session.active_sink = new_sink
        vc.start_recording(new_sink, _slice_finished, None)
        session.recording_failure_count = 0
        logger.info("transcribe_finalize_restart_recording_ok guild_id=%s next_slice=%s", guild.id, session.slice_number + 1)
    except Exception:
        session.recording_failure_count += 1
        logger.exception(
            "transcribe_slice_restart_failed guild_id=%s failure_count=%s",
            guild.id,
            session.recording_failure_count,
        )
        if session.recording_failure_count >= TRANSCRIBE_MAX_FAILURES:
            await teardown_transcription_session_for_recording_failure(guild, session, "restart_failed")


async def teardown_transcription_session_for_recording_failure(
    guild: discord.Guild | None,
    session: GuildTranscriptionSession,
    failure_reason: str,
):
    guild_id = session.guild_id
    session.closed = True
    if guild is not None:
        vc = guild.voice_client
        if vc is not None and vc.is_connected():
            try:
                await vc.disconnect(force=True)
            except (discord.HTTPException, discord.ClientException):
                logger.exception("transcribe_teardown_disconnect_failed guild_id=%s", guild_id)
        thread = find_active_transcription_thread(guild, session)
        if thread is not None:
            try:
                await thread.send("🛑 Transcription session ended due to recording failure. Please start `/gtranscribe` again.")
            except Exception:
                logger.exception("transcribe_teardown_thread_notify_failed guild_id=%s", guild_id)
    remove_transcription_session(guild_id)
    logger.error(
        "transcribe_recording_failure guild_id=%s failure_count=%s action=torn_down reason=%s",
        guild_id,
        session.recording_failure_count,
        failure_reason,
    )


async def transcription_live_loop(guild_id: int):
    logger.info("transcribe_live_loop_started guild_id=%s interval_seconds=%s", guild_id, TRANSCRIBE_SLICE_SECONDS)
    while True:
        await asyncio.sleep(TRANSCRIBE_SLICE_SECONDS)
        session = get_transcription_session(guild_id)
        if session is None or session.closed:
            logger.info("transcribe_live_loop_exit guild_id=%s reason=%s", guild_id, "session_missing" if session is None else "session_closed")
            return
        guild = bot.get_guild(guild_id)
        if guild is None:
            logger.warning("transcribe_live_loop_skip guild_id=%s reason=guild_not_found", guild_id)
            continue
        vc = guild.voice_client
        if vc is None or not vc.is_connected() or not getattr(vc, "recording", False):
            logger.warning(
                "transcribe_live_loop_skip guild_id=%s reason=voice_not_recording vc_present=%s vc_connected=%s vc_recording=%s",
                guild_id,
                vc is not None,
                vc.is_connected() if vc is not None else None,
                getattr(vc, "recording", False) if vc is not None else None,
            )
            continue
        try:
            logger.info("transcribe_live_loop_finalize guild_id=%s current_slice=%s", guild_id, session.slice_number)
            await finalize_recording_slice(vc, guild, session)
        except asyncio.TimeoutError:
            recovery_session = get_transcription_session(guild_id)
            if recovery_session is None or recovery_session.closed:
                logger.warning("transcribe_slice_timeout guild_id=%s failure_count=%s", guild_id, None)
                continue
            recovery_session.recording_failure_count += 1
            logger.warning(
                "transcribe_slice_timeout guild_id=%s failure_count=%s",
                guild_id,
                recovery_session.recording_failure_count,
            )
            recovery_guild = bot.get_guild(guild_id)
            if recovery_session.recording_failure_count >= TRANSCRIBE_MAX_FAILURES:
                await teardown_transcription_session_for_recording_failure(
                    recovery_guild,
                    recovery_session,
                    "finalize_timeout",
                )
                continue
            recovery_vc = recovery_guild.voice_client if recovery_guild is not None else None
            next_slice = (recovery_session.slice_number + 1) if recovery_session is not None else None
            try:
                should_recover = (
                    recovery_session is not None
                    and not recovery_session.closed
                    and recovery_vc is not None
                    and recovery_vc.is_connected()
                    and not getattr(recovery_vc, "recording", False)
                )
                logger.info(
                    "transcribe_timeout_recovery_check guild_id=%s next_slice=%s session_open=%s vc_present=%s vc_connected=%s vc_recording=%s",
                    guild_id,
                    next_slice,
                    recovery_session is not None and not recovery_session.closed,
                    recovery_vc is not None,
                    recovery_vc.is_connected() if recovery_vc is not None else None,
                    getattr(recovery_vc, "recording", False) if recovery_vc is not None else None,
                )
                if not should_recover:
                    logger.info("transcribe_timeout_recovery_skipped guild_id=%s next_slice=%s", guild_id, next_slice)
                    continue

                done_event = asyncio.Event()
                recovery_session.active_slice_done = done_event

                async def _slice_finished(sink: object, channel: object, *_: object):
                    logger.info(
                        "transcribe_timeout_recovery_callback_start guild_id=%s slice=%s sink_type=%s",
                        recovery_guild.id,
                        recovery_session.slice_number,
                        type(sink).__name__,
                    )
                    copied = copy_recorded_audio_slice(sink, recovery_session)
                    try:
                        await asyncio.wait_for(post_transcription_slice_lines(recovery_guild, recovery_session, copied), timeout=90)
                    except Exception:
                        logger.exception("transcribe_slice_post_failed guild_id=%s", recovery_guild.id)
                    logger.info(
                        "transcribe_timeout_recovery_callback_done guild_id=%s slice=%s copied_users=%s",
                        recovery_guild.id,
                        recovery_session.slice_number,
                        sorted(copied.keys()),
                    )
                    done_event.set()

                logger.info("transcribe_timeout_recovery_start guild_id=%s next_slice=%s", guild_id, next_slice)
                recovery_sink = discord.sinks.WaveSink()
                recovery_session.active_sink = recovery_sink
                recovery_vc.start_recording(recovery_sink, _slice_finished, None)
                recovery_session.recording_failure_count = 0
                logger.info("transcribe_timeout_recovery_success guild_id=%s next_slice=%s", guild_id, next_slice)
            except Exception:
                recovery_session.recording_failure_count += 1
                logger.exception(
                    "transcribe_timeout_recovery_failed guild_id=%s next_slice=%s failure_count=%s",
                    guild_id,
                    next_slice,
                    recovery_session.recording_failure_count,
                )
                if recovery_session.recording_failure_count >= TRANSCRIBE_MAX_FAILURES:
                    await teardown_transcription_session_for_recording_failure(
                        recovery_guild,
                        recovery_session,
                        "timeout_recovery_restart_failed",
                    )

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
    logger.info("music_timing step=%s phase=%s elapsed_s=%.2f details=%r", step, phase, elapsed, fields)
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


async def wait_for_voice_client_ready(vc: discord.VoiceClient, timeout_seconds: float = 120.0) -> bool:
    """Wait for the voice websocket handshake to be ready for recording/playback."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        connected = vc.is_connected()
        connected_event = getattr(vc, "_connected", None)
        connected_event_set = (
            connected_event is not None and hasattr(connected_event, "is_set") and connected_event.is_set()
        )

        ws = getattr(vc, "ws", None)
        ws_poll_ready = ws is not None and callable(getattr(ws, "poll_event", None))

        if connected and (ws_poll_ready or connected_event_set):
            return True

        if not connected:
            return False

        await asyncio.sleep(0.1)
    return False


def describe_voice_client_state(vc: discord.VoiceClient) -> str:
    connected_event = getattr(vc, "_connected", None)
    connected_event_set = (
        connected_event.is_set() if connected_event is not None and hasattr(connected_event, "is_set") else None
    )
    ws = getattr(vc, "ws", None)
    ws_ready = ws is not None and callable(getattr(ws, "poll_event", None))
    channel = getattr(vc, "channel", None)
    return (
        f"connected={vc.is_connected()} "
        f"event_set={connected_event_set} "
        f"ws={type(ws).__name__ if ws is not None else None} "
        f"ws_ready={ws_ready} "
        f"channel_id={getattr(channel, 'id', None)}"
    )

def describe_transcription_session_state(session: GuildTranscriptionSession | None) -> str:
    if session is None:
        return "session=none"
    return (
        f"temp_dir={session.temp_dir} "
        f"slice={session.slice_number} "
        f"consented={len(session.consented_user_ids)} "
        f"thread_id={session.transcript_thread_id}"
    )


def build_interaction_log_context(
    interaction: discord.Interaction,
    vc: discord.VoiceClient | None = None,
    session: GuildTranscriptionSession | None = None,
) -> dict[str, object]:
    context = interaction_log_context(interaction)
    context["voice_state"] = describe_voice_client_state(vc) if vc is not None else "voice_client=none"
    if session is not None:
        context["transcription_session"] = describe_transcription_session_state(session)
    return context



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
    music_context = {
        "guild_id": guild.id,
        "channel_id": getattr(getattr(voice_client, "channel", None), "id", None),
        "user_id": next_track.requested_by,
        "interaction": "music_playback",
        "voice_state": describe_voice_client_state(voice_client),
    }
    try:
        stream_url = await resolve_stream_url(next_track.source_url)
    except RuntimeError:
        logger.exception("music_stream_resolve_failed track=%s context=%r", next_track.title, music_context)
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
            logger.exception("music_playback_error context=%r", music_context, exc_info=play_error)
        fut = asyncio.run_coroutine_threadsafe(play_next_track(guild), bot.loop)
        try:
            fut.result()
        except Exception:
            logger.exception("music_next_track_start_failed context=%r", music_context)
    logger.info("music_play_start track=%s context=%r", next_track.title, music_context)
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
        conn.execute("""
        CREATE TABLE IF NOT EXISTS transcription_consent (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            display_name TEXT NOT NULL,
            consented_at_utc TEXT NOT NULL,
            expires_at_utc TEXT NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS transcription_consent_prompts_sent (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            prompted_at_utc TEXT NOT NULL,
            PRIMARY KEY (guild_id, user_id)
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
        logger.exception("wesroth_channel_fetch_failed")
        return None
    channel_id = _extract_channel_id_from_handle_page(html)
    if not channel_id:
        logger.warning("wesroth_channel_id_parse_failed")
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
        logger.exception("wesroth_feed_fetch_failed")
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
    logger.info(
        "cleanup_message_stored user=%s user_id=%s channel_id=%s created_at=%s text=%r",
        message.author,
        message.author.id,
        message.channel.id,
        created_at.isoformat(),
        message.content,
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
    if message.author.bot:
        await bot.process_commands(message)
        return
    # delete any non-bot message in the cleanup channel
    if message.channel.id == CLEANUP_CHANNEL_ID:
        try:
            await log_cleanup_message(message)
            await message.delete()
        except (discord.Forbidden, discord.HTTPException):
            pass
        # still allow commands processing elsewhere; this message is gone anyway
        return
    await bot.process_commands(message)
@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot or member.guild is None:
        return
    session = get_transcription_session(member.guild.id)
    if session is None or session.closed:
        return
    if after.channel is None or after.channel.id != session.voice_channel_id:
        return
    if before.channel is not None and before.channel.id == session.voice_channel_id:
        return
    thread = find_active_transcription_thread(member.guild, session)
    if thread is not None:
        try:
            await thread.add_user(member)
        except (discord.Forbidden, discord.HTTPException):
            pass
    display_name, _, _ = get_active_transcription_consent(member.guild.id, member.id)
    if display_name:
        session.consented_user_ids.add(member.id)
        session.aliases_by_user[member.id] = display_name
        logger.info(
            "transcribe_voice_join_skip_prompt guild_id=%s user_id=%s prompt_reason=already_consented",
            member.guild.id,
            member.id,
        )
        return
    if member.id in session.prompted_user_ids:
        logger.info(
            "transcribe_voice_join_skip_prompt guild_id=%s user_id=%s prompt_reason=session_already_prompted",
            member.guild.id,
            member.id,
        )
        return
    try:
        await send_transcription_consent_dm(member.guild, member)
        session.prompted_user_ids.add(member.id)
        logger.info(
            "transcribe_voice_join_dm_sent guild_id=%s user_id=%s prompt_reason=session_new",
            member.guild.id,
            member.id,
        )
        await mark_transcription_consent_prompt_sent(member.guild.id, member.id)
    except (discord.Forbidden, discord.HTTPException):
        logger.exception(
            "transcribe_voice_join_dm_failed guild_id=%s user_id=%s prompt_reason=send_failed",
            member.guild.id,
            member.id,
        )
    if thread is not None:
        await prompt_transcription_consent(member.guild, session, thread, [member])

# =========================
# REACTIONS
# =========================
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.user_id == bot.user.id:
        return
    if payload.guild_id is None:
        return
    emoji = str(payload.emoji)
    consent_prompt = transcription_consent_prompts.get(payload.message_id)
    if consent_prompt is not None and emoji == TRANSCRIBE_CONSENT_EMOJI:
        guild_id, _ = consent_prompt
        if guild_id != payload.guild_id:
            return
        guild = bot.get_guild(payload.guild_id)
        member = guild.get_member(payload.user_id) if guild is not None else None
        if member is None or member.bot:
            return
        clean_name = normalize_transcript_display_name(member.display_name)
        if clean_name:
            await upsert_transcription_consent(payload.guild_id, payload.user_id, clean_name)
        session = get_transcription_session(payload.guild_id)
        if session is not None and not session.closed:
            session.consented_user_ids.add(payload.user_id)
            if clean_name:
                session.aliases_by_user[payload.user_id] = clean_name
            thread = find_active_transcription_thread(guild, session)
            if thread is not None:
                try:
                    await thread.add_user(member)
                except (discord.Forbidden, discord.HTTPException):
                    pass
                await thread.send(f"✅ {member.mention} opted into transcription.")
        return
    active_message_id = gget(payload.guild_id, "active_message_id")
    if not active_message_id or str(payload.message_id) != active_message_id:
        return
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
    vc = interaction.guild.voice_client
    cmd_context = build_interaction_log_context(interaction, vc=vc)
    logger.info("music_command_start name=gplay context=%r", cmd_context)
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
        except discord.DiscordException:
            logger.exception("music_voice_connect_failed context=%r", build_interaction_log_context(interaction, vc=interaction.guild.voice_client))
            await interaction.followup.send("Could not join voice channel.", ephemeral=True)
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
    except RuntimeError:
        logger.exception("music_fetch_tracks_failed context=%r source=%r", build_interaction_log_context(interaction, vc=interaction.guild.voice_client), source)
        await interaction.followup.send("Could not fetch audio.", ephemeral=True)
        return
    for track in tracks:
        track.requested_by = interaction.user.id
    state = get_music_state(interaction.guild.id)
    async with state.lock:
        starting_queue_size = len(state.queue)
        state.queue.extend(tracks)
        first_queue_position = starting_queue_size + 1
    logger.info("music_tracks_queued count=%s first_position=%s context=%r", len(tracks), first_queue_position, build_interaction_log_context(interaction, vc=interaction.guild.voice_client))
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
    logger.info("music_command_start name=gqueue context=%r", build_interaction_log_context(interaction, vc=vc))
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
    logger.info("music_command_start name=gskip context=%r", build_interaction_log_context(interaction, vc=vc))
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
    logger.info("music_command_execute name=gskip context=%r", build_interaction_log_context(interaction, vc=vc))
    vc.stop()
    await interaction.response.send_message("⏭️ Skipped current track.", ephemeral=True)
@discord.guild_only()
@bot.slash_command(name="gtranscribe", description="Join your voice channel and start live transcription in a transcript thread.")
async def gtranscribe(interaction: discord.Interaction):
    logger.info("transcribe_command_start context=%r", build_interaction_log_context(interaction, vc=getattr(getattr(interaction, "guild", None), "voice_client", None)))
    if interaction.guild is None or interaction.channel is None:
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
    if get_transcription_session(interaction.guild.id) is not None:
        await interaction.response.send_message(
            "A transcription session is already active in this server. Use `/gendsession` first.",
            ephemeral=True,
        )
        return
    await interaction.response.defer(ephemeral=True)
    logger.info(
        "transcribe_start_intents guild_id=%s members=%s voice_states=%s",
        interaction.guild.id,
        bot.intents.members,
        bot.intents.voice_states,
    )

    start_label = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M")
    transcript_thread = await interaction.channel.create_thread(
        name=f"transcript-{start_label}",
        type=discord.ChannelType.private_thread,
    )
    await transcript_thread.send(
        (
            f"🎙️ **Transcription session has begun** for {voice_channel.mention}.\n"
            f"React with {TRANSCRIBE_CONSENT_EMOJI} on the consent prompt to opt in.\n"
            "To change your display name `/gsetname <display name>`."
        )
    )



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
        except (discord.ClientException, discord.HTTPException, asyncio.TimeoutError):
            logger.exception("transcribe_voice_connect_failed context=%r", build_interaction_log_context(interaction, vc=interaction.guild.voice_client))
            await interaction.followup.send(
                "I couldn't join your voice channel. Confirm I have **Connect/Speak** permissions and that dependencies are installed (`pip install -r requirements.txt`).",
                ephemeral=True,
            )
            return
    if vc is None:
        await interaction.followup.send("I couldn't initialize a voice client.", ephemeral=True)
        return
    ready = await wait_for_voice_client_ready(vc)
    if not ready:
        if connected_here:
            try:
                await vc.disconnect(force=True)
            except (discord.HTTPException, discord.ClientException):
                pass
        await interaction.followup.send(
            "I connected to voice, but Discord voice setup took too long. Please try `/gtranscribe` again.",
            ephemeral=True,
        )
        return
    if getattr(vc, "recording", False):
        await interaction.followup.send("I am already recording in this server.", ephemeral=True)
        return

    session = GuildTranscriptionSession(interaction.guild.id, voice_channel.id, transcript_thread.id)
    transcription_sessions[interaction.guild.id] = session
    await sync_voice_channel_members_for_transcription(interaction.guild, voice_channel, session, transcript_thread)
    logger.info(
        "transcribe_session_initialized guild_id=%s voice_channel_id=%s thread_id=%s consented_users=%s",
        interaction.guild.id,
        voice_channel.id,
        transcript_thread.id,
        sorted(session.consented_user_ids),
    )

    async def _slice_finished(sink: object, channel: object, *_: object):
        logger.info(
            "transcribe_initial_callback_start guild_id=%s slice=%s sink_type=%s",
            interaction.guild.id,
            session.slice_number,
            type(sink).__name__,
        )
        copied = copy_recorded_audio_slice(sink, session)
        try:
            await asyncio.wait_for(post_transcription_slice_lines(interaction.guild, session, copied), timeout=90)
        except Exception:
            logger.exception("transcribe_slice_post_failed guild_id=%s", interaction.guild.id)
        logger.info(
            "transcribe_initial_callback_done guild_id=%s slice=%s copied_users=%s",
            interaction.guild.id,
            session.slice_number,
            sorted(copied.keys()),
        )
        session.active_slice_done.set()

    try:
        sink = discord.sinks.WaveSink()
        session.active_sink = sink
        vc.start_recording(sink, _slice_finished, None)
        logger.info(
            "transcribe_start_recording_ok guild_id=%s voice_channel_id=%s thread_id=%s",
            interaction.guild.id,
            voice_channel.id,
            transcript_thread.id,
        )
    except Exception as exc:
        logger.exception("transcribe_start_recording_failed error_type=%s context=%r", type(exc).__name__, build_interaction_log_context(interaction, vc=vc, session=session))
        remove_transcription_session(interaction.guild.id)
        if connected_here:
            try:
                await vc.disconnect(force=True)
            except (discord.HTTPException, discord.ClientException):
                pass
        await interaction.followup.send(
            f"I joined voice but couldn't start recording: `{type(exc).__name__}`. Ensure dependencies are installed (`pip install -r requirements.txt`) and system Opus is available (`libopus0`/`libopus.so`).",
            ephemeral=True,
        )
        return

    session.loop_task = asyncio.create_task(transcription_live_loop(interaction.guild.id))
    await interaction.followup.send(
        f"🎙️ Live transcription started in {voice_channel.mention}. Updates will be posted in {transcript_thread.mention}.",
        ephemeral=True,
    )


@discord.guild_only()
@bot.slash_command(name="gsetname", description="Set your transcription display name")
async def gsetname(
    interaction: discord.Interaction,
    name: discord.Option(str, "Display name to use in transcripts"),
):
    if interaction.guild is None:
        await interaction.response.send_message("This command only works in a server.", ephemeral=True)
        return
    clean_name = normalize_transcript_display_name(name)
    if not clean_name:
        await interaction.response.send_message("Display name cannot be empty.", ephemeral=True)
        return
    await upsert_transcription_consent(interaction.guild.id, interaction.user.id, clean_name)
    session = get_transcription_session(interaction.guild.id)
    if session is not None:
        session.consented_user_ids.add(interaction.user.id)
        session.aliases_by_user[interaction.user.id] = clean_name
    await interaction.response.send_message(
        f"✅ Saved transcript display name **{clean_name}**.",
        ephemeral=True,
    )


@discord.guild_only()
@bot.slash_command(name="gendsession", description="Stop live transcription and disconnect from voice.")
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
    await interaction.response.defer(ephemeral=True)
    if vc is not None and vc.is_connected() and getattr(vc, "recording", False):
        try:
            session.closed = True
            await finalize_recording_slice(vc, interaction.guild, session)
        except asyncio.TimeoutError:
            logger.warning("transcribe_final_slice_timeout guild_id=%s", interaction.guild.id)
    if session.loop_task is not None:
        session.loop_task.cancel()
    if vc is not None and vc.is_connected():
        try:
            await vc.disconnect(force=True)
        except (discord.HTTPException, discord.ClientException):
            pass
    thread = interaction.guild.get_thread(session.transcript_thread_id)
    if thread is not None:
        await thread.send("🛑 Transcription session ended. Bot disconnected from voice.")
    remove_transcription_session(interaction.guild.id)
    await interaction.followup.send("✅ Transcription session ended and bot disconnected.", ephemeral=True)


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
        "- `/gtranscribe` — Start live transcription in a timestamped transcript thread.",
        "- `/gsetname <name>` — Set your transcript display name",
        "- `/gendsession` — Stop live transcription and disconnect.",
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
    register_loop_exception_handler(asyncio.get_running_loop())
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
    logger.info("bot_ready user=%s user_id=%s", bot.user, bot.user.id)
if not TOKEN or TOKEN == "PUT_TOKEN_HERE_FOR_TESTING":
    raise RuntimeError("Set DISCORD_TOKEN_POOPBOT env var or paste token into TOKEN.")
bot.run(TOKEN)
