from dotenv import load_dotenv
import os
import math
import uuid
import random
import sqlite3
import asyncio
from datetime import datetime, timezone, date, time as dtime

import discord
from discord.ext import commands, tasks

try:
    from zoneinfo import ZoneInfo
    from zoneinfo import ZoneInfoNotFoundError
except ImportError:
    raise RuntimeError("Python 3.9+ required for zoneinfo")

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
intents.message_content = True  # needed for prefix commands like !poopstats, !setpoopchannel

bot = commands.Bot(command_prefix="!", intents=intents)

# serialize DB writes to avoid sqlite "database is locked"
db_write_lock = asyncio.Lock()


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
        f"React {UNDO_EMOJI} to undo your most recent log."
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
# COMMANDS
# =========================
@bot.command()
@commands.has_permissions(administrator=True)
async def setpoopchannel(ctx):
    if ctx.guild is None:
        return
    set_guild_channel(ctx.guild.id, ctx.channel.id)
    await ctx.send(f"✅ Poop channel set to {ctx.channel.mention} for this server.")


@bot.command()
@commands.has_permissions(administrator=True)
async def disablepoop(ctx):
    if ctx.guild is None:
        return
    disable_guild(ctx.guild.id)
    await ctx.send("🛑 Poop posting disabled for this server.")


@bot.command()
@commands.has_permissions(administrator=True)
async def debugpoop(ctx):
    """Force-create a new poop button message in this guild's configured channel."""
    if ctx.guild is None:
        return
    cfg = get_guild_config(ctx.guild.id)
    if not cfg:
        await ctx.send("Run !setpoopchannel in the channel you want first.")
        return
    await post_button_for_guild(ctx.guild.id, int(cfg["channel_id"]))
    await ctx.send("🧪 Debug: recreated poop button.")


@bot.command()
async def poopstats(ctx):
    user_id = ctx.author.id
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

    await ctx.send(
        f"**{ctx.author.mention} — {year} Poop Stats**\n"
        f"- Total poops: **{total}**\n"
        f"- Avg poops/day ({year}, since first logged): **{avg_per_day:.3f}**\n"
        f"- Avg local time: **{mean_time_str} Pacific**\n"
        f"- Latest poop: **{latest_str}**\n"
        f"- Most poops in one day: **{max_day_str}**"
    )


@bot.command()
async def featurerequest(ctx):
    if ctx.guild is None:
        await ctx.send("Feature requests can only be created in a server.")
        return

    dev_user_id = get_ticket_dev_user_id()
    dev_member = ctx.guild.get_member(dev_user_id) if dev_user_id else None

    try:
        await ctx.message.delete()
    except (discord.Forbidden, discord.HTTPException):
        pass

    ticket_id = await create_ticket_request(
        guild_id=ctx.guild.id,
        requester_id=ctx.author.id,
        requester_name=str(ctx.author)
    )
    thread_name = f"ticket-{ticket_id}-{ctx.author.name}".lower().replace(" ", "-")
    ticket_target = await ctx.channel.create_thread(
        name=thread_name,
        type=discord.ChannelType.private_thread
    )
    await ticket_target.add_user(ctx.author)
    if dev_member:
        await ticket_target.add_user(dev_member)

    await update_ticket_request(ticket_id, ctx.channel.id, ticket_target.id)
    set_ticket_target(ctx.guild.id, ctx.author.id, ticket_target.id)

    dev_mention = dev_member.mention if dev_member else ""
    mention_line = " ".join(part for part in [ctx.author.mention, dev_mention] if part)
    prompt_lines = [
        f"{mention_line} **(Ticket #{ticket_id})**",
        "**Feature request intake**",
        "- **What is the feature?**",
        "- **How do you want to use it?**",
        "- **Give an example of how it is triggered, what happens, etc.**"
    ]
    await ticket_target.send("\n".join(prompt_lines))


@bot.command()
async def closeticket(ctx):
    if ctx.guild is None:
        return

    dev_user_id = get_ticket_dev_user_id()
    if dev_user_id is None or ctx.author.id != dev_user_id:
        return

    ticket = get_ticket_by_thread_id(ctx.channel.id)
    if not ticket:
        await ctx.send("No ticket is associated with this channel.")
        return
    if ticket["status"] != "open":
        await ctx.send(f"Ticket #{ticket['ticket_id']} is already closed.")
        return

    archive_channel_id = get_ticket_archive_channel_id()
    if archive_channel_id is None:
        await ctx.send("Archive channel is not configured.")
        return

    await ctx.send("🔒 This ticket has been closed and will be archived and deleted in 24h.")

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
    async for message in ctx.channel.history(oldest_first=True, limit=None):
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


# =========================
# STARTUP
# =========================
@bot.event
async def on_ready():
    init_config_db()
    init_year_db(current_year_local())
    init_cleanup_db()

    if not daily_midnight_pacific.is_running():
        daily_midnight_pacific.start()

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
