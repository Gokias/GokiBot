import os
import math
import uuid
import random
import sqlite3
from datetime import datetime, timezone, date, time as dtime

import discord
from discord.ext import commands, tasks

try:
    from zoneinfo import ZoneInfo
except ImportError:
    raise RuntimeError("Python 3.9+ required for zoneinfo")



# ---------- CONFIG ----------
TOKEN = os.getenv("DISCORD_TOKEN_POOPBOT") 
POOP_EMOJI = "💩"
UNDO_EMOJI = "🧻"
DB_DIR = "db"  # folder to store yearly db files
LOCAL_TZ = ZoneInfo("America/Los_Angeles")  # Pacific time
# --------------------------

if not TOKEN or TOKEN == "PUT_TOKEN_HERE_FOR_TESTING":
    raise RuntimeError("Set DISCORD_TOKEN env var or paste token into TOKEN.")

# 60 congratulatory / poop-punny messages
CONGRATS = [
    "Delivery confirmed, {user}.",
    "Another successful drop, {user}.",
    "System pressure normalized, {user}.",
    "Operational success, {user}.",
    "Flow restored. Nice work, {user}.",
    "Pipeline cleared efficiently, {user}.",
    "That went exactly as planned, {user}.",
    "Another smooth release, {user}.",
    "Containment breach resolved, {user}.",
    "Throughput exceeded expectations, {user}.",
    "Release window utilized perfectly, {user}.",
    "Payload offloaded successfully, {user}.",
    "Stability restored, {user}.",
    "System latency reduced to zero, {user}.",
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
    
    # First 30
    "Mission accomplished, {user}. 💩",
    "Another successful delivery, {user}.",
    "Nature has been served. Well done, {user}.",
    "A job well done, {user}. Flush with pride.",
    "Textbook execution, {user}.",
    "Certified drop-off complete, {user}.",
    "Smooth operation, {user}.",
    "Excellent form, {user}.",
    "Another one in the books, {user}.",
    "Poop status: logged. Nice work, {user}.",
    "Handled with precision, {user}.",
    "Clean break. Strong showing, {user}.",
    "That’s how it’s done, {user}.",
    "Release achieved. Congrats, {user}.",
    "Well timed and well placed, {user}.",
    "Efficiency at its finest, {user}.",
    "Successful payload delivery, {user}.",
    "A flawless run, {user}.",
    "Poop confirmed. Solid work, {user}.",
    "Gravity remains undefeated, {user}.",
    "Form, function, fulfillment — nice one, {user}.",
    "Another victory for biology, {user}.",
    "Operation Flushpoint completed, {user}.",
    "Clean exit. Professional work, {user}.",
    "Your body thanks you, {user}.",
    "Another log logged, {user}.",
    "Nature called. You answered, {user}.",
    "One small dump for man, one giant relief for {user}.",
    "Poop cycle complete. Well done, {user}.",
    "Balance restored. Nice work, {user}.",
    # Second 30
    "All systems go. Nice dump, {user}.",
    "Another successful evacuation, {user}.",
    "Release valve opened flawlessly, {user}.",
    "Throughput achieved. Well done, {user}.",
    "Nothing left behind. Strong work, {user}.",
    "Clean operation from start to finish, {user}.",
    "Payload delivered on schedule, {user}.",
    "Stress relieved. Mission complete, {user}.",
    "Peak performance achieved, {user}.",
    "System integrity maintained. Nice one, {user}.",
    "Flow state reached, {user}.",
    "Successful offload confirmed, {user}.",
    "Poop pipeline functioning optimally, {user}.",
    "Another blockage cleared, {user}.",
    "Excellent timing, excellent release, {user}.",
    "Process completed without incident, {user}.",
    "That’s one for the records, {user}.",
    "Optimal evacuation confirmed, {user}.",
    "Well executed exit strategy, {user}.",
    "Biomechanics win again. Congrats, {user}.",
    "Another pressure cycle resolved, {user}.",
    "All clear. Nice work, {user}.",
    "Successful transfer to porcelain, {user}.",
    "Relief level: maximum. Good job, {user}.",
    "System reset complete, {user}.",
    "Another clean handoff, {user}.",
    "Your digestive tract salutes you, {user}.",
    "Another load successfully shed, {user}.",
    "Completion achieved. Stand proud, {user}.",
    "Poop event recorded. Excellent, {user}.",
    # ---------- Dota 2 ----------
    "Space created, {user}.",
    "Objective secured, {user}.",
    "Clean disengage, {user}.",
    "{user} traded 2 for 1. Worth.",
    "Nice toilet gank, {user}.",
    "Game sense on point, {user}.",
    "Nicely played around cooldowns, {user}.",
    "Map control restored, {user}.",

    # ---------- Arc Raiders ----------
    "Extraction successful, {user}.",
    "ARC avoided. Nice one, {user}.",
    "Good instincts out there, {user}.",

    # ---------- Factorio ----------
    "Factory efficiency increased, {user}.",
    "The system flows again, {user}.",
    "Bottleneck resolved, {user}.",
    "Throughput optimized, {user}.",
    "Ratios restored, {user}.",
    "Production never stops, {user}.",
    "Lines are moving again, {user}.",

    # ---------- Hearthstone Battlegrounds ----------
    "Highrolled with dignity, {user}.",
    "Perfect curve, {user}.",
    "That shop cooperated nicely, {user}.",
    "Strong tempo turn, {user}.",
    "Top four secured, {user}.",
    "Scaling achieved, {user}.",
    "Clean pivot, {user}.",

    # ---------- World of Warcraft ----------
    "Cooldowns well spent, {user}.",
    "Clean pull, no deaths, {user}.",
    "Threat managed perfectly, {user}.",
    "Parse secured, {user}.",

    # ---------- Path of Exile ----------
    "Another atlas objective done, {user}.",
    "Clean clear speed, {user}.",

    # ---------- Dwarf Fortress ----------
    "The fortress endures, {user}.",
    "No tantrum spiral today, {user}.",
    "History will remember this moment, {user}.",
    "The mountainhome approves, {user}.",
    "Losses were acceptable, {user}.",

    # ---------- Minecraft ----------

    # ---------- Final Fantasy XIV ----------
    "No vuln stacks — impressive, {user}.",
    "Limit break well timed, {user}.",
    "Duty complete, {user}.",
    "Echo not required, {user}.",

    # ---------- Diablo ----------
    "Rift cleared efficiently, {user}.",

    # ---------- Old School RuneScape ----------
    "Tick-perfect execution, {user}.",
    "That grind paid off, {user}.",
    "Another KC secured, {user}.",
    "That was clean pathing, {user}.",
    "XP waste minimized, {user}.",

    # ---------- General gamer crossover ----------
    "That was the correct play, {user}.",
    "Muscle memory carried you, {user}.",
    "Decision making on point, {user}.",
    "Clean fundamentals, {user}.",
    "That felt optimal, {user}.",
    "Consistency pays off, {user}.",

]

UNDO_MSGS = [
    "Okay {user}, I removed your last poop.",
    "Rollback complete, {user}.",
    "Deleted one (1) poop from the timeline, {user}.",
    "Wiped from history, {user}.",
]

# ---------- DISCORD INTENTS ----------
# NOTE: message_content=True is a privileged intent.
# Enable it in the Developer Portal if you want prefix commands like !poopstats / !debugpoop.
intents = discord.Intents.default()
intents.reactions = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

##Amish Channel cleaner##
CLEANUP_CHANNEL_ID = 1419130398683959398
@bot.event
async def on_message(message: discord.Message):
    # Ignore bot messages (including itself)
    if message.author.bot:
        await bot.process_commands(message)
        return

    # Only enforce in the cleanup channel
    if message.channel.id == CLEANUP_CHANNEL_ID:
        try:
            await message.delete()
        except discord.Forbidden:
            pass
        except discord.HTTPException:
            pass
        return

    # Make sure commands still work
    await bot.process_commands(message)


# ---------- DB ----------
CONFIG_DB_PATH = os.path.join(DB_DIR, "poopbot_config.db")

def db_config():
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(CONFIG_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def current_year_local() -> int:
    return datetime.now(LOCAL_TZ).year

def db_path_for_year(year: int) -> str:
    os.makedirs(DB_DIR, exist_ok=True)
    return os.path.join(DB_DIR, f"poopbot_{year}.db")


def db(year: int | None = None):
    if year is None:
        year = current_year_local()
    path = db_path_for_year(year)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(year: int | None = None):
    with db_config(year) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            timestamp_utc TEXT NOT NULL,
            timestamp_local TEXT NOT NULL,
            date_local TEXT NOT NULL,
            time_local TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            guild_id INTEGER,
            channel_id INTEGER,
            message_id INTEGER,
            target_event_id TEXT,
            note TEXT
        );
        """)
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

        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_user_time ON events(user_id, timestamp_utc);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """)

def get_enabled_guilds():
    with db_config() as conn:
        return conn.execute("""
            SELECT guild_id, channel_id, timezone
            FROM guild_config
            WHERE enabled=1
        """).fetchall()

def get_guild_config(guild_id: int):
    with db_config() as conn:
        return conn.execute(
            "SELECT guild_id, channel_id, timezone FROM guild_config WHERE enabled=1 AND guild_id=?",
            (guild_id,)
        ).fetchone()
def set_guild_channel(guild_id: int, channel_id: int):
    with db_config() as conn:
        conn.execute("""
            INSERT INTO guild_config(guild_id, channel_id, enabled)
            VALUES(?, ?, 1)
            ON CONFLICT(guild_id) DO UPDATE SET channel_id=excluded.channel_id, enabled=1
        """, (guild_id, channel_id))

@bot.command()
@commands.has_permissions(administrator=True)
async def setpoopchannel(ctx):
    set_guild_channel(ctx.guild.id, ctx.channel.id)
    await ctx.send(f"✅ Poop channel set to {ctx.channel.mention} for this server.")



def set_state(key: str, value: str):
    with db_config() as conn:
        conn.execute("""
            INSERT INTO state(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """, (key, value))

def get_state(key: str):
    with db_config() as conn:
        row = conn.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None


def get_int_state(key: str, default: int = 0) -> int:
    v = get_state(key)
    try:
        return int(v) if v is not None else default
    except ValueError:
        return default

def set_int_state(key: str, value: int):
    set_state(key, str(value))

def now_utc_local():
    utc = datetime.now(timezone.utc)
    local = utc.astimezone(LOCAL_TZ)
    return utc, local

def log_event(
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
    event_id = str(uuid.uuid4())
    event_year = local.year
    init_db(event_year)  # ensures year db exists even right after midnight rollover

    with db_config(event_year) as conn:
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

def find_last_active_poop_event_id(user_id: int) -> str | None:
    """Most recent POOP event_id by user that has NOT been undone (no date filtering)."""
    with db_config() as conn:
        poops = conn.execute("""
            SELECT event_id
            FROM events
            WHERE event_type='POOP' AND user_id=?
            ORDER BY timestamp_utc DESC
            LIMIT 200
        """, (user_id,)).fetchall()

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
def gset(guild_id: int, key: str, value: str):
    with db_config() as conn:
        conn.execute("""
        INSERT INTO guild_state(guild_id, key, value)
        VALUES (?, ?, ?)
        ON CONFLICT(guild_id, key)
        DO UPDATE SET value=excluded.value
        """, (guild_id, key, value))

def gget(guild_id: int, key: str):
    with db_config() as conn:
        row = conn.execute("""
        SELECT value FROM guild_state
        WHERE guild_id=? AND key=?
        """, (guild_id, key)).fetchone()
        return row["value"] if row else None

def gset_int(guild_id: int, key: str, value: int):
    gset(guild_id, key, str(value))

def gget_int(guild_id: int, key: str, default: int = 0) -> int:
    v = gget(guild_id, key)
    try:
        return int(v) if v is not None else default
    except ValueError:
        return default


# ---------- DAILY POST ----------
async def post_button_for_guild(guild_id: int, channel_id: int):
    channel = await bot.fetch_channel(channel_id)

    # 🔽 delete previous button message if it exists
    old_message_id = gget(guild_id, "active_message_id")
    if old_message_id:
        try:
            old_msg = await channel.fetch_message(int(old_message_id))
            await old_msg.delete()
        except (discord.NotFound, discord.Forbidden):
            pass  # already gone or no perms
        except discord.HTTPException:
            pass

    local_now = datetime.now(LOCAL_TZ)

    msg = await channel.send(
        f"💩 **Poop Button** — {local_now.strftime('%Y-%m-%d')} (Pacific)\n"
        f"React {POOP_EMOJI} to log a poop.\n"
        f"React {UNDO_EMOJI} to undo your most recent logged poop."
    )

    await msg.add_reaction(POOP_EMOJI)
    await msg.add_reaction(UNDO_EMOJI)

    gset(guild_id, "active_message_id", str(msg.id))
    gset(guild_id, "active_date_local", local_now.date().isoformat())
    gset_int(guild_id, "poops_since_post", 0)


@tasks.loop(time=dtime(hour=0, minute=0, tzinfo=LOCAL_TZ))  # 12:00am Pacific
async def daily_midnight_pacific():
    for row in get_enabled_guilds():
        guild_id = int(row["guild_id"])
        channel_id = int(row["channel_id"])
        try:
            await post_button_for_guild(guild_id, channel_id)
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            # Forbidden: missing perms
            # NotFound: channel deleted / bot removed
            # HTTPException: transient API failure
            continue

@bot.event
async def on_ready():
    init_db(current_year_local())


    if not daily_midnight_pacific.is_running():
        daily_midnight_pacific.start()

    # Post immediately for each configured guild if today's button wasn't posted
    today_local = datetime.now(LOCAL_TZ).date().isoformat()
    for row in get_enabled_guilds():
        gid = int(row["guild_id"])
        last_date = gget(gid, "active_date_local")
        if last_date != today_local:
            await post_button_for_guild(gid, int(row["channel_id"]))


    print(f"Logged in as {bot.user} (id={bot.user.id})")

# ---------- DEBUG COMMAND ----------
@bot.command()
@commands.has_permissions(administrator=True)
async def debugpoop(ctx):
    cfg = get_guild_config(ctx.guild.id)
    if not cfg:
        await ctx.send("Run !setpoopchannel in the channel you want first.")
        return
    await post_button_for_guild(ctx.guild.id, int(cfg["channel_id"]))
    await ctx.send("🧪 Debug: recreated poop button.")


# ---------- REACTIONS ----------
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.user_id == bot.user.id:
        return

    if payload.guild_id is None:
        return
    active_message_id = gget(payload.guild_id, "active_message_id")

    if not active_message_id:
        return

    # Only respond on the current active button message
    if str(payload.message_id) != active_message_id:
        return

    emoji = str(payload.emoji)

    channel = await bot.fetch_channel(payload.channel_id)
    message = await channel.fetch_message(payload.message_id)
    user = await bot.fetch_user(payload.user_id)
    mention = f"<@{payload.user_id}>"

    if emoji == POOP_EMOJI:
        log_event(
            event_type="POOP",
            user_id=payload.user_id,
            username=str(user),
            guild_id=payload.guild_id,
            channel_id=payload.channel_id,
            message_id=payload.message_id
        )

        await channel.send(random.choice(CONGRATS).format(user=mention))

        # Remove their reaction so they can click again (needs Manage Messages)
        try:
            await message.remove_reaction(payload.emoji, user)
        except discord.Forbidden:
            pass

        # Rotate button message every 10 logged poops (per guild)
        count = gget_int(payload.guild_id, "poops_since_post", 0) + 1
        gset_int(payload.guild_id, "poops_since_post", count)

        if count >= 10:
            cfg = get_guild_config(payload.guild_id)
            if cfg:
                await post_button_for_guild(payload.guild_id, int(cfg["channel_id"]))

        return

    if emoji == UNDO_EMOJI:
        target = find_last_active_poop_event_id(payload.user_id)
        if not target:
            await channel.send(f"{mention} you don’t have a poop to undo.")
        else:
            log_event(
                event_type="UNDO",
                user_id=payload.user_id,
                username=str(user),
                guild_id=payload.guild_id,
                channel_id=payload.channel_id,
                message_id=payload.message_id,
                target_event_id=target
            )
            await channel.send(random.choice(UNDO_MSGS).format(user=mention))

        # Remove their reaction so they can click again (needs Manage Messages)
        try:
            await message.remove_reaction(payload.emoji, user)
        except discord.Forbidden:
            pass

        return

# ---------- STATS ----------
def circular_mean_time(minutes_list: list[float]) -> float | None:
    """minutes since midnight -> circular mean minutes since midnight"""
    if not minutes_list:
        return None
    angles = [m * 2 * math.pi / 1440.0 for m in minutes_list]
    s = sum(math.sin(a) for a in angles)
    c = sum(math.cos(a) for a in angles)
    mean_angle = math.atan2(s, c)
    if mean_angle < 0:
        mean_angle += 2 * math.pi
    mean_minutes = mean_angle * 1440.0 / (2 * math.pi)
    return mean_minutes

def fmt_minutes_as_time(mins: float) -> str:
    mins = mins % 1440.0
    h = int(mins // 60)
    m = int(mins % 60)
    return f"{h:02d}:{m:02d}"

def get_user_year_stats(user_id: int, year: int):
    start_local = datetime(year, 1, 1, 0, 0, 0, tzinfo=LOCAL_TZ)
    end_local = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=LOCAL_TZ)
    init_db(year)
    with db_config(year) as conn:

        poops = conn.execute("""
            SELECT event_id, time_local, date_local
            FROM events
            WHERE event_type='POOP'
              AND user_id=?
              AND timestamp_local >= ?
              AND timestamp_local < ?
        """, (user_id, start_local.isoformat(), end_local.isoformat())).fetchall()

        if not poops:
            return 0, [], 0

        poop_ids = [r["event_id"] for r in poops]

        undone = conn.execute(f"""
            SELECT target_event_id
            FROM events
            WHERE event_type='UNDO'
              AND user_id=?
              AND target_event_id IN ({",".join("?" * len(poop_ids))})
        """, (user_id, *poop_ids)).fetchall()

        undone_set = {r["target_event_id"] for r in undone}

        active_poop_times: list[float] = []
        active_count = 0
        first_active_date: date | None = None

        for r in poops:
            if r["event_id"] in undone_set:
                continue

            active_count += 1

            d = date.fromisoformat(r["date_local"])
            if first_active_date is None or d < first_active_date:
                first_active_date = d

            hh, mm, ss = r["time_local"].split(":")
            minutes = int(hh) * 60 + int(mm) + (int(ss) / 60.0)
            active_poop_times.append(minutes)

    if active_count == 0 or first_active_date is None:
        return 0, [], 0

    today_local = datetime.now(LOCAL_TZ).date()
    days_elapsed = (today_local - first_active_date).days + 1  # inclusive

    return active_count, active_poop_times, days_elapsed

def get_latest_poop(user_id: int, year: int) -> str | None:
    start_local = datetime(year, 1, 1, tzinfo=LOCAL_TZ)
    end_local = datetime(year + 1, 1, 1, tzinfo=LOCAL_TZ)

    init_db(year)
    with db_config(year) as conn:
        poops = conn.execute("""
            SELECT event_id, timestamp_local
            FROM events
            WHERE event_type='POOP'
              AND user_id=?
              AND timestamp_local >= ?
              AND timestamp_local < ?
            ORDER BY timestamp_local DESC
            LIMIT 100
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

        for r in poops:
            if r["event_id"] not in undone_set:
                return r["timestamp_local"]

    return None

def get_max_poops_in_one_day(user_id: int, year: int) -> tuple[int, str | None]:
    start_local = datetime(year, 1, 1, tzinfo=LOCAL_TZ)
    end_local = datetime(year + 1, 1, 1, tzinfo=LOCAL_TZ)

    init_db(year)
    with db_config(year) as conn:
        poops = conn.execute("""
            SELECT event_id, date_local
            FROM events
            WHERE event_type='POOP'
              AND user_id=?
              AND timestamp_local >= ?
              AND timestamp_local < ?
        """, (user_id, start_local.isoformat(), end_local.isoformat())).fetchall()

        if not poops:
            return 0, None

        poop_ids = [r["event_id"] for r in poops]

        undone = conn.execute(f"""
            SELECT target_event_id
            FROM events
            WHERE event_type='UNDO'
              AND user_id=?
              AND target_event_id IN ({",".join("?" * len(poop_ids))})
        """, (user_id, *poop_ids)).fetchall()

        undone_set = {r["target_event_id"] for r in undone}

        counts: dict[str, int] = {}
        for r in poops:
            if r["event_id"] in undone_set:
                continue
            counts[r["date_local"]] = counts.get(r["date_local"], 0) + 1

        if not counts:
            return 0, None

        best_date = max(counts, key=lambda d: counts[d])
        return counts[best_date], best_date

@bot.command()
async def poopstats(ctx):
    user_id = ctx.author.id
    now_local = datetime.now(LOCAL_TZ)
    year = now_local.year

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

    max_day_str = (
        f"{max_day_count} on {max_day_date}"
        if max_day_date else "N/A"
    )

    await ctx.send(
        f"**{ctx.author.mention} — {year} Poop Stats**\n"
        f"- Total poops: **{total}**\n"
        f"- Avg poops/day ({year}, since first logged): **{avg_per_day:.3f}**\n"
        f"- Avg local time: **{mean_time_str} Pacific**\n"
        f"- Latest poop: **{latest_str}**\n"
        f"- Most poops in one day: **{max_day_str}**"
    )




bot.run(TOKEN)
