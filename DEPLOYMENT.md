# GokiBot deployment notes

## Python dependencies
Install dependencies from `requirements.txt`:

```bash
python -m pip install -r requirements.txt
```

This bot is pinned to **Pycord** so Discord voice recording sinks are available:

- `py-cord==2.6.1` (provides `discord.sinks`)

Additional runtime dependencies used by this bot:

- `PyNaCl==1.6.1` (voice transport encryption support)
- `python-dotenv==1.2.1` (loads `.env` values)

## Required for music
Music playback and URL resolution commands depend on `yt-dlp` (installed from `requirements.txt`) and system `ffmpeg`.

## Required for transcription export
Transcript export uses a local Whisper backend. This project installs `faster-whisper` from `requirements.txt` (recommended backend).

## Raspberry Pi / Linux system packages
Voice/music features require FFmpeg and Opus on the host:

```bash
sudo apt update
sudo apt install -y ffmpeg libopus0
```

## Environment variables
At minimum, configure:

- `DISCORD_TOKEN`

Optional feature variables used by ticketing and alerts:

- `TICKET_DEV_USER_ID`
- `TICKET_ARCHIVE_CHANNEL_ID`
- `WESROTH_CHANNEL_ID`

## Discord bot permissions (for all current features)
When inviting the bot, grant the following **OAuth2 bot permissions** so every command works as implemented today:

- `View Channels`
- `Read Message History`
- `Send Messages`
- `Send Messages in Threads`
- `Create Private Threads`
- `Manage Threads`
- `Add Reactions`
- `Manage Messages`
- `Connect`
- `Speak`

### Why these are needed
- Poop logging + consent flows use reactions, reaction removal, and message cleanup.
- Ticketing + transcription create private threads, add collaborators, and post in thread channels.
- Ticket archive export reads prior thread history.
- Music playback and live transcription require voice connection and speaking permissions.

## Discord privileged intent configuration
Enable **Message Content Intent** in the Discord Developer Portal for this bot. The runtime explicitly sets `intents.message_content = True`.

## Maintenance note
If new bot features require additional Discord permissions or intents, update this document at the same time as the feature change.
