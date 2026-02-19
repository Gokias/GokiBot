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

## Raspberry Pi / Linux system packages
Voice/music features require FFmpeg on the host:

```bash
sudo apt update
sudo apt install -y ffmpeg
```

## Environment variables
At minimum, configure:

- `DISCORD_TOKEN`

Optional feature variables used by ticketing and alerts:

- `TICKET_DEV_USER_ID`
- `TICKET_ARCHIVE_CHANNEL_ID`
- `WESROTH_CHANNEL_ID`
