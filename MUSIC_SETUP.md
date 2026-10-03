# Music playback setup

After updating PoopBot, activate its existing Python virtual environment and run:

```sh
python -m pip install --upgrade -r requirements.txt
```

On the Raspberry Pi, music needs:

- FFmpeg with the `libopus` encoder. Raspberry Pi OS's FFmpeg package provides this: `sudo apt-get install ffmpeg`.
- A supported JavaScript runtime on the bot service's `PATH`: current Deno (recommended) or Node.js 22 or newer. PoopBot enables both runtimes; only one needs to be installed.

YouTube extraction now needs JavaScript challenge solving. The `yt-dlp[default]` requirement installs the matching solver package. See the [official yt-dlp setup guide](https://github.com/yt-dlp/yt-dlp/wiki/EJS) and [Deno installation instructions](https://docs.deno.com/runtime/getting_started/installation/).

Check the binaries as the same account that runs the bot:

```sh
ffmpeg -version
deno --version
# If using Node instead:
node --version
```

For a systemd service, installing Deno into your interactive shell's PATH may not make it available to the service. Ensure its executable directory is included in the service's PATH, then restart the existing bot service. No new `.env` entries are required.

Try `/gplay` with a single song, then `/gskip` and a playlist. The bot should play audio, advance the queue, and disconnect when the queue and playlist loading are finished. Extraction warnings and playback errors appear in the bot logs.

Local verification (no Discord token or connection needed):

```sh
python -B -m unittest discover -s tests
```

The audio integration tests require FFmpeg; they exercise AAC conversion, Opus packet timing, and failed HTTP streams using a local server.
