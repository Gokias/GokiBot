# Daily quotes and facts

The poop logging message now ends with a short bowel-themed quote or fact, replacing the missed-log reminder. It uses the existing `OPENAI_API_KEY` and `OPENAI_MODEL` settings and OpenAI client. No new environment variables, Python packages, or system binaries are required.

One entry is saved per Pacific calendar day in the `daily_bowel_content` table in `db/poopbot_config.db`. The table is created automatically on startup. All servers share that day's entry, including after restarts, ten-poop message rotations, and `/debugpoop`. Existing active messages are updated on startup.

The generator receives the most recent 90 entries and is asked to avoid their ideas and wording. Every candidate is also checked against the full saved history for exact matches (ignoring case, punctuation, and spacing) and close textual matches. Rejected candidates are retried up to three times. Keep the config database to preserve this history.

Generation has a 25-second total timeout. On API errors, missing credentials, or unusable results, the bot saves an unused backup quote for that day. Backup quotes also go through the history check. If all backups have been used, that day's message omits the quote/fact rather than repeating one; normal poop logging continues, and generation is attempted again the next day.

To update the Raspberry Pi, pull the pushed changes from GitHub and restart the existing bot service. No dependency or virtual environment changes are needed for this feature.

Verification:

```sh
python -B -m unittest discover -s tests
```

Tests use temporary databases and mocked AI responses; they do not contact Discord or OpenAI.
