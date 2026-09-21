import asyncio
import os
import tempfile
import types
import unittest
from datetime import datetime, timezone
from unittest import mock

import poopbot


class BackdatedTimeTests(unittest.TestCase):
    reference = datetime(2026, 9, 21, 18, 30, tzinfo=timezone.utc)

    def test_english_dates(self):
        cases = {
            "yesterday at noon": "2026-09-20T19:00:00+00:00",
            "earlier today at 6am": "2026-09-21T13:00:00+00:00",
            "two hours ago": "2026-09-21T16:30:00+00:00",
            "last Friday at 3pm": "2026-09-18T22:00:00+00:00",
            "on Friday at 3pm": "2026-09-18T22:00:00+00:00",
            "2025-12-31 at 11pm": "2026-01-01T07:00:00+00:00",
            "yesterday at midnight": "2026-09-20T07:00:00+00:00",
            "2026-03-08 at 10:30 UTC": "2026-03-08T10:30:00+00:00",
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual(poopbot.parse_backdated_poop_time(text, self.reference).isoformat(), expected)

    def test_unsafe_or_unclear_times_are_rejected(self):
        for text in (
            "", "yesterday", "yesterday at 3", "yesterday at 25:00",
            "yesterday at 3:80pm", "yesterday at 13pm", "tomorrow at noon",
            "today at 11pm", "yesterday at noon and 4pm", "someday at noon",
            "2026-03-08 at 2:30am", "2025-11-02 at 1:30am",
        ):
            with self.subTest(text=text), self.assertRaises(ValueError):
                poopbot.parse_backdated_poop_time(text, self.reference)

    def test_yesterday_uses_pacific_message_date(self):
        reference = datetime(2026, 1, 1, 3, tzinfo=timezone.utc)
        actual = poopbot.parse_backdated_poop_time("yesterday at noon", reference)
        self.assertEqual(actual.isoformat(), "2025-12-30T20:00:00+00:00")


class BackdatedMentionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        connections = []
        connect = poopbot.sqlite3.connect

        def track_connection(*args, **kwargs):
            connection = connect(*args, **kwargs)
            connections.append(connection)
            return connection

        patcher = mock.patch.object(poopbot.sqlite3, "connect", side_effect=track_connection)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(lambda: [connection.close() for connection in connections])
        for name, value in (
            ("DB_DIR", self.directory.name),
            ("CONFIG_DB_PATH", os.path.join(self.directory.name, "config.db")),
            ("db_write_lock", asyncio.Lock()),
        ):
            patcher = mock.patch.object(poopbot, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        poopbot.init_config_db()
        with poopbot.db_config() as conn:
            conn.execute("INSERT INTO guild_config(guild_id, channel_id) VALUES (1, 2)")
        self.message = types.SimpleNamespace(
            id=100, guild=types.SimpleNamespace(id=1),
            channel=types.SimpleNamespace(id=2, send=mock.AsyncMock()),
            author=types.SimpleNamespace(id=42, bot=False), webhook_id=None,
            created_at=datetime(2026, 1, 2, 18, tzinfo=timezone.utc),
            content="<@123> I pooped December 31 2025 at 11pm",
        )

    async def test_backdated_write_uses_occurrence_year_and_is_idempotent(self):
        for _ in range(2):
            self.assertTrue(await poopbot.handle_backdated_poop_mention(self.message, 123))
        with poopbot.db_year(2025) as conn:
            rows = conn.execute("SELECT * FROM events").fetchall()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["user_id"], 42)
        self.assertEqual(row["date_local"], "2025-12-31")
        self.assertEqual(row["time_local"], "23:00:00")
        self.assertEqual(row["timestamp_utc"], "2026-01-01T07:00:00+00:00")
        self.assertEqual(row["message_id"], 100)
        self.assertEqual(row["guild_id"], 1)
        self.assertEqual(row["event_type"], "POOP")
        self.assertFalse(os.path.exists(os.path.join(self.directory.name, "poopbot_2026.db")))
        self.assertIn("already logged", self.message.channel.send.call_args.args[0])
        self.assertEqual(len(poopbot._net_poop_rows_for_year(42, 2025)), 1)

    async def test_invalid_request_does_not_write(self):
        self.message.content = "<@123> I pooped tomorrow at noon"
        self.assertTrue(await poopbot.handle_backdated_poop_mention(self.message, 123))
        self.assertIn("Nothing logged", self.message.channel.send.call_args.args[0])
        self.assertFalse(os.path.exists(os.path.join(self.directory.name, "poopbot_2026.db")))

    async def test_channel_scope_and_normal_chat(self):
        for content in ("<@123> tell me a joke", "<@123> did I poop yesterday at noon?",
                        "<@123> Bob pooped yesterday at noon", "<@123> I didn't poop yesterday at noon"):
            self.message.content = content
            self.assertFalse(await poopbot.handle_backdated_poop_mention(self.message, 123))
        self.message.content = "<@123> I pooped yesterday at noon"
        self.message.channel.id = 3
        self.assertFalse(await poopbot.handle_backdated_poop_mention(self.message, 123))
        self.message.channel.id = 2
        with poopbot.db_config() as conn:
            conn.execute("UPDATE guild_config SET enabled=0")
        self.assertFalse(await poopbot.handle_backdated_poop_mention(self.message, 123))
        self.message.channel.send.assert_not_awaited()

    async def test_database_error_does_not_claim_success(self):
        with mock.patch.object(poopbot, "replay_event_from_history", side_effect=poopbot.sqlite3.OperationalError):
            self.assertTrue(await poopbot.handle_backdated_poop_mention(self.message, 123))
        self.assertIn("couldn't save", self.message.channel.send.call_args.args[0])

    async def test_mentions_are_routed_before_ai_chat(self):
        with mock.patch.object(poopbot.bot._connection, "user", types.SimpleNamespace(id=123)), \
             mock.patch.object(poopbot, "handle_ai_mention", new_callable=mock.AsyncMock) as ai, \
             mock.patch.object(poopbot.bot, "process_commands", new_callable=mock.AsyncMock):
            await poopbot.on_message(self.message)
            ai.assert_not_awaited()
            self.message.content = "<@123> tell me a joke"
            await poopbot.on_message(self.message)
            ai.assert_awaited_once_with(self.message, 123)


if __name__ == "__main__":
    unittest.main()
