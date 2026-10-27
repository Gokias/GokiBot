import asyncio
import json
import os
import tempfile
import types
import unittest
from contextlib import ExitStack
from datetime import date, datetime, timedelta
from unittest import mock

import poopbot


class DailyContentNormalizationTests(unittest.TestCase):
    def test_duplicate_matching_ignores_case_punctuation_and_unicode_width(self):
        original = "Give your bowels time to find their rhythm."
        variant = "Ｇｉｖｅ YOUR bowels time—to find their rhythm!"
        self.assertEqual(
            poopbot.normalize_daily_content(original),
            poopbot.normalize_daily_content(variant),
        )
        self.assertTrue(poopbot.is_repeated_daily_content(variant, [original]))

    def test_minor_rewording_is_rejected_but_new_quote_is_accepted(self):
        history = ["Give your bowels time to find their own rhythm today."]
        self.assertTrue(poopbot.is_repeated_daily_content(
            "Give your bowels time to find their own rhythm this morning.", history
        ))
        self.assertFalse(poopbot.is_repeated_daily_content(
            "Every porcelain throne deserves a peaceful royal visit.", history
        ))


class DailyContentTests(unittest.IsolatedAsyncioTestCase):
    day = date(2026, 10, 27)
    quote = "Every porcelain throne deserves a peaceful royal visit."
    fact = "Your colon absorbs water as waste moves through it."

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
            ("daily_content_lock", asyncio.Lock()),
        ):
            patcher = mock.patch.object(poopbot, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        poopbot.init_config_db()

    def store_entry(self, local_day, content, kind="quote"):
        with poopbot.db_config() as conn:
            conn.execute(
                "INSERT INTO daily_bowel_content "
                "(date_local, kind, content, normalized_content) VALUES (?, ?, ?, ?)",
                (local_day.isoformat(), kind, content, poopbot.normalize_daily_content(content)),
            )

    def read_entries(self):
        with poopbot.db_config() as conn:
            return conn.execute("SELECT * FROM daily_bowel_content ORDER BY date_local").fetchall()

    async def test_same_day_reuses_persisted_content_after_lock_reset(self):
        with mock.patch.object(
            poopbot, "request_daily_bowel_content",
            new=mock.AsyncMock(return_value=("quote", self.quote)),
        ) as request:
            first = await poopbot.get_daily_bowel_content(self.day)
            poopbot.init_config_db()
            with mock.patch.object(poopbot, "daily_content_lock", asyncio.Lock()):
                second = await poopbot.get_daily_bowel_content(self.day)

        self.assertEqual(first, f"**Quote of the day:** {self.quote}")
        self.assertEqual(second, first)
        request.assert_awaited_once()
        self.assertEqual(len(self.read_entries()), 1)

    async def test_concurrent_guild_requests_generate_once(self):
        async def generate(*args):
            await asyncio.sleep(0)
            return "quote", self.quote

        with mock.patch.object(
            poopbot, "request_daily_bowel_content", new=mock.AsyncMock(side_effect=generate),
        ) as request:
            results = await asyncio.gather(*(
                poopbot.get_daily_bowel_content(self.day) for _ in range(4)
            ))

        self.assertEqual(len(set(results)), 1)
        request.assert_awaited_once()
        self.assertEqual(len(self.read_entries()), 1)

    async def test_new_date_generates_new_content_and_preserves_history(self):
        with mock.patch.object(
            poopbot, "request_daily_bowel_content",
            new=mock.AsyncMock(side_effect=[("quote", self.quote), ("fact", self.fact)]),
        ) as request:
            first = await poopbot.get_daily_bowel_content(self.day)
            second = await poopbot.get_daily_bowel_content(self.day + timedelta(days=1))

        self.assertNotEqual(first, second)
        self.assertEqual(second, f"**Fact of the day:** {self.fact}")
        self.assertEqual(request.await_count, 2)
        self.assertIn(self.quote, request.await_args.args[1])
        self.assertEqual(len(self.read_entries()), 2)

    async def test_generation_retries_malformed_and_repeated_responses(self):
        with mock.patch.object(poopbot, "request_ai_reply", new=mock.AsyncMock(side_effect=[
            "This is not JSON",
            json.dumps({"kind": "quote", "text": self.quote.upper()}),
            json.dumps({"kind": "fact", "text": self.fact}),
        ])) as request:
            result = await poopbot.request_daily_bowel_content(self.day, [self.quote])

        self.assertEqual(result, ("fact", self.fact))
        self.assertEqual(request.await_count, 3)
        self.assertEqual(request.await_args.kwargs["system_prompt"], poopbot.DAILY_CONTENT_SYSTEM_PROMPT)
        self.assertIn(self.day.isoformat(), request.await_args.args[0])
        self.assertIn(self.quote, request.await_args.args[0])

    async def test_duplicate_older_than_prompt_history_is_still_rejected(self):
        oldest_day = self.day - timedelta(days=poopbot.DAILY_CONTENT_HISTORY_LIMIT + 2)
        self.store_entry(oldest_day, self.quote)
        with poopbot.db_config() as conn:
            for index in range(poopbot.DAILY_CONTENT_HISTORY_LIMIT + 1):
                content = f"Archived bowel quote number {index} with its own place in history."
                conn.execute(
                    "INSERT INTO daily_bowel_content "
                    "(date_local, kind, content, normalized_content) VALUES (?, ?, ?, ?)",
                    ((oldest_day + timedelta(days=index + 1)).isoformat(), "quote", content,
                     poopbot.normalize_daily_content(content)),
                )
        with mock.patch.object(poopbot, "request_ai_reply", new=mock.AsyncMock(side_effect=[
            json.dumps({"kind": "quote", "text": self.quote}),
            json.dumps({"kind": "fact", "text": self.fact}),
        ])) as request:
            result = await poopbot.get_daily_bowel_content(self.day)

        self.assertEqual(result, f"**Fact of the day:** {self.fact}")
        self.assertEqual(request.await_count, 2)

    async def test_invalid_responses_exhaust_bounded_retries_then_use_fallback(self):
        invalid_responses = (
            {"kind": "other", "text": "Unrecognized format"},
            {"kind": "quote", "text": ""},
            {"kind": "fact", "text": "x" * 351},
        )
        fallbacks = (
            self.quote,
            "Give your bowels time to find their own rhythm today.",
            "A quiet loo can turn a busy morning into a moment of peace.",
        )
        for index, payload in enumerate(invalid_responses):
            with self.subTest(payload=payload), mock.patch.object(
                poopbot, "request_ai_reply", new=mock.AsyncMock(return_value=json.dumps(payload)),
            ) as request, mock.patch.object(
                poopbot, "DAILY_CONTENT_FALLBACK_QUOTES", (fallbacks[index],),
            ):
                result = await poopbot.get_daily_bowel_content(self.day + timedelta(days=index))
            self.assertEqual(request.await_count, poopbot.DAILY_CONTENT_MAX_ATTEMPTS)
            self.assertEqual(result, f"**Quote of the day:** {fallbacks[index]}")

    async def test_api_failure_uses_and_persists_unused_fallback(self):
        with mock.patch.object(
            poopbot, "request_daily_bowel_content", new=mock.AsyncMock(side_effect=RuntimeError("offline")),
        ) as request, mock.patch.object(poopbot, "DAILY_CONTENT_FALLBACK_QUOTES", (self.quote,)):
            first = await poopbot.get_daily_bowel_content(self.day)
            second = await poopbot.get_daily_bowel_content(self.day)
            exhausted = await poopbot.get_daily_bowel_content(self.day + timedelta(days=1))
            still_exhausted = await poopbot.get_daily_bowel_content(self.day + timedelta(days=1))

        self.assertEqual(first, f"**Quote of the day:** {self.quote}")
        self.assertEqual(second, first)
        self.assertEqual(exhausted, "")
        self.assertEqual(still_exhausted, "")
        self.assertEqual(request.await_count, 2)
        self.assertEqual([row["content"] for row in self.read_entries()], [self.quote, ""])

    async def test_generation_timeout_does_not_prevent_fallback(self):
        async def never_finishes(*args):
            await asyncio.Event().wait()

        with mock.patch.object(
            poopbot, "request_daily_bowel_content", new=mock.AsyncMock(side_effect=never_finishes),
        ), mock.patch.object(poopbot, "DAILY_CONTENT_TIMEOUT_SECONDS", 0.01), mock.patch.object(
            poopbot, "DAILY_CONTENT_FALLBACK_QUOTES", (self.quote,),
        ):
            result = await poopbot.get_daily_bowel_content(self.day)
        self.assertEqual(result, f"**Quote of the day:** {self.quote}")
        self.assertEqual(len(self.read_entries()), 1)

    async def test_missing_api_key_still_provides_daily_fallback(self):
        with mock.patch.object(poopbot, "ai_client", None), \
                mock.patch.object(poopbot, "OPENAI_API_KEY", None), \
                mock.patch.object(poopbot, "DAILY_CONTENT_FALLBACK_QUOTES", (self.quote,)):
            result = await poopbot.get_daily_bowel_content(self.day)

        self.assertEqual(result, f"**Quote of the day:** {self.quote}")
        self.assertEqual(len(self.read_entries()), 1)

    async def test_database_rejects_duplicate_normalized_content_on_another_day(self):
        self.store_entry(self.day, self.quote)
        with self.assertRaises(poopbot.sqlite3.IntegrityError):
            self.store_entry(self.day + timedelta(days=1), self.quote.upper())
        self.assertEqual(len(self.read_entries()), 1)

    async def test_button_content_replaces_tip_and_preserves_reaction_instructions(self):
        daily = f"**Quote of the day:** {self.quote}"
        with mock.patch.object(
            poopbot, "get_daily_bowel_content", new=mock.AsyncMock(return_value=daily),
        ) as get_daily:
            result = await poopbot.poop_button_content(self.day)

        get_daily.assert_awaited_once_with(self.day)
        self.assertIn("2026-10-27 (Pacific)", result)
        self.assertIn(f"React {poopbot.POOP_EMOJI} to log.", result)
        self.assertIn(f"React {poopbot.UNDO_EMOJI} to undo your most recent log.", result)
        self.assertIn(daily, result)
        self.assertNotIn("Missed a log?", result)

    async def test_current_button_refreshes_date_when_generation_crosses_pacific_midnight(self):
        before_midnight = datetime(2026, 10, 27, 23, 59, 59, tzinfo=poopbot.LOCAL_TZ)
        after_midnight = datetime(2026, 10, 28, 0, 0, 1, tzinfo=poopbot.LOCAL_TZ)
        with mock.patch.object(poopbot, "datetime") as clock, mock.patch.object(
            poopbot, "get_daily_bowel_content", new=mock.AsyncMock(side_effect=[
                f"**Quote of the day:** {self.quote}",
                f"**Fact of the day:** {self.fact}",
            ]),
        ) as get_daily:
            clock.now.side_effect = [before_midnight, after_midnight, after_midnight, after_midnight]
            local_day, content = await poopbot.current_poop_button_content()

        self.assertEqual(local_day, date(2026, 10, 28))
        self.assertIn("2026-10-28 (Pacific)", content)
        self.assertIn(f"**Fact of the day:** {self.fact}", content)
        self.assertNotIn("2026-10-27", content)
        self.assertNotIn(self.quote, content)
        self.assertEqual(get_daily.await_args_list, [mock.call(self.day), mock.call(date(2026, 10, 28))])
        self.assertEqual(clock.now.call_args_list, [mock.call(poopbot.LOCAL_TZ)] * 4)

    async def test_post_prepares_daily_content_before_deleting_old_button(self):
        events = []
        old_message = types.SimpleNamespace(
            author=types.SimpleNamespace(id=999),
            delete=mock.AsyncMock(side_effect=lambda: events.append("delete")),
        )
        new_message = types.SimpleNamespace(id=456, add_reaction=mock.AsyncMock())

        async def send(*args, **kwargs):
            events.append("send")
            return new_message

        async def daily(*args):
            events.append("generate")
            return f"**Quote of the day:** {self.quote}"

        channel = types.SimpleNamespace(
            fetch_message=mock.AsyncMock(return_value=old_message),
            send=mock.AsyncMock(side_effect=send),
        )
        poopbot.gset(1, "active_message_id", "123")
        with mock.patch.object(poopbot.bot, "fetch_channel", new=mock.AsyncMock(return_value=channel)), \
                mock.patch.object(poopbot.bot._connection, "user", types.SimpleNamespace(id=999)), \
                mock.patch.object(poopbot, "get_daily_bowel_content", new=mock.AsyncMock(side_effect=daily)):
            await poopbot.post_button_for_guild(1, 2)

        self.assertEqual(events, ["generate", "delete", "send"])
        self.assertIn(self.quote, channel.send.await_args.args[0])
        self.assertEqual(channel.send.await_args.kwargs["allowed_mentions"].to_dict(), {"parse": []})
        self.assertEqual(poopbot.gget(1, "active_message_id"), "456")
        self.assertEqual(new_message.add_reaction.await_args_list, [
            mock.call(poopbot.POOP_EMOJI), mock.call(poopbot.UNDO_EMOJI),
        ])

    async def test_startup_edits_existing_button_with_persisted_daily_content(self):
        today = datetime.now(poopbot.LOCAL_TZ).date()
        self.store_entry(today, self.quote)
        poopbot.set_guild_channel(1, 2)
        poopbot.gset(1, "active_date_local", today.isoformat())
        poopbot.gset(1, "active_message_id", "123")
        message = types.SimpleNamespace(author=types.SimpleNamespace(id=999), edit=mock.AsyncMock())
        channel = types.SimpleNamespace(fetch_message=mock.AsyncMock(return_value=message))
        with ExitStack() as stack:
            for name in ("init_year_db", "init_cleanup_db", "init_wordle_db"):
                stack.enter_context(mock.patch.object(poopbot, name))
            for name in ("daily_midnight_pacific", "wesroth_upload_watch", "wordle_daily_sync"):
                stack.enter_context(mock.patch.object(
                    poopbot, name, types.SimpleNamespace(is_running=lambda: True),
                ))
            stack.enter_context(mock.patch.object(poopbot.bot.tree, "sync", new=mock.AsyncMock()))
            stack.enter_context(mock.patch.object(
                poopbot.bot, "fetch_channel", new=mock.AsyncMock(return_value=channel),
            ))
            stack.enter_context(mock.patch.object(
                poopbot.bot._connection, "user", types.SimpleNamespace(id=999),
            ))
            request = stack.enter_context(mock.patch.object(
                poopbot, "request_daily_bowel_content", new=mock.AsyncMock(),
            ))
            await poopbot.on_ready()

        request.assert_not_awaited()
        message.edit.assert_awaited_once()
        content = message.edit.await_args.kwargs["content"]
        self.assertIn(f"**Quote of the day:** {self.quote}", content)
        self.assertNotIn("Missed a log?", content)
        self.assertEqual(message.edit.await_args.kwargs["allowed_mentions"].to_dict(), {"parse": []})

    async def test_debugpoop_defers_before_waiting_for_daily_generation(self):
        events = []
        poopbot.set_guild_channel(1, 2)
        interaction = types.SimpleNamespace(
            guild=types.SimpleNamespace(id=1),
            response=types.SimpleNamespace(
                defer=mock.AsyncMock(side_effect=lambda: events.append("defer")),
                send_message=mock.AsyncMock(),
            ),
            followup=types.SimpleNamespace(send=mock.AsyncMock(side_effect=lambda *a: events.append("followup"))),
        )
        with mock.patch.object(
            poopbot, "post_button_for_guild",
            new=mock.AsyncMock(side_effect=lambda *args: events.append("post")),
        ) as post:
            await poopbot.debugpoop.callback(interaction)

        self.assertEqual(events, ["defer", "post", "followup"])
        post.assert_awaited_once_with(1, 2)
        interaction.response.send_message.assert_not_awaited()

    async def test_daily_prompt_uses_existing_client_and_model(self):
        response = types.SimpleNamespace(output_text=json.dumps({"kind": "quote", "text": self.quote}))
        create = mock.AsyncMock(return_value=response)
        client = types.SimpleNamespace(responses=types.SimpleNamespace(create=create))
        with mock.patch.object(poopbot, "ai_client", client):
            result = await poopbot.request_daily_bowel_content(self.day, [])

        self.assertEqual(result, ("quote", self.quote))
        self.assertEqual(create.await_args.kwargs["model"], poopbot.OPENAI_MODEL)
        self.assertEqual(create.await_args.kwargs["instructions"], poopbot.DAILY_CONTENT_SYSTEM_PROMPT)


if __name__ == "__main__":
    unittest.main()
