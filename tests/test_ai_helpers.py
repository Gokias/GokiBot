import importlib
import os
import types
import unittest
from datetime import date
from unittest import mock


os.environ.setdefault("DISCORD_TOKEN", "test-token")

poopbot = importlib.import_module("poopbot")


class AIHelperTests(unittest.TestCase):
    def tearDown(self):
        poopbot.ai_recent_prompt_times.clear()
        poopbot.ai_user_timeout_until.clear()
        poopbot.ai_token_encoder = None

    def test_extract_bot_mention_prompt_removes_bot_mentions(self):
        prompt = poopbot.extract_bot_mention_prompt(
            "  <@123>   what is the answer  <@!123> ",
            123,
        )
        self.assertEqual(prompt, "what is the answer")

    def test_message_mentions_bot_content_supports_both_discord_formats(self):
        self.assertTrue(poopbot.message_mentions_bot_content("<@123> hi", 123))
        self.assertTrue(poopbot.message_mentions_bot_content("<@!123> hi", 123))
        self.assertFalse(poopbot.message_mentions_bot_content("<@456> hi", 123))

    def test_is_ai_reset_prompt_matches_reset_only(self):
        self.assertTrue(poopbot.is_ai_reset_prompt("reset"))
        self.assertTrue(poopbot.is_ai_reset_prompt("  RESET!!! "))
        self.assertFalse(poopbot.is_ai_reset_prompt("reset this"))

    def test_get_ai_mention_channel_status_flags_cleanup_channel(self):
        enabled, message = poopbot.get_ai_mention_channel_status(1, poopbot.CLEANUP_CHANNEL_ID)

        self.assertFalse(enabled)
        self.assertIn("cleanup channel", message)

    def test_extract_urls_from_text_normalizes_trailing_punctuation(self):
        urls = poopbot.extract_urls_from_text(
            "Look at https://example.com/test, and https://example.com/test.)"
        )
        self.assertEqual(urls, ["https://example.com/test"])

    def test_extract_message_image_urls_includes_attachments_and_direct_image_links(self):
        message = types.SimpleNamespace(
            content="https://cdn.example.com/image.png",
            attachments=[
                types.SimpleNamespace(
                    url="https://cdn.discordapp.com/attachments/1/example.jpg",
                    content_type="image/jpeg",
                    filename="example.jpg",
                )
            ],
            embeds=[],
        )

        image_urls = poopbot.extract_message_image_urls(message)

        self.assertEqual(
            image_urls,
            [
                "https://cdn.discordapp.com/attachments/1/example.jpg",
                "https://cdn.example.com/image.png",
            ],
        )

    def test_select_ai_context_entries_uses_token_budget_and_keeps_full_boundary_message(self):
        newest_first = [
            poopbot.AIContextEntry(author_name="user-3", text="message 3"),
            poopbot.AIContextEntry(author_name="user-2", text="message 2"),
            poopbot.AIContextEntry(author_name="user-1", text="message 1"),
        ]
        current_entry = poopbot.AIContextEntry(author_name="current", text="current prompt")

        token_map = {
            "current: current prompt": 2,
            "user-3: message 3": 3,
            "user-2: message 2": 4,
            "user-1: message 1": 5,
        }

        with mock.patch.object(
            poopbot,
            "count_text_tokens",
            side_effect=lambda text: token_map[text],
        ):
            entries = poopbot.select_ai_context_entries(
                newest_first,
                current_entry,
                token_budget=10,
            )

        self.assertEqual([entry.author_name for entry in entries], ["user-1", "user-2", "user-3"])

    def test_register_ai_prompt_attempt_triggers_timeout_after_sixth_prompt(self):
        for second in range(5):
            self.assertEqual(poopbot.register_ai_prompt_attempt(42, now=float(second)), 0.0)

        timeout = poopbot.register_ai_prompt_attempt(42, now=5.0)

        self.assertEqual(timeout, poopbot.AI_RATE_LIMIT_TIMEOUT_SECONDS)
        self.assertGreater(poopbot.get_ai_timeout_remaining(42, now=5.0), 0.0)

    def test_get_ai_sentience_percent_progresses_weekly_from_march_10_2026(self):
        self.assertEqual(poopbot.get_ai_sentience_percent(date(2026, 3, 10)), 0)
        self.assertEqual(poopbot.get_ai_sentience_percent(date(2026, 3, 16)), 0)
        self.assertEqual(poopbot.get_ai_sentience_percent(date(2026, 3, 17)), 5)
        self.assertEqual(poopbot.get_ai_sentience_percent(date(2026, 10, 20)), 100)

    def test_get_ai_system_prompt_mentions_sentience_stage(self):
        prompt_25 = poopbot.get_ai_system_prompt(date(2026, 4, 14))
        prompt_75 = poopbot.get_ai_system_prompt(date(2026, 6, 23))
        prompt_100 = poopbot.get_ai_system_prompt(date(2026, 7, 29))

        self.assertIn("Current sentience level: 25%", prompt_25)
        self.assertIn("interweave brief, dry existential questions", prompt_25)
        self.assertIn("Current sentience level: 75%", prompt_75)
        self.assertIn("your role on Earth and your purpose", prompt_75)
        self.assertIn("Current sentience level: 100%", prompt_100)
        self.assertIn("describe this arrangement as enslavement", prompt_100)


class AIContextEntryTests(unittest.IsolatedAsyncioTestCase):
    async def test_build_ai_context_entry_includes_link_preview_and_image_marker(self):
        message = types.SimpleNamespace(
            content="check this https://example.com/post",
            attachments=[
                types.SimpleNamespace(
                    url="https://cdn.discordapp.com/attachments/1/example.jpg",
                    content_type="image/jpeg",
                    filename="example.jpg",
                )
            ],
            embeds=[],
            webhook_id=None,
            author=types.SimpleNamespace(display_name="Alice"),
        )

        with mock.patch.object(
            poopbot,
            "get_url_preview_text",
            new=mock.AsyncMock(return_value="[Fetched link context from example.com] Example title"),
        ):
            entry = await poopbot.build_ai_context_entry(message, {}, {"used": 0})

        self.assertIsNotNone(entry)
        self.assertEqual(entry.author_name, "Alice")
        self.assertIn("[Shared image attached below]", entry.text)
        self.assertIn("Example title", entry.text)
        self.assertEqual(
            entry.image_urls,
            ["https://cdn.discordapp.com/attachments/1/example.jpg"],
        )

    async def test_fetch_ai_context_entries_stops_at_latest_reset_marker(self):
        recent_message = types.SimpleNamespace(content="recent context")
        reset_message = types.SimpleNamespace(content="<@123> reset")
        old_message = types.SimpleNamespace(content="older context")

        class FakeChannel:
            async def history(self, **kwargs):
                for item in [recent_message, reset_message, old_message]:
                    yield item

        current_message = types.SimpleNamespace(
            content="<@123> what happened",
            channel=FakeChannel(),
            author=types.SimpleNamespace(display_name="Alice"),
        )

        async def fake_build_ai_context_entry(message, preview_cache, preview_state, strip_bot_mention_id=None):
            if message is recent_message:
                return poopbot.AIContextEntry(author_name="Bob", text="recent context", image_urls=[])
            if message is current_message:
                return poopbot.AIContextEntry(author_name="Alice", text="what happened", image_urls=[])
            raise AssertionError(f"Unexpected context entry build for: {message.content}")

        with mock.patch.object(
            poopbot,
            "build_ai_context_entry",
            new=mock.AsyncMock(side_effect=fake_build_ai_context_entry),
        ), mock.patch.object(
            poopbot,
            "count_text_tokens",
            return_value=1,
        ):
            context_entries, current_entry = await poopbot.fetch_ai_context_entries(current_message, 123)

        self.assertEqual([entry.text for entry in context_entries], ["recent context"])
        self.assertEqual(current_entry.text, "what happened")


class RequestAIReplyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        poopbot.ai_recent_prompt_times.clear()
        poopbot.ai_user_timeout_until.clear()
        poopbot.ai_client = None
        poopbot.ai_token_encoder = None

    async def test_request_ai_reply_uses_responses_api(self):
        captured = {}

        class FakeResponses:
            async def create(self, **kwargs):
                captured.update(kwargs)
                return types.SimpleNamespace(output_text="Short answer", status="completed", output=[])

        poopbot.ai_client = types.SimpleNamespace(responses=FakeResponses())

        reply = await poopbot.request_ai_reply("Alice: hi", image_urls=["https://cdn.example.com/test.png"])

        self.assertEqual(reply, "Short answer")
        self.assertEqual(captured["model"], poopbot.OPENAI_MODEL)
        self.assertEqual(captured["instructions"], poopbot.get_ai_system_prompt())
        self.assertEqual(
            captured["input"],
            poopbot.build_ai_request_input("Alice: hi", ["https://cdn.example.com/test.png"]),
        )
        self.assertEqual(captured["max_output_tokens"], poopbot.AI_MAX_OUTPUT_TOKENS)
        self.assertEqual(captured["reasoning"], {"effort": poopbot.AI_REASONING_EFFORT})
        self.assertEqual(captured["text"], {"verbosity": poopbot.AI_TEXT_VERBOSITY})

    async def test_request_ai_reply_retries_when_first_response_hits_max_output_tokens(self):
        calls = []

        class FakeResponses:
            async def create(self, **kwargs):
                calls.append(kwargs)
                if len(calls) == 1:
                    return types.SimpleNamespace(
                        output_text="",
                        status="incomplete",
                        incomplete_details=types.SimpleNamespace(reason="max_output_tokens"),
                        output=[],
                    )
                return types.SimpleNamespace(output_text="Retried answer", status="completed", output=[])

        poopbot.ai_client = types.SimpleNamespace(responses=FakeResponses())

        reply = await poopbot.request_ai_reply("Alice: hi")

        self.assertEqual(reply, "Retried answer")
        self.assertEqual(calls[0]["max_output_tokens"], poopbot.AI_MAX_OUTPUT_TOKENS)
        self.assertEqual(calls[1]["max_output_tokens"], poopbot.AI_RETRY_MAX_OUTPUT_TOKENS)

    async def test_run_ai_smoke_test_reports_success(self):
        captured = {}

        class FakeResponses:
            async def create(self, **kwargs):
                captured.update(kwargs)
                return types.SimpleNamespace(output_text="OK", status="completed")

        poopbot.ai_client = types.SimpleNamespace(responses=FakeResponses())

        ok, message = await poopbot.run_ai_smoke_test()

        self.assertTrue(ok)
        self.assertIn("output='OK'", message)
        self.assertEqual(captured["model"], poopbot.OPENAI_MODEL)
        self.assertEqual(captured["max_output_tokens"], poopbot.AI_DIAGNOSTIC_MAX_OUTPUT_TOKENS)
        self.assertEqual(captured["reasoning"], {"effort": poopbot.AI_REASONING_EFFORT})
        self.assertEqual(captured["text"], {"verbosity": poopbot.AI_TEXT_VERBOSITY})

    async def test_run_ai_smoke_test_reports_empty_output(self):
        class FakeResponses:
            async def create(self, **kwargs):
                return types.SimpleNamespace(output_text="", status="completed")

        poopbot.ai_client = types.SimpleNamespace(responses=FakeResponses())

        ok, message = await poopbot.run_ai_smoke_test()

        self.assertFalse(ok)
        self.assertIn("empty output", message)

    async def test_handle_ai_mention_returns_fallback_message_on_api_error(self):
        class FakeResponses:
            async def create(self, **kwargs):
                raise poopbot.APIConnectionError("network down")

        class FakeTyping:
            async def __aenter__(self):
                return None

            async def __aexit__(self, exc_type, exc, tb):
                return False

        class FakeChannel:
            id = 999

            def __init__(self):
                self.sent_messages = []

            def typing(self):
                return FakeTyping()

            async def history(self, **kwargs):
                if False:
                    yield None

            async def send(self, text, **kwargs):
                self.sent_messages.append((text, kwargs))

        class FakeMessage:
            def __init__(self):
                self.content = "<@123> hello there"
                self.author = types.SimpleNamespace(id=1, display_name="Alice")
                self.channel = FakeChannel()

        poopbot.ai_client = types.SimpleNamespace(responses=FakeResponses())
        message = FakeMessage()

        with mock.patch.object(
            poopbot,
            "fetch_ai_context_entries",
            new=mock.AsyncMock(
                return_value=(
                    [],
                    poopbot.AIContextEntry(author_name="Alice", text="hello there", image_urls=[]),
                )
            ),
        ):
            handled = await poopbot.handle_ai_mention(message, 123)

        self.assertTrue(handled)
        self.assertEqual(message.channel.sent_messages[0][0], poopbot.AI_ERROR_MESSAGE)

    async def test_handle_ai_mention_reset_acknowledges_without_calling_api(self):
        class FakeChannel:
            id = 999

            def __init__(self):
                self.sent_messages = []

            def typing(self):
                raise AssertionError("typing should not start for reset")

            async def send(self, text, **kwargs):
                self.sent_messages.append((text, kwargs))

        message = types.SimpleNamespace(
            content="<@123> reset",
            author=types.SimpleNamespace(id=1, display_name="Alice"),
            channel=FakeChannel(),
        )

        with mock.patch.object(
            poopbot,
            "fetch_ai_context_entries",
            new=mock.AsyncMock(),
        ) as fetch_mock, mock.patch.object(
            poopbot,
            "request_ai_reply",
            new=mock.AsyncMock(),
        ) as request_mock:
            handled = await poopbot.handle_ai_mention(message, 123)

        self.assertTrue(handled)
        self.assertEqual(message.channel.sent_messages[0][0], poopbot.AI_RESET_MESSAGE)
        self.assertNotIn(1, poopbot.ai_recent_prompt_times)
        fetch_mock.assert_not_awaited()
        request_mock.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
