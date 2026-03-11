import importlib
import os
import types
import unittest
from unittest import mock


os.environ.setdefault("DISCORD_TOKEN", "test-token")

poopbot = importlib.import_module("poopbot")


class AIHelperTests(unittest.TestCase):
    def tearDown(self):
        poopbot.ai_recent_prompt_times.clear()
        poopbot.ai_user_timeout_until.clear()

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

    def test_select_ai_context_entries_filters_empty_messages_and_caps_to_ten(self):
        history_messages = []
        for index in range(12, 0, -1):
            content = f"message {index}"
            if index == 7:
                content = "   "
            history_messages.append(
                types.SimpleNamespace(
                    content=content,
                    attachments=[],
                    embeds=[],
                    webhook_id=None,
                    author=types.SimpleNamespace(display_name=f"user-{index}"),
                )
            )

        entries = poopbot.select_ai_context_entries(history_messages)

        self.assertEqual(len(entries), 10)
        self.assertEqual(entries[0].author_name, "user-2")
        self.assertEqual(entries[0].text, "message 2")
        self.assertEqual(entries[-1].author_name, "user-12")
        self.assertEqual(entries[-1].text, "message 12")

    def test_register_ai_prompt_attempt_triggers_timeout_after_sixth_prompt(self):
        for second in range(5):
            self.assertEqual(poopbot.register_ai_prompt_attempt(42, now=float(second)), 0.0)

        timeout = poopbot.register_ai_prompt_attempt(42, now=5.0)

        self.assertEqual(timeout, poopbot.AI_RATE_LIMIT_TIMEOUT_SECONDS)
        self.assertGreater(poopbot.get_ai_timeout_remaining(42, now=5.0), 0.0)


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


class RequestAIReplyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        poopbot.ai_recent_prompt_times.clear()
        poopbot.ai_user_timeout_until.clear()
        poopbot.ai_client = None

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
        self.assertEqual(captured["instructions"], poopbot.AI_SYSTEM_PROMPT)
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

            def typing(self):
                return FakeTyping()

            async def history(self, **kwargs):
                if False:
                    yield None

        class FakeMessage:
            def __init__(self):
                self.content = "<@123> hello there"
                self.author = types.SimpleNamespace(id=1, display_name="Alice")
                self.channel = FakeChannel()
                self.replies = []

            async def reply(self, text, **kwargs):
                self.replies.append((text, kwargs))

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
        self.assertEqual(message.replies[0][0], poopbot.AI_ERROR_MESSAGE)


if __name__ == "__main__":
    unittest.main()
