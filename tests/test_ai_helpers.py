import importlib
import os
import types
import unittest
from unittest import mock


os.environ.setdefault("DISCORD_TOKEN", "test-token")

poopbot = importlib.import_module("poopbot")


class AIHelperTests(unittest.TestCase):
    def tearDown(self):
        poopbot.ai_cooldowns.clear()

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

    def test_select_ai_context_entries_filters_empty_messages_and_caps_to_ten(self):
        history_messages = []
        for index in range(12, 0, -1):
            content = f"message {index}"
            if index == 7:
                content = "   "
            history_messages.append(
                types.SimpleNamespace(
                    content=content,
                    webhook_id=None,
                    author=types.SimpleNamespace(display_name=f"user-{index}"),
                )
            )

        entries = poopbot.select_ai_context_entries(history_messages)

        self.assertEqual(len(entries), 10)
        self.assertEqual(entries[0], ("user-2", "message 2"))
        self.assertEqual(entries[-1], ("user-12", "message 12"))
        self.assertNotIn(("user-7", ""), entries)

    def test_get_ai_cooldown_remaining_expires(self):
        poopbot.mark_ai_cooldown(42, now=100.0)
        self.assertEqual(poopbot.get_ai_cooldown_remaining(42, now=100.0), poopbot.AI_COOLDOWN_SECONDS)
        self.assertEqual(poopbot.get_ai_cooldown_remaining(42, now=131.0), 0.0)


class RequestAIReplyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        poopbot.ai_cooldowns.clear()
        poopbot.ai_client = None

    async def test_request_ai_reply_uses_responses_api(self):
        captured = {}

        class FakeResponses:
            async def create(self, **kwargs):
                captured.update(kwargs)
                return types.SimpleNamespace(output_text="Short answer")

        poopbot.ai_client = types.SimpleNamespace(responses=FakeResponses())

        reply = await poopbot.request_ai_reply("Alice: hi")

        self.assertEqual(reply, "Short answer")
        self.assertEqual(captured["model"], poopbot.OPENAI_MODEL)
        self.assertEqual(captured["instructions"], poopbot.AI_SYSTEM_PROMPT)
        self.assertEqual(captured["input"], "Alice: hi")
        self.assertEqual(captured["max_output_tokens"], poopbot.AI_MAX_OUTPUT_TOKENS)

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

        with mock.patch.object(poopbot, "fetch_ai_context_entries", new=mock.AsyncMock(return_value=[])):
            handled = await poopbot.handle_ai_mention(message, 123)

        self.assertTrue(handled)
        self.assertEqual(message.replies[0][0], poopbot.AI_ERROR_MESSAGE)


if __name__ == "__main__":
    unittest.main()
