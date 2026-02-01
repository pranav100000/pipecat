#
# Copyright (c) 2024-2025 Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import unittest

from pipecat.frames.frames import TextFrame, UserStartedSpeakingFrame
from pipecat.processors.text_transformer import StatelessTextTransformer
from pipecat.tests.utils import run_test


class TestStatelessTextTransformer(unittest.IsolatedAsyncioTestCase):
    async def test_sync_transform(self):
        """Test transformer with synchronous function."""

        def uppercase(text: str) -> str:
            return text.upper()

        transformer = StatelessTextTransformer(transform_fn=uppercase)
        frames_to_send = [TextFrame(text="hello world")]
        expected_down_frames = [TextFrame]

        (received_down, _) = await run_test(
            transformer,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )

        assert received_down[0].text == "HELLO WORLD"

    async def test_async_transform(self):
        """Test transformer with asynchronous function."""

        async def async_uppercase(text: str) -> str:
            return text.upper()

        transformer = StatelessTextTransformer(transform_fn=async_uppercase)
        frames_to_send = [TextFrame(text="async test")]
        expected_down_frames = [TextFrame]

        (received_down, _) = await run_test(
            transformer,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )

        assert received_down[0].text == "ASYNC TEST"

    async def test_passthrough_non_text_frames(self):
        """Test that non-TextFrame frames pass through unchanged."""
        transformer = StatelessTextTransformer(transform_fn=lambda x: x.upper())
        frames_to_send = [UserStartedSpeakingFrame()]
        expected_down_frames = [UserStartedSpeakingFrame]

        await run_test(
            transformer,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )

    async def test_mixed_frames(self):
        """Test processing of mixed frame types."""

        def reverse(text: str) -> str:
            return text[::-1]

        transformer = StatelessTextTransformer(transform_fn=reverse)
        frames_to_send = [
            UserStartedSpeakingFrame(),
            TextFrame(text="hello"),
            TextFrame(text="world"),
        ]
        expected_down_frames = [UserStartedSpeakingFrame, TextFrame, TextFrame]

        (received_down, _) = await run_test(
            transformer,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )

        assert received_down[0].__class__.__name__ == "UserStartedSpeakingFrame"
        assert received_down[1].text == "olleh"
        assert received_down[2].text == "dlrow"

    async def test_empty_string_transform(self):
        """Test transformation of empty string."""

        def add_prefix(text: str) -> str:
            return f"prefix: {text}"

        transformer = StatelessTextTransformer(transform_fn=add_prefix)
        frames_to_send = [TextFrame(text="")]
        expected_down_frames = [TextFrame]

        (received_down, _) = await run_test(
            transformer,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )

        assert received_down[0].text == "prefix: "

    async def test_lambda_transform(self):
        """Test transformer with lambda function."""
        transformer = StatelessTextTransformer(transform_fn=lambda x: x.replace("a", "b"))
        frames_to_send = [TextFrame(text="banana")]
        expected_down_frames = [TextFrame]

        (received_down, _) = await run_test(
            transformer,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )

        assert received_down[0].text == "bbnbnb"

    async def test_complex_transform(self):
        """Test transformer with more complex transformation."""

        def complex_transform(text: str) -> str:
            words = text.split()
            return " ".join(word.capitalize() for word in words)

        transformer = StatelessTextTransformer(transform_fn=complex_transform)
        frames_to_send = [TextFrame(text="hello beautiful world")]
        expected_down_frames = [TextFrame]

        (received_down, _) = await run_test(
            transformer,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )

        assert received_down[0].text == "Hello Beautiful World"

    async def test_multiple_text_frames(self):
        """Test processing multiple text frames sequentially."""

        def lowercase(text: str) -> str:
            return text.lower()

        transformer = StatelessTextTransformer(transform_fn=lowercase)
        frames_to_send = [
            TextFrame(text="FIRST"),
            TextFrame(text="SECOND"),
            TextFrame(text="THIRD"),
        ]
        expected_down_frames = [TextFrame, TextFrame, TextFrame]

        (received_down, _) = await run_test(
            transformer,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )

        assert received_down[0].text == "first"
        assert received_down[1].text == "second"
        assert received_down[2].text == "third"
