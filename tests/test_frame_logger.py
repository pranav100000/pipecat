#
# Copyright (c) 2024-2025 Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import unittest
from unittest.mock import patch

from pipecat.frames.frames import (
    BotSpeakingFrame,
    InputAudioRawFrame,
    OutputAudioRawFrame,
    TextFrame,
    TransportMessageFrame,
    UserStartedSpeakingFrame,
)
from pipecat.processors.logger import FrameLogger
from pipecat.tests.utils import run_test


class TestFrameLogger(unittest.IsolatedAsyncioTestCase):
    async def test_passthrough_text_frames(self):
        """Test that FrameLogger passes through text frames unchanged."""
        logger = FrameLogger()
        frames_to_send = [
            TextFrame(text="Hello"),
            TextFrame(text="World"),
        ]
        expected_down_frames = [TextFrame, TextFrame]

        (received_down, _) = await run_test(
            logger,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )
        assert received_down[0].text == "Hello"
        assert received_down[1].text == "World"

    async def test_passthrough_system_frames(self):
        """Test that FrameLogger passes through system frames unchanged."""
        logger = FrameLogger()
        frames_to_send = [UserStartedSpeakingFrame()]
        expected_down_frames = [UserStartedSpeakingFrame]

        await run_test(
            logger,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )

    async def test_default_ignored_frames(self):
        """Test that default ignored frames are not logged but still passed through."""
        logger = FrameLogger()

        # These frames are ignored from logging but still pass through
        # Test only InputAudioRawFrame since it's a DataFrame (ordered)
        frames_to_send = [
            InputAudioRawFrame(audio=b"\x00", sample_rate=16000, num_channels=1),
        ]
        expected_down_frames = [InputAudioRawFrame]

        await run_test(
            logger,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )

    async def test_custom_prefix(self):
        """Test FrameLogger with custom prefix."""
        logger = FrameLogger(prefix="CustomPrefix")
        frames_to_send = [TextFrame(text="Test")]
        expected_down_frames = [TextFrame]

        await run_test(
            logger,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )

    async def test_custom_color(self):
        """Test FrameLogger with custom color."""
        logger = FrameLogger(color="green")
        frames_to_send = [TextFrame(text="Test")]
        expected_down_frames = [TextFrame]

        await run_test(
            logger,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )

    async def test_no_ignored_frames(self):
        """Test FrameLogger with no ignored frame types."""
        logger = FrameLogger(ignored_frame_types=())
        # Only use data frames for consistent ordering
        frames_to_send = [
            TextFrame(text="Test"),
        ]
        expected_down_frames = [TextFrame]

        (received_down, _) = await run_test(
            logger,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )
        assert received_down[0].text == "Test"

    async def test_custom_ignored_frames(self):
        """Test FrameLogger with custom ignored frame types."""
        logger = FrameLogger(ignored_frame_types=(TextFrame,))
        # Only use data frames for consistent ordering
        frames_to_send = [
            TextFrame(text="Ignored"),
        ]
        expected_down_frames = [TextFrame]

        (received_down, _) = await run_test(
            logger,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )
        # Frame should still pass through even if logging is ignored
        assert received_down[0].text == "Ignored"

    async def test_frame_text_content_preserved(self):
        """Test that logged frames retain their content."""
        logger = FrameLogger(prefix="Test")
        frames_to_send = [TextFrame(text="Hello Pipecat!")]
        expected_down_frames = [TextFrame]

        (received_down, _) = await run_test(
            logger,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )

        assert received_down[0].text == "Hello Pipecat!"

    async def test_multiple_frames_in_sequence(self):
        """Test logging multiple frames in sequence."""
        logger = FrameLogger()
        frames_to_send = [
            TextFrame(text="First"),
            TextFrame(text="Second"),
            TextFrame(text="Third"),
        ]
        expected_down_frames = [TextFrame, TextFrame, TextFrame]

        (received_down, _) = await run_test(
            logger,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )

        assert received_down[0].text == "First"
        assert received_down[1].text == "Second"
        assert received_down[2].text == "Third"
