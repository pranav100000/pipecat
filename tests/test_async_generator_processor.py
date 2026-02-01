#
# Copyright (c) 2024-2025 Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import asyncio
import unittest
from typing import Optional

from pipecat.frames.frames import CancelFrame, EndFrame, Frame, TextFrame
from pipecat.processors.async_generator import AsyncGeneratorProcessor
from pipecat.serializers.base_serializer import FrameSerializer, FrameSerializerType
from pipecat.tests.utils import run_test


class MockTextSerializer(FrameSerializer):
    """Mock serializer that converts frames to simple text format."""

    @property
    def type(self) -> FrameSerializerType:
        return FrameSerializerType.TEXT

    async def serialize(self, frame: Frame) -> Optional[str]:
        if isinstance(frame, TextFrame):
            return f"TEXT:{frame.text}"
        return None

    async def deserialize(self, data: str) -> Optional[Frame]:
        if data.startswith("TEXT:"):
            return TextFrame(text=data[5:])
        return None


class MockBinarySerializer(FrameSerializer):
    """Mock serializer that converts frames to binary format."""

    @property
    def type(self) -> FrameSerializerType:
        return FrameSerializerType.BINARY

    async def serialize(self, frame: Frame) -> Optional[bytes]:
        if isinstance(frame, TextFrame):
            return f"BIN:{frame.text}".encode()
        return None

    async def deserialize(self, data: bytes) -> Optional[Frame]:
        text = data.decode()
        if text.startswith("BIN:"):
            return TextFrame(text=text[4:])
        return None


class TestAsyncGeneratorProcessor(unittest.IsolatedAsyncioTestCase):
    async def test_frames_pass_through(self):
        """Test that frames pass through the processor unchanged."""
        serializer = MockTextSerializer()
        processor = AsyncGeneratorProcessor(serializer=serializer)

        frames_to_send = [TextFrame(text="Hello")]
        expected_down_frames = [TextFrame]

        (received_down, _) = await run_test(
            processor,
            frames_to_send=frames_to_send,
            expected_down_frames=expected_down_frames,
        )

        assert received_down[0].text == "Hello"

    async def test_generator_produces_serialized_data(self):
        """Test that generator produces serialized frame data."""
        serializer = MockTextSerializer()
        processor = AsyncGeneratorProcessor(serializer=serializer)

        collected_data = []

        async def collect_data():
            async for data in processor.generator():
                collected_data.append(data)

        # Run generator collection and frame sending concurrently
        async def send_frames():
            # Small delay to ensure generator is running
            await asyncio.sleep(0.01)
            from pipecat.processors.frame_processor import FrameDirection

            await processor.process_frame(TextFrame(text="Test1"), FrameDirection.DOWNSTREAM)
            await processor.process_frame(TextFrame(text="Test2"), FrameDirection.DOWNSTREAM)
            await processor.process_frame(EndFrame(), FrameDirection.DOWNSTREAM)

        await asyncio.gather(collect_data(), send_frames())

        assert "TEXT:Test1" in collected_data
        assert "TEXT:Test2" in collected_data

    async def test_generator_stops_on_end_frame(self):
        """Test that generator stops when EndFrame is processed."""
        serializer = MockTextSerializer()
        processor = AsyncGeneratorProcessor(serializer=serializer)

        generator_finished = False

        async def run_generator():
            nonlocal generator_finished
            async for _ in processor.generator():
                pass
            generator_finished = True

        async def send_frames():
            await asyncio.sleep(0.01)
            from pipecat.processors.frame_processor import FrameDirection

            await processor.process_frame(EndFrame(), FrameDirection.DOWNSTREAM)

        await asyncio.gather(run_generator(), send_frames())

        assert generator_finished

    async def test_generator_stops_on_cancel_frame(self):
        """Test that generator stops when CancelFrame is processed."""
        serializer = MockTextSerializer()
        processor = AsyncGeneratorProcessor(serializer=serializer)

        generator_finished = False

        async def run_generator():
            nonlocal generator_finished
            async for _ in processor.generator():
                pass
            generator_finished = True

        async def send_frames():
            await asyncio.sleep(0.01)
            from pipecat.processors.frame_processor import FrameDirection

            await processor.process_frame(CancelFrame(), FrameDirection.DOWNSTREAM)

        await asyncio.gather(run_generator(), send_frames())

        assert generator_finished

    async def test_binary_serializer(self):
        """Test processor with binary serializer."""
        serializer = MockBinarySerializer()
        processor = AsyncGeneratorProcessor(serializer=serializer)

        collected_data = []

        async def collect_data():
            async for data in processor.generator():
                collected_data.append(data)

        async def send_frames():
            await asyncio.sleep(0.01)
            from pipecat.processors.frame_processor import FrameDirection

            await processor.process_frame(TextFrame(text="Binary"), FrameDirection.DOWNSTREAM)
            await processor.process_frame(EndFrame(), FrameDirection.DOWNSTREAM)

        await asyncio.gather(collect_data(), send_frames())

        assert b"BIN:Binary" in collected_data

    async def test_non_serializable_frames_skipped(self):
        """Test that frames that serialize to None are skipped."""
        serializer = MockTextSerializer()
        processor = AsyncGeneratorProcessor(serializer=serializer)

        collected_data = []

        async def collect_data():
            async for data in processor.generator():
                collected_data.append(data)

        async def send_frames():
            await asyncio.sleep(0.01)
            from pipecat.processors.frame_processor import FrameDirection
            from pipecat.frames.frames import UserStartedSpeakingFrame

            # UserStartedSpeakingFrame is not serialized by MockTextSerializer
            await processor.process_frame(
                UserStartedSpeakingFrame(), FrameDirection.DOWNSTREAM
            )
            await processor.process_frame(TextFrame(text="Valid"), FrameDirection.DOWNSTREAM)
            await processor.process_frame(EndFrame(), FrameDirection.DOWNSTREAM)

        await asyncio.gather(collect_data(), send_frames())

        # Should only have the TextFrame serialization
        assert len(collected_data) == 1
        assert collected_data[0] == "TEXT:Valid"

    async def test_multiple_frames_ordering(self):
        """Test that frames are produced in order."""
        serializer = MockTextSerializer()
        processor = AsyncGeneratorProcessor(serializer=serializer)

        collected_data = []

        async def collect_data():
            async for data in processor.generator():
                collected_data.append(data)

        async def send_frames():
            await asyncio.sleep(0.01)
            from pipecat.processors.frame_processor import FrameDirection

            for i in range(5):
                await processor.process_frame(
                    TextFrame(text=f"Frame{i}"), FrameDirection.DOWNSTREAM
                )
            await processor.process_frame(EndFrame(), FrameDirection.DOWNSTREAM)

        await asyncio.gather(collect_data(), send_frames())

        expected = [f"TEXT:Frame{i}" for i in range(5)]
        assert collected_data == expected
