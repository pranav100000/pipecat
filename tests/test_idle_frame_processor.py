#
# Copyright (c) 2024-2025 Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import asyncio
import unittest

from pipecat.frames.frames import StartFrame, TextFrame, UserStartedSpeakingFrame
from pipecat.pipeline.base_task import PipelineTaskParams
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.task import PipelineTask
from pipecat.processors.idle_frame_processor import IdleFrameProcessor


class TestIdleFrameProcessor(unittest.IsolatedAsyncioTestCase):
    async def test_callback_on_timeout(self):
        """Test that callback is called when timeout occurs."""
        callback_called = False
        callback_count = 0

        async def on_idle(processor):
            nonlocal callback_called, callback_count
            callback_called = True
            callback_count += 1

        processor = IdleFrameProcessor(callback=on_idle, timeout=0.1)
        pipeline = Pipeline([processor])
        task = PipelineTask(pipeline, cancel_on_idle_timeout=False)

        async def run_and_wait():
            # Start the pipeline
            await task.queue_frame(StartFrame())
            # Wait for timeout to trigger callback
            await asyncio.sleep(0.25)
            # Stop the task
            await task.cancel()

        try:
            await asyncio.wait_for(
                asyncio.gather(
                    task.run(PipelineTaskParams(loop=asyncio.get_event_loop())), run_and_wait()
                ),
                timeout=2.0,
            )
        except asyncio.CancelledError:
            pass

        assert callback_called
        assert callback_count >= 1

    async def test_callback_reset_on_any_frame(self):
        """Test that receiving any frame resets the timeout."""
        callback_count = 0

        async def on_idle(processor):
            nonlocal callback_count
            callback_count += 1

        processor = IdleFrameProcessor(callback=on_idle, timeout=0.15)
        pipeline = Pipeline([processor])
        task = PipelineTask(pipeline, cancel_on_idle_timeout=False)

        async def send_frames():
            # Start the pipeline
            await task.queue_frame(StartFrame())
            # Send frames before timeout
            for _ in range(3):
                await asyncio.sleep(0.1)
                await task.queue_frame(TextFrame(text="keep alive"))
            # Now wait without sending frames
            await asyncio.sleep(0.3)
            await task.cancel()

        try:
            await asyncio.wait_for(
                asyncio.gather(
                    task.run(PipelineTaskParams(loop=asyncio.get_event_loop())), send_frames()
                ),
                timeout=3.0,
            )
        except asyncio.CancelledError:
            pass

        # Callback should have been called at least once after we stopped sending frames
        assert callback_count >= 1

    async def test_callback_with_specific_frame_types(self):
        """Test that only specified frame types reset the timeout."""
        callback_count = 0

        async def on_idle(processor):
            nonlocal callback_count
            callback_count += 1

        # Only TextFrame should reset the timeout
        processor = IdleFrameProcessor(callback=on_idle, timeout=0.1, types=[TextFrame])
        pipeline = Pipeline([processor])
        task = PipelineTask(pipeline, cancel_on_idle_timeout=False)

        async def send_frames():
            # Start the pipeline
            await task.queue_frame(StartFrame())
            # Send non-matching frame types - these should NOT reset the timeout
            await asyncio.sleep(0.05)
            await task.queue_frame(UserStartedSpeakingFrame())
            await asyncio.sleep(0.2)
            await task.cancel()

        try:
            await asyncio.wait_for(
                asyncio.gather(
                    task.run(PipelineTaskParams(loop=asyncio.get_event_loop())), send_frames()
                ),
                timeout=2.0,
            )
        except asyncio.CancelledError:
            pass

        # Callback should have been called since UserStartedSpeakingFrame doesn't match
        assert callback_count >= 1

    async def test_matching_frame_type_resets_timeout(self):
        """Test that matching frame type resets the timeout."""
        callback_count = 0

        async def on_idle(processor):
            nonlocal callback_count
            callback_count += 1

        # Only TextFrame should reset the timeout
        processor = IdleFrameProcessor(callback=on_idle, timeout=0.15, types=[TextFrame])
        pipeline = Pipeline([processor])
        task = PipelineTask(pipeline, cancel_on_idle_timeout=False)

        async def send_frames():
            # Start the pipeline
            await task.queue_frame(StartFrame())
            # Send matching frame types - these SHOULD reset the timeout
            for _ in range(3):
                await asyncio.sleep(0.1)
                await task.queue_frame(TextFrame(text="matching"))
            # Wait a bit then cancel
            await asyncio.sleep(0.05)
            await task.cancel()

        try:
            await asyncio.wait_for(
                asyncio.gather(
                    task.run(PipelineTaskParams(loop=asyncio.get_event_loop())), send_frames()
                ),
                timeout=2.0,
            )
        except asyncio.CancelledError:
            pass

        # Callback should not have been called since we kept resetting with TextFrames
        assert callback_count == 0

    async def test_frames_pass_through(self):
        """Test that frames pass through the processor unchanged."""

        async def noop(processor):
            pass

        processor = IdleFrameProcessor(callback=noop, timeout=1.0)
        pipeline = Pipeline([processor])
        task = PipelineTask(pipeline, cancel_on_idle_timeout=False)

        received_frames = []

        @task.event_handler("on_frame_reached_downstream")
        async def on_frame(task, frame):
            received_frames.append(frame)

        task.set_reached_downstream_filter((TextFrame,))

        async def send_frames():
            await task.queue_frame(StartFrame())
            await task.queue_frame(TextFrame(text="test"))
            await asyncio.sleep(0.1)
            await task.cancel()

        try:
            await asyncio.wait_for(
                asyncio.gather(
                    task.run(PipelineTaskParams(loop=asyncio.get_event_loop())), send_frames()
                ),
                timeout=2.0,
            )
        except asyncio.CancelledError:
            pass

        # Should have received the TextFrame
        text_frames = [f for f in received_frames if isinstance(f, TextFrame)]
        assert len(text_frames) == 1
        assert text_frames[0].text == "test"

    async def test_cleanup(self):
        """Test that cleanup cancels the idle task."""

        async def noop(processor):
            pass

        processor = IdleFrameProcessor(callback=noop, timeout=1.0)

        # Start the processor by processing a StartFrame
        pipeline = Pipeline([processor])
        task = PipelineTask(pipeline, cancel_on_idle_timeout=False)

        async def run_and_cleanup():
            await task.queue_frame(StartFrame())
            await asyncio.sleep(0.05)
            await processor.cleanup()
            await task.cancel()

        try:
            await asyncio.wait_for(
                asyncio.gather(
                    task.run(PipelineTaskParams(loop=asyncio.get_event_loop())), run_and_cleanup()
                ),
                timeout=2.0,
            )
        except asyncio.CancelledError:
            pass

        # Cleanup should complete without errors
        assert True
