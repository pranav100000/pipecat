#
# Copyright (c) 2024-2025 Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import asyncio
import unittest

from pipecat.frames.frames import EndFrame, TextFrame
from pipecat.observers.base_observer import BaseObserver, FrameProcessed, FramePushed
from pipecat.pipeline.base_task import PipelineTaskParams
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.task import PipelineTask
from pipecat.processors.filters.identity_filter import IdentityFilter
from pipecat.processors.frame_processor import FrameDirection


class TestFrameProcessed(unittest.IsolatedAsyncioTestCase):
    async def test_frame_processed_dataclass(self):
        """Test FrameProcessed dataclass creation."""
        processor = IdentityFilter()
        frame = TextFrame(text="Test")

        data = FrameProcessed(
            processor=processor,
            frame=frame,
            direction=FrameDirection.DOWNSTREAM,
            timestamp=12345,
        )

        assert data.processor == processor
        assert data.frame == frame
        assert data.direction == FrameDirection.DOWNSTREAM
        assert data.timestamp == 12345


class TestFramePushed(unittest.IsolatedAsyncioTestCase):
    async def test_frame_pushed_dataclass(self):
        """Test FramePushed dataclass creation."""
        source = IdentityFilter()
        destination = IdentityFilter()
        frame = TextFrame(text="Test")

        data = FramePushed(
            source=source,
            destination=destination,
            frame=frame,
            direction=FrameDirection.DOWNSTREAM,
            timestamp=12345,
        )

        assert data.source == source
        assert data.destination == destination
        assert data.frame == frame
        assert data.direction == FrameDirection.DOWNSTREAM
        assert data.timestamp == 12345


class TestBaseObserver(unittest.IsolatedAsyncioTestCase):
    async def test_observer_receives_push_events(self):
        """Test that observer receives frame push events."""
        push_events = []

        class TestObserver(BaseObserver):
            async def on_push_frame(self, data: FramePushed):
                push_events.append(data)

        identity = IdentityFilter()
        pipeline = Pipeline([identity])
        observer = TestObserver()
        task = PipelineTask(pipeline, observers=[observer])

        await task.queue_frames([TextFrame(text="Hello"), EndFrame()])
        await task.run(PipelineTaskParams(loop=asyncio.get_event_loop()))

        # Should have received push events
        text_pushes = [e for e in push_events if isinstance(e.frame, TextFrame)]
        assert len(text_pushes) > 0

    async def test_observer_receives_process_events(self):
        """Test that observer receives frame process events."""
        process_events = []

        class TestObserver(BaseObserver):
            async def on_process_frame(self, data: FrameProcessed):
                process_events.append(data)

        identity = IdentityFilter()
        pipeline = Pipeline([identity])
        observer = TestObserver()
        task = PipelineTask(pipeline, observers=[observer])

        await task.queue_frames([TextFrame(text="Hello"), EndFrame()])
        await task.run(PipelineTaskParams(loop=asyncio.get_event_loop()))

        # Should have received process events
        text_processes = [e for e in process_events if isinstance(e.frame, TextFrame)]
        assert len(text_processes) > 0

    async def test_observer_with_multiple_processors(self):
        """Test observer with multiple processors in pipeline."""
        push_events = []

        class TestObserver(BaseObserver):
            async def on_push_frame(self, data: FramePushed):
                push_events.append(data)

        identity1 = IdentityFilter()
        identity2 = IdentityFilter()
        pipeline = Pipeline([identity1, identity2])
        observer = TestObserver()
        task = PipelineTask(pipeline, observers=[observer])

        await task.queue_frames([TextFrame(text="Test"), EndFrame()])
        await task.run(PipelineTaskParams(loop=asyncio.get_event_loop()))

        # Should see pushes from multiple processors
        assert len(push_events) > 0

    async def test_multiple_observers(self):
        """Test that multiple observers all receive events."""
        events1 = []
        events2 = []

        class Observer1(BaseObserver):
            async def on_push_frame(self, data: FramePushed):
                events1.append(data)

        class Observer2(BaseObserver):
            async def on_push_frame(self, data: FramePushed):
                events2.append(data)

        identity = IdentityFilter()
        pipeline = Pipeline([identity])
        task = PipelineTask(pipeline, observers=[Observer1(), Observer2()])

        await task.queue_frames([TextFrame(text="Test"), EndFrame()])
        await task.run(PipelineTaskParams(loop=asyncio.get_event_loop()))

        # Both observers should receive events
        assert len(events1) > 0
        assert len(events2) > 0

    async def test_observer_default_methods_no_op(self):
        """Test that default observer methods are no-ops."""
        observer = BaseObserver()

        # Should not raise any exceptions
        await observer.on_push_frame(
            FramePushed(
                source=IdentityFilter(),
                destination=IdentityFilter(),
                frame=TextFrame(text="Test"),
                direction=FrameDirection.DOWNSTREAM,
                timestamp=0,
            )
        )
        await observer.on_process_frame(
            FrameProcessed(
                processor=IdentityFilter(),
                frame=TextFrame(text="Test"),
                direction=FrameDirection.DOWNSTREAM,
                timestamp=0,
            )
        )

    async def test_observer_tracks_frame_direction(self):
        """Test that observer correctly tracks frame direction."""
        downstream_events = []
        upstream_events = []

        class DirectionObserver(BaseObserver):
            async def on_push_frame(self, data: FramePushed):
                if data.direction == FrameDirection.DOWNSTREAM:
                    downstream_events.append(data)
                else:
                    upstream_events.append(data)

        identity = IdentityFilter()
        pipeline = Pipeline([identity])
        observer = DirectionObserver()
        task = PipelineTask(pipeline, observers=[observer])

        await task.queue_frames([TextFrame(text="Down"), EndFrame()])
        await task.run(PipelineTaskParams(loop=asyncio.get_event_loop()))

        # Should have downstream events (at minimum)
        assert len(downstream_events) > 0

    async def test_observer_receives_timestamp(self):
        """Test that observer events include timestamps."""
        timestamps = []

        class TimestampObserver(BaseObserver):
            async def on_push_frame(self, data: FramePushed):
                timestamps.append(data.timestamp)

        identity = IdentityFilter()
        pipeline = Pipeline([identity])
        observer = TimestampObserver()
        task = PipelineTask(pipeline, observers=[observer])

        await task.queue_frames([TextFrame(text="Test"), EndFrame()])
        await task.run(PipelineTaskParams(loop=asyncio.get_event_loop()))

        # Should have received timestamps
        assert len(timestamps) > 0
        # Timestamps should be integers
        for ts in timestamps:
            assert isinstance(ts, int)
