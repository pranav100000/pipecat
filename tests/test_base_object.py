#
# Copyright (c) 2024-2025 Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import asyncio
import unittest

from pipecat.utils.base_object import BaseObject, EventHandler


class ConcreteObject(BaseObject):
    """Concrete implementation of BaseObject for testing."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._register_event_handler("on_test_event")
        self._register_event_handler("on_sync_event", sync=True)
        self._register_event_handler("on_another_event")


class TestBaseObject(unittest.IsolatedAsyncioTestCase):
    async def test_unique_id(self):
        """Test that each object gets a unique ID."""
        obj1 = ConcreteObject()
        obj2 = ConcreteObject()
        assert obj1.id != obj2.id

    async def test_auto_generated_name(self):
        """Test that objects get auto-generated names."""
        obj = ConcreteObject()
        assert "ConcreteObject#" in obj.name
        assert str(obj) == obj.name

    async def test_custom_name(self):
        """Test that custom names are respected."""
        obj = ConcreteObject(name="CustomName")
        assert obj.name == "CustomName"
        assert str(obj) == "CustomName"

    async def test_event_handler_decorator(self):
        """Test that event handler decorator works."""
        obj = ConcreteObject()
        event_called = False

        @obj.event_handler("on_test_event")
        async def handler(source):
            nonlocal event_called
            event_called = True

        await obj._call_event_handler("on_test_event")
        # Give time for async task to complete
        await asyncio.sleep(0.1)
        assert event_called

    async def test_add_event_handler(self):
        """Test that add_event_handler works."""
        obj = ConcreteObject()
        event_called = False

        async def handler(source):
            nonlocal event_called
            event_called = True

        obj.add_event_handler("on_test_event", handler)
        await obj._call_event_handler("on_test_event")
        await asyncio.sleep(0.1)
        assert event_called

    async def test_event_handler_with_args(self):
        """Test that event handlers receive arguments."""
        obj = ConcreteObject()
        received_args = []

        @obj.event_handler("on_test_event")
        async def handler(source, *args, **kwargs):
            received_args.extend(args)
            received_args.append(kwargs)

        await obj._call_event_handler("on_test_event", "arg1", "arg2", key="value")
        await asyncio.sleep(0.1)
        assert "arg1" in received_args
        assert "arg2" in received_args
        assert {"key": "value"} in received_args

    async def test_multiple_handlers_same_event(self):
        """Test that multiple handlers can be registered for the same event."""
        obj = ConcreteObject()
        call_count = 0

        @obj.event_handler("on_test_event")
        async def handler1(source):
            nonlocal call_count
            call_count += 1

        @obj.event_handler("on_test_event")
        async def handler2(source):
            nonlocal call_count
            call_count += 1

        await obj._call_event_handler("on_test_event")
        await asyncio.sleep(0.1)
        assert call_count == 2

    async def test_sync_event_handler(self):
        """Test that sync event handlers run synchronously."""
        obj = ConcreteObject()
        execution_order = []

        @obj.event_handler("on_sync_event")
        async def handler(source):
            execution_order.append("handler")

        await obj._call_event_handler("on_sync_event")
        execution_order.append("after_call")

        # For sync handlers, handler should complete before "after_call"
        assert execution_order == ["handler", "after_call"]

    async def test_sync_function_handler(self):
        """Test that regular (non-async) functions work as handlers."""
        obj = ConcreteObject()
        event_called = False

        def sync_handler(source):
            nonlocal event_called
            event_called = True

        obj.add_event_handler("on_sync_event", sync_handler)
        await obj._call_event_handler("on_sync_event")
        assert event_called

    async def test_unregistered_event_warning(self):
        """Test that adding handler to unregistered event logs warning."""
        obj = ConcreteObject()

        async def handler(source):
            pass

        # This should not raise but should log a warning
        obj.add_event_handler("on_nonexistent_event", handler)

    async def test_register_duplicate_event(self):
        """Test that registering same event twice logs warning."""
        obj = ConcreteObject()
        # on_test_event is already registered in __init__
        # Registering it again should log a warning
        obj._register_event_handler("on_test_event")

    async def test_call_unregistered_event(self):
        """Test that calling unregistered event is a no-op."""
        obj = ConcreteObject()
        # Should not raise any exception
        await obj._call_event_handler("on_nonexistent_event")

    async def test_cleanup_waits_for_tasks(self):
        """Test that cleanup waits for running event handler tasks."""
        obj = ConcreteObject()
        task_completed = False

        @obj.event_handler("on_test_event")
        async def slow_handler(source):
            nonlocal task_completed
            await asyncio.sleep(0.2)
            task_completed = True

        await obj._call_event_handler("on_test_event")
        # Task should be running but not completed
        assert not task_completed

        await obj.cleanup()
        # After cleanup, task should be completed
        assert task_completed

    async def test_event_handler_exception_handling(self):
        """Test that exceptions in handlers are caught and logged."""
        obj = ConcreteObject()
        second_handler_called = False

        @obj.event_handler("on_test_event")
        async def failing_handler(source):
            raise ValueError("Test error")

        @obj.event_handler("on_test_event")
        async def second_handler(source):
            nonlocal second_handler_called
            second_handler_called = True

        # Should not raise
        await obj._call_event_handler("on_test_event")
        await asyncio.sleep(0.1)

        # Second handler should still be called
        assert second_handler_called


class TestEventHandler(unittest.IsolatedAsyncioTestCase):
    async def test_event_handler_dataclass(self):
        """Test EventHandler dataclass creation."""
        handler = EventHandler(name="test_event", handlers=[], is_sync=False)
        assert handler.name == "test_event"
        assert handler.handlers == []
        assert handler.is_sync is False

    async def test_event_handler_with_handlers(self):
        """Test EventHandler dataclass with handlers list."""

        async def handler1():
            pass

        async def handler2():
            pass

        handler = EventHandler(name="test_event", handlers=[handler1, handler2], is_sync=True)
        assert len(handler.handlers) == 2
        assert handler.is_sync is True
