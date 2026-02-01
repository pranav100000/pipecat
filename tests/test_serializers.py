#
# Copyright (c) 2024-2025 Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import unittest
from typing import Optional

from pipecat.frames.frames import Frame, StartFrame, TextFrame
from pipecat.serializers.base_serializer import FrameSerializer, FrameSerializerType


class TextSerializer(FrameSerializer):
    """Concrete text serializer for testing."""

    @property
    def type(self) -> FrameSerializerType:
        return FrameSerializerType.TEXT

    async def serialize(self, frame: Frame) -> Optional[str]:
        if isinstance(frame, TextFrame):
            return f"text:{frame.text}"
        return None

    async def deserialize(self, data: str) -> Optional[Frame]:
        if data.startswith("text:"):
            return TextFrame(text=data[5:])
        return None


class BinarySerializer(FrameSerializer):
    """Concrete binary serializer for testing."""

    @property
    def type(self) -> FrameSerializerType:
        return FrameSerializerType.BINARY

    async def serialize(self, frame: Frame) -> Optional[bytes]:
        if isinstance(frame, TextFrame):
            return frame.text.encode("utf-8")
        return None

    async def deserialize(self, data: bytes) -> Optional[Frame]:
        try:
            return TextFrame(text=data.decode("utf-8"))
        except Exception:
            return None


class TestFrameSerializerType(unittest.IsolatedAsyncioTestCase):
    async def test_binary_type(self):
        """Test BINARY serializer type."""
        assert FrameSerializerType.BINARY.value == "binary"

    async def test_text_type(self):
        """Test TEXT serializer type."""
        assert FrameSerializerType.TEXT.value == "text"

    async def test_enum_members(self):
        """Test that both types are available as enum members."""
        members = list(FrameSerializerType)
        assert FrameSerializerType.BINARY in members
        assert FrameSerializerType.TEXT in members
        assert len(members) == 2


class TestTextSerializer(unittest.IsolatedAsyncioTestCase):
    async def test_type_property(self):
        """Test that text serializer returns TEXT type."""
        serializer = TextSerializer()
        assert serializer.type == FrameSerializerType.TEXT

    async def test_serialize_text_frame(self):
        """Test serializing a TextFrame."""
        serializer = TextSerializer()
        frame = TextFrame(text="Hello World")
        result = await serializer.serialize(frame)
        assert result == "text:Hello World"

    async def test_serialize_non_text_frame(self):
        """Test serializing a non-TextFrame returns None."""
        serializer = TextSerializer()
        frame = StartFrame()
        result = await serializer.serialize(frame)
        assert result is None

    async def test_deserialize_text_data(self):
        """Test deserializing text data."""
        serializer = TextSerializer()
        result = await serializer.deserialize("text:Test Message")
        assert isinstance(result, TextFrame)
        assert result.text == "Test Message"

    async def test_deserialize_invalid_data(self):
        """Test deserializing invalid data returns None."""
        serializer = TextSerializer()
        result = await serializer.deserialize("invalid:data")
        assert result is None

    async def test_roundtrip(self):
        """Test serialize/deserialize roundtrip."""
        serializer = TextSerializer()
        original = TextFrame(text="Roundtrip Test")
        serialized = await serializer.serialize(original)
        deserialized = await serializer.deserialize(serialized)
        assert isinstance(deserialized, TextFrame)
        assert deserialized.text == original.text

    async def test_empty_text(self):
        """Test serializing/deserializing empty text."""
        serializer = TextSerializer()
        original = TextFrame(text="")
        serialized = await serializer.serialize(original)
        assert serialized == "text:"
        deserialized = await serializer.deserialize(serialized)
        assert deserialized.text == ""

    async def test_special_characters(self):
        """Test handling special characters."""
        serializer = TextSerializer()
        original = TextFrame(text="Hello\nWorld\t!")
        serialized = await serializer.serialize(original)
        deserialized = await serializer.deserialize(serialized)
        assert deserialized.text == "Hello\nWorld\t!"


class TestBinarySerializer(unittest.IsolatedAsyncioTestCase):
    async def test_type_property(self):
        """Test that binary serializer returns BINARY type."""
        serializer = BinarySerializer()
        assert serializer.type == FrameSerializerType.BINARY

    async def test_serialize_text_frame(self):
        """Test serializing a TextFrame to bytes."""
        serializer = BinarySerializer()
        frame = TextFrame(text="Hello Binary")
        result = await serializer.serialize(frame)
        assert result == b"Hello Binary"

    async def test_serialize_non_text_frame(self):
        """Test serializing a non-TextFrame returns None."""
        serializer = BinarySerializer()
        frame = StartFrame()
        result = await serializer.serialize(frame)
        assert result is None

    async def test_deserialize_bytes(self):
        """Test deserializing bytes data."""
        serializer = BinarySerializer()
        result = await serializer.deserialize(b"Test Message")
        assert isinstance(result, TextFrame)
        assert result.text == "Test Message"

    async def test_roundtrip(self):
        """Test serialize/deserialize roundtrip."""
        serializer = BinarySerializer()
        original = TextFrame(text="Binary Roundtrip")
        serialized = await serializer.serialize(original)
        deserialized = await serializer.deserialize(serialized)
        assert isinstance(deserialized, TextFrame)
        assert deserialized.text == original.text

    async def test_unicode_characters(self):
        """Test handling unicode characters."""
        serializer = BinarySerializer()
        original = TextFrame(text="Hello 世界 🌍")
        serialized = await serializer.serialize(original)
        deserialized = await serializer.deserialize(serialized)
        assert deserialized.text == "Hello 世界 🌍"


class TestFrameSerializerSetup(unittest.IsolatedAsyncioTestCase):
    async def test_setup_default_is_noop(self):
        """Test that default setup method is a no-op."""
        serializer = TextSerializer()
        # Should not raise any exception
        await serializer.setup(StartFrame())
