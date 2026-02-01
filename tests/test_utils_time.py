#
# Copyright (c) 2024-2025 Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import datetime
import unittest

from pipecat.utils.time import (
    nanoseconds_to_seconds,
    nanoseconds_to_str,
    seconds_to_nanoseconds,
    time_now_iso8601,
)


class TestUtilsTime(unittest.IsolatedAsyncioTestCase):
    async def test_seconds_to_nanoseconds(self):
        # Test basic conversions
        assert seconds_to_nanoseconds(1) == 1_000_000_000
        assert seconds_to_nanoseconds(0) == 0
        assert seconds_to_nanoseconds(0.5) == 500_000_000
        assert seconds_to_nanoseconds(0.001) == 1_000_000  # 1 millisecond
        assert seconds_to_nanoseconds(0.000001) == 1_000  # 1 microsecond
        assert seconds_to_nanoseconds(2.5) == 2_500_000_000

    async def test_nanoseconds_to_seconds(self):
        # Test basic conversions
        assert nanoseconds_to_seconds(1_000_000_000) == 1.0
        assert nanoseconds_to_seconds(0) == 0.0
        assert nanoseconds_to_seconds(500_000_000) == 0.5
        assert nanoseconds_to_seconds(1_000_000) == 0.001  # 1 millisecond
        assert nanoseconds_to_seconds(1_000) == 0.000001  # 1 microsecond
        assert nanoseconds_to_seconds(2_500_000_000) == 2.5

    async def test_seconds_nanoseconds_roundtrip(self):
        # Test that conversions are inverse of each other
        original_seconds = 3.14159
        converted = nanoseconds_to_seconds(seconds_to_nanoseconds(original_seconds))
        assert abs(converted - original_seconds) < 1e-9

        original_nanoseconds = 1_234_567_890
        converted = seconds_to_nanoseconds(nanoseconds_to_seconds(original_nanoseconds))
        assert converted == original_nanoseconds

    async def test_nanoseconds_to_str_basic(self):
        # Test basic time formatting
        assert nanoseconds_to_str(0) == "0:00:00.000000"
        assert nanoseconds_to_str(1_000_000_000) == "0:00:01.000000"  # 1 second
        assert nanoseconds_to_str(60_000_000_000) == "0:01:00.000000"  # 1 minute
        assert nanoseconds_to_str(3600_000_000_000) == "1:00:00.000000"  # 1 hour

    async def test_nanoseconds_to_str_with_microseconds(self):
        # Test microsecond precision
        assert nanoseconds_to_str(500_000) == "0:00:00.000500"  # 500 microseconds
        assert nanoseconds_to_str(1_500_000_000) == "0:00:01.500000"  # 1.5 seconds
        assert nanoseconds_to_str(123_456_789) == "0:00:00.123456"

    async def test_nanoseconds_to_str_complex(self):
        # Test complex time values
        # 1 hour, 30 minutes, 45 seconds, 123456 microseconds
        total_ns = (
            1 * 3600_000_000_000  # 1 hour
            + 30 * 60_000_000_000  # 30 minutes
            + 45 * 1_000_000_000  # 45 seconds
            + 123_456_000  # 123456 microseconds
        )
        assert nanoseconds_to_str(total_ns) == "1:30:45.123456"

    async def test_nanoseconds_to_str_multiple_hours(self):
        # Test values over an hour
        # 25 hours, 59 minutes, 59 seconds
        total_ns = 25 * 3600_000_000_000 + 59 * 60_000_000_000 + 59 * 1_000_000_000
        assert nanoseconds_to_str(total_ns) == "25:59:59.000000"

    async def test_time_now_iso8601_format(self):
        # Test that the returned string is valid ISO8601 format
        result = time_now_iso8601()

        # Should be able to parse the result
        parsed = datetime.datetime.fromisoformat(result)
        assert parsed is not None

        # Should have UTC timezone
        assert parsed.tzinfo == datetime.timezone.utc

        # Should be close to current time
        now = datetime.datetime.now(datetime.timezone.utc)
        diff = abs((now - parsed).total_seconds())
        assert diff < 2  # Should be within 2 seconds

    async def test_time_now_iso8601_contains_milliseconds(self):
        # Test that the format includes millisecond precision
        result = time_now_iso8601()

        # ISO8601 with milliseconds should have a period followed by digits before timezone
        assert "." in result
        # The format should include timezone indicator
        assert "+" in result or "Z" in result
