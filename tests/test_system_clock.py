#
# Copyright (c) 2024-2025 Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import time
import unittest

from pipecat.clocks.base_clock import BaseClock
from pipecat.clocks.system_clock import SystemClock


class TestSystemClock(unittest.IsolatedAsyncioTestCase):
    async def test_inherits_from_base_clock(self):
        """Test that SystemClock inherits from BaseClock."""
        clock = SystemClock()
        assert isinstance(clock, BaseClock)

    async def test_initial_state(self):
        """Test clock returns 0 before being started."""
        clock = SystemClock()
        assert clock.get_time() == 0

    async def test_start_clock(self):
        """Test that clock starts correctly."""
        clock = SystemClock()
        clock.start()
        # Should return a positive value after starting
        time.sleep(0.001)  # Small delay to ensure time has passed
        elapsed = clock.get_time()
        assert elapsed > 0

    async def test_elapsed_time(self):
        """Test that clock measures elapsed time correctly."""
        clock = SystemClock()
        clock.start()

        time.sleep(0.1)  # Sleep for 100ms
        elapsed = clock.get_time()

        # Should be approximately 100 million nanoseconds (100ms)
        # Allow some tolerance for system timing
        assert elapsed >= 90_000_000  # At least 90ms
        assert elapsed <= 200_000_000  # At most 200ms

    async def test_monotonic_increasing(self):
        """Test that clock values are monotonically increasing."""
        clock = SystemClock()
        clock.start()

        times = []
        for _ in range(5):
            times.append(clock.get_time())
            time.sleep(0.01)  # Small delay between readings

        # Each time should be greater than or equal to the previous
        for i in range(1, len(times)):
            assert times[i] >= times[i - 1]

    async def test_returns_nanoseconds(self):
        """Test that clock returns time in nanoseconds."""
        clock = SystemClock()
        clock.start()

        time.sleep(0.001)  # Sleep for 1ms
        elapsed = clock.get_time()

        # 1ms = 1,000,000 nanoseconds, should be at least that
        assert elapsed >= 900_000  # At least 0.9ms in nanoseconds

    async def test_multiple_start_calls(self):
        """Test that calling start multiple times resets the clock."""
        clock = SystemClock()

        clock.start()
        time.sleep(0.05)
        first_elapsed = clock.get_time()

        clock.start()
        # After restart, time should be nearly zero
        time.sleep(0.001)
        second_elapsed = clock.get_time()

        assert first_elapsed > second_elapsed

    async def test_precision(self):
        """Test that clock has reasonable precision."""
        clock = SystemClock()
        clock.start()

        # Take two quick readings
        t1 = clock.get_time()
        t2 = clock.get_time()

        # Should be able to detect small differences
        # The difference should be small but potentially non-zero
        assert t2 >= t1
