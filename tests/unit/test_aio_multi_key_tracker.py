#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Tests for how AsyncOpenedConnectionTracker keys its connections.

An instance endpoint names the instance, so one key is enough. Any other
endpoint may be a custom domain name, so the connection is tracked under both
the host id and the endpoint, and an invalidation naming either one finds it.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.aio.aurora_connection_tracker import \
    AsyncOpenedConnectionTracker
from aws_advanced_python_wrapper.hostinfo import HostInfo

INSTANCE = "instance-1.abcd.us-east-1.rds.amazonaws.com"
CUSTOM = "db.example.com"


@pytest.fixture(autouse=True)
def _reset_tracker():
    AsyncOpenedConnectionTracker._tracked.clear()
    yield
    AsyncOpenedConnectionTracker._tracked.clear()


def _keys_holding(conn) -> set:
    return {
        key for key, conn_set in AsyncOpenedConnectionTracker._tracked.items()
        if conn in list(conn_set)}


def test_an_instance_endpoint_is_tracked_under_one_key() -> None:
    tracker = AsyncOpenedConnectionTracker()
    conn = MagicMock()
    tracker.track(HostInfo(host=INSTANCE, port=5432, host_id="instance-1"), conn)

    assert _keys_holding(conn) == {f"{INSTANCE}:5432"}


def test_any_other_endpoint_is_tracked_under_the_host_id_and_the_endpoint() -> None:
    tracker = AsyncOpenedConnectionTracker()
    conn = MagicMock()
    tracker.track(HostInfo(host=CUSTOM, port=5432, host_id="instance-1"), conn)

    assert _keys_holding(conn) == {"instance-1", f"{CUSTOM}:5432"}


def test_a_host_without_a_host_id_is_tracked_under_the_endpoint_alone() -> None:
    tracker = AsyncOpenedConnectionTracker()
    conn = MagicMock()
    tracker.track(HostInfo(host=CUSTOM, port=5432), conn)

    assert _keys_holding(conn) == {f"{CUSTOM}:5432"}


def test_invalidate_reaches_a_connection_through_its_host_id() -> None:
    """The endpoint a connection was opened against can differ from the one a
    later invalidation names; the host id is what they share."""
    tracker = AsyncOpenedConnectionTracker()

    class _Conn:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    conn = _Conn()
    tracker.track(HostInfo(host=CUSTOM, port=5432, host_id="instance-1"), conn)

    asyncio.run(tracker.invalidate_all(
        HostInfo(host=INSTANCE, port=5432, host_id="instance-1")))

    assert conn.closed is True


def test_invalidate_closes_each_connection_exactly_once() -> None:
    tracker = AsyncOpenedConnectionTracker()

    class _Conn:
        def __init__(self) -> None:
            self.close_count = 0

        def close(self) -> None:
            self.close_count += 1

    conn = _Conn()
    host = HostInfo(host=CUSTOM, port=5432, host_id="instance-1")
    tracker.track(host, conn)
    asyncio.run(tracker.invalidate_all(host))

    assert conn.close_count == 1


def test_invalidate_swallows_close_errors() -> None:
    tracker = AsyncOpenedConnectionTracker()

    class _BadConn:
        def close(self) -> None:
            raise RuntimeError("boom")

    host = HostInfo(host=INSTANCE, port=5432)
    tracker.track(host, _BadConn())
    # Must not raise.
    asyncio.run(tracker.invalidate_all(host))


def test_remove_clears_the_connection_from_every_key() -> None:
    tracker = AsyncOpenedConnectionTracker()
    conn = MagicMock()
    host = HostInfo(host=CUSTOM, port=5432, host_id="instance-1")
    tracker.track(host, conn)
    tracker.remove(host, conn)

    assert _keys_holding(conn) == set()
