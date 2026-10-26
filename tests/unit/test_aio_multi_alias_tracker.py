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

"""Tests for multi-alias invalidation in AsyncOpenedConnectionTracker (N.2).

Fixes the prior canonical-key-only semantics: a tracked connection is
now reachable from ANY alias in host_info.as_aliases(), so an
invalidate_all() call via any alias closes every peer connection to
the same underlying instance.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.aio.aurora_connection_tracker import \
    AsyncOpenedConnectionTracker
from aws_advanced_python_wrapper.hostinfo import HostInfo


@pytest.fixture(autouse=True)
def _reset_tracker():
    AsyncOpenedConnectionTracker._tracked.clear()
    yield
    AsyncOpenedConnectionTracker._tracked.clear()


def _host_with_aliases(*aliases: str) -> HostInfo:
    """Build a HostInfo whose as_aliases() returns ``aliases``."""
    host = HostInfo(host=aliases[0] if aliases else "h", port=5432)
    for alias in aliases[1:]:
        host.add_alias(alias)
    return host


def test_track_records_under_every_alias() -> None:
    tracker = AsyncOpenedConnectionTracker()
    conn = MagicMock()
    host = _host_with_aliases(
        "instance-1.abcd.us-east-1.rds.amazonaws.com",
        "cluster.abcd.us-east-1.rds.amazonaws.com",
    )
    tracker.track(host, conn)

    keys = AsyncOpenedConnectionTracker._tracked
    # Both aliases (plus canonical) resolve to this conn.
    matching = [
        k for k, weak_set in keys.items()
        if conn in list(weak_set)
    ]
    assert len(matching) >= 2


def test_invalidate_reaches_conn_via_secondary_alias() -> None:
    """A HostInfo that shares the same secondary alias reaches the same
    tracked connection -- even when its primary host differs from the
    one the connection was tracked under."""
    tracker = AsyncOpenedConnectionTracker()

    class _Conn:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    conn = _Conn()
    shared_alias = "cluster.abcd.us-east-1.rds.amazonaws.com"

    # Track via a HostInfo whose primary is the instance endpoint.
    track_host = _host_with_aliases(
        "instance-1.abcd.us-east-1.rds.amazonaws.com",
        shared_alias,
    )
    tracker.track(track_host, conn)

    # Invalidate via a different HostInfo whose primary is different but
    # whose alias set includes the shared cluster endpoint.
    invalidate_host = _host_with_aliases(
        "instance-7.abcd.us-east-1.rds.amazonaws.com",
        shared_alias,
    )
    asyncio.run(tracker.invalidate_all(invalidate_host))

    assert conn.closed is True


def test_invalidate_closes_each_conn_exactly_once() -> None:
    """Even if a single connection is reachable from multiple aliases,
    invalidate_all must only call close() once per connection."""
    tracker = AsyncOpenedConnectionTracker()

    class _Conn:
        def __init__(self) -> None:
            self.close_count = 0

        def close(self) -> None:
            self.close_count += 1

    conn = _Conn()
    host = _host_with_aliases(
        "instance-1.abcd.us-east-1.rds.amazonaws.com",
        "cluster.abcd.us-east-1.rds.amazonaws.com",
        "cluster-ro.abcd.us-east-1.rds.amazonaws.com",
    )
    tracker.track(host, conn)
    asyncio.run(tracker.invalidate_all(host))

    assert conn.close_count == 1


def test_invalidate_swallows_close_errors() -> None:
    tracker = AsyncOpenedConnectionTracker()

    class _BadConn:
        def close(self) -> None:
            raise RuntimeError("boom")

    host = _host_with_aliases("instance-1.abcd.us-east-1.rds.amazonaws.com")
    tracker.track(host, _BadConn())
    # Must not raise.
    asyncio.run(tracker.invalidate_all(host))


def test_remove_clears_conn_from_every_alias() -> None:
    tracker = AsyncOpenedConnectionTracker()
    conn = MagicMock()
    host = _host_with_aliases(
        "instance-1.abcd.us-east-1.rds.amazonaws.com",
        "cluster.abcd.us-east-1.rds.amazonaws.com",
    )
    tracker.track(host, conn)
    tracker.remove(host, conn)

    for weak_set in AsyncOpenedConnectionTracker._tracked.values():
        assert conn not in list(weak_set)
