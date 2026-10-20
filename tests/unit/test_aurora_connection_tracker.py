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

from __future__ import annotations

from _weakrefset import WeakSet
from typing import TYPE_CHECKING

import pytest
from sqlalchemy.pool import QueuePool

from aws_advanced_python_wrapper.errors import FailoverError

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.pep249 import Connection

from aws_advanced_python_wrapper.aurora_connection_tracker_plugin import (
    AuroraConnectionTrackerPlugin, OpenedConnectionTracker)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import Properties
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_cursor(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_plugin_service(mocker):
    service = mocker.MagicMock()
    # A MagicMock attribute would stand in for a routed host and send the
    # tracker down the identify path.
    service.routed_host_info = None
    return service


@pytest.fixture
def mock_rds_utils(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_tracker(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_callable(mocker):
    return mocker.MagicMock()


@pytest.fixture
def props():
    return Properties({"host": "instance-1.xyz.us-east-2.rds.amazonaws.com"})


def test_track_new_instance_connection(
        mocker, mock_plugin_service, mock_rds_utils, mock_tracker, mock_cursor, mock_callable,
        mock_conn):
    host_info: HostInfo = HostInfo("instance1")
    mock_plugin_service.all_hosts = [host_info]
    mock_plugin_service.current_host_info = host_info
    mock_rds_utils.is_rds_instance.return_value = True
    mock_rds_utils.identify_rds_type.return_value = RdsUrlType.RDS_INSTANCE
    mock_callable.return_value = mock_conn

    plugin: AuroraConnectionTrackerPlugin = AuroraConnectionTrackerPlugin(mock_plugin_service, Properties(),
                                                                          mock_rds_utils, mock_tracker)

    conn: Connection = plugin.connect(
        mocker.MagicMock(), mocker.MagicMock(), host_info, Properties(), True, mock_callable)
    assert mock_conn == conn
    mock_tracker.populate_opened_connection_set.assert_called_with(host_info, mock_conn)
    # An instance endpoint names the instance, so there is nothing to identify.
    mock_plugin_service.identify_connection.assert_not_called()

    conn = plugin.connect(
        mocker.MagicMock(), mocker.MagicMock(), host_info, Properties(), False, mock_callable)
    assert mock_conn == conn
    mock_tracker.populate_opened_connection_set.assert_called_with(host_info, mock_conn)
    mock_plugin_service.identify_connection.assert_not_called()


def test_connect_tracks_a_cluster_connection_under_its_identified_host(
        mocker, mock_plugin_service, mock_rds_utils, mock_tracker, mock_callable, mock_conn):
    """A cluster endpoint does not name the instance the connection landed on,
    so the tracker needs the identified host to key it correctly."""
    cluster_host = HostInfo("mydb.cluster-abc.us-east-2.rds.amazonaws.com", 5432)
    identified = HostInfo("instance-1.abc.us-east-2.rds.amazonaws.com", 5432,
                          host_id="instance-1")
    mock_rds_utils.identify_rds_type.return_value = RdsUrlType.RDS_WRITER_CLUSTER
    mock_plugin_service.identify_connection.return_value = identified
    mock_callable.return_value = mock_conn

    plugin = AuroraConnectionTrackerPlugin(
        mock_plugin_service, Properties(), mock_rds_utils, mock_tracker)
    plugin.connect(
        mocker.MagicMock(), mocker.MagicMock(), cluster_host, Properties(), True, mock_callable)

    mock_tracker.populate_opened_connection_set.assert_called_with(identified, mock_conn)
    assert mock_plugin_service.routed_host_info is identified


def test_invalidate_opened_connections(
        mocker, mock_plugin_service, mock_rds_utils, mock_tracker, mock_cursor, mock_callable):
    expected_exception = FailoverError("exception")
    original_host = HostInfo("host")
    new_host = HostInfo("new-host")
    mock_callable.side_effect = expected_exception
    mock_hosts_prop = mocker.PropertyMock(side_effect=[(original_host,), (new_host,)])
    type(mock_plugin_service).all_hosts = mock_hosts_prop

    plugin: AuroraConnectionTrackerPlugin = AuroraConnectionTrackerPlugin(
        mock_plugin_service, Properties(), mock_rds_utils, mock_tracker)

    with pytest.raises(FailoverError):
        plugin.execute(mock_cursor, "Cursor.execute", mock_callable, ("select 1", {}))

    mock_tracker.invalidate_current_connection.assert_not_called()
    mock_tracker.invalidate_all_connections.assert_called_with(host_info=original_host)


def test_prune_connections_logs_none_connection(mocker, caplog):
    """Test that pruning None connections logs the expected message."""

    tracker = OpenedConnectionTracker()
    mock_conn_set = mocker.MagicMock(spec=WeakSet)
    mock_conn_set.__iter__ = mocker.MagicMock(return_value=iter([None]))
    mock_conn_set.__len__ = mocker.MagicMock(return_value=1)
    mock_conn_set.discard = mocker.MagicMock()

    OpenedConnectionTracker._opened_connections = {"test-host": mock_conn_set}

    with caplog.at_level("DEBUG"):
        tracker._prune_connections()

    mock_conn_set.discard.assert_called_with(None)


def test_prune_connections_logs_closed_connection(mocker, caplog):
    """Test that pruning closed connections logs the connection class name."""

    mock_conn = mocker.MagicMock()
    mock_conn.__module__ = "psycopg"
    mock_conn.is_closed.return_value = True

    mock_conn_set = mocker.MagicMock(spec=WeakSet)
    mock_conn_set.__iter__ = mocker.MagicMock(return_value=iter([mock_conn]))
    mock_conn_set.__len__ = mocker.MagicMock(return_value=1)
    mock_conn_set.discard = mocker.MagicMock()

    tracker = OpenedConnectionTracker()
    OpenedConnectionTracker._opened_connections = {"test-host": mock_conn_set}

    with caplog.at_level("DEBUG"):
        tracker._prune_connections()

    mock_conn_set.discard.assert_called_with(mock_conn)


def test_invalidate_all_connections_drains_set(mocker):
    tracker = OpenedConnectionTracker()
    OpenedConnectionTracker._opened_connections = {}

    mocker.patch.object(OpenedConnectionTracker, "_start_prune_thread")
    mock_rds = mocker.MagicMock()
    mock_rds.is_rds_instance.return_value = True
    mock_rds.remove_port.side_effect = lambda value: value
    mocker.patch.object(OpenedConnectionTracker, "_rds_utils", mock_rds)

    captured = []
    mocker.patch.object(
        OpenedConnectionTracker,
        "_invalidate_connections",
        side_effect=lambda connections_list: captured.append(list(connections_list)))

    host_info = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com")
    conn1 = mocker.MagicMock()
    conn2 = mocker.MagicMock()
    tracker.populate_opened_connection_set(host_info, conn1)
    tracker.populate_opened_connection_set(host_info, conn2)

    tracker.invalidate_all_connections(host_info=host_info)

    assert len(captured) == 1
    assert set(captured[0]) == {conn1, conn2}
    assert all(len(conn_set) == 0 for conn_set in OpenedConnectionTracker._opened_connections.values())

    tracker.invalidate_all_connections(host_info=host_info)
    assert len(captured) == 1


def _real_tracker(mocker):
    """A tracker with a real RdsUtils and no prune thread or closer thread."""
    OpenedConnectionTracker._opened_connections = {}
    mocker.patch.object(OpenedConnectionTracker, "_start_prune_thread")
    captured: list = []
    mocker.patch.object(
        OpenedConnectionTracker,
        "_invalidate_connections",
        side_effect=lambda connections_list: captured.append(list(connections_list)))
    return OpenedConnectionTracker(), captured


def test_a_non_instance_endpoint_is_tracked_under_its_host_id_too(mocker):
    """The endpoint a connection was opened against can differ from the one a
    later invalidation names; the host id is what they share."""
    tracker, _ = _real_tracker(mocker)
    conn = mocker.MagicMock()

    tracker.populate_opened_connection_set(
        HostInfo("db.example.com", 5432, host_id="instance-1"), conn)

    assert set(OpenedConnectionTracker._opened_connections) == {
        "instance-1", "db.example.com:5432"}


def test_an_instance_endpoint_is_tracked_under_one_key(mocker):
    tracker, _ = _real_tracker(mocker)

    tracker.populate_opened_connection_set(
        HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432, host_id="instance-1"),
        mocker.MagicMock())

    assert set(OpenedConnectionTracker._opened_connections) == {
        "instance-1.xyz.us-east-2.rds.amazonaws.com:5432"}


def test_invalidate_reaches_a_connection_through_its_host_id(mocker):
    tracker, captured = _real_tracker(mocker)
    conn = mocker.MagicMock()
    tracker.populate_opened_connection_set(
        HostInfo("db.example.com", 5432, host_id="instance-1"), conn)

    tracker.invalidate_all_connections(
        HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432, host_id="instance-1"))

    assert captured and conn in captured[0]


def test_invalidate_closes_a_multi_key_connection_once(mocker):
    """Closing a pool-proxied connection twice would invalidate whatever the
    pool handed out in between."""
    tracker, captured = _real_tracker(mocker)
    conn = mocker.MagicMock()
    host = HostInfo("db.example.com", 5432, host_id="instance-1")
    tracker.populate_opened_connection_set(host, conn)

    tracker.invalidate_all_connections(host)

    assert len(captured) == 1
    assert captured[0].count(conn) == 1


def test_notify_strips_the_trailing_slash_from_a_change_key(
        mocker, mock_plugin_service, mock_rds_utils, mock_callable):
    """Change keys are urls; tracking keys are not. Matching them without
    stripping would never find the connection."""
    from aws_advanced_python_wrapper.utils.notifications import HostEvent

    tracker, captured = _real_tracker(mocker)
    conn = mocker.MagicMock()
    host = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432)
    tracker.populate_opened_connection_set(host, conn)

    plugin = AuroraConnectionTrackerPlugin(
        mock_plugin_service, Properties(), mock_rds_utils, tracker)
    plugin.notify_host_list_changed({host.url: {HostEvent.CONVERTED_TO_READER}})

    assert captured and conn in captured[0]


def test_remove_connection_tracking_clears_every_key(mocker):
    tracker, _ = _real_tracker(mocker)
    conn = mocker.MagicMock()
    host = HostInfo("db.example.com", 5432, host_id="instance-1")
    tracker.populate_opened_connection_set(host, conn)

    tracker.remove_connection_tracking(host, conn)

    assert all(conn not in conn_set
               for conn_set in OpenedConnectionTracker._opened_connections.values())


def test_task_invalidates_pool_proxied_connections(mocker):
    pool_proxied_conn = mocker.MagicMock()

    OpenedConnectionTracker._task([pool_proxied_conn])

    pool_proxied_conn.invalidate.assert_called_once_with()
    pool_proxied_conn.close.assert_not_called()


def test_task_closes_plain_connections_without_invalidate(mocker):
    plain_conn = mocker.MagicMock()
    del plain_conn.invalidate

    OpenedConnectionTracker._task([plain_conn])

    plain_conn.close.assert_called_once_with()


def test_task_discards_pooled_connection_from_queue_pool(mocker):
    # End-to-end against a real SQLAlchemy QueuePool: invalidating the
    # checked-out fairy must discard the underlying driver connection without
    # running rollback-on-return against the failed host, so the pool opens a
    # fresh connection on the next checkout instead of re-pooling the old one.
    raw_connections = []

    def creator():
        raw_conn = mocker.MagicMock()
        raw_connections.append(raw_conn)
        return raw_conn

    queue_pool = QueuePool(creator, pool_size=1, max_overflow=0)
    fairy = queue_pool.connect()

    OpenedConnectionTracker._task([fairy])

    assert len(raw_connections) == 1
    raw_connections[0].rollback.assert_not_called()
    raw_connections[0].close.assert_called_once()

    replacement = queue_pool.connect()
    assert len(raw_connections) == 2
    assert replacement.driver_connection is raw_connections[1]
    replacement.close()
    queue_pool.dispose()
