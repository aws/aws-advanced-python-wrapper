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

from typing import TYPE_CHECKING

import pytest
from _weakrefset import WeakSet

from aws_advanced_python_wrapper.errors import FailoverError

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.pep249 import Connection

from aws_advanced_python_wrapper.aurora_connection_tracker_plugin import (
    AuroraConnectionTrackerPlugin, OpenedConnectionTracker)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_cursor(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_plugin_service(mocker):
    return mocker.MagicMock()


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
        mocker, mock_plugin_service, mock_rds_utils, mock_tracker, mock_cursor, mock_callable):
    host_info: HostInfo = HostInfo("instance1")
    mock_plugin_service.all_hosts = [host_info]
    mock_plugin_service.current_host_info = host_info
    mock_rds_utils.is_rds_instance.return_value = True
    mock_callable.return_value = mock_conn

    plugin: AuroraConnectionTrackerPlugin = AuroraConnectionTrackerPlugin(mock_plugin_service, Properties(),
                                                                          mock_rds_utils, mock_tracker)

    conn: Connection = plugin.connect(
        mocker.MagicMock(), mocker.MagicMock(), host_info, Properties(), True, mock_callable)
    assert mock_conn == conn
    mock_tracker.populate_opened_connection_queue(host_info, mock_conn)
    assert 0 == len(host_info.aliases)

    conn = plugin.connect(
        mocker.MagicMock(), mocker.MagicMock(), host_info, Properties(), False, mock_callable)
    assert mock_conn == conn
    mock_tracker.populate_opened_connection_queue(host_info, mock_conn)
    assert 0 == len(host_info.aliases)


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
