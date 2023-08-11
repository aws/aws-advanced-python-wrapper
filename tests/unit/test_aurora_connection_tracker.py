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

if TYPE_CHECKING:
    from aws_wrapper.pep249 import Connection

from aws_wrapper.aurora_connection_tracker_plugin import \
    AuroraConnectionTrackerPlugin
from aws_wrapper.errors import FailoverError
from aws_wrapper.hostinfo import HostInfo
from aws_wrapper.utils.properties import Properties


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


def test_track_new_instance_connection(mock_plugin_service, mock_rds_utils, mock_tracker, mock_cursor, mock_callable):
    host_info: HostInfo = HostInfo("instance1")
    mock_plugin_service.hosts = [host_info]
    mock_plugin_service.current_host_info = host_info
    mock_rds_utils.is_rds_instance.return_value = True
    mock_callable.return_value = mock_conn

    plugin: AuroraConnectionTrackerPlugin = AuroraConnectionTrackerPlugin(mock_plugin_service, Properties(),
                                                                          mock_rds_utils, mock_tracker)

    conn: Connection = plugin.connect(host_info, Properties(), True, mock_callable)
    assert mock_conn == conn
    mock_tracker.populate_opened_connection_queue(host_info, mock_conn)
    assert 0 == len(host_info.aliases)

    conn = plugin.connect(host_info, Properties(), False, mock_callable)
    assert mock_conn == conn
    mock_tracker.populate_opened_connection_queue(host_info, mock_conn)
    assert 0 == len(host_info.aliases)


def test_invalidate_opened_connections(mock_plugin_service, mock_rds_utils, mock_tracker, mock_cursor, mock_callable):
    expected_exception = FailoverError("exception")
    original_host = HostInfo("host")

    mock_callable.side_effect = expected_exception
    plugin: AuroraConnectionTrackerPlugin = AuroraConnectionTrackerPlugin(mock_plugin_service, Properties(),
                                                                          mock_rds_utils, mock_tracker)

    with pytest.raises(FailoverError):
        mock_plugin_service.hosts = [original_host]
        plugin.execute(mock_cursor, "Cursor.execute", mock_callable, ("select 1", {}))

    mock_tracker.invalidate_current_connection.assert_not_called()
    mock_tracker.invalidate_all_connections.assert_called_with(host_info=original_host)
