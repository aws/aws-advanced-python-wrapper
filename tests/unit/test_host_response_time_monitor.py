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

from time import sleep
from typing import TYPE_CHECKING

import psycopg
import pytest

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.pep249 import Connection

from aws_advanced_python_wrapper.fastest_response_strategy_plugin import \
    HostResponseTimeMonitor
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


@pytest.fixture
def host_info():
    return HostInfo("localhost")


@pytest.fixture
def props():
    return Properties({WrapperProperties.RESPONSE_MEASUREMENT_INTERVAL_MS.name: 30000, "frt-some_prop": "some_value"})


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mock_driver_dialect(mocker):
    mock_driver_dialect = mocker.MagicMock()
    mock_driver_dialect.is_closed.return_value = False
    return mock_driver_dialect


@pytest.fixture
def mock_plugin_service(mocker, mock_conn, mock_driver_dialect):
    plugin_service = mocker.MagicMock()
    plugin_service.force_connect.return_value = mock_conn
    plugin_service.driver_dialect = mock_driver_dialect
    return plugin_service


def test_run_host_available(mock_conn, mock_plugin_service, host_info, props):

    monitor: HostResponseTimeMonitor = HostResponseTimeMonitor(
        mock_plugin_service,
        host_info,
        props, 1_000)
    sleep(0.1)
    monitor.close()

    mock_plugin_service.driver_dialect.ping.assert_called()
    mock_plugin_service.force_connect.assert_called()

    mock_conn.close.assert_called_once()
    monitor._is_stopped.is_set()
    monitor._monitoring_conn is None


def test_run_interrupted(mocker, mock_plugin_service, host_info, props):
    mocker.patch(
        "aws_advanced_python_wrapper.fastest_response_strategy_plugin.HostResponseTimeMonitor._open_connection", side_effect=InterruptedError())
    monitor: HostResponseTimeMonitor = HostResponseTimeMonitor(
        mock_plugin_service,
        host_info,
        props, 1_000)

    mock_plugin_service.driver_dialect.ping.assert_not_called()
    mock_plugin_service.force_connect.assert_not_called()

    monitor._is_stopped.is_set()
    monitor._monitoring_conn is None


def test_connect_interrupted(mocker, mock_conn, mock_plugin_service, host_info, props, mock_driver_dialect):
    exception = Exception("Test Exception")
    plugin_service = mocker.MagicMock()
    plugin_service.driver_dialect = mock_driver_dialect

    monitor: HostResponseTimeMonitor = HostResponseTimeMonitor(
        mock_plugin_service,
        host_info,
        props, 1_000)

    def force_connect_side_effect(host_info, properties, timeout_event) -> Connection:

        monitor._monitoring_conn = mock_conn
        raise exception

    mock_plugin_service.force_connect.side_effect = force_connect_side_effect

    monitor._is_stopped.is_set()
    monitor._monitoring_conn is None
