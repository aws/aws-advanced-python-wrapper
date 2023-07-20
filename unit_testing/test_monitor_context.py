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

from time import perf_counter_ns

import pytest

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.host_monitoring_plugin import MonitoringContext


@pytest.fixture
def mock_monitor(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_dialect(mocker):
    return mocker.MagicMock()


@pytest.fixture
def failure_time_ms():
    return 5000


@pytest.fixture
def failure_interval_ms():
    return 1000


@pytest.fixture
def failure_count():
    return 3


@pytest.fixture
def context(mock_monitor, mock_conn, mock_dialect, failure_time_ms, failure_interval_ms, failure_count):
    return MonitoringContext(mock_monitor, mock_conn, mock_dialect, failure_time_ms, failure_interval_ms, failure_count)


def test_set_monitor_start_time(context, failure_time_ms):
    start_time = perf_counter_ns()
    expected_end_time = start_time + failure_time_ms * 1_000_000

    context._set_monitor_start_time_ns(start_time)
    assert expected_end_time == context.expected_active_monitoring_start_time_ns


def test_abort_connection(context, mock_conn, mock_dialect):
    context._connection = None
    context._abort_connection()
    mock_dialect.abort_connection.assert_not_called()

    context._connection = mock_conn
    mock_dialect.abort_connection.side_effect = AwsWrapperError()
    context._abort_connection()
    mock_dialect.abort_connection.assert_called_once()


def test_update_connection_status_abort_connection(
        context, mock_dialect, failure_time_ms, failure_interval_ms, failure_count):
    status_check_end_ns = perf_counter_ns()
    status_check_start_ns = status_check_end_ns - (failure_interval_ms * failure_count * 1_000_000) - 1
    monitor_start_ns = status_check_start_ns - (failure_time_ms * 1_000_000)
    context._monitor_start_time_ns = monitor_start_ns

    context.update_connection_status("url", status_check_start_ns, status_check_end_ns, False)
    mock_dialect.abort_connection.assert_called_once()
    assert context._is_host_unavailable is True


def test_update_connection_status_invalid_without_abort(
        context, mock_dialect, failure_time_ms, failure_interval_ms, failure_count):
    # The connection is invalid, but we have not reached the max failure time
    status_check_end_ns = perf_counter_ns()
    status_check_start_ns = status_check_end_ns - (failure_interval_ms / 2)
    monitor_start_ns = status_check_start_ns - (failure_time_ms * 1_000_000)
    context._monitor_start_time_ns = monitor_start_ns

    context.update_connection_status("url", status_check_start_ns, status_check_end_ns, False)
    mock_dialect.abort_connection.assert_not_called()
    assert context._is_host_unavailable is False
    assert 1 == context._current_failure_count


def test_update_connection_status_valid(context, mock_dialect, failure_time_ms, failure_interval_ms, failure_count):
    status_check_end_ns = perf_counter_ns()
    status_check_start_ns = status_check_end_ns - (failure_interval_ms / 2)
    monitor_start_ns = status_check_start_ns - (failure_time_ms * 1_000_000)
    context._monitor_start_time_ns = monitor_start_ns
    context._current_failure_count = 1
    context._unavailable_host_start_time_ns = status_check_start_ns

    context.update_connection_status("url", status_check_start_ns, status_check_end_ns, True)
    mock_dialect.abort_connection.assert_not_called()
    assert 0 == context._current_failure_count
    assert 0 == context._unavailable_host_start_time_ns
    assert context._is_host_unavailable is False


def test_update_connection_status_inactive_context(mocker, context, failure_time_ms, failure_interval_ms):
    status_check_end_ns = perf_counter_ns()
    status_check_start_ns = status_check_end_ns - (failure_interval_ms / 2)
    monitor_start_ns = status_check_start_ns - (failure_time_ms * 1_000_000)
    context._monitor_start_time_ns = monitor_start_ns
    context._is_active_context = False
    spy = mocker.spy(context, "_set_connection_valid")

    context.update_connection_status("url", status_check_start_ns, status_check_end_ns, False)
    spy.assert_not_called()


def test_update_connection_status_before_failure_time(mocker, context, failure_time_ms, failure_interval_ms):
    status_check_end_ns = perf_counter_ns()
    status_check_start_ns = status_check_end_ns - (failure_interval_ms * 1_000_000 / 4)
    monitor_start_ns = status_check_start_ns - (failure_time_ms * 1_000_000 / 2)
    context._monitor_start_time_ns = monitor_start_ns
    spy = mocker.spy(context, "_set_connection_valid")

    context.update_connection_status("url", status_check_start_ns, status_check_end_ns, False)
    spy.assert_not_called()
