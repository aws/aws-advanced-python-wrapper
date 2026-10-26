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
from unittest.mock import call

import psycopg
import pytest

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.failover_result import ReaderFailoverResult
    from aws_advanced_python_wrapper.pep249 import Connection

from time import sleep, time

from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.reader_failover_handler import (
    ReaderFailoverHandler, ReaderFailoverHandlerImpl)
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture
def connection_mock(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def plugin_service_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def default_properties():
    return Properties()


@pytest.fixture
def default_hosts():
    return (HostInfo("writer", 1234, HostRole.WRITER, HostAvailability.AVAILABLE),
            HostInfo("reader1", 1234, HostRole.READER, HostAvailability.AVAILABLE),
            HostInfo("reader2", 1234, HostRole.READER, HostAvailability.AVAILABLE),
            HostInfo("reader3", 1234, HostRole.READER, HostAvailability.AVAILABLE),
            HostInfo("reader4", 1234, HostRole.READER, HostAvailability.AVAILABLE),
            HostInfo("reader5", 1234, HostRole.READER, HostAvailability.AVAILABLE))


@pytest.fixture
def set_available_count_for_host():
    return 0


def test_failover(plugin_service_mock, connection_mock, default_properties, default_hosts):
    """
    original host list: [active writer, active reader, current connection (reader), active
    reader, down reader, active reader]
    priority order by index (the subsets will be shuffled): [[1, 3, 5], 0, [2, 4]]
    connection attempts are made in pairs using the above list
    expected test result: successful connection for host at index 4
    """
    hosts = tuple(default_hosts)
    props = default_properties
    current_host = hosts[2]
    success_host = hosts[4]
    expected = [call(x.all_aliases, HostAvailability.UNAVAILABLE)
                if x != success_host
                else call(x.all_aliases, HostAvailability.AVAILABLE)
                for x in hosts]

    exception = Exception("Test Exception")

    def force_connect_side_effect(host_info, _) -> Connection:
        if host_info == success_host:
            return connection_mock
        else:
            raise exception

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect
    plugin_service_mock.is_network_exception.return_value = True

    hosts[2].availability = HostAvailability.UNAVAILABLE
    hosts[4].availability = HostAvailability.UNAVAILABLE

    target: ReaderFailoverHandler = ReaderFailoverHandlerImpl(plugin_service_mock, props)
    result: ReaderFailoverResult = target.failover(hosts, current_host)

    # Confirm we got a successful connection with the expected host
    assert result.is_connected
    assert result.connection == connection_mock
    assert result.new_host == success_host

    plugin_service_mock.set_availability.assert_has_calls(expected, any_order=True)


def test_failover_timeout(plugin_service_mock, connection_mock, default_properties, default_hosts):
    hosts = default_hosts
    props = default_properties
    current_host = hosts[2]

    def force_connect_side_effect(_, __) -> Connection:
        sleep(20)  # The failover handler should hit the max timeout before we hit the 20-second wait and return early.
        return connection_mock

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect

    hosts[2].availability = HostAvailability.UNAVAILABLE
    hosts[4].availability = HostAvailability.UNAVAILABLE

    # Set max failover timeout to 5 seconds
    target: ReaderFailoverHandler = ReaderFailoverHandlerImpl(plugin_service_mock, props, 5, 30)

    start_time = time()
    result: ReaderFailoverResult = target.failover(hosts, current_host)
    finish_time = time()

    duration = finish_time - start_time

    # Confirm we did not get a successful connection
    assert not result.is_connected
    assert result.connection is None
    assert result.new_host is None

    # Confirm we timed out after 5 seconds (plus some extra time for breathing room)
    assert duration < 6.1


def test_failover_empty_host_list(plugin_service_mock, connection_mock, default_properties, default_hosts):
    props = default_properties
    target: ReaderFailoverHandler = ReaderFailoverHandlerImpl(plugin_service_mock, props)
    current_host: HostInfo = HostInfo("writer", 1234)

    result = target.failover((), current_host)
    assert not result.is_connected
    assert result.connection is None
    assert result.new_host is None


def test_get_reader_connection_success(plugin_service_mock, connection_mock, default_properties, default_hosts):
    hosts = default_hosts[0:3]
    props = default_properties
    slow_host = hosts[1]
    fast_host = hosts[2]

    def force_connect_side_effect(host_info, _) -> Connection:
        # we want slow host to take 20 seconds before returning connection
        if host_info == slow_host:
            sleep(20)
        return connection_mock

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect

    # Will attempt to connect to both hosts and will ultimately connect to fast host
    target: ReaderFailoverHandler = ReaderFailoverHandlerImpl(plugin_service_mock, props)
    result: ReaderFailoverResult = target.get_reader_connection(hosts)

    assert result.is_connected
    assert result.connection == connection_mock
    assert result.new_host == fast_host

    plugin_service_mock.set_availability.assert_any_call(fast_host.all_aliases, HostAvailability.AVAILABLE)


def test_get_reader_connection_failure(plugin_service_mock, connection_mock, default_properties, default_hosts):
    hosts = default_hosts[0:4]
    props = default_properties
    exception = Exception("Test Exception")

    def force_connect_side_effect(_, __) -> Connection:
        raise exception

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect

    # Connection attempt raises exception and returns failed result
    target: ReaderFailoverHandler = ReaderFailoverHandlerImpl(plugin_service_mock, props)
    result: ReaderFailoverResult = target.get_reader_connection(hosts)

    assert not result.is_connected
    assert result.connection is None
    assert result.new_host is None


def test_get_reader_connection_attempts_timeout(plugin_service_mock, connection_mock, default_properties,
                                                default_hosts):
    hosts = default_hosts[0:3]
    props = default_properties

    def force_connect_side_effect(_, __) -> Connection:
        try:
            sleep(5)
        except Exception:
            pass
        return connection_mock

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect

    # Connection attempt times out and returns failed result
    target: ReaderFailoverHandler = ReaderFailoverHandlerImpl(plugin_service_mock, props, 60, 1)
    result: ReaderFailoverResult = target.get_reader_connection(hosts)

    assert not result.is_connected
    assert result.connection is None
    assert result.new_host is None


def test_get_host_tuples_by_priority(plugin_service_mock, connection_mock, default_properties, default_hosts):
    hosts = default_hosts

    hosts[2].availability = HostAvailability.UNAVAILABLE
    hosts[4].availability = HostAvailability.UNAVAILABLE
    hosts[5].availability = HostAvailability.UNAVAILABLE

    hosts_by_priority = ReaderFailoverHandlerImpl.get_hosts_by_priority(hosts, False)

    i = 0

    # expecting active readers
    while (i < len(hosts_by_priority) and
           hosts_by_priority[i].role == HostRole.READER and
           hosts_by_priority[i].availability == HostAvailability.AVAILABLE):
        i += 1

    # expecting a writer
    while i < len(hosts_by_priority) and hosts_by_priority[i].role == HostRole.WRITER:
        i += 1

    # expecting down readers
    while (i < len(hosts_by_priority) and
           hosts_by_priority[i].role == HostRole.READER and
           hosts_by_priority[i].availability == HostAvailability.UNAVAILABLE):
        i += 1

    assert i == len(hosts_by_priority)


def test_get_reader_tuples_by_priority(plugin_service_mock, connection_mock, default_properties, default_hosts):
    hosts = default_hosts

    hosts[2].availability = HostAvailability.UNAVAILABLE
    hosts[4].availability = HostAvailability.UNAVAILABLE
    hosts[5].availability = HostAvailability.UNAVAILABLE

    hosts_by_priority = ReaderFailoverHandlerImpl.get_reader_hosts_by_priority(hosts)

    i = 0

    # expecting active readers
    while (i < len(hosts_by_priority) and
           hosts_by_priority[i].role == HostRole.READER and
           hosts_by_priority[i].availability == HostAvailability.AVAILABLE):
        i += 1

    # expecting down readers
    while (i < len(hosts_by_priority) and
           hosts_by_priority[i].role == HostRole.READER and
           hosts_by_priority[i].availability == HostAvailability.UNAVAILABLE):
        i += 1

    assert i == len(hosts_by_priority)


def test_host_failover_strict_reader_enabled(plugin_service_mock, connection_mock, default_properties, default_hosts):
    writer = HostInfo("writer", 1234, HostRole.WRITER, HostAvailability.AVAILABLE)
    reader = HostInfo("reader1", 1234, HostRole.READER, HostAvailability.AVAILABLE)
    hosts = (writer, reader)

    # only reader hosts should be chosen
    hosts_by_priority = ReaderFailoverHandlerImpl.get_hosts_by_priority(hosts, True)
    assert hosts_by_priority == (reader, )

    # should select the reader even if unavailable
    reader.availability = HostAvailability.UNAVAILABLE
    hosts_by_priority = ReaderFailoverHandlerImpl.get_hosts_by_priority(hosts, True)
    assert hosts_by_priority == (reader,)

    # writer host will only be selected when it is the only host in the topology
    hosts_by_priority = ReaderFailoverHandlerImpl.get_hosts_by_priority((writer,), True)
    assert hosts_by_priority == (writer,)
