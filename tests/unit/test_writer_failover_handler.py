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

from itertools import chain, cycle
from time import sleep, time
from typing import TYPE_CHECKING
from unittest.mock import call

import psycopg
import pytest

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.pep249 import Connection

from aws_advanced_python_wrapper.errors import FailoverError
from aws_advanced_python_wrapper.failover_result import (ReaderFailoverResult,
                                                         WriterFailoverResult)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties
from aws_advanced_python_wrapper.writer_failover_handler import (WriterFailoverHandler,
                                                                 WriterFailoverHandlerImpl)


@pytest.fixture
def plugin_service_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def reader_failover_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def writer_failover_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def writer_connection_mock(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def new_writer_connection_mock(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def reader_a_connection_mock(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def reader_b_connection_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def default_properties():
    return Properties()


@pytest.fixture
def new_writer_host():
    return HostInfo("new-writer-host", 1234, HostRole.WRITER, HostAvailability.AVAILABLE)


@pytest.fixture
def writer():
    return HostInfo("writer-host", 1234, HostRole.WRITER, HostAvailability.AVAILABLE)


@pytest.fixture
def reader_a():
    return HostInfo("reader-a-host", 1234, HostRole.READER, HostAvailability.AVAILABLE)


@pytest.fixture
def reader_b():
    return HostInfo("reader-b-host", 1234, HostRole.READER, HostAvailability.AVAILABLE)


@pytest.fixture
def topology(writer, reader_a, reader_b):
    return [writer, reader_a, reader_b]


@pytest.fixture
def new_topology(new_writer_host, reader_a, reader_b):
    return [new_writer_host, reader_a, reader_b]


@pytest.fixture(autouse=True)
def setup(writer, new_writer_host, reader_a, reader_b):
    writer.add_alias("writer-host")
    new_writer_host.add_alias("new-writer-host")
    reader_a.add_alias("reader-a")
    reader_b.add_alias("reader-b")


def test_reconnect_to_writer_task_b_reader_exception(
        writer_connection_mock, plugin_service_mock, reader_failover_mock, default_properties, writer, topology):
    exception = Exception("Test Exception")

    def force_connect_side_effect(host_info, _, __) -> Connection:
        if host_info == writer:
            return writer_connection_mock
        else:
            raise exception

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect

    plugin_service_mock.hosts = topology
    reader_failover_mock.get_reader_connection.side_effect = FailoverError("error")

    target: WriterFailoverHandler = WriterFailoverHandlerImpl(
        plugin_service_mock,
        reader_failover_mock,
        default_properties,
        5,
        2,
        2)
    result: WriterFailoverResult = target.failover(topology)

    assert result.is_connected
    assert not result.is_new_host
    assert result.new_connection is writer_connection_mock

    expected = [call(writer.as_aliases(), HostAvailability.UNAVAILABLE),
                call(writer.as_aliases(), HostAvailability.AVAILABLE)]

    plugin_service_mock.set_availability.assert_has_calls(expected)


def test_reconnect_to_writer_slow_task_b(
        mocker, plugin_service_mock, reader_failover_mock, writer_connection_mock, new_writer_connection_mock,
        reader_a_connection_mock, default_properties, new_writer_host, writer, reader_a, reader_b, topology,
        new_topology):
    exception = Exception("Test Exception")
    expected = [call(writer.as_aliases(), HostAvailability.UNAVAILABLE),
                call(writer.as_aliases(), HostAvailability.AVAILABLE)]

    mock_hosts_property = mocker.PropertyMock(side_effect=chain([topology], cycle([new_topology])))
    type(plugin_service_mock).hosts = mock_hosts_property

    def force_connect_side_effect(host_info, _, __) -> Connection:
        if host_info == writer:
            return writer_connection_mock
        elif host_info == new_writer_host:
            return new_writer_connection_mock
        else:
            raise exception

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect

    def get_reader_connection_side_effect(_):
        sleep(5)
        return ReaderFailoverResult(reader_a_connection_mock, True, reader_a, None)

    reader_failover_mock.get_reader_connection.side_effect = get_reader_connection_side_effect

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect
    target: WriterFailoverHandler = WriterFailoverHandlerImpl(
        plugin_service_mock,
        reader_failover_mock,
        default_properties,
        60,
        5,
        5)
    result: WriterFailoverResult = target.failover(topology)

    assert result.is_connected
    assert not result.is_new_host
    assert result.new_connection is writer_connection_mock

    plugin_service_mock.set_availability.assert_has_calls(expected)


def test_reconnect_to_writer_task_b_defers(
        plugin_service_mock, reader_failover_mock, writer_connection_mock, reader_a_connection_mock, default_properties,
        writer, reader_a, reader_b, topology):
    exception = Exception("Test Exception")

    def force_connect_side_effect(host_info, _, __) -> Connection:
        if host_info == writer:
            sleep(5)
            return writer_connection_mock
        else:
            raise exception

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect

    def get_reader_connection_side_effect(_):
        return ReaderFailoverResult(reader_a_connection_mock, True, reader_a, None)

    plugin_service_mock.hosts = topology
    reader_failover_mock.get_reader_connection.side_effect = get_reader_connection_side_effect

    target: WriterFailoverHandler = WriterFailoverHandlerImpl(
        plugin_service_mock,
        reader_failover_mock,
        default_properties,
        60,
        2,
        2)
    result: WriterFailoverResult = target.failover(topology)

    assert result.is_connected
    assert not result.is_new_host
    assert result.new_connection is writer_connection_mock

    expected = [call(writer.as_aliases(), HostAvailability.UNAVAILABLE),
                call(writer.as_aliases(), HostAvailability.AVAILABLE)]

    plugin_service_mock.set_availability.assert_has_calls(expected)


def test_connect_to_new_writer_slow_task_a(
        plugin_service_mock, reader_failover_mock, writer_connection_mock, new_writer_connection_mock,
        reader_a_connection_mock, reader_b_connection_mock, default_properties, new_writer_host, writer, reader_a,
        reader_b, topology, new_topology):
    exception = Exception("Test Exception")

    def force_connect_side_effect(host_info, _, __) -> Connection:
        if host_info == writer:
            sleep(5)
            return writer_connection_mock
        elif host_info == reader_a:
            return reader_a_connection_mock
        elif host_info == reader_b:
            return reader_b_connection_mock
        elif host_info == new_writer_host:
            return new_writer_connection_mock
        else:
            raise exception

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect

    def get_reader_connection_side_effect(_):
        return ReaderFailoverResult(reader_a_connection_mock, True, reader_a, None)

    plugin_service_mock.hosts = new_topology
    reader_failover_mock.get_reader_connection.side_effect = get_reader_connection_side_effect

    target: WriterFailoverHandler = WriterFailoverHandlerImpl(
        plugin_service_mock,
        reader_failover_mock,
        default_properties,
        60,
        2,
        2)
    result: WriterFailoverResult = target.failover(topology)

    assert result.is_connected
    assert result.is_new_host
    assert result.new_connection is new_writer_connection_mock

    expected = [call(writer.as_aliases(), HostAvailability.UNAVAILABLE),
                call(new_writer_host.as_aliases(), HostAvailability.AVAILABLE)]

    plugin_service_mock.set_availability.assert_has_calls(expected)


def test_connect_to_new_writer_task_a_defers(
        plugin_service_mock, reader_failover_mock, writer_connection_mock, new_writer_connection_mock,
        reader_a_connection_mock, reader_b_connection_mock, default_properties, new_writer_host, writer, reader_a,
        reader_b, topology):
    updated_topology = [new_writer_host, writer, reader_a, reader_b]

    exception = Exception("Test Exception")

    def force_connect_side_effect(host_info, _, __) -> Connection:
        if host_info == writer:
            return writer_connection_mock
        elif host_info == reader_a:
            return reader_a_connection_mock
        elif host_info == reader_b:
            return reader_b_connection_mock
        elif host_info == new_writer_host:
            sleep(5)
            return new_writer_connection_mock
        else:
            raise exception

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect

    def get_reader_connection_side_effect(_):
        return ReaderFailoverResult(reader_a_connection_mock, True, reader_a, None)

    plugin_service_mock.hosts = updated_topology
    reader_failover_mock.get_reader_connection.side_effect = get_reader_connection_side_effect

    target: WriterFailoverHandler = WriterFailoverHandlerImpl(
        plugin_service_mock,
        reader_failover_mock,
        default_properties,
        60,
        5,
        5)
    result: WriterFailoverResult = target.failover(topology)

    assert result.is_connected
    assert result.is_new_host
    assert result.new_connection is new_writer_connection_mock
    assert len(result.topology) == 4
    assert "new-writer-host" == result.topology[0].host

    expected = [call(writer.as_aliases(), HostAvailability.UNAVAILABLE),
                call(new_writer_host.as_aliases(), HostAvailability.AVAILABLE)]

    plugin_service_mock.set_availability.assert_has_calls(expected, any_order=True)
    plugin_service_mock.force_refresh_host_list.assert_called()


def test_failed_to_connect_failover_timeout(
        plugin_service_mock, reader_failover_mock, writer_connection_mock, new_writer_connection_mock,
        reader_a_connection_mock, reader_b_connection_mock, default_properties, new_writer_host, writer, reader_a,
        reader_b, topology, new_topology):
    exception = Exception("Test Exception")

    def force_connect_side_effect(host_info, _, timeout_event) -> Connection:
        if host_info == writer:
            for _ in range(0, 30):
                if timeout_event.is_set():
                    break
                sleep(1)
            return writer_connection_mock
        elif host_info == reader_a:
            return reader_a_connection_mock
        elif host_info == reader_b:
            return reader_b_connection_mock
        elif host_info == new_writer_host:
            for _ in range(0, 30):
                if timeout_event.is_set():
                    break
                sleep(1)
            return new_writer_connection_mock
        else:
            raise exception

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect

    def get_reader_connection_side_effect(_):
        return ReaderFailoverResult(reader_a_connection_mock, True, reader_a, None)

    plugin_service_mock.hosts = new_topology
    reader_failover_mock.get_reader_connection.side_effect = get_reader_connection_side_effect

    target: WriterFailoverHandler = WriterFailoverHandlerImpl(
        plugin_service_mock,
        reader_failover_mock,
        default_properties,
        5,
        2,
        2)
    start_time = time()
    result: WriterFailoverResult = target.failover(topology)
    end_time = time()
    elapsed_time = end_time - start_time

    assert not result.is_connected
    assert not result.is_new_host

    expected = [call(writer.as_aliases(), HostAvailability.UNAVAILABLE)]

    plugin_service_mock.set_availability.assert_has_calls(expected)
    plugin_service_mock.force_refresh_host_list.assert_called()

    # Confirm we timed out after 5 seconds (plus some extra time for breathing room)
    assert elapsed_time < 6.1


def test_failed_to_connect_task_a_exception_task_b_writer_exception(
        plugin_service_mock, reader_failover_mock, reader_a_connection_mock, reader_b_connection_mock,
        default_properties, new_writer_host, writer, reader_a, reader_b, topology, new_topology):
    exception = Exception("Test Exception")

    def force_connect_side_effect(host_info, _, __) -> Connection:
        if host_info == reader_a:
            return reader_a_connection_mock
        elif host_info == reader_b:
            return reader_b_connection_mock
        else:
            raise exception

    plugin_service_mock.is_network_exception.return_value = True
    plugin_service_mock.force_connect.side_effect = force_connect_side_effect

    def get_reader_connection_side_effect(_):
        return ReaderFailoverResult(reader_a_connection_mock, True, reader_a, None)

    plugin_service_mock.hosts = new_topology
    reader_failover_mock.get_reader_connection.side_effect = get_reader_connection_side_effect

    target: WriterFailoverHandler = WriterFailoverHandlerImpl(
        plugin_service_mock,
        reader_failover_mock,
        default_properties,
        5,
        2,
        2)
    result: WriterFailoverResult = target.failover(topology)

    assert not result.is_connected
    assert not result.is_new_host

    expected = [call(writer.as_aliases(), HostAvailability.UNAVAILABLE),
                call(new_writer_host.as_aliases(), HostAvailability.UNAVAILABLE)]

    plugin_service_mock.set_availability.assert_has_calls(expected, any_order=True)
