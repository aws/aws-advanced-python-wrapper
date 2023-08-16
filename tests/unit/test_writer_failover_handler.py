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

from typing import TYPE_CHECKING
from unittest.mock import call

import pytest

from aws_wrapper.errors import FailoverError
from aws_wrapper.failover_result import WriterFailoverResult

if TYPE_CHECKING:
    from threading import Event
    from aws_wrapper.pep249 import Connection

from time import sleep, time

from aws_wrapper.failover_result import ReaderFailoverResult
from aws_wrapper.hostinfo import HostAvailability, HostInfo, HostRole
from aws_wrapper.utils.properties import Properties
from aws_wrapper.writer_failover_handler import (WriterFailoverHandler,
                                                 WriterFailoverHandlerImpl)


@pytest.fixture
def connection_mock(mocker):
    return mocker.MagicMock()


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
    return mocker.MagicMock()


@pytest.fixture
def new_writer_connection_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def reader_a_connection_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def reader_b_connection_mock(mocker):
    return mocker.MagicMock()


@pytest.fixture
def default_properties():
    return Properties()


@pytest.fixture
def new_writer_host():
    return HostInfo("new-writer-host", 1234, HostAvailability.AVAILABLE, HostRole.WRITER)


@pytest.fixture
def writer():
    return HostInfo("writer-host", 1234, HostAvailability.AVAILABLE, HostRole.WRITER)


@pytest.fixture
def reader_a():
    return HostInfo("reader-a-host", 1234, HostAvailability.AVAILABLE, HostRole.READER)


@pytest.fixture
def reader_b():
    return HostInfo("reader-b-host", 1234, HostAvailability.AVAILABLE, HostRole.READER)


@pytest.fixture
def topology(writer, reader_a, reader_b):
    return [writer, reader_a, reader_b]


@pytest.fixture
def new_topology(new_writer_host, reader_a, reader_b):
    return [new_writer_host, reader_a, reader_b]


@pytest.fixture
def set_available_count_for_host():
    return 0


@pytest.fixture(autouse=True)
def setup(writer, new_writer_host, reader_a, reader_b):
    writer.add_alias("writer-host")
    new_writer_host.add_alias("new-writer-host")
    reader_a.add_alias("writer-host")
    reader_b.add_alias("writer-host")
    yield


def test_reconnect_to_writer_task_b_reader_exception(connection_mock, plugin_service_mock, reader_failover_mock, default_properties, writer, topology):
    props = default_properties
    current_topology = topology.copy()

    exception = Exception("Test Exception")
    expected = [call(writer.as_aliases(), HostAvailability.NOT_AVAILABLE),
                call(writer.as_aliases(), HostAvailability.AVAILABLE)]

    def force_connect_side_effect(host_info: HostInfo, properties: Properties, timeout_event: Event) -> Connection:
        if host_info == writer:
            return connection_mock
        else:
            raise exception

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect

    plugin_service_mock.hosts = current_topology
    reader_failover_mock.get_reader_connection.side_effect = FailoverError("error")

    target: WriterFailoverHandler = WriterFailoverHandlerImpl(plugin_service_mock, reader_failover_mock, props, 5, 2, 2)
    result: WriterFailoverResult = target.failover(current_topology)

    assert result.is_connected
    assert not result.is_new_host
    assert result.new_connection is connection_mock

    plugin_service_mock.set_availability.assert_has_calls(expected)


def test_reconnect_to_writer_slow_reader_a(plugin_service_mock, reader_failover_mock, writer_connection_mock, new_writer_connection_mock,
                                           reader_a_connection_mock, default_properties, new_writer_host, writer, reader_a, reader_b,
                                           topology, new_topology):
    props = default_properties
    current_topology = topology.copy()

    exception = Exception("Test Exception")
    expected = [call(writer.as_aliases(), HostAvailability.NOT_AVAILABLE),
                call(writer.as_aliases(), HostAvailability.AVAILABLE)]
    plugin_service_mock.hosts.side_effect = chain([current_topology], cycle([new_topology]))

    def force_connect_side_effect(host_info, props, timeout_event) -> Connection:
        if host_info == writer:
            return writer_connection_mock
        elif host_info == new_writer_host:
            return new_writer_connection_mock
        elif host_info == reader_b:
            raise exception
        else:
            raise exception

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect

    def get_reader_connection_side_effect(current_topology):
        sleep(5)
        return ReaderFailoverResult(reader_a_connection_mock, True, reader_a, None)

    reader_failover_mock.get_reader_connection.side_effect = get_reader_connection_side_effect

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect
    target: WriterFailoverHandler = WriterFailoverHandlerImpl(plugin_service_mock, reader_failover_mock, props, 60, 5, 5)
    result: WriterFailoverResult = target.failover(current_topology)

    assert result.is_connected
    assert not result.is_new_host
    assert result.new_connection is writer_connection_mock

    plugin_service_mock.set_availability.assert_has_calls(expected)


def test_reconnect_to_writer_task_b_defers(plugin_service_mock, reader_failover_mock, writer_connection_mock, default_properties, writer, reader_a,
                                           reader_b, topology):
    props = default_properties
    current_topology = topology.copy()

    exception = Exception("Test Exception")
    expected = [call(writer.as_aliases(), HostAvailability.NOT_AVAILABLE),
                call(writer.as_aliases(), HostAvailability.AVAILABLE)]

    def force_connect_side_effect(host_info: HostInfo, properties: Properties, timeout_event: Event) -> Connection:
        if host_info == writer:
            sleep(5)
            return writer_connection_mock
        elif host_info == reader_b:
            raise exception
        else:
            raise exception

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect

    plugin_service_mock.hosts = current_topology
    reader_failover_mock.get_reader_connection.side_effect = FailoverError("error")

    target: WriterFailoverHandler = WriterFailoverHandlerImpl(plugin_service_mock, reader_failover_mock, props, 60, 2, 2)
    result: WriterFailoverResult = target.failover(current_topology)

    assert result.is_connected
    assert not result.is_new_host
    assert result.new_connection is writer_connection_mock

    plugin_service_mock.set_availability.assert_has_calls(expected)


def test_connect_to_reader_a_slow_writer(plugin_service_mock, reader_failover_mock, writer_connection_mock, new_writer_connection_mock,
                                         reader_a_connection_mock, default_properties, new_writer_host, writer, reader_a, reader_b, topology,
                                         new_topology):
    props = default_properties
    current_topology = topology.copy()

    exception = Exception("Test Exception")
    expected = [call(writer.as_aliases(), HostAvailability.NOT_AVAILABLE),
                call(writer.as_aliases(), HostAvailability.AVAILABLE)]

    def force_connect_side_effect(host_info: HostInfo, properties: Properties, timeout_event: Event) -> Connection:
        if host_info == writer:
            sleep(5)
            return writer_connection_mock
        elif host_info == reader_a:
            return reader_a
        elif host_info == reader_b:
            return reader_b
        elif host_info == new_writer_host:
            return new_writer_connection_mock
        else:
            raise exception

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect

    def get_reader_connection_side_effect(current_topology):
        sleep(5)
        return ReaderFailoverResult(reader_a_connection_mock, True, reader_a, None)

    plugin_service_mock.hosts = new_topology
    reader_failover_mock.get_reader_connection.side_effect = get_reader_connection_side_effect

    target: WriterFailoverHandler = WriterFailoverHandlerImpl(plugin_service_mock, reader_failover_mock, props, 60, 2, 2)
    result: WriterFailoverResult = target.failover(current_topology)

    assert result.is_connected
    assert result.is_new_host
    assert result.new_connection is new_writer_connection_mock

    plugin_service_mock.set_availability.assert_has_calls(expected)


def test_connect_to_reader_a_task_a_defers(connection_mock, plugin_service_mock, reader_failover_mock, new_writer_connection_mock,
                                           reader_a_connection_mock, reader_b_connection_mock, default_properties, new_writer_host, writer, reader_a,
                                           reader_b, topology):
    props = default_properties
    current_topology = topology.copy()
    updated_topology = [new_writer_host, writer, reader_a, reader_b]

    exception = Exception("Test Exception")
    expected = [call(writer.as_aliases(), HostAvailability.NOT_AVAILABLE),
                call(new_writer_host.as_aliases(), HostAvailability.AVAILABLE)]

    def force_connect_side_effect(host_info: HostInfo, properties: Properties, timeout_event: Event) -> Connection:
        if host_info == writer:
            return connection_mock
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

    def get_reader_connection_side_effect(current_topology):
        return ReaderFailoverResult(reader_a_connection_mock, True, reader_a, None)

    plugin_service_mock.hosts = updated_topology
    reader_failover_mock.get_reader_connection.side_effect = get_reader_connection_side_effect

    target: WriterFailoverHandler = WriterFailoverHandlerImpl(plugin_service_mock, reader_failover_mock, props, 60, 5, 5)
    result: WriterFailoverResult = target.failover(current_topology)

    assert result.is_connected
    assert result.is_new_host
    assert result.new_connection is new_writer_connection_mock
    assert len(result.topology) == 4
    assert "new-writer-host" == result.topology[0].host

    plugin_service_mock.set_availability.assert_has_calls(expected, any_order=True)
    plugin_service_mock.force_refresh_host_list.assert_called()


def test_failed_to_connect_failover_timeout(plugin_service_mock, reader_failover_mock, writer_connection_mock, new_writer_connection_mock,
                                            reader_a_connection_mock, reader_b_connection_mock, default_properties, new_writer_host, writer,
                                            reader_a, reader_b, topology, new_topology):
    props = default_properties
    current_topology = topology.copy()
    exception = Exception("Test Exception")
    expected = [call(writer.as_aliases(), HostAvailability.NOT_AVAILABLE)]

    def force_connect_side_effect(host_info: HostInfo, properties: Properties, timeout_event: Event) -> Connection:
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

    def get_reader_connection_side_effect(new_topology):
        return ReaderFailoverResult(reader_a_connection_mock, True, reader_a, None)

    plugin_service_mock.hosts = new_topology
    reader_failover_mock.get_reader_connection.side_effect = get_reader_connection_side_effect

    target: WriterFailoverHandler = WriterFailoverHandlerImpl(plugin_service_mock, reader_failover_mock, props, 5, 2, 2)
    start_time = time()
    result: WriterFailoverResult = target.failover(current_topology)
    end_time = time()
    elapsed_time = end_time - start_time

    assert not result.is_connected
    assert not result.is_new_host

    plugin_service_mock.set_availability.assert_has_calls(expected)
    plugin_service_mock.force_refresh_host_list.assert_called()

    assert elapsed_time < 7


def test_failed_to_connect_task_a_exception_task_b_writer_exception(plugin_service_mock, reader_failover_mock, reader_a_connection_mock,
                                                                    reader_b_connection_mock, default_properties, new_writer_host, writer,
                                                                    reader_a, reader_b, topology, new_topology):
    props = default_properties
    current_topology = topology.copy()

    exception = Exception("Test Exception")
    expected = [call(writer.as_aliases(), HostAvailability.NOT_AVAILABLE),
                call(new_writer_host.as_aliases(), HostAvailability.NOT_AVAILABLE)]

    def force_connect_side_effect(host_info: HostInfo, properties: Properties, timeout_event: Event) -> Connection:
        if host_info == writer:
            raise exception
        elif host_info == reader_a:
            return reader_a_connection_mock
        elif host_info == reader_b:
            return reader_b_connection_mock
        elif host_info == new_writer_host:
            raise exception
        else:
            raise exception

    plugin_service_mock.is_network_exception.return_value = True

    plugin_service_mock.force_connect.side_effect = force_connect_side_effect

    def get_reader_connection_side_effect(current_topology):
        return ReaderFailoverResult(reader_a_connection_mock, True, reader_a, None)

    plugin_service_mock.hosts = new_topology
    reader_failover_mock.get_reader_connection.side_effect = get_reader_connection_side_effect

    target: WriterFailoverHandler = WriterFailoverHandlerImpl(plugin_service_mock, reader_failover_mock, props, 5, 2, 2)
    result: WriterFailoverResult = target.failover(current_topology)

    assert not result.is_connected
    assert not result.is_new_host

    plugin_service_mock.set_availability.assert_has_calls(expected)
