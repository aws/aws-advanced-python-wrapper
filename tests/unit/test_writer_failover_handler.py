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

from typing import TYPE_CHECKING, Any, List
from unittest.mock import call

import pytest
from aws_wrapper.errors import AwsWrapperError, FailoverError

from aws_wrapper.failover_result import WriterFailoverResult

if TYPE_CHECKING:
    from threading import Event
    from aws_wrapper.failover_result import ReaderFailoverResult
    from aws_wrapper.pep249 import Error, Connection


from time import sleep, time
from aws_wrapper.pep249 import Error, Connection, FailoverSuccessError


from aws_wrapper.hostinfo import HostAvailability, HostInfo, HostRole
from aws_wrapper.reader_failover_handler import (ReaderFailoverHandler,
                                                 ReaderFailoverHandlerImpl)
from aws_wrapper.writer_failover_handler import (WriterFailoverHandler,
                                                 WriterFailoverHandlerImpl)
from aws_wrapper.utils.properties import Properties


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


def test_reconnect_to_writer_task_b_reader_exception(connection_mock: Connection, plugin_service_mock, reader_failover_mock: ReaderFailoverHandler,
                                                     writer_failover_mock: WriterFailoverHandler,
                                                     writer_connection_mock: Connection, new_writer_connection_mock: Connection,
                                                     reader_a_connection_mock: Connection, reader_b_connection_mock: Connection,
                                                     default_properties: Properties,
                                                     new_writer_host: HostInfo, writer: HostInfo, reader_a: HostInfo, reader_b: HostInfo,
                                                     topology: List[HostInfo], new_topology: List[HostInfo]):
    props = default_properties
    current_topology = topology.copy()
    writer.add_alias("writer-host")
    reader_a.add_alias("reader-a-host")
    reader_b.add_alias("reader-b-host")

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
    reader_failover_mock.get_reader_connection = FailoverError("error")

    target: WriterFailoverHandler = WriterFailoverHandlerImpl(plugin_service_mock, reader_failover_mock, props, 5, 2, 2)
    result: WriterFailoverResult = target.failover(current_topology)

    assert result.is_connected
    assert not result.is_new_host
    assert result.new_connection is connection_mock

    plugin_service_mock.set_availability.assert_has_calls(expected)
