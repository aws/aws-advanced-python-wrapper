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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from threading import Event
    from aws_wrapper.failover_result import ReaderFailoverResult
    from aws_wrapper.pep249 import Connection
    from aws_wrapper.plugin_service import PluginService

from time import sleep, time
from typing import Set
from unittest import TestCase
from unittest.mock import MagicMock

from aws_wrapper.hostinfo import HostAvailability, HostInfo, HostRole
from aws_wrapper.reader_failover_handler import (ReaderFailoverHandler,
                                                 ReaderFailoverHandlerImpl)
from aws_wrapper.utils.properties import Properties


class TestReaderFailoverHandler(TestCase):
    set_available_count_for_host = 0
    set_not_available_count_for_host = 0
    set_not_available_count = 0
    default_properties = Properties()
    default_hosts = [HostInfo("writer", 1234, HostAvailability.AVAILABLE, HostRole.WRITER),
                     HostInfo("reader1", 1234, HostAvailability.AVAILABLE, HostRole.READER),
                     HostInfo("reader2", 1234, HostAvailability.AVAILABLE, HostRole.READER),
                     HostInfo("reader3", 1234, HostAvailability.AVAILABLE, HostRole.READER),
                     HostInfo("reader4", 1234, HostAvailability.AVAILABLE, HostRole.READER),
                     HostInfo("reader5", 1234, HostAvailability.AVAILABLE, HostRole.READER)]

    def test_failover(self):
        plugin_service_mock: PluginService = MagicMock()
        connection_mock: Connection = MagicMock()

        hosts = TestReaderFailoverHandler.default_hosts
        props = TestReaderFailoverHandler.default_properties
        current_host = hosts[2]
        success_host = hosts[4]
        exception = Exception("Test Exception")

        def force_connect_side_effect(host_info: HostInfo, properties: Properties, timeout_event: Event) -> Connection:
            if host_info == success_host:
                return connection_mock
            else:
                raise exception
        plugin_service_mock.force_connect.side_effect = force_connect_side_effect
        plugin_service_mock.is_network_exception.return_value = True

        def set_availability_side_effect(host_aliases: Set[str], availability: HostAvailability):
            if host_aliases == success_host.all_aliases:
                if availability == HostAvailability.AVAILABLE:
                    TestReaderFailoverHandler.set_available_count_for_host += 1
                elif availability == HostAvailability.NOT_AVAILABLE:
                    TestReaderFailoverHandler.set_not_available_count_for_host += 1
                    TestReaderFailoverHandler.set_not_available_count += 1
            elif availability == HostAvailability.NOT_AVAILABLE:
                TestReaderFailoverHandler.set_not_available_count += 1
        plugin_service_mock.set_availability.side_effect = set_availability_side_effect
        TestReaderFailoverHandler.set_available_count_for_host = 0
        TestReaderFailoverHandler.set_not_available_count_for_host = 0
        TestReaderFailoverHandler.set_not_available_count = 0

        hosts[2].availability = HostAvailability.NOT_AVAILABLE
        hosts[4].availability = HostAvailability.NOT_AVAILABLE

        target: ReaderFailoverHandler = ReaderFailoverHandlerImpl(plugin_service_mock, props)
        result: ReaderFailoverResult = target.failover(hosts, current_host)

        # Confirm we got a successful connection with the expected node
        assert result.is_connected
        assert result.connection == connection_mock
        assert result.new_host == success_host

        # Confirm we tried at least 4 hosts before we got a successful host
        assert TestReaderFailoverHandler.set_not_available_count >= 4

        # Confirm we only needed one successful attempt
        assert TestReaderFailoverHandler.set_available_count_for_host == 1

        # Confirm we did not get any failed attempts before succeeding with the host
        assert TestReaderFailoverHandler.set_not_available_count_for_host == 0

    def test_failover_timeout(self):
        plugin_service_mock: PluginService = MagicMock()
        connection_mock: Connection = MagicMock()

        hosts = TestReaderFailoverHandler.default_hosts
        props = TestReaderFailoverHandler.default_properties
        current_host = hosts[2]

        def force_connect_side_effect(host_info: HostInfo, properties: Properties, timeout_event: Event) -> Connection:
            # Sleep for 1 second 20 times unless interrupted by timeout
            for _ in range(0, 20):
                if timeout_event.is_set():
                    break
                sleep(1)
            return connection_mock
        plugin_service_mock.force_connect.side_effect = force_connect_side_effect

        hosts[2].availability = HostAvailability.NOT_AVAILABLE
        hosts[4].availability = HostAvailability.NOT_AVAILABLE

        # Set max failover timeout to 5 seconds
        target: ReaderFailoverHandler = ReaderFailoverHandlerImpl(plugin_service_mock, props, 5, 30, False)

        start_time = time()
        result: ReaderFailoverResult = target.failover(hosts, current_host)
        finish_time = time()

        duration = finish_time - start_time

        # Confirm we did not get a successful connection
        assert not result.is_connected
        assert result.connection is None
        assert result.new_host is None

        # Confirm we timed out after 5 seconds (plus an extra second for breathing room)
        assert duration < 6

    def test_failover_null_or_empty_host_list(self):
        plugin_service_mock: PluginService = MagicMock()
        props = TestReaderFailoverHandler.default_properties
        target: ReaderFailoverHandler = ReaderFailoverHandlerImpl(plugin_service_mock, props)
        current_host: HostInfo = HostInfo("writer", 1234)

        result = target.failover(None, current_host)
        assert not result.is_connected
        assert result.connection is None
        assert result.new_host is None

        result = target.failover([], current_host)
        assert not result.is_connected
        assert result.connection is None
        assert result.new_host is None

    def test_get_reader_connection_success(self):
        plugin_service_mock: PluginService = MagicMock()
        connection_mock: Connection = MagicMock()

        hosts = TestReaderFailoverHandler.default_hosts[0:3]
        props = TestReaderFailoverHandler.default_properties
        slow_host = hosts[1]
        fast_host = hosts[2]

        def force_connect_side_effect(host_info: HostInfo, properties: Properties, timeout_event: Event) -> Connection:
            # we want slow host to take 20 seconds before returning connection
            if host_info == slow_host:
                sleep(20)
            return connection_mock
        plugin_service_mock.force_connect.side_effect = force_connect_side_effect

        def set_availability_side_effect(host_aliases: Set[str], availability: HostAvailability):
            if host_aliases == fast_host.all_aliases and availability == HostAvailability.AVAILABLE:
                TestReaderFailoverHandler.set_available_count_for_host += 1
            elif availability == HostAvailability.NOT_AVAILABLE:
                TestReaderFailoverHandler.set_not_available_count += 1
        plugin_service_mock.set_availability.side_effect = set_availability_side_effect
        TestReaderFailoverHandler.set_available_count_for_host = 0
        TestReaderFailoverHandler.set_not_available_count = 0

        # Will attempt to connect to both hosts and will ultimately connect to fast host
        target: ReaderFailoverHandler = ReaderFailoverHandlerImpl(plugin_service_mock, props)
        result: ReaderFailoverResult = target.get_reader_connection(hosts)

        assert result.is_connected
        assert result.connection == connection_mock
        assert result.new_host == fast_host

        assert TestReaderFailoverHandler.set_available_count_for_host == 1
        assert TestReaderFailoverHandler.set_not_available_count == 0

    def test_get_reader_connection_failure(self):
        plugin_service_mock: PluginService = MagicMock()

        hosts = TestReaderFailoverHandler.default_hosts[0:4]
        props = TestReaderFailoverHandler.default_properties
        exception = Exception("Test Exception")

        def force_connect_side_effect(host_info: HostInfo, properties: Properties, timeout_event: Event) -> Connection:
            raise exception
        plugin_service_mock.force_connect.side_effect = force_connect_side_effect

        # Connection attempt raises exception and returns failed result
        target: ReaderFailoverHandler = ReaderFailoverHandlerImpl(plugin_service_mock, props)
        result: ReaderFailoverResult = target.get_reader_connection(hosts)

        assert not result.is_connected
        assert result.connection is None
        assert result.new_host is None

    def test_get_reader_connection_attempts_timeout(self):
        plugin_service_mock: PluginService = MagicMock()
        connection_mock: Connection = MagicMock()

        hosts = TestReaderFailoverHandler.default_hosts[0:3]
        props = TestReaderFailoverHandler.default_properties

        def force_connect_side_effect(host_info: HostInfo, properties: Properties, timeout_event: Event) -> Connection:
            try:
                sleep(5)
            except Exception:
                pass
            return connection_mock
        plugin_service_mock.force_connect.side_effect = force_connect_side_effect

        # Connection attempt times out and returns failed result
        target: ReaderFailoverHandler = ReaderFailoverHandlerImpl(plugin_service_mock, props, 60, 1, False)
        result: ReaderFailoverResult = target.get_reader_connection(hosts)

        assert not result.is_connected
        assert result.connection is None
        assert result.new_host is None

    def test_get_host_tuples_by_priority(self):
        hosts = TestReaderFailoverHandler.default_hosts

        hosts[2].availability = HostAvailability.NOT_AVAILABLE
        hosts[4].availability = HostAvailability.NOT_AVAILABLE
        hosts[5].availability = HostAvailability.NOT_AVAILABLE

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
               hosts_by_priority[i].availability == HostAvailability.NOT_AVAILABLE):
            i += 1

        assert i == len(hosts_by_priority)

    def test_get_reader_tuples_by_priority(self):
        hosts = TestReaderFailoverHandler.default_hosts

        hosts[2].availability = HostAvailability.NOT_AVAILABLE
        hosts[4].availability = HostAvailability.NOT_AVAILABLE
        hosts[5].availability = HostAvailability.NOT_AVAILABLE

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
               hosts_by_priority[i].availability == HostAvailability.NOT_AVAILABLE):
            i += 1

        assert i == len(hosts_by_priority)

    def test_host_failover_strict_reader_enabled(self):
        writer = HostInfo("writer", 1234, HostAvailability.AVAILABLE, HostRole.WRITER)
        reader = HostInfo("reader1", 1234, HostAvailability.AVAILABLE, HostRole.READER)
        hosts = [writer, reader]

        # only reader nodes should be chosen
        hosts_by_priority = ReaderFailoverHandlerImpl.get_hosts_by_priority(hosts, True)
        assert hosts_by_priority == [reader]

        # should select the reader even if unavailable
        reader.availability = HostAvailability.NOT_AVAILABLE
        hosts_by_priority = ReaderFailoverHandlerImpl.get_hosts_by_priority(hosts, True)
        assert hosts_by_priority == [reader]

        # writer node will only be selected when it is the only node in the topology
        hosts_by_priority = ReaderFailoverHandlerImpl.get_hosts_by_priority([writer], True)
        assert hosts_by_priority == [writer]
