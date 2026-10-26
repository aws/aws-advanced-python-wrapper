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

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from time import sleep
from typing import FrozenSet, List

import psycopg
import pytest

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.host_monitoring_plugin import (
    HostMonitorService, MonitoringContext)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.atomic import AtomicInt
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mock_monitor(mocker):
    monitor = mocker.MagicMock()
    monitor.is_stopped = False
    return monitor


@pytest.fixture
def mock_plugin_service(mocker):
    return mocker.MagicMock()


@pytest.fixture
def host_info():
    return HostInfo("localhost")


@pytest.fixture
def props():
    return Properties({WrapperProperties.MONITOR_DISPOSAL_TIME_MS.name: "60000"})


@pytest.fixture
def concurrent_counter():
    return AtomicInt()


@pytest.fixture
def counter():
    return AtomicInt()


@pytest.fixture
def mock_aborted_connection_counter(mocker):
    return mocker.MagicMock()


@pytest.fixture(autouse=True)
def verify_concurrency(counter, concurrent_counter):
    yield

    counter.set(0)
    assert concurrent_counter.get() > 0
    concurrent_counter.set(0)

    release_resources()


def test_start_monitoring__connections_to_different_hosts(
        mocker,
        mock_monitor,
        mock_plugin_service,
        host_info,
        props,
        generate_services,
        start_monitoring):

    num_conns = 10
    host_alias_list = generate_host_aliases(num_conns, True)
    services = generate_services(num_conns)

    mocker.patch(
        "aws_advanced_python_wrapper.host_monitoring_plugin.Monitor.__init__",
        return_value=None)
    mocker.patch.object(
        services[0]._monitor_service, 'run_if_absent_with_aliases', return_value=mock_monitor)
    # Patch all services to use the same mock
    for svc in services:
        svc._monitor_service = services[0]._monitor_service

    contexts = start_monitoring(num_conns, services, host_alias_list)
    expected_start_monitoring_calls = [mocker.call(context) for context in contexts]
    mock_monitor.start_monitoring.assert_has_calls(expected_start_monitoring_calls, True)


def test_start_monitoring__connections_to_same_host(
        mocker,
        mock_monitor,
        mock_plugin_service,
        host_info,
        props,
        generate_services,
        start_monitoring):

    num_conns = 10
    host_alias_list = generate_host_aliases(num_conns, False)
    services = generate_services(num_conns)

    mocker.patch(
        "aws_advanced_python_wrapper.host_monitoring_plugin.Monitor.__init__",
        return_value=None)
    mocker.patch.object(
        services[0]._monitor_service, 'run_if_absent_with_aliases', return_value=mock_monitor)
    for svc in services:
        svc._monitor_service = services[0]._monitor_service

    contexts = start_monitoring(num_conns, services, host_alias_list)
    expected_start_monitoring_calls = [mocker.call(context) for context in contexts]
    mock_monitor.start_monitoring.assert_has_calls(expected_start_monitoring_calls, True)


def test_stop_monitoring__connections_to_different_hosts(
        mocker,
        mock_monitor,
        mock_plugin_service,
        host_info,
        props,
        generate_services,
        generate_contexts,
        stop_monitoring):

    num_conns = 10
    contexts = generate_contexts(num_conns, True)
    services = generate_services(num_conns)

    stop_monitoring(num_conns, services, contexts)
    expected_stop_monitoring_calls = [mocker.call(context) for context in contexts]
    mock_monitor.stop_monitoring.assert_has_calls(expected_stop_monitoring_calls, True)


def test_stop_monitoring__connections_to_same_host(
        mocker,
        mock_monitor,
        mock_plugin_service,
        host_info,
        props,
        generate_services,
        generate_contexts,
        stop_monitoring):

    num_conns = 10
    contexts = generate_contexts(num_conns, False)
    services = generate_services(num_conns)

    stop_monitoring(num_conns, services, contexts)
    expected_stop_monitoring_calls = [mocker.call(context) for context in contexts]
    mock_monitor.stop_monitoring.assert_has_calls(expected_stop_monitoring_calls, True)


def generate_host_aliases(num_aliases: int, generate_unique_aliases: bool) -> List[FrozenSet[str]]:
    if generate_unique_aliases:
        return [frozenset({f"host-{i}"}) for i in range(num_aliases)]
    else:
        return [frozenset({"single-host"}) for _ in range(num_aliases)]


@pytest.fixture
def generate_services(mock_plugin_service):
    def _generate_services(num_services: int) -> List[HostMonitorService]:
        return [HostMonitorService(mock_plugin_service) for _ in range(num_services)]
    return _generate_services


@pytest.fixture
def generate_contexts(
        mocker, mock_monitor, mock_aborted_connection_counter):
    def _generate_contexts(num_contexts: int, generate_unique_contexts) -> List[MonitoringContext]:
        contexts = []
        for _ in range(num_contexts):
            contexts.append(
                MonitoringContext(
                    mock_monitor,
                    mocker.MagicMock(),
                    mocker.MagicMock(),
                    10,
                    100,
                    3,
                    mock_aborted_connection_counter))
        return contexts
    return _generate_contexts


@pytest.fixture
def start_monitoring(counter, concurrent_counter, start_monitoring_thread):
    def _start_monitoring(
            num_threads: int,
            services: List[HostMonitorService],
            host_aliases_list: List[FrozenSet[str]]) -> List[MonitoringContext]:
        barrier = Barrier(num_threads)
        futures = []
        with ThreadPoolExecutor(num_threads) as executor:
            for i in range(num_threads):
                future = executor.submit(
                    start_monitoring_thread,
                    barrier,
                    counter,
                    concurrent_counter,
                    services[i],
                    host_aliases_list[i])
                futures.append(future)
        return [future.result() for future in futures]
    return _start_monitoring


@pytest.fixture
def start_monitoring_thread(
        mock_conn,
        host_info,
        props):
    def _start_monitoring_thread(
            barrier: Barrier,
            counter: AtomicInt,
            concurrent_counter: AtomicInt,
            service: HostMonitorService,
            host_aliases: FrozenSet[str]) -> MonitoringContext:

        barrier.wait()
        val = counter.get_and_increment()
        if val != 0:
            concurrent_counter.get_and_increment()

        context = service.start_monitoring(
            mock_conn,
            host_aliases,
            host_info,
            Properties(),
            10,
            100,
            3)

        sleep(0.01)
        counter.get_and_decrement()
        return context
    return _start_monitoring_thread


@pytest.fixture
def stop_monitoring(counter, concurrent_counter):
    def _stop_monitoring(num_threads: int, services: List[HostMonitorService], contexts: List[MonitoringContext]):
        barrier = Barrier(num_threads)
        with ThreadPoolExecutor(num_threads) as executor:
            for i in range(num_threads):
                executor.submit(
                    stop_monitoring_thread,
                    barrier,
                    counter,
                    concurrent_counter,
                    services[i],
                    contexts[i])
    return _stop_monitoring


def stop_monitoring_thread(
        barrier: Barrier,
        counter: AtomicInt,
        concurrent_counter: AtomicInt,
        service: HostMonitorService,
        context: MonitoringContext):

    barrier.wait()
    val = counter.get_and_increment()
    if val != 0:
        concurrent_counter.get_and_increment()

    sleep(0.01)
    service.stop_monitoring(context)
    counter.get_and_decrement()
