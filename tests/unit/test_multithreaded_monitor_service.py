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

from aws_advanced_python_wrapper.host_monitoring_plugin import (
    MonitoringContext, MonitoringThreadContainer, MonitorService)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.atomic import AtomicInt
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mock_monitor(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_executor(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_plugin_service(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_future(mocker):
    return mocker.MagicMock()


@pytest.fixture
def failure_detection_time_ms():
    return 10


@pytest.fixture
def failure_detection_interval_ms():
    return 100


@pytest.fixture
def failure_detection_count():
    return 3


@pytest.fixture
def thread_container(mock_executor):
    MonitoringThreadContainer._executor = mock_executor
    mock_executor.return_value = mock_future
    return mock_executor


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
def verify_concurrency(mock_monitor, mock_executor, mock_future, counter, concurrent_counter):
    # The ThreadPoolExecutor may have been shut down by a previous test, so we'll need to recreate it here.
    MonitoringThreadContainer._executor = ThreadPoolExecutor(thread_name_prefix="MonitoringThreadContainerExecutor")
    yield

    counter.set(0)
    assert concurrent_counter.get() > 0
    concurrent_counter.set(0)

    MonitoringThreadContainer.clean_up()


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

    try:
        mock_create_monitor = mocker.patch(
            "aws_advanced_python_wrapper.host_monitoring_plugin.MonitorService._create_monitor",
            return_value=mock_monitor)
        contexts = start_monitoring(num_conns, services, host_alias_list)
        expected_start_monitoring_calls = [mocker.call(context) for context in contexts]
        mock_monitor.start_monitoring.assert_has_calls(expected_start_monitoring_calls, True)
        assert num_conns == len(MonitoringThreadContainer()._monitor_map)
        expected_create_monitor_calls = [mocker.call(host_info, props, MonitoringThreadContainer())] * num_conns
        mock_create_monitor.assert_has_calls(expected_create_monitor_calls)
    finally:
        release_resources(services)


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

    try:
        mock_create_monitor = mocker.patch(
            "aws_advanced_python_wrapper.host_monitoring_plugin.MonitorService._create_monitor",
            return_value=mock_monitor)
        contexts = start_monitoring(num_conns, services, host_alias_list)
        expected_start_monitoring_calls = [mocker.call(context) for context in contexts]
        mock_monitor.start_monitoring.assert_has_calls(expected_start_monitoring_calls, True)
        assert 1 == len(MonitoringThreadContainer()._monitor_map)
        expected_create_monitor_calls = [mocker.call(host_info, props, MonitoringThreadContainer())]
        mock_create_monitor.assert_has_calls(expected_create_monitor_calls)
    finally:
        release_resources(services)


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

    try:
        stop_monitoring(num_conns, services, contexts)
        expected_stop_monitoring_calls = [mocker.call(context) for context in contexts]
        mock_monitor.stop_monitoring.assert_has_calls(expected_stop_monitoring_calls, True)
    finally:
        release_resources(services)


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

    try:
        stop_monitoring(num_conns, services, contexts)
        expected_stop_monitoring_calls = [mocker.call(context) for context in contexts]
        mock_monitor.stop_monitoring.assert_has_calls(expected_stop_monitoring_calls, True)
    finally:
        release_resources(services)


def generate_host_aliases(num_aliases: int, generate_unique_aliases: bool) -> List[FrozenSet[str]]:
    if generate_unique_aliases:
        return [frozenset({f"host-{i}"}) for i in range(num_aliases)]
    else:
        return [frozenset({"single-host"}) for _ in range(num_aliases)]


@pytest.fixture
def generate_services(mock_plugin_service):
    def _generate_services(num_services: int) -> List[MonitorService]:
        return [MonitorService(mock_plugin_service) for _ in range(num_services)]
    return _generate_services


@pytest.fixture
def generate_contexts(
        mocker, mock_monitor, failure_detection_time_ms, failure_detection_interval_ms,
        failure_detection_count, mock_aborted_connection_counter):
    def _generate_contexts(num_contexts: int, generate_unique_contexts) -> List[MonitoringContext]:
        host_aliases_list = generate_host_aliases(num_contexts, generate_unique_contexts)
        contexts = []
        for host_aliases in host_aliases_list:
            MonitoringThreadContainer().get_or_create_monitor(host_aliases, lambda: mock_monitor)
            contexts.append(
                MonitoringContext(
                    mock_monitor,
                    mocker.MagicMock(),
                    mocker.MagicMock(),
                    failure_detection_time_ms,
                    failure_detection_interval_ms,
                    failure_detection_count,
                    mock_aborted_connection_counter))
        return contexts
    return _generate_contexts


def release_resources(services):
    for service in services:
        service.release_resources()


@pytest.fixture
def start_monitoring(counter, concurrent_counter, start_monitoring_thread):
    def _start_monitoring(
            num_threads: int,
            services: List[MonitorService],
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
        props,
        failure_detection_time_ms,
        failure_detection_interval_ms,
        failure_detection_count):
    def _start_monitoring_thread(
            barrier: Barrier,
            counter: AtomicInt,
            concurrent_counter: AtomicInt,
            service: MonitorService,
            host_aliases: FrozenSet[str]) -> MonitoringContext:

        barrier.wait()
        val = counter.get_and_increment()
        if val != 0:
            # If the counter value is greater than 0 it means that this method was called by another thread concurrently
            concurrent_counter.get_and_increment()

        context = service.start_monitoring(
            mock_conn,
            host_aliases,
            host_info,
            props,
            failure_detection_time_ms,
            failure_detection_interval_ms,
            failure_detection_count)

        sleep(0.01)  # Briefly sleep to allow other threads to be executed concurrently
        counter.get_and_decrement()
        return context
    return _start_monitoring_thread


@pytest.fixture
def stop_monitoring(counter, concurrent_counter):
    def _stop_monitoring(num_threads: int, services: List[MonitorService], contexts: List[MonitoringContext]):
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
        service: MonitorService,
        context: MonitoringContext):

    barrier.wait()
    val = counter.get_and_increment()
    if val != 0:
        # If the counter value is greater than 0 it means that this method was called by another thread concurrently
        concurrent_counter.get_and_increment()

    sleep(0.01)  # Briefly sleep to allow other threads to be executed concurrently
    service.stop_monitoring(context)
    counter.get_and_decrement()
