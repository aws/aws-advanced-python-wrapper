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

from typing import TYPE_CHECKING, List, Tuple

from aws_advanced_python_wrapper.utils.properties import PropertiesUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties
    from aws_advanced_python_wrapper.pep249 import Connection

from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from copy import deepcopy
from random import shuffle
from threading import Event
from time import sleep
from typing import Optional

from aws_advanced_python_wrapper.failover_result import ReaderFailoverResult
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.failover_mode import (FailoverMode,
                                                             get_failover_mode)
from aws_advanced_python_wrapper.utils.log import Logger

logger = Logger(__name__)


class ReaderFailoverHandler:
    """
    Interface for Reader Failover Process handler. This handler implements all necessary logic to try to reconnect to another reader host.
    """

    @abstractmethod
    def failover(self, current_topology: Tuple[HostInfo, ...], current_host: Optional[HostInfo]) -> ReaderFailoverResult:
        """
        Called to start Reader Failover Process. This process tries to connect to any reader.
        If no reader is available then driver may also try to connect to a writer host, down hosts, and the current reader host.

        :param current_topology: The current cluster topology.
        :param current_host: The :py:class:`HostInfo` containing information regarding the current connection.
        :return: The results of this process.
        """
        pass

    @abstractmethod
    def get_reader_connection(self, hosts: Tuple[HostInfo, ...]) -> ReaderFailoverResult:
        """
        Called to get any available reader connection. If no reader is available then result of process is unsuccessful.
        This process will not attempt to connect to the writer host.

        :param hosts: The current cluster topology.
        :return: The results of the failover process.
        """
        pass


class ReaderFailoverHandlerImpl(ReaderFailoverHandler):
    failed_reader_failover_result = ReaderFailoverResult(None, False, None, None)

    def __init__(
            self,
            plugin_service: PluginService,
            properties: Properties,
            max_timeout_sec: float = 60,
            timeout_sec: float = 30):
        self._plugin_service = plugin_service
        self._properties = properties
        self._max_failover_timeout_sec = max_timeout_sec
        self._timeout_sec = timeout_sec
        mode = get_failover_mode(self._properties)
        self._strict_reader_failover = True if mode is not None and mode == FailoverMode.STRICT_READER else False
        self._timeout_event = Event()

    @property
    def timeout_sec(self):
        return self._timeout_sec

    @timeout_sec.setter
    def timeout_sec(self, value):
        self._timeout_sec = value

    def failover(self, current_topology: Tuple[HostInfo, ...], current_host: Optional[HostInfo]) -> ReaderFailoverResult:
        if current_topology is None or len(current_topology) == 0:
            logger.debug("ReaderFailoverHandler.InvalidTopology", "failover")
            return ReaderFailoverHandlerImpl.failed_reader_failover_result

        result: ReaderFailoverResult = ReaderFailoverHandlerImpl.failed_reader_failover_result
        with ThreadPoolExecutor(thread_name_prefix="ReaderFailoverHandlerExecutor") as executor:
            future = executor.submit(self._internal_failover_task, current_topology, current_host)

            try:
                result = future.result(timeout=self._max_failover_timeout_sec)
                if result is None:
                    result = ReaderFailoverHandlerImpl.failed_reader_failover_result
            except TimeoutError:
                self._timeout_event.set()

        return result

    def _internal_failover_task(
            self, topology: Tuple[HostInfo, ...], current_host: Optional[HostInfo]) -> ReaderFailoverResult:
        try:
            while not self._timeout_event.is_set():
                result = self._failover_internal(topology, current_host)
                if result is not None and result.is_connected:
                    if not self._strict_reader_failover:
                        return result  # any host is fine

                    # need to ensure that the new connection is to a reader host

                    self._plugin_service.force_refresh_host_list(result.connection)
                    if result.new_host is not None:
                        topology = self._plugin_service.all_hosts
                        for host in topology:
                            # found new connection host in the latest topology
                            if host.url == result.new_host.url and host.role == HostRole.READER:
                                return result

                    # New host is not found in the latest topology. There are few possible reasons for that.
                    # - Host is not yet presented in the topology due to failover process in progress
                    # - Host is in the topology but its role isn't a READER (that is not acceptable option due to this.strictReader setting)
                    # Need to continue this loop and to make another try to connect to a reader.
                    if result.connection is not None:
                        result.connection.close()

                sleep(1)  # Sleep for 1 second
        except Exception as err:
            return ReaderFailoverResult(None, False, None, err)

        return ReaderFailoverHandlerImpl.failed_reader_failover_result

    def _failover_internal(self, hosts: Tuple[HostInfo, ...], current_host: Optional[HostInfo]) -> ReaderFailoverResult:
        if current_host is not None:
            self._plugin_service.set_availability(current_host.all_aliases, HostAvailability.UNAVAILABLE)

        hosts_by_priority = ReaderFailoverHandlerImpl.get_hosts_by_priority(hosts, self._strict_reader_failover)
        return self._get_connection_from_host_group(hosts_by_priority)

    def get_reader_connection(self, hosts: Tuple[HostInfo, ...]) -> ReaderFailoverResult:
        if hosts is None or len(hosts) == 0:
            logger.debug("ReaderFailoverHandler.InvalidTopology", "get_reader_connection")
            return ReaderFailoverHandlerImpl.failed_reader_failover_result

        hosts_by_priority = ReaderFailoverHandlerImpl.get_reader_hosts_by_priority(hosts)
        return self._get_connection_from_host_group(hosts_by_priority)

    def _get_connection_from_host_group(self, hosts: Tuple[HostInfo, ...]) -> ReaderFailoverResult:
        for i in range(0, len(hosts), 2):
            result = self._get_result_from_next_task_batch(hosts, i)
            if result.is_connected or result.exception is not None:
                return result

            sleep(1)  # Sleep for 1 second

        return ReaderFailoverHandlerImpl.failed_reader_failover_result

    def _get_result_from_next_task_batch(self, hosts: Tuple[HostInfo, ...], i: int) -> ReaderFailoverResult:
        with ThreadPoolExecutor(thread_name_prefix="ReaderFailoverHandlerRetrieveResultsExecutor") as executor:
            futures = [executor.submit(self.attempt_connection, hosts[i])]
            if i + 1 < len(hosts):
                futures.append(executor.submit(self.attempt_connection, hosts[i + 1]))

            try:
                for future in as_completed(futures, timeout=self.timeout_sec):
                    result = future.result()
                    if result.is_connected or result.exception is not None:
                        executor.shutdown(wait=False)
                        return result
            except TimeoutError:
                self._timeout_event.set()
            finally:
                self._timeout_event.set()

        return ReaderFailoverHandlerImpl.failed_reader_failover_result

    def attempt_connection(self, host: HostInfo) -> ReaderFailoverResult:
        props: Properties = deepcopy(self._properties)
        logger.debug("ReaderFailoverHandler.AttemptingReaderConnection", host.url, PropertiesUtils.mask_properties(props))

        try:
            conn: Connection = self._plugin_service.force_connect(host, props, self._timeout_event)
            self._plugin_service.set_availability(host.all_aliases, HostAvailability.AVAILABLE)

            logger.debug("ReaderFailoverHandler.SuccessfulReaderConnection", host.url)
            return ReaderFailoverResult(conn, True, host, None)
        except Exception as ex:
            logger.debug("ReaderFailoverHandler.FailedReaderConnection", host.url)
            self._plugin_service.set_availability(host.all_aliases, HostAvailability.UNAVAILABLE)
            if not self._plugin_service.is_network_exception(ex):
                return ReaderFailoverResult(None, False, None, ex)

        return ReaderFailoverHandlerImpl.failed_reader_failover_result

    @classmethod
    def get_hosts_by_priority(cls, hosts, readers_only: bool):
        active_readers: List[HostInfo] = []
        down_hosts: List[HostInfo] = []
        writer_host: Optional[HostInfo] = None

        for host in hosts:
            if host.role == HostRole.WRITER:
                writer_host = host
                continue
            if host.get_raw_availability() == HostAvailability.AVAILABLE:
                active_readers.append(host)
            else:
                down_hosts.append(host)

        shuffle(active_readers)
        shuffle(down_hosts)

        hosts_by_priority = active_readers
        num_readers = len(active_readers) + len(down_hosts)
        if writer_host is not None and (not readers_only or num_readers == 0):
            hosts_by_priority.append(writer_host)
        hosts_by_priority += down_hosts

        return tuple(hosts_by_priority)

    @classmethod
    def get_reader_hosts_by_priority(cls, hosts: Tuple[HostInfo, ...]) -> Tuple[HostInfo, ...]:
        active_readers: List[HostInfo] = []
        down_hosts: List[HostInfo] = []

        for host in hosts:
            if host.role == HostRole.WRITER:
                continue
            if host.get_raw_availability() == HostAvailability.AVAILABLE:
                active_readers.append(host)
            else:
                down_hosts.append(host)

        shuffle(active_readers)
        shuffle(down_hosts)

        return tuple(active_readers + down_hosts)
