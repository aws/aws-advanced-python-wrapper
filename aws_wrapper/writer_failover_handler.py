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
from concurrent.futures import ThreadPoolExecutor, wait

from typing import TYPE_CHECKING, Optional
from aws_wrapper.failover_result import ReaderFailoverResult
from aws_wrapper.hostinfo import HostAvailability, HostRole

from aws_wrapper.plugin_service import PluginService
from aws_wrapper.reader_failover_handler import ReaderFailoverHandler
from aws_wrapper.utils.properties import Properties

if TYPE_CHECKING:
    from aws_wrapper.failover_result import WriterFailoverResult
    from aws_wrapper.hostinfo import HostInfo
    from aws_wrapper.pep249 import Connection


from abc import abstractmethod
from logging import getLogger
from copy import deepcopy
from time import sleep
from typing import List
import time

from aws_wrapper.utils.messages import Messages


logger = getLogger(__name__)


class WriterFailoverHandler:
    @abstractmethod
    def failover(self, current_topology: List[HostInfo]) -> WriterFailoverResult:
        pass


class WriterFailoverHandlerImpl(WriterFailoverHandler):
    failed_writer_failover_result = WriterFailoverResult(None, False, False, None, None, None)

    def __init__(
            self,
            plugin_service: PluginService,
            reader_failover_handler: ReaderFailoverHandler,
            initial_connection_properties: Properties,
            current_reader_connection: Connection,
            current_connection: Connection,
            current_reader_host: HostInfo):
        self._plugin_service = plugin_service
        self._reader_failover_handler = reader_failover_handler
        self._initial_connection_properties = initial_connection_properties
        self._current_reader_connection = current_reader_connection
        self._current_reader_host = current_reader_host
        self._current_connection = current_connection
        self._max_failover_timeout_sec = 60
        self._read_topology_interval_sec: int = 5
        self._reconnect_writer_interval_sec: int = 5

    def failover(self, current_topology: List[HostInfo]) -> WriterFailoverResult:
        if current_topology is None or len(current_topology) == 0:
            logger.debug(Messages.get_formatted("WriterFailoverHandler.InvalidTopology"))
            return WriterFailoverHandlerImpl.failed_writer_failover_result

        with ThreadPoolExecutor() as executor:
            self.submit_tasks(current_topology, executor)

            try:
                start_time_nano: int = time.time_ns()
                result: WriterFailoverResult = self.get_next_result(executor, self._max_failover_timeout_sec)
                if (result.is_connected() | result.exception is not None):
                    return result

                end_time_nano: int = time.time_ns()
                duration_ms: int = round((end_time_nano - start_time_nano)/1000000)
                remaining_time_ms = self._failover_timeout_ms - duration_ms

                if remaining_time_ms > 0:
                    result = self.get_next_result(executor, remaining_time_ms)
                if (result.is_connected() | result.exception is not None):
                    return result

                logger.debug(Messages.get_formatted("WriterFailoverHandler.failedToConnectToWriterInstance"))
                return WriterFailoverHandlerImpl.failed_writer_failover_result
            finally: 
                return result

    def get_writer(self, topology: List[HostInfo]) -> Optional[HostInfo]:
        if topology is None or len(topology) == 0:
            return None

        for host in topology:
            if host.role == HostRole.WRITER:
                return host

        return None

    def submit_tasks(self, current_topology: List[HostInfo], executor_service: ThreadPoolExecutor):
        writer_host: HostInfo = self.get_writer(current_topology)
        self._plugin_service.set_availability(writer_host.as_aliases, HostAvailability.NOT_AVAILABLE)
        with ThreadPoolExecutor() as executor:
            executor.submit(self.reconnect_to_writer_handler, writer_host)
            executor.submit(self.wait_for_new_writer_handler, current_topology, writer_host)

    def get_next_result(self, executor: ThreadPoolExecutor, timeout_ms: int) -> WriterFailoverResult:
        try:
            first_completed = wait(executor, timeout_ms)
            if first_completed is not None:
                return WriterFailoverHandlerImpl.failed_writer_failover_result
            result: WriterFailoverResult = first_completed.result()
            if result.is_connected or result.exception is not None:
                executor.shutdown()
                self.log_task_success(result)
                return result

        except Exception:
            pass
        return WriterFailoverHandlerImpl.failed_writer_failover_result

    def log_task_success(self, result: WriterFailoverResult) -> None:
        logger.debug(Messages.get_formatted("ReaderFailoverHandler.FailedReaderConnection"))

    def reconnect_to_writer_handler(self, initial_writer_host: HostInfo):
        conn: Optional[Connection] = None
        latest_topology: Optional[List[HostInfo]] = None
        success: bool = False

        props: Properties = deepcopy(self._properties)

        try:
            while latest_topology is None or len(latest_topology) == 0:
                try:
                    if conn is not None:
                        conn.close()

                    conn: Connection = self._plugin_service.force_connect(initial_writer_host, props, self._timeout_event)
                    self._plugin_service.force_refresh_host_list(conn)
                    latest_topology = PluginService.hosts

                except Exception as err:
                    return WriterFailoverResult(False, False, None, None, "TaskA", err)

            if latest_topology is None or len(latest_topology) == 0:
                sleep(self._reconnect_writer_interval_sec)

            success = self.is_current_host_writer(latest_topology)
            PluginService.set_availability(initial_writer_host.as_aliases, HostAvailability.AVAILABLE)

            return WriterFailoverResult(success, False, latest_topology, True if success else None, "TaskA")

        except Exception:
            return WriterFailoverResult(success, False, None, "TaskA")
        finally:
            try:
                if conn is not None and not success:
                    conn.close()
            except Exception:
                pass
            logger.debug(Messages.get_formatted("ClusterAwareWriterFailoverHandler.taskAFinished"))

    def is_current_host_writer(self, latest_topology: List[HostInfo], initial_writer_host: HostInfo) -> bool:
        latest_writer: HostInfo = self.get_writer(latest_topology)
        latest_writer_all_aliases: frozenset[str] = latest_writer.as_aliases
        current_aliases: frozenset[str] = initial_writer_host.as_alias
        return latest_writer is not None and any(current_aliases in latest_writer_all_aliases)

    def wait_for_new_writer_handler(self, ) -> None:
        logger.debug(Messages.get_formatted("WriterFailoverHandler.taskBAttemptConnectionToNewWriterInstance"))
        try:
            success: bool = False
            while not success:
                self.connect_to_reader()
                success = self.refresh_topology_and_connect_to_new_writer()
                if not success:
                    self.close_reader_connection()
                return WriterFailoverResult(True, True, self._current_topology, self._current_connection, "TaskB", None)

        except InterruptedError:
            return WriterFailoverResult(False, False, None, None, "TaskB")
        except Exception:
            logger.debug(Messages.get_formatted("ClusterAwareWriterFailoverHandler.taskBEncounteredException"))

        finally:
            self.cleanup()
            logger.debug(Messages.get_formatted("WriterFailoverHandler.taskBFinished"))

    def connect_to_reader(self) -> None:
        while True:
            try: 
                conn_result: ReaderFailoverResult = ReaderFailoverHandler.get_reader_connection(self._current_topology)
                if (self.is_valid_reader_connection(conn_result)):
                    self._current_reader_connection = conn_result.connection
                    self._current_reader_host = conn_result.new_host
                    logger.debug(Messages.get_formatted("WriterFailoverHandler.taskBConnectedToReader"))
                    break

            except Exception:
                pass
        logger.debug(Messages.get_formatted("WriterFailoverHandler.taskBFailedToConnectToAnyReader"))
        sleep(1)

    def is_valid_reader_connection(self, result: ReaderFailoverResult) -> bool:
        if (not result.is_connected | result.connection is None | result.new_host is None):
            return False
        return True

    def refresh_topology_and_connect_to_new_writer(self, initial_writer_host: HostInfo) -> bool:
        while True:
            try:
                PluginService.force_refresh_host_list(self._current_reader_connection)
                topology: List[HostInfo] = PluginService.hosts

                if len(topology) == 0:
                    if len(topology) == 1:
                        logger.debug(Messages.get_formatted("WriterFailoverHandler.standaloneNode"))
                    else:
                        self._current_topology = topology
                        writer_candidate: HostInfo = self.get_writer(self._current_topology)

                        if not self.is_same(writer_candidate, initial_writer_host):
                            logger.debug(Messages.get_formatted(self._current_topology, "[TaskB]"))
                            if self.connect_to_writer(writer_candidate):
                                return True
            except Exception:
                logger.debug(Messages.get_formatted("WriterFailoverHandler.taskBEncounteredException"))
                return False
            sleep(1)

    def is_same(self, host_info: HostInfo, current_host_info: HostInfo) -> bool:
        if (host_info is None | current_host_info is None):
            return False

        return host_info.url == current_host_info.url

    def connect_to_writer(self, writer_candidate: HostInfo, initial_connection_props: Properties) -> bool:
        if self.is_same(writer_candidate, self._current_host):
            logger.debug(Messages.get_formatted("ClusterAwareWriterFailoverHandler.alreadyWriter"))
            self._current_connection = self._current_reader_connection
            return True
        else:
            logger.debug(Messages.get_formatted("ClusterAwareWriterFailoverHandler.taskBAttemptConnectionToNewWriter"))

        try:
            self._current_connection = PluginService.force_connect(writer_candidate, initial_connection_props)
            PluginService.set_availability(writer_candidate.as_aliases, HostAvailability.AVAILABLE)
            return True
        except Exception:
            PluginService.set_availability(writer_candidate.as_aliases, HostAvailability.NOT_AVAILABLE)
            return False

    def close_reader_connection(self) -> None:
        try:
            if self._current_reader_connection is None:
                self._current_reader_connection.close()
        finally:
            self._current_reader_connection = None
            self._current_reader_host = None

    def cleanup(self) -> None:
        if self._current_reader_connection is not None:
            try:
                self._current_reader_connection.close()
            except Exception:
                pass
