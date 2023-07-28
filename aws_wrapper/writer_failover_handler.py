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

from aws_wrapper.utils.utils import Utils

if TYPE_CHECKING:
    from aws_wrapper.plugin_service import PluginService
    from aws_wrapper.utils.properties import Properties
    from aws_wrapper.pep249 import Connection
    from aws_wrapper.reader_failover_handler import ReaderFailoverHandler

from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging import getLogger
from threading import Event
from time import sleep
from typing import List, Optional

from aws_wrapper.failover_result import (ReaderFailoverResult,
                                         WriterFailoverResult)
from aws_wrapper.hostinfo import HostAvailability, HostInfo, HostRole
from aws_wrapper.utils.messages import Messages

logger = getLogger(__name__)


class WriterFailoverHandler:
    @abstractmethod
    def failover(self, current_topology: List[HostInfo]) -> WriterFailoverResult:
        pass


class WriterFailoverHandlerImpl(WriterFailoverHandler):
    failed_writer_failover_result = WriterFailoverResult(False, False, None, None, None, None)
    _current_connection: Optional[Connection] = None
    _current_reader_connection: Optional[Connection] = None
    _current_reader_host: Optional[HostInfo] = None

    def __init__(
            self,
            plugin_service: PluginService,
            reader_failover_handler: ReaderFailoverHandler,
            initial_connection_properties: Properties,
            timeout_sec: int = 30,
            read_topology_interval_sec: int = 5,
            reconnect_writer_interval_sec: int = 5):
        self._plugin_service = plugin_service
        self._reader_failover_handler = reader_failover_handler
        self._initial_connection_properties = initial_connection_properties
        self._timeout_sec = timeout_sec
        self._read_topology_interval_sec = read_topology_interval_sec
        self._reconnect_writer_interval_sec = reconnect_writer_interval_sec
        self._timeout_event = Event()

    @property
    def timeout_sec(self):
        return self._timeout_sec

    @timeout_sec.setter
    def timeout_sec(self, value):
        self._timeout_sec = value

    def failover(self, current_topology: List[HostInfo]) -> WriterFailoverResult:
        if current_topology is None or len(current_topology) == 0:
            logger.debug(Messages.get_formatted("WriterFailoverHandler.FailoverCalledWithInvalidTopology"))
            return WriterFailoverHandlerImpl.failed_writer_failover_result
        result: WriterFailoverResult = self.get_result_from_future(current_topology)
        if result.is_connected or result.exception is not None:
            return result

        logger.debug(Messages.get_formatted("WriterFailoverHandler.FailedToConnectToWriterInstance"))
        return WriterFailoverHandlerImpl.failed_writer_failover_result

    def get_writer(self, topology: List[HostInfo]) -> Optional[HostInfo]:
        if topology is None or len(topology) == 0:
            return None

        for host in topology:
            if host.role == HostRole.WRITER:
                return host

        return None

    def get_result_from_future(self, current_topology: List[HostInfo]) -> WriterFailoverResult:
        writer_host: Optional[HostInfo] = self.get_writer(current_topology)
        if writer_host is not None:
            self._plugin_service.set_availability(writer_host.as_aliases(), HostAvailability.NOT_AVAILABLE)
        with ThreadPoolExecutor() as executor:
            if writer_host is not None:
                futures = [executor.submit(self.reconnect_to_writer_handler, writer_host),
                           executor.submit(self.wait_for_new_writer_handler, current_topology, writer_host)]
                try:
                    for future in as_completed(futures, timeout=self.timeout_sec):
                        result = future.result()
                        if result.is_connected or result.exception is not None:
                            executor.shutdown()
                            self.log_task_success(result)
                            return result
                except TimeoutError:
                    self._timeout_event.set()

        return WriterFailoverHandlerImpl.failed_writer_failover_result

    def log_task_success(self, result: WriterFailoverResult) -> None:
        topology: Optional[List[HostInfo]] = result.topology
        if topology is None or len(topology) == 0:
            task_name: Optional[str] = result.task_name if not None else "None"
            logger.error(Messages.get_formatted("ReaderFailoverHandler.FailedReaderConnection", task_name))

    def reconnect_to_writer_handler(self, initial_writer_host: HostInfo):
        logger.debug(Messages.get_formatted("WriterFailoverHandler.TaskAAttemptReconnectToWriterInstance", initial_writer_host.url))

        conn: Optional[Connection] = None
        latest_topology: Optional[List[HostInfo]] = None
        success: bool = False

        try:
            while latest_topology is None or len(latest_topology) == 0:
                try:
                    if conn is not None:
                        conn.close()

                    conn = self._plugin_service.force_connect(initial_writer_host, self._initial_connection_properties, self._timeout_event)
                    self._plugin_service.force_refresh_host_list(conn)
                    latest_topology = self._plugin_service.hosts

                except Exception as err:
                    if self._plugin_service.is_network_exception(err):
                        logger.debug(Messages.get_formatted("WriterFailoverHandler.TaskAEncounteredException", err))
                        return WriterFailoverResult(False, False, None, None, "TaskA", err)

            if latest_topology is None or len(latest_topology) == 0:
                sleep(self._reconnect_writer_interval_sec)

            success = self.is_current_host_writer(latest_topology, initial_writer_host)
            self._plugin_service.set_availability(initial_writer_host.as_aliases(), HostAvailability.AVAILABLE)

            return WriterFailoverResult(success, False, latest_topology, conn if success else None, "TaskA", None)

        except Exception as err:
            logger.error(Messages.get_formatted("WriterFailoverHandler.TaskAEncounteredException", err))
            return WriterFailoverResult(False, False, None, None, "TaskA", None)

        finally:
            try:
                if conn is not None and not success:
                    conn.close()
            except Exception:
                pass
            logger.debug(Messages.get_formatted("WriterFailoverHandler.TaskAFinished"))

    def is_current_host_writer(self, latest_topology: List[HostInfo], initial_writer_host: HostInfo) -> bool:
        latest_writer: Optional[HostInfo] = self.get_writer(latest_topology)
        if latest_writer is not None:
            latest_writer_all_aliases: frozenset[str] = latest_writer.all_aliases
            current_aliases: frozenset[str] = initial_writer_host.all_aliases

        return latest_writer is not None and bool(current_aliases.intersection(latest_writer_all_aliases))

    def wait_for_new_writer_handler(self, current_topology: List[HostInfo], current_host: HostInfo) -> WriterFailoverResult:
        logger.debug(Messages.get_formatted("WriterFailoverHandler.TaskBAttemptConnectionToNewWriterInstance"))
        try:
            success: bool = False
            while not success:
                self.connect_to_reader(current_topology)
                success = self.refresh_topology_and_connect_to_new_writer(current_host)
                if not success:
                    self.close_reader_connection()
                return WriterFailoverResult(True, True, current_topology, self._current_connection, "TaskB", None)

        except InterruptedError:
            return WriterFailoverResult(False, False, None, None, "TaskB", None)
        except Exception:
            logger.debug(Messages.get_formatted("WriterFailoverHandler.TaskBEncounteredException"))
        finally:
            self.cleanup()
            logger.error(Messages.get_formatted("WriterFailoverHandler.TaskBFinished"))

        return WriterFailoverHandlerImpl.failed_writer_failover_result

    def connect_to_reader(self, current_topology: List[HostInfo]) -> None:
        while True:
            try:
                conn_result: ReaderFailoverResult = self._reader_failover_handler.get_reader_connection(current_topology)
                if (self.is_valid_reader_connection(conn_result)):
                    self._current_reader_connection = conn_result.connection
                    self._current_reader_host = conn_result.new_host
                    logger.debug(Messages.get_formatted("WriterFailoverHandler.TaskBConnectedToReader"))
                    break

            except Exception:
                pass
        logger.debug(Messages.get_formatted("WriterFailoverHandler.TaskBFailedToConnectToAnyReader"))
        sleep(1)

    def is_valid_reader_connection(self, result: ReaderFailoverResult) -> bool:
        if result.is_connected or result.connection is not None or result.new_host is not None:
            return False

        return True

    def refresh_topology_and_connect_to_new_writer(self, initial_writer_host: HostInfo) -> bool:
        while True:
            try:
                self._plugin_service.force_refresh_host_list(self._current_reader_connection)
                current_topology: List[HostInfo] = self._plugin_service.hosts

                if len(current_topology) == 0:
                    if len(current_topology) == 1:
                        # currently connected reader is in the middle of failover. It is not yet connected to a new writer and works as a standalone
                        # node. The handler must wait until the reader connects to the entire cluster to fetch the cluster topology
                        logger.debug(Messages.get_formatted("WriterFailoverHandler.StandaloneNode"))
                    else:
                        writer_candidate: Optional[HostInfo] = self.get_writer(current_topology)

                        if not self.is_same(writer_candidate, initial_writer_host):
                            # new writer available
                            logger.debug(Utils.log_topology(current_topology, "[TaskB] "))

                            if self.connect_to_writer(writer_candidate):
                                return True
            except Exception as err:
                logger.error(Messages.get_formatted("WriterFailoverHandler.TaskBEncounteredException", err))
                return False

            sleep(self._read_topology_interval_sec)

    def is_same(self, host_info: Optional[HostInfo], current_host_info: Optional[HostInfo]) -> bool:
        if host_info is None or current_host_info is None:
            return False

        return host_info.url == current_host_info.url

    def connect_to_writer(self, writer_candidate: Optional[HostInfo]) -> bool:
        if self.is_same(writer_candidate, self._current_reader_host):
            logger.debug(Messages.get_formatted("WriterFailoverHandler.AlreadyWriter"))
            self._current_connection = self._current_reader_connection
            return True
        else:
            writer_candidate_url: str = "None"
            if writer_candidate is not None:
                writer_candidate_url = writer_candidate.url
            logger.debug(Messages.get_formatted("WriterFailoverHandler.TaskBAttemptConnectionToNewWriter", writer_candidate_url))

        try:
            # connect to new writer
            if writer_candidate is not None:
                self._current_connection = self._plugin_service.force_connect(writer_candidate,
                                                                              self._initial_connection_properties,
                                                                              self._timeout_event)
                self._plugin_service.set_availability(writer_candidate.as_aliases(), HostAvailability.AVAILABLE)
                return True
        except Exception:
            if writer_candidate is not None:
                self._plugin_service.set_availability(writer_candidate.as_aliases(), HostAvailability.NOT_AVAILABLE)

        return False

    def close_reader_connection(self) -> None:
        try:
            if self._current_reader_connection is not None:
                self._current_reader_connection.close()
        except Exception:
            pass
        finally:
            self._current_reader_connection = None
            self._current_reader_host = None

    def cleanup(self) -> None:
        if self._current_reader_connection is not None and self._current_connection is not self._current_reader_connection:
            try:
                self._current_reader_connection.close()
            except Exception:
                pass
