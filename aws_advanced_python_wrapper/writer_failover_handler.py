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

from typing import TYPE_CHECKING, Tuple

from aws_advanced_python_wrapper.utils.properties import PropertiesUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.reader_failover_handler import ReaderFailoverHandler

from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from threading import Event
from time import sleep
from typing import Optional

from aws_advanced_python_wrapper import LogUtils
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.failover_result import (ReaderFailoverResult,
                                                         WriterFailoverResult)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages

logger = Logger(__name__)


class WriterFailoverHandler:
    """
    Interface for Writer Failover Process handler.
    This handler implements all necessary logic to try to reconnect to a current writer host or to a newly elected writer.
    """

    @abstractmethod
    def failover(self, current_topology: Tuple[HostInfo, ...]) -> WriterFailoverResult:
        """
        Called to start Writer Failover Process. This process tries to establish a connection to a writer instance after failover has occurred.

        :param current_topology: The current cluster topology.
        :return: The results of this process.
        """
        pass


class WriterFailoverHandlerImpl(WriterFailoverHandler):
    """
    An implementation of :py:class`WriterFailoverHandler`.

    Writer Failover Process goal is to re-establish connection to a writer.
    Connection to a writer may be disrupted either by temporary network issue, or due to writer host unavailability during cluster failover.
    This handler tries two approaches in parallel:
        1. try to re-connect to the writer host that originally failed.
        2. try to update the cluster topology using a reader connection, and use that information to connect to a newly elected writer.
    """
    failed_writer_failover_result = WriterFailoverResult(False, False, None, None, None, None)
    _current_connection: Optional[Connection] = None
    _current_topology: Optional[Tuple[HostInfo, ...]] = None
    _current_reader_connection: Optional[Connection] = None
    _current_reader_host: Optional[HostInfo] = None

    def __init__(
            self,
            plugin_service: PluginService,
            reader_failover_handler: ReaderFailoverHandler,
            initial_connection_properties: Properties,
            max_timeout_sec: float = 60,
            read_topology_interval_sec: float = 5,
            reconnect_writer_interval_sec: float = 5):
        self._plugin_service = plugin_service
        self._reader_failover_handler = reader_failover_handler
        self._initial_connection_properties = initial_connection_properties
        self._max_failover_timeout_sec = max_timeout_sec
        self._read_topology_interval_sec = read_topology_interval_sec
        self._reconnect_writer_interval_sec = reconnect_writer_interval_sec
        self._timeout_event = Event()

    def failover(self, current_topology: Tuple[HostInfo, ...]) -> WriterFailoverResult:
        if current_topology is None or len(current_topology) == 0:
            logger.debug("WriterFailoverHandler.FailoverCalledWithInvalidTopology")
            return WriterFailoverHandlerImpl.failed_writer_failover_result

        result: WriterFailoverResult = self.get_result_from_future(current_topology)
        if result.is_connected or result.exception is not None:
            return result

        logger.debug("WriterFailoverHandler.FailedToConnectToWriterInstance")
        return WriterFailoverHandlerImpl.failed_writer_failover_result

    def get_writer(self, topology: Tuple[HostInfo, ...]) -> Optional[HostInfo]:
        if topology is None or len(topology) == 0:
            return None

        for host in topology:
            if host.role == HostRole.WRITER:
                return host

        return None

    def get_result_from_future(self, current_topology: Tuple[HostInfo, ...]) -> WriterFailoverResult:
        writer_host: Optional[HostInfo] = self.get_writer(current_topology)
        if writer_host is not None:
            self._plugin_service.set_availability(writer_host.as_aliases(), HostAvailability.UNAVAILABLE)

            with ThreadPoolExecutor(thread_name_prefix="WriterFailoverHandlerExecutor") as executor:
                try:
                    futures = [executor.submit(self.reconnect_to_writer, writer_host),
                               executor.submit(self.wait_for_new_writer, current_topology, writer_host)]
                    for future in as_completed(futures, timeout=self._max_failover_timeout_sec):
                        result = future.result()
                        if result.is_connected:
                            executor.shutdown(wait=False)
                            self.log_task_success(result)
                            return result
                        if result.exception is not None:
                            executor.shutdown(wait=False)
                            return result
                except TimeoutError:
                    self._timeout_event.set()
                finally:
                    self._timeout_event.set()

        return WriterFailoverHandlerImpl.failed_writer_failover_result

    def log_task_success(self, result: WriterFailoverResult) -> None:
        topology: Optional[Tuple[HostInfo, ...]] = result.topology
        if topology is None or len(topology) == 0:
            task_name: Optional[str] = result.task_name if not None else "None"
            logger.error("WriterFailoverHandler.SuccessfulConnectionInvalidTopology", task_name)
            return

        writer_host: Optional[HostInfo] = self.get_writer(topology)
        new_writer_host: Optional[str] = "None" if writer_host is None else writer_host.url
        if result.is_new_host:
            logger.debug("WriterFailoverHandler.SuccessfullyConnectedToNewWriterInstance", new_writer_host)
        else:
            logger.debug("WriterFailoverHandler.SuccessfullyReconnectedToWriterInstance", new_writer_host)

    def reconnect_to_writer(self, initial_writer_host: HostInfo):
        """
        Task A: Attempt to reconnect to the writer that originally failed.

        :param initial_writer_host: the writer host that originally failed.
        :return: the :py:class:`WriterFailoverResult` of the failover process.
        """
        logger.debug("WriterFailoverHandler.TaskAAttemptReconnectToWriterInstance", initial_writer_host.url,
                     PropertiesUtils.mask_properties(self._initial_connection_properties))

        conn: Optional[Connection] = None
        latest_topology: Optional[Tuple[HostInfo, ...]] = None
        success: bool = False

        try:
            while not self._timeout_event.is_set() and (latest_topology is None or len(latest_topology) == 0):
                try:
                    if conn is not None:
                        conn.close()

                    conn = self._plugin_service.force_connect(initial_writer_host, self._initial_connection_properties, self._timeout_event)
                    self._plugin_service.force_refresh_host_list(conn)
                    latest_topology = self._plugin_service.all_hosts

                except Exception as ex:
                    if not self._plugin_service.is_network_exception(ex):
                        logger.debug("WriterFailoverHandler.TaskAEncounteredException", ex)
                        return WriterFailoverResult(False, False, None, None, "TaskA", ex)

                if latest_topology is None or len(latest_topology) == 0:
                    sleep(self._reconnect_writer_interval_sec)
                else:
                    success = self.is_current_host_writer(latest_topology, initial_writer_host)

            self._plugin_service.set_availability(initial_writer_host.as_aliases(), HostAvailability.AVAILABLE)
            return WriterFailoverResult(success, False, latest_topology, conn if success else None, "TaskA", None)

        except Exception as ex:
            logger.error("WriterFailoverHandler.TaskAEncounteredException", ex)
            return WriterFailoverResult(False, False, None, None, "TaskA", None)

        finally:
            try:
                if conn is not None and not success:
                    conn.close()
            except Exception:
                pass
            logger.debug("WriterFailoverHandler.TaskAFinished")

    def is_current_host_writer(self, latest_topology: Tuple[HostInfo, ...], initial_writer_host: HostInfo) -> bool:
        latest_writer: Optional[HostInfo] = self.get_writer(latest_topology)
        if latest_writer is None:
            return False

        latest_writer_all_aliases: frozenset[str] = latest_writer.all_aliases
        current_aliases: frozenset[str] = initial_writer_host.all_aliases

        return current_aliases is not None and len(current_aliases) > 0 and bool(current_aliases.intersection(latest_writer_all_aliases))

    def wait_for_new_writer(self, current_topology: Tuple[HostInfo, ...], current_host: HostInfo) -> WriterFailoverResult:
        """
        Task B: Attempt to connect to a newly elected writer.
        :param current_topology: the latest topology.
        :param current_host: the current host.
        :return: the :py:class:`WriterFailoverResult` of the process.
        """
        logger.debug("WriterFailoverHandler.TaskBAttemptConnectionToNewWriterInstance",
                     PropertiesUtils.mask_properties(self._initial_connection_properties))
        self._current_topology = current_topology
        try:
            success: bool = False
            while not self._timeout_event.is_set() and not success:
                self.connect_to_reader()
                success = self.refresh_topology_and_connect_to_new_writer(current_host)
                if not success:
                    self.close_reader_connection()
            return WriterFailoverResult(True, True, self._current_topology, self._current_connection, "TaskB", None)

        except InterruptedError:
            return WriterFailoverResult(False, False, None, None, "TaskB", None)
        except Exception as ex:
            logger.debug("WriterFailoverHandler.TaskBEncounteredException", ex)
            raise ex
        finally:
            self.cleanup()
            logger.debug("WriterFailoverHandler.TaskBFinished")

    def connect_to_reader(self) -> None:
        while not self._timeout_event.is_set():
            if self._current_topology is None:
                raise AwsWrapperError(Messages.get("WriterFailoverHandler.CurrentTopologyNone"))
            try:
                conn_result: ReaderFailoverResult = self._reader_failover_handler.get_reader_connection(self._current_topology)

                # check if valid reader connection
                if conn_result.is_connected and conn_result.connection is not None and conn_result.new_host is not None:
                    self._current_reader_connection = conn_result.connection
                    self._current_reader_host = conn_result.new_host
                    logger.debug("WriterFailoverHandler.TaskBConnectedToReader", self._current_reader_host.url)
                    break

            except Exception:
                pass
            logger.debug("WriterFailoverHandler.TaskBFailedToConnectToAnyReader")
            sleep(1)

    def refresh_topology_and_connect_to_new_writer(self, initial_writer_host: HostInfo) -> bool:
        """
        Re-fetch topology and wait for a new writer.
        :param initial_writer_host: the writer host that originally failed.
        :return: `True` if a connection to a newly elected writer was  successfully established. `False` otherwise.
        """
        while not self._timeout_event.is_set():
            try:
                self._plugin_service.force_refresh_host_list(self._current_reader_connection)
                current_topology: Tuple[HostInfo, ...] = self._plugin_service.all_hosts

                if len(current_topology) > 0:
                    if len(current_topology) == 1:
                        # currently connected reader is in the middle of failover. It is not yet connected to a new writer and works
                        # as a standalone host. The handler must wait until the reader connects to the entire cluster to fetch the
                        # cluster topology
                        logger.debug("WriterFailoverHandler.StandaloneHost",
                                     "None" if self._current_reader_host is None else self._current_reader_host.url)
                    else:
                        self._current_topology = current_topology
                        writer_candidate: Optional[HostInfo] = self.get_writer(self._current_topology)

                        if not self.is_same(writer_candidate, initial_writer_host):
                            # new writer available
                            logger.debug("LogUtils.Topology", LogUtils.log_topology(self._current_topology, "[TaskB]"))

                            if self.connect_to_writer(writer_candidate):
                                return True
            except Exception as ex:
                logger.debug("WriterFailoverHandler.TaskBEncounteredException", ex)
                return False

            sleep(self._read_topology_interval_sec)

        return False

    def is_same(self, host_info: Optional[HostInfo], current_host_info: Optional[HostInfo]) -> bool:
        if host_info is None or current_host_info is None:
            return False

        return host_info.url == current_host_info.url

    def connect_to_writer(self, writer_candidate: Optional[HostInfo]) -> bool:
        if self.is_same(writer_candidate, self._current_reader_host):
            logger.debug("WriterFailoverHandler.AlreadyWriter")
            self._current_connection = self._current_reader_connection
            return True
        else:
            logger.debug("WriterFailoverHandler.TaskBAttemptConnectionToNewWriter", "None" if writer_candidate is None else writer_candidate.url)

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
                self._plugin_service.set_availability(writer_candidate.as_aliases(), HostAvailability.UNAVAILABLE)

        return False

    def close_reader_connection(self) -> None:
        """
        Close the reader connection if not done so already, and set the relevant fields to None.
        """
        try:
            if self._current_reader_connection is not None:
                self._current_reader_connection.close()
        except Exception:
            pass

        finally:
            self._current_reader_connection = None
            self._current_reader_host = None

    def cleanup(self) -> None:
        """
        Close the reader connection if it's not needed.
        """
        if self._current_reader_connection is not None and self._current_connection is not self._current_reader_connection:
            try:
                self._current_reader_connection.close()
            except Exception:
                pass
