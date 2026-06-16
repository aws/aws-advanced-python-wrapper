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

import time
from typing import TYPE_CHECKING, Callable, List, Optional

from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.utils import LogUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin import Plugin
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties

logger = Logger(__name__)


class RetryUtil:
    _SHORT_DELAY_SEC = 0.1

    class Results:
        def __init__(self, connection: Connection, host_info: HostInfo):
            self._connection = connection
            self._host_info = host_info

        @property
        def connection(self) -> Connection:
            return self._connection

        @property
        def host_info(self) -> HostInfo:
            return self._host_info

    def get_writer_connection(
            self,
            plugin_service: PluginService,
            properties: Properties,
            plugin: Optional[Plugin],
            verify_role: bool,
            timeout_end_time: float) -> RetryUtil.Results:
        def allowed_writer_hosts() -> Optional[List[HostInfo]]:
            updated_hosts = plugin_service.all_hosts
            writer = next((host for host in updated_hosts if host.role == HostRole.WRITER), None)
            if writer is None:
                logger.debug("RetryUtil.NoWriterHost", LogUtils.log_topology(updated_hosts))
                return None

            allowed_hosts = plugin_service.hosts
            if not any(host.host == writer.host and host.port == writer.port for host in allowed_hosts):
                logger.debug("RetryUtil.NewWriterNotAllowed", writer.host, LogUtils.log_topology(allowed_hosts))
                return None
            return [writer]

        return self.get_allowed_connection(
            plugin_service,
            properties,
            plugin,
            allowed_writer_hosts,
            None,
            HostRole.WRITER if verify_role else None,
            timeout_end_time)

    def get_allowed_connection(
            self,
            plugin_service: PluginService,
            properties: Properties,
            plugin: Optional[Plugin],
            allowed_hosts: Callable[[], Optional[List[HostInfo]]],
            strategy: Optional[str],
            verify_role: Optional[HostRole],
            retry_end_time: float) -> RetryUtil.Results:
        if strategy is None or not strategy.strip():
            strategy = "random"

        candidate_conn: Optional[Connection] = None
        try:
            while time.time() < retry_end_time:
                # The roles in this list might not be accurate, depending on whether the new
                # topology has become available yet.
                plugin_service.refresh_host_list()
                updated_allowed_hosts = allowed_hosts()
                if updated_allowed_hosts is None:
                    self._short_delay()
                    continue

                # Make a copy of hosts and mark them available so the host selector considers them.
                remaining_hosts = [self._available_copy(host) for host in updated_allowed_hosts]
                if not remaining_hosts:
                    self._short_delay()
                    continue

                while remaining_hosts and time.time() < retry_end_time:
                    candidate_host = None
                    try:
                        # The host selector requires a non-null role, so default to READER when
                        # no specific role needs to be verified.
                        candidate_host = plugin_service.get_host_info_by_strategy(
                            verify_role if verify_role is not None else HostRole.READER,
                            strategy,
                            remaining_hosts)
                    except Exception:
                        # Strategy can't get a host according to the requested conditions.
                        # Do nothing
                        pass

                    if candidate_host is None:
                        logger.debug("RetryUtil.CandidateNone", verify_role)
                        self._short_delay()
                        break  # Exit loop over remaining_hosts and refresh topology.

                    try:
                        candidate_conn = plugin_service.connect(candidate_host, properties, plugin)
                        # Roles in the host list might be stale, so verify the role with a query.
                        role = plugin_service.get_host_role(candidate_conn) if verify_role is not None else None
                        if verify_role is None or verify_role == role:
                            if role is not None and role != candidate_host.role:
                                updated_host_info = candidate_host.__copy__()
                                updated_host_info.role = role
                            else:
                                updated_host_info = candidate_host
                            result = RetryUtil.Results(candidate_conn, updated_host_info)
                            candidate_conn = None  # Prevents closing the returned connection below.
                            return result
                    except Exception as ex:
                        logger.debug("RetryUtil.ExceptionConnectingToWriter", candidate_host.host, ex)

                    # The connection couldn't be opened or the role is not as expected, so it is not valid.
                    remaining_hosts = [host for host in remaining_hosts
                                       if not (host.host == candidate_host.host and host.port == candidate_host.port)]
                    if candidate_conn is not None:
                        self.close_connection(plugin_service, candidate_conn)
                        candidate_conn = None

            raise TimeoutError(Messages.get("RetryUtil.Timeout"))
        finally:
            if candidate_conn is not None:
                self.close_connection(plugin_service, candidate_conn)

    @staticmethod
    def _available_copy(host: HostInfo) -> HostInfo:
        host_copy = host.__copy__()
        host_copy.set_availability(HostAvailability.AVAILABLE)
        return host_copy

    @staticmethod
    def close_connection(plugin_service: PluginService, conn: Connection) -> None:
        try:
            plugin_service.driver_dialect.execute(
                DbApiMethod.CONNECTION_CLOSE.method_name, lambda: conn.close())
        except Exception:
            pass

    def _short_delay(self) -> None:
        time.sleep(self._SHORT_DELAY_SEC)
