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

"""Async read/write splitting plugin for Aurora Global Databases.

Async counterpart of :class:`GdbReadWriteSplittingPlugin`. Extends the async
topology-based :class:`AsyncReadWriteSplittingPlugin` to keep reader and writer
connections inside a configured home region. When enabled it refuses to switch
to a writer or reader instance outside that region; when Global Write
Forwarding is enabled it keeps the existing reader connection in a secondary
region instead of failing.

The base-class override points are :meth:`_select_reader_candidates` (reader
restriction) and :meth:`_should_switch_to_writer` (writer restriction / GWF),
mirroring the sync plugin's ``_get_reader_host_candidates`` /
``_initialize_writer_connection`` / ``_set_writer_connection``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Awaitable, Callable, List, Optional

from aws_advanced_python_wrapper.aio.read_write_splitting_plugin import \
    AsyncReadWriteSplittingPlugin
from aws_advanced_python_wrapper.errors import ReadWriteSplittingError
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncHostListProvider
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo

logger = Logger(__name__)


class AsyncGdbReadWriteSplittingPlugin(AsyncReadWriteSplittingPlugin):
    """Async Global-Database read/write splitting plugin."""

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            host_list_provider: AsyncHostListProvider,
            props: Properties) -> None:
        super().__init__(plugin_service, host_list_provider, props)
        self._rds_utils = RdsUtils()
        self._restrict_writer_to_home_region: bool = (
            WrapperProperties.GDB_RW_RESTRICT_WRITER_TO_HOME_REGION.get_bool(props))
        self._restrict_reader_to_home_region: bool = (
            WrapperProperties.GDB_RW_RESTRICT_READER_TO_HOME_REGION.get_bool(props))
        self._enable_global_write_forwarding: bool = (
            WrapperProperties.GDB_ENABLE_GLOBAL_WRITE_FORWARDING.get_bool(props))
        self._home_region: Optional[str] = None
        self._initialized: bool = False

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        self._init_settings(host_info, props)
        return await super().connect(
            target_driver_func, driver_dialect, host_info, props,
            is_initial_connection, connect_func)

    def _init_settings(self, init_host_info: HostInfo, props: Properties) -> None:
        if self._initialized:
            return
        self._initialized = True

        home_region = WrapperProperties.GDB_RW_HOME_REGION.get(props)
        if not home_region:
            url_type = self._rds_utils.identify_rds_type(init_host_info.host)
            if url_type is not None and url_type.has_region:
                home_region = self._rds_utils.get_rds_region(init_host_info.host)

        if not home_region:
            raise ReadWriteSplittingError(
                Messages.get_formatted(
                    "GdbReadWriteSplittingPlugin.MissingHomeRegion",
                    init_host_info.host))

        self._home_region = home_region
        logger.debug(
            "GdbReadWriteSplittingPlugin.ParameterValue",
            WrapperProperties.GDB_RW_HOME_REGION.name, self._home_region)

    def _select_reader_candidates(self, topology: tuple) -> List[HostInfo]:
        readers = super()._select_reader_candidates(topology)
        if not self._restrict_reader_to_home_region:
            return readers
        in_region = [h for h in readers if self._is_in_home_region(h)]
        if not in_region:
            raise ReadWriteSplittingError(
                Messages.get_formatted(
                    "GdbReadWriteSplittingPlugin.NoAvailableReadersInHomeRegion",
                    self._home_region))
        return in_region

    async def _should_switch_to_writer(self, writer_host: HostInfo) -> bool:
        if self._is_writer_outside_home_region(writer_host):
            if self._enable_global_write_forwarding:
                # Keep the current (reader) connection; writes are forwarded.
                logger.debug(
                    "GdbReadWriteSplittingPlugin.EnabledGwf",
                    self._rds_utils.get_rds_region(writer_host.host))
                return False
            raise ReadWriteSplittingError(
                Messages.get_formatted(
                    "GdbReadWriteSplittingPlugin.CantConnectWriterOutOfHomeRegion",
                    writer_host.host, self._home_region))
        return True

    def _is_writer_outside_home_region(self, host_info: HostInfo) -> bool:
        return (self._restrict_writer_to_home_region
                and not self._is_in_home_region(host_info))

    def _is_in_home_region(self, host_info: HostInfo) -> bool:
        if self._home_region is None:
            return True
        host_region = self._rds_utils.get_rds_region(host_info.host)
        if host_region is None:
            return False
        return host_region.casefold() == self._home_region.casefold()
