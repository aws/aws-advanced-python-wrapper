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

from typing import TYPE_CHECKING, Callable, List, Optional

from aws_advanced_python_wrapper.errors import ReadWriteSplittingError
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.read_write_splitting_plugin import \
    ReadWriteSplittingPlugin
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService

logger = Logger(__name__)


class GdbReadWriteSplittingPlugin(ReadWriteSplittingPlugin):
    """Read/write splitting plugin for Aurora Global Databases.

    Extends the topology-based :class:`ReadWriteSplittingPlugin` to keep
    reader and writer connections inside a configured home region. When
    enabled, the plugin will refuse to switch to a writer or reader instance
    that lives outside that region. Optionally, when Global Write
    Forwarding is enabled, the plugin will keep the existing reader
    connection in a secondary region instead of failing.
    """

    def __init__(self, plugin_service: PluginService, props: Properties):
        super().__init__(plugin_service, props)

        self._rds_utils: RdsUtils = RdsUtils()
        self._restrict_writer_to_home_region: bool = (
            WrapperProperties.GDB_RW_RESTRICT_WRITER_TO_HOME_REGION.get_bool(props)
        )
        self._restrict_reader_to_home_region: bool = (
            WrapperProperties.GDB_RW_RESTRICT_READER_TO_HOME_REGION.get_bool(props)
        )
        self._enable_global_write_forwarding: bool = (
            WrapperProperties.GDB_ENABLE_GLOBAL_WRITE_FORWARDING.get_bool(props)
        )
        self._home_region: Optional[str] = None
        self._initialized: bool = False

    def connect(
        self,
        target_driver_func: Callable,
        driver_dialect: DriverDialect,
        host_info: HostInfo,
        props: Properties,
        is_initial_connection: bool,
        connect_func: Callable,
    ) -> Connection:
        self._init_settings(host_info, props)
        return super().connect(
            target_driver_func,
            driver_dialect,
            host_info,
            props,
            is_initial_connection,
            connect_func,
        )

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
                    init_host_info.host,
                )
            )

        self._home_region = home_region
        logger.debug(
            "GdbReadWriteSplittingPlugin.ParameterValue",
            WrapperProperties.GDB_RW_HOME_REGION.name,
            self._home_region,
        )

    def _initialize_writer_connection(self) -> None:
        writer_host = self._get_writer_host_info()
        if writer_host is not None and self._is_writer_outside_home_region(writer_host):
            if self._enable_global_write_forwarding:
                logger.debug(
                    "GdbReadWriteSplittingPlugin.EnabledGwf",
                    self._rds_utils.get_rds_region(writer_host.host),
                )
                return

            raise ReadWriteSplittingError(
                Messages.get_formatted(
                    "GdbReadWriteSplittingPlugin.CantConnectWriterOutOfHomeRegion",
                    writer_host.host,
                    self._home_region,
                )
            )

        super()._initialize_writer_connection()

    def _set_writer_connection(
        self, writer_conn: Connection, writer_host_info: HostInfo
    ) -> None:
        if self._is_writer_outside_home_region(writer_host_info):
            self._close_connection(writer_conn)
            raise ReadWriteSplittingError(
                Messages.get_formatted(
                    "GdbReadWriteSplittingPlugin.CantConnectWriterOutOfHomeRegion",
                    writer_host_info.host,
                    self._home_region,
                )
            )
        super()._set_writer_connection(writer_conn, writer_host_info)

    def _get_reader_host_candidates(self) -> List[HostInfo]:
        if not self._restrict_reader_to_home_region:
            return super()._get_reader_host_candidates()

        hosts_in_region = [
            host
            for host in self._plugin_service.hosts
            if self._is_in_home_region(host)
        ]

        if not hosts_in_region:
            raise ReadWriteSplittingError(
                Messages.get_formatted(
                    "GdbReadWriteSplittingPlugin.NoAvailableReadersInHomeRegion",
                    self._home_region,
                )
            )

        return hosts_in_region

    def _is_writer_outside_home_region(self, host_info: HostInfo) -> bool:
        return (
            self._restrict_writer_to_home_region
            and not self._is_in_home_region(host_info)
        )

    def _is_in_home_region(self, host_info: HostInfo) -> bool:
        if self._home_region is None:
            return True
        host_region = self._rds_utils.get_rds_region(host_info.host)
        if host_region is None:
            return False
        return host_region.casefold() == self._home_region.casefold()


class GdbReadWriteSplittingPluginFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service: PluginService, props: Properties) -> Plugin:
        return GdbReadWriteSplittingPlugin(plugin_service, props)
