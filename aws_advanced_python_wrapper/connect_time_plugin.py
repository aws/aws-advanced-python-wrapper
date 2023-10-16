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

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties

from logging import getLogger
from time import perf_counter_ns
from typing import Callable, Set

from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.messages import Messages

logger = getLogger(__name__)


class ConnectTimePlugin(Plugin):
    connect_time: int = 0

    @staticmethod
    def reset_connect_time():
        ConnectTimePlugin.connect_time = 0

    @property
    def subscribed_methods(self) -> Set[str]:
        return {"connect", "force_connect"}

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        start_time_ns = perf_counter_ns()

        result = connect_func()

        elapsed_time_ns = perf_counter_ns() - start_time_ns
        ConnectTimePlugin.connect_time += elapsed_time_ns

        logger.debug(Messages.get_formatted("ConnectTimePlugin.ConnectTime", elapsed_time_ns))

        return result


class ConnectTimePluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return ConnectTimePlugin()
