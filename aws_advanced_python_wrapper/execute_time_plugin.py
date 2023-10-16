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
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties

from time import perf_counter_ns
from typing import Any, Callable, Set

from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.log import Logger

logger = Logger(__name__)


class ExecuteTimePlugin(Plugin):
    execute_time: int = 0

    @staticmethod
    def reset_execute_time():
        ExecuteTimePlugin.execute_time = 0

    @property
    def subscribed_methods(self) -> Set[str]:
        return {"*"}

    def execute(self, target: type, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        start_time_ns = perf_counter_ns()

        result = execute_func()

        elapsed_time_ns = perf_counter_ns() - start_time_ns
        ExecuteTimePlugin.execute_time += elapsed_time_ns

        logger.debug("ExecuteTimePlugin.ExecuteTime", method_name, elapsed_time_ns)

        return result


class ExecuteTimePluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return ExecuteTimePlugin()
