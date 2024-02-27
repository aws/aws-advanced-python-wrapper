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

from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.connection_provider import \
        ConnectionProvider
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.exception_handling import ExceptionHandler
    from aws_advanced_python_wrapper.plugin import PluginFactory

from aws_advanced_python_wrapper.utils.properties import Properties


class ConfigurationProfile:
    def __init__(
            self,
            name: str,
            properties: Properties = Properties(),
            plugin_factories: List[PluginFactory] = [],
            dialect: Optional[DatabaseDialect] = None,
            target_driver_dialect: Optional[DriverDialect] = None,
            exception_handler: Optional[ExceptionHandler] = None,
            connection_provider: Optional[ConnectionProvider] = None):
        self._name = name
        self._plugin_factories = plugin_factories
        self._properties = properties
        self._database_dialect = dialect
        self._target_driver_dialect = target_driver_dialect
        self._exception_handler = exception_handler
        self._connection_provider = connection_provider

    @property
    def name(self) -> str:
        return self._name

    @property
    def properties(self) -> Properties:
        return self._properties

    @property
    def plugin_factories(self) -> List[PluginFactory]:
        return self._plugin_factories

    @property
    def database_dialect(self) -> Optional[DatabaseDialect]:
        return self._database_dialect

    @property
    def target_driver_dialect(self) -> Optional[DriverDialect]:
        return self._target_driver_dialect

    @property
    def connection_provider(self) -> Optional[ConnectionProvider]:
        return self._connection_provider
