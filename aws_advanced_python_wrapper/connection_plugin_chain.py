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
    from aws_advanced_python_wrapper.connection_provider import ConnectionProviderManager
    from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
    from aws_advanced_python_wrapper.plugin_service import PluginService

from typing import Dict, List, Type

from aws_advanced_python_wrapper.aurora_connection_tracker_plugin import \
    AuroraConnectionTrackerPluginFactory
from aws_advanced_python_wrapper.aws_secrets_manager_plugin import \
    AwsSecretsManagerPluginFactory
from aws_advanced_python_wrapper.connect_time_plugin import \
    ConnectTimePluginFactory
from aws_advanced_python_wrapper.default_plugin import DefaultPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.execute_time_plugin import \
    ExecuteTimePluginFactory
from aws_advanced_python_wrapper.failover_plugin import FailoverPluginFactory
from aws_advanced_python_wrapper.host_monitoring_plugin import \
    HostMonitoringPluginFactory
from aws_advanced_python_wrapper.iam_plugin import IamAuthPluginFactory
from aws_advanced_python_wrapper.read_write_splitting_plugin import \
    ReadWriteSplittingPluginFactory
from aws_advanced_python_wrapper.stale_dns_plugin import StaleDnsPluginFactory
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

PLUGIN_FACTORIES: Dict[str, Type[PluginFactory]] = {
    "iam": IamAuthPluginFactory,
    "aws_secrets_manager": AwsSecretsManagerPluginFactory,
    "aurora_connection_tracker": AuroraConnectionTrackerPluginFactory,
    "host_monitoring": HostMonitoringPluginFactory,
    "failover": FailoverPluginFactory,
    "read_write_splitting": ReadWriteSplittingPluginFactory,
    "stale_dns": StaleDnsPluginFactory,
    "connect_time": ConnectTimePluginFactory,
    "execute_time": ExecuteTimePluginFactory,
}

WEIGHT_RELATIVE_TO_PRIOR_PLUGIN = -1

PLUGIN_FACTORY_WEIGHTS: Dict[Type[PluginFactory], int] = {
    AuroraConnectionTrackerPluginFactory: 100,
    StaleDnsPluginFactory: 200,
    ReadWriteSplittingPluginFactory: 300,
    FailoverPluginFactory: 400,
    HostMonitoringPluginFactory: 500,
    IamAuthPluginFactory: 600,
    AwsSecretsManagerPluginFactory: 700,
    ConnectTimePluginFactory: WEIGHT_RELATIVE_TO_PRIOR_PLUGIN,
    ExecuteTimePluginFactory: WEIGHT_RELATIVE_TO_PRIOR_PLUGIN
}

logger = Logger(__name__)


def get_plugins(
        plugin_service: PluginService,
        connection_provider_manager: ConnectionProviderManager,
        props: Properties) -> List[Plugin]:
    plugins: List[Plugin] = []

    plugin_codes = WrapperProperties.PLUGINS.get(props)
    if plugin_codes is None:
        plugin_codes = WrapperProperties.DEFAULT_PLUGINS

    if plugin_codes != "":
        plugins = create_plugins_from_list(plugin_codes.split(","), plugin_service, props)

    plugins.append(DefaultPlugin(plugin_service, connection_provider_manager))

    return plugins


def create_plugins_from_list(
        plugin_code_list: List[str],
        plugin_service: PluginService,
        props: Properties) -> List[Plugin]:

    factory_types: List[Type[PluginFactory]] = []
    for plugin_code in plugin_code_list:
        plugin_code = plugin_code.strip()
        if plugin_code not in PLUGIN_FACTORIES:
            raise AwsWrapperError(Messages.get_formatted("PluginManager.InvalidPlugin", plugin_code))

        factory_types.append(PLUGIN_FACTORIES[plugin_code])

    if len(factory_types) <= 0:
        return []

    auto_sort_plugins = WrapperProperties.AUTO_SORT_PLUGIN_ORDER.get(props)
    if auto_sort_plugins:
        weights = get_factory_weights(factory_types)
        factory_types.sort(key=lambda factory_type: weights[factory_type])
        plugin_code_list.sort(key=lambda plugin_code: weights[PLUGIN_FACTORIES[plugin_code]])
        logger.debug("ConnectionPluginChain.ResortedPlugins", plugin_code_list)

    plugins: List[Plugin] = []
    for factory_type in factory_types:
        factory = object.__new__(factory_type)
        plugin = factory.get_instance(plugin_service, props)
        plugins.append(plugin)

    return plugins


def get_factory_weights(factory_types: List[Type[PluginFactory]]) -> Dict[Type[PluginFactory], int]:
    last_weight = 0
    weights: Dict[Type[PluginFactory], int] = {}
    for factory_type in factory_types:
        weight = PLUGIN_FACTORY_WEIGHTS[factory_type]
        if weight is None or weight == WEIGHT_RELATIVE_TO_PRIOR_PLUGIN:
            # This plugin factory is unknown, or it has relative (to prior plugin factory) weight.
            last_weight += 1
            weights[factory_type] = last_weight
        else:
            # Otherwise, use the wight assigned to this plugin factory
            weights[factory_type] = weight

            # Remember this weight for next plugin factories that might have relative (to this plugin factory) weight.
            last_weight = weight

    return weights
