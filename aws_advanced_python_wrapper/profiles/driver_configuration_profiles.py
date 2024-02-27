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

from typing import Dict, Optional

from aws_advanced_python_wrapper.aurora_connection_tracker_plugin import \
    AuroraConnectionTrackerPluginFactory
from aws_advanced_python_wrapper.aurora_initial_connection_strategy_plugin import \
    AuroraInitialConnectionStrategyPluginFactory
from aws_advanced_python_wrapper.failover_plugin import FailoverPluginFactory
from aws_advanced_python_wrapper.host_monitoring_plugin import \
    HostMonitoringPluginFactory
from aws_advanced_python_wrapper.profiles.configuration_profile import \
    ConfigurationProfile
from aws_advanced_python_wrapper.profiles.configuration_profile_preset_codes import \
    ConfigurationProfilePresetCodes
from aws_advanced_python_wrapper.read_write_splitting_plugin import \
    ReadWriteSplittingPluginFactory
from aws_advanced_python_wrapper.sql_alchemy_connection_provider import \
    SqlAlchemyPooledConnectionProvider
from aws_advanced_python_wrapper.stale_dns_plugin import StaleDnsPluginFactory
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


class DriverConfigurationProfiles:
    _profiles: Dict[str, Optional[ConfigurationProfile]] = {}
    _presets: Dict[str, ConfigurationProfile] = {
        ConfigurationProfilePresetCodes.A0: ConfigurationProfile(
            name=ConfigurationProfilePresetCodes.A0,
            properties=Properties({WrapperProperties.CONNECT_TIMEOUT_SEC.name: 10,
                                   WrapperProperties.SOCKET_TIMEOUT_SEC.name: 5,
                                   WrapperProperties.TCP_KEEPALIVE.name: False,
                                   "autocommit": True})
        ),
        ConfigurationProfilePresetCodes.A1: ConfigurationProfile(
            name=ConfigurationProfilePresetCodes.A1,
            properties=Properties({WrapperProperties.CONNECT_TIMEOUT_SEC.name: 30,
                                   WrapperProperties.SOCKET_TIMEOUT_SEC.name: 3,
                                   WrapperProperties.TCP_KEEPALIVE.name: False,
                                   "autocommit": True})
        ),
        ConfigurationProfilePresetCodes.A2: ConfigurationProfile(
            name=ConfigurationProfilePresetCodes.A2,
            properties=Properties({WrapperProperties.CONNECT_TIMEOUT_SEC.name: 3,
                                   WrapperProperties.SOCKET_TIMEOUT_SEC.name: 3,
                                   WrapperProperties.TCP_KEEPALIVE.name: False,
                                   "autocommit": True})
        ),
        ConfigurationProfilePresetCodes.B: ConfigurationProfile(
            name=ConfigurationProfilePresetCodes.B,
            properties=Properties({WrapperProperties.CONNECT_TIMEOUT_SEC.name: 10,
                                   WrapperProperties.SOCKET_TIMEOUT_SEC.name: 0,
                                   WrapperProperties.TCP_KEEPALIVE.name: True,
                                   "autocommit": True})
        ),
        ConfigurationProfilePresetCodes.PG_C0: ConfigurationProfile(
            name=ConfigurationProfilePresetCodes.PG_C0,
            plugin_factories=[HostMonitoringPluginFactory()],
            properties=Properties({WrapperProperties.CONNECT_TIMEOUT_SEC.name: 10,
                                   WrapperProperties.SOCKET_TIMEOUT_SEC.name: 0,
                                   WrapperProperties.FAILURE_DETECTION_COUNT.name: 5,
                                   WrapperProperties.FAILURE_DETECTION_TIME_MS.name: 60000,
                                   WrapperProperties.FAILURE_DETECTION_INTERVAL_MS.name: 15000,
                                   WrapperProperties.TCP_KEEPALIVE.name: False,
                                   "autocommit": True})
        ),
        ConfigurationProfilePresetCodes.PG_C1: ConfigurationProfile(
            name=ConfigurationProfilePresetCodes.PG_C1,
            plugin_factories=[HostMonitoringPluginFactory()],
            properties=Properties({WrapperProperties.CONNECT_TIMEOUT_SEC.name: 10,
                                   WrapperProperties.SOCKET_TIMEOUT_SEC.name: 0,
                                   WrapperProperties.FAILURE_DETECTION_COUNT.name: 5,
                                   WrapperProperties.FAILURE_DETECTION_TIME_MS.name: 30000,
                                   WrapperProperties.FAILURE_DETECTION_INTERVAL_MS.name: 5000,
                                   "monitoring-" + WrapperProperties.CONNECT_TIMEOUT_SEC.name: 3,
                                   "monitoring-" + WrapperProperties.SOCKET_TIMEOUT_SEC.name: 3,
                                   WrapperProperties.TCP_KEEPALIVE.name: False,
                                   "autocommit": True})
        ),
        ConfigurationProfilePresetCodes.D0: ConfigurationProfile(
            name=ConfigurationProfilePresetCodes.D0,
            plugin_factories=[AuroraInitialConnectionStrategyPluginFactory(), AuroraConnectionTrackerPluginFactory(),
                              ReadWriteSplittingPluginFactory(), FailoverPluginFactory()],
            properties=Properties({WrapperProperties.CONNECT_TIMEOUT_SEC.name: 10,
                                   WrapperProperties.SOCKET_TIMEOUT_SEC.name: 5,
                                   WrapperProperties.TCP_KEEPALIVE.name: False,
                                   "autocommit": True}),
            connection_provider=SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 30,
                                                                                  "pool_recycle": 86400,
                                                                                  "pool_timeout": 180})
        ),
        ConfigurationProfilePresetCodes.D1: ConfigurationProfile(
            name=ConfigurationProfilePresetCodes.D1,
            plugin_factories=[AuroraInitialConnectionStrategyPluginFactory(), AuroraConnectionTrackerPluginFactory(),
                              ReadWriteSplittingPluginFactory(), FailoverPluginFactory()],
            properties=Properties({WrapperProperties.CONNECT_TIMEOUT_SEC.name: 30,
                                   WrapperProperties.SOCKET_TIMEOUT_SEC.name: 30,
                                   WrapperProperties.TCP_KEEPALIVE.name: False,
                                   "autocommit": True}),
            connection_provider=SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 30,
                                                                                  "pool_recycle": 86400,
                                                                                  "pool_timeout": 10})
        ),
        ConfigurationProfilePresetCodes.E: ConfigurationProfile(
            name=ConfigurationProfilePresetCodes.E,
            plugin_factories=[AuroraInitialConnectionStrategyPluginFactory(), AuroraConnectionTrackerPluginFactory(),
                              ReadWriteSplittingPluginFactory(), FailoverPluginFactory()],
            properties=Properties({WrapperProperties.CONNECT_TIMEOUT_SEC.name: 10,
                                   WrapperProperties.SOCKET_TIMEOUT_SEC.name: 0,
                                   WrapperProperties.TCP_KEEPALIVE.name: False,
                                   "autocommit": True}),
            connection_provider=SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 30,
                                                                                  "pool_recycle": 86400,
                                                                                  "pool_timeout": 10})
        ),
        ConfigurationProfilePresetCodes.PG_F0: ConfigurationProfile(
            name=ConfigurationProfilePresetCodes.PG_F0,
            plugin_factories=[AuroraInitialConnectionStrategyPluginFactory(), AuroraConnectionTrackerPluginFactory(),
                              ReadWriteSplittingPluginFactory(), FailoverPluginFactory(), HostMonitoringPluginFactory()],
            properties=Properties({WrapperProperties.CONNECT_TIMEOUT_SEC.name: 10,
                                   WrapperProperties.SOCKET_TIMEOUT_SEC.name: 0,
                                   WrapperProperties.FAILURE_DETECTION_COUNT.name: 5,
                                   WrapperProperties.FAILURE_DETECTION_TIME_MS.name: 60000,
                                   WrapperProperties.FAILURE_DETECTION_INTERVAL_MS.name: 15000,
                                   "monitoring-" + WrapperProperties.CONNECT_TIMEOUT_SEC.name: 10,
                                   "monitoring-" + WrapperProperties.SOCKET_TIMEOUT_SEC.name: 5,
                                   WrapperProperties.TCP_KEEPALIVE.name: False,
                                   "autocommit": True}),
            connection_provider=SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 30,
                                                                                  "pool_recycle": 86400,
                                                                                  "pool_timeout": 10})
        ),
        ConfigurationProfilePresetCodes.PG_F1: ConfigurationProfile(
            name=ConfigurationProfilePresetCodes.PG_F1,
            plugin_factories=[AuroraInitialConnectionStrategyPluginFactory(), AuroraConnectionTrackerPluginFactory(),
                              ReadWriteSplittingPluginFactory(), FailoverPluginFactory(), HostMonitoringPluginFactory()],
            properties=Properties({WrapperProperties.CONNECT_TIMEOUT_SEC.name: 10,
                                   WrapperProperties.SOCKET_TIMEOUT_SEC.name: 0,
                                   WrapperProperties.FAILURE_DETECTION_COUNT.name: 5,
                                   WrapperProperties.FAILURE_DETECTION_TIME_MS.name: 30000,
                                   WrapperProperties.FAILURE_DETECTION_INTERVAL_MS.name: 5000,
                                   "monitoring-" + WrapperProperties.CONNECT_TIMEOUT_SEC.name: 3,
                                   "monitoring-" + WrapperProperties.SOCKET_TIMEOUT_SEC.name: 3,
                                   WrapperProperties.TCP_KEEPALIVE.name: False,
                                   "autocommit": True}),
            connection_provider=SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 30,
                                                                                  "pool_recycle": 86400,
                                                                                  "pool_timeout": 10})
        ),
        ConfigurationProfilePresetCodes.G0: ConfigurationProfile(
            name=ConfigurationProfilePresetCodes.G0,
            plugin_factories=[AuroraConnectionTrackerPluginFactory(), StaleDnsPluginFactory(), FailoverPluginFactory()],
            properties=Properties({WrapperProperties.CONNECT_TIMEOUT_SEC.name: 10,
                                   WrapperProperties.SOCKET_TIMEOUT_SEC.name: 5,
                                   WrapperProperties.TCP_KEEPALIVE.name: False,
                                   "autocommit": True})
        ),
        ConfigurationProfilePresetCodes.G1: ConfigurationProfile(
            name=ConfigurationProfilePresetCodes.G1,
            plugin_factories=[AuroraConnectionTrackerPluginFactory(), StaleDnsPluginFactory(), FailoverPluginFactory()],
            properties=Properties({WrapperProperties.CONNECT_TIMEOUT_SEC.name: 30,
                                   WrapperProperties.SOCKET_TIMEOUT_SEC.name: 30,
                                   WrapperProperties.TCP_KEEPALIVE.name: False,
                                   "autocommit": True})
        ),
        ConfigurationProfilePresetCodes.H: ConfigurationProfile(
            name=ConfigurationProfilePresetCodes.G1,
            plugin_factories=[AuroraConnectionTrackerPluginFactory(), StaleDnsPluginFactory(), FailoverPluginFactory()],
            properties=Properties({WrapperProperties.CONNECT_TIMEOUT_SEC.name: 10,
                                   WrapperProperties.SOCKET_TIMEOUT_SEC.name: 0,
                                   WrapperProperties.TCP_KEEPALIVE.name: True,
                                   "autocommit": True})
        ),
        ConfigurationProfilePresetCodes.PG_I0: ConfigurationProfile(
            name=ConfigurationProfilePresetCodes.PG_I0,
            plugin_factories=[AuroraConnectionTrackerPluginFactory(),
                              StaleDnsPluginFactory(),
                              FailoverPluginFactory(),
                              HostMonitoringPluginFactory()],
            properties=Properties({WrapperProperties.CONNECT_TIMEOUT_SEC.name: 10,
                                   WrapperProperties.SOCKET_TIMEOUT_SEC.name: 0,
                                   WrapperProperties.FAILURE_DETECTION_COUNT.name: 5,
                                   WrapperProperties.FAILURE_DETECTION_TIME_MS.name: 60000,
                                   WrapperProperties.FAILURE_DETECTION_INTERVAL_MS.name: 15000,
                                   "monitoring-" + WrapperProperties.CONNECT_TIMEOUT_SEC.name: 10,
                                   "monitoring-" + WrapperProperties.SOCKET_TIMEOUT_SEC.name: 3,
                                   WrapperProperties.TCP_KEEPALIVE.name: False,
                                   "autocommit": True})
        ),
        ConfigurationProfilePresetCodes.PG_I1: ConfigurationProfile(
            name=ConfigurationProfilePresetCodes.PG_I1,
            plugin_factories=[AuroraConnectionTrackerPluginFactory(),
                              StaleDnsPluginFactory(),
                              FailoverPluginFactory(),
                              HostMonitoringPluginFactory()],
            properties=Properties({WrapperProperties.CONNECT_TIMEOUT_SEC.name: 10,
                                   WrapperProperties.SOCKET_TIMEOUT_SEC.name: 0,
                                   WrapperProperties.FAILURE_DETECTION_COUNT.name: 3,
                                   WrapperProperties.FAILURE_DETECTION_TIME_MS.name: 30000,
                                   WrapperProperties.FAILURE_DETECTION_INTERVAL_MS.name: 5000,
                                   "monitoring-" + WrapperProperties.CONNECT_TIMEOUT_SEC.name: 3,
                                   "monitoring-" + WrapperProperties.SOCKET_TIMEOUT_SEC.name: 3,
                                   WrapperProperties.TCP_KEEPALIVE.name: False,
                                   "autocommit": True})
        )
    }

    @classmethod
    def clear_profiles(cls):
        cls._profiles.clear()

    @classmethod
    def add_or_replace_profile(cls, profile_name: str, profile: Optional[ConfigurationProfile]):
        cls._profiles[profile_name] = profile

    @classmethod
    def remove_profile(cls, profile_name: str):
        cls._profiles.pop(profile_name)

    @classmethod
    def contains_profile(cls, profile_name: str):
        return profile_name in cls._profiles

    @classmethod
    def get_plugin_factories(cls, profile_name: str):
        return cls._profiles[profile_name]

    @classmethod
    def get_profile_configuration(cls, profile_name: str):
        profile: Optional[ConfigurationProfile] = cls._profiles.get(profile_name)
        if profile is not None:
            return profile
        return cls._presets.get(profile_name)
