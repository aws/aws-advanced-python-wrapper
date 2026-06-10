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

from enum import Enum
from time import perf_counter_ns, sleep
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Set

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.host_list_provider import \
        HostListProviderService
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

logger = Logger(__name__)


class InstanceSubstitutionStrategy(Enum):
    """Determines which host the plugin should connect to when opening a new connection."""
    SUBSTITUTE_WITH_WRITER = "writer"
    SUBSTITUTE_WITH_READER = "reader"
    SUBSTITUTE_WITH_ANY = "any"
    DO_NOT_SUBSTITUTE = "none"

    @classmethod
    def from_property_value(cls, value: Optional[str]) -> Optional[InstanceSubstitutionStrategy]:
        if value is None:
            return None

        strategy = _SUBSTITUTION_STRATEGY_BY_KEY.get(value.lower())
        if strategy is None:
            raise AwsWrapperError(Messages.get_formatted(
                "AuroraInitialConnectionStrategyPlugin.InvalidPropertyValue",
                WrapperProperties.ENDPOINT_SUBSTITUTION_ROLE.name,
                value,
                ", ".join(item.value for item in cls)))
        return strategy

    def to_target_role(self) -> Optional[HostRole]:
        if self is InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER:
            return HostRole.WRITER
        if self is InstanceSubstitutionStrategy.SUBSTITUTE_WITH_READER:
            return HostRole.READER
        return None


_SUBSTITUTION_STRATEGY_BY_KEY: Dict[str, InstanceSubstitutionStrategy] = {
    item.value: item for item in InstanceSubstitutionStrategy
}


class RoleVerificationSetting(Enum):
    """Determines what role, if any, an opened connection should be verified against."""
    WRITER = "writer"
    READER = "reader"
    NO_VERIFICATION = "none"

    @classmethod
    def from_property_value(cls, value: Optional[str]) -> Optional[RoleVerificationSetting]:
        if value is None:
            return None

        setting = _VERIFICATION_SETTING_BY_KEY.get(value.lower())
        if setting is None:
            raise AwsWrapperError(Messages.get_formatted(
                "AuroraInitialConnectionStrategyPlugin.InvalidPropertyValue",
                WrapperProperties.VERIFY_OPENED_CONNECTION_ROLE.name,
                value,
                ", ".join(item.value for item in cls)))
        return setting


_VERIFICATION_SETTING_BY_KEY: Dict[str, RoleVerificationSetting] = {
    item.value: item for item in RoleVerificationSetting
}


class AuroraInitialConnectionStrategyPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {"init_host_provider", "connect"}

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service: PluginService = plugin_service
        self._rds_utils = RdsUtils()
        self._host_list_provider_service: Optional[HostListProviderService] = None

        self._retry_delay_ms: int = WrapperProperties.OPEN_CONNECTION_RETRY_INTERVAL_MS.get_int(props)
        self._open_connection_retry_timeout_ns: int = \
            WrapperProperties.OPEN_CONNECTION_RETRY_TIMEOUT_MS.get_int(props) * 1_000_000

        verify_role_value = WrapperProperties.VERIFY_OPENED_CONNECTION_ROLE.get(props)
        self._verify_role_prop_value: Optional[str] = \
            verify_role_value.lower() if verify_role_value is not None else None

        # INITIAL_CONNECTION_HOST_SELECTOR_STRATEGY overrides the deprecated
        # READER_INITIAL_HOST_SELECTOR_STRATEGY when it is explicitly set.
        if WrapperProperties.INITIAL_CONNECTION_HOST_SELECTOR_STRATEGY.name in props:
            self._selection_strategy: Optional[str] = \
                WrapperProperties.INITIAL_CONNECTION_HOST_SELECTOR_STRATEGY.get(props)
        else:
            self._selection_strategy = WrapperProperties.READER_INITIAL_HOST_SELECTOR_STRATEGY.get(props)

    @property
    def subscribed_methods(self) -> Set[str]:
        return AuroraInitialConnectionStrategyPlugin._SUBSCRIBED_METHODS

    def init_host_provider(
            self,
            props: Properties,
            host_list_provider_service: HostListProviderService,
            init_host_provider_func: Callable):
        self._host_list_provider_service = host_list_provider_service
        init_host_provider_func()

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        original_host = host_info.host
        url_type: RdsUrlType = self._rds_utils.identify_rds_type(original_host)
        substitution_strategy = self._get_instance_substitution_strategy(
            props, url_type, is_initial_connection, original_host)
        role_to_verify = self._get_role_to_verify(url_type, is_initial_connection, props, original_host)
        end_time_ns = perf_counter_ns() + self._open_connection_retry_timeout_ns

        while perf_counter_ns() < end_time_ns:
            candidate_conn: Optional[Connection] = None
            candidate_host: Optional[HostInfo] = None

            try:
                if substitution_strategy is InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE:
                    candidate_host = host_info
                    candidate_conn = connect_func()
                else:
                    candidate_host = self._get_candidate_host(host_info, url_type, substitution_strategy)
                    if candidate_host is None or not self._rds_utils.is_rds_instance(candidate_host.host):
                        # Unable to find an instance URL host. Topology may not exist yet, or may be outdated.
                        candidate_conn = connect_func()
                        candidate_host = host_info
                        self._plugin_service.force_refresh_host_list(candidate_conn)
                    else:
                        candidate_conn = self._plugin_service.connect(candidate_host, props)

                if role_to_verify is None:
                    # No verification required.
                    self._set_initial_connection_host_info(is_initial_connection, candidate_host)
                    return candidate_conn

                conn_role = self._plugin_service.get_host_role(candidate_conn)
                if conn_role == role_to_verify:
                    # Verification succeeded.
                    self._set_initial_connection_host_info(is_initial_connection, candidate_host)
                    return candidate_conn

                # Verification failed. Retry, unless a reader was requested but the cluster has no readers.
                self._plugin_service.force_refresh_host_list(candidate_conn)
                if role_to_verify == HostRole.READER and self._has_hosts() and not self._has_readers():
                    # A reader was requested but the cluster has no readers.
                    # Simulate the reader cluster endpoint logic and return the current (writer) connection.
                    if self._verify_role_prop_value == RoleVerificationSetting.READER.value:
                        logger.debug(
                            "AuroraInitialConnectionStrategyPlugin.VerifyReaderConfiguredButNoReadersExist",
                            WrapperProperties.VERIFY_OPENED_CONNECTION_ROLE.name)
                    self._set_initial_connection_host_info(is_initial_connection, candidate_host)
                    return candidate_conn

                logger.debug(
                    "AuroraInitialConnectionStrategyPlugin.IncorrectRole", candidate_host.host, role_to_verify)
                self._close_connection(candidate_conn)
                self._delay(self._retry_delay_ms)
            except Exception as e:
                self._close_connection(candidate_conn)
                if self._plugin_service.is_login_exception(e):
                    raise

                if candidate_host is not None:
                    self._plugin_service.set_availability(
                        candidate_host.as_aliases(), HostAvailability.UNAVAILABLE)

                if self._plugin_service.is_network_exception(e):
                    # Retry connection.
                    continue

                if (self._plugin_service.is_read_only_connection_exception(e)
                        and (role_to_verify == HostRole.WRITER
                             or substitution_strategy is InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER)):
                    # Retry connection.
                    continue

                raise

        raise AwsWrapperError(Messages.get_formatted(
            "AuroraInitialConnectionStrategyPlugin.Timeout",
            self._open_connection_retry_timeout_ns // 1_000_000,
            WrapperProperties.VERIFY_OPENED_CONNECTION_ROLE.name))

    def _get_instance_substitution_strategy(
            self,
            props: Properties,
            url_type: RdsUrlType,
            is_initial_connection: bool,
            original_host: str) -> InstanceSubstitutionStrategy:
        if is_initial_connection:
            strategy = InstanceSubstitutionStrategy.from_property_value(
                WrapperProperties.ENDPOINT_SUBSTITUTION_ROLE.get(props))
            if strategy is not None:
                self._validate_substitution_strategy(strategy, url_type)
                return strategy

        # This is not an initial connection, or ENDPOINT_SUBSTITUTION_ROLE was not set.
        # Pick a strategy according to the default behavior.
        if url_type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER:
            return InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER

        if url_type == RdsUrlType.RDS_WRITER_CLUSTER:
            writer = self._get_writer()
            if writer is None or not self._rds_utils.is_rds_instance(writer.host):
                return InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE

            if self._rds_utils.is_same_region(writer.host, original_host):
                return InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER

            # The cluster writer endpoint belongs to a different region than the current writer region.
            # This means the cluster is an Aurora Global Database and the cluster writer endpoint is in a
            # secondary region. In this case the cluster writer endpoint is inactive and doesn't represent
            # the current writer. A user setting decides whether to substitute it with a writer instance URL.
            inactive_strategy = InstanceSubstitutionStrategy.from_property_value(
                WrapperProperties.INACTIVE_CLUSTER_WRITER_SUBSTITUTION_ROLE.get(props))
            return inactive_strategy if inactive_strategy is not None \
                else InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER

        if url_type == RdsUrlType.RDS_READER_CLUSTER:
            return InstanceSubstitutionStrategy.SUBSTITUTE_WITH_READER

        return InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE

    def _validate_substitution_strategy(
            self, setting: InstanceSubstitutionStrategy, url_type: RdsUrlType):
        if setting is InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE:
            return

        if url_type == RdsUrlType.RDS_INSTANCE:
            raise AwsWrapperError(Messages.get_formatted(
                "AuroraInitialConnectionStrategyPlugin.InvalidSettingForInstanceEndpoint",
                WrapperProperties.ENDPOINT_SUBSTITUTION_ROLE.name))

        if not url_type.is_rds_cluster:
            return

        # A custom cluster can only be of type "reader" or "any", so SUBSTITUTE_WITH_WRITER is not allowed.
        if (setting is InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER
                and url_type in (RdsUrlType.RDS_READER_CLUSTER, RdsUrlType.RDS_CUSTOM_CLUSTER)):
            raise AwsWrapperError(Messages.get_formatted(
                "AuroraInitialConnectionStrategyPlugin.InvalidSettingForEndpoint",
                WrapperProperties.ENDPOINT_SUBSTITUTION_ROLE.name, "writer", "reader cluster or custom cluster"))

        if (setting is InstanceSubstitutionStrategy.SUBSTITUTE_WITH_READER
                and url_type in (RdsUrlType.RDS_WRITER_CLUSTER, RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER)):
            raise AwsWrapperError(Messages.get_formatted(
                "AuroraInitialConnectionStrategyPlugin.InvalidSettingForEndpoint",
                WrapperProperties.ENDPOINT_SUBSTITUTION_ROLE.name, "reader", "writer cluster or global cluster"))

        if (setting is InstanceSubstitutionStrategy.SUBSTITUTE_WITH_ANY
                and url_type != RdsUrlType.RDS_CUSTOM_CLUSTER):
            raise AwsWrapperError(Messages.get_formatted(
                "AuroraInitialConnectionStrategyPlugin.InvalidSettingForEndpoint",
                WrapperProperties.ENDPOINT_SUBSTITUTION_ROLE.name, "any",
                "writer cluster, reader cluster, or global cluster"))

    def _get_role_to_verify(
            self,
            url_type: RdsUrlType,
            is_initial_connection: bool,
            props: Properties,
            original_host: str) -> Optional[HostRole]:
        if not is_initial_connection:
            return None

        setting = RoleVerificationSetting.from_property_value(self._verify_role_prop_value)
        if setting is not None:
            self._validate_verification_setting(setting, url_type)

        if setting is RoleVerificationSetting.NO_VERIFICATION:
            return None
        if setting is RoleVerificationSetting.WRITER:
            return HostRole.WRITER
        if setting is RoleVerificationSetting.READER:
            return HostRole.READER

        # Role verification setting is not set. We still verify the correct role for a writer/reader cluster.
        if url_type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER:
            return HostRole.WRITER

        if url_type == RdsUrlType.RDS_WRITER_CLUSTER:
            writer = self._get_writer()
            if (writer is not None and self._rds_utils.is_rds_instance(writer.host)
                    and self._rds_utils.is_same_region(writer.host, original_host)):
                # The cluster writer endpoint belongs to the same region as the current writer; it's active.
                return HostRole.WRITER

            # Writer is not found (topology cache may not be available yet) or the cluster writer endpoint
            # belongs to a different region. In either case, assume the cluster writer endpoint may be
            # inactive and use the corresponding setting.
            inactive_strategy = InstanceSubstitutionStrategy.from_property_value(
                WrapperProperties.VERIFY_INACTIVE_CLUSTER_WRITER_CONNECTION_ROLE.get(props))
            return inactive_strategy.to_target_role() if inactive_strategy is not None else HostRole.WRITER

        if url_type == RdsUrlType.RDS_READER_CLUSTER:
            return HostRole.READER

        return None

    def _validate_verification_setting(self, setting: RoleVerificationSetting, url_type: RdsUrlType):
        if (setting is RoleVerificationSetting.READER
                and url_type in (RdsUrlType.RDS_WRITER_CLUSTER, RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER)):
            raise AwsWrapperError(Messages.get_formatted(
                "AuroraInitialConnectionStrategyPlugin.InvalidSettingForEndpoint",
                WrapperProperties.VERIFY_OPENED_CONNECTION_ROLE.name, "reader", "writer cluster or global cluster"))

        # A custom cluster can only be of type "reader" or "any".
        if (setting is RoleVerificationSetting.WRITER
                and url_type in (RdsUrlType.RDS_READER_CLUSTER, RdsUrlType.RDS_CUSTOM_CLUSTER)):
            raise AwsWrapperError(Messages.get_formatted(
                "AuroraInitialConnectionStrategyPlugin.InvalidSettingForEndpoint",
                WrapperProperties.VERIFY_OPENED_CONNECTION_ROLE.name, "writer", "reader cluster or custom cluster"))

    def _get_candidate_host(
            self,
            original_connect_host: HostInfo,
            url_type: RdsUrlType,
            substitution_strategy: InstanceSubstitutionStrategy) -> Optional[HostInfo]:
        if substitution_strategy is InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE:
            return original_connect_host

        if substitution_strategy is InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER:
            return self._get_writer()

        if self._selection_strategy is None:
            raise AwsWrapperError(Messages.get_formatted(
                "AuroraInitialConnectionStrategyPlugin.UnsupportedStrategy", self._selection_strategy))

        target_role = substitution_strategy.to_target_role()
        # SUBSTITUTE_WITH_ANY has no specific target role; default to READER for host selection,
        # mirroring the JDBC behavior where the selector treats a null role as "any".
        selection_role = target_role if target_role is not None else HostRole.READER

        if not self._plugin_service.accepts_strategy(selection_role, self._selection_strategy):
            raise AwsWrapperError(Messages.get_formatted(
                "AuroraInitialConnectionStrategyPlugin.UnsupportedStrategy", self._selection_strategy))

        try:
            aws_region = self._rds_utils.get_rds_region(original_connect_host.host) \
                if url_type.has_region else None
            if aws_region:
                hosts_in_region: List[HostInfo] = [
                    host for host in self._plugin_service.all_hosts
                    if (host_region := self._rds_utils.get_rds_region(host.host)) is not None
                    and aws_region.casefold() == host_region.casefold()]
                return self._plugin_service.get_host_info_by_strategy(
                    selection_role, self._selection_strategy, hosts_in_region)

            return self._plugin_service.get_host_info_by_strategy(selection_role, self._selection_strategy)
        except Exception:
            # Unable to find a candidate host.
            return None

    def _set_initial_connection_host_info(
            self, is_initial_connection: bool, host_info: Optional[HostInfo]):
        if (is_initial_connection
                and self._host_list_provider_service is not None
                and host_info is not None):
            self._host_list_provider_service.initial_connection_host_info = host_info

    def _get_writer(self) -> Optional[HostInfo]:
        for host in self._plugin_service.all_hosts:
            if host.role == HostRole.WRITER:
                return host
        return None

    def _has_hosts(self) -> bool:
        return len(self._plugin_service.all_hosts) > 0

    def _has_readers(self) -> bool:
        return any(host.role == HostRole.READER for host in self._plugin_service.all_hosts)

    def _close_connection(self, connection: Optional[Connection]):
        if connection is not None:
            try:
                connection.close()
            except Exception:
                # ignore
                pass

    def _delay(self, delay_ms: int):
        sleep(delay_ms / 1000)


class AuroraInitialConnectionStrategyPluginFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service: PluginService, props: Properties) -> Plugin:
        return AuroraInitialConnectionStrategyPlugin(plugin_service, props)
