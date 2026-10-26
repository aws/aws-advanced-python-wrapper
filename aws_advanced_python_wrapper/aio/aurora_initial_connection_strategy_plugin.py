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

"""Async Aurora Initial Connection Strategy plugin.

Ports sync ``aurora_initial_connection_strategy_plugin.py``. On initial
``connect()`` against an Aurora cluster DNS endpoint, verifies the landed
connection's actual role matches what the URL implies -- and, if not,
retries via the topology's instance endpoints.

Async adaptations:

* Retry loop uses :func:`asyncio.sleep`.
* Instance connection goes through :meth:`AsyncPluginService.connect`,
  so auth plugins (IAM, Secrets, Federated, Okta) and connection
  tracker re-apply on the new connection just like a user-driven
  connect.
* ``initial_connection_host_info`` is set on the plugin service directly:
  ``AsyncPluginServiceImpl`` implements ``AsyncHostListProviderService``
  and ``aio/wrapper.py`` passes it as the host list provider service, so
  the two are the same object.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, List, Optional,
                    Set, Tuple)

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.aurora_initial_connection_strategy_plugin import (
    InstanceSubstitutionStrategy, RoleVerificationSetting)
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService


logger = Logger(__name__)


class AsyncAuroraInitialConnectionStrategyPlugin(AsyncPlugin):
    """Async counterpart of :class:`AuroraInitialConnectionStrategyPlugin`."""

    _SUBSCRIBED: Set[str] = {
        DbApiMethod.INIT_HOST_PROVIDER.method_name,
        DbApiMethod.CONNECT.method_name,
    }

    def __init__(self, plugin_service: AsyncPluginService, props: Properties) -> None:
        self._plugin_service = plugin_service
        self._rds_utils = RdsUtils()

        self._retry_delay_ms: int = WrapperProperties.OPEN_CONNECTION_RETRY_INTERVAL_MS.get_int(props)
        self._open_connection_retry_timeout_ns: int = \
            WrapperProperties.OPEN_CONNECTION_RETRY_TIMEOUT_MS.get_int(props) * 1_000_000
        self._wait_for_initial_topology_ms: int = max(
            0, WrapperProperties.WAIT_FOR_INITIAL_TOPOLOGY_MS.get_int(props))

        verify_role_value = WrapperProperties.VERIFY_OPENED_CONNECTION_TYPE.get(props)
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
        return set(self._SUBSCRIBED)

    def init_host_provider(
            self,
            props: Properties,
            host_list_provider_service: Any,
            init_host_provider_func: Callable) -> None:
        # Sync captures the service here; async sets initial_connection_host_info on the plugin
        # service directly so there is nothing to hold.
        init_host_provider_func()

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        original_host = host_info.host
        url_type: RdsUrlType = self._rds_utils.identify_rds_type(original_host)
        substitution_strategy = self._get_instance_substitution_strategy(
            props, url_type, is_initial_connection, original_host)
        role_to_verify = self._get_role_to_verify(url_type, is_initial_connection, props, original_host)

        loop = asyncio.get_running_loop()
        deadline = loop.time() + (self._open_connection_retry_timeout_ns / 1_000_000_000)

        while loop.time() < deadline:
            candidate_conn: Optional[Any] = None
            candidate_host: Optional[HostInfo] = None

            try:
                candidate_host, candidate_conn = await self._open_candidate_connection(
                    host_info, url_type, substitution_strategy, props, connect_func, driver_dialect)

                if candidate_conn is None:
                    # _open_candidate_connection always returns a connection on success; if none is
                    # present the attempt did not yield a usable connection, so retry until the
                    # timeout is reached.
                    continue

                if role_to_verify is None:
                    # No verification required.
                    self._set_initial_connection_host_info(is_initial_connection, candidate_host)
                    return candidate_conn

                conn_role = await self._plugin_service.get_host_role(candidate_conn)
                if conn_role == role_to_verify:
                    # Verification succeeded.
                    self._set_initial_connection_host_info(is_initial_connection, candidate_host)
                    return candidate_conn

                # Verification failed. Retry, unless a reader was requested but the cluster has no readers.
                await self._plugin_service.force_refresh_host_list(candidate_conn)
                if role_to_verify == HostRole.READER and self._has_hosts() and not self._has_readers():
                    # A reader was requested but the cluster has no readers.
                    # Simulate the reader cluster endpoint logic and return the current (writer) connection.
                    if self._verify_role_prop_value == RoleVerificationSetting.READER.value:
                        logger.debug(
                            "AuroraInitialConnectionStrategyPlugin.VerifyReaderConfiguredButNoReadersExist",
                            WrapperProperties.VERIFY_OPENED_CONNECTION_TYPE.name)
                    self._set_initial_connection_host_info(is_initial_connection, candidate_host)
                    return candidate_conn

                logger.debug(
                    "AuroraInitialConnectionStrategyPlugin.IncorrectRole", candidate_host.host, role_to_verify)
                await self._close_connection(candidate_conn, driver_dialect)
                await self._delay(self._retry_delay_ms)
            except Exception as e:
                await self._close_connection(candidate_conn, driver_dialect)
                if self._plugin_service.is_login_exception(error=e):
                    raise

                if candidate_host is not None:
                    self._plugin_service.set_availability(
                        candidate_host.as_aliases(), HostAvailability.UNAVAILABLE)

                if self._plugin_service.is_network_exception(error=e):
                    # Retry connection.
                    continue

                if (self._plugin_service.is_read_only_connection_exception(error=e)
                        and (role_to_verify == HostRole.WRITER
                             or substitution_strategy is InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER)):
                    # Retry connection.
                    continue

                raise

        raise AwsWrapperError(Messages.get_formatted(
            "AuroraInitialConnectionStrategyPlugin.Timeout",
            self._open_connection_retry_timeout_ns // 1_000_000,
            WrapperProperties.VERIFY_OPENED_CONNECTION_TYPE.name))

    async def _open_candidate_connection(
            self,
            original_connect_host: HostInfo,
            url_type: RdsUrlType,
            substitution_strategy: InstanceSubstitutionStrategy,
            props: Properties,
            connect_func: Callable[..., Awaitable[Any]],
            driver_dialect: AsyncDriverDialect) -> Tuple[HostInfo, Optional[Any]]:
        """Opens a candidate connection, returning the host that was connected to and the connection.

        If no substitution is needed, the original endpoint is used. Otherwise, an instance host is
        selected from the topology when available; if the topology isn't available yet, a connection is
        opened via the initial endpoint (which also confirms the dialect and acts as a fallback) and,
        when ``wait_for_initial_topology_ms > 0``, the topology fetch is awaited before re-attempting
        instance selection.
        """
        if substitution_strategy is InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE:
            return original_connect_host, await connect_func()

        candidate_host = self._get_candidate_host(original_connect_host, url_type, substitution_strategy)
        if candidate_host is not None and self._rds_utils.is_rds_instance(candidate_host.host):
            # Topology is already available; connect to the selected instance. Skipping this plugin
            # avoids re-entering the pipeline here, while still letting the auth plugins (IAM,
            # Secrets, Federated, Okta) re-apply on the new connection.
            return candidate_host, await self._plugin_service.connect(
                candidate_host, props, plugin_to_skip=self)

        # Unable to find an instance URL host. Topology may not exist yet, or may be outdated.
        # Connect via the initial endpoint. This connection also confirms the dialect (done by
        # the default plugin on the initial connection), which is a prerequisite for fetching the
        # topology, and it serves as a fallback connection if instance selection or connection fails.
        candidate_conn = await connect_func()

        if self._wait_for_initial_topology_ms <= 0:
            # Feature disabled. Preserve the previous behavior.
            await self._plugin_service.force_refresh_host_list(candidate_conn)
            return original_connect_host, candidate_conn

        return await self._wait_for_topology_and_connect_to_instance(
            original_connect_host, url_type, substitution_strategy, props, candidate_conn, driver_dialect)

    async def _wait_for_topology_and_connect_to_instance(
            self,
            original_connect_host: HostInfo,
            url_type: RdsUrlType,
            substitution_strategy: InstanceSubstitutionStrategy,
            props: Properties,
            fallback_conn: Any,
            driver_dialect: AsyncDriverDialect) -> Tuple[HostInfo, Optional[Any]]:
        """Blocks until the topology for this cluster has been fetched, then re-attempts instance
        selection and connection. This serializes concurrent/prefill connections (all waiting on the
        same per-cluster topology monitor) so that the configured host selection strategy can
        distribute them across instances instead of all relying on the initial endpoint resolved via
        DNS.

        Returns the selected instance host and its connection if the topology was fetched and the
        instance connection succeeded. Otherwise returns ``original_connect_host`` and the
        already-opened initial-endpoint connection, which is kept as a fallback.
        """
        logger.debug(
            "AuroraInitialConnectionStrategyPlugin.WaitingForTopology",
            self._wait_for_initial_topology_ms, original_connect_host.host)

        # Unlike sync, this needs no guard: async providers without monitor support fall back to a
        # plain refresh and report whether any topology is held, rather than raising.
        timeout_sec = self._wait_for_initial_topology_ms / 1000
        topology_fetched = await self._plugin_service.force_monitoring_refresh_host_list(True, timeout_sec)

        if not topology_fetched:
            logger.debug(
                "AuroraInitialConnectionStrategyPlugin.WaitForTopologyTimeout",
                self._wait_for_initial_topology_ms, original_connect_host.host)
            return original_connect_host, fallback_conn

        instance_host = self._get_candidate_host(original_connect_host, url_type, substitution_strategy)
        if instance_host is None or not self._rds_utils.is_rds_instance(instance_host.host):
            return original_connect_host, fallback_conn

        try:
            instance_conn = await self._plugin_service.connect(
                instance_host, props, plugin_to_skip=self)
        except Exception:
            # Failed to connect to the selected instance; keep the initial-endpoint connection.
            logger.debug(
                "AuroraInitialConnectionStrategyPlugin.FailedToConnectToSelectedInstance", instance_host.host)
            return original_connect_host, fallback_conn

        # Close the previous (fallback) connection once the instance connection is held.
        await self._close_connection(fallback_conn, driver_dialect)
        return instance_host, instance_conn

    # ---- strategy and role resolution (ported from sync) ----

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
                WrapperProperties.VERIFY_OPENED_CONNECTION_TYPE.name, "reader", "writer cluster or global cluster"))

        # A custom cluster can only be of type "reader" or "any".
        if (setting is RoleVerificationSetting.WRITER
                and url_type in (RdsUrlType.RDS_READER_CLUSTER, RdsUrlType.RDS_CUSTOM_CLUSTER)):
            raise AwsWrapperError(Messages.get_formatted(
                "AuroraInitialConnectionStrategyPlugin.InvalidSettingForEndpoint",
                WrapperProperties.VERIFY_OPENED_CONNECTION_TYPE.name, "writer", "reader cluster or custom cluster"))

    def _get_candidate_host(
            self,
            original_connect_host: HostInfo,
            url_type: RdsUrlType,
            substitution_strategy: InstanceSubstitutionStrategy) -> Optional[HostInfo]:
        if substitution_strategy is InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE:
            return original_connect_host

        if substitution_strategy is InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER:
            return self._get_writer()

        # SUBSTITUTE_WITH_ANY has no specific target role, so to_target_role() returns None
        target_role = substitution_strategy.to_target_role()
        if (target_role is None
                or self._selection_strategy is None
                or not self._plugin_service.accepts_strategy(target_role, self._selection_strategy)):
            raise AwsWrapperError(Messages.get_formatted(
                "AuroraInitialConnectionStrategyPlugin.UnsupportedStrategy", self._selection_strategy))

        try:
            aws_region = self._rds_utils.get_rds_region(original_connect_host.host) \
                if url_type.has_region else None
            if aws_region:
                hosts_in_region: List[HostInfo] = [
                    host for host in self._plugin_service.hosts
                    if (host_region := self._rds_utils.get_rds_region(host.host)) is not None
                    and aws_region.casefold() == host_region.casefold()]
                return self._plugin_service.get_host_info_by_strategy(
                    target_role, self._selection_strategy, hosts_in_region)

            return self._plugin_service.get_host_info_by_strategy(target_role, self._selection_strategy)
        except Exception:
            # Unable to find a candidate host.
            return None

    def _get_writer(self) -> Optional[HostInfo]:
        for host in self._plugin_service.all_hosts:
            if host.role == HostRole.WRITER:
                return host
        return None

    def _has_hosts(self) -> bool:
        return len(self._plugin_service.all_hosts) > 0

    def _has_readers(self) -> bool:
        return any(host.role == HostRole.READER for host in self._plugin_service.all_hosts)

    def _set_initial_connection_host_info(
            self, is_initial_connection: bool, host_info: Optional[HostInfo]):
        # AsyncPluginServiceImpl implements AsyncHostListProviderService and is passed as the host
        # list provider service, so this is the same attribute sync writes through that service.
        if is_initial_connection and host_info is not None:
            self._plugin_service.initial_connection_host_info = host_info

    async def _close_connection(
            self, connection: Optional[Any], driver_dialect: AsyncDriverDialect) -> None:
        if connection is None:
            return
        with suppress(Exception):
            await driver_dialect.abort_connection(connection)

    @staticmethod
    async def _delay(delay_ms: int) -> None:
        await asyncio.sleep(delay_ms / 1000)


__all__ = ["AsyncAuroraInitialConnectionStrategyPlugin"]
