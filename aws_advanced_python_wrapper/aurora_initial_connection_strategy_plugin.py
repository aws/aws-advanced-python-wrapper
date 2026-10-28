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
from typing import (TYPE_CHECKING, Callable, Dict, FrozenSet, List, Optional,
                    Sequence, Set, Tuple)

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
from aws_advanced_python_wrapper.utils.accessible_regions import \
    AccessibleRegions
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
                WrapperProperties.VERIFY_OPENED_CONNECTION_TYPE.name,
                value,
                ", ".join(item.value for item in cls)))
        return setting


_VERIFICATION_SETTING_BY_KEY: Dict[str, RoleVerificationSetting] = {
    item.value: item for item in RoleVerificationSetting
}

_INACTIVE_SUBSTITUTION_STRATEGY_BY_KEY: Dict[str, InstanceSubstitutionStrategy] = {
    InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER.value: InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER,
    InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE.value: InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE,
}

_INACTIVE_VERIFICATION_ROLE_BY_KEY: Dict[str, Optional[HostRole]] = {
    "writer": HostRole.WRITER,
    "none": None,
}


def _parse_restricted(
        value: str, allowed: Dict, prop) -> object:
    """Look ``value`` up in ``allowed``, raising with the allowed values when absent."""
    key = value.lower()
    if key not in allowed:
        raise AwsWrapperError(Messages.get_formatted(
            "AuroraInitialConnectionStrategyPlugin.InvalidPropertyValue",
            prop.name, value, ", ".join(sorted(allowed))))
    return allowed[key]


def _inactive_cluster_writer_strategy(props: Properties) -> InstanceSubstitutionStrategy:
    """Return whether an inactive cluster writer endpoint should be substituted."""
    # return_default=False because the declared default is 'writer'
    value = WrapperProperties.INACTIVE_CLUSTER_WRITER_SUBSTITUTION_ROLE.get(props, return_default=False)
    if value is None:
        return InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER

    strategy = _parse_restricted(
        value, _INACTIVE_SUBSTITUTION_STRATEGY_BY_KEY,
        WrapperProperties.INACTIVE_CLUSTER_WRITER_SUBSTITUTION_ROLE)
    assert isinstance(strategy, InstanceSubstitutionStrategy)
    return strategy


def _inactive_cluster_writer_role(props: Properties) -> Optional[HostRole]:
    """Return the role a connection through an inactive cluster writer endpoint must have."""
    # return_default=False because the declared default is 'writer'
    value = WrapperProperties.VERIFY_INACTIVE_CLUSTER_WRITER_CONNECTION_ROLE.get(props, return_default=False)
    if value is not None:
        role = _parse_restricted(
            value, _INACTIVE_VERIFICATION_ROLE_BY_KEY,
            WrapperProperties.VERIFY_INACTIVE_CLUSTER_WRITER_CONNECTION_ROLE)
        assert role is None or isinstance(role, HostRole)
        return role

    if _inactive_cluster_writer_strategy(props) is InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE:
        return None
    return HostRole.WRITER


class AuroraInitialConnectionStrategyPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {"init_host_provider", "connect"}

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service: PluginService = plugin_service
        self._rds_utils = RdsUtils()
        self._host_list_provider_service: Optional[HostListProviderService] = None
        self._accessible_regions: Optional[FrozenSet[str]] = AccessibleRegions.parse(props)

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
        end_time_ns = perf_counter_ns() + self._open_connection_retry_timeout_ns
        # Carries the most recent attempt failure into the timeout error, so the caller is
        # told what kept failing rather than only that time ran out.
        last_error: Optional[Exception] = None

        while True:
            # Both are re-derived every attempt because both read the topology, which the
            # previous attempt may have refreshed. Deriving the strategy once pinned a
            # connection whose topology was empty for the whole retry window, so it could
            # never substitute the writer it had just discovered. The role can change across
            # attempts too, and that is deliberate: a cold topology makes a writer cluster
            # endpoint look inactive, and once a refresh names a same-region writer the
            # endpoint is an active cluster writer endpoint whose role must be verified.
            substitution_strategy = self._get_instance_substitution_strategy(
                props, url_type, is_initial_connection, original_host)
            role_to_verify = self._get_role_to_verify(url_type, is_initial_connection, props, original_host)

            if (substitution_strategy is InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE
                    and role_to_verify is None):
                # Nothing to substitute and no role to verify, so retrying cannot change the
                # outcome. Connect once and let the driver's own error surface instead of
                # replacing it with a timeout that names a parameter the caller never set.
                # The host is still recorded: the host list provider derives the initial
                # connection from it, and skipping that moved topology resolution off the
                # endpoint the caller named.
                conn = connect_func()
                self._set_initial_connection_host_info(is_initial_connection, host_info)
                return conn

            candidate_conn: Optional[Connection] = None
            candidate_host: Optional[HostInfo] = None
            attempted: List[HostInfo] = []

            try:
                candidate_host, candidate_conn = self._open_candidate_connection(
                    host_info, url_type, substitution_strategy, props, connect_func, attempted)

                if candidate_conn is not None:
                    if role_to_verify is None:
                        # No verification required.
                        self._set_initial_connection_host_info(is_initial_connection, candidate_host)
                        return candidate_conn

                    conn_role = self._plugin_service.get_host_role(candidate_conn)
                    if conn_role == role_to_verify:
                        # Verification succeeded.
                        self._set_initial_connection_host_info(is_initial_connection, candidate_host)
                        return candidate_conn

                    # Verification failed. Retry, unless a reader was requested but the cluster
                    # has no readers.
                    self._plugin_service.force_refresh_host_list(candidate_conn)
                    if role_to_verify == HostRole.READER and self._has_hosts() and not self._has_readers():
                        # A reader was requested but the cluster has no readers. Simulate the
                        # reader cluster endpoint logic and return the current (writer) connection.
                        if self._verify_role_prop_value == RoleVerificationSetting.READER.value:
                            logger.debug(
                                "AuroraInitialConnectionStrategyPlugin.VerifyReaderConfiguredButNoReadersExist",
                                WrapperProperties.VERIFY_OPENED_CONNECTION_TYPE.name)
                        self._set_initial_connection_host_info(is_initial_connection, candidate_host)
                        return candidate_conn

                    last_error = AwsWrapperError(Messages.get_formatted(
                        "AuroraInitialConnectionStrategyPlugin.IncorrectRole",
                        candidate_host.host, role_to_verify.name.lower(), conn_role.name.lower()))
                    logger.debug(
                        "AuroraInitialConnectionStrategyPlugin.IncorrectRole",
                        candidate_host.host, role_to_verify.name.lower(), conn_role.name.lower())
                    self._close_connection(candidate_conn)
            except Exception as e:
                self._close_connection(candidate_conn)
                if self._plugin_service.is_login_exception(e):
                    raise

                last_error = e
                # Marked only for a substituted candidate, and only once the candidate is known.
                # Reading it back from the unpacked return value never worked, because an
                # exception means there was nothing to unpack -- so a reader that refused a
                # connection was never marked, and the next attempt's selector could keep
                # choosing it for the whole retry window. Marking the host the user named in
                # the connection string is deliberately skipped: that would hand the selectors
                # a verdict about a host they were never asked to choose.
                failed_host = attempted[-1] if attempted else None
                if failed_host is not None:
                    self._plugin_service.set_availability(
                        failed_host.as_aliases(), HostAvailability.UNAVAILABLE)

                retryable = (
                    self._plugin_service.is_network_exception(e)
                    or (self._plugin_service.is_read_only_connection_exception(e)
                        and (role_to_verify == HostRole.WRITER
                             or substitution_strategy is InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER)))
                if not retryable:
                    raise

            # Checked after the attempt rather than before it, so a zero retry budget means
            # "try once" rather than "never connect at all".
            if not self._delay_unless_expired(end_time_ns):
                break

        timeout_ms = self._open_connection_retry_timeout_ns // 1_000_000
        if last_error is None:
            raise AwsWrapperError(Messages.get_formatted(
                "AuroraInitialConnectionStrategyPlugin.Timeout",
                timeout_ms, WrapperProperties.VERIFY_OPENED_CONNECTION_TYPE.name))
        raise AwsWrapperError(Messages.get_formatted(
            "AuroraInitialConnectionStrategyPlugin.TimeoutWithCause",
            timeout_ms, WrapperProperties.VERIFY_OPENED_CONNECTION_TYPE.name, str(last_error)))

    def _delay_unless_expired(self, end_time_ns: int) -> bool:
        """Sleep the retry interval, reporting whether the retry budget allows another attempt.

        Returns ``False`` once the budget is exhausted. The sleep never overshoots the
        deadline, so a budget shorter than the retry interval does not stretch the call.
        """
        remaining_ns = end_time_ns - perf_counter_ns()
        if remaining_ns <= 0:
            return False

        self._delay(min(self._retry_delay_ms, remaining_ns // 1_000_000))
        return perf_counter_ns() < end_time_ns

    def _open_candidate_connection(
            self,
            original_connect_host: HostInfo,
            url_type: RdsUrlType,
            substitution_strategy: InstanceSubstitutionStrategy,
            props: Properties,
            connect_func: Callable,
            attempted: Optional[List[HostInfo]] = None) -> Tuple[HostInfo, Optional[Connection]]:
        """Opens a candidate connection, returning the host that was connected to and the connection.

        If no substitution is needed, the original endpoint is used. Otherwise, an instance host is
        selected from the topology when available; if the topology isn't available yet, a connection is
        opened via the initial endpoint (which also confirms the dialect and acts as a fallback) and,
        when ``wait_for_initial_topology_ms > 0``, the topology fetch is awaited before re-attempting
        instance selection.

        Each substituted host is appended to ``attempted`` before it is connected to. The caller
        cannot otherwise learn which host failed, because an exception here means its return value
        was never unpacked -- which left the caller unable to mark anything unavailable.
        """
        if substitution_strategy is InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE:
            return original_connect_host, connect_func()

        candidate_host = self._get_candidate_host(original_connect_host, url_type, substitution_strategy)
        if candidate_host is not None and self._rds_utils.is_rds_instance(candidate_host.host):
            # Topology is already available; connect to the selected instance.
            if attempted is not None:
                attempted.append(candidate_host)
            return candidate_host, self._plugin_service.connect(candidate_host, props, self)

        # Unable to find an instance URL host. Topology may not exist yet, or may be outdated.
        # Connect via the initial endpoint. This connection also confirms the dialect (done by
        # the default plugin on the initial connection), which is a prerequisite for fetching the
        # topology, and it serves as a fallback connection if instance selection or connection fails.
        candidate_conn = connect_func()

        try:
            if self._wait_for_initial_topology_ms <= 0:
                # Feature disabled. Preserve the previous behavior.
                self._plugin_service.force_refresh_host_list(candidate_conn)
                return original_connect_host, candidate_conn

            return self._wait_for_topology_and_connect_to_instance(
                original_connect_host, url_type, substitution_strategy, props, candidate_conn)
        except Exception:
            self._close_connection(candidate_conn)
            raise

    def _wait_for_topology_and_connect_to_instance(
            self,
            original_connect_host: HostInfo,
            url_type: RdsUrlType,
            substitution_strategy: InstanceSubstitutionStrategy,
            props: Properties,
            fallback_conn: Connection) -> Tuple[HostInfo, Optional[Connection]]:
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

        # force_monitoring_refresh_host_list takes seconds, and host list
        # providers without monitor support raise instead of returning their host list.
        timeout_sec = self._wait_for_initial_topology_ms / 1000
        try:
            # Deliberately does not ask for the writer to be verified. Doing so closes the
            # shared per-cluster monitoring connection and clears its verified-writer state,
            # which is right after a failover but not at connect time, when nothing has failed
            # over: under a pool prefill every arriving connection would tear down what the
            # previous one had just established. The role of the connection returned here is
            # verified directly, on the real connection, by the caller.
            topology_fetched = self._plugin_service.force_monitoring_refresh_host_list(False, timeout_sec)
        except Exception as e:
            # A provider without monitor support raises rather than returning a host list, and
            # so does a genuine refresh failure. Both fall back to the endpoint connection, but
            # a failure is not a timeout and must not be reported as one.
            logger.debug("AuroraInitialConnectionStrategyPlugin.WaitForTopologyError", str(e))
            topology_fetched = False

        if not topology_fetched:
            logger.debug(
                "AuroraInitialConnectionStrategyPlugin.WaitForTopologyTimeout",
                self._wait_for_initial_topology_ms, original_connect_host.host)
            return original_connect_host, fallback_conn

        instance_host = self._get_candidate_host(original_connect_host, url_type, substitution_strategy)
        if instance_host is None or not self._rds_utils.is_rds_instance(instance_host.host):
            return original_connect_host, fallback_conn

        try:
            instance_conn = self._plugin_service.connect(instance_host, props, self)
        except Exception:
            # Failed to connect to the selected instance; keep the initial-endpoint connection.
            logger.debug(
                "AuroraInitialConnectionStrategyPlugin.FailedToConnectToSelectedInstance", instance_host.host)
            return original_connect_host, fallback_conn

        # Close the previous (fallback) connection once the instance connection is held.
        self._close_connection(fallback_conn)
        return instance_host, instance_conn

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
            writer = self._find_writer(self._plugin_service.all_hosts)
            if writer is None or not self._rds_utils.is_rds_instance(writer.host):
                # With no topology there is no way to tell an active cluster writer endpoint
                # from an inactive one, so this defers to the same setting the cross-region
                # case below uses. Without that, a request not to substitute an inactive
                # endpoint would be overridden on the first connection of every process, and
                # then overridden again with a writer in another region once topology arrived.
                return _inactive_cluster_writer_strategy(props)

            if self._rds_utils.is_same_region(writer.host, original_host):
                return InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER

            # The cluster writer endpoint belongs to a different region than the current writer region.
            # This means the cluster is an Aurora Global Database and the cluster writer endpoint is in a
            # secondary region. In this case the cluster writer endpoint is inactive and doesn't represent
            # the current writer. A user setting decides whether to substitute it with a writer instance URL.
            return _inactive_cluster_writer_strategy(props)

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

        if url_type in (RdsUrlType.RDS_PROXY, RdsUrlType.RDS_AURORA_LIMITLESS_DB_SHARD_GROUP):
            # Substituting an instance host would route connections around the endpoint the
            # user asked to connect through, losing its pooling, authentication handling and
            # failover behaviour.
            raise AwsWrapperError(Messages.get_formatted(
                "AuroraInitialConnectionStrategyPlugin.InvalidSettingForManagedEndpoint",
                WrapperProperties.ENDPOINT_SUBSTITUTION_ROLE.name, setting.value))

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
            writer = self._find_writer(self._plugin_service.all_hosts)
            if (writer is not None and self._rds_utils.is_rds_instance(writer.host)
                    and self._rds_utils.is_same_region(writer.host, original_host)):
                # The cluster writer endpoint belongs to the same region as the current writer; it's active.
                return HostRole.WRITER

            # Writer is not found (topology cache may not be available yet) or the cluster writer endpoint
            # belongs to a different region. In either case, assume the cluster writer endpoint may be
            # inactive and use the corresponding setting.
            return _inactive_cluster_writer_role(props)

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
            # Filter by accessible regions BEFORE picking the writer so a writer in
            # an unreachable region is never selected (no-op unless
            # gdb_accessible_regions is set on a Global Aurora dialect); the
            # candidate host is chosen from the filtered host list here.
            available_hosts = self._filter_by_accessible_regions(self._plugin_service.all_hosts)
            return self._find_writer(available_hosts)

        # SUBSTITUTE_WITH_ANY has no specific target role, so to_target_role() returns None
        target_role = substitution_strategy.to_target_role()
        if (target_role is None
                or self._selection_strategy is None
                or not self._plugin_service.accepts_strategy(target_role, self._selection_strategy)):
            raise AwsWrapperError(Messages.get_formatted(
                "AuroraInitialConnectionStrategyPlugin.UnsupportedStrategy", self._selection_strategy))

        try:
            # Filter to accessible regions BEFORE any strategy/region selection so
            # a candidate is never chosen from an unreachable region (no-op unless
            # gdb_accessible_regions is set on a Global Aurora dialect).
            available_hosts = self._filter_by_accessible_regions(self._plugin_service.hosts)

            aws_region = self._rds_utils.get_rds_region(original_connect_host.host) \
                if url_type.has_region else None
            if aws_region:
                hosts_in_region: List[HostInfo] = [
                    host for host in available_hosts
                    if (host_region := self._rds_utils.get_rds_region(host.host)) is not None
                    and aws_region.casefold() == host_region.casefold()]
                return self._plugin_service.get_host_info_by_strategy(
                    target_role, self._selection_strategy, hosts_in_region)

            return self._plugin_service.get_host_info_by_strategy(
                target_role, self._selection_strategy, available_hosts)
        except Exception:
            # Unable to find a candidate host.
            return None

    def _set_initial_connection_host_info(
            self, is_initial_connection: bool, host_info: Optional[HostInfo]):
        if (is_initial_connection
                and self._host_list_provider_service is not None
                and host_info is not None):
            self._host_list_provider_service.initial_connection_host_info = host_info

    @staticmethod
    def _find_writer(hosts: Sequence[HostInfo]) -> Optional[HostInfo]:
        """Return the first WRITER in ``hosts``, or ``None``.

        Does NOT filter by accessible regions — the caller decides whether to
        pass an already-filtered list.
        """
        for host in hosts:
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

    def _filter_by_accessible_regions(self, hosts: Sequence[HostInfo]) -> List[HostInfo]:
        """Filter hosts down to the configured ``gdb_accessible_regions``.

        Returns the list unchanged when no accessible-regions restriction is
        set. Filtering is delegated to the dialect's ``filter_available_hosts``
        (a no-op default; Global Aurora dialects filter by region), so this is a
        pass-through for non-Global clusters.
        """
        if self._accessible_regions is None:
            return list(hosts)
        dialect = self._plugin_service.database_dialect
        if dialect is None:
            return list(hosts)
        return dialect.filter_available_hosts(hosts, self._accessible_regions)


class AuroraInitialConnectionStrategyPluginFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service: PluginService, props: Properties) -> Plugin:
        return AuroraInitialConnectionStrategyPlugin(plugin_service, props)
