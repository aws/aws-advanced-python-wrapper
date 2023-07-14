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

from aws_wrapper.utils.subscribed_method_utils import SubscribedMethodUtils

if TYPE_CHECKING:
    from aws_wrapper.pep249 import Connection
    from aws_wrapper.plugin_service import PluginService

from logging import getLogger
from threading import Lock
from typing import Any, Callable, Dict, FrozenSet, Optional, Set

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.hostinfo import HostAvailability, HostInfo
from aws_wrapper.plugin import CanReleaseResources, Plugin, PluginFactory
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.notifications import HostEvent
from aws_wrapper.utils.properties import Properties, WrapperProperties
from aws_wrapper.utils.rdsutils import RdsUtils

logger = getLogger(__name__)


class HostMonitoringPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return HostMonitoringPlugin(plugin_service, props)


class HostMonitoringPlugin(Plugin, CanReleaseResources):
    _SUBSCRIBED_METHODS: Set[str] = {"*"}

    def __init__(self, plugin_service, props):
        self._props: Properties = props
        self._plugin_service: PluginService = plugin_service
        self._monitoring_host_info: Optional[HostInfo] = None
        self._rds_utils: RdsUtils = RdsUtils()
        self._monitor_service: MonitorService = MonitorService(plugin_service)
        self._lock: Lock = Lock()

    @property
    def subscribed_methods(self) -> Set[str]:
        return HostMonitoringPlugin._SUBSCRIBED_METHODS

    def connect(self, host_info: HostInfo, props: Properties,
                initial: bool, connect_func: Callable) -> Connection:
        return self._connect(host_info, connect_func)

    def force_connect(self, host_info: HostInfo, props: Properties,
                      initial: bool, force_connect_func: Callable) -> Connection:
        return self._connect(host_info, force_connect_func)

    def _connect(self, host_info: HostInfo, connect_func: Callable) -> Connection:
        conn = connect_func()
        if conn:
            rds_type = self._rds_utils.identify_rds_type(host_info.host)
            if rds_type.is_rds_cluster:
                host_info.reset_aliases()
                self._plugin_service.fill_aliases(conn, host_info)
        return conn

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: Any) -> Any:
        connection = self._plugin_service.current_connection
        if connection is None:
            raise AwsWrapperError(Messages.get_formatted("HostMonitoringPlugin.NullConnection", method_name))

        host_info = self._plugin_service.current_host_info
        if host_info is None:
            raise AwsWrapperError(Messages.get_formatted("HostMonitoringPlugin.NullHostInfo", method_name))

        is_enabled = WrapperProperties.FAILURE_DETECTION_ENABLED.get_bool(self._props)
        if not is_enabled or method_name not in SubscribedMethodUtils.NETWORK_BOUND_METHODS:
            return execute_func()

        failure_detection_time_ms = WrapperProperties.FAILURE_DETECTION_TIME_MS.get_int(self._props)
        failure_detection_interval = WrapperProperties.FAILURE_DETECTION_INTERVAL_MS.get_int(self._props)
        failure_detection_count = WrapperProperties.FAILURE_DETECTION_COUNT.get_int(self._props)

        monitor_context = None
        result = None

        try:
            logger.debug(Messages.get_formatted("HostMonitoringPlugin.ActivatedMonitoring", method_name))
            monitor_context = self._monitor_service.start_monitoring(
                connection,
                self._get_monitoring_host_info().all_aliases,
                self._get_monitoring_host_info(),
                self._props,
                failure_detection_time_ms,
                failure_detection_interval,
                failure_detection_count
            )
            result = execute_func()
        finally:
            if monitor_context:
                with self._lock:
                    self._monitor_service.stop_monitoring(monitor_context)
                    if monitor_context.is_host_unavailable:
                        self._plugin_service.set_availability(
                            self._get_monitoring_host_info().all_aliases, HostAvailability.NOT_AVAILABLE)
                        dialect = self._plugin_service.dialect
                        if dialect is not None and not dialect.is_closed(connection):
                            try:
                                connection.close()
                            except Exception:
                                pass
                            raise AwsWrapperError(
                                Messages.get_formatted("HostMonitoringPlugin.UnavailableHost", host_info.as_alias()))
                logger.debug(Messages.get_formatted("HostMonitoringPlugin.MonitoringDeactivated", method_name))

        return result

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]):
        if HostEvent.WENT_DOWN in changes or HostEvent.HOST_DELETED in changes:
            monitoring_aliases = self._get_monitoring_host_info().all_aliases
            if monitoring_aliases:
                self._monitor_service.stop_monitoring_host_connections(monitoring_aliases)

        self._monitoring_host_info = None

    def _get_monitoring_host_info(self):
        if self._monitoring_host_info is None:
            self._monitoring_host_info = self._plugin_service.current_host_info
            rds_type = self._rds_utils.identify_rds_type(self._monitoring_host_info.url)

            try:
                if rds_type.is_rds_cluster:
                    logger.debug(Messages.get("HostMonitoringPlugin.ClusterEndpointHostInfo"))
                    self._monitoring_host_info = self._plugin_service.identify_connection()
                    if self._monitoring_host_info is None:
                        raise AwsWrapperError(
                            Messages.get_formatted(
                                "HostMonitoringPlugin.UnableToIdentifyConnection",
                                self._plugin_service.current_host_info.host,
                                self._plugin_service.host_list_provider))
                    self._plugin_service.fill_aliases(host_info=self._monitoring_host_info)
            except Exception as e:
                message = Messages.get_formatted("HostMonitoringPlugin.ErrorIdentifyingConnection", e)
                logger.debug(message)
                raise AwsWrapperError(message) from e
        return self._monitoring_host_info

    def release_resources(self):
        if self._monitor_service is not None:
            self._monitor_service.release_resources()

        self._monitor_service = None


class MonitoringConnectionContext:
    @property
    def is_host_unavailable(self) -> bool:
        return False


class Monitor:
    ...


class MonitorService:
    def __init__(self, plugin_service: PluginService):
        self._plugin_service = plugin_service

    def start_monitoring(self,
                         conn: Connection,
                         host_aliases: FrozenSet,
                         host_info: HostInfo,
                         props: Properties,
                         failure_detection_time_ms: int,
                         failure_detection_interval_ms: int,
                         failure_detection_count: int) -> MonitoringConnectionContext:
        # TODO: Finish implementing
        return MonitoringConnectionContext()

    def stop_monitoring(self, context: MonitoringConnectionContext):
        ...

    def stop_monitoring_host_connections(self, host_aliases: FrozenSet):
        ...

    def release_resources(self):
        ...

    def notify_unused(self, monitor: Monitor):
        ...
