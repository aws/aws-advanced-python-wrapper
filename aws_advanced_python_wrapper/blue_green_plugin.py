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

import socket
from datetime import datetime
from time import perf_counter_ns
from typing import TYPE_CHECKING, FrozenSet, List, cast

from aws_advanced_python_wrapper.database_dialect import BlueGreenDialect
from aws_advanced_python_wrapper.host_list_provider import HostListProvider
from aws_advanced_python_wrapper.utils.value_container import ValueContainer

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.host_list_provider import HostListProviderService
    from aws_advanced_python_wrapper.plugin_service import PluginService

import time
from abc import ABC, abstractmethod
from copy import copy
from dataclasses import dataclass
from enum import Enum, auto
from threading import Condition, Event, RLock, Thread
from typing import Any, Callable, ClassVar, Dict, Optional, Set, Tuple

from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                UnsupportedOperationError)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.iam_plugin import IamAuthPlugin
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.atomic import AtomicInt
from aws_advanced_python_wrapper.utils.concurrent import (ConcurrentDict,
                                                          ConcurrentSet)
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils
from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
    TelemetryTraceLevel

logger = Logger(__name__)


class BlueGreenIntervalRate(Enum):
    BASELINE = auto()
    INCREASED = auto()
    HIGH = auto()


class BlueGreenPhase(Enum):
    NOT_CREATED = (0, False)
    CREATED = (1, False)
    PREPARATION = (2, True)  # hosts are accessible
    IN_PROGRESS = (3, True)  # active phase; hosts are not accessible
    POST = (4, True)  # hosts are accessible; some changes are still in progress
    COMPLETED = (5, True)  # all changes are completed

    def __new__(cls, value: int, is_switchover_active_or_completed: bool):
        obj = object.__new__(cls)
        obj._value_ = (value, is_switchover_active_or_completed)
        return obj

    @property
    def phase_value(self) -> int:
        return self.value[0]

    @property
    def is_switchover_active_or_completed(self) -> bool:
        return self.value[1]

    @staticmethod
    def parse_phase(phase_str: Optional[str]) -> BlueGreenPhase:
        if not phase_str:
            return BlueGreenPhase.NOT_CREATED

        phase_upper = phase_str.upper()
        if phase_upper == "AVAILABLE":
            return BlueGreenPhase.CREATED
        elif phase_upper == "SWITCHOVER_INITIATED":
            return BlueGreenPhase.PREPARATION
        elif phase_upper == "SWITCHOVER_IN_PROGRESS":
            return BlueGreenPhase.IN_PROGRESS
        elif phase_upper == "SWITCHOVER_IN_POST_PROCESSING":
            return BlueGreenPhase.POST
        elif phase_upper == "SWITCHOVER_COMPLETED":
            return BlueGreenPhase.COMPLETED
        else:
            raise ValueError(Messages.get_formatted("BlueGreenPhase.UnknownStatus", phase_str))


class BlueGreenRole(Enum):
    SOURCE = 0
    TARGET = 1

    @staticmethod
    def parse_role(role_str: str, version: str) -> BlueGreenRole:
        if "1.0" != version:
            raise ValueError(Messages.get_formatted("BlueGreenRole.UnknownVersion", version))

        if role_str == "BLUE_GREEN_DEPLOYMENT_SOURCE":
            return BlueGreenRole.SOURCE
        elif role_str == "BLUE_GREEN_DEPLOYMENT_TARGET":
            return BlueGreenRole.TARGET
        else:
            raise ValueError(Messages.get_formatted("BlueGreenRole.UnknownRole", role_str))


class BlueGreenStatus:
    def __init__(
            self,
            bg_id: str,
            phase: BlueGreenPhase,
            connect_routings: Optional[List[ConnectRouting]] = None,
            execute_routings: Optional[List[ExecuteRouting]] = None,
            role_by_host: Optional[ConcurrentDict[str, BlueGreenRole]] = None,
            corresponding_hosts: Optional[ConcurrentDict[str, Tuple[HostInfo, Optional[HostInfo]]]] = None):
        self.bg_id = bg_id
        self.phase = phase
        self.connect_routings = [] if connect_routings is None else list(connect_routings)
        self.execute_routings = [] if execute_routings is None else list(execute_routings)
        self.roles_by_endpoint: ConcurrentDict[str, BlueGreenRole] = ConcurrentDict()
        if role_by_host is not None:
            self.roles_by_endpoint.put_all(role_by_host)

        self.corresponding_hosts: ConcurrentDict[str, Tuple[HostInfo, Optional[HostInfo]]] = ConcurrentDict()
        if corresponding_hosts is not None:
            self.corresponding_hosts.put_all(corresponding_hosts)

        self.cv = Condition()

    def get_role(self, host_info: HostInfo) -> Optional[BlueGreenRole]:
        return self.roles_by_endpoint.get(host_info.host.lower())

    def __str__(self) -> str:
        connect_routings_str = ',\n        '.join(str(cr) for cr in self.connect_routings)
        execute_routings_str = ',\n        '.join(str(er) for er in self.execute_routings)
        role_mappings = ',\n        '.join(f"{endpoint}: {role}" for endpoint, role in self.roles_by_endpoint.items())

        return (f"{self.__class__.__name__}(\n"
                f"    id='{self.bg_id}',\n"
                f"    phase={self.phase},\n"
                f"    connect_routings=[\n"
                f"        {connect_routings_str}\n"
                f"    ],\n"
                f"    execute_routings=[\n"
                f"        {execute_routings_str}\n"
                f"    ],\n"
                f"    role_by_endpoint={{\n"
                f"        {role_mappings}\n"
                f"    }}\n"
                f")")


@dataclass
class BlueGreenInterimStatus:
    phase: BlueGreenPhase
    version: str
    port: int
    start_topology: Tuple[HostInfo, ...]
    start_ip_addresses_by_host_map: ConcurrentDict[str, ValueContainer[str]]
    current_topology: Tuple[HostInfo, ...]
    current_ip_addresses_by_host_map: ConcurrentDict[str, ValueContainer[str]]
    host_names: Set[str]
    all_start_topology_ip_changed: bool
    all_start_topology_endpoints_removed: bool
    all_topology_changed: bool

    def get_custom_hashcode(self):
        result: int = self.get_value_hash(1, "" if self.phase is None else str(self.phase))
        result = self.get_value_hash(result, str(self.version))
        result = self.get_value_hash(result, str(self.port))
        result = self.get_value_hash(result, str(self.all_start_topology_ip_changed))
        result = self.get_value_hash(result, str(self.all_start_topology_endpoints_removed))
        result = self.get_value_hash(result, str(self.all_topology_changed))
        result = self.get_value_hash(result, "" if self.host_names is None else ",".join(sorted(self.host_names)))
        result = self.get_host_tuple_hash(result, self.start_topology)
        result = self.get_host_tuple_hash(result, self.current_topology)
        result = self.get_ip_dict_hash(result, self.start_ip_addresses_by_host_map)
        result = self.get_ip_dict_hash(result, self.current_ip_addresses_by_host_map)
        return result

    def get_host_tuple_hash(self, current_hash: int, host_tuple: Optional[Tuple[HostInfo, ...]]) -> int:
        if host_tuple is None or len(host_tuple) == 0:
            tuple_str = ""
        else:
            tuple_str = ",".join(sorted(f"{x.url}{x.role}" for x in host_tuple))

        return self.get_value_hash(current_hash, tuple_str)

    def get_ip_dict_hash(self, current_hash: int, ip_dict: Optional[ConcurrentDict[str, ValueContainer[str]]]) -> int:
        if ip_dict is None or len(ip_dict) == 0:
            dict_str = ""
        else:
            dict_str = ",".join(sorted(f"{key}{str(value)}" for key, value in ip_dict.items()))

        return self.get_value_hash(current_hash, dict_str)

    def get_value_hash(self, current_hash: int, val: Optional[str]) -> int:
        return current_hash * 31 + hash("" if val is None else val)

    def __str__(self):
        host_names_str = ',\n        '.join(self.host_names)
        start_topology_str = ',\n        '.join(str(h) for h in self.start_topology)
        start_addresses_by_host_str = ',\n        '.join(
            f"{k}: {v}" for k, v in self.start_ip_addresses_by_host_map.items()
        )
        current_topology_str = ',\n        '.join(str(h) for h in self.current_topology)
        current_addresses_by_host_str = ',\n        '.join(
            f"{k}: {v}" for k, v in self.current_ip_addresses_by_host_map.items()
        )

        return (f"{self.__class__.__name__}(\n"
                f"    phase={self.phase},\n"
                f"    version={self.version},\n"
                f"    port={self.port},\n"
                f"    host_names=[\n"
                f"        {host_names_str}\n"
                f"    ],\n"
                f"    start_topology=[\n"
                f"        {start_topology_str}\n"
                f"    ],\n"
                f"    start_ip_addresses_by_host_map={{\n"
                f"        {start_addresses_by_host_str}\n"
                f"    }}\n"
                f"    current_topology=[\n"
                f"        {current_topology_str}\n"
                f"    ],\n"
                f"    current_ip_addresses_by_host_map={{\n"
                f"        {current_addresses_by_host_str}\n"
                f"    }}\n"
                f"    all_start_topology_ip_changed={self.all_start_topology_ip_changed}\n"
                f"    all_start_topology_endpoints_removed={self.all_start_topology_endpoints_removed}\n"
                f"    all_topology_changed={self.all_topology_changed}\n"
                f")")


class ConnectRouting(ABC):
    @abstractmethod
    def is_match(self, host_info: Optional[HostInfo], role: BlueGreenRole) -> bool:
        ...

    @abstractmethod
    def apply(
            self,
            plugin: Plugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable,
            plugin_service: PluginService) -> Optional[Connection]:
        ...


class ExecuteRouting(ABC):
    @abstractmethod
    def is_match(self, host_info: Optional[HostInfo], role: BlueGreenRole) -> bool:
        ...

    @abstractmethod
    def apply(
            self,
            plugin: Plugin,
            plugin_service: PluginService,
            props: Properties,
            target: type,
            method_name: str,
            execute_func: Callable,
            *args: Any,
            **kwargs: Any) -> ValueContainer[Any]:
        ...


class BaseRouting:
    _MIN_SLEEP_MS = 50

    def __init__(self, endpoint: Optional[str], bg_role: Optional[BlueGreenRole]):
        self._endpoint = endpoint  # host and optionally port as well
        self._bg_role = bg_role

    def delay(self, delay_ms: int, bg_status: Optional[BlueGreenStatus], plugin_service: PluginService, bg_id: str):
        end_time_sec = time.time() + (delay_ms / 1_000)
        min_delay_ms = min(delay_ms, BaseRouting._MIN_SLEEP_MS)

        if bg_status is None:
            time.sleep(delay_ms / 1_000)
            return

        while bg_status is plugin_service.get_status(BlueGreenStatus, bg_id) and time.time() <= end_time_sec:
            with bg_status.cv:
                bg_status.cv.wait(min_delay_ms / 1_000)

    def is_match(self, host_info: Optional[HostInfo], bg_role: BlueGreenRole) -> bool:
        if self._endpoint is None:
            return self._bg_role is None or self._bg_role == bg_role

        if host_info is None:
            return False

        return self._endpoint == host_info.url.lower() and (self._bg_role is None or self._bg_role == bg_role)

    def __str__(self):
        endpoint_str = "None" if self._endpoint is None else f"'{self._endpoint}'"
        return f"{self.__class__.__name__}(endpoint={endpoint_str}, bg_role={self._bg_role})"


class PassThroughConnectRouting(BaseRouting, ConnectRouting):
    def __init__(self, endpoint: Optional[str], bg_role: Optional[BlueGreenRole]):
        super().__init__(endpoint, bg_role)

    def apply(
            self,
            plugin: Plugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable,
            plugin_service: PluginService) -> Optional[Connection]:
        return connect_func()


class RejectConnectRouting(BaseRouting, ConnectRouting):
    def __init__(self, endpoint: Optional[str], bg_role: Optional[BlueGreenRole]):
        super().__init__(endpoint, bg_role)

    def apply(
            self,
            plugin: Plugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable,
            plugin_service: PluginService) -> Optional[Connection]:
        raise AwsWrapperError(Messages.get("RejectConnectRouting.InProgressCantConnect"))


class SubstituteConnectRouting(BaseRouting, ConnectRouting):
    _rds_utils: ClassVar[RdsUtils] = RdsUtils()

    def __init__(
            self,
            substitute_host_info: HostInfo,
            endpoint: Optional[str] = None,
            bg_role: Optional[BlueGreenRole] = None,
            iam_hosts: Optional[Tuple[HostInfo, ...]] = None,
            iam_auth_success_handler: Optional[Callable[[str], None]] = None):
        super().__init__(endpoint, bg_role)
        self._substitute_host_info = substitute_host_info
        self._iam_hosts = iam_hosts
        self._iam_auth_success_handler = iam_auth_success_handler

    def __str__(self):
        iam_hosts_str = ',\n        '.join(str(iam_host) for iam_host in self._iam_hosts)
        return (f"{self.__class__.__name__}(\n"
                f"    substitute_host_info={self._substitute_host_info},\n"
                f"    endpoint={self._endpoint},\n"
                f"    bg_role={self._bg_role},\n"
                f"    iam_hosts=[\n"
                f"        {iam_hosts_str}\n"
                f"    ],\n"
                f"    hash={hex(hash(self))}\n"
                f")")

    def apply(
            self,
            plugin: Plugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable,
            plugin_service: PluginService) -> Optional[Connection]:
        if not SubstituteConnectRouting._rds_utils.is_ip(self._substitute_host_info.host):
            return plugin_service.connect(self._substitute_host_info, props, plugin)

        is_iam_in_use = plugin_service.is_plugin_in_use(IamAuthPlugin)
        if not is_iam_in_use:
            return plugin_service.connect(self._substitute_host_info, props, plugin)

        if not self._iam_hosts:
            raise AwsWrapperError(Messages.get("SubstituteConnectRouting.RequireIamHost"))

        for iam_host in self._iam_hosts:
            rerouted_host_info = copy(self._substitute_host_info)
            rerouted_host_info.host_id = iam_host.host_id
            rerouted_host_info.availability = HostAvailability.AVAILABLE
            rerouted_host_info.add_alias(iam_host.host)

            rerouted_props = copy(props)
            WrapperProperties.IAM_HOST.set(rerouted_props, iam_host.host)
            if iam_host.is_port_specified():
                WrapperProperties.IAM_DEFAULT_PORT.set(rerouted_props, iam_host.port)

            try:
                conn = plugin_service.connect(rerouted_host_info, rerouted_props)
                if self._iam_auth_success_handler is not None:
                    try:
                        self._iam_auth_success_handler(iam_host.host)
                    except Exception:
                        pass  # do nothing

                return conn
            except AwsWrapperError as e:
                if not plugin_service.is_login_exception(e):
                    raise e
                # do nothing - try with another iam host

        raise AwsWrapperError(
            Messages.get_formatted(
                "SubstituteConnectRouting.InProgressCantOpenConnection", self._substitute_host_info.url))


class SuspendConnectRouting(BaseRouting, ConnectRouting):
    _TELEMETRY_SWITCHOVER: ClassVar[str] = "Blue/Green switchover"
    _SLEEP_TIME_MS = 100

    def __init__(
            self,
            endpoint: Optional[str],
            bg_role: Optional[BlueGreenRole],
            bg_id: str):
        super().__init__(endpoint, bg_role)
        self._bg_id = bg_id

    def apply(
            self,
            plugin: Plugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable,
            plugin_service: PluginService) -> Optional[Connection]:
        logger.debug("SuspendConnectRouting.InProgressSuspendConnect")

        telemetry_factory = plugin_service.get_telemetry_factory()
        telemetry_context = telemetry_factory.open_telemetry_context(
            SuspendConnectRouting._TELEMETRY_SWITCHOVER, TelemetryTraceLevel.NESTED)

        bg_status = plugin_service.get_status(BlueGreenStatus, self._bg_id)
        timeout_ms = WrapperProperties.BG_CONNECT_TIMEOUT_MS.get_int(props)
        start_time_sec = time.time()
        end_time_sec = start_time_sec + timeout_ms / 1_000

        try:
            while time.time() <= end_time_sec and \
                    bg_status is not None and \
                    bg_status.phase == BlueGreenPhase.IN_PROGRESS:
                self.delay(SuspendConnectRouting._SLEEP_TIME_MS, bg_status, plugin_service, self._bg_id)
                bg_status = plugin_service.get_status(BlueGreenStatus, self._bg_id)

            if bg_status is not None and bg_status.phase == BlueGreenPhase.IN_PROGRESS:
                raise TimeoutError(
                    Messages.get_formatted("SuspendConnectRouting.InProgressTryConnectLater", timeout_ms))

            logger.debug(
                Messages.get_formatted(
                    "SuspendConnectRouting.SwitchoverCompleteContinueWithConnect",
                    (time.time() - start_time_sec) * 1000))
        finally:
            telemetry_context.close_context()

        # return None so that the next routing can attempt a connection
        return None


class SuspendUntilCorrespondingHostFoundConnectRouting(BaseRouting, ConnectRouting):
    _TELEMETRY_SWITCHOVER: ClassVar[str] = "Blue/Green switchover"
    _SLEEP_TIME_MS = 100

    def __init__(
            self,
            endpoint: Optional[str],
            bg_role: Optional[BlueGreenRole],
            bg_id: str):
        super().__init__(endpoint, bg_role)
        self._bg_id = bg_id

    def apply(
            self,
            plugin: Plugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable,
            plugin_service: PluginService) -> Optional[Connection]:
        logger.debug("SuspendConnectRouting.WaitConnectUntilCorrespondingHostFound", host_info.host)

        telemetry_factory = plugin_service.get_telemetry_factory()
        telemetry_context = telemetry_factory.open_telemetry_context(
            SuspendUntilCorrespondingHostFoundConnectRouting._TELEMETRY_SWITCHOVER, TelemetryTraceLevel.NESTED)

        bg_status = plugin_service.get_status(BlueGreenStatus, self._bg_id)
        corresponding_pair = None if bg_status is None else bg_status.corresponding_hosts.get(host_info.host)

        timeout_ms = WrapperProperties.BG_CONNECT_TIMEOUT_MS.get_int(props)
        start_time_sec = time.time()
        end_time_sec = start_time_sec + timeout_ms / 1_000

        try:
            while time.time() <= end_time_sec and \
                    bg_status is not None and \
                    bg_status.phase != BlueGreenPhase.COMPLETED and \
                    (corresponding_pair is None or corresponding_pair[1] is None):
                # wait until the corresponding host is found, or until switchover is completed
                self.delay(
                    SuspendUntilCorrespondingHostFoundConnectRouting._SLEEP_TIME_MS, bg_status, plugin_service, self._bg_id)
                bg_status = plugin_service.get_status(BlueGreenStatus, self._bg_id)
                corresponding_pair = None if bg_status is None else bg_status.corresponding_hosts.get(host_info.host)

            if bg_status is None or bg_status.phase == BlueGreenPhase.COMPLETED:
                logger.debug(
                    "SuspendUntilCorrespondingHostFoundConnectRouting.CompletedContinueWithConnect",
                    (time.time() - start_time_sec) * 1000)
                return None

            if time.time() > end_time_sec:
                raise TimeoutError(
                    Messages.get_formatted(
                        "SuspendUntilCorrespondingHostFoundConnectRouting.CorrespondingHostNotFoundTryConnectLater",
                        host_info.host,
                        (time.time() - start_time_sec) * 1000))

            logger.debug(
                Messages.get_formatted(
                    "SuspendUntilCorrespondingHostFoundConnectRouting.CorrespondingHostFoundContinueWithConnect",
                    host_info.host,
                    (time.time() - start_time_sec) * 1000))
        finally:
            telemetry_context.close_context()

        # return None so that the next routing can attempt a connection
        return None


class PassThroughExecuteRouting(BaseRouting, ExecuteRouting):
    def __init__(self, endpoint: Optional[str], bg_role: Optional[BlueGreenRole]):
        super().__init__(endpoint, bg_role)

    def apply(
            self,
            plugin: Plugin,
            plugin_service: PluginService,
            props: Properties,
            target: type,
            method_name: str,
            execute_func: Callable,
            *args: Any,
            **kwargs: Any) -> ValueContainer[Any]:
        return ValueContainer.of(execute_func())


class SuspendExecuteRouting(BaseRouting, ExecuteRouting):
    _TELEMETRY_SWITCHOVER: ClassVar[str] = "Blue/Green switchover"
    _SLEEP_TIME_MS = 100

    def __init__(
            self,
            endpoint: Optional[str],
            bg_role: Optional[BlueGreenRole],
            bg_id: str):
        super().__init__(endpoint, bg_role)
        self._bg_id = bg_id

    def apply(
            self,
            plugin: Plugin,
            plugin_service: PluginService,
            props: Properties,
            target: type,
            method_name: str,
            execute_func: Callable,
            *args: Any,
            **kwargs: Any) -> ValueContainer[Any]:
        logger.debug("SuspendExecuteRouting.InProgressSuspendMethod", method_name)

        telemetry_factory = plugin_service.get_telemetry_factory()
        telemetry_context = telemetry_factory.open_telemetry_context(
            SuspendExecuteRouting._TELEMETRY_SWITCHOVER, TelemetryTraceLevel.NESTED)

        bg_status = plugin_service.get_status(BlueGreenStatus, self._bg_id)
        timeout_ms = WrapperProperties.BG_CONNECT_TIMEOUT_MS.get_int(props)
        start_time_sec = time.time()
        end_time_sec = start_time_sec + timeout_ms / 1_000

        try:
            while time.time() <= end_time_sec and \
                    bg_status is not None and \
                    bg_status.phase == BlueGreenPhase.IN_PROGRESS:
                self.delay(SuspendExecuteRouting._SLEEP_TIME_MS, bg_status, plugin_service, self._bg_id)
                bg_status = plugin_service.get_status(BlueGreenStatus, self._bg_id)

            if bg_status is not None and bg_status.phase == BlueGreenPhase.IN_PROGRESS:
                raise TimeoutError(
                    Messages.get_formatted(
                        "SuspendExecuteRouting.InProgressTryMethodLater",
                        timeout_ms, method_name))

            logger.debug(
                Messages.get_formatted(
                    "SuspendExecuteRouting.SwitchoverCompleteContinueWithMethod",
                    method_name,
                    (time.time() - start_time_sec) * 1000))
        finally:
            telemetry_context.close_context()

        # return empty so that the next routing can attempt a connection
        return ValueContainer.empty()


class BlueGreenPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {"connect"}
    _CLOSE_METHODS: ClassVar[Set[str]] = {"Connection.close", "Cursor.close"}
    _status_providers: ClassVar[ConcurrentDict[str, BlueGreenStatusProvider]] = ConcurrentDict()

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._props = props
        self._telemetry_factory = plugin_service.get_telemetry_factory()
        self._provider_supplier: Callable[[PluginService, Properties, str], BlueGreenStatusProvider] = \
            lambda _plugin_service, _props, bg_id: BlueGreenStatusProvider(_plugin_service, _props, bg_id)
        self._bg_id = WrapperProperties.BG_ID.get_or_default(props).strip().lower()
        self._rds_utils = RdsUtils()
        self._bg_status: Optional[BlueGreenStatus] = None
        self._is_iam_in_use = False
        self._start_time_ns = AtomicInt(0)
        self._end_time_ns = AtomicInt(0)

        self._SUBSCRIBED_METHODS.update(self._plugin_service.network_bound_methods)

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        self._reset_routing_time()
        try:
            self._bg_status = self._plugin_service.get_status(BlueGreenStatus, self._bg_id)
            if self._bg_status is None:
                return self._open_direct_connection(connect_func, is_initial_connection)

            if is_initial_connection:
                self._is_iam_in_use = self._plugin_service.is_plugin_in_use(IamAuthPlugin)

            bg_role = self._bg_status.get_role(host_info)
            if bg_role is None:
                # The host is not participating in BG switchover - connect directly
                return self._open_direct_connection(connect_func, is_initial_connection)

            routing = next((r for r in self._bg_status.connect_routings if r.is_match(host_info, bg_role)), None)
            if not routing:
                return self._open_direct_connection(connect_func, is_initial_connection)

            self._start_time_ns.set(perf_counter_ns())
            conn: Optional[Connection] = None
            while routing is not None and conn is None:
                conn = routing.apply(self, host_info, props, is_initial_connection, connect_func, self._plugin_service)
                if conn is not None:
                    break

                latest_status = self._plugin_service.get_status(BlueGreenStatus, self._bg_id)
                if latest_status is None:
                    self._end_time_ns.set(perf_counter_ns())
                    return self._open_direct_connection(connect_func, is_initial_connection)

                routing = \
                    next((r for r in self._bg_status.connect_routings if r.is_match(host_info, bg_role)), None)

            self._end_time_ns.set(perf_counter_ns())
            if conn is None:
                conn = connect_func()

            if is_initial_connection:
                self._init_status_provider()

            return conn
        finally:
            if self._start_time_ns.get() > 0:
                self._end_time_ns.compare_and_set(0, perf_counter_ns())

    def _reset_routing_time(self):
        self._start_time_ns.set(0)
        self._end_time_ns.set(0)

    def _open_direct_connection(self, connect_func: Callable, is_initial_connection: bool) -> Connection:
        conn = connect_func()
        if is_initial_connection:
            self._init_status_provider()

        return conn

    def _init_status_provider(self):
        self._status_providers.compute_if_absent(
            self._bg_id,
            lambda key: self._provider_supplier(self._plugin_service, self._props, self._bg_id))

    def execute(self, target: type, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        self._reset_routing_time()
        try:
            self._init_status_provider()
            if method_name in BlueGreenPlugin._CLOSE_METHODS:
                return execute_func()

            self._bg_status = self._plugin_service.get_status(BlueGreenStatus, self._bg_id)
            if self._bg_status is None:
                return execute_func()

            host_info = self._plugin_service.current_host_info
            bg_role = None if host_info is None else self._bg_status.get_role(host_info)
            if bg_role is None:
                # The host is not participating in BG switchover - execute directly
                return execute_func()

            routing = next((r for r in self._bg_status.execute_routings if r.is_match(host_info, bg_role)), None)
            if routing is None:
                return execute_func()

            result_container: ValueContainer[Any] = ValueContainer.empty()
            self._start_time_ns.set(perf_counter_ns())
            while routing is not None and not result_container.is_present():
                result_container = routing.apply(
                    self,
                    self._plugin_service,
                    self._props,
                    target,
                    method_name,
                    execute_func,
                    *args,
                    **kwargs)
                if result_container.is_present():
                    break

                latest_status = self._plugin_service.get_status(BlueGreenStatus, self._bg_id)
                if latest_status is None:
                    self._end_time_ns.set(perf_counter_ns())
                    return execute_func()

                routing = \
                    next((r for r in self._bg_status.execute_routings if r.is_match(host_info, bg_role)), None)

            self._end_time_ns.set(perf_counter_ns())
            if result_container.is_present():
                return result_container.get()

            return execute_func()
        finally:
            if self._start_time_ns.get() > 0:
                self._end_time_ns.compare_and_set(0, perf_counter_ns())

    # For testing purposes only.
    def get_hold_time_ns(self) -> int:
        if self._start_time_ns.get() == 0:
            return 0

        if self._end_time_ns.get() == 0:
            return perf_counter_ns() - self._start_time_ns.get()
        else:
            return self._end_time_ns.get() - self._start_time_ns.get()


class BlueGreenPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return BlueGreenPlugin(plugin_service, props)


BlueGreenInterimStatusProcessor = Callable[[BlueGreenRole, BlueGreenInterimStatus], None]


class BlueGreenStatusMonitor:
    _DEFAULT_STATUS_CHECK_INTERVAL_MS: ClassVar[int] = 5 * 60_000  # 5 minutes
    _BG_CLUSTER_ID: ClassVar[str] = "941d00a8-8238-4f7d-bf59-771bff783a8e"
    _LATEST_KNOWN_VERSION: ClassVar[str] = "1.0"
    # Add more versions here if needed.
    _KNOWN_VERSIONS: ClassVar[FrozenSet[str]] = frozenset({_LATEST_KNOWN_VERSION})

    def __init__(
            self,
            bg_role: BlueGreenRole,
            bg_id: str,
            initial_host_info: HostInfo,
            plugin_service: PluginService,
            props: Properties,
            status_check_intervals_ms: Dict[BlueGreenIntervalRate, int],
            interim_status_processor: Optional[BlueGreenInterimStatusProcessor] = None):
        self._bg_role = bg_role
        self._bg_id = bg_id
        self._initial_host_info = initial_host_info
        self._plugin_service = plugin_service

        # autocommit is False by default. When False, the BG status query may return stale data, so we set it to True.
        props["autocommit"] = True
        self._props = props
        self._status_check_intervals_ms = status_check_intervals_ms
        self._interim_status_processor = interim_status_processor

        self._rds_utils = RdsUtils()
        self._cv = Condition()
        self.should_collect_ip_addresses = Event()
        self.should_collect_ip_addresses.set()
        self.should_collect_topology = Event()
        self.should_collect_topology.set()
        self.use_ip_address = Event()
        self._panic_mode = Event()
        self._panic_mode.set()
        self.stop = Event()
        self.interval_rate = BlueGreenIntervalRate.BASELINE
        self._host_list_provider: Optional[HostListProvider] = None
        self._start_topology: Tuple[HostInfo, ...] = ()
        self._current_topology: Tuple[HostInfo, ...] = ()
        self._start_ip_addresses_by_host: ConcurrentDict[str, ValueContainer[str]] = ConcurrentDict()
        self._current_ip_addresses_by_host: ConcurrentDict[str, ValueContainer[str]] = ConcurrentDict()
        self._all_start_topology_ip_changed = False
        self._all_start_topology_endpoints_removed = False
        self._all_topology_changed = False
        self._current_phase: Optional[BlueGreenPhase] = BlueGreenPhase.NOT_CREATED
        self._host_names: Set[str] = set()
        self._version = "1.0"
        self._port = -1
        self._connection: Optional[Connection] = None
        self._connection_host_info: Optional[HostInfo] = None
        self._connected_ip_address: Optional[str] = None
        self._is_host_info_correct = Event()
        self._has_started = Event()

        db_dialect = self._plugin_service.database_dialect
        if not isinstance(db_dialect, BlueGreenDialect):
            raise AwsWrapperError(Messages.get_formatted("BlueGreenStatusMonitor.UnexpectedDialect", db_dialect))

        self._bg_dialect: BlueGreenDialect = cast('BlueGreenDialect', self._plugin_service.database_dialect)

        self._open_connection_thread: Optional[Thread] = None
        self._monitor_thread = Thread(daemon=True, name="BlueGreenMonitorThread", target=self._run)

    def start(self):
        if not self._has_started.is_set():
            self._has_started.set()
            self._monitor_thread.start()

    def _run(self):
        try:
            while not self.stop.is_set():
                try:
                    old_phase = self._current_phase
                    self._open_connection()
                    self._collect_status()
                    self.collect_topology()
                    self._collect_ip_addresses()
                    self._update_ip_address_flags()

                    if self._current_phase is not None and (old_phase is None or old_phase != self._current_phase):
                        logger.debug("BlueGreenStatusMonitor.StatusChanged", self._bg_role, self._current_phase)

                    if self._interim_status_processor is not None:
                        self._interim_status_processor(
                            self._bg_role,
                            BlueGreenInterimStatus(
                                self._current_phase,
                                self._version,
                                self._port,
                                self._start_topology,
                                self._start_ip_addresses_by_host,
                                self._current_topology,
                                self._current_ip_addresses_by_host,
                                self._host_names,
                                self._all_start_topology_ip_changed,
                                self._all_start_topology_endpoints_removed,
                                self._all_topology_changed)
                        )

                    interval_rate = BlueGreenIntervalRate.HIGH if self._panic_mode.is_set() else self.interval_rate
                    delay_ms = self._status_check_intervals_ms.get(
                        interval_rate, BlueGreenStatusMonitor._DEFAULT_STATUS_CHECK_INTERVAL_MS)
                    self._delay(delay_ms)
                except Exception as e:
                    logger.warning("BlueGreenStatusMonitor.MonitoringUnhandledException", self._bg_role, e)

        finally:
            self._close_connection()
            logger.debug("BlueGreenStatusMonitor.ThreadCompleted", self._bg_role)

    def _open_connection(self):
        conn = self._connection
        if not self._is_connection_closed(conn):
            return

        if self._open_connection_thread is not None:
            if self._open_connection_thread.is_alive():
                return  # The task to open the connection is in progress, let's wait.
            elif not self._panic_mode.is_set():
                return  # The connection should be open by now since the open connection task is not running.

        self._connection = None
        self._panic_mode.set()
        self._open_connection_thread = \
            Thread(daemon=True, name="BlueGreenMonitorConnectionOpener", target=self._open_connection_task)
        self._open_connection_thread.start()

    def _open_connection_task(self):
        host_info = self._connection_host_info
        ip_address = self._connected_ip_address
        if host_info is None:
            self._connection_host_info = self._initial_host_info
            host_info = self._initial_host_info
            self._connected_ip_address = None
            ip_address = None
            self._is_host_info_correct.clear()

        try:
            if self.use_ip_address.is_set() and ip_address is not None:
                ip_host_info = copy(host_info)
                ip_host_info.host = ip_address
                props_copy = copy(self._props)
                WrapperProperties.IAM_HOST.set(props_copy, ip_host_info.host)

                logger.debug(
                    "BlueGreenStatusMonitor.OpeningConnectionWithIp", self._bg_role, ip_host_info.host)
                self._connection = self._plugin_service.force_connect(ip_host_info, props_copy)
                logger.debug(
                    "BlueGreenStatusMonitor.OpenedConnectionWithIp", self._bg_role, ip_host_info.host)
            else:
                logger.debug("BlueGreenStatusMonitor.OpeningConnection", self._bg_role, host_info.host)
                self._connection = self._plugin_service.force_connect(host_info, self._props)
                self._connected_ip_address = self._get_ip_address(host_info.host).or_else(None)
                logger.debug("BlueGreenStatusMonitor.OpenedConnection", self._bg_role, host_info.host)

            self._panic_mode.clear()
            self._notify_changes()
        except Exception:
            # Attempt to open connection failed.
            self._connection = None
            self._panic_mode.set()
            self._notify_changes()

    def _get_ip_address(self, host: str) -> ValueContainer[str]:
        try:
            return ValueContainer.of(socket.gethostbyname(host))
        except socket.gaierror:
            return ValueContainer.empty()

    def _notify_changes(self):
        with self._cv:
            self._cv.notify_all()

    def _collect_status(self):
        conn = self._connection
        try:
            if self._is_connection_closed(conn):
                return

            if not self._bg_dialect.is_blue_green_status_available(conn):
                if self._plugin_service.driver_dialect.is_closed(conn):
                    self._connection = None
                    self._current_phase = None
                    self._panic_mode.set()
                else:
                    self._current_phase = BlueGreenPhase.NOT_CREATED
                    logger.debug(
                        "BlueGreenStatusMonitor.StatusNotAvailable", self._bg_role, BlueGreenPhase.NOT_CREATED)
                return

            status_entries = []
            with conn.cursor() as cursor:
                cursor.execute(self._bg_dialect.blue_green_status_query)
                for record in cursor:
                    # columns: version, endpoint, port, role, status
                    version = record[0]
                    if version not in BlueGreenStatusMonitor._KNOWN_VERSIONS:
                        self._version = BlueGreenStatusMonitor._LATEST_KNOWN_VERSION
                        logger.warning(
                            "BlueGreenStatusMonitor.UsesVersion", self._bg_role, version, self._version)

                    endpoint = record[1]
                    port = record[2]
                    bg_role = BlueGreenRole.parse_role(record[3], self._version)
                    phase = BlueGreenPhase.parse_phase(record[4])

                    if self._bg_role != bg_role:
                        continue

                    status_entries.append(BlueGreenDbStatusInfo(version, endpoint, port, phase, bg_role))

            # Attempt to find the writer cluster status info
            status_info = next((status for status in status_entries
                                if self._rds_utils.is_writer_cluster_dns(status.endpoint) and
                                self._rds_utils.is_not_old_instance(status.endpoint)),
                               None)
            if status_info is None:
                # Grab an instance endpoint instead
                status_info = next((status for status in status_entries
                                    if self._rds_utils.is_rds_instance(status.endpoint) and
                                    self._rds_utils.is_not_old_instance(status.endpoint)),
                                   None)
            else:
                # Writer cluster endpoint has been found, add the reader cluster endpoint as well.
                self._host_names.add(status_info.endpoint.replace(".cluster-", ".cluster-ro-"))

            if status_info is None:
                if len(status_entries) == 0:
                    # The status table may have no entries after BGD is completed. The old1 cluster/instance has
                    # been separated and no longer receives updates from the related green cluster/instance.
                    if self._bg_role != BlueGreenRole.SOURCE:
                        logger.warning("BlueGreenStatusMonitor.NoEntriesInStatusTable", self._bg_role)

                    self._current_phase = None
            else:
                self._current_phase = status_info.phase
                self._version = status_info.version
                self._port = status_info.port

            if self.should_collect_topology.is_set():
                current_host_names = {status.endpoint.lower() for status in status_entries
                                      if status.endpoint is not None and
                                      self._rds_utils.is_not_old_instance(status.endpoint)}
                self._host_names.update(current_host_names)

            if not self._is_host_info_correct.is_set() and status_info is not None:
                # We connected to an initial host info that might not be the desired blue or green cluster. Let's check
                # if we need to reconnect to the correct one.
                status_info_ip_address = self._get_ip_address(status_info.endpoint).or_else(None)
                connected_ip_address = self._connected_ip_address
                if connected_ip_address is not None and connected_ip_address != status_info_ip_address:
                    # We are not connected to the desired blue or green cluster, we need to reconnect.
                    self._connection_host_info = HostInfo(host=status_info.endpoint, port=status_info.port)
                    self._props["host"] = status_info.endpoint
                    self._is_host_info_correct.set()
                    self._close_connection()
                    self._panic_mode.set()
                else:
                    # We are already connected to the right host.
                    self._is_host_info_correct.set()
                    self._panic_mode.clear()

            if self._is_host_info_correct.is_set() and self._host_list_provider is None:
                # A connection to the correct cluster (blue or green) has been established. Let's initialize the host
                # list provider.
                self._init_host_list_provider()
        except Exception as e:
            if not self._is_connection_closed(self._connection):
                # It's normal to get a connection closed error during BGD switchover, but the connection isn't closed so
                # let's log the error.
                logger.debug("BlueGreenStatusMonitor.UnhandledException", self._bg_role, e)
            self._close_connection()
            self._panic_mode.set()

    def _close_connection(self):
        conn = self._connection
        self._connection = None
        if conn is not None and not self._plugin_service.driver_dialect.is_closed(conn):
            try:
                conn.close()
            except Exception:
                pass

    def _init_host_list_provider(self):
        if self._host_list_provider is not None or not self._is_host_info_correct.is_set():
            return

        # We need to instantiate a separate HostListProvider with a special unique cluster ID to avoid interference with
        # other HostListProviders opened for this cluster. Blue and Green clusters should have different cluster IDs.

        props_copy = copy(self._props)
        cluster_id = f"{self._bg_id}::{self._bg_role}::{BlueGreenStatusMonitor._BG_CLUSTER_ID}"
        WrapperProperties.CLUSTER_ID.set(props_copy, cluster_id)
        logger.debug("BlueGreenStatusMonitor.CreateHostListProvider", self._bg_role, cluster_id)

        host_info = self._connection_host_info
        if host_info is None:
            logger.warning("BlueGreenStatusMonitor.HostInfoNone")
            return

        host_list_provider_supplier = self._plugin_service.database_dialect.get_host_list_provider_supplier()
        host_list_provider_service: HostListProviderService = cast('HostListProviderService', self._plugin_service)
        self._host_list_provider = host_list_provider_supplier(host_list_provider_service, props_copy)

    def _is_connection_closed(self, conn: Optional[Connection]) -> bool:
        return conn is None or self._plugin_service.driver_dialect.is_closed(conn)

    def _delay(self, delay_ms: int):
        start_ns = perf_counter_ns()
        end_ns = start_ns + delay_ms * 1_000_000
        initial_interval_rate = self.interval_rate
        initial_panic_mode_val = self._panic_mode.is_set()
        min_delay_sec = min(delay_ms, 50) / 1_000

        while self.interval_rate == initial_interval_rate and \
                perf_counter_ns() < end_ns and \
                not self.stop.is_set() and \
                initial_panic_mode_val == self._panic_mode.is_set():
            with self._cv:
                self._cv.wait(min_delay_sec)

    def collect_topology(self):
        if self._host_list_provider is None:
            return

        conn = self._connection
        if self._is_connection_closed(conn):
            return

        self._current_topology = self._host_list_provider.force_refresh(conn)
        if self.should_collect_topology.is_set():
            self._start_topology = self._current_topology

        current_topology_copy = self._current_topology
        if current_topology_copy is not None and self.should_collect_topology.is_set():
            self._host_names.update({host_info.host for host_info in current_topology_copy})

    def _collect_ip_addresses(self):
        self._current_ip_addresses_by_host.clear()
        if self._host_names is not None:
            for host in self._host_names:
                self._current_ip_addresses_by_host.put_if_absent(host, self._get_ip_address(host))

        if self.should_collect_ip_addresses.is_set():
            self._start_ip_addresses_by_host.clear()
            self._start_ip_addresses_by_host.put_all(self._current_ip_addresses_by_host)

    def _update_ip_address_flags(self):
        if self.should_collect_topology.is_set():
            self._all_start_topology_ip_changed = False
            self._all_start_topology_endpoints_removed = False
            self._all_topology_changed = False
            return

        if not self.should_collect_ip_addresses.is_set():
            # Check whether all hosts in start_topology resolve to new IP addresses
            self._all_start_topology_ip_changed = self._has_all_start_topology_ip_changed()

        # Check whether all hosts in start_topology no longer have IP addresses. This indicates that the start_topology
        # hosts can no longer be resolved because their DNS entries no longer exist.
        self._all_start_topology_endpoints_removed = self._are_all_start_endpoints_removed()

        if not self.should_collect_topology.is_set():
            # Check whether all hosts in current_topology do not exist in start_topology
            start_topology_hosts = set() if self._start_topology is None else \
                {host_info.host for host_info in self._start_topology}
            current_topology_copy = self._current_topology
            self._all_topology_changed = (
                    current_topology_copy and
                    start_topology_hosts and
                    all(host_info.host not in start_topology_hosts for host_info in current_topology_copy))

    def _has_all_start_topology_ip_changed(self) -> bool:
        if not self._start_topology:
            return False

        for host_info in self._start_topology:
            start_ip_container = self._start_ip_addresses_by_host.get(host_info.host)
            current_ip_container = self._current_ip_addresses_by_host.get(host_info.host)
            if start_ip_container is None or not start_ip_container.is_present() or \
                    current_ip_container is None or not current_ip_container.is_present():
                return False

            if start_ip_container.get() == current_ip_container.get():
                return False

        return True

    def _are_all_start_endpoints_removed(self) -> bool:
        start_topology = self._start_topology
        if not start_topology:
            return False

        for host_info in start_topology:
            start_ip_container = self._start_ip_addresses_by_host.get(host_info.host)
            current_ip_container = self._current_ip_addresses_by_host.get(host_info.host)
            if start_ip_container is None or current_ip_container is None or \
                    not start_ip_container.is_present() or current_ip_container.is_present():
                return False

        return True

    def reset_collected_data(self):
        self._start_ip_addresses_by_host.clear()
        self._start_topology = []
        self._host_names.clear()


@dataclass
class BlueGreenDbStatusInfo:
    version: str
    endpoint: str
    port: int
    phase: BlueGreenPhase
    bg_role: BlueGreenRole


class BlueGreenStatusProvider:
    _MONITORING_PROPERTY_PREFIX: ClassVar[str] = "blue-green-monitoring-"
    _DEFAULT_CONNECT_TIMEOUT_MS: ClassVar[int] = 10_000
    _DEFAULT_SOCKET_TIMEOUT_MS: ClassVar[int] = 10_000

    def __init__(self, plugin_service: PluginService, props: Properties, bg_id: str):
        self._plugin_service = plugin_service
        self._props = props
        self._bg_id = bg_id

        self._interim_status_hashes = [0, 0]
        self._latest_context_hash = 0
        self._interim_statuses: List[Optional[BlueGreenInterimStatus]] = [None, None]
        self._host_ip_addresses: ConcurrentDict[str, ValueContainer[str]] = ConcurrentDict()
        # The second element of the Tuple is None when no corresponding host is found.
        self._corresponding_hosts: ConcurrentDict[str, Tuple[HostInfo, Optional[HostInfo]]] = ConcurrentDict()
        # Keys are host URLs (port excluded)
        self._roles_by_host: ConcurrentDict[str, BlueGreenRole] = ConcurrentDict()
        self._iam_auth_success_hosts: ConcurrentDict[str, ConcurrentSet[str]] = ConcurrentDict()
        self._green_host_name_change_times: ConcurrentDict[str, datetime] = ConcurrentDict()
        self._summary_status: Optional[BlueGreenStatus] = None
        self._latest_phase = BlueGreenPhase.NOT_CREATED
        self._rollback = False
        self._blue_dns_update_completed = False
        self._green_dns_removed = False
        self._green_topology_changed = False
        self._all_green_hosts_changed_name = False
        self._post_status_end_time_ns = 0
        self._process_status_lock = RLock()
        self._status_check_intervals_ms: Dict[BlueGreenIntervalRate, int] = {}
        self._phase_times_ns: ConcurrentDict[str, PhaseTimeInfo] = ConcurrentDict()
        self._rds_utils = RdsUtils()

        self._switchover_timeout_ns = WrapperProperties.BG_SWITCHOVER_TIMEOUT_MS.get_int(props) * 1_000_000
        self._suspend_blue_connections_when_in_progress = (
            WrapperProperties.BG_SUSPEND_NEW_BLUE_CONNECTIONS.get_bool(props))
        self._status_check_intervals_ms.update({
            BlueGreenIntervalRate.BASELINE: WrapperProperties.BG_INTERVAL_BASELINE_MS.get_int(props),
            BlueGreenIntervalRate.INCREASED: WrapperProperties.BG_INTERVAL_INCREASED_MS.get_int(props),
            BlueGreenIntervalRate.HIGH: WrapperProperties.BG_INTERVAL_HIGH_MS.get_int(props)
        })

        dialect = self._plugin_service.database_dialect
        if not isinstance(dialect, BlueGreenDialect):
            raise AwsWrapperError(
                Messages.get_formatted(
                    "BlueGreenStatusProvider.UnsupportedDialect", self._bg_id, dialect.__class__.__name__))

        current_host_info = self._plugin_service.current_host_info
        blue_monitor = BlueGreenStatusMonitor(
            BlueGreenRole.SOURCE,
            self._bg_id,
            current_host_info,
            self._plugin_service,
            self._get_monitoring_props(),
            self._status_check_intervals_ms,
            self._process_interim_status)
        green_monitor = BlueGreenStatusMonitor(
            BlueGreenRole.TARGET,
            self._bg_id,
            current_host_info,
            self._plugin_service,
            self._get_monitoring_props(),
            self._status_check_intervals_ms,
            self._process_interim_status)

        self._monitors: List[BlueGreenStatusMonitor] = [blue_monitor, green_monitor]

        for monitor in self._monitors:
            monitor.start()

    def _get_monitoring_props(self) -> Properties:
        monitoring_props = copy(self._props)
        for key in self._props.keys():
            if key.startswith(BlueGreenStatusProvider._MONITORING_PROPERTY_PREFIX):
                new_key = key[len(BlueGreenStatusProvider._MONITORING_PROPERTY_PREFIX):]
                monitoring_props[new_key] = self._props[key]
                monitoring_props.pop(key, None)

        monitoring_props.put_if_absent(
            WrapperProperties.CONNECT_TIMEOUT_SEC.name, BlueGreenStatusProvider._DEFAULT_CONNECT_TIMEOUT_MS // 1_000)
        monitoring_props.put_if_absent(
            WrapperProperties.SOCKET_TIMEOUT_SEC.name, BlueGreenStatusProvider._DEFAULT_SOCKET_TIMEOUT_MS // 1_000)
        return monitoring_props

    def _process_interim_status(self, bg_role: BlueGreenRole, interim_status: BlueGreenInterimStatus):
        with self._process_status_lock:
            status_hash = interim_status.get_custom_hashcode()
            context_hash = self._get_context_hash()
            if self._interim_status_hashes[bg_role.value] == status_hash and self._latest_context_hash == context_hash:
                # no changes detected
                return

            logger.debug("BlueGreenStatusProvider.InterimStatus", self._bg_id, bg_role, interim_status)
            self._update_phase(bg_role, interim_status)

            # Store interim_status and corresponding hash
            self._interim_statuses[bg_role.value] = interim_status
            self._interim_status_hashes[bg_role.value] = status_hash
            self._latest_context_hash = context_hash

            # Update map of IP addresses.
            self._host_ip_addresses.put_all(interim_status.start_ip_addresses_by_host_map)

            # Update role_by_host based on the provided host names.
            self._roles_by_host.put_all({host_name.lower(): bg_role for host_name in interim_status.host_names})

            self._update_corresponding_hosts()
            self._update_summary_status(bg_role, interim_status)
            self._update_monitors()
            self._update_status_cache()
            self._log_current_context()
            self._log_switchover_final_summary()
            self._reset_context_when_completed()

    def _get_context_hash(self) -> int:
        result = self._get_value_hash(1, str(self._all_green_hosts_changed_name))
        result = self._get_value_hash(result, str(len(self._iam_auth_success_hosts)))
        return result

    def _get_value_hash(self, current_hash: int, val: str) -> int:
        return current_hash * 31 + hash(val)

    def _update_phase(self, bg_role: BlueGreenRole, interim_status: BlueGreenInterimStatus):
        role_status = self._interim_statuses[bg_role.value]
        latest_phase = BlueGreenPhase.NOT_CREATED if role_status is None else role_status.phase
        if latest_phase is not None and \
                interim_status.phase is not None and \
                interim_status.phase.value < latest_phase.value:
            self._rollback = True
            logger.debug("BlueGreenStatusProvider.Rollback", self._bg_id)

        if interim_status.phase is None:
            return

        # The phase should not move backwards unless we're rolling back.
        if self._rollback:
            if interim_status.phase.value < self._latest_phase.value:
                self._latest_phase = interim_status.phase
        else:
            if interim_status.phase.value >= self._latest_phase.value:
                self._latest_phase = interim_status.phase

    def _update_corresponding_hosts(self):
        """
        Update corresponding hosts. The blue writer host is mapped to the green writer host, and each blue reader host is
        mapped to a green reader host
        """

        self._corresponding_hosts.clear()
        source_status = self._interim_statuses[BlueGreenRole.SOURCE.value]
        target_status = self._interim_statuses[BlueGreenRole.TARGET.value]
        if source_status is None or target_status is None:
            return

        if source_status.start_topology and target_status.start_topology:
            blue_writer_host_info = self._get_writer_host(BlueGreenRole.SOURCE)
            green_writer_host_info = self._get_writer_host(BlueGreenRole.TARGET)
            sorted_blue_readers = self._get_reader_hosts(BlueGreenRole.SOURCE)
            sorted_green_readers = self._get_reader_hosts(BlueGreenRole.TARGET)

            if blue_writer_host_info is not None:
                # green_writer_host_info may be None, but that will be handled properly by the corresponding routing.
                self._corresponding_hosts.put(
                    blue_writer_host_info.host, (blue_writer_host_info, green_writer_host_info))

            if sorted_blue_readers:
                # Map blue readers to green hosts
                if sorted_green_readers:
                    # Map each to blue reader to a green reader.
                    green_index = 0
                    for blue_host_info in sorted_blue_readers:
                        self._corresponding_hosts.put(
                            blue_host_info.host, (blue_host_info, sorted_green_readers[green_index]))
                        green_index += 1
                        # The modulo operation prevents us from exceeding the bounds of sorted_green_readers if there are
                        # more blue readers than green readers. In this case, multiple blue readers may be mapped to the
                        # same green reader.
                        green_index %= len(sorted_green_readers)
                else:
                    # There's no green readers - map all blue reader hosts to the green writer
                    for blue_host_info in sorted_blue_readers:
                        self._corresponding_hosts.put(blue_host_info.host, (blue_host_info, green_writer_host_info))

        if source_status.host_names and target_status.host_names:
            blue_hosts = source_status.host_names
            green_hosts = target_status.host_names

            # Map blue writer cluster host to green writer cluster host.
            blue_cluster_host = next(
                (blue_host for blue_host in blue_hosts if self._rds_utils.is_writer_cluster_dns(blue_host)),
                None)
            green_cluster_host = next(
                (green_host for green_host in green_hosts if self._rds_utils.is_writer_cluster_dns(green_host)),
                None)
            if blue_cluster_host and green_cluster_host:
                self._corresponding_hosts.put_if_absent(
                    blue_cluster_host, (HostInfo(host=blue_cluster_host), HostInfo(host=green_cluster_host)))

            # Map blue reader cluster host to green reader cluster host.
            blue_reader_cluster_host = next(
                (blue_host for blue_host in blue_hosts if self._rds_utils.is_reader_cluster_dns(blue_host)),
                None)
            green_reader_cluster_host = next(
                (green_host for green_host in green_hosts if self._rds_utils.is_reader_cluster_dns(green_host)),
                None)
            if blue_reader_cluster_host and green_reader_cluster_host:
                self._corresponding_hosts.put_if_absent(
                    blue_reader_cluster_host,
                    (HostInfo(host=blue_reader_cluster_host), HostInfo(host=green_reader_cluster_host)))

            # Map blue custom cluster hosts to green custom cluster hosts.
            for blue_host in blue_hosts:
                if not self._rds_utils.is_rds_custom_cluster_dns(blue_host):
                    continue

                custom_cluster_name = self._rds_utils.get_cluster_id(blue_host)
                if not custom_cluster_name:
                    continue

                corresponding_green_host = next(
                    (green_host for green_host in green_hosts
                     if self._rds_utils.is_rds_custom_cluster_dns(green_host)
                     and custom_cluster_name == self._rds_utils.remove_green_instance_prefix(
                        self._rds_utils.get_cluster_id(green_host))),
                    None
                )

                if corresponding_green_host:
                    self._corresponding_hosts.put_if_absent(
                        blue_host, (HostInfo(blue_host), HostInfo(corresponding_green_host)))

    def _get_writer_host(self, bg_role: BlueGreenRole) -> Optional[HostInfo]:
        role_status = self._interim_statuses[bg_role.value]
        if role_status is None:
            return None

        hosts = role_status.start_topology
        return next((host for host in hosts if host.role == HostRole.WRITER), None)

    def _get_reader_hosts(self, bg_role: BlueGreenRole) -> Optional[List[HostInfo]]:
        role_status = self._interim_statuses[bg_role.value]
        if role_status is None:
            return []

        hosts = role_status.start_topology
        reader_hosts = [host for host in hosts if host.role != HostRole.WRITER]
        reader_hosts.sort(key=lambda host_info: host_info.host)
        return reader_hosts

    def _update_summary_status(self, bg_role: BlueGreenRole, interim_status: BlueGreenInterimStatus):
        if self._latest_phase == BlueGreenPhase.NOT_CREATED:
            self._summary_status = BlueGreenStatus(self._bg_id, BlueGreenPhase.NOT_CREATED)

        elif self._latest_phase == BlueGreenPhase.CREATED:
            self._update_dns_flags(bg_role, interim_status)
            self._summary_status = self._get_status_of_created()

        elif self._latest_phase == BlueGreenPhase.PREPARATION:
            self._start_switchover_timer()
            self._update_dns_flags(bg_role, interim_status)
            self._summary_status = self._get_status_of_preparation()

        elif self._latest_phase == BlueGreenPhase.IN_PROGRESS:
            self._update_dns_flags(bg_role, interim_status)
            self._summary_status = self._get_status_of_in_progress()

        elif self._latest_phase == BlueGreenPhase.POST:
            self._update_dns_flags(bg_role, interim_status)
            self._summary_status = self._get_status_of_post()

        elif self._latest_phase == BlueGreenPhase.COMPLETED:
            self._update_dns_flags(bg_role, interim_status)
            self._summary_status = self._get_status_of_completed()

        else:
            raise ValueError(Messages.get_formatted("BlueGreenStatusProvider.UnknownPhase", self._bg_id, self._latest_phase))

    def _update_dns_flags(self, bg_role: BlueGreenRole, interim_status: BlueGreenInterimStatus):
        if bg_role == BlueGreenRole.SOURCE and not self._blue_dns_update_completed and interim_status.all_start_topology_ip_changed:
            logger.debug("BlueGreenStatusProvider.BlueDnsCompleted", self._bg_id)
            self._blue_dns_update_completed = True
            self._store_event_phase_time("Blue DNS updated")

        if bg_role == BlueGreenRole.TARGET and not self._green_dns_removed and interim_status.all_start_topology_endpoints_removed:
            logger.debug("BlueGreenStatusProvider.GreenDnsRemoved", self._bg_id)
            self._green_dns_removed = True
            self._store_event_phase_time("Green DNS removed")

        if bg_role == BlueGreenRole.TARGET and not self._green_topology_changed and interim_status.all_topology_changed:
            logger.debug("BlueGreenStatusProvider.GreenTopologyChanged", self._bg_id)
            self._green_topology_changed = True
            self._store_event_phase_time("Green topology changed")

    def _store_event_phase_time(self, key_prefix: str, phase: Optional[BlueGreenPhase] = None):
        rollback_str = " (rollback)" if self._rollback else ""
        key = f"{key_prefix}{rollback_str}"
        self._phase_times_ns.put_if_absent(key, PhaseTimeInfo(datetime.now(), perf_counter_ns(), phase))

    def _start_switchover_timer(self):
        if self._post_status_end_time_ns == 0:
            self._post_status_end_time_ns = perf_counter_ns() + self._switchover_timeout_ns

    def _get_status_of_created(self) -> BlueGreenStatus:
        """
        New connect requests: go to blue or green hosts; default behaviour; no routing.
        Existing connections: default behaviour; no action.
        Execute JDBC calls: default behaviour; no action.
        """
        return BlueGreenStatus(
            self._bg_id,
            BlueGreenPhase.CREATED,
            [],
            [],
            self._roles_by_host,
            self._corresponding_hosts
        )

    def _get_status_of_preparation(self):
        """
        New connect requests to blue: route to corresponding IP address.
        New connect requests to green: route to corresponding IP address.
        New connect requests with IP address: default behaviour; no routing.
        Existing connections: default behaviour; no action.
        Execute JDBC calls: default behaviour; no action.
        """

        if self._is_switchover_timer_expired():
            logger.debug("BlueGreenStatusProvider.SwitchoverTimeout")
            if self._rollback:
                return self._get_status_of_created()
            return self._get_status_of_completed()

        connect_routings = self._get_blue_ip_address_connect_routings()
        return BlueGreenStatus(
            self._bg_id,
            BlueGreenPhase.PREPARATION,
            connect_routings,
            [],
            self._roles_by_host,
            self._corresponding_hosts
        )

    def _is_switchover_timer_expired(self) -> bool:
        return 0 < self._post_status_end_time_ns < perf_counter_ns()

    def _get_blue_ip_address_connect_routings(self) -> List[ConnectRouting]:
        connect_routings: List[ConnectRouting] = []
        for host, role in self._roles_by_host.items():
            host_pair = self._corresponding_hosts.get(host)
            if role == BlueGreenRole.TARGET or host_pair is None:
                continue

            blue_host_info = host_pair[0]
            blue_ip_container = self._host_ip_addresses.get(blue_host_info.host)
            if blue_ip_container is None or not blue_ip_container.is_present():
                blue_ip_host_info = blue_host_info
            else:
                blue_ip_host_info = copy(blue_host_info)
                blue_ip_host_info.host = blue_ip_container.get()

            host_routing = SubstituteConnectRouting(blue_ip_host_info, host, role, (blue_host_info,))
            interim_status = self._interim_statuses[role.value]
            if interim_status is None:
                continue

            host_and_port = self._get_host_and_port(host, interim_status.port)
            host_and_port_routing = SubstituteConnectRouting(blue_ip_host_info, host_and_port, role, (blue_host_info,))
            connect_routings.extend([host_routing, host_and_port_routing])

        return connect_routings

    def _get_host_and_port(self, host: str, port: int):
        return f"{host}:{port}" if port > 0 else host

    def _get_status_of_in_progress(self) -> BlueGreenStatus:
        """
        New connect requests to blue: suspend or route to corresponding IP address (depending on settings).
        New connect requests to green: suspend.
        New connect requests with IP address: suspend.
        Existing connections: default behaviour; no action.
        Execute JDBC calls: suspend.
        """

        if self._is_switchover_timer_expired():
            logger.debug("BlueGreenStatusProvider.SwitchoverTimeout")
            if self._rollback:
                return self._get_status_of_created()
            return self._get_status_of_completed()

        connect_routings: List[ConnectRouting] = []
        if self._suspend_blue_connections_when_in_progress:
            connect_routings.append(SuspendConnectRouting(None, BlueGreenRole.SOURCE, self._bg_id))
        else:
            # If we aren't suspending new blue connections, we should use IP addresses.
            connect_routings.extend(self._get_blue_ip_address_connect_routings())

        connect_routings.append(SuspendConnectRouting(None, BlueGreenRole.TARGET, self._bg_id))

        ip_addresses: Set[str] = {address_container.get() for address_container in self._host_ip_addresses.values()
                                  if address_container.is_present()}
        for ip_address in ip_addresses:
            if self._suspend_blue_connections_when_in_progress:
                # Check if the IP address belongs to one of the blue hosts.
                interim_status = self._interim_statuses[BlueGreenRole.SOURCE.value]
                if interim_status is not None and self._interim_status_contains_ip_address(interim_status, ip_address):
                    host_connect_routing = SuspendConnectRouting(ip_address, None, self._bg_id)
                    host_and_port = self._get_host_and_port(ip_address, interim_status.port)
                    host_port_connect_routing = SuspendConnectRouting(host_and_port, None, self._bg_id)
                    connect_routings.extend([host_connect_routing, host_port_connect_routing])
                    continue

            # Check if the IP address belongs to one of the green hosts.
            interim_status = self._interim_statuses[BlueGreenRole.TARGET.value]
            if interim_status is not None and self._interim_status_contains_ip_address(interim_status, ip_address):
                host_connect_routing = SuspendConnectRouting(ip_address, None, self._bg_id)
                host_and_port = self._get_host_and_port(ip_address, interim_status.port)
                host_port_connect_routing = SuspendConnectRouting(host_and_port, None, self._bg_id)
                connect_routings.extend([host_connect_routing, host_port_connect_routing])
                continue

        # All blue and green traffic should be suspended.
        execute_routings: List[ExecuteRouting] = [
            SuspendExecuteRouting(None, BlueGreenRole.SOURCE, self._bg_id),
            SuspendExecuteRouting(None, BlueGreenRole.TARGET, self._bg_id)]

        # All traffic through connections with IP addresses that belong to blue or green hosts should be suspended.
        for ip_address in ip_addresses:
            # Check if the IP address belongs to one of the blue hosts.
            interim_status = self._interim_statuses[BlueGreenRole.SOURCE.value]
            if interim_status is not None and self._interim_status_contains_ip_address(interim_status, ip_address):
                host_execute_routing = SuspendExecuteRouting(ip_address, None, self._bg_id)
                host_and_port = self._get_host_and_port(ip_address, interim_status.port)
                host_port_execute_routing = SuspendExecuteRouting(host_and_port, None, self._bg_id)
                execute_routings.extend([host_execute_routing, host_port_execute_routing])
                continue

            # Check if the IP address belongs to one of the green hosts.
            interim_status = self._interim_statuses[BlueGreenRole.TARGET.value]
            if interim_status is not None and self._interim_status_contains_ip_address(interim_status, ip_address):
                host_execute_routing = SuspendExecuteRouting(ip_address, None, self._bg_id)
                host_and_port = self._get_host_and_port(ip_address, interim_status.port)
                host_port_execute_routing = SuspendExecuteRouting(host_and_port, None, self._bg_id)
                execute_routings.extend([host_execute_routing, host_port_execute_routing])
                continue

            execute_routings.append(SuspendExecuteRouting(ip_address, None, self._bg_id))

        return BlueGreenStatus(
            self._bg_id,
            BlueGreenPhase.IN_PROGRESS,
            connect_routings,
            execute_routings,
            self._roles_by_host,
            self._corresponding_hosts
        )

    def _interim_status_contains_ip_address(self, interim_status: BlueGreenInterimStatus, ip_address: str) -> bool:
        for ip_address_container in interim_status.start_ip_addresses_by_host_map.values():
            if ip_address_container.is_present() and ip_address_container.get() == ip_address:
                return True

        return False

    def _get_status_of_post(self) -> BlueGreenStatus:
        if self._is_switchover_timer_expired():
            logger.debug("BlueGreenStatusProvider.SwitchoverTimeout")
            if self._rollback:
                return self._get_status_of_created()
            return self._get_status_of_completed()

        return BlueGreenStatus(
            self._bg_id,
            BlueGreenPhase.POST,
            self._get_post_status_connect_routings(),
            [],
            self._roles_by_host,
            self._corresponding_hosts
        )

    def _get_post_status_connect_routings(self) -> List[ConnectRouting]:
        if self._blue_dns_update_completed and self._all_green_hosts_changed_name:
            return [] if self._green_dns_removed else [RejectConnectRouting(None, BlueGreenRole.TARGET)]

        routings: List[ConnectRouting] = []
        # New connect calls to blue hosts should be routed to green hosts
        for host, role in self._roles_by_host.items():
            if role != BlueGreenRole.SOURCE or host not in self._corresponding_hosts.keys():
                continue

            blue_host = host
            is_blue_host_instance = self._rds_utils.is_rds_instance(blue_host)
            host_pair = self._corresponding_hosts.get(blue_host)
            blue_host_info = None if host_pair is None else host_pair[0]
            green_host_info = None if host_pair is None else host_pair[1]

            if green_host_info is None:
                # The corresponding green host was not found. We need to suspend the connection request.
                host_suspend_routing = SuspendUntilCorrespondingHostFoundConnectRouting(blue_host, role, self._bg_id)
                interim_status = self._interim_statuses[role.value]
                if interim_status is None:
                    continue

                host_and_port = self._get_host_and_port(blue_host, interim_status.port)
                host_port_suspend_routing = (
                    SuspendUntilCorrespondingHostFoundConnectRouting(host_and_port, None, self._bg_id))
                routings.extend([host_suspend_routing, host_port_suspend_routing])
            else:
                green_host = green_host_info.host
                green_ip_container = self._host_ip_addresses.get(green_host)
                if green_ip_container is None or not green_ip_container.is_present():
                    green_ip_host_info = green_host_info
                else:
                    green_ip_host_info = copy(green_host_info)
                    green_ip_host_info.host = green_ip_container.get()

                # Check whether the green host has already been connected to a non-prefixed blue IAM host name.
                if self._is_already_successfully_connected(green_host, blue_host):
                    # Green host has already changed its name, and it's not a new non-prefixed blue host.
                    iam_hosts: Optional[Tuple[HostInfo, ...]] = None if blue_host_info is None else (blue_host_info,)
                else:
                    # The green host has not yet changed its name, so we need to try both possible IAM hosts.
                    iam_hosts = (green_host_info,) if blue_host_info is None else (green_host_info, blue_host_info)

                iam_auth_success_handler = None if is_blue_host_instance \
                    else lambda iam_host: self._register_iam_host(green_host, iam_host)
                host_substitute_routing = SubstituteConnectRouting(
                    green_ip_host_info, blue_host, role, iam_hosts, iam_auth_success_handler)
                interim_status = self._interim_statuses[role.value]
                if interim_status is None:
                    continue

                host_and_port = self._get_host_and_port(blue_host, interim_status.port)
                host_port_substitute_routing = SubstituteConnectRouting(
                    green_ip_host_info, host_and_port, role, iam_hosts, iam_auth_success_handler)
                routings.extend([host_substitute_routing, host_port_substitute_routing])

        if not self._green_dns_removed:
            routings.append(RejectConnectRouting(None, BlueGreenRole.TARGET))

        return routings

    def _is_already_successfully_connected(self, connect_host: str, iam_host: str):
        success_hosts = self._iam_auth_success_hosts.compute_if_absent(connect_host, lambda _: ConcurrentSet())
        return success_hosts is not None and iam_host in success_hosts

    def _register_iam_host(self, connect_host: str, iam_host: str):
        success_hosts = self._iam_auth_success_hosts.compute_if_absent(connect_host, lambda _: ConcurrentSet())
        if success_hosts is None:
            success_hosts = ConcurrentSet()

        if connect_host != iam_host:
            if success_hosts is not None and iam_host in success_hosts:
                self._green_host_name_change_times.compute_if_absent(connect_host, lambda _: datetime.now())
                logger.debug("BlueGreenStatusProvider.GreenHostChangedName", connect_host, iam_host)

        success_hosts.add(iam_host)
        if connect_host != iam_host:
            # Check whether all IAM hosts have changed their names
            all_hosts_changed_names = all(
                any(iam_host != original_host for iam_host in iam_hosts)
                for original_host, iam_hosts in self._iam_auth_success_hosts.items()
                if iam_hosts  # Filter out empty sets
            )

            if all_hosts_changed_names and not self._all_green_hosts_changed_name:
                logger.debug("BlueGreenStatusProvider.AllGreenHostsChangedName")
                self._all_green_hosts_changed_name = True
                self._store_event_phase_time("Green host certificates changed")

    def _get_status_of_completed(self) -> BlueGreenStatus:
        if self._is_switchover_timer_expired():
            logger.debug("BlueGreenStatusProvider.SwitchoverTimeout")
            if self._rollback:
                return self._get_status_of_created()

            return BlueGreenStatus(
                self._bg_id, BlueGreenPhase.COMPLETED, [], [], self._roles_by_host, self._corresponding_hosts)

        if not self._blue_dns_update_completed or not self._green_dns_removed:
            return self._get_status_of_post()

        return BlueGreenStatus(
            self._bg_id, BlueGreenPhase.COMPLETED, [], [], self._roles_by_host, ConcurrentDict())

    def _update_monitors(self):
        phase = self._summary_status.phase
        if phase == BlueGreenPhase.NOT_CREATED:
            for monitor in self._monitors:
                monitor.interval_rate = BlueGreenIntervalRate.BASELINE
                monitor.should_collect_ip_addresses.clear()
                monitor.should_collect_topology.clear()
                monitor.use_ip_address.clear()
        elif phase == BlueGreenPhase.CREATED:
            for monitor in self._monitors:
                monitor.interval_rate = BlueGreenIntervalRate.INCREASED
                monitor.should_collect_ip_addresses.set()
                monitor.should_collect_topology.set()
                monitor.use_ip_address.clear()
                if self._rollback:
                    monitor.reset_collected_data()
        elif phase == BlueGreenPhase.PREPARATION \
                or phase == BlueGreenPhase.IN_PROGRESS \
                or phase == BlueGreenPhase.POST:
            for monitor in self._monitors:
                monitor.interval_rate = BlueGreenIntervalRate.HIGH
                monitor.should_collect_ip_addresses.clear()
                monitor.should_collect_topology.clear()
                monitor.use_ip_address.set()
        elif phase == BlueGreenPhase.COMPLETED:
            for monitor in self._monitors:
                monitor.interval_rate = BlueGreenIntervalRate.BASELINE
                monitor.should_collect_ip_addresses.clear()
                monitor.should_collect_topology.clear()
                monitor.use_ip_address.clear()
                monitor.reset_collected_data()

            # Stop monitoring old1 cluster/instance.
            if not self._rollback and self._monitors[BlueGreenRole.SOURCE.value] is not None:
                self._monitors[BlueGreenRole.SOURCE.value].stop.set()
        else:
            raise UnsupportedOperationError(
                Messages.get_formatted(
                    "BlueGreenStatusProvider.UnknownPhase", self._bg_id, self._summary_status.phase))

    def _update_status_cache(self):
        latest_status = self._plugin_service.get_status(BlueGreenStatus, self._bg_id)
        self._plugin_service.set_status(BlueGreenStatus, self._summary_status, self._bg_id)
        phase = self._summary_status.phase
        self._store_event_phase_time(phase.name, phase)

        if latest_status is not None:
            # Notify all waiting threads that the status has been updated.
            with latest_status.cv:
                latest_status.cv.notify_all()

    def _log_current_context(self):
        logger.debug(f"[bg_id: '{self._bg_id}'] Summary status: \n{self._summary_status}")
        hosts_str = "\n".join(
            f"   {blue_host} -> {host_pair[1] if host_pair else None}"
            for blue_host, host_pair in self._corresponding_hosts.items())
        logger.debug(f"Corresponding hosts:\n{hosts_str}")
        phase_times = \
            "\n".join(f"   {event_desc} -> {info.date_time}" for event_desc, info in self._phase_times_ns.items())
        logger.debug(f"Phase times:\n{phase_times}")
        change_name_times = \
            "\n".join(f"   {host} -> {date_time}" for host, date_time in self._green_host_name_change_times.items())
        logger.debug(f"Green host certificate change times:\n{change_name_times}")
        logger.debug("\n"
                     f"   latest_status_phase: {self._latest_phase}\n"
                     f"   blue_dns_update_completed: {self._blue_dns_update_completed}\n"
                     f"   green_dns_removed: {self._green_dns_removed}\n"
                     f"   all_green_hosts_changed_name: {self._all_green_hosts_changed_name}\n"
                     f"   green_topology_changed: {self._green_topology_changed}\n")

    def _log_switchover_final_summary(self):
        switchover_completed = (not self._rollback and self._summary_status.phase == BlueGreenPhase.COMPLETED) or \
                               (self._rollback and self._summary_status.phase == BlueGreenPhase.CREATED)
        has_active_switchover_phases = \
            any(phase_info.phase is not None and phase_info.phase.is_switchover_active_or_completed
                for phase_info in self._phase_times_ns.values())

        if not switchover_completed or not has_active_switchover_phases:
            return

        time_zero_phase = BlueGreenPhase.PREPARATION if self._rollback else BlueGreenPhase.IN_PROGRESS
        time_zero_key = f"{time_zero_phase.name} (rollback)" if self._rollback else time_zero_phase.name
        time_zero = self._phase_times_ns.get(time_zero_key)
        sorted_phase_entries = sorted(self._phase_times_ns.items(), key=lambda entry: entry[1].timestamp_ns)
        formatted_phase_entries = [
            "{:>28s} {:>18s} ms {:>31s}".format(
                str(entry[1].date_time),
                "" if time_zero is None else str((entry[1].timestamp_ns - time_zero.timestamp_ns) // 1_000_000),
                entry[0]
            ) for entry in sorted_phase_entries
        ]
        phase_times_str = "\n".join(formatted_phase_entries)
        divider = "----------------------------------------------------------------------------------\n"
        header = "{:<28s} {:>21s} {:>31s}\n".format("timestamp", "time offset (ms)", "event")
        log_message = (f"[bg_id: '{self._bg_id}']\n{divider}"
                       f"{header}{divider}"
                       f"{phase_times_str}\n{divider}")
        logger.debug(log_message)

    def _reset_context_when_completed(self):
        switchover_completed = (not self._rollback and self._summary_status.phase == BlueGreenPhase.COMPLETED) or \
                               (self._rollback and self._summary_status.phase == BlueGreenPhase.CREATED)
        has_active_switchover_phases = \
            any(phase_info.phase is not None and phase_info.phase.is_switchover_active_or_completed
                for phase_info in self._phase_times_ns.values())

        if not switchover_completed or not has_active_switchover_phases:
            return

        logger.debug("BlueGreenStatusProvider.ResetContext")
        self._rollback = False
        self._summary_status = None
        self._latest_phase = BlueGreenPhase.NOT_CREATED
        self._phase_times_ns.clear()
        self._blue_dns_update_completed = False
        self._green_dns_removed = False
        self._green_topology_changed = False
        self._all_green_hosts_changed_name = False
        self._post_status_end_time_ns = 0
        self._interim_status_hashes = [0, 0]
        self._latest_context_hash = 0
        self._interim_statuses = [None, None]
        self._host_ip_addresses.clear()
        self._corresponding_hosts.clear()
        self._roles_by_host.clear()
        self._iam_auth_success_hosts.clear()
        self._green_host_name_change_times.clear()


@dataclass
class PhaseTimeInfo:
    date_time: datetime
    timestamp_ns: int
    phase: Optional[BlueGreenPhase]
