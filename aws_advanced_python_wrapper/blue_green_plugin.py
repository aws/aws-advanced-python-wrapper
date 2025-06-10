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
from time import perf_counter_ns
from typing import TYPE_CHECKING, FrozenSet, cast

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
from threading import Condition, Event, Thread
from types import MappingProxyType
from typing import (Any, Callable, ClassVar, Dict, Optional, Protocol, Set,
                    Tuple)

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.iam_plugin import IamAuthPlugin
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.atomic import AtomicInt
from aws_advanced_python_wrapper.utils.concurrent import ConcurrentDict
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
    PREPARATION = (2, True)  # nodes are accessible
    IN_PROGRESS = (3, True)  # active phase; nodes are not accessible
    POST = (4, True)  # nodes are accessible; some change are still in progress
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

        match phase_str.upper():
            case "AVAILABLE":
                return BlueGreenPhase.CREATED
            case "SWITCHOVER_INITIATED":
                return BlueGreenPhase.PREPARATION
            case "SWITCHOVER_IN_PROGRESS":
                return BlueGreenPhase.IN_PROGRESS
            case "SWITCHOVER_IN_POST_PROCESSING":
                return BlueGreenPhase.POST
            case "SWITCHOVER_COMPLETED":
                return BlueGreenPhase.COMPLETED
            case _:
                raise ValueError(Messages.get_formatted("BlueGreenPhase.UnknownStatus", phase_str))


class BlueGreenRole(Enum):
    SOURCE = 0
    TARGET = 1

    @staticmethod
    def parse_role(role_str: str, version: str) -> BlueGreenRole:
        if "1.0" != version:
            raise ValueError(Messages.get_formatted("BlueGreenRole.UnknownVersion", version))

        match role_str:
            case "BLUE_GREEN_DEPLOYMENT_SOURCE":
                return BlueGreenRole.SOURCE
            case "BLUE_GREEN_DEPLOYMENT_TARGET":
                return BlueGreenRole.TARGET
            case _:
                raise ValueError(Messages.get_formatted("BlueGreenRole.UnknownRole", role_str))


class BlueGreenStatus:
    def __init__(
            self,
            bg_id: str,
            phase: BlueGreenPhase,
            connect_routing: Tuple[ConnectRouting, ...] = (),
            execute_routing: Tuple[ExecuteRouting, ...] = (),
            role_by_host: MappingProxyType[str, BlueGreenRole] = MappingProxyType({}),
            node_pairs_by_host: MappingProxyType[str, Tuple[HostInfo, Optional[HostInfo]]] = MappingProxyType({})):
        self.bg_id = bg_id
        self.phase = phase
        self.connect_routings = connect_routing
        self.execute_routings = execute_routing
        self.role_by_endpoint = role_by_host
        self.node_pairs_by_host = node_pairs_by_host

    def get_role(self, host_info: HostInfo) -> Optional[BlueGreenRole]:
        return self.role_by_endpoint.get(host_info.host.lower())

    def __str__(self) -> str:
        connect_routings_str = ',\n        '.join(str(cr) for cr in self.connect_routings)
        execute_routings_str = ',\n        '.join(str(er) for er in self.execute_routings)
        role_mappings = ',\n        '.join(f"{endpoint}: {role}" for endpoint, role in self.role_by_endpoint.items())

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
            tuple_str = ",".join(sorted(x.url + x.role for x in host_tuple))

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
        self._cv = Condition()
        self._endpoint = endpoint  # host and optionally port as well
        self._bg_role = bg_role

    def delay(self, delay_ms: int, bg_status: Optional[BlueGreenStatus], plugin_service: PluginService, bg_id: str):
        end_time_sec = time.time() + (delay_ms / 1_000)
        min_delay_ms = min(delay_ms, BaseRouting._MIN_SLEEP_MS)

        if bg_status is None:
            time.sleep(delay_ms / 1_000)
            return

        while bg_status is plugin_service.get_status(BlueGreenStatus, bg_id) and time.time() < end_time_sec:
            with self._cv:
                self._cv.wait(min_delay_ms / 1_000)

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
            endpoint: Optional[str],
            bg_role: Optional[BlueGreenRole],
            substitute_host_info: HostInfo,
            iam_hosts: Optional[Tuple[HostInfo, ...]],
            iam_auth_success_handler: Optional[IamAuthSuccessHandler]):
        super().__init__(endpoint, bg_role)
        self._substitute_host_info = substitute_host_info
        self._iam_hosts = iam_hosts
        self._iam_auth_success_handler = iam_auth_success_handler

    def __str__(self):
        iam_hosts_str = ',\n        '.join(str(iam_host) for iam_host in self._iam_hosts)
        return (f"{self.__class__.__name__}(\n"
                f"    endpoint={self._endpoint},\n"
                f"    bg_role={self._bg_role},\n"
                f"    substitute_host_info={self._substitute_host_info},\n"
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
            rerouted_host_info = copy(host_info)
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
                        self._iam_auth_success_handler.on_iam_success(iam_host.host)
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


class IamAuthSuccessHandler(Protocol):
    def on_iam_success(self, iam_host: str):
        ...


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
            while time.time() < end_time_sec and \
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
                    (time.time() - start_time_sec) / 1000))
        finally:
            telemetry_context.close_context()

        # return None so that the next routing can attempt a connection
        return None


class SuspendUntilCorrespondingNodeFoundConnectRouting(BaseRouting, ConnectRouting):
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
        logger.debug("SuspendConnectRouting.WaitConnectUntilCorrespondingNodeFound", host_info.host)

        telemetry_factory = plugin_service.get_telemetry_factory()
        telemetry_context = telemetry_factory.open_telemetry_context(
            SuspendUntilCorrespondingNodeFoundConnectRouting._TELEMETRY_SWITCHOVER, TelemetryTraceLevel.NESTED)

        bg_status = plugin_service.get_status(BlueGreenStatus, self._bg_id)
        corresponding_pair = None if bg_status is None else bg_status.node_pairs_by_host.get(host_info.host)

        timeout_ms = WrapperProperties.BG_CONNECT_TIMEOUT_MS.get_int(props)
        start_time_sec = time.time()
        end_time_sec = start_time_sec + timeout_ms / 1_000

        try:
            while time.time() < end_time_sec and \
                    bg_status is not None and \
                    bg_status.phase != BlueGreenPhase.COMPLETED and \
                    (corresponding_pair is None or corresponding_pair[1] is None):
                # wait until the corresponding node is found, or until switchover is completed
                self.delay(
                    SuspendUntilCorrespondingNodeFoundConnectRouting._SLEEP_TIME_MS, bg_status, plugin_service, self._bg_id)
                bg_status = plugin_service.get_status(BlueGreenStatus, self._bg_id)
                corresponding_pair = None if bg_status is None else bg_status.node_pairs_by_host.get(host_info.host)

            if bg_status is None or bg_status.phase == BlueGreenPhase.COMPLETED:
                logger.debug(
                    "SuspendUntilCorrespondingNodeFoundConnectRouting.CompletedContinueWithConnect",
                    (time.time() - start_time_sec) / 1000)
                return None

            if time.time() > end_time_sec:
                raise TimeoutError(
                    Messages.get_formatted(
                        "SuspendUntilCorrespondingNodeFoundConnectRouting.CorrespondingNodeNotFoundTryConnectLater",
                        host_info.host,
                        (time.time() - start_time_sec) / 1000))

            logger.debug(
                Messages.get_formatted(
                    "SuspendUntilCorrespondingNodeFoundConnectRouting.CorrespondingNodeFoundContinueWithConnect",
                    host_info.host,
                    (time.time() - start_time_sec) / 1000))
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
        logger.debug("SuspendExecuteRouting.InProgressSuspendMethod")

        telemetry_factory = plugin_service.get_telemetry_factory()
        telemetry_context = telemetry_factory.open_telemetry_context(
            SuspendExecuteRouting._TELEMETRY_SWITCHOVER, TelemetryTraceLevel.NESTED)

        bg_status = plugin_service.get_status(BlueGreenStatus, self._bg_id)
        timeout_ms = WrapperProperties.BG_CONNECT_TIMEOUT_MS.get_int(props)
        start_time_sec = time.time()
        end_time_sec = start_time_sec + timeout_ms / 1_000

        try:
            while time.time() < end_time_sec and \
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
                    (time.time() - start_time_sec) / 1000))
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
        self._provider_supplier: Callable = \
            lambda _plugin_service, _props, bg_id: BlueGreenStatusProvider(_plugin_service, _props, bg_id)
        self._bg_id = WrapperProperties.BG_ID.get_or_default(props).strip().lower()
        self._rds_utils = RdsUtils()
        self._bg_status: Optional[BlueGreenStatus] = None
        self._is_iam_in_use = False
        self._start_time_nano = AtomicInt(0)
        self._end_time_nano = AtomicInt(0)

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

            self._start_time_nano.set(perf_counter_ns())
            conn: Optional[Connection] = None
            while routing is not None and conn is None:
                conn = routing.apply(self, host_info, props, is_initial_connection, connect_func, self._plugin_service)
                if conn is None:
                    self._bg_status = self._plugin_service.get_status(BlueGreenStatus, self._bg_id)
                    if self._bg_status is None:
                        # TODO: should we just continue in this case?
                        continue
                    routing = \
                        next((r for r in self._bg_status.connect_routings if r.is_match(host_info, bg_role)), None)

            self._end_time_nano.set(perf_counter_ns())
            if conn is None:
                conn = connect_func()

            if is_initial_connection:
                self._init_status_provider()

            return conn
        finally:
            if self._start_time_nano.get() > 0:
                self._end_time_nano.compare_and_set(0, perf_counter_ns())

    def _reset_routing_time(self):
        self._start_time_nano.set(0)
        self._end_time_nano.set(0)

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

            result: ValueContainer[Any] = ValueContainer.empty()
            self._start_time_nano.set(perf_counter_ns())
            while routing is not None and result is None:
                result = routing.apply(
                    self,
                    self._plugin_service,
                    self._props,
                    target,
                    method_name,
                    execute_func,
                    *args,
                    **kwargs)
                if not result.is_present():
                    self._bg_status = self._plugin_service.get_status(BlueGreenStatus, self._bg_id)
                    routing = \
                        next((r for r in self._bg_status.execute_routings if r.is_match(host_info, bg_role)), None)

            self._end_time_nano.set(perf_counter_ns())
            if result.is_present():
                return result.get()

            return execute_func()
        finally:
            if self._start_time_nano.get() > 0:
                self._end_time_nano.compare_and_set(0, perf_counter_ns())


class BlueGreenPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return BlueGreenPlugin(plugin_service, props)


class BlueGreenInterimStatusProcessor(Protocol):
    def process_interim_status(self, role: BlueGreenRole, interim_status: BlueGreenInterimStatus):
        ...


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
        self._props = props
        self._status_check_intervals_ms = status_check_intervals_ms
        self._interim_status_processor = interim_status_processor

        self._rds_utils = RdsUtils()
        self._cv = Condition()
        self._should_collect_ip_addresses = Event()
        self._should_collect_ip_addresses.set()
        self._should_collect_topology = Event()
        self._should_collect_topology.set()
        self._use_ip_address = Event()
        self._panic_mode = Event()
        self._panic_mode.set()
        self._stop = Event()
        self._interval_rate = BlueGreenIntervalRate.BASELINE
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

        db_dialect = self._plugin_service.database_dialect
        if not isinstance(db_dialect, BlueGreenDialect):
            raise AwsWrapperError(Messages.get_formatted("BlueGreenStatusMonitor.UnexpectedDialect", db_dialect))

        self._bg_dialect: BlueGreenDialect = cast('BlueGreenDialect', self._plugin_service.database_dialect)

        self._open_connection_thread: Optional[Thread] = None
        self._monitor_thread = Thread(daemon=True, name="BlueGreenMonitorThread", target=self._run)
        self._monitor_thread.start()

    def _run(self):
        try:
            while not self._stop.is_set():
                try:
                    old_phase = self._current_phase
                    self._open_connection()
                    self._collect_status()
                    self._collect_topology()
                    self._collect_ip_addresses()
                    self._update_ip_address_flags()

                    if self._current_phase is not None and (old_phase is None or old_phase != self._current_phase):
                        logger.debug("BlueGreenStatusMonitor.StatusChanged", self._bg_role, self._current_phase)

                    if self._interim_status_processor is not None:
                        self._interim_status_processor.process_interim_status(
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

                    interval_rate = BlueGreenIntervalRate.HIGH if self._panic_mode.is_set() else self._interval_rate
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
            self._is_host_info_correct = False

        try:
            if self._use_ip_address.is_set() and ip_address is not None:
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
                self._connected_ip_address = self._get_ip_address(host_info.host)
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
                    version = record["version"]
                    if version not in BlueGreenStatusMonitor._KNOWN_VERSIONS:
                        self._version = BlueGreenStatusMonitor._LATEST_KNOWN_VERSION
                        logger.warning(
                            "BlueGreenStatusMonitor.UsesVersion", self._bg_role, version, self._version)

                    endpoint = record["endpoint"]
                    port = record["port"]
                    bg_role = BlueGreenRole.parse_role(record["role"], self._version)
                    phase = BlueGreenPhase.parse_phase(record["status"])

                    if self._bg_role != bg_role:
                        continue

                    status_entries.append(BlueGreenDbStatusInfo(version, endpoint, port, phase, bg_role))

            # Attempt to find the writer cluster status info
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

            if self._should_collect_topology.is_set():
                current_host_names = {status.endpoint.lower() for status in status_entries
                                      if status.endpoint is not None and
                                      self._rds_utils.is_not_old_instance(status.endpoint)}
                self._host_names.update(current_host_names)

            if not self._is_host_info_correct and status_info is not None:
                # We connected to an initial host info that might not be the desired blue or green cluster. Let's check
                # if we need to reconnect to the correct one.
                status_info_ip_address = self._get_ip_address(status_info.endpoint)
                connected_ip_address = self._connected_ip_address
                if connected_ip_address is not None and connected_ip_address != status_info_ip_address:
                    # We are not connected to the desired blue or green cluster, we need to reconnect.
                    self._connection_host_info = HostInfo(host=status_info.endpoint, port=status_info.port)
                    self._is_host_info_correct = True
                    self._close_connection()
                    self._panic_mode.set()
                else:
                    # We are already connected to the right node.
                    self._is_host_info_correct = True
                    self._panic_mode.clear()

            if self._is_host_info_correct and self._host_list_provider is not None:
                # A connection to the correct cluster (blue or green) has been stablished. Let's initialize the host
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
        if self._host_list_provider is not None or not self._is_host_info_correct:
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
        initial_interval_rate = self._interval_rate
        initial_panic_mode_val = self._panic_mode.is_set()
        min_delay_sec = min(delay_ms, 50) / 1_000

        while self._interval_rate == initial_interval_rate and \
                perf_counter_ns() < end_ns and \
                not self._stop.is_set() and \
                initial_panic_mode_val == self._panic_mode.is_set():
            with self._cv:
                self._cv.wait(min_delay_sec)

    def _collect_topology(self):
        if self._host_list_provider is None:
            return

        conn = self._connection
        if self._is_connection_closed(conn):
            return

        self._current_topology = self._host_list_provider.force_refresh(conn)
        if self._should_collect_topology:
            self._start_topology = self._current_topology

        current_topology_copy = self._current_topology
        if current_topology_copy is not None and self._should_collect_topology:
            self._host_names.update({host_info.host for host_info in current_topology_copy})

    def _collect_ip_addresses(self):
        self._current_ip_addresses_by_host.clear()
        if self._host_names is not None:
            for host in self._host_names:
                self._current_ip_addresses_by_host.put_if_absent(host, self._get_ip_address(host))

        if self._should_collect_ip_addresses:
            self._start_ip_addresses_by_host.clear()
            for k, v in self._current_ip_addresses_by_host.items():
                self._start_ip_addresses_by_host.put_if_absent(k, v)

    def _update_ip_address_flags(self):
        if self._should_collect_topology:
            self._all_start_topology_ip_changed = False
            self._all_start_topology_endpoints_removed = False
            self._all_topology_changed = False
            return

        if not self._should_collect_ip_addresses:
            # Check whether all hosts in start_topology resolve to new IP addresses
            # TODO: do we need to make the value type equivalent to Java.Optional?
            self._all_start_topology_ip_changed = bool(self._start_topology) and \
                all(
                    self._start_ip_addresses_by_host.get(node.host) is not None and
                    self._current_ip_addresses_by_host.get(node.host) is not None and
                    self._start_ip_addresses_by_host.get(node.host) != self._current_ip_addresses_by_host.get(node.host)
                    for node in self._start_topology)

        # Check whether all hosts in start_topology no longer have IP addresses. This indicates that the start_topology
        # hosts can no longer be resolved because their DNS entries no longer exist.
        self._all_start_topology_endpoints_removed = bool(self._start_topology) and \
            all(
                self._start_ip_addresses_by_host.get(node.host) is not None and
                self._current_ip_addresses_by_host.get(node.host) is None
                for node in self._start_topology
            )

        if not self._should_collect_topology:
            # Check whether all hosts in current_topology do not exist in start_topology
            start_topology_hosts = set() if self._start_topology is None else \
                {host_info.host for host_info in self._start_topology}
            current_topology_copy = self._current_topology
            self._all_topology_changed = (
                    current_topology_copy and
                    start_topology_hosts and
                    all(node.host not in start_topology_hosts for node in current_topology_copy))


@dataclass
class BlueGreenDbStatusInfo:
    version: str
    endpoint: str
    port: int
    phase: BlueGreenPhase
    bg_role: BlueGreenRole


class BlueGreenStatusProvider:
    def __init__(self, plugin_service: PluginService, props: Properties, bg_id: str):
        self._plugin_service = plugin_service
        self._props = props
        self._bg_id = bg_id
