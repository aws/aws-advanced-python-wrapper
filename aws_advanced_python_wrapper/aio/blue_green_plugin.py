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

"""Async Blue/Green deployment plugin.

Full async port of :mod:`aws_advanced_python_wrapper.blue_green_plugin`
(the sync module is the behavioural source of truth). The provider drives
two role-scoped monitors (blue / green), consumes their per-role interim
statuses, and builds the per-phase routing tables that the plugin applies
on ``connect`` / ``execute``.

Async-idiomatic divergences from sync (behaviour preserved):

- ``asyncio.Task`` per monitor instead of a ``threading.Thread``; the
  monitor opens its probe connection inline (awaiting yields the loop)
  rather than in a dedicated connection-opener thread.
- Routings poll ``plugin_service.get_status`` with ``asyncio.sleep``
  instead of waiting on a :class:`threading.Condition`; a published status
  is a new object, so identity change is the release signal.
- ``is_blue_green_status_available`` (a sync-cursor probe) is skipped; the
  monitor runs the status query directly and treats a failure as "status
  not available".
- ``is_plugin_in_use(IamAuthPlugin)`` is approximated from the ``plugins``
  property because the async plugin service exposes no plugin registry.

Long-lived monitor tasks register their teardown with
:func:`aws_advanced_python_wrapper.aio.cleanup.register_shutdown_hook`; no
threads/tasks are created at import time.
"""

from __future__ import annotations

import asyncio
import socket
from abc import ABC, abstractmethod
from copy import copy
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from threading import Lock
from time import perf_counter_ns
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, ClassVar, Dict,
                    List, Optional, Set, Tuple)

from aws_advanced_python_wrapper.aio.cleanup import register_shutdown_hook
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.database_dialect import BlueGreenDialect
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                UnsupportedOperationError)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.concurrent import (ConcurrentDict,
                                                          ConcurrentSet)
from aws_advanced_python_wrapper.utils.events import MonitorResetEvent
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils
from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
    TelemetryTraceLevel
from aws_advanced_python_wrapper.utils.value_container import ValueContainer

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService

logger = Logger(__name__)

# Plugin codes (see aio/plugin_factory.py) whose presence implies IAM token
# authentication is active, gating the SubstituteConnectRouting IAM-host
# rerouting. Approximation of sync's plugin_service.is_plugin_in_use.
_IAM_PLUGIN_CODES: frozenset = frozenset({"iam"})


# ---- Enums -----------------------------------------------------------


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

    def __new__(cls, value: int, is_switchover_active_or_completed: bool) -> BlueGreenPhase:
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


# ---- Status containers -----------------------------------------------


class BlueGreenStatus:
    def __init__(
            self,
            bg_id: str,
            phase: BlueGreenPhase,
            connect_routings: Optional[List[ConnectRouting]] = None,
            execute_routings: Optional[List[ExecuteRouting]] = None,
            role_by_host: Optional[ConcurrentDict[str, BlueGreenRole]] = None,
            corresponding_hosts: Optional[ConcurrentDict[str, Tuple[HostInfo, Optional[HostInfo]]]] = None) -> None:
        self.bg_id = bg_id
        self.phase = phase
        self.connect_routings: List[ConnectRouting] = [] if connect_routings is None else list(connect_routings)
        self.execute_routings: List[ExecuteRouting] = [] if execute_routings is None else list(execute_routings)
        self.roles_by_endpoint: ConcurrentDict[str, BlueGreenRole] = ConcurrentDict()
        if role_by_host is not None:
            self.roles_by_endpoint.put_all(role_by_host)

        self.corresponding_hosts: ConcurrentDict[str, Tuple[HostInfo, Optional[HostInfo]]] = ConcurrentDict()
        if corresponding_hosts is not None:
            self.corresponding_hosts.put_all(corresponding_hosts)

    def get_role(self, host_info: Optional[HostInfo]) -> Optional[BlueGreenRole]:
        if host_info is None:
            return None
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

    def get_custom_hashcode(self) -> int:
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


# ---- Routing ABCs + concrete implementations -------------------------


class ConnectRouting(ABC):
    @abstractmethod
    def is_match(self, host_info: Optional[HostInfo], role: BlueGreenRole) -> bool:
        ...

    @abstractmethod
    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Optional[Any]:
        ...


class ExecuteRouting(ABC):
    @abstractmethod
    def is_match(self, host_info: Optional[HostInfo], role: BlueGreenRole) -> bool:
        ...

    @abstractmethod
    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            props: Properties,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]]) -> ValueContainer[Any]:
        ...


class BaseRouting:
    _MIN_SLEEP_MS: ClassVar[int] = 50

    def __init__(self, endpoint: Optional[str], bg_role: Optional[BlueGreenRole]) -> None:
        self._endpoint = endpoint  # host and optionally port as well
        self._bg_role = bg_role

    async def delay(
            self,
            delay_ms: int,
            bg_status: Optional[BlueGreenStatus],
            plugin_service: AsyncPluginService,
            bg_id: str) -> None:
        loop = asyncio.get_event_loop()
        end_time_sec = loop.time() + (delay_ms / 1_000)
        min_delay_sec = min(delay_ms, BaseRouting._MIN_SLEEP_MS) / 1_000

        if bg_status is None:
            await asyncio.sleep(delay_ms / 1_000)
            return

        while bg_status is plugin_service.get_status(BlueGreenStatus, bg_id) and loop.time() <= end_time_sec:
            await asyncio.sleep(min_delay_sec)

    def is_match(self, host_info: Optional[HostInfo], bg_role: BlueGreenRole) -> bool:
        if self._endpoint is None:
            return self._bg_role is None or self._bg_role == bg_role

        if host_info is None:
            return False

        return self._endpoint == host_info.url.lower() and (self._bg_role is None or self._bg_role == bg_role)

    def __str__(self) -> str:
        endpoint_str = "None" if self._endpoint is None else f"'{self._endpoint}'"
        return f"{self.__class__.__name__}(endpoint={endpoint_str}, bg_role={self._bg_role})"


class PassThroughConnectRouting(BaseRouting, ConnectRouting):
    def __init__(self, endpoint: Optional[str] = None, bg_role: Optional[BlueGreenRole] = None) -> None:
        super().__init__(endpoint, bg_role)

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Optional[Any]:
        return await connect_func()


class RejectConnectRouting(BaseRouting, ConnectRouting):
    def __init__(self, endpoint: Optional[str] = None, bg_role: Optional[BlueGreenRole] = None) -> None:
        super().__init__(endpoint, bg_role)

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Optional[Any]:
        raise AwsWrapperError(Messages.get("RejectConnectRouting.InProgressCantConnect"))


class SubstituteConnectRouting(BaseRouting, ConnectRouting):
    _rds_utils: ClassVar[RdsUtils] = RdsUtils()

    def __init__(
            self,
            substitute_host_info: HostInfo,
            endpoint: Optional[str] = None,
            bg_role: Optional[BlueGreenRole] = None,
            iam_hosts: Optional[Tuple[HostInfo, ...]] = None,
            iam_auth_success_handler: Optional[Callable[[str], None]] = None) -> None:
        super().__init__(endpoint, bg_role)
        self._substitute_host_info = substitute_host_info
        self._iam_hosts = iam_hosts
        self._iam_auth_success_handler = iam_auth_success_handler

    def __str__(self) -> str:
        iam_hosts_str = ',\n        '.join(str(iam_host) for iam_host in (self._iam_hosts or ()))
        return (f"{self.__class__.__name__}(\n"
                f"    substitute_host_info={self._substitute_host_info},\n"
                f"    endpoint={self._endpoint},\n"
                f"    bg_role={self._bg_role},\n"
                f"    iam_hosts=[\n"
                f"        {iam_hosts_str}\n"
                f"    ]\n"
                f")")

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Optional[Any]:
        plugin_service = plugin.plugin_service
        if not SubstituteConnectRouting._rds_utils.is_ip(self._substitute_host_info.host):
            return await plugin_service.connect(self._substitute_host_info, props, plugin_to_skip=plugin)

        if not _is_iam_in_use(props):
            return await plugin_service.connect(self._substitute_host_info, props, plugin_to_skip=plugin)

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
                conn = await plugin_service.connect(rerouted_host_info, rerouted_props)
                if self._iam_auth_success_handler is not None:
                    try:
                        self._iam_auth_success_handler(iam_host.host)
                    except Exception:  # noqa: BLE001 - handler is best-effort bookkeeping
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
    _SLEEP_TIME_MS: ClassVar[int] = 100

    def __init__(self, endpoint: Optional[str], bg_role: Optional[BlueGreenRole], bg_id: str) -> None:
        super().__init__(endpoint, bg_role)
        self._bg_id = bg_id

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Optional[Any]:
        logger.debug("SuspendConnectRouting.InProgressSuspendConnect")
        plugin_service = plugin.plugin_service

        telemetry_factory = plugin_service.get_telemetry_factory()
        telemetry_context = telemetry_factory.open_telemetry_context(
            SuspendConnectRouting._TELEMETRY_SWITCHOVER, TelemetryTraceLevel.NESTED)

        bg_status = plugin_service.get_status(BlueGreenStatus, self._bg_id)
        timeout_ms = WrapperProperties.BG_CONNECT_TIMEOUT_MS.get_int(props)
        loop = asyncio.get_event_loop()
        start_time_sec = loop.time()
        end_time_sec = start_time_sec + timeout_ms / 1_000

        try:
            while loop.time() <= end_time_sec and \
                    bg_status is not None and \
                    bg_status.phase == BlueGreenPhase.IN_PROGRESS:
                await self.delay(SuspendConnectRouting._SLEEP_TIME_MS, bg_status, plugin_service, self._bg_id)
                bg_status = plugin_service.get_status(BlueGreenStatus, self._bg_id)

            if bg_status is not None and bg_status.phase == BlueGreenPhase.IN_PROGRESS:
                raise TimeoutError(
                    Messages.get_formatted("SuspendConnectRouting.InProgressTryConnectLater", timeout_ms))

            logger.debug(
                Messages.get_formatted(
                    "SuspendConnectRouting.SwitchoverCompleteContinueWithConnect",
                    (loop.time() - start_time_sec) * 1000))
        finally:
            if telemetry_context is not None:
                telemetry_context.close_context()

        # return None so that the next routing can attempt a connection
        return None


class SuspendUntilCorrespondingHostFoundConnectRouting(BaseRouting, ConnectRouting):
    _TELEMETRY_SWITCHOVER: ClassVar[str] = "Blue/Green switchover"
    _SLEEP_TIME_MS: ClassVar[int] = 100

    def __init__(self, endpoint: Optional[str], bg_role: Optional[BlueGreenRole], bg_id: str) -> None:
        super().__init__(endpoint, bg_role)
        self._bg_id = bg_id

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Optional[Any]:
        # NOTE: sync references message key
        # "SuspendConnectRouting.WaitConnectUntilCorrespondingHostFound", which is
        # absent from the messages bundle; using it with a format arg raises. Log a
        # plain string (falls back cleanly) until the key is added upstream.
        logger.debug(
            f"[SuspendUntilCorrespondingHostFoundConnectRouting] Waiting until the corresponding host "
            f"for '{host_info.host}' is found.")
        plugin_service = plugin.plugin_service

        telemetry_factory = plugin_service.get_telemetry_factory()
        telemetry_context = telemetry_factory.open_telemetry_context(
            SuspendUntilCorrespondingHostFoundConnectRouting._TELEMETRY_SWITCHOVER, TelemetryTraceLevel.NESTED)

        bg_status = plugin_service.get_status(BlueGreenStatus, self._bg_id)
        corresponding_pair = None if bg_status is None else bg_status.corresponding_hosts.get(host_info.host)

        timeout_ms = WrapperProperties.BG_CONNECT_TIMEOUT_MS.get_int(props)
        loop = asyncio.get_event_loop()
        start_time_sec = loop.time()
        end_time_sec = start_time_sec + timeout_ms / 1_000

        try:
            while loop.time() <= end_time_sec and \
                    bg_status is not None and \
                    bg_status.phase != BlueGreenPhase.COMPLETED and \
                    (corresponding_pair is None or corresponding_pair[1] is None):
                # wait until the corresponding host is found, or until switchover is completed
                await self.delay(
                    SuspendUntilCorrespondingHostFoundConnectRouting._SLEEP_TIME_MS, bg_status, plugin_service, self._bg_id)
                bg_status = plugin_service.get_status(BlueGreenStatus, self._bg_id)
                corresponding_pair = None if bg_status is None else bg_status.corresponding_hosts.get(host_info.host)

            if bg_status is None or bg_status.phase == BlueGreenPhase.COMPLETED:
                logger.debug(
                    "SuspendUntilCorrespondingHostFoundConnectRouting.CompletedContinueWithConnect",
                    (loop.time() - start_time_sec) * 1000)
                return None

            if loop.time() > end_time_sec:
                raise TimeoutError(
                    Messages.get_formatted(
                        "SuspendUntilCorrespondingHostFoundConnectRouting.CorrespondingHostNotFoundTryConnectLater",
                        host_info.host,
                        (loop.time() - start_time_sec) * 1000))

            logger.debug(
                Messages.get_formatted(
                    "SuspendUntilCorrespondingHostFoundConnectRouting.CorrespondingHostFoundContinueWithConnect",
                    host_info.host,
                    (loop.time() - start_time_sec) * 1000))
        finally:
            if telemetry_context is not None:
                telemetry_context.close_context()

        # return None so that the next routing can attempt a connection
        return None


class PassThroughExecuteRouting(BaseRouting, ExecuteRouting):
    def __init__(self, endpoint: Optional[str] = None, bg_role: Optional[BlueGreenRole] = None) -> None:
        super().__init__(endpoint, bg_role)

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            props: Properties,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]]) -> ValueContainer[Any]:
        return ValueContainer.of(await execute_func())


class SuspendExecuteRouting(BaseRouting, ExecuteRouting):
    _TELEMETRY_SWITCHOVER: ClassVar[str] = "Blue/Green switchover"
    _SLEEP_TIME_MS: ClassVar[int] = 100

    def __init__(self, endpoint: Optional[str], bg_role: Optional[BlueGreenRole], bg_id: str) -> None:
        super().__init__(endpoint, bg_role)
        self._bg_id = bg_id

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            props: Properties,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]]) -> ValueContainer[Any]:
        logger.debug("SuspendExecuteRouting.InProgressSuspendMethod", method_name)
        plugin_service = plugin.plugin_service

        telemetry_factory = plugin_service.get_telemetry_factory()
        telemetry_context = telemetry_factory.open_telemetry_context(
            SuspendExecuteRouting._TELEMETRY_SWITCHOVER, TelemetryTraceLevel.NESTED)

        bg_status = plugin_service.get_status(BlueGreenStatus, self._bg_id)
        timeout_ms = WrapperProperties.BG_CONNECT_TIMEOUT_MS.get_int(props)
        loop = asyncio.get_event_loop()
        start_time_sec = loop.time()
        end_time_sec = start_time_sec + timeout_ms / 1_000

        try:
            while loop.time() <= end_time_sec and \
                    bg_status is not None and \
                    bg_status.phase == BlueGreenPhase.IN_PROGRESS:
                await self.delay(SuspendExecuteRouting._SLEEP_TIME_MS, bg_status, plugin_service, self._bg_id)
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
                    (loop.time() - start_time_sec) * 1000))
        finally:
            if telemetry_context is not None:
                telemetry_context.close_context()

        # return empty so that the next routing can attempt the method
        return ValueContainer.empty()


def _is_iam_in_use(props: Properties) -> bool:
    plugins = WrapperProperties.PLUGINS.get(props)
    if not plugins:
        return False
    codes = {code.strip() for code in str(plugins).split(",")}
    return bool(codes & _IAM_PLUGIN_CODES)


# ---- Plugin ----------------------------------------------------------


class AsyncBlueGreenPlugin(AsyncPlugin):
    _SUBSCRIBED_METHODS: ClassVar[Set[str]] = {DbApiMethod.CONNECT.method_name}
    _CLOSE_METHODS: ClassVar[Set[str]] = {DbApiMethod.CONNECTION_CLOSE.method_name, DbApiMethod.CURSOR_CLOSE.method_name}
    _status_providers: ClassVar[ConcurrentDict[str, AsyncBlueGreenStatusProvider]] = ConcurrentDict()
    _providers_lock: ClassVar[Lock] = Lock()

    def __init__(self, plugin_service: AsyncPluginService, props: Properties) -> None:
        self._plugin_service = plugin_service
        self._props = props
        bg_id = WrapperProperties.BG_ID.get(props)
        self._bg_id = bg_id.strip().lower() if bg_id is not None else "1"
        self._rds_utils = RdsUtils()
        self._bg_status: Optional[BlueGreenStatus] = None
        self._start_time_ns = 0
        self._end_time_ns = 0

        self._subscribed_methods: Set[str] = set(AsyncBlueGreenPlugin._SUBSCRIBED_METHODS)
        self._subscribed_methods.update(self._plugin_service.network_bound_methods)

    @property
    def plugin_service(self) -> AsyncPluginService:
        return self._plugin_service

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._subscribed_methods

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        self._reset_routing_time()
        try:
            self._bg_status = self._plugin_service.get_status(BlueGreenStatus, self._bg_id)
            if self._bg_status is None:
                return await self._open_direct_connection(connect_func, host_info, is_initial_connection)

            bg_role = self._bg_status.get_role(host_info)
            if bg_role is None:
                # The host is not participating in BG switchover - connect directly
                return await self._open_direct_connection(connect_func, host_info, is_initial_connection)

            routing = next((r for r in self._bg_status.connect_routings if r.is_match(host_info, bg_role)), None)
            if not routing:
                return await self._open_direct_connection(connect_func, host_info, is_initial_connection)

            self._start_time_ns = perf_counter_ns()
            conn: Optional[Any] = None
            while routing is not None and conn is None:
                conn = await routing.apply(self, host_info, props, is_initial_connection, connect_func)
                if conn is not None:
                    break

                # Re-select against the LATEST published status: a suspend/wait
                # routing returns None once switchover moves past its phase, so
                # the current routing table (not the stale captured one) decides
                # what happens next. Falls through to connect_func when the
                # latest status no longer matches (avoids a stale-status busy
                # loop that the literal sync re-selection is prone to).
                latest_status = self._plugin_service.get_status(BlueGreenStatus, self._bg_id)
                if latest_status is None:
                    self._end_time_ns = perf_counter_ns()
                    return await self._open_direct_connection(connect_func, host_info, is_initial_connection)

                self._bg_status = latest_status
                bg_role = latest_status.get_role(host_info)
                if bg_role is None:
                    break
                routing = next((r for r in latest_status.connect_routings if r.is_match(host_info, bg_role)), None)

            self._end_time_ns = perf_counter_ns()
            if conn is None:
                conn = await connect_func()

            if is_initial_connection:
                self._init_status_provider(host_info)

            return conn
        finally:
            if self._start_time_ns > 0 and self._end_time_ns == 0:
                self._end_time_ns = perf_counter_ns()

    def _reset_routing_time(self) -> None:
        self._start_time_ns = 0
        self._end_time_ns = 0

    async def _open_direct_connection(
            self,
            connect_func: Callable[..., Awaitable[Any]],
            host_info: HostInfo,
            is_initial_connection: bool) -> Any:
        conn = await connect_func()
        if is_initial_connection:
            self._init_status_provider(host_info)

        return conn

    def _init_status_provider(self, initial_host_info: Optional[HostInfo]) -> None:
        provider = AsyncBlueGreenPlugin._status_providers.compute_if_absent(
            self._bg_id,
            lambda key: AsyncBlueGreenStatusProvider(self._plugin_service, self._props, self._bg_id, initial_host_info))
        if provider is not None:
            provider.schedule_start()

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        self._reset_routing_time()
        try:
            self._init_status_provider(self._plugin_service.current_host_info)
            if method_name in AsyncBlueGreenPlugin._CLOSE_METHODS:
                return await execute_func()

            self._bg_status = self._plugin_service.get_status(BlueGreenStatus, self._bg_id)
            if self._bg_status is None:
                return await execute_func()

            host_info = self._plugin_service.current_host_info
            bg_role = None if host_info is None else self._bg_status.get_role(host_info)
            if bg_role is None:
                # The host is not participating in BG switchover - execute directly
                return await execute_func()

            routing = next((r for r in self._bg_status.execute_routings if r.is_match(host_info, bg_role)), None)
            if routing is None:
                return await execute_func()

            result_container: ValueContainer[Any] = ValueContainer.empty()
            self._start_time_ns = perf_counter_ns()
            while routing is not None and not result_container.is_present():
                result_container = await routing.apply(self, self._props, method_name, execute_func)
                if result_container.is_present():
                    break

                latest_status = self._plugin_service.get_status(BlueGreenStatus, self._bg_id)
                if latest_status is None:
                    self._end_time_ns = perf_counter_ns()
                    return await execute_func()

                self._bg_status = latest_status
                bg_role = None if host_info is None else latest_status.get_role(host_info)
                if bg_role is None:
                    break
                routing = next((r for r in latest_status.execute_routings if r.is_match(host_info, bg_role)), None)

            self._end_time_ns = perf_counter_ns()
            if result_container.is_present():
                return result_container.get()

            return await execute_func()
        finally:
            if self._start_time_ns > 0 and self._end_time_ns == 0:
                self._end_time_ns = perf_counter_ns()

    # For testing purposes only.
    def get_hold_time_ns(self) -> int:
        if self._start_time_ns == 0:
            return 0

        if self._end_time_ns == 0:
            return perf_counter_ns() - self._start_time_ns
        else:
            return self._end_time_ns - self._start_time_ns

    @classmethod
    def _reset_for_tests(cls) -> None:
        with cls._providers_lock:
            cls._status_providers.clear()


# ---- Monitor ---------------------------------------------------------


BlueGreenInterimStatusProcessor = Callable[[BlueGreenRole, BlueGreenInterimStatus], None]


@dataclass
class BlueGreenDbStatusInfo:
    version: str
    endpoint: str
    port: int
    phase: BlueGreenPhase
    bg_role: BlueGreenRole


class AsyncBlueGreenStatusMonitor:
    _DEFAULT_STATUS_CHECK_INTERVAL_MS: ClassVar[int] = 5 * 60_000  # 5 minutes
    _BG_CLUSTER_ID: ClassVar[str] = "941d00a8-8238-4f7d-bf59-771bff783a8e"
    _LATEST_KNOWN_VERSION: ClassVar[str] = "1.0"
    _KNOWN_VERSIONS: ClassVar[frozenset] = frozenset({_LATEST_KNOWN_VERSION})

    def __init__(
            self,
            bg_role: BlueGreenRole,
            bg_id: str,
            initial_host_info: Optional[HostInfo],
            plugin_service: AsyncPluginService,
            props: Properties,
            status_check_intervals_ms: Dict[BlueGreenIntervalRate, int],
            interim_status_processor: Optional[BlueGreenInterimStatusProcessor] = None) -> None:
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
        self.should_collect_ip_addresses = True
        self.should_collect_topology = True
        self.use_ip_address = False
        self._panic_mode = True
        self.stop = False
        self.interval_rate = BlueGreenIntervalRate.BASELINE
        self._host_list_provider: Optional[Any] = None
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
        self._connection: Optional[Any] = None
        self._connection_host_info: Optional[HostInfo] = None
        self._connected_ip_address: Optional[str] = None
        self._is_host_info_correct = False
        self._has_started = False

        db_dialect = self._plugin_service.database_dialect
        if not isinstance(db_dialect, BlueGreenDialect):
            raise AwsWrapperError(Messages.get_formatted("BlueGreenStatusMonitor.UnexpectedDialect", db_dialect))

        self._bg_dialect: BlueGreenDialect = db_dialect
        self._task: Optional[asyncio.Task[None]] = None

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        if not self._has_started:
            self._has_started = True
            self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        try:
            while not self.stop:
                try:
                    old_phase = self._current_phase
                    await self._open_connection()
                    await self._collect_status()
                    await self.collect_topology()
                    await self._collect_ip_addresses()
                    self._update_ip_address_flags()

                    if self._current_phase is not None and (old_phase is None or old_phase != self._current_phase):
                        logger.debug("BlueGreenStatusMonitor.StatusChanged", self._bg_role, self._current_phase)

                    if self._interim_status_processor is not None:
                        self._interim_status_processor(
                            self._bg_role,
                            self._build_interim_status())

                    interval_rate = BlueGreenIntervalRate.HIGH if self._panic_mode else self.interval_rate
                    delay_ms = self._status_check_intervals_ms.get(
                        interval_rate, AsyncBlueGreenStatusMonitor._DEFAULT_STATUS_CHECK_INTERVAL_MS)
                    await self._delay(delay_ms)
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001 - monitor resilience
                    logger.warning("BlueGreenStatusMonitor.MonitoringUnhandledException", self._bg_role, e)
        except asyncio.CancelledError:
            return
        finally:
            await self._close_connection()
            if self._host_list_provider is not None:
                await self._stop_host_list_provider()
                self._host_list_provider = None
            logger.debug("BlueGreenStatusMonitor.ThreadCompleted", self._bg_role)

    def _build_interim_status(self) -> BlueGreenInterimStatus:
        return BlueGreenInterimStatus(
            self._current_phase if self._current_phase is not None else BlueGreenPhase.NOT_CREATED,
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

    async def _open_connection(self) -> None:
        conn = self._connection
        if not await self._is_connection_closed(conn):
            return

        self._connection = None
        self._panic_mode = True
        await self._open_connection_task()

    async def _open_connection_task(self) -> None:
        host_info = self._connection_host_info
        ip_address = self._connected_ip_address
        if host_info is None:
            self._connection_host_info = self._initial_host_info
            host_info = self._initial_host_info
            self._connected_ip_address = None
            ip_address = None
            self._is_host_info_correct = False

        if host_info is None:
            return

        try:
            if self.use_ip_address and ip_address is not None:
                ip_host_info = copy(host_info)
                ip_host_info.host = ip_address
                props_copy = copy(self._props)
                WrapperProperties.IAM_HOST.set(props_copy, ip_host_info.host)

                logger.debug("BlueGreenStatusMonitor.OpeningConnectionWithIp", self._bg_role, ip_host_info.host)
                self._connection = await self._plugin_service.force_connect(ip_host_info, props_copy)
                logger.debug("BlueGreenStatusMonitor.OpenedConnectionWithIp", self._bg_role, ip_host_info.host)
            else:
                logger.debug("BlueGreenStatusMonitor.OpeningConnection", self._bg_role, host_info.host)
                self._connection = await self._plugin_service.force_connect(host_info, self._props)
                self._connected_ip_address = (await self._get_ip_address(host_info.host)).or_else(None)
                logger.debug("BlueGreenStatusMonitor.OpenedConnection", self._bg_role, host_info.host)

            self._panic_mode = False
        except Exception:  # noqa: BLE001 - attempt to open connection failed
            self._connection = None
            self._panic_mode = True

    async def _get_ip_address(self, host: str) -> ValueContainer[str]:
        try:
            loop = asyncio.get_event_loop()
            ip = await loop.run_in_executor(None, socket.gethostbyname, host)
            return ValueContainer.of(ip)
        except (socket.gaierror, OSError):
            return ValueContainer.empty()

    async def _collect_status(self) -> None:
        conn = self._connection
        try:
            if await self._is_connection_closed(conn):
                return

            query = getattr(self._bg_dialect, "blue_green_status_query", None)
            if not query:
                self._current_phase = BlueGreenPhase.NOT_CREATED
                return

            status_entries: List[BlueGreenDbStatusInfo] = []
            rows = await self._fetch_status_rows(conn, query)
            for record in rows:
                # columns: version, endpoint, port, role, status
                if record is None or len(record) < 5:
                    continue
                version = record[0]
                if version not in AsyncBlueGreenStatusMonitor._KNOWN_VERSIONS:
                    self._version = AsyncBlueGreenStatusMonitor._LATEST_KNOWN_VERSION
                    logger.warning("BlueGreenStatusMonitor.UsesVersion", self._bg_role, version, self._version)

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
                    # The status table may have no entries after BGD is completed.
                    if self._bg_role != BlueGreenRole.SOURCE:
                        logger.warning("BlueGreenStatusMonitor.NoEntriesInStatusTable", self._bg_role)

                    self._current_phase = None
            else:
                self._current_phase = status_info.phase
                self._version = status_info.version
                self._port = status_info.port

            if self.should_collect_topology:
                current_host_names = {status.endpoint.lower() for status in status_entries
                                      if status.endpoint is not None and
                                      self._rds_utils.is_not_old_instance(status.endpoint)}
                self._host_names.update(current_host_names)

            if not self._is_host_info_correct and status_info is not None:
                await self._reconnect_to_correct_host_if_needed(status_info)

            if self._is_host_info_correct and self._host_list_provider is None:
                self._init_host_list_provider()
        except Exception as e:  # noqa: BLE001
            if not await self._is_connection_closed(self._connection):
                logger.debug("BlueGreenStatusMonitor.UnhandledException", self._bg_role, e)
            await self._close_connection()
            self._panic_mode = True

    async def _reconnect_to_correct_host_if_needed(self, status_info: BlueGreenDbStatusInfo) -> None:
        status_info_ip_address = (await self._get_ip_address(status_info.endpoint)).or_else(None)
        connected_ip_address = self._connected_ip_address
        if connected_ip_address is not None and connected_ip_address != status_info_ip_address:
            # We are not connected to the desired blue or green cluster, we need to reconnect.
            self._connection_host_info = HostInfo(host=status_info.endpoint, port=status_info.port)
            self._props["host"] = status_info.endpoint
            self._is_host_info_correct = True
            await self._close_connection()
            self._panic_mode = True
        else:
            # We are already connected to the right host.
            self._is_host_info_correct = True
            self._panic_mode = False

    async def _fetch_status_rows(self, conn: Any, query: str) -> List[tuple]:
        cursor = conn.cursor()
        async with cursor as cur:
            await cur.execute(query)
            return list(await cur.fetchall())

    async def _close_connection(self) -> None:
        conn = self._connection
        self._connection = None
        if conn is not None and not await self._plugin_service.driver_dialect.is_closed(conn):
            try:
                await self._plugin_service.driver_dialect.abort_connection(conn)
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass

    def _init_host_list_provider(self) -> None:
        if self._host_list_provider is not None or not self._is_host_info_correct:
            return

        # A separate HostListProvider with a special unique cluster ID avoids interference with other
        # HostListProviders opened for this cluster. Blue and Green clusters should have different cluster IDs.
        props_copy = copy(self._props)
        cluster_id = f"{self._bg_id}::{self._bg_role}::{AsyncBlueGreenStatusMonitor._BG_CLUSTER_ID}"
        WrapperProperties.CLUSTER_ID.set(props_copy, cluster_id)
        logger.debug("BlueGreenStatusMonitor.CreateHostListProvider", self._bg_role, cluster_id)

        host_info = self._connection_host_info
        if host_info is None:
            logger.warning("BlueGreenStatusMonitor.HostInfoNone")
            return

        # Divergence from sync: the shared DatabaseDialect only exposes a *sync*
        # host-list-provider supplier, which would build a blocking provider that
        # can't be awaited on the event loop. A dedicated async blue/green
        # host-list provider is deferred; topology-based corresponding-host
        # mapping therefore relies on host_names collected from the status query
        # (cluster-DNS mapping), not on a per-monitor topology refresh.
        async_supplier = getattr(
            self._plugin_service.database_dialect, "get_async_host_list_provider_supplier", None)
        if async_supplier is None:
            return
        self._host_list_provider = async_supplier(self._plugin_service, props_copy)

    async def _stop_host_list_provider(self) -> None:
        stop = getattr(self._host_list_provider, "stop", None)
        if stop is None:
            return
        try:
            result = stop()
            if asyncio.iscoroutine(result):
                await result
        except Exception:  # noqa: BLE001 - best-effort teardown
            pass

    async def _is_connection_closed(self, conn: Optional[Any]) -> bool:
        if conn is None:
            return True
        return await self._plugin_service.driver_dialect.is_closed(conn)

    async def _delay(self, delay_ms: int) -> None:
        loop = asyncio.get_event_loop()
        end_ns = loop.time() + delay_ms / 1_000
        initial_interval_rate = self.interval_rate
        initial_panic_mode_val = self._panic_mode
        min_delay_sec = min(delay_ms, 50) / 1_000

        while self.interval_rate == initial_interval_rate and \
                loop.time() < end_ns and \
                not self.stop and \
                initial_panic_mode_val == self._panic_mode:
            await asyncio.sleep(min_delay_sec)

    async def collect_topology(self) -> None:
        if self._host_list_provider is None:
            return

        conn = self._connection
        if await self._is_connection_closed(conn):
            return

        self._current_topology = tuple(await self._host_list_provider.force_refresh(conn))
        if self.should_collect_topology:
            self._start_topology = self._current_topology

        current_topology_copy = self._current_topology
        if current_topology_copy is not None and self.should_collect_topology:
            self._host_names.update({host_info.host for host_info in current_topology_copy})

    async def _collect_ip_addresses(self) -> None:
        self._current_ip_addresses_by_host.clear()
        if self._host_names is not None:
            for host in self._host_names:
                self._current_ip_addresses_by_host.put_if_absent(host, await self._get_ip_address(host))

        if self.should_collect_ip_addresses:
            self._start_ip_addresses_by_host.clear()
            self._start_ip_addresses_by_host.put_all(self._current_ip_addresses_by_host)

    def _update_ip_address_flags(self) -> None:
        if self.should_collect_topology:
            self._all_start_topology_ip_changed = False
            self._all_start_topology_endpoints_removed = False
            self._all_topology_changed = False
            return

        if not self.should_collect_ip_addresses:
            # Check whether all hosts in start_topology resolve to new IP addresses
            self._all_start_topology_ip_changed = self._has_all_start_topology_ip_changed()

        # Check whether all hosts in start_topology no longer have IP addresses.
        self._all_start_topology_endpoints_removed = self._are_all_start_endpoints_removed()

        if not self.should_collect_topology:
            # Check whether all hosts in current_topology do not exist in start_topology
            start_topology_hosts = set() if self._start_topology is None else \
                {host_info.host for host_info in self._start_topology}
            current_topology_copy = self._current_topology
            self._all_topology_changed = bool(
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

    def reset_collected_data(self) -> None:
        self._start_ip_addresses_by_host.clear()
        self._start_topology = ()
        self._host_names.clear()

    async def stop_monitor(self) -> None:
        self.stop = True
        task = self._task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._task = None
        await self._close_connection()


# ---- Status provider -------------------------------------------------


class AsyncBlueGreenStatusProvider:
    _MONITORING_PROPERTY_PREFIX: ClassVar[str] = "blue-green-monitoring-"
    _DEFAULT_CONNECT_TIMEOUT_MS: ClassVar[int] = 10_000
    _DEFAULT_SOCKET_TIMEOUT_MS: ClassVar[int] = 10_000

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties,
            bg_id: str,
            initial_host_info: Optional[HostInfo] = None) -> None:
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
        self._monitor_reset_on_in_progress_completed = False
        self._monitor_reset_on_topology_completed = False
        self._post_status_end_time_ns = 0
        self._status_check_intervals_ms: Dict[BlueGreenIntervalRate, int] = {}
        self._phase_times_ns: ConcurrentDict[str, PhaseTimeInfo] = ConcurrentDict()
        self._rds_utils = RdsUtils()
        self._started = False
        self._start_lock = Lock()

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

        current_host_info = initial_host_info if initial_host_info is not None else self._plugin_service.current_host_info
        blue_monitor = AsyncBlueGreenStatusMonitor(
            BlueGreenRole.SOURCE,
            self._bg_id,
            current_host_info,
            self._plugin_service,
            self._get_monitoring_props(),
            self._status_check_intervals_ms,
            self._process_interim_status)
        green_monitor = AsyncBlueGreenStatusMonitor(
            BlueGreenRole.TARGET,
            self._bg_id,
            current_host_info,
            self._plugin_service,
            self._get_monitoring_props(),
            self._status_check_intervals_ms,
            self._process_interim_status)

        self._monitors: List[AsyncBlueGreenStatusMonitor] = [blue_monitor, green_monitor]

    def schedule_start(self) -> None:
        """Start both monitors and register their teardown. Idempotent.

        Called from the plugin's async ``connect`` / ``execute``, so a running
        event loop is available for ``create_task``.
        """
        with self._start_lock:
            if self._started:
                return
            self._started = True

        for monitor in self._monitors:
            monitor.start()
        register_shutdown_hook(self.stop)

    async def stop(self) -> None:
        for monitor in self._monitors:
            await monitor.stop_monitor()

    def _get_monitoring_props(self) -> Properties:
        monitoring_props = copy(self._props)
        for key in list(self._props.keys()):
            if key.startswith(AsyncBlueGreenStatusProvider._MONITORING_PROPERTY_PREFIX):
                new_key = key[len(AsyncBlueGreenStatusProvider._MONITORING_PROPERTY_PREFIX):]
                monitoring_props[new_key] = self._props[key]
                monitoring_props.pop(key, None)

        monitoring_props.put_if_absent(
            WrapperProperties.CONNECT_TIMEOUT_SEC.name, AsyncBlueGreenStatusProvider._DEFAULT_CONNECT_TIMEOUT_MS // 1_000)
        monitoring_props.put_if_absent(
            WrapperProperties.SOCKET_TIMEOUT_SEC.name, AsyncBlueGreenStatusProvider._DEFAULT_SOCKET_TIMEOUT_MS // 1_000)
        return monitoring_props

    def _process_interim_status(self, bg_role: BlueGreenRole, interim_status: BlueGreenInterimStatus) -> None:
        # No await points below, so this runs atomically w.r.t. the event loop
        # even though the two monitors are separate tasks (sync uses an RLock).
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
        self._reset_context_when_completed()

    def _get_context_hash(self) -> int:
        result = self._get_value_hash(1, str(self._all_green_hosts_changed_name))
        result = self._get_value_hash(result, str(len(self._iam_auth_success_hosts)))
        return result

    def _get_value_hash(self, current_hash: int, val: str) -> int:
        return current_hash * 31 + hash(val)

    def _update_phase(self, bg_role: BlueGreenRole, interim_status: BlueGreenInterimStatus) -> None:
        role_status = self._interim_statuses[bg_role.value]
        latest_phase = BlueGreenPhase.NOT_CREATED if role_status is None else role_status.phase
        if latest_phase is not None and \
                interim_status.phase is not None and \
                interim_status.phase.phase_value < latest_phase.phase_value:
            self._rollback = True
            logger.debug("BlueGreenStatusProvider.Rollback", self._bg_id)

        if interim_status.phase is None:
            return

        # The phase should not move backwards unless we're rolling back.
        if self._rollback:
            if interim_status.phase.phase_value < self._latest_phase.phase_value:
                self._latest_phase = interim_status.phase
        else:
            if interim_status.phase.phase_value >= self._latest_phase.phase_value:
                self._latest_phase = interim_status.phase

    def _update_corresponding_hosts(self) -> None:
        """
        Update corresponding hosts. The blue writer host is mapped to the green writer host, and each blue reader host is
        mapped to a green reader host.
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
                    # Map each blue reader to a green reader.
                    green_index = 0
                    for blue_host_info in sorted_blue_readers:
                        self._corresponding_hosts.put(
                            blue_host_info.host, (blue_host_info, sorted_green_readers[green_index]))
                        green_index += 1
                        # The modulo operation prevents us from exceeding the bounds of sorted_green_readers if there
                        # are more blue readers than green readers. In this case, multiple blue readers may be mapped to
                        # the same green reader.
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
                        self._rds_utils.get_cluster_id(green_host) or "")),
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

    def _update_summary_status(self, bg_role: BlueGreenRole, interim_status: BlueGreenInterimStatus) -> None:
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
            self._reset_monitors("_monitor_reset_on_in_progress_completed", "- start")
        elif self._latest_phase == BlueGreenPhase.POST:
            self._update_dns_flags(bg_role, interim_status)
            self._summary_status = self._get_status_of_post()
        elif self._latest_phase == BlueGreenPhase.COMPLETED:
            self._update_dns_flags(bg_role, interim_status)
            self._summary_status = self._get_status_of_completed()
        else:
            raise ValueError(Messages.get_formatted("BlueGreenStatusProvider.UnknownPhase", self._bg_id, self._latest_phase))

    def _update_dns_flags(self, bg_role: BlueGreenRole, interim_status: BlueGreenInterimStatus) -> None:
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
            self._reset_monitors("_monitor_reset_on_topology_completed", "- green topology")

    def _store_event_phase_time(self, key_prefix: str, phase: Optional[BlueGreenPhase] = None) -> None:
        rollback_str = " (rollback)" if self._rollback else ""
        key = f"{key_prefix}{rollback_str}"
        self._phase_times_ns.put_if_absent(key, PhaseTimeInfo(datetime.now(), perf_counter_ns(), phase))

    def _start_switchover_timer(self) -> None:
        if self._post_status_end_time_ns == 0:
            self._post_status_end_time_ns = perf_counter_ns() + self._switchover_timeout_ns

    def _get_status_of_created(self) -> BlueGreenStatus:
        return BlueGreenStatus(
            self._bg_id,
            BlueGreenPhase.CREATED,
            [],
            [],
            self._roles_by_host,
            self._corresponding_hosts
        )

    def _get_status_of_preparation(self) -> BlueGreenStatus:
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

    def _get_host_and_port(self, host: str, port: int) -> str:
        return f"{host}:{port}" if port > 0 else host

    def _get_status_of_in_progress(self) -> BlueGreenStatus:
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
            interim_status = self._interim_statuses[BlueGreenRole.SOURCE.value]
            if interim_status is not None and self._interim_status_contains_ip_address(interim_status, ip_address):
                host_execute_routing = SuspendExecuteRouting(ip_address, None, self._bg_id)
                host_and_port = self._get_host_and_port(ip_address, interim_status.port)
                host_port_execute_routing = SuspendExecuteRouting(host_and_port, None, self._bg_id)
                execute_routings.extend([host_execute_routing, host_port_execute_routing])
                continue

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
                    else self._make_iam_success_handler(green_host)
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

    def _make_iam_success_handler(self, green_host: str) -> Callable[[str], None]:
        return lambda iam_host: self._register_iam_host(green_host, iam_host)

    def _is_already_successfully_connected(self, connect_host: str, iam_host: str) -> bool:
        success_hosts = self._iam_auth_success_hosts.compute_if_absent(connect_host, lambda _: ConcurrentSet())
        return success_hosts is not None and iam_host in success_hosts

    def _register_iam_host(self, connect_host: str, iam_host: str) -> None:
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

    def _update_monitors(self) -> None:
        if self._summary_status is None:
            return
        phase = self._summary_status.phase
        if phase == BlueGreenPhase.NOT_CREATED:
            for monitor in self._monitors:
                monitor.interval_rate = BlueGreenIntervalRate.BASELINE
                monitor.should_collect_ip_addresses = False
                monitor.should_collect_topology = False
                monitor.use_ip_address = False
        elif phase == BlueGreenPhase.CREATED:
            for monitor in self._monitors:
                monitor.interval_rate = BlueGreenIntervalRate.INCREASED
                monitor.should_collect_ip_addresses = True
                monitor.should_collect_topology = True
                monitor.use_ip_address = False
                if self._rollback:
                    monitor.reset_collected_data()
        elif phase == BlueGreenPhase.PREPARATION \
                or phase == BlueGreenPhase.IN_PROGRESS \
                or phase == BlueGreenPhase.POST:
            for monitor in self._monitors:
                monitor.interval_rate = BlueGreenIntervalRate.HIGH
                monitor.should_collect_ip_addresses = False
                monitor.should_collect_topology = False
                monitor.use_ip_address = True
        elif phase == BlueGreenPhase.COMPLETED:
            for monitor in self._monitors:
                monitor.interval_rate = BlueGreenIntervalRate.BASELINE
                monitor.should_collect_ip_addresses = False
                monitor.should_collect_topology = False
                monitor.use_ip_address = False
                monitor.reset_collected_data()

            # Stop monitoring old1 cluster/instance.
            if not self._rollback and self._monitors[BlueGreenRole.SOURCE.value] is not None:
                self._monitors[BlueGreenRole.SOURCE.value].stop = True
        else:
            raise UnsupportedOperationError(
                Messages.get_formatted(
                    "BlueGreenStatusProvider.UnknownPhase", self._bg_id, self._summary_status.phase))

    def _update_status_cache(self) -> None:
        if self._summary_status is None:
            return
        self._plugin_service.set_status(BlueGreenStatus, self._bg_id, self._summary_status)
        phase = self._summary_status.phase
        self._store_event_phase_time(phase.name, phase)

    def _reset_monitors(self, completed_flag_attr: str, event_name: str) -> None:
        if getattr(self, completed_flag_attr):
            return
        setattr(self, completed_flag_attr, True)

        blue_endpoints = frozenset(
            host for host, role in self._roles_by_host.items()
            if role == BlueGreenRole.SOURCE)

        host_list_provider = self._plugin_service.host_list_provider
        if host_list_provider is not None:
            try:
                cluster_id = host_list_provider.get_cluster_id()
                services_container.get_event_publisher().publish(
                    MonitorResetEvent(cluster_id=cluster_id, endpoints=blue_endpoints))
            except Exception:  # noqa: BLE001 - reset signalling is best-effort
                pass
        self._store_event_phase_time(f"Monitor reset {event_name}")

    def _log_current_context(self) -> None:
        if self._summary_status is None:
            return
        logger.debug(f"[bg_id: '{self._bg_id}'] Summary status: \n{self._summary_status}")
        logger.debug("\n"
                     f"   latest_status_phase: {self._latest_phase}\n"
                     f"   blue_dns_update_completed: {self._blue_dns_update_completed}\n"
                     f"   green_dns_removed: {self._green_dns_removed}\n"
                     f"   all_green_hosts_changed_name: {self._all_green_hosts_changed_name}\n"
                     f"   green_topology_changed: {self._green_topology_changed}\n")

    def _reset_context_when_completed(self) -> None:
        if self._summary_status is None:
            return
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
        self._monitor_reset_on_in_progress_completed = False
        self._monitor_reset_on_topology_completed = False
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


__all__ = [
    "AsyncBlueGreenPlugin",
    "AsyncBlueGreenStatusMonitor",
    "AsyncBlueGreenStatusProvider",
    "BlueGreenIntervalRate",
    "BlueGreenPhase",
    "BlueGreenRole",
    "BlueGreenStatus",
    "BlueGreenInterimStatus",
    "BlueGreenDbStatusInfo",
    "ConnectRouting",
    "ExecuteRouting",
    "PassThroughConnectRouting",
    "RejectConnectRouting",
    "SubstituteConnectRouting",
    "SuspendConnectRouting",
    "SuspendUntilCorrespondingHostFoundConnectRouting",
    "PassThroughExecuteRouting",
    "SuspendExecuteRouting",
    "PhaseTimeInfo",
]
