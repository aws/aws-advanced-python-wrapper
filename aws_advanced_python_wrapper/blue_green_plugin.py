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

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from threading import Condition
from types import MappingProxyType
from typing import Optional, Tuple, Any, Callable, Dict, Set, NoReturn, ClassVar

from mysql.connector import Connect

from aws_advanced_python_wrapper.errors import AwsWrapperError, UnsupportedOperationError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249 import Connection
from aws_advanced_python_wrapper.plugin import Plugin
from aws_advanced_python_wrapper.plugin_service import PluginService
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import Properties
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils

logger = Logger(__name__)


class BlueGreenIntervalRate(Enum):
    BASELINE = auto()
    INCREASED = auto()
    HIGH = auto()


class BlueGreenPhase(Enum):
    NOT_CREATED = (0, False)
    CREATED = (1, False)
    PREPARATION = (2, True) # nodes are accessible
    IN_PROGRESS = (3, True) # active phase; nodes are not accessible
    POST = (4, True) # nodes are accessible; some change are still in progress
    COMPLETED = (5, True) # all changes are completed

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
    def __init__(self,
                 bg_id: str,
                 phase: BlueGreenPhase,
                 connect_routing: Tuple[ConnectRouting, ...] = (),
                 execute_routing: Tuple[ExecuteRouting, ...] = (),
                 role_by_endpoint: MappingProxyType[str, BlueGreenRole] = MappingProxyType({})
                 ):
        self.bg_id = bg_id
        self.phase = phase
        self.connect_routings = tuple(connect_routing)
        self.execute_routings = tuple(execute_routing)
        self.role_by_endpoint = MappingProxyType(role_by_endpoint)

    def get_role(self, host_info: HostInfo) -> BlueGreenRole:
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
    start_ip_addresses_by_host_map: Dict[str, Optional[str]]
    current_topology: Tuple[HostInfo, ...]
    current_ip_addresses_by_host_map: Dict[str, Optional[str]]
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
    
    def get_ip_dict_hash(self, current_hash: int, ip_dict: Optional[Dict[str, Optional[str]]]) -> int:
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
    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        ...

    @abstractmethod
    def apply(self,
              plugin: Plugin,
              host_info: HostInfo,
              props: Properties,
              is_initial_connection: bool,
              connect_func: Callable,
              plugin_service: PluginService) -> Connection:
        ...


class ExecuteRouting(ABC):
    @abstractmethod
    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        ...

    @abstractmethod
    def apply(self,
              plugin: Plugin,
              plugin_service: PluginService,
              props: Properties,
              target: type,
              method_name: str,
              execute_func: Callable,
              *args: Any,
              **kwargs: Any) -> Optional[Any]:
        ...


class BaseRouting:
    _MIN_SLEEP_MS = 50

    def __init__(self, endpoint: Optional[str], bg_role: Optional[BlueGreenRole]):
        self._cv = Condition()
        self._endpoint = endpoint  # host and optionally port as well
        self._bg_role = bg_role

    def delay(self, delay_ms: int, bg_status: BlueGreenStatus, plugin_service: PluginService, bg_id: str):
        end_time = time.time() + (delay_ms / 1_000)
        min_delay_ms = min(delay_ms, BaseRouting._MIN_SLEEP_MS)

        if bg_status is None:
            time.sleep(delay_ms / 1_000)
            return

        while bg_status is plugin_service.get_status(BlueGreenStatus, bg_id) and time.time() < end_time:
            with self._cv:
                self._cv.wait(min_delay_ms / 1_000)

    def is_match(self, host_info: HostInfo, bg_role: BlueGreenRole) -> bool:
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

    def apply(self,
              plugin: Plugin,
              host_info: HostInfo,
              props: Properties,
              is_initial_connection: bool,
              connect_func: Callable,
              plugin_service: PluginService) -> Connection:
        return connect_func()


class RejectConnectRouting(BaseRouting, ConnectRouting):
    def __init__(self, endpoint: Optional[str], bg_role: Optional[BlueGreenRole]):
        super().__init__(endpoint, bg_role)

    def apply(self,
              plugin: Plugin,
              host_info: HostInfo,
              props: Properties,
              is_initial_connection: bool,
              connect_func: Callable,
              plugin_service: PluginService) -> Connection:
        raise AwsWrapperError(Messages.get("RejectConnectRouting.InProgressCantConnect"))


class SubstituteConnectRouting(BaseRouting, ConnectRouting):
    _rds_utils: ClassVar[RdsUtils] = RdsUtils()

    def __init__(self,
                 endpoint: Optional[str],
                 bg_role: Optional[BlueGreenRole],
                 substitute_host_info: HostInfo,
                 iam_hosts: Optional[Tuple[HostInfo, ...]],
                 on_iam_connect_func: Optional[Callable]):
        super().__init__(endpoint, bg_role)
        self._substitute_host_info = substitute_host_info
        self._iam_hosts = iam_hosts
        self._on_iam_connect_func = on_iam_connect_func

    def apply(self,
              plugin: Plugin,
              host_info: HostInfo,
              props: Properties,
              is_initial_connection: bool,
              connect_func: Callable,
              plugin_service: PluginService) -> Connection:
        ...


class PassThroughExecuteRouting(BaseRouting, ExecuteRouting):
    def __init__(self, endpoint: Optional[str], bg_role: Optional[BlueGreenRole]):
        super().__init__(endpoint, bg_role)

    def apply(self,
              plugin: Plugin,
              plugin_service: PluginService,
              props: Properties,
              target: type,
              method_name: str,
              execute_func: Callable,
              *args: Any,
              **kwargs: Any) -> Optional[Any]:
        return execute_func()
