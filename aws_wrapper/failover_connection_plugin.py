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

from typing import Callable, Dict, Set

from aws_wrapper.host_list_provider import HostListProviderService
from aws_wrapper.hostinfo import HostInfo
from aws_wrapper.pep249 import Connection
from aws_wrapper.utils.notifications import NodeChangeOptions
from aws_wrapper.utils.properties import Properties
from aws_wrapper.utils.rds_url_type import RdsUrlType


class FailoverConnectionPlugin:
    def init_host_provider(
            self,
            driver_protocol: str,
            initial_url: str,
            properties: Properties,
            host_list_provider_service: HostListProviderService,
            init_host_provider_func: Callable):
        ...

    def notify_connection_changed(self, changes: Set[NodeChangeOptions]):  # -> OldConnectionSuggestedAction:
        ...

    def notify_node_list_changed(self, changes: Dict[str, Set[NodeChangeOptions]]):
        ...

    def set_rds_url_type(self, rds_url_type: RdsUrlType):
        ...

    def connect(
            self,
            driver_protocol: str,
            host: HostInfo,
            properties: Properties,
            is_initial_connection: bool,
            connect_func: Callable):  # -> Connection:
        ...

    def force_connect(
            self,
            driver_protocol: str,
            host: HostInfo,
            properties: Properties,
            is_initial_connection: bool,
            connect_func: Callable):  # -> Connection:
        ...

    def _update_topology(self, force_update: bool):
        ...

    def _transfer_session_state(self, from_conn: Connection, to_conn: Connection):
        ...

    def _failover(self, failed_host: HostInfo):
        ...

    def _failover_reader(self, failed_host: HostInfo):
        ...

    def _failover_writer(self):
        ...

    def _invalidate_current_connection(self):
        ...
