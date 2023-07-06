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

if TYPE_CHECKING:
    from aws_wrapper.host_list_provider import HostListProviderService
    from aws_wrapper.hostinfo import HostInfo
    from aws_wrapper.pep249 import Connection
    from aws_wrapper.utils.properties import Properties

from typing import Callable, Dict, Set

from aws_wrapper.plugin_service import Plugin
from aws_wrapper.utils.notifications import (ConnectionEvent, HostEvent,
                                             OldConnectionSuggestedAction)


class FailoverPlugin(Plugin):
    def init_host_provider(
            self,
            properties: Properties,
            host_list_provider_service: HostListProviderService,
            init_host_provider_func: Callable):
        ...

    def subscribed_methods(self):  # -> Set[str]:
        ...

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        return OldConnectionSuggestedAction.NO_OPINION

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]):
        ...

    def connect(
            self,
            host: HostInfo,
            properties: Properties,
            is_initial_connection: bool,
            connect_func: Callable):  # -> Connection:
        ...

    def force_connect(
            self,
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
