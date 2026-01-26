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

from threading import Lock
from typing import TYPE_CHECKING, Any, Callable, Optional

from boto3 import Session

from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties


class AwsCredentialsManager:
    _handler: Optional[Callable[[HostInfo, Properties], Optional[Session]]] = None
    _lock = Lock()
    _sessions: dict[str, Session] = {}
    _clients: dict[str, Any] = {}

    @staticmethod
    def set_custom_handler(custom_handler: Callable[[HostInfo, Properties], Optional[Session]]) -> None:
        if not callable(custom_handler):
            raise TypeError("custom_handler must be callable")
        with AwsCredentialsManager._lock:
            AwsCredentialsManager._handler = custom_handler

    @staticmethod
    def reset_custom_handler() -> None:
        with AwsCredentialsManager._lock:
            AwsCredentialsManager._handler = None

    @staticmethod
    def get_session(host_info: HostInfo, props: Properties, region: str) -> Session:
        host_key = f'{host_info.as_alias()}{region}'

        handler = None
        with AwsCredentialsManager._lock:
            if host_key in AwsCredentialsManager._sessions:
                return AwsCredentialsManager._sessions[host_key]
            handler = AwsCredentialsManager._handler

        # Initialize session outside of lock.
        session = handler(host_info, props) if handler else None

        if session is not None and not isinstance(session, type(Session())):
            raise TypeError(Messages.get_formatted("AwsCredentialsManager.InvalidHandler", type(session).__name__))

        if session is None:
            profile_name = WrapperProperties.AWS_PROFILE.get(props)
            session = Session(profile_name=profile_name, region_name=region) if profile_name else Session(region_name=region)

        with AwsCredentialsManager._lock:
            if host_key not in AwsCredentialsManager._sessions:
                AwsCredentialsManager._sessions[host_key] = session
            return AwsCredentialsManager._sessions[host_key]

    @staticmethod
    def get_client(service_name: str, session: Session, host: Optional[str], region: Optional[str], endpoint_url: Optional[str] = None):
        key = f'{host}{region}{service_name}{endpoint_url}'

        with AwsCredentialsManager._lock:
            if key in AwsCredentialsManager._clients:
                return AwsCredentialsManager._clients[key]

        # Initialize client outside of lock.
        if endpoint_url:
            client = session.client(service_name=service_name, endpoint_url=endpoint_url)  # type: ignore[call-overload]
        else:
            client = session.client(service_name=service_name)  # type: ignore[call-overload]

        with AwsCredentialsManager._lock:
            if key not in AwsCredentialsManager._clients:
                AwsCredentialsManager._clients[key] = client
            return AwsCredentialsManager._clients[key]

    @staticmethod
    def release_resources() -> None:
        with AwsCredentialsManager._lock:
            for key, client in AwsCredentialsManager._clients.items():
                client.close()
            AwsCredentialsManager._clients.clear()
            AwsCredentialsManager._sessions.clear()
        return None
