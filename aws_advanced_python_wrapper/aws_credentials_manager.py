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
from typing import TYPE_CHECKING, Callable, Optional

import boto3
from boto3 import Session
from aws_advanced_python_wrapper.utils.properties import WrapperProperties
if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties


class AwsCredentialsManager:
    _handler: Optional[Callable[[HostInfo, Properties], Optional[Session]]] = None
    _lock = Lock()

    @staticmethod
    def set_custom_handler(custom_handler: Callable[[HostInfo, Properties], Optional[Session]]) -> None:
        with AwsCredentialsManager._lock:
            AwsCredentialsManager._handler = custom_handler

    @staticmethod
    def reset_custom_handler() -> None:
        with AwsCredentialsManager._lock:
            AwsCredentialsManager._handler = None

    @staticmethod
    def get_session(host_info: HostInfo, props: Properties) -> Session:
        with AwsCredentialsManager._lock:
            session = AwsCredentialsManager._handler(host_info, props) if AwsCredentialsManager._handler else None
            
            if session is None:
                profile_name = WrapperProperties.AWS_PROFILE.get(props)
                session = boto3.Session(profile_name=profile_name) if profile_name else boto3.Session()
            
            return session