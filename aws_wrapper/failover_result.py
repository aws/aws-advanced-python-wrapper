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
    from aws_wrapper.hostinfo import HostInfo
    from aws_wrapper.pep249 import Connection

from dataclasses import dataclass
from typing import List


@dataclass
class ReaderFailoverResult:
    connection: Connection
    is_connected: bool
    new_host: HostInfo
    exception: Exception


@dataclass
class WriterFailoverResult:
    new_connection: Connection
    is_connected: bool
    is_new_host: bool
    topology: List[HostInfo]
    task_name: str
    exception: Exception
