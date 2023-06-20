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

from abc import abstractmethod
from typing import List

from aws_wrapper.failover_result import WriterFailoverResult
from aws_wrapper.hostinfo import HostInfo


class WriterFailoverHandlerInterface:
    @abstractmethod
    def failover(self, current_topology: List[HostInfo]) -> WriterFailoverResult:
        pass


class WriterFailoverHandler(WriterFailoverHandlerInterface):
    def failover(self, current_topology: List[HostInfo]):  # -> WriterFailoverResult:
        ...
