#   Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#   Licensed under the Apache License, Version 2.0 (the "License").
#   You may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from typing import Optional, Set


class AllowedAndBlockedHosts:
    def __init__(self, allowed_host_ids: Optional[Set[str]], blocked_host_ids: Optional[Set[str]]):
        self._allowed_host_ids = None if not allowed_host_ids else allowed_host_ids
        self._blocked_host_ids = None if not blocked_host_ids else blocked_host_ids

    @property
    def allowed_host_ids(self) -> Optional[Set[str]]:
        return self._allowed_host_ids

    @property
    def blocked_host_ids(self) -> Optional[Set[str]]:
        return self._blocked_host_ids
