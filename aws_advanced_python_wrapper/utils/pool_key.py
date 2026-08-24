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

"""Cache key shared by the sync and async internal connection pool providers.

This lives in its own module, free of any pool implementation imports, so that
the async provider can key its pools identically without taking a dependency on
SQLAlchemy (which the sync provider requires but the async provider does not).
"""

from __future__ import annotations

from typing import Optional, Tuple


class PoolKey:
    def __init__(self, url: str, extra_key: Optional[str] = None):
        self._url = url
        self._extra_key = extra_key

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.__members() == other.__members()
        else:
            return False

    def __hash__(self):
        return hash(self.__members())

    def __members(self) -> Tuple[str, Optional[str]]:
        return self._url, self._extra_key

    @property
    def url(self):
        return self._url

    @property
    def extra_key(self):
        return self._extra_key
