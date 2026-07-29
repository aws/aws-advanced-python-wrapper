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

"""Pass-through stubs for plugins not yet ported to async.

Currently empty: every plugin code that previously stood in as a stub
has a real async port. Kept as an importable module for backwards
compatibility with downstream code; may be removed in a later cleanup.

Historical context: phase H.2 introduced ``AsyncBlueGreenStubPlugin``
as the only stub; it was replaced by :class:`AsyncBlueGreenPlugin`
(skeleton port) in a later commit.
"""

from __future__ import annotations

from typing import Set

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.utils.log import Logger

logger = Logger(__name__)


class _AsyncStubPlugin(AsyncPlugin):
    """Shared base for async plugin stubs.

    Retained so a future unimplemented plugin code can register a
    pass-through stub without re-deriving the scaffolding. Subclasses
    set :attr:`_STUB_NAME` to the plugin code they stand in for.
    Construction logs a WARNING; the plugin subscribes to no pipeline
    methods, so the plugin manager skips it for every call.
    """

    _STUB_NAME: str = "unknown"

    def __init__(self) -> None:
        # Pre-format the message and pass it to the logger with no extra
        # args -- the wrapper Logger tries a resource-bundle lookup when
        # args are supplied, which would fail for our ad-hoc strings.
        # With no args it falls back to the raw message cleanly.
        logger.warning(
            "Plugin '{0}' is not implemented in the async wrapper; "
            "connections using it will behave as if the plugin were "
            "absent.".format(self._STUB_NAME)
        )

    @property
    def subscribed_methods(self) -> Set[str]:
        return set()


__all__: list = []
