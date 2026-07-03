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

"""Async resource cleanup.

Application shutdown ordering::

    await engine.dispose()                        # SQLAlchemy pool
    await release_resources_async()               # wrapper async tasks
    release_resources()                           # wrapper sync threads

In practice ``release_resources_async()`` is a lightweight wrapper around
the sync :func:`aws_advanced_python_wrapper.cleanup.release_resources`,
plus a hook for SP-ers to register their per-instance shutdown coroutines
(topology monitor, any EFM standing tasks in a future version, etc.).
"""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, List

from aws_advanced_python_wrapper.cleanup import \
    release_resources as _sync_release_resources

_registered_shutdown_hooks: List[Callable[[], Awaitable[None]]] = []


def register_shutdown_hook(hook: Callable[[], Awaitable[None]]) -> None:
    """Register a coroutine to be awaited during :func:`release_resources_async`.

    Plugin/monitor instances that own background tasks should register a
    stop method here so application code can fully drain before exit.
    """
    _registered_shutdown_hooks.append(hook)


def clear_shutdown_hooks() -> None:
    """Testing helper: drop all registered hooks without awaiting them."""
    _registered_shutdown_hooks.clear()


async def release_resources_async() -> None:
    """Drain registered async shutdown hooks then run sync cleanup.

    Safe to call multiple times; hooks are consumed on each call and the
    registry is emptied afterward.
    """
    hooks = list(_registered_shutdown_hooks)
    _registered_shutdown_hooks.clear()
    if hooks:
        await asyncio.gather(*(hook() for hook in hooks), return_exceptions=True)
    _sync_release_resources()
