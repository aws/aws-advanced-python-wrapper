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

"""F3-B SP-10: async release_resources tests."""

from __future__ import annotations

import asyncio

from aws_advanced_python_wrapper.aio import cleanup as aio_cleanup


def test_release_resources_async_with_no_hooks_runs_sync_cleanup():
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        await aio_cleanup.release_resources_async()

    asyncio.run(_body())


def test_register_and_await_shutdown_hook():
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        called: list = []

        async def _hook() -> None:
            called.append(1)

        aio_cleanup.register_shutdown_hook(_hook)
        await aio_cleanup.release_resources_async()
        assert called == [1]

    asyncio.run(_body())


def test_release_resources_async_is_idempotent():
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        called: list = []

        async def _hook() -> None:
            called.append(1)

        aio_cleanup.register_shutdown_hook(_hook)
        await aio_cleanup.release_resources_async()
        # Second call should not re-invoke the hook (registry is empty now).
        await aio_cleanup.release_resources_async()
        assert called == [1]

    asyncio.run(_body())


def test_release_resources_async_swallows_hook_exceptions():
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        completed: list = []

        async def _bad_hook() -> None:
            raise RuntimeError("shutdown failure")

        async def _good_hook() -> None:
            completed.append("good")

        aio_cleanup.register_shutdown_hook(_bad_hook)
        aio_cleanup.register_shutdown_hook(_good_hook)
        # Should not raise; all hooks run.
        await aio_cleanup.release_resources_async()
        assert completed == ["good"]

    asyncio.run(_body())


def test_aio_package_exports_release_resources_async():
    from aws_advanced_python_wrapper import aio
    assert hasattr(aio, "release_resources_async")
    assert hasattr(aio, "register_shutdown_hook")
    assert hasattr(aio, "AsyncAwsWrapperConnection")
