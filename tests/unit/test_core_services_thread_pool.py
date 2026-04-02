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

from concurrent.futures import ThreadPoolExecutor

import pytest

from aws_advanced_python_wrapper.utils import services_container


@pytest.fixture(autouse=True)
def cleanup_pools():
    yield
    services_container._instance._thread_pools.clear()


def test_get_thread_pool_creates_new_pool():
    pool = services_container.get_thread_pool("test_pool")
    assert isinstance(pool, ThreadPoolExecutor)
    assert "test_pool" in services_container._instance._thread_pools


def test_get_thread_pool_returns_existing_pool():
    pool1 = services_container.get_thread_pool("test_pool")
    pool2 = services_container.get_thread_pool("test_pool")
    assert pool1 is pool2


def test_get_thread_pool_with_max_workers():
    pool = services_container.get_thread_pool("test_pool", max_workers=5)
    assert pool._max_workers == 5


def test_thread_name_prefix():
    pool = services_container.get_thread_pool("custom_name")
    assert pool._thread_name_prefix == "custom_name"


def test_release_thread_pool():
    services_container.get_thread_pool("test_pool")
    assert "test_pool" in services_container._instance._thread_pools

    result = services_container.release_thread_pool("test_pool")
    assert result is True
    assert "test_pool" not in services_container._instance._thread_pools


def test_release_nonexistent_pool():
    result = services_container.release_thread_pool("nonexistent")
    assert result is False


def test_release_resources_clears_pools():
    services_container.get_thread_pool("pool1")
    services_container.get_thread_pool("pool2")
    assert len(services_container._instance._thread_pools) == 2

    services_container.release_resources()
    assert len(services_container._instance._thread_pools) == 0
