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

from aws_advanced_python_wrapper.thread_pool_container import \
    ThreadPoolContainer


@pytest.fixture(autouse=True)
def cleanup_pools():
    """Clean up all pools after each test"""
    yield
    ThreadPoolContainer.release_resources()


def test_get_thread_pool_creates_new_pool():
    pool = ThreadPoolContainer.get_thread_pool("test_pool")
    assert isinstance(pool, ThreadPoolExecutor)
    assert ThreadPoolContainer.has_pool("test_pool")


def test_get_thread_pool_returns_existing_pool():
    pool1 = ThreadPoolContainer.get_thread_pool("test_pool")
    pool2 = ThreadPoolContainer.get_thread_pool("test_pool")
    assert pool1 is pool2


def test_get_thread_pool_with_max_workers():
    pool = ThreadPoolContainer.get_thread_pool("test_pool", max_workers=5)
    assert pool._max_workers == 5


def test_has_pool():
    assert not ThreadPoolContainer.has_pool("nonexistent")
    ThreadPoolContainer.get_thread_pool("test_pool")
    assert ThreadPoolContainer.has_pool("test_pool")


def test_get_pool_names():
    assert ThreadPoolContainer.get_pool_names() == []
    ThreadPoolContainer.get_thread_pool("pool1")
    ThreadPoolContainer.get_thread_pool("pool2")
    names = ThreadPoolContainer.get_pool_names()
    assert "pool1" in names
    assert "pool2" in names
    assert len(names) == 2


def test_get_pool_count():
    assert ThreadPoolContainer.get_pool_count() == 0
    ThreadPoolContainer.get_thread_pool("pool1")
    assert ThreadPoolContainer.get_pool_count() == 1
    ThreadPoolContainer.get_thread_pool("pool2")
    assert ThreadPoolContainer.get_pool_count() == 2


def test_release_pool():
    ThreadPoolContainer.get_thread_pool("test_pool")
    assert ThreadPoolContainer.has_pool("test_pool")

    result = ThreadPoolContainer.release_pool("test_pool")
    assert result is True
    assert not ThreadPoolContainer.has_pool("test_pool")


def test_release_nonexistent_pool():
    result = ThreadPoolContainer.release_pool("nonexistent")
    assert result is False


def test_release_resources():
    ThreadPoolContainer.get_thread_pool("pool1")
    ThreadPoolContainer.get_thread_pool("pool2")
    assert ThreadPoolContainer.get_pool_count() == 2

    ThreadPoolContainer.release_resources()
    assert ThreadPoolContainer.get_pool_count() == 0


def test_set_default_max_workers():
    ThreadPoolContainer.set_default_max_workers(10)
    pool = ThreadPoolContainer.get_thread_pool("test_pool")
    assert pool._max_workers == 10


def test_thread_name_prefix():
    pool = ThreadPoolContainer.get_thread_pool("custom_name")
    # Check that the thread name prefix is set correctly
    assert pool._thread_name_prefix == "custom_name"
