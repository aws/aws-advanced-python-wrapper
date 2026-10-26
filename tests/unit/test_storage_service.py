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

from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.utils.storage.storage_service import \
    StorageService


class TypeA:
    pass


class TypeB:
    pass


@pytest.fixture
def publisher():
    return MagicMock()


@pytest.fixture
def storage(publisher):
    return StorageService(publisher)


def test_register_and_get(storage):
    storage.register(TypeA, item_expiration_time=timedelta(minutes=5))
    storage.put(TypeA, "key1", "value1")
    assert storage.get(TypeA, "key1") == "value1"


def test_get_unregistered_type_returns_none(storage):
    assert storage.get(TypeA, "key1") is None


def test_put_unregistered_type_raises(storage):
    with pytest.raises(ValueError):
        storage.put(TypeA, "key1", "value1")


def test_put_replaces_existing_value(storage):
    storage.register(TypeA, item_expiration_time=timedelta(minutes=5))
    storage.put(TypeA, "key1", "old")
    storage.put(TypeA, "key1", "new")
    assert storage.get(TypeA, "key1") == "new"


def test_multiple_types_are_independent(storage):
    storage.register(TypeA, item_expiration_time=timedelta(minutes=5))
    storage.register(TypeB, item_expiration_time=timedelta(minutes=5))

    storage.put(TypeA, "key1", "a_value")
    storage.put(TypeB, "key1", "b_value")

    assert storage.get(TypeA, "key1") == "a_value"
    assert storage.get(TypeB, "key1") == "b_value"


def test_remove(storage):
    storage.register(TypeA, item_expiration_time=timedelta(minutes=5))
    storage.put(TypeA, "key1", "value1")
    storage.remove(TypeA, "key1")
    assert storage.get(TypeA, "key1") is None


def test_remove_unregistered_type_is_noop(storage):
    storage.remove(TypeA, "key1")  # should not raise


def test_clear(storage):
    storage.register(TypeA, item_expiration_time=timedelta(minutes=5))
    storage.put(TypeA, "k1", "v1")
    storage.put(TypeA, "k2", "v2")
    storage.clear(TypeA)
    assert storage.get(TypeA, "k1") is None
    assert storage.get(TypeA, "k2") is None


def test_clear_all(storage):
    storage.register(TypeA, item_expiration_time=timedelta(minutes=5))
    storage.register(TypeB, item_expiration_time=timedelta(minutes=5))
    storage.put(TypeA, "k1", "v1")
    storage.put(TypeB, "k2", "v2")

    storage.clear_all()

    assert storage.get(TypeA, "k1") is None
    assert storage.get(TypeB, "k2") is None


def test_exists(storage):
    storage.register(TypeA, item_expiration_time=timedelta(minutes=5))
    assert storage.exists(TypeA, "key1") is False
    storage.put(TypeA, "key1", "value1")
    assert storage.exists(TypeA, "key1") is True


def test_exists_unregistered_type(storage):
    assert storage.exists(TypeA, "key1") is False


def test_release_resources(storage):
    storage.register(TypeA, item_expiration_time=timedelta(minutes=5))
    storage.put(TypeA, "key1", "value1")

    storage.release_resources()

    assert storage.get(TypeA, "key1") is None
    assert len(storage._caches) == 0


def test_get_publishes_data_access_event(storage, publisher):
    storage.register(TypeA, item_expiration_time=timedelta(minutes=5))
    storage.put(TypeA, "key1", "value1")

    publisher.reset_mock()
    storage.get(TypeA, "key1")

    publisher.publish.assert_called_once()
    event = publisher.publish.call_args[0][0]
    assert event.data_type is TypeA
    assert event.key == "key1"


def test_get_miss_does_not_publish_event(storage, publisher):
    storage.register(TypeA, item_expiration_time=timedelta(minutes=5))

    publisher.reset_mock()
    storage.get(TypeA, "nonexistent")

    publisher.publish.assert_not_called()


def test_register_is_idempotent(storage):
    storage.register(TypeA, item_expiration_time=timedelta(minutes=5))
    storage.put(TypeA, "key1", "value1")
    storage.register(TypeA, item_expiration_time=timedelta(minutes=10))  # should not replace
    assert storage.get(TypeA, "key1") == "value1"
