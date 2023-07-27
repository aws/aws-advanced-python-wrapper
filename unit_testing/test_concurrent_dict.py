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

import pytest

from aws_wrapper.utils.concurrent import ConcurrentDict


@pytest.fixture
def test_dict():
    return ConcurrentDict()


def test_put_if_absent(test_dict):
    assert "a" == test_dict.put_if_absent(1, "a")
    assert "a" == test_dict.get(1)

    assert "a" == test_dict.put_if_absent(1, "b")
    assert "a" == test_dict.get(1)


def test_compute_if_absent(test_dict):
    assert test_dict.compute_if_absent(1, lambda k: None) is None
    assert test_dict.get(1) is None

    assert "a" == test_dict.compute_if_absent(1, lambda k: "a")
    assert "a" == test_dict.get(1)

    assert "a" == test_dict.compute_if_absent(1, lambda k: "b")
    assert "a" == test_dict.get(1)


def test_compute_if_present(test_dict):
    assert test_dict.compute_if_present(1, lambda k, v: "a") is None

    test_dict.put_if_absent(1, "a")
    assert "a" == test_dict.get(1)
    assert test_dict.compute_if_present(1, lambda k, v: None) is None
    assert test_dict.get(1) is None

    test_dict.put_if_absent(1, "a")
    assert "a" == test_dict.get(1)
    assert "b" == test_dict.compute_if_present(1, lambda k, v: "b")
    assert "b" == test_dict.get(1)


def test_clear(test_dict):
    test_dict.put_if_absent(1, "a")
    test_dict.put_if_absent(2, "b")
    assert "a" == test_dict.get(1)
    assert "b" == test_dict.get(2)

    test_dict.clear()
    assert test_dict.get(1) is None
    assert test_dict.get(2) is None


def test_remove_if(test_dict):
    test_dict.put_if_absent(1, [1, 2])
    test_dict.put_if_absent(2, [2, 3])
    test_dict.put_if_absent(3, [4, 5])

    assert test_dict.remove_if(lambda k, v: 2 in v) is True
    assert 1 == len(test_dict._dict)
    assert test_dict.get(1) is None
    assert test_dict.get(2) is None
    assert [4, 5] == test_dict.get(3)

    assert test_dict.remove_if(lambda k, v: 3 in v) is False
    assert 1 == len(test_dict._dict)
    assert [4, 5] == test_dict.get(3)


def test_remove_matching_values(test_dict):
    test_dict.put_if_absent(1, [1, 2])
    test_dict.put_if_absent(2, [2, 3])
    test_dict.put_if_absent(3, [4, 5])

    assert test_dict.remove_matching_values([[1, 2], [2, 3]]) is True
    assert 1 == len(test_dict._dict)
    assert test_dict.get(1) is None
    assert test_dict.get(2) is None
    assert [4, 5] == test_dict.get(3)

    assert test_dict.remove_if(lambda k, v: 3 in v) is False
    assert 1 == len(test_dict._dict)
    assert [4, 5] == test_dict.get(3)


def test_apply_if(mocker, test_dict):
    class SomeClass:
        def some_method(self):
            pass

    spies = []
    num_objects = 3
    num_applications = num_objects - 1
    for i in range(0, num_objects):
        some_object = SomeClass()
        test_dict.put_if_absent(i, some_object)
        spy = mocker.spy(some_object, "some_method")
        spies.append(spy)

    test_dict.apply_if(lambda k, v: k < num_objects - 1, lambda k, v: v.some_method())
    for i in range(0, num_applications):
        spies[i].assert_called_once()
    spies[num_objects - 1].assert_not_called()
