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

from aws_wrapper.utils.atomic import AtomicInt


def test_set_and_get():
    n = AtomicInt(1)
    assert 1 == n.get()
    n.set(3)
    assert 3 == n.get()


def test_get_and_increment():
    n = AtomicInt()
    assert 0 == n.get_and_increment()
    assert 1 == n.get_and_increment()
    n.set(5)
    assert 5 == n.get_and_increment()


def test_get_and_increment__multithreaded():
    n = AtomicInt()
    num_threads = 50

    def get_and_increment_thread(atomic_num: AtomicInt):
        atomic_num.get_and_increment()

    with ThreadPoolExecutor(num_threads) as executor:
        for _ in range(num_threads):
            executor.submit(get_and_increment_thread, n)

    assert num_threads == n.get()


def test_increment_and_get():
    n = AtomicInt()
    assert 1 == n.increment_and_get()
    assert 2 == n.increment_and_get()
    assert 2 == n.get()
    n.set(5)
    assert 6 == n.increment_and_get()


def test_increment_and_get__multithreaded():
    n = AtomicInt()
    num_threads = 50

    def increment_and_get_thread(atomic_num: AtomicInt):
        atomic_num.increment_and_get()

    with ThreadPoolExecutor(num_threads) as executor:
        for _ in range(num_threads):
            executor.submit(increment_and_get_thread, n)

    assert num_threads == n.get()


def test_get_and_decrement():
    n = AtomicInt()
    assert 0 == n.get_and_decrement()
    assert -1 == n.get_and_decrement()
    n.set(5)
    assert 5 == n.get_and_decrement()


def test_get_and_decrement__multithreaded():
    num_threads = 50
    n = AtomicInt(num_threads)

    def get_and_decrement_thread(atomic_num: AtomicInt):
        atomic_num.get_and_decrement()

    with ThreadPoolExecutor(num_threads) as executor:
        for _ in range(num_threads):
            executor.submit(get_and_decrement_thread, n)

    assert 0 == n.get()


def test_decrement_and_get():
    n = AtomicInt()
    assert -1 == n.decrement_and_get()
    assert -2 == n.decrement_and_get()
    assert -2 == n.get()
    n.set(5)
    assert 4 == n.decrement_and_get()


def test_decrement_and_get__multithreaded():
    num_threads = 50
    n = AtomicInt(num_threads)

    def decrement_and_get_thread(atomic_num: AtomicInt):
        atomic_num.decrement_and_get()

    with ThreadPoolExecutor(num_threads) as executor:
        for _ in range(num_threads):
            executor.submit(decrement_and_get_thread, n)

    assert 0 == n.get()
