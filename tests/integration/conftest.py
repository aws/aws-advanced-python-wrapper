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

from typing import List

from .framework.proxy_helper import ProxyHelper
from .framework.test_driver import TestDriver
from .framework.test_environment import TestEnvironment


def pytest_runtest_setup(item):

    if hasattr(item, "callspec"):
        current_driver = item.callspec.params.get("testDriver")
        TestEnvironment.get_current().set_current_driver(current_driver)
    else:
        TestEnvironment.get_current().set_current_driver(None)

    ProxyHelper.enable_all_connectivity()
    # TODO: Check cluster health
    # TODO: Reset internal caches, if any


def pytest_generate_tests(metafunc):
    if "test_environment" in metafunc.fixturenames:
        metafunc.parametrize("test_environment", [TestEnvironment.get_current()])
    if "test_driver" in metafunc.fixturenames:
        allowed_drivers: List[TestDriver] = TestEnvironment.get_current().get_allowed_test_drivers()  # type: ignore
        metafunc.parametrize("test_driver", allowed_drivers)


def pytest_sessionstart(session):
    TestEnvironment.get_current()
