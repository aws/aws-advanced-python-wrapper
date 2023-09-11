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

from typing import Callable, Optional

import psycopg

from aws_wrapper.errors import UnsupportedOperationError
from aws_wrapper.utils.messages import Messages
from .test_driver import TestDriver
from .test_environment import TestEnvironment


class DriverHelper:

    @staticmethod
    def get_connect_func(test_driver: TestDriver) -> Callable:
        d: Optional[TestDriver] = test_driver
        if d is None:
            d = TestEnvironment.get_current().get_current_driver()

        if d is None:
            raise Exception(Messages.get("Testing.RequiredTestDriver"))

        if d == TestDriver.PG:
            return psycopg.Connection.connect
        else:
            raise UnsupportedOperationError(d.value)
