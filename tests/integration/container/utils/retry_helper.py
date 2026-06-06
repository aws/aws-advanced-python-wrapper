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

from __future__ import annotations

import timeit
from time import sleep
from typing import TYPE_CHECKING, Callable

from aws_advanced_python_wrapper.utils.log import Logger

if TYPE_CHECKING:
    from .rds_test_utility import RdsTestUtility

logger = Logger(__name__)

_DEFAULT_TIMEOUT_SEC: float = 5 * 60  # 5 minutes
_DEFAULT_DELAY_SEC: float = 5  # 5 seconds


def retry_until(
        condition: Callable[[], bool],
        timeout_sec: float = _DEFAULT_TIMEOUT_SEC,
        delay_sec: float = _DEFAULT_DELAY_SEC) -> bool:
    deadline = timeit.default_timer() + timeout_sec

    while timeit.default_timer() < deadline:
        if condition():
            return True
        sleep(delay_sec)

    return False


def verify_writer(aurora_util: RdsTestUtility, expected_writer_id: str) -> bool:
    logger.debug("Expected writer (API): " + expected_writer_id)

    def _condition() -> bool:
        api_writer_id = aurora_util.get_cluster_writer_instance_id()
        logger.debug("Writer (API): " + api_writer_id)
        return expected_writer_id.lower() == api_writer_id.lower()

    return retry_until(_condition)
