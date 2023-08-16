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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .utils.test_driver import TestDriver
    from .utils.test_environment import TestEnvironment

from logging import getLogger

import pytest

from .utils.aurora_test_utility import AuroraTestUtility
from .utils.conditions import failover_support_required


class TestAuroraFailover:
    logger = getLogger(__name__)

    @pytest.mark.skip(reason="This test is just sample code and it will eventually be removed")
    @failover_support_required
    def test_dummy(
            self, test_environment: TestEnvironment, test_driver: TestDriver):
        # TODO: this test is an example on how to use AuroraTestUtility. Remove this test.

        region: str = test_environment.get_aurora_region()
        aurora_utility = AuroraTestUtility(region)
        TestAuroraFailover.logger.debug(aurora_utility.get_aurora_instance_ids())
        current_writer_id = aurora_utility.get_cluster_writer_instance_id()
        TestAuroraFailover.logger.debug("Current writer: " + current_writer_id)
        aurora_utility.failover_cluster_and_wait_until_writer_changed(current_writer_id)
        current_writer_id = aurora_utility.get_cluster_writer_instance_id()
        TestAuroraFailover.logger.debug("New writer: " + current_writer_id)
