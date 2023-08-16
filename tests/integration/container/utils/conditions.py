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
    from .database_engine import DatabaseEngine
    from .database_engine_deployment import DatabaseEngineDeployment
    from .test_driver import TestDriver

from typing import List

import pytest

from .test_environment import TestEnvironment
from .test_environment_features import TestEnvironmentFeatures

failover_support_required = pytest.mark.skipif(
    TestEnvironmentFeatures.FAILOVER_SUPPORTED not in TestEnvironment.get_current().get_features(),
    reason="FAILOVER_SUPPORTED required"
)

network_outages_required = pytest.mark.skipif(
    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED not in TestEnvironment.get_current().get_features(),
    reason="NETWORK_OUTAGES_ENABLED required"
)

iam_required = pytest.mark.skipif(
    TestEnvironmentFeatures.IAM not in TestEnvironment.get_current().get_features(),
    reason="IAM required"
)


def enable_on_deployment(requested_deployment: DatabaseEngineDeployment):
    current_deployment = TestEnvironment.get_current().get_deployment()
    return pytest.mark.skipif(
        requested_deployment != current_deployment,
        reason=f"This test is not supported for {current_deployment.value} deployments"
    )


def enable_on_engines(requested_engines: List[DatabaseEngine]):
    current_engine = TestEnvironment.get_current().get_engine()
    return pytest.mark.skipif(
        current_engine not in requested_engines,
        reason=f"This test is not supported for {current_engine.value}"
    )


def enable_on_drivers(enabled_drivers: List[TestDriver]):
    current_driver = TestEnvironment.get_current().get_current_driver()
    if current_driver is None:
        reason = "The current driver evaluated to None"
    else:
        reason = f"This test does not support the {current_driver.value} driver"
    return pytest.mark.skipif(
        current_driver is None or current_driver not in enabled_drivers,
        reason=reason
    )


def enable_on_num_instances(min_instances=-1, max_instances=-1):
    num_instances = len(TestEnvironment.get_current().get_instances())
    should_disable = False
    if min_instances > -1 and num_instances < min_instances:
        should_disable = True
    if max_instances > -1 and num_instances > max_instances:
        should_disable = True
    return pytest.mark.skipif(
        should_disable,
        reason=f"This test is not supported for test configurations with {num_instances} instances"
    )


def enable_on_features(enable_on_test_features: List[TestEnvironmentFeatures]):
    current_features = TestEnvironment.get_current().get_features()
    should_disable = False
    for feature in enable_on_test_features:
        if feature not in current_features:
            should_disable = True

    return pytest.mark.skipif(
        should_disable,
        reason="The current test environment does not contain test features required for this test"
    )


def disable_on_drivers(disabled_drivers: List[TestDriver]):
    current_driver = TestEnvironment.get_current().get_current_driver()
    if current_driver is None:
        reason = "The current driver evaluated to None"
    else:
        reason = f"This test does not support the {current_driver.value} driver"
    return pytest.mark.skipif(
        current_driver is None or current_driver in disabled_drivers,
        reason=reason
    )


def disable_on_features(disable_on_test_features: List[TestEnvironmentFeatures]):
    current_features = TestEnvironment.get_current().get_features()
    should_disable = False
    for feature in disable_on_test_features:
        if feature in current_features:
            should_disable = True

    return pytest.mark.skipif(
        should_disable,
        reason="The current test environment contains test features for which this test is disabled"
    )
