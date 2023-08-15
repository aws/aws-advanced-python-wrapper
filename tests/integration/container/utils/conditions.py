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

from .test_environment import TestEnvironment
from .test_environment_features import TestEnvironmentFeatures

failover_support_required = pytest.mark.skipif(
    TestEnvironmentFeatures.FAILOVER_SUPPORTED not in TestEnvironment.get_current().get_info().get_request().get_features(),
    reason="FAILOVER_SUPPORTED required"
)

network_outages_enabled_required = pytest.mark.skipif(
    TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED not in TestEnvironment.get_current().get_info().get_request().get_features(),
    reason="NETWORK_OUTAGES_ENABLED required"
)

iam_required = pytest.mark.skipif(
    TestEnvironmentFeatures.IAM not in TestEnvironment.get_current().get_info().get_request().get_features(),
    reason="IAM required"
)

disable_on_mariadb_driver = pytest.mark.skipif(
    TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS not in TestEnvironment.get_current().get_info().get_request().get_features(),
    reason="This test does not support the MariaDB driver"
)