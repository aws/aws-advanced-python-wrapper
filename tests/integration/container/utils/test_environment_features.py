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

from enum import Enum


class TestEnvironmentFeatures(Enum):
    __test__ = False

    IAM = "IAM"
    SECRETS_MANAGER = "SECRETS_MANAGER"
    FAILOVER_SUPPORTED = "FAILOVER_SUPPORTED"
    NETWORK_OUTAGES_ENABLED = "NETWORK_OUTAGES_ENABLED"
    AWS_CREDENTIALS_ENABLED = "AWS_CREDENTIALS_ENABLED"
    PERFORMANCE = "PERFORMANCE"
    SKIP_MYSQL_DRIVER_TESTS = "SKIP_MYSQL_DRIVER_TESTS"
    SKIP_PG_DRIVER_TESTS = "SKIP_PG_DRIVER_TESTS"
    SKIP_MARIADB_DRIVER_TESTS = "SKIP_MARIADB_DRIVER_TESTS"
