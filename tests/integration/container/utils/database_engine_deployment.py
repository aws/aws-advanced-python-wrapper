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


class DatabaseEngineDeployment(str, Enum):
    DOCKER = "DOCKER"
    RDS = "RDS"
    RDS_MULTI_AZ_CLUSTER = "RDS_MULTI_AZ_CLUSTER"
    RDS_MULTI_AZ_INSTANCE = "RDS_MULTI_AZ_INSTANCE"
    AURORA = "AURORA"
