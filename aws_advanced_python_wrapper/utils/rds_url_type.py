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


class RdsUrlType(Enum):

    def __new__(cls, *args, **kwargs):
        value = len(cls.__members__) + 1
        obj = object.__new__(cls)
        obj._value_ = value
        return obj

    def __init__(self, is_rds: bool, is_rds_cluster: bool):
        self.is_rds: bool = is_rds
        self.is_rds_cluster: bool = is_rds_cluster

    IP_ADDRESS = False, False,
    RDS_WRITER_CLUSTER = True, True,
    RDS_READER_CLUSTER = True, True,
    RDS_CUSTOM_CLUSTER = True, True,
    RDS_PROXY = True, False,
    RDS_INSTANCE = True, False,
    OTHER = False, False
