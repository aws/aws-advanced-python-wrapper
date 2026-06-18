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

import sys
from logging import DEBUG, getLogger

from aws_advanced_python_wrapper import _dbapi
from aws_advanced_python_wrapper.cleanup import release_resources
from aws_advanced_python_wrapper.driver_info import DriverInfo
from aws_advanced_python_wrapper.utils.utils import LogUtils
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection

# Populate the full PEP 249 module surface (exceptions, type ctors/singletons,
# apilevel/threadsafety/paramstyle). `connect` stays bound to
# AwsWrapperConnection.connect for back-compat with existing callers.
_dbapi.install(sys.modules[__name__].__dict__, connect=AwsWrapperConnection.connect)

__version__ = DriverInfo.DRIVER_VERSION


def set_logger(name="aws_advanced_python_wrapper", level=DEBUG, format_string=None):
    LogUtils.setup_logger(getLogger(name), level, format_string)


__all__ = (
    "AwsWrapperConnection",
    "release_resources",
    "set_logger",
    *_dbapi._PEP249_NAMES,
    "connect",
)
