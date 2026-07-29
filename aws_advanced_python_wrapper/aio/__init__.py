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

"""Async counterparts of the sync wrapper classes.

Structure:
  - ``aio.wrapper`` -- ``AsyncAwsWrapperConnection`` (async connection facade)
  - ``aio.plugin`` -- ``AsyncPlugin`` / ``AsyncConnectionProvider`` ABCs
  - ``aio.driver_dialect`` -- the sole driver-specific interface
    (``AsyncDriverDialect`` ABC + per-driver concrete dialects)
  - ``aio.pooled_connection_provider`` -- ``AsyncPooledConnectionProvider``
    (per-host, per-user async pool with sliding expiration)

Typical usage::

    from aws_advanced_python_wrapper.aio import AsyncAwsWrapperConnection

    conn = await AsyncAwsWrapperConnection.connect(...)
"""

from aws_advanced_python_wrapper.aio.cleanup import (register_shutdown_hook,
                                                     release_resources_async)
from aws_advanced_python_wrapper.aio.pooled_connection_provider import \
    AsyncPooledConnectionProvider
from aws_advanced_python_wrapper.aio.wrapper import AsyncAwsWrapperConnection

__all__ = [
    "AsyncAwsWrapperConnection",
    "AsyncPooledConnectionProvider",
    "release_resources_async",
    "register_shutdown_hook",
]
