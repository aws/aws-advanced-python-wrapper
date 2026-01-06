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

from aws_advanced_python_wrapper.host_monitoring_plugin import \
    MonitoringThreadContainer
from aws_advanced_python_wrapper.thread_pool_container import \
    ThreadPoolContainer


def release_resources() -> None:
    """Release all global resources used by the wrapper."""
    MonitoringThreadContainer.clean_up()
    ThreadPoolContainer.release_resources()
