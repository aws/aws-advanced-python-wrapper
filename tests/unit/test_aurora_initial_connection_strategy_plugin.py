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

from unittest.mock import MagicMock

from aws_advanced_python_wrapper.aurora_initial_connection_strategy_plugin import \
    AuroraInitialConnectionStrategyPlugin
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


def test_retry_deadline_uses_timeout_property():
    """Regression (parity review): the retry deadline previously reused
    OPEN_CONNECTION_RETRY_INTERVAL_MS; it must come from
    OPEN_CONNECTION_RETRY_TIMEOUT_MS."""
    plugin = AuroraInitialConnectionStrategyPlugin.__new__(
        AuroraInitialConnectionStrategyPlugin)
    plugin._plugin_service = MagicMock()
    plugin._plugin_service.get_host_info_by_strategy = MagicMock(return_value=None)
    plugin._host_list_provider_service = MagicMock()

    props = Properties({})
    # Zero total budget: the retry loop must not run even once despite the
    # 10-minute interval (with the old bug the deadline WAS the interval).
    WrapperProperties.OPEN_CONNECTION_RETRY_TIMEOUT_MS.set(props, "0")
    WrapperProperties.OPEN_CONNECTION_RETRY_INTERVAL_MS.set(props, "600000")

    connect_func = MagicMock()
    result = plugin._get_verified_writer_connection(props, True, connect_func)
    assert result is None
    connect_func.assert_not_called()
