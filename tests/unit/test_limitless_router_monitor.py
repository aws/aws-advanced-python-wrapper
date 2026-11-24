#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.limitless_plugin import LimitlessRouterMonitor
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


def test_monitor_props_strip_prefix_and_force_no_wait(mocker):
    # The constructor starts the monitoring thread; keep it from running so the
    # test covers only how the monitoring properties are built.
    mocker.patch("aws_advanced_python_wrapper.limitless_plugin.Thread")
    mocker.patch("aws_advanced_python_wrapper.limitless_plugin.services_container")
    props = Properties({
        "limitless-router-monitor-connect_timeout": "3",
        "limitless-router-monitor-socket_timeout": "5",
        "limitless_wait_for_transaction_router_info": True,
    })

    monitor = LimitlessRouterMonitor(mocker.MagicMock(), HostInfo("h", 5432, role=HostRole.WRITER), "cache-key", props, 10_000)

    assert monitor._properties.get("connect_timeout") == "3"
    assert monitor._properties.get("socket_timeout") == "5"
    assert "limitless-router-monitor-connect_timeout" not in monitor._properties
    assert "limitless-router-monitor-socket_timeout" not in monitor._properties
    assert WrapperProperties.WAIT_FOR_ROUTER_INFO.get(monitor._properties) is False
    # The caller's properties are left unchanged.
    assert props.get("limitless-router-monitor-connect_timeout") == "3"
