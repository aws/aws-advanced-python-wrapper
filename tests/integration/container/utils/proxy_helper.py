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
    from .proxy_info import ProxyInfo
    from toxiproxy import Proxy  # type: ignore

from aws_wrapper.utils.log import Logger

from toxiproxy.api import APIConsumer  # type: ignore
from toxiproxy.exceptions import NotFound  # type: ignore

from .test_environment import TestEnvironment


class ProxyHelper:
    logger = Logger(__name__)

    @staticmethod
    def disable_all_connectivity():
        for p in TestEnvironment.get_current().get_proxy_infos():
            ProxyHelper._disable_proxy_connectivity(p)

    @staticmethod
    def disable_connectivity(instance_name: str):
        proxy: Proxy = TestEnvironment.get_current().get_proxy_info(
            instance_name)
        ProxyHelper._disable_proxy_connectivity(proxy)

    @staticmethod
    def _disable_proxy_connectivity(proxy_info: ProxyInfo):
        APIConsumer.host = proxy_info.control_host
        APIConsumer.port = proxy_info.control_port

        attributes = dict()
        attributes["rate"] = 0
        proxy_info.proxy.add_toxic(type="bandwidth",
                                   name="DOWN-STREAM",
                                   stream="downstream",
                                   toxicity=1,
                                   attributes=attributes)
        proxy_info.proxy.add_toxic(type="bandwidth",
                                   name="UP-STREAM",
                                   stream="upstream",
                                   toxicity=1,
                                   attributes=attributes)
        ProxyHelper.logger.debug("Disabled connectivity to " + proxy_info.proxy.name)

    @staticmethod
    def enable_all_connectivity():
        for p in TestEnvironment.get_current().get_proxy_infos():
            ProxyHelper._enable_proxy_connectivity(p)

    @staticmethod
    def enable_connectivity(instance_name: str):
        proxy_info: ProxyInfo = TestEnvironment.get_current().get_proxy_info(
            instance_name)
        ProxyHelper._enable_proxy_connectivity(proxy_info)

    @staticmethod
    def _enable_proxy_connectivity(proxy_info: ProxyInfo):

        APIConsumer.host = proxy_info.control_host
        APIConsumer.port = proxy_info.control_port

        try:
            toxics = proxy_info.proxy.toxics()
        except NotFound:
            toxics = dict()

        down_stream = toxics.get("DOWN-STREAM")
        if down_stream is not None:
            proxy_info.proxy.destroy_toxic("DOWN-STREAM")
        up_stream = toxics.get("UP-STREAM")
        if up_stream is not None:
            proxy_info.proxy.destroy_toxic("UP-STREAM")

        ProxyHelper.logger.debug("Enabled connectivity to " + proxy_info.proxy.name)
