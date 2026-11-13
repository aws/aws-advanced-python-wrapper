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

from typing import Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest

from aws_advanced_python_wrapper.blue_green_plugin import (
    BlueGreenPhase, BlueGreenRole, BlueGreenStatus, SubstituteConnectRouting,
    SuspendUntilCorrespondingHostFoundConnectRouting)
from aws_advanced_python_wrapper.federated_plugin import FederatedAuthPlugin
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.okta_plugin import OktaAuthPlugin
from aws_advanced_python_wrapper.utils.concurrent import ConcurrentDict
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

BG_ID = "1"
PORT = 3306
BLUE_HOST = "database-1.cluster-c1a2b3c4d5e6.us-east-1.rds.amazonaws.com"
GREEN_HOST = "database-1-green-abc123.cluster-c1a2b3c4d5e6.us-east-1.rds.amazonaws.com"
GREEN_IP = "10.0.1.20"


def test_blue_green_log_message_keys_resolve():
    """Regression (parity review): both Blue/Green keys referenced by logger
    calls exist in the bundle. The formatted
    WaitConnectUntilCorrespondingHostFound call previously raised
    NotInResourceBundleError whenever DEBUG logging was enabled during a
    switchover (Logger's formatted path has no missing-key guard)."""
    formatted = Messages.get_formatted(
        "SuspendConnectRouting.WaitConnectUntilCorrespondingHostFound", "host-1")
    assert "host-1" in formatted
    assert Messages.get("BlueGreenStatusProvider.AllGreenHostsChangedName")


@pytest.mark.parametrize("auth_plugin", [FederatedAuthPlugin, OktaAuthPlugin])
def test_substitute_connect_routing_to_an_ip_sets_the_iam_host_for_federated_and_okta_auth(auth_plugin):
    """The ADFS and Okta plugins get an IAM token for the host they connect to and reject an IP, so a connect
    substituted by an IP sets the IAM host for them as it does for the IAM plugin."""
    plugin_service = MagicMock()
    plugin_service.is_plugin_in_use.side_effect = lambda plugin_class: plugin_class is auth_plugin
    routing = SubstituteConnectRouting(
        HostInfo(GREEN_IP, PORT), BLUE_HOST, BlueGreenRole.SOURCE, (HostInfo(GREEN_HOST, PORT),))

    routing.apply(MagicMock(), HostInfo(BLUE_HOST, PORT), Properties(), True, MagicMock(), plugin_service)

    rerouted_props = plugin_service.connect.call_args.args[1]
    assert WrapperProperties.IAM_HOST.get(rerouted_props) == GREEN_HOST


def _corresponding_hosts() -> ConcurrentDict[str, Tuple[HostInfo, Optional[HostInfo]]]:
    corresponding_hosts: ConcurrentDict[str, Tuple[HostInfo, Optional[HostInfo]]] = ConcurrentDict()
    corresponding_hosts.put_all({BLUE_HOST: (HostInfo(BLUE_HOST, PORT), HostInfo(GREEN_HOST, PORT))})
    return corresponding_hosts


def test_suspend_until_corresponding_host_found_does_not_wait_for_a_known_host_given_in_upper_case():
    """The corresponding hosts are keyed by lower-case host name, so a host given in upper case whose
    corresponding host is already known continues at once."""
    plugin_service = MagicMock()
    plugin_service.get_status.return_value = BlueGreenStatus(
        BG_ID, BlueGreenPhase.POST, corresponding_hosts=_corresponding_hosts())
    props = Properties({WrapperProperties.BG_CONNECT_TIMEOUT_MS.name: 500})
    routing = SuspendUntilCorrespondingHostFoundConnectRouting(BLUE_HOST, BlueGreenRole.SOURCE, BG_ID)

    with patch.object(routing, "delay", wraps=routing.delay) as delay:
        assert routing.apply(MagicMock(), HostInfo(BLUE_HOST.upper(), PORT), props, True, MagicMock(), plugin_service) is None
    delay.assert_not_called()


def test_suspend_until_corresponding_host_found_continues_when_a_host_given_in_upper_case_is_found():
    """A host given in upper case continues once a later status has its corresponding host, instead of waiting
    out bg_connect_timeout_ms."""
    found = BlueGreenStatus(BG_ID, BlueGreenPhase.POST, corresponding_hosts=_corresponding_hosts())
    statuses = iter([BlueGreenStatus(BG_ID, BlueGreenPhase.POST)])
    plugin_service = MagicMock()
    plugin_service.get_status.side_effect = lambda *args: next(statuses, found)
    props = Properties({WrapperProperties.BG_CONNECT_TIMEOUT_MS.name: 500})
    routing = SuspendUntilCorrespondingHostFoundConnectRouting(BLUE_HOST, BlueGreenRole.SOURCE, BG_ID)

    assert routing.apply(MagicMock(), HostInfo(BLUE_HOST.upper(), PORT), props, True, MagicMock(), plugin_service) is None
