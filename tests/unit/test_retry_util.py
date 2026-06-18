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

"""Unit tests for the (sync) RetryUtil failover connection helper."""

import time
from unittest.mock import MagicMock

from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.retry_util import RetryUtil

_W = HostInfo(host="writer.example.com", port=5432, role=HostRole.WRITER)
_R1 = HostInfo(host="r1.example.com", port=5432, role=HostRole.READER)


def _svc(*, all_hosts, role=HostRole.WRITER, conn=None):
    """Build a MagicMock sync plugin_service for the retry helper."""
    svc = MagicMock()
    svc.all_hosts = tuple(all_hosts)
    svc.hosts = list(all_hosts)
    svc.filter_hosts = MagicMock(side_effect=lambda hosts: list(hosts))
    svc.refresh_host_list = MagicMock()
    svc.force_refresh_host_list = MagicMock()
    svc.get_host_info_by_strategy = MagicMock(
        side_effect=lambda role_, strat, hosts: hosts[0] if hosts else None)
    svc.get_host_role = MagicMock(return_value=role)
    c = conn or MagicMock(name="new_conn")
    svc.connect = MagicMock(return_value=c)
    svc.driver_dialect = MagicMock()
    return svc, c


def _deadline(seconds=0.5):
    return time.time() + seconds


def test_get_allowed_connection_falls_back_to_writer_when_no_reader():
    # Regression (#1246 GDB live-run bug): the
    # *_OR_WRITER failover modes pass verify_role=None with an allowed list that
    # includes the writer. Right after a writer failover the newly elected
    # writer can be the only reachable host. The host selector requires a
    # concrete role, so the helper must try READER then fall back to WRITER. The
    # old code asked only for READER, so it could never select the writer and
    # timed out with UnableToConnectToReader even though the writer was valid.
    svc, conn = _svc(all_hosts=[_W], role=HostRole.WRITER)

    def _role_selector(role_, strat, hosts):
        matches = [h for h in hosts if h.role == role_]
        if not matches:
            raise Exception("strategy can't get a host of the requested role")
        return matches[0]
    svc.get_host_info_by_strategy = MagicMock(side_effect=_role_selector)

    util = RetryUtil()
    result = util.get_allowed_connection(
        svc, MagicMock(), object(),
        lambda: [_W], "random", None, _deadline())

    assert result.connection is conn
    assert result.host_info.host == _W.host
    roles_asked = [call.args[0] for call in svc.get_host_info_by_strategy.call_args_list]
    assert HostRole.WRITER in roles_asked  # the fix's writer fallback fired


def test_get_allowed_connection_selects_reader_when_available():
    # When a reader IS reachable, the reader-first preference still holds
    # (read-offload): a *_OR_WRITER mode should pick the reader, not the writer.
    svc, conn = _svc(all_hosts=[_R1], role=HostRole.READER)

    def _role_selector(role_, strat, hosts):
        matches = [h for h in hosts if h.role == role_]
        if not matches:
            raise Exception("no host for role")
        return matches[0]
    svc.get_host_info_by_strategy = MagicMock(side_effect=_role_selector)

    util = RetryUtil()
    result = util.get_allowed_connection(
        svc, MagicMock(), object(),
        lambda: [_R1], "random", None, _deadline())

    assert result.connection is conn
    assert result.host_info.host == _R1.host
    assert svc.get_host_info_by_strategy.call_args_list[0].args[0] == HostRole.READER
