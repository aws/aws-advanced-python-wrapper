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

"""Unit tests for the async Global-Database read/write splitting plugin."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.aio.gdb_read_write_splitting_plugin import \
    AsyncGdbReadWriteSplittingPlugin
from aws_advanced_python_wrapper.errors import ReadWriteSplittingError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties

_R1_EAST = "r1.cvqiy8w244sz.us-east-1.rds.amazonaws.com"
_R2_WEST = "r2.cvqiy8w244sz.us-west-2.rds.amazonaws.com"
_W_EAST = "w.cvqiy8w244sz.us-east-1.rds.amazonaws.com"
_W_WEST = "w.cvqiy8w244sz.us-west-2.rds.amazonaws.com"


def _make_plugin(*, home_region="us-east-1", overrides=None):
    props = Properties({"host": _W_EAST, "port": "5432"})
    if home_region is not None:
        props["gdb_rw_home_region"] = home_region
    if overrides:
        props.update(overrides)
    plugin = AsyncGdbReadWriteSplittingPlugin(MagicMock(), MagicMock(), props)
    plugin._init_settings(HostInfo(host=props["host"], port=5432), props)
    return plugin


def test_init_settings_resolves_home_region_from_prop():
    plugin = _make_plugin(home_region="us-east-1")
    assert plugin._home_region == "us-east-1"


def test_init_settings_resolves_home_region_from_host_when_prop_absent():
    # No explicit prop -> derive the region from the connection host.
    props = Properties({"host": _W_WEST, "port": "5432"})
    plugin = AsyncGdbReadWriteSplittingPlugin(MagicMock(), MagicMock(), props)
    plugin._init_settings(HostInfo(host=_W_WEST, port=5432), props)
    assert plugin._home_region == "us-west-2"


def test_init_settings_raises_when_no_home_region():
    props = Properties({"host": "localhost", "port": "5432"})
    plugin = AsyncGdbReadWriteSplittingPlugin(MagicMock(), MagicMock(), props)
    with pytest.raises(ReadWriteSplittingError):
        plugin._init_settings(HostInfo(host="localhost", port=5432), props)


def test_select_reader_candidates_filters_to_home_region():
    plugin = _make_plugin(home_region="us-east-1")
    topology = (
        HostInfo(host=_R1_EAST, port=5432, role=HostRole.READER),
        HostInfo(host=_R2_WEST, port=5432, role=HostRole.READER),
        HostInfo(host=_W_EAST, port=5432, role=HostRole.WRITER),
    )
    readers = plugin._select_reader_candidates(topology)
    assert [r.host for r in readers] == [_R1_EAST]


def test_select_reader_candidates_unrestricted_returns_all_readers():
    plugin = _make_plugin(
        home_region="us-east-1",
        overrides={"gdb_rw_restrict_reader_to_home_region": "False"})
    topology = (
        HostInfo(host=_R1_EAST, port=5432, role=HostRole.READER),
        HostInfo(host=_R2_WEST, port=5432, role=HostRole.READER),
    )
    readers = plugin._select_reader_candidates(topology)
    assert {r.host for r in readers} == {_R1_EAST, _R2_WEST}


def test_select_reader_candidates_raises_when_none_in_region():
    plugin = _make_plugin(home_region="eu-west-1")
    topology = (HostInfo(host=_R1_EAST, port=5432, role=HostRole.READER),)
    with pytest.raises(ReadWriteSplittingError):
        plugin._select_reader_candidates(topology)


def test_should_switch_to_writer_in_region_true():
    plugin = _make_plugin(home_region="us-east-1")
    writer = HostInfo(host=_W_EAST, port=5432, role=HostRole.WRITER)
    assert asyncio.run(plugin._should_switch_to_writer(writer)) is True


def test_should_switch_to_writer_outside_region_raises_without_gwf():
    plugin = _make_plugin(home_region="us-east-1")
    writer = HostInfo(host=_W_WEST, port=5432, role=HostRole.WRITER)
    with pytest.raises(ReadWriteSplittingError):
        asyncio.run(plugin._should_switch_to_writer(writer))


def test_should_switch_to_writer_outside_region_gwf_skips_switch():
    # Global write forwarding -> keep the current reader (return False), don't
    # raise, even though the writer is outside the home region.
    plugin = _make_plugin(
        home_region="us-east-1",
        overrides={"gdb_enable_global_write_forwarding": "True"})
    writer = HostInfo(host=_W_WEST, port=5432, role=HostRole.WRITER)
    assert asyncio.run(plugin._should_switch_to_writer(writer)) is False


def test_should_switch_to_writer_unrestricted_writer_true():
    # restrict_writer disabled -> always switch, even cross-region.
    plugin = _make_plugin(
        home_region="us-east-1",
        overrides={"gdb_rw_restrict_writer_to_home_region": "False"})
    writer = HostInfo(host=_W_WEST, port=5432, role=HostRole.WRITER)
    assert asyncio.run(plugin._should_switch_to_writer(writer)) is True
