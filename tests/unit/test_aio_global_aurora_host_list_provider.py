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

"""``AsyncGlobalAuroraHostListProvider`` unit tests.

Validates region-template substitution, missing-region fallback,
port-parsing, writer/reader role assignment from the sync-parity
``(server_id, is_writer, lag, aws_region)`` row shape (the real
Global Aurora dialect topology-query columns), and the
``GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS`` parser (sync-parity
``region:host:port`` format).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.aio.host_list_provider import \
    AsyncGlobalAuroraHostListProvider
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.utils.properties import Properties

# Matches the real GlobalAuroraMysqlDialect / GlobalAuroraPgDialect column
# order: SERVER_ID, is_writer, VISIBILITY_LAG_IN_MSEC, AWS_REGION.
TOPOLOGY_QUERY = (
    "SELECT SERVER_ID, SESSION_ID = 'MASTER_SESSION_ID' AS is_writer, "
    "VISIBILITY_LAG_IN_MSEC, AWS_REGION FROM global_topology"
)


def _props(**kwargs: Any) -> Properties:
    base = {"host": "cluster-xyz.example.com", "port": "5432"}
    base.update({k: str(v) for k, v in kwargs.items()})
    return Properties(base)


def _make_provider(**overrides: Any) -> AsyncGlobalAuroraHostListProvider:
    kwargs: dict = {
        "props": _props(),
        "driver_dialect": MagicMock(),
        "topology_query": TOPOLOGY_QUERY,
    }
    kwargs.update(overrides)
    return AsyncGlobalAuroraHostListProvider(**kwargs)


# ---- _parse_templates ----------------------------------------------------


def test_parse_templates_colon_format_pairs():
    """``region:host:port,region:host`` (the sync-documented
    ``global_cluster_instance_host_patterns`` format) -> dict of entries,
    whitespace trimmed; a port-less template gets no ``:port`` suffix."""
    raw = (
        "us-east-1:?.xyz.us-east-1.rds.amazonaws.com:5432,"
        " us-west-2 : ?.abc.us-west-2.rds.amazonaws.com , "
    )
    parsed = AsyncGlobalAuroraHostListProvider._parse_templates(raw)
    assert parsed == {
        "us-east-1": "?.xyz.us-east-1.rds.amazonaws.com:5432",
        "us-west-2": "?.abc.us-west-2.rds.amazonaws.com",
    }


def test_parse_templates_malformed_entry_raises():
    """An entry without a host part raises, matching sync
    ``GlobalAuroraTopologyUtils.parse_instance_templates``."""
    with pytest.raises(AwsWrapperError):
        AsyncGlobalAuroraHostListProvider._parse_templates(
            "us-east-1:?.xyz.us-east-1.rds.amazonaws.com:5432,bad-entry-no-colon")


# ---- _rows_to_topology ---------------------------------------------------


def test_empty_templates_returns_empty_topology():
    """No templates configured -> every row is skipped -> empty topology."""
    prov = _make_provider(instance_templates_by_region={})
    rows = [
        ("instance-1", True, 0.0, "us-east-1"),
        ("instance-2", False, 12.5, "us-west-2"),
    ]
    assert prov._rows_to_topology(rows) == ()


def test_region_without_matching_template_skipped():
    """Row referencing a region with no configured template is dropped.

    With only ``us-east-1`` configured, the ``us-west-2`` reader row is
    discarded and we return a topology of just the writer.
    """
    prov = _make_provider(
        instance_templates_by_region={
            "us-east-1": "?.xyz.us-east-1.rds.amazonaws.com:5432",
        },
    )
    rows = [
        ("instance-1", True, 0.0, "us-east-1"),
        ("instance-2", False, 12.5, "us-west-2"),  # skipped
    ]
    topo = prov._rows_to_topology(rows)
    assert len(topo) == 1
    assert topo[0].host == "instance-1.xyz.us-east-1.rds.amazonaws.com"
    assert topo[0].port == 5432
    assert topo[0].role == HostRole.WRITER


def test_template_substitution_with_port():
    """``?.host:PORT`` template -> host has ``?`` replaced, port parsed."""
    prov = _make_provider(
        instance_templates_by_region={
            "us-east-1": "?.xyz.us-east-1.rds.amazonaws.com:6000",
        },
    )
    rows = [("my-instance", True, 0.0, "us-east-1")]
    topo = prov._rows_to_topology(rows)
    assert len(topo) == 1
    assert topo[0].host == "my-instance.xyz.us-east-1.rds.amazonaws.com"
    assert topo[0].port == 6000
    assert topo[0].host_id == "my-instance"


def test_template_substitution_without_port_uses_default_port():
    """Template without ``:port`` -> port falls back to provider default."""
    prov = _make_provider(
        default_port=5433,
        instance_templates_by_region={
            "us-east-1": "?.xyz.us-east-1.rds.amazonaws.com",
        },
    )
    rows = [("my-instance", True, 0.0, "us-east-1")]
    topo = prov._rows_to_topology(rows)
    assert len(topo) == 1
    assert topo[0].host == "my-instance.xyz.us-east-1.rds.amazonaws.com"
    assert topo[0].port == 5433


def test_writer_and_reader_roles_assigned_from_is_writer_column():
    """``is_writer`` (column 1) True -> WRITER role; False -> READER
    role; writer is sorted first; lag column (2) is ignored."""
    prov = _make_provider(
        instance_templates_by_region={
            "us-east-1": "?.xyz.us-east-1.rds.amazonaws.com:5432",
            "us-west-2": "?.abc.us-west-2.rds.amazonaws.com:5432",
        },
    )
    rows = [
        ("reader-a", False, 55.0, "us-west-2"),
        ("writer-a", True, 0.0, "us-east-1"),
        ("reader-b", False, 31.0, "us-west-2"),
    ]
    topo = prov._rows_to_topology(rows)
    assert len(topo) == 3
    # Writer first.
    assert topo[0].role == HostRole.WRITER
    assert topo[0].host_id == "writer-a"
    # Readers after.
    reader_ids = {h.host_id for h in topo if h.role == HostRole.READER}
    assert reader_ids == {"reader-a", "reader-b"}


def test_short_rows_are_skipped():
    """A row with fewer than the 4 sync-parity columns is skipped rather
    than mis-parsed (guards against a wrong/legacy query shape)."""
    prov = _make_provider(
        instance_templates_by_region={
            "us-east-1": "?.xyz.us-east-1.rds.amazonaws.com:5432",
        },
    )
    rows = [
        ("legacy-row", "us-east-1", True),  # old 3-col shape: skipped
        ("writer-a", True, 0.0, "us-east-1"),
    ]
    topo = prov._rows_to_topology(rows)
    assert [h.host_id for h in topo] == ["writer-a"]


def test_rows_without_writer_return_empty_tuple():
    """No writer among configured regions -> empty tuple (callers fall
    back to cache)."""
    prov = _make_provider(
        instance_templates_by_region={
            "us-east-1": "?.xyz.us-east-1.rds.amazonaws.com:5432",
        },
    )
    rows = [
        ("reader-a", False, 10.0, "us-east-1"),
        ("reader-b", False, 20.0, "us-east-1"),
    ]
    assert prov._rows_to_topology(rows) == ()


# ---- Property-based construction ----------------------------------------


def test_construction_from_property_parses_patterns():
    """When ``instance_templates_by_region`` is omitted, the provider
    reads ``GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS`` from props (sync
    ``region:host:port`` format) and parses it. Subsequent
    ``_rows_to_topology`` calls use that map."""
    patterns = (
        "us-east-1:?.xyz.us-east-1.rds.amazonaws.com:5432,"
        "us-west-2:?.abc.us-west-2.rds.amazonaws.com:5432"
    )
    prov = _make_provider(
        props=_props(global_cluster_instance_host_patterns=patterns),
    )
    rows = [
        ("instance-1", True, 0.0, "us-east-1"),
        ("instance-2", False, 15.0, "us-west-2"),
    ]
    topo = prov._rows_to_topology(rows)
    assert len(topo) == 2
    assert topo[0].role == HostRole.WRITER
    assert topo[0].host == "instance-1.xyz.us-east-1.rds.amazonaws.com"
    assert topo[1].host == "instance-2.abc.us-west-2.rds.amazonaws.com"
