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

"""Unit tests for the async psycopg driver dialect's connect-info prep."""

from __future__ import annotations

from aws_advanced_python_wrapper.aio.driver_dialect.psycopg import \
    AsyncPsycopgDriverDialect
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import Properties


def test_prepare_connect_info_sets_host_port():
    d = AsyncPsycopgDriverDialect()
    prepared = d.prepare_connect_info(HostInfo("h", 5432), Properties({"user": "u"}))
    assert prepared["host"] == "h"
    assert prepared["port"] == "5432"
    assert prepared["user"] == "u"


def test_prepare_connect_info_propagates_connect_timeout():
    # Regression: the base prepare_connect_info strips connect_timeout via
    # remove_wrapper_props; the async psycopg dialect must re-add it (parity with
    # the sync PgDriverDialect and the async aiomysql dialect). Without it an
    # async connect to a down host hangs at the OS TCP timeout (~2 min) instead
    # of the configured bound -- burning the failover deadline on multi-instance
    # clusters (test_writer_failover_in_idle_connections_async).
    d = AsyncPsycopgDriverDialect()
    props = Properties({"connect_timeout": "10", "user": "u", "password": "p"})
    prepared = d.prepare_connect_info(HostInfo("h", 5432), props)
    assert "connect_timeout" in prepared
    assert int(prepared["connect_timeout"]) == 10


def test_prepare_connect_info_omits_connect_timeout_when_unset():
    d = AsyncPsycopgDriverDialect()
    prepared = d.prepare_connect_info(HostInfo("h", 5432), Properties({"user": "u"}))
    assert "connect_timeout" not in prepared


def test_prepare_connect_info_propagates_tcp_keepalive():
    d = AsyncPsycopgDriverDialect()
    props = Properties({
        "tcp_keepalive": "True",
        "tcp_keepalive_time": "30",
        "tcp_keepalive_interval": "5",
        "tcp_keepalive_probes": "3",
    })
    prepared = d.prepare_connect_info(HostInfo("h", 5432), props)
    assert prepared.get("keepalives") is not None
    assert int(prepared["keepalives_idle"]) == 30
    assert int(prepared["keepalives_interval"]) == 5
    assert int(prepared["keepalives_count"]) == 3


def test_prepare_connect_info_maps_database_to_dbname():
    """The wrapper-level ``database`` prop (URL path / database= kwarg) must
    reach psycopg as ``dbname`` (sync PgDriverDialect parity) -- previously it
    was stripped and connects landed on the default database."""
    d = AsyncPsycopgDriverDialect()
    prepared = d.prepare_connect_info(
        HostInfo("h", 5432), Properties({"user": "u", "database": "mydb"}))
    assert prepared.get("dbname") == "mydb"
    assert "database" not in prepared


def test_prepare_connect_info_keeps_explicit_dbname():
    """A libpq-style ``dbname=`` prop passes through untouched."""
    d = AsyncPsycopgDriverDialect()
    prepared = d.prepare_connect_info(
        HostInfo("h", 5432), Properties({"user": "u", "dbname": "libpqdb"}))
    assert prepared.get("dbname") == "libpqdb"
