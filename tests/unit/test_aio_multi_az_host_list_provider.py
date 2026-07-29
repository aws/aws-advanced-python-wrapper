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

"""Phase H deferred P1 #13: ``AsyncMultiAzHostListProvider`` unit tests.

Validates the two-step MultiAz query pattern (writer_host_query =>
topology_query), instance-template substitution, empty-writer fallback,
and that the cache/TTL scaffolding inherited from
:class:`AsyncAuroraHostListProvider` still applies.
"""

from __future__ import annotations

import asyncio
from typing import Any, List, Optional
from unittest.mock import AsyncMock, MagicMock

from aws_advanced_python_wrapper.aio.host_list_provider import \
    AsyncMultiAzHostListProvider
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.utils.properties import Properties

WRITER_HOST_QUERY = "SELECT id FROM writer_role"
TOPOLOGY_QUERY = "SELECT id, host, port FROM topology"
HOST_ID_QUERY = "SELECT @@server_id"


# ---- Helpers ------------------------------------------------------------


def _props(**kwargs: Any) -> Properties:
    base = {"host": "cluster-xyz.example.com", "port": "5432"}
    base.update({k: str(v) for k, v in kwargs.items()})
    return Properties(base)


def _build_cursor(
        writer_row: Optional[tuple],
        topology_rows: List[tuple],
        host_id_row: Optional[tuple] = None,
) -> MagicMock:
    """Builds a cursor whose fetchone/fetchall serve the right rows for
    each step. We rely on execute-call ordering:
      execute #1 -> writer_host_query -> fetchone returns writer_row
      execute #2 -> host_id_query (only if writer_row is None) -> fetchone returns host_id_row
      final execute -> topology_query -> fetchall returns topology_rows
    """
    cur = MagicMock()
    # A stateful queue of fetchone responses: writer_row first, then
    # (optionally) host_id_row. We track how many fetchones have been
    # served.
    fetchone_queue: List[Optional[tuple]] = [writer_row]
    if writer_row is None and host_id_row is not None:
        fetchone_queue.append(host_id_row)

    fetchone_idx = [0]

    async def _fetchone() -> Optional[tuple]:
        idx = fetchone_idx[0]
        fetchone_idx[0] += 1
        if idx < len(fetchone_queue):
            return fetchone_queue[idx]
        return None

    cur.execute = AsyncMock()
    cur.fetchone = AsyncMock(side_effect=_fetchone)
    cur.fetchall = AsyncMock(return_value=topology_rows)
    cur.close = AsyncMock()
    cur.__aenter__ = AsyncMock(return_value=cur)
    cur.__aexit__ = AsyncMock(return_value=None)
    return cur


def _build_conn(
        writer_row: Optional[tuple],
        topology_rows: List[tuple],
        host_id_row: Optional[tuple] = None,
) -> MagicMock:
    conn = MagicMock()
    conn.cursor = MagicMock(
        return_value=_build_cursor(writer_row, topology_rows, host_id_row)
    )
    return conn


def _make_provider(**overrides: Any) -> AsyncMultiAzHostListProvider:
    kwargs: dict = {
        "props": _props(),
        "driver_dialect": MagicMock(),
        "writer_host_query": WRITER_HOST_QUERY,
        "topology_query": TOPOLOGY_QUERY,
        "host_id_query": HOST_ID_QUERY,
    }
    kwargs.update(overrides)
    return AsyncMultiAzHostListProvider(**kwargs)


def _conn_and_provider(conn_kwargs: dict, **prov_overrides: Any):
    """Return ``(app_conn, provider)`` where the provider's background topology
    monitor gets its OWN dedicated connection -- a fresh ``_build_conn`` with the
    same canned rows, mirroring production's dedicated monitoring connection.

    Without it the background monitor queries (and drains) the app connection's
    stateful mock cursor, racing the foreground probe -- deterministically empty
    on Python 3.10, where the scheduler runs the background task first.
    """
    async def _monitor_factory() -> MagicMock:
        return _build_conn(**conn_kwargs)
    prov = _make_provider(monitor_connection_factory=_monitor_factory,
                          **prov_overrides)
    return _build_conn(**conn_kwargs), prov


# ---- Basic two-step query ------------------------------------------------


def test_basic_topology_two_step_query():
    """writer_host_query returns writer_id; topology_query returns the
    full list; roles are assigned by id == writer_id."""
    async def _body() -> None:
        writer_id = "db-WRITER"
        reader_id = "db-READER"
        conn, prov = _conn_and_provider(dict(
            writer_row=(writer_id,),
            topology_rows=[
                (writer_id, "host1.example.com", 5432),
                (reader_id, "host2.example.com", 5432),
            ],
        ))
        topo = await prov.force_refresh(conn)
        assert len(topo) == 2

        writers = [h for h in topo if h.role == HostRole.WRITER]
        readers = [h for h in topo if h.role == HostRole.READER]
        assert len(writers) == 1 and len(readers) == 1
        assert writers[0].host == "host1.example.com"
        assert writers[0].port == 5432
        assert writers[0].host_id == writer_id
        assert readers[0].host == "host2.example.com"
        assert readers[0].host_id == reader_id
        # Writer first.
        assert topo[0].role == HostRole.WRITER

    asyncio.run(_body())


def test_writer_host_column_index_reads_mysql_replica_status_column():
    """MultiAz MySQL identifies the writer from column 39 of
    ``SHOW REPLICA STATUS`` (sync ``MultiAzClusterMysqlDialect.
    _WRITER_HOST_COLUMN_INDEX``); the provider must honor the configured
    column index instead of hardcoding column 0."""
    async def _body() -> None:
        writer_id = "db-WRITER"
        reader_id = "db-READER"
        # 40-column row; the writer host lives at index 39.
        replica_status_row = tuple(["filler"] * 39 + [writer_id])
        conn, prov = _conn_and_provider(
            dict(
                writer_row=replica_status_row,
                topology_rows=[
                    (writer_id, "host1.example.com", 3306),
                    (reader_id, "host2.example.com", 3306),
                ],
            ),
            writer_host_column_index=39,
        )
        topo = await prov.force_refresh(conn)
        writers = [h for h in topo if h.role == HostRole.WRITER]
        assert len(writers) == 1
        assert writers[0].host_id == writer_id

    asyncio.run(_body())


# ---- MySQL fallback: empty writer_host_query -----------------------------


def test_empty_writer_host_query_falls_back_to_host_id_query():
    """MySQL case: writer_host_query returns empty when we're connected
    to the writer. We must fall back to host_id_query for our own ID.
    """
    async def _body() -> None:
        writer_id = "0123456789"
        other_id = "9876543210"
        conn, prov = _conn_and_provider(dict(
            writer_row=None,  # empty
            host_id_row=(writer_id,),
            topology_rows=[
                (writer_id, "mysql-writer.example.com", 3306),
                (other_id, "mysql-reader.example.com", 3306),
            ],
        ))
        topo = await prov.force_refresh(conn)
        assert len(topo) == 2
        writers = [h for h in topo if h.role == HostRole.WRITER]
        assert len(writers) == 1
        assert writers[0].host == "mysql-writer.example.com"

    asyncio.run(_body())


def test_empty_writer_and_empty_host_id_yields_empty():
    """Both queries return no row -> provider returns empty topology
    (no cache update)."""
    async def _body() -> None:
        conn = _build_conn(
            writer_row=None,
            host_id_row=None,
            topology_rows=[],
        )
        prov = _make_provider()
        topo = await prov.force_refresh(conn)
        assert topo == ()

    asyncio.run(_body())


# ---- No writer in topology rows ------------------------------------------


def test_no_writer_in_topology_returns_empty():
    """If the topology query returns only non-matching IDs, there's no
    writer -> empty tuple."""
    async def _body() -> None:
        conn = _build_conn(
            writer_row=("db-WRITER",),
            topology_rows=[
                ("db-OTHER-1", "h1.example.com", 5432),
                ("db-OTHER-2", "h2.example.com", 5432),
            ],
        )
        prov = _make_provider()
        topo = await prov.force_refresh(conn)
        assert topo == ()

    asyncio.run(_body())


# ---- Instance-template substitution --------------------------------------


def test_instance_template_substitution_rewrites_hosts():
    """``instance_template_host`` with ``?`` is replaced by the extracted
    instance ID from the row's host.

    :meth:`RdsUtils.get_instance_id` only extracts the leaf when the DNS
    is the *instance* pattern (no ``cluster-`` etc. dns group), e.g.
    ``postgres-instance-1.XYZ.us-east-1.rds.amazonaws.com`` ->
    ``postgres-instance-1``.
    """
    async def _body() -> None:
        writer_id = "db-WRITER"
        reader_id = "db-READER"
        writer_host = (
            "postgres-instance-1.XYZ.us-east-1.rds.amazonaws.com"
        )
        reader_host = (
            "postgres-instance-2.XYZ.us-east-1.rds.amazonaws.com"
        )
        conn, prov = _conn_and_provider(dict(
            writer_row=(writer_id,),
            topology_rows=[
                (writer_id, writer_host, 5432),
                (reader_id, reader_host, 5432),
            ],
        ), instance_template_host="?.internal.example.com:6000")
        topo = await prov.force_refresh(conn)
        assert len(topo) == 2
        writers = [h for h in topo if h.role == HostRole.WRITER]
        readers = [h for h in topo if h.role == HostRole.READER]
        assert writers[0].host == "postgres-instance-1.internal.example.com"
        assert writers[0].port == 6000
        assert readers[0].host == "postgres-instance-2.internal.example.com"
        assert readers[0].port == 6000

    asyncio.run(_body())


def test_instance_template_without_port_keeps_row_port():
    """``instance_template_host`` without ``:port`` only rewrites the
    host; the port from the topology row is preserved."""
    async def _body() -> None:
        writer_id = "db-WRITER"
        writer_host = (
            "postgres-instance-1.XYZ.us-east-1.rds.amazonaws.com"
        )
        conn, prov = _conn_and_provider(dict(
            writer_row=(writer_id,),
            topology_rows=[(writer_id, writer_host, 5433)],
        ), instance_template_host="?.internal.example.com")
        topo = await prov.force_refresh(conn)
        assert len(topo) == 1
        assert topo[0].host == "postgres-instance-1.internal.example.com"
        assert topo[0].port == 5433

    asyncio.run(_body())


# ---- Caching (TTL scaffolding inherited from Aurora provider) ------------


def test_refresh_uses_cache_within_ttl():
    """Second ``refresh()`` within the TTL returns the cached topology
    without re-querying."""
    async def _body() -> None:
        writer_id = "db-WRITER"
        conn, prov = _conn_and_provider(dict(
            writer_row=(writer_id,),
            topology_rows=[(writer_id, "h1.example.com", 5432)],
        ), props=_props(topology_refresh_ms="60000"))
        first = await prov.refresh(conn)
        # Replace the cursor with one that would return different rows.
        # If the cache is honored, we should not observe the change.
        conn.cursor = MagicMock(return_value=_build_cursor(
            writer_row=("db-OTHER",),
            topology_rows=[("db-OTHER", "h2.example.com", 5432)],
        ))
        second = await prov.refresh(conn)
        assert first == second

    asyncio.run(_body())


def test_force_refresh_bypasses_cache():
    """``force_refresh`` always re-queries."""
    async def _body() -> None:
        conn = _build_conn(
            writer_row=("db-W1",),
            topology_rows=[("db-W1", "h1.example.com", 5432)],
        )
        prov = _make_provider()
        first = await prov.force_refresh(conn)

        conn.cursor = MagicMock(return_value=_build_cursor(
            writer_row=("db-W2",),
            topology_rows=[("db-W2", "h2.example.com", 5432)],
        ))
        second = await prov.force_refresh(conn)
        assert first != second
        assert second[0].host == "h2.example.com"

    asyncio.run(_body())


# ---- Cluster-id derivation (inherited) -----------------------------------


def test_explicit_cluster_id_respected():
    prov = _make_provider(
        props=_props(cluster_id="my-multi-az-cluster"),
    )
    assert prov.get_cluster_id() == "my-multi-az-cluster"


def test_default_cluster_id_uses_host_port():
    prov = _make_provider()
    cid = prov.get_cluster_id()
    assert "aurora:" in cid  # derived helper is shared
    assert "cluster-xyz.example.com" in cid


# ---- Query failure tolerance --------------------------------------------


def test_query_failure_yields_empty_topology():
    """An exception during the two-step query produces an empty tuple
    rather than raising, so callers fall back to the cache."""
    async def _body() -> None:
        conn = MagicMock()
        cur = _build_cursor(
            writer_row=("db-W",),
            topology_rows=[],
        )
        cur.execute = AsyncMock(side_effect=RuntimeError("boom"))
        conn.cursor = MagicMock(return_value=cur)
        prov = _make_provider()
        topo = await prov.force_refresh(conn)
        assert topo == ()

    asyncio.run(_body())
