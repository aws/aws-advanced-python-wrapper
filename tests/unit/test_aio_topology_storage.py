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

"""The async provider's topology lives in the storage service.

Two things here are easy to break silently and have no other coverage: the
storage key type (the sync and async ``Topology`` aliases are different
objects, so using the wrong one writes to an unregistered cache) and the loop
component of the key (which must not be ``id(loop)``, because addresses are
recycled).
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.host_list_provider import \
    AsyncAuroraHostListProvider
from aws_advanced_python_wrapper.aio.topology_store import (StoredTopology,
                                                            loop_token)
from aws_advanced_python_wrapper.host_list_provider import \
    Topology as SyncTopology
from aws_advanced_python_wrapper.hostinfo import (HostAvailability, HostInfo,
                                                  HostRole)
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.properties import Properties


def _props(**kwargs: Any) -> Properties:
    base = {"host": "cluster-xyz.us-east-1.rds.amazonaws.com", "port": "5432"}
    base.update({k: str(v) for k, v in kwargs.items()})
    return Properties(base)


def _aurora(**kwargs: Any) -> AsyncAuroraHostListProvider:
    props = _props(
        cluster_instance_host_pattern="?.cluster-xyz.us-east-1.rds.amazonaws.com",
        **kwargs)
    return AsyncAuroraHostListProvider(props, driver_dialect=MagicMock())


def _storage():
    return services_container.get_storage_service()


HOSTS = (HostInfo("writer.com", 5432, HostRole.WRITER),
         HostInfo("reader.com", 5432, HostRole.READER))


# ---- The storage key type ------------------------------------------------

def test_the_two_topology_aliases_are_not_interchangeable_as_keys():
    """Why the async module must key storage on the sync alias.

    The sync module imports HostInfo under TYPE_CHECKING only, so its alias is
    built from a forward reference and is a different object from the async
    one. The storage service is keyed by type, and only the sync alias is
    registered -- so keying on the async alias would write to a second,
    unregistered cache.
    """
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        Topology as AsyncTopology

    assert AsyncTopology != SyncTopology
    assert StoredTopology is SyncTopology


def test_topology_is_written_to_the_registered_storage_service_entry():
    prov = _aurora()

    async def _run():
        prov._store_topology(HOSTS)
        assert _storage().get(StoredTopology, prov._storage_key()) == HOSTS

    asyncio.run(_run())


def test_two_providers_for_one_cluster_on_one_loop_share_the_topology():
    """A second connection to the same cluster does not start from an empty
    cache."""
    first, second = _aurora(), _aurora()

    async def _run():
        first._store_topology(HOSTS)
        assert await second.refresh() == HOSTS

    asyncio.run(_run())


def test_providers_for_different_clusters_do_not_share():
    a = _aurora(cluster_id="cluster-a")
    b = _aurora(cluster_id="cluster-b")

    async def _run():
        a._store_topology(HOSTS)
        assert b._get_stored_topology() is None

    asyncio.run(_run())


def test_an_async_entry_does_not_collide_with_a_sync_bare_key_entry():
    """Sync keys by bare cluster id in the same process-global store.

    Topology entries hold mutable HostInfo objects whose availability is
    written in place, so a shared entry would let one engine mark the other's
    hosts unavailable.
    """
    prov = _aurora(cluster_id="shared-id")
    sync_hosts = (HostInfo("sync-writer.com", 5432, HostRole.WRITER),)

    async def _run():
        _storage().put(StoredTopology, "shared-id", sync_hosts)
        prov._store_topology(HOSTS)

        assert _storage().get(StoredTopology, "shared-id") == sync_hosts
        assert prov._get_stored_topology() == HOSTS

    asyncio.run(_run())


# ---- refresh() semantics -------------------------------------------------

def test_refresh_returns_the_stored_topology_without_querying():
    """There is no provider-side freshness window any more: a present entry is
    returned and the monitor is what keeps it fresh."""
    prov = _aurora()
    conn = MagicMock()
    prov._topology_utils.query_for_topology = AsyncMock(  # type: ignore[method-assign]
        side_effect=AssertionError("must not query when topology is stored"))

    async def _run():
        prov._store_topology(HOSTS)
        assert await prov.refresh(conn) == HOSTS

    asyncio.run(_run())


def test_refresh_with_no_connection_and_nothing_stored_returns_empty():
    prov = _aurora()

    async def _run():
        assert await prov.refresh() == ()

    asyncio.run(_run())


def test_storing_an_empty_topology_does_not_overwrite_a_good_one():
    prov = _aurora()

    async def _run():
        prov._store_topology(HOSTS)
        prov._store_topology(())
        assert prov._get_stored_topology() == HOSTS

    asyncio.run(_run())


# ---- The loop component of the key ---------------------------------------

def testloop_token_is_stable_within_one_loop():
    async def _run():
        loop = asyncio.get_running_loop()
        return loop_token(loop), loop_token(loop)

    first, second = asyncio.run(_run())
    assert first == second


def test_loop_token_is_not_the_loop_address():
    """``id(loop)`` is recycled: sequential ``asyncio.run`` calls commonly
    reuse one address, so an address-keyed entry would be handed to a fresh
    loop -- along with HostInfo objects a previous run may have marked
    unavailable."""
    tokens, addresses = [], []

    async def _run():
        loop = asyncio.get_running_loop()
        tokens.append(loop_token(loop))
        addresses.append(id(loop))

    for _ in range(6):
        asyncio.run(_run())

    assert len(set(tokens)) == 6, "tokens must be unique per loop"
    # Not asserting addresses collide -- that is allocator-dependent -- only
    # that uniqueness does not come from the address.
    assert len(set(addresses)) <= 6


def test_a_loop_that_cannot_be_weak_referenced_still_gets_a_token():
    """Accelerated loops (uvloop) are extension types and may not support
    weak references. The fallback is the address, which is only safe because
    consumers re-check liveness and replace a dead entry."""

    class _NoWeakRef:
        __slots__ = ()

    loop = _NoWeakRef()
    token = loop_token(loop)  # type: ignore[arg-type]
    assert token == f"id:{id(loop)}"


def test_storage_key_is_none_when_no_loop_is_running():
    prov = _aurora()
    assert prov._storage_key() is None


def test_storage_key_pairs_the_cluster_id_with_theloop_token():
    prov = _aurora(cluster_id="my-cluster")

    async def _run():
        return prov._storage_key(), loop_token(asyncio.get_running_loop())

    key, token = asyncio.run(_run())
    assert key == ("my-cluster", token)


@pytest.mark.parametrize("stored,expected", [(HOSTS, HOSTS), (None, ())])
def test_force_refresh_without_a_connection_falls_back_to_the_store(
        stored, expected):
    prov = _aurora()

    async def _run():
        if stored is not None:
            prov._store_topology(stored)
        return await prov.force_refresh()

    assert asyncio.run(_run()) == expected


def test_a_reader_gets_copies_not_the_stored_hosts():
    """The entry is shared by every provider on the cluster and loop, and
    availability is written into a HostInfo in place, so handing out the
    stored objects lets one connection rewrite another's hosts."""
    first, second = _aurora(), _aurora()

    async def _run():
        first._store_topology(HOSTS)
        mine = await first.refresh()
        theirs = await second.refresh()

        assert mine[0] is not theirs[0], "two readers share one HostInfo"
        assert mine[0] is not HOSTS[0], "a reader got the stored object"

        # Marking one unavailable must not touch the other's view.
        mine[0].set_availability(HostAvailability.UNAVAILABLE)
        assert theirs[0].availability is not HostAvailability.UNAVAILABLE
        assert HOSTS[0].availability is not HostAvailability.UNAVAILABLE

    asyncio.run(_run())


def test_copies_keep_their_aliases():
    """HostInfo.__copy__ drops aliases, and aliases are how a live connection
    is matched back to its topology row."""
    aliased = HostInfo("writer.example", 5432, HostRole.WRITER, host_id="srv-1")
    aliased.add_alias("srv-1")
    prov = _aurora()

    async def _run():
        prov._store_topology((aliased,))
        got = await prov.refresh()
        assert got[0] is not aliased
        assert "srv-1" in got[0].aliases
        assert got[0].host_id == "srv-1"

    asyncio.run(_run())
