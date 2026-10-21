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

"""Shared access to the stored async topology.

The host list provider and the topology monitor both read and write the
cluster's topology, and they key the storage service and the monitor service
the same way, so the key and the accessors live here rather than on either of
them.
"""

from __future__ import annotations

import asyncio
import copy
import threading
import weakref
from typing import Optional, Tuple
from uuid import uuid4

from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.hostinfo import Topology as StoredTopology
from aws_advanced_python_wrapper.utils import services_container

# The storage service is keyed by type, and the sync Topology alias is a
# forward reference, so an async-side alias is a different object. Only the
# sync alias is registered, so keying on anything else writes to an
# unregistered cache.

TopologyKey = Tuple[str, str]

_loop_tokens: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, str] = \
    weakref.WeakKeyDictionary()
# setdefault on a WeakKeyDictionary is not documented atomic, and two threads
# minting competing tokens for one loop would split its topology entries.
_loop_tokens_lock = threading.Lock()


def loop_token(loop: asyncio.AbstractEventLoop) -> str:
    """A stable identity for ``loop``, for use in a storage or monitor key.

    Deliberately not ``id(loop)``: addresses are reused, so a fresh loop could
    be handed a dead loop's entry, including HostInfo objects a previous run
    marked unavailable.
    """
    with _loop_tokens_lock:
        try:
            token = _loop_tokens.get(loop)
            if token is None:
                token = uuid4().hex
                _loop_tokens[loop] = token
            return token
        except TypeError:
            # Some accelerated loops (Cython extension types) are not
            # weak-referenceable, and the lookup raises as readily as the
            # assignment. Falling back to the address is only safe because
            # every consumer re-checks liveness and replaces a dead entry, so
            # a recycled address is detected rather than silently reused.
            return f"id:{id(loop)}"


def topology_key(cluster_id: str) -> Optional[TopologyKey]:
    """The key for this cluster's topology on the running event loop.

    Compound because the store is process-global and shared with the sync
    provider, which keys by bare cluster id. ``None`` off-loop: the key names
    a loop, so there is no entry to read or invalidate.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return None
    return (cluster_id, loop_token(loop))


def get_topology(
        key: Optional[TopologyKey]) -> Optional[StoredTopology]:
    """Read the stored topology as fresh HostInfo objects.

    The entry is shared by every provider on this cluster and loop, and
    availability is written into a HostInfo in place, so handing out the
    stored objects would let one connection's availability marking rewrite
    the hosts another connection is holding.
    """
    if key is None:
        return None
    stored = services_container.get_storage_service().get(StoredTopology, key)
    if stored is None:
        return None
    return tuple(_copy_host(host) for host in stored)


def _copy_host(host: HostInfo) -> HostInfo:
    """Copy a host, restoring what ``HostInfo.__copy__`` drops.

    It carries neither the aliases, which match a live connection back to its
    topology row, nor the availability strategy, which decides when a host
    marked unavailable becomes usable again.
    """
    clone = copy.copy(host)
    clone.host_availability_strategy = host.host_availability_strategy
    aliases = host.aliases
    if aliases:
        clone.add_alias(*aliases)
    return clone


def put_topology(key: Optional[TopologyKey], topology: StoredTopology) -> None:
    if key is None or not topology:
        return
    services_container.get_storage_service().put(StoredTopology, key, topology)


def remove_topology(key: Optional[TopologyKey]) -> None:
    if key is None:
        return
    services_container.get_storage_service().remove(StoredTopology, key)
