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

"""Unit tests for :class:`AsyncAuroraInitialConnectionStrategyPlugin`."""

from __future__ import annotations

import asyncio
from typing import Any, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.aurora_initial_connection_strategy_plugin import \
    AsyncAuroraInitialConnectionStrategyPlugin
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.aurora_initial_connection_strategy_plugin import \
    InstanceSubstitutionStrategy
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

# ---- Helpers -----------------------------------------------------------


_WRITER_CLUSTER = "my-cluster.cluster-XYZ.us-east-1.rds.amazonaws.com"
_READER_CLUSTER = "my-cluster.cluster-ro-XYZ.us-east-1.rds.amazonaws.com"
_WRITER_INSTANCE = "my-cluster-inst-1.XYZ.us-east-1.rds.amazonaws.com"
_READER_INSTANCE = "my-cluster-inst-2.XYZ.us-east-1.rds.amazonaws.com"


def _writer_host() -> HostInfo:
    return HostInfo(host=_WRITER_INSTANCE, port=5432, role=HostRole.WRITER)


def _reader_host() -> HostInfo:
    return HostInfo(host=_READER_INSTANCE, port=5432, role=HostRole.READER)


def _cluster_host_info(host: str = _WRITER_CLUSTER) -> HostInfo:
    return HostInfo(host=host, port=5432, role=HostRole.WRITER)


def _build(
        all_hosts: Tuple[HostInfo, ...] = (),
        role: HostRole = HostRole.WRITER,
        accepts_strategy_result: bool = True,
        strategy_pick: Optional[HostInfo] = None,
        props_overrides: Optional[dict] = None):
    props = Properties({
        "host": _WRITER_CLUSTER,
        "port": "5432",
        # Keep retry bounds short so "exhaustion" tests don't burn
        # real wall-clock time.
        "open_connection_retry_timeout_ms": "100",
        "open_connection_retry_interval_ms": "10",
    })
    if props_overrides:
        for k, v in props_overrides.items():
            props[k] = v
    driver_dialect = MagicMock()
    driver_dialect.connect = AsyncMock(name="direct_conn")
    driver_dialect.abort_connection = AsyncMock()

    svc = AsyncPluginServiceImpl(props, driver_dialect)
    svc._all_hosts = all_hosts

    # Patch async plugin-service surface used by the plugin.
    svc.get_host_role = AsyncMock(return_value=role)  # type: ignore[method-assign]
    svc.force_refresh_host_list = AsyncMock()  # type: ignore[method-assign]
    svc.accepts_strategy = MagicMock(  # type: ignore[method-assign]
        return_value=accepts_strategy_result)
    svc.get_host_info_by_strategy = MagicMock(  # type: ignore[method-assign]
        return_value=strategy_pick)
    svc.is_login_exception = MagicMock(return_value=False)  # type: ignore[method-assign]
    svc.set_availability = MagicMock()  # type: ignore[method-assign]
    # _open_direct routes fresh instance connects through the plugin
    # pipeline via ``plugin_service.connect``. Replace it with the same
    # AsyncMock the old pipeline-bypass tests used for
    # ``driver_dialect.connect``, so existing test configuration
    # (return_value / side_effect on driver_dialect.connect) carries
    # over unchanged.
    svc.connect = driver_dialect.connect  # type: ignore[method-assign]

    plugin = AsyncAuroraInitialConnectionStrategyPlugin(svc, props)
    return plugin, svc, driver_dialect


async def _noop_connect_func_return(conn: Any):
    return conn


# ---- 1. Non-RDS-cluster URL passes through -----------------------------


def test_non_rds_cluster_host_passes_through():
    plugin, svc, driver_dialect = _build()
    host = HostInfo(host="some-random.example.com", port=5432)
    expected = MagicMock(name="conn")

    async def _connect_func():
        return expected

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    result = asyncio.run(_run())
    assert result is expected
    # No verification path taken.
    driver_dialect.connect.assert_not_awaited()
    svc.get_host_role.assert_not_awaited()
    # A non-cluster URL needs no role verification, so the loop records the
    # original host and returns on the first pass.
    assert svc.initial_connection_host_info is host


# ---- 2. Writer cluster URL + already writer -> direct writer conn ------


def test_writer_cluster_already_writer_returns_direct_conn():
    writer = _writer_host()
    # Plugin picks writer directly from topology (non-cluster DNS), opens
    # driver_dialect.connect, verifies role -> WRITER -> returns that conn.
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer,),
        role=HostRole.WRITER,
    )
    writer_conn = MagicMock(name="writer_direct_conn")
    driver_dialect.connect.return_value = writer_conn

    host = _cluster_host_info(_WRITER_CLUSTER)

    async def _connect_func():  # pragma: no cover - not used
        return MagicMock(name="cluster_conn")

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    result = asyncio.run(_run())
    assert result is writer_conn
    driver_dialect.connect.assert_awaited_once()
    assert svc.initial_connection_host_info is writer


# ---- 3. Writer cluster URL + connected to reader -> retry --------------


def test_writer_cluster_connected_to_reader_retries_and_swaps():
    writer = _writer_host()
    # First direct-connect lands on a reader, second lands on a writer.
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer,),
    )
    first_conn = MagicMock(name="reader_conn_first")
    second_conn = MagicMock(name="writer_conn_second")
    driver_dialect.connect.side_effect = [first_conn, second_conn]
    # get_host_role returns READER then WRITER.
    svc.get_host_role.side_effect = [HostRole.READER, HostRole.WRITER]

    host = _cluster_host_info(_WRITER_CLUSTER)

    async def _connect_func():  # pragma: no cover - not used
        return MagicMock()

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    result = asyncio.run(_run())
    assert result is second_conn
    # First (stale) conn was aborted.
    driver_dialect.abort_connection.assert_any_await(first_conn)
    # Topology force-refreshed on the bad conn.
    svc.force_refresh_host_list.assert_awaited()
    # initial_connection_host_info -> writer from topology.
    assert svc.initial_connection_host_info is writer


# ---- 4. Reader cluster URL + connected to reader -> returns conn -------


def test_reader_cluster_connected_to_reader_returns_conn():
    reader = _reader_host()
    writer = _writer_host()
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer, reader),
        role=HostRole.READER,
        strategy_pick=reader,
    )
    reader_conn = MagicMock(name="reader_conn")
    driver_dialect.connect.return_value = reader_conn

    host = _cluster_host_info(_READER_CLUSTER)

    async def _connect_func():  # pragma: no cover - not used
        return MagicMock(name="cluster_conn")

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    result = asyncio.run(_run())
    assert result is reader_conn
    assert svc.initial_connection_host_info is reader


# ---- 5. Reader cluster URL + no readers -> writer fallback -------------


def test_reader_cluster_no_readers_returns_writer_fallback():
    writer = _writer_host()
    # Topology has only a writer. _get_candidate_host returns None
    # (strategy_pick=None), so the plugin falls through the "topology
    # stale" branch, opens via connect_func, probes, and since no
    # readers exist it returns that connection unmodified.
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer,),
        role=HostRole.WRITER,  # connect_func-opened conn reports WRITER
        strategy_pick=None,
    )
    cluster_conn = MagicMock(name="cluster_conn")

    async def _connect_func():
        return cluster_conn

    host = _cluster_host_info(_READER_CLUSTER)

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    result = asyncio.run(_run())
    assert result is cluster_conn
    # The no-readers fallback records the candidate host, which on the stale
    # path is the original connect host rather than the writer.
    assert svc.initial_connection_host_info is host


# ---- 6. Timeout exhausted -> raises (parity with sync) -----------------


def test_timeout_exhausted_raises():
    writer = _writer_host()
    # Plugin always sees role=READER on a writer-cluster URL, so every
    # retry fails and the timeout kicks in. On exhaustion the plugin raises
    # rather than returning a connection whose role was never verified.
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer,),
        role=HostRole.READER,  # never a WRITER -> loop exhausts
    )
    driver_dialect.connect.return_value = MagicMock(name="reader_direct")

    async def _connect_func():
        return MagicMock(name="fallback_conn")

    host = _cluster_host_info(_WRITER_CLUSTER)

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    with pytest.raises(AwsWrapperError):
        asyncio.run(_run())


# ---- 7. Unsupported strategy -> AwsWrapperError ------------------------


def test_unsupported_reader_strategy_raises():
    reader = _reader_host()
    writer = _writer_host()
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer, reader),
        role=HostRole.READER,
        accepts_strategy_result=False,  # reject the strategy
    )

    host = _cluster_host_info(_READER_CLUSTER)

    async def _connect_func():  # pragma: no cover - not used
        return MagicMock()

    async def _run():
        await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    with pytest.raises(AwsWrapperError):
        asyncio.run(_run())


# ---- 8. Reader non-login exception -> host marked UNAVAILABLE ----------


def test_network_exception_retries_then_raises():
    """A network exception is retried until the budget expires, then the
    plugin raises on timeout (parity with sync)."""
    reader = _reader_host()
    writer = _writer_host()
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer, reader),
        role=HostRole.READER,
        strategy_pick=reader,
    )
    driver_dialect.connect.side_effect = RuntimeError("network-down")
    svc.is_network_exception = MagicMock(return_value=True)  # type: ignore[method-assign]

    host = _cluster_host_info(_READER_CLUSTER)

    async def _connect_func():
        return MagicMock(name="fallback_conn")

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    with pytest.raises(AwsWrapperError):
        asyncio.run(_run())

    # Retried rather than giving up on the first network failure.
    assert driver_dialect.connect.await_count > 1


def test_availability_marked_when_verification_fails_after_connecting():
    """set_availability is reached only when the failure happens after
    _open_candidate_connection returned a host. When the candidate connect
    itself throws, candidate_host is still None and the mark is skipped."""
    reader = _reader_host()
    plugin, svc, driver_dialect = _build(
        all_hosts=(_writer_host(), reader),
        role=HostRole.READER,
        strategy_pick=reader,
    )
    # Candidate connect succeeds; the role probe then fails with a network error.
    svc.connect = AsyncMock(return_value=MagicMock(name="instance_conn"))  # type: ignore[method-assign]
    svc.get_host_role = AsyncMock(  # type: ignore[method-assign]
        side_effect=RuntimeError("probe failed"))
    svc.is_network_exception = MagicMock(return_value=True)  # type: ignore[method-assign]

    host = _cluster_host_info(_READER_CLUSTER)

    async def _connect_func():
        return MagicMock(name="fallback_conn")

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    with pytest.raises(AwsWrapperError):
        asyncio.run(_run())

    assert svc.set_availability.call_count >= 1
    called_aliases = svc.set_availability.call_args_list[0][0][0]
    assert reader.host in "".join(called_aliases)
    assert svc.set_availability.call_args_list[0][0][1] == \
        HostAvailability.UNAVAILABLE


def test_login_exception_raises_without_retrying():
    """A login failure raises on the first attempt rather than being retried
    to exhaustion."""
    reader = _reader_host()
    plugin, svc, driver_dialect = _build(
        all_hosts=(_writer_host(), reader),
        role=HostRole.READER,
        strategy_pick=reader,
    )
    driver_dialect.connect.side_effect = RuntimeError("bad password")
    svc.is_login_exception = MagicMock(return_value=True)  # type: ignore[method-assign]

    host = _cluster_host_info(_READER_CLUSTER)

    async def _connect_func():
        return MagicMock(name="fallback_conn")

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    with pytest.raises(RuntimeError, match="bad password"):
        asyncio.run(_run())

    # Exactly one attempt -- no retry loop on a credentials failure.
    assert driver_dialect.connect.await_count == 1


def test_unclassified_exception_propagates():
    """An error that is neither login, network, nor read-only surfaces rather
    than being swallowed."""
    reader = _reader_host()
    plugin, svc, driver_dialect = _build(
        all_hosts=(_writer_host(), reader),
        role=HostRole.READER,
        strategy_pick=reader,
    )
    driver_dialect.connect.side_effect = RuntimeError("something unexpected")

    host = _cluster_host_info(_READER_CLUSTER)

    async def _connect_func():
        return MagicMock(name="fallback_conn")

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    with pytest.raises(RuntimeError, match="something unexpected"):
        asyncio.run(_run())


def test_instance_connect_skips_this_plugin():
    """Regression: the instance connect must skip this plugin so the pipeline
    does not re-enter it. Async has always done this; the rewrite must keep it
    (sync still omits it at both call sites)."""
    writer = _writer_host()
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer,), role=HostRole.WRITER, strategy_pick=writer)
    svc.connect = AsyncMock(return_value=MagicMock(name="instance_conn"))  # type: ignore[method-assign]

    host = _cluster_host_info(_WRITER_CLUSTER)

    async def _connect_func():
        return MagicMock(name="cluster_conn")

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    asyncio.run(_run())

    svc.connect.assert_awaited()
    assert svc.connect.await_args.kwargs["plugin_to_skip"] is plugin


def test_stale_topology_uses_is_rds_instance_not_cluster_dns():
    """A custom-cluster host in the topology is not an instance endpoint, so
    the plugin falls back to the initial endpoint rather than connecting
    directly to it."""
    custom_cluster_host = HostInfo(
        host="my-cluster.cluster-custom-XYZ.us-east-1.rds.amazonaws.com",
        port=5432, role=HostRole.WRITER)
    plugin, svc, driver_dialect = _build(
        all_hosts=(custom_cluster_host,), role=HostRole.WRITER)
    svc.connect = AsyncMock(name="should_not_be_used")  # type: ignore[method-assign]
    cluster_conn = MagicMock(name="cluster_conn")

    host = _cluster_host_info(_WRITER_CLUSTER)

    async def _connect_func():
        return cluster_conn

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    result = asyncio.run(_run())

    # The cluster endpoint connection is used; the custom-cluster topology host
    # is never treated as an instance to connect directly to.
    assert result is cluster_conn
    svc.connect.assert_not_awaited()


# ---- 9. E3: region-aware reader filtering -------------------------------


_READER_INSTANCE_OTHER_REGION = \
    "my-cluster-inst-3.XYZ.eu-west-1.rds.amazonaws.com"


def _other_region_reader_host() -> HostInfo:
    return HostInfo(
        host=_READER_INSTANCE_OTHER_REGION, port=5432, role=HostRole.READER)


def test_reader_candidates_restricted_to_connect_url_region():
    """When the connect URL encodes a region, only readers in that region are
    offered to the selection strategy."""
    writer = _writer_host()
    in_region_reader = _reader_host()          # us-east-1
    out_of_region_reader = _other_region_reader_host()  # eu-west-1
    plugin, svc, driver_dialect = _build(
        all_hosts=(writer, in_region_reader, out_of_region_reader),
        role=HostRole.READER,
        strategy_pick=in_region_reader,
    )
    reader_conn = MagicMock(name="reader_conn")
    driver_dialect.connect.return_value = reader_conn

    # Connect URL is the us-east-1 reader cluster endpoint.
    host = _cluster_host_info(_READER_CLUSTER)

    async def _connect_func():  # pragma: no cover - not used
        return MagicMock(name="cluster_conn")

    async def _run():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )

    result = asyncio.run(_run())
    assert result is reader_conn

    # The strategy only ever saw the in-region reader.
    assert svc.get_host_info_by_strategy.call_count >= 1
    for call in svc.get_host_info_by_strategy.call_args_list:
        candidate_list = call[0][2]
        assert in_region_reader in candidate_list
        assert out_of_region_reader not in candidate_list


def test_candidate_host_keeps_all_when_no_region_in_url():
    """A connect URL without a region (e.g. a bare hostname) must not
    restrict the candidates -- the unfiltered selector call is used."""
    in_region = _reader_host()
    out_of_region = _other_region_reader_host()
    plugin, svc, _ = _build(all_hosts=(in_region, out_of_region),
                            strategy_pick=in_region)
    no_region_host = HostInfo(host="some-random.example.com", port=5432)

    plugin._get_candidate_host(
        no_region_host,
        RdsUtils().identify_rds_type(no_region_host.host),
        InstanceSubstitutionStrategy.SUBSTITUTE_WITH_READER)

    # No host_list argument => selector sees the full topology.
    assert svc.get_host_info_by_strategy.call_count == 1
    assert len(svc.get_host_info_by_strategy.call_args_list[0].args) == 2


def test_candidate_host_filters_cross_region_readers():
    in_region = _reader_host()
    out_of_region = _other_region_reader_host()
    plugin, svc, _ = _build(all_hosts=(in_region, out_of_region),
                            strategy_pick=in_region)
    connect_host = _cluster_host_info(_READER_CLUSTER)  # us-east-1

    plugin._get_candidate_host(
        connect_host,
        RdsUrlType.RDS_READER_CLUSTER,
        InstanceSubstitutionStrategy.SUBSTITUTE_WITH_READER)

    assert svc.get_host_info_by_strategy.call_count == 1
    candidate_list = svc.get_host_info_by_strategy.call_args_list[0].args[2]
    assert in_region in candidate_list
    assert out_of_region not in candidate_list


def test_candidate_host_substitute_with_writer_ignores_strategy():
    """SUBSTITUTE_WITH_WRITER short-circuits to the topology writer without
    consulting the host selection strategy at all."""
    writer = _writer_host()
    plugin, svc, _ = _build(all_hosts=(writer, _reader_host()))

    result = plugin._get_candidate_host(
        _cluster_host_info(_WRITER_CLUSTER),
        RdsUrlType.RDS_WRITER_CLUSTER,
        InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER)

    assert result == writer
    svc.get_host_info_by_strategy.assert_not_called()


def test_candidate_host_substitute_with_any_raises_unsupported_strategy():
    """SUBSTITUTE_WITH_ANY has no target role, so it raises unsupportedStrategy
    rather than being coerced to a reader-only selection."""
    reader = _reader_host()
    plugin, svc, _ = _build(all_hosts=(reader,))

    with pytest.raises(AwsWrapperError):
        plugin._get_candidate_host(
            _cluster_host_info(
                "my-cluster.cluster-custom-XYZ.us-east-1.rds.amazonaws.com"),
            RdsUrlType.RDS_CUSTOM_CLUSTER,
            InstanceSubstitutionStrategy.SUBSTITUTE_WITH_ANY)

    svc.get_host_info_by_strategy.assert_not_called()


def test_endpoint_substitution_role_on_instance_endpoint_raises():
    """Substitution cannot be requested for an instance endpoint."""
    props_overrides = {"endpoint_substitution_role": "writer"}
    plugin, _, _ = _build(all_hosts=(_writer_host(),),
                          props_overrides=props_overrides)
    props = Properties(props_overrides)

    with pytest.raises(AwsWrapperError):
        plugin._get_instance_substitution_strategy(
            props, RdsUrlType.RDS_INSTANCE, True, _WRITER_INSTANCE)


def test_invalid_verify_opened_connection_type_raises():
    """A typo in verify_opened_connection_type must not be swallowed."""
    plugin, _, _ = _build(
        all_hosts=(_writer_host(),),
        props_overrides={"verify_opened_connection_type": "bogus_value"})

    with pytest.raises(AwsWrapperError):
        plugin._get_role_to_verify(
            RdsUrlType.RDS_WRITER_CLUSTER, True, Properties({}), _WRITER_CLUSTER)


def test_verify_reader_on_writer_cluster_raises():
    """Verifying 'reader' against a writer cluster endpoint is invalid."""
    plugin, _, _ = _build(
        all_hosts=(_writer_host(),),
        props_overrides={"verify_opened_connection_type": "reader"})

    with pytest.raises(AwsWrapperError):
        plugin._get_role_to_verify(
            RdsUrlType.RDS_WRITER_CLUSTER, True, Properties({}), _WRITER_CLUSTER)


def test_global_writer_cluster_substitutes_and_verifies_writer():
    """A global writer cluster endpoint resolves to writer substitution and
    writer verification, rather than falling through unhandled."""
    plugin, _, _ = _build(all_hosts=(_writer_host(),))
    props = Properties({})
    global_host = "my-global.global-XYZ.global.rds.amazonaws.com"

    strategy = plugin._get_instance_substitution_strategy(
        props, RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER, True, global_host)
    role = plugin._get_role_to_verify(
        RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER, True, props, global_host)

    assert strategy is InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER
    assert role == HostRole.WRITER


def test_inactive_cluster_writer_substitution_role_honored():
    """When the cluster writer endpoint is in a different region than the
    current writer (Aurora Global secondary), the inactive-writer setting
    decides whether to substitute."""
    # Writer lives in us-west-2; the connect URL is us-east-1.
    out_of_region_writer = HostInfo(
        host="my-cluster-inst-1.XYZ.us-west-2.rds.amazonaws.com",
        port=5432, role=HostRole.WRITER)
    plugin, _, _ = _build(all_hosts=(out_of_region_writer,))
    props = Properties({"inactive_cluster_writer_endpoint_substitution_role": "none"})

    strategy = plugin._get_instance_substitution_strategy(
        props, RdsUrlType.RDS_WRITER_CLUSTER, True, _WRITER_CLUSTER)

    assert strategy is InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE


def test_initial_connection_host_selector_strategy_overrides_reader_variant():
    """The non-deprecated property wins when explicitly set."""
    plugin, _, _ = _build(props_overrides={
        "reader_initial_connection_host_selector_strategy": "random",
        "initial_connection_host_selector_strategy": "round_robin",
    })
    assert plugin._selection_strategy == "round_robin"


def test_reader_strategy_falls_back_to_deprecated_property():
    plugin, _, _ = _build(props_overrides={
        "reader_initial_connection_host_selector_strategy": "least_connections",
    })
    assert plugin._selection_strategy == "least_connections"


def test_retry_bounds_honor_explicit_zero():
    """An explicit 0 for the retry bounds must be taken literally rather than
    falling back to the default."""
    plugin, _, _ = _build(props_overrides={
        "open_connection_retry_timeout_ms": "0",
        "open_connection_retry_interval_ms": "0",
    })
    assert plugin._open_connection_retry_timeout_ns == 0
    assert plugin._retry_delay_ms == 0


def test_wait_for_initial_topology_defaults_to_zero():
    """get_int returns -1 for an absent property; the plugin normalizes it to 0."""
    plugin, _, _ = _build()
    assert plugin._wait_for_initial_topology_ms == 0
