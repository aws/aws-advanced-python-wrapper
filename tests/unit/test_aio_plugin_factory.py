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

"""Post-3.0 Task 1-A: async plugin factory registry."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.auth_plugins import AsyncIamAuthPlugin
from aws_advanced_python_wrapper.aio.failover_plugin import AsyncFailoverPlugin
from aws_advanced_python_wrapper.aio.host_list_provider import \
    AsyncStaticHostListProvider
from aws_advanced_python_wrapper.aio.minor_plugins import (
    AsyncConnectTimePlugin, AsyncDeveloperPlugin, AsyncExecuteTimePlugin)
from aws_advanced_python_wrapper.aio.plugin_factory import (
    PLUGIN_FACTORIES, PLUGIN_FACTORY_WEIGHTS, _FailoverFactory,
    _HostMonitoringFactory, _IamAuthFactory, _ReadWriteSplittingFactory,
    build_async_plugins, parse_plugins_property, resolve_plugin_factories,
    sort_factories_by_weight)
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.aio.read_write_splitting_plugin import \
    AsyncReadWriteSplittingPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import Properties


def _svc(props: Properties) -> AsyncPluginServiceImpl:
    return AsyncPluginServiceImpl(props, MagicMock(), HostInfo("h", 5432))


def _hlp(props: Properties) -> AsyncStaticHostListProvider:
    return AsyncStaticHostListProvider(props)


# ---- Registry presence -------------------------------------------------


def test_registry_covers_every_shipped_async_plugin():
    """Every F3-B async plugin must be resolvable by a plugin code."""
    expected_codes = {
        "failover", "failover_v2",
        "host_monitoring", "host_monitoring_v2",
        "read_write_splitting",
        "iam", "aws_secrets_manager",
        "aurora_connection_tracker",
        "connect_time", "execute_time", "dev",
        "custom_endpoint",
    }
    assert expected_codes.issubset(PLUGIN_FACTORIES.keys()), (
        f"missing codes: {expected_codes - PLUGIN_FACTORIES.keys()}"
    )


# ---- parse_plugins_property --------------------------------------------


def test_parse_plugins_property_returns_list_from_comma_string():
    props = Properties({"plugins": "failover,host_monitoring_v2,iam"})
    assert parse_plugins_property(props) == ["failover", "host_monitoring_v2", "iam"]


def test_parse_plugins_property_trims_whitespace_and_drops_empties():
    props = Properties({"plugins": " failover , , host_monitoring_v2 "})
    # Entries trimmed; empty entries (" , ") dropped.
    assert parse_plugins_property(props) == ["failover", "host_monitoring_v2"]


def test_parse_plugins_property_returns_none_when_unset():
    props = Properties({"host": "h"})
    assert parse_plugins_property(props) is None


def test_parse_plugins_property_returns_empty_when_blank_string():
    props = Properties({"plugins": ""})
    # Blank string -> no codes after filtering.
    assert parse_plugins_property(props) == []


# ---- resolve_plugin_factories ------------------------------------------


def test_resolve_factories_maps_codes_to_instances():
    factories = resolve_plugin_factories(["failover", "host_monitoring_v2", "iam"])
    assert len(factories) == 3
    assert isinstance(factories[0], _FailoverFactory)
    assert isinstance(factories[1], _HostMonitoringFactory)
    assert isinstance(factories[2], _IamAuthFactory)


def test_resolve_factories_rejects_unknown_codes():
    with pytest.raises(AwsWrapperError):
        resolve_plugin_factories(["failover", "nonexistent"])


def test_resolve_factories_handles_whitespace_and_empty_entries():
    # build_async_plugins feeds already-trimmed codes, but the registry
    # strips again for safety.
    factories = resolve_plugin_factories([" failover ", "", "iam"])
    assert len(factories) == 2


# ---- sort_factories_by_weight ------------------------------------------


def test_sort_factories_by_weight_orders_known_factories():
    # Input in reverse weight order; expect sorted ascending by weight.
    factories = [
        _IamAuthFactory(),                    # 700
        _FailoverFactory(),                   # 400
        _ReadWriteSplittingFactory(),         # 300
    ]
    sorted_ = sort_factories_by_weight(factories)
    assert isinstance(sorted_[0], _ReadWriteSplittingFactory)
    assert isinstance(sorted_[1], _FailoverFactory)
    assert isinstance(sorted_[2], _IamAuthFactory)


def test_sort_factories_respects_custom_endpoint_before_failover():
    factories = [
        _FailoverFactory(),
        PLUGIN_FACTORIES["custom_endpoint"],
    ]
    sorted_ = sort_factories_by_weight(factories)
    # custom_endpoint weight is 40, before failover (400).
    assert type(sorted_[0]).__name__ == "_CustomEndpointFactory"


# ---- build_async_plugins -----------------------------------------------


def test_build_async_plugins_returns_empty_when_no_plugins_property():
    # build_async_plugins itself applies NO defaults -- the default plugin list
    # is injected into props by AsyncAwsWrapperConnection.connect (so
    # _build_host_list_provider stays in sync). Given bare props, this returns
    # []. See test_connect_applies_default_plugins_when_unset for the
    # connect-level default behavior.
    props = Properties({"host": "h"})
    plugins = build_async_plugins(_svc(props), props)
    assert plugins == []


def test_build_async_plugins_instantiates_plugins_from_string():
    async def _body() -> None:
        props = Properties({
            "host": "h", "port": "5432",
            "plugins": "failover,iam",
        })
        plugins = build_async_plugins(
            _svc(props), props, host_list_provider=_hlp(props)
        )
        types = {type(p) for p in plugins}
        assert AsyncFailoverPlugin in types
        assert AsyncIamAuthPlugin in types

    asyncio.run(_body())


def test_build_async_plugins_auto_sorts_by_weight_by_default():
    props = Properties({
        "host": "h", "port": "5432",
        "plugins": "iam,failover",  # order-reversed vs weight
    })
    plugins = build_async_plugins(
        _svc(props), props, host_list_provider=_hlp(props)
    )
    # Weight: failover(400) < iam(700) -- auto-sort puts failover first.
    assert isinstance(plugins[0], AsyncFailoverPlugin)
    assert isinstance(plugins[1], AsyncIamAuthPlugin)


def test_build_async_plugins_preserves_order_with_auto_sort_off():
    props = Properties({
        "host": "h", "port": "5432",
        "plugins": "iam,failover",
    })
    plugins = build_async_plugins(
        _svc(props), props,
        host_list_provider=_hlp(props),
        auto_sort=False,
    )
    assert isinstance(plugins[0], AsyncIamAuthPlugin)
    assert isinstance(plugins[1], AsyncFailoverPlugin)


def test_build_async_plugins_failover_without_host_list_provider_raises():
    props = Properties({
        "host": "h", "port": "5432",
        "plugins": "failover",
    })
    with pytest.raises(AwsWrapperError):
        build_async_plugins(_svc(props), props, host_list_provider=None)


def test_build_async_plugins_minor_plugin_does_not_need_host_list_provider():
    async def _body() -> None:
        props = Properties({
            "host": "h", "port": "5432",
            "plugins": "connect_time,execute_time,dev",
        })
        plugins = build_async_plugins(
            _svc(props), props, host_list_provider=None
        )
        types = {type(p) for p in plugins}
        assert AsyncConnectTimePlugin in types
        assert AsyncExecuteTimePlugin in types
        assert AsyncDeveloperPlugin in types

    asyncio.run(_body())


def test_build_async_plugins_read_write_splitting_requires_host_list_provider():
    props = Properties({
        "host": "h", "port": "5432",
        "plugins": "read_write_splitting",
    })
    with pytest.raises(AwsWrapperError):
        build_async_plugins(_svc(props), props, host_list_provider=None)


def test_build_async_plugins_with_rws_and_host_list_provider():
    async def _body() -> None:
        props = Properties({
            "host": "h", "port": "5432",
            "plugins": "read_write_splitting",
        })
        plugins = build_async_plugins(
            _svc(props), props, host_list_provider=_hlp(props)
        )
        assert isinstance(plugins[0], AsyncReadWriteSplittingPlugin)

    asyncio.run(_body())


def test_build_async_plugins_covers_all_registered_plugins():
    """Smoke test: every registered plugin instantiates cleanly."""
    async def _body() -> None:
        props = Properties({
            "host": "h", "port": "5432",
            "plugins": ",".join(PLUGIN_FACTORIES.keys()),
            "secrets_manager_secret_id": "dummy",  # for AwsSecretsManager
            # SRW endpoints are required at construction time.
            "srw_read_endpoint": "reader.example.com",
            "srw_write_endpoint": "writer.example.com",
        })
        # Use Aurora-capable hlp so failover / rws / custom_endpoint
        # don't error.
        hlp = _hlp(props)
        plugins = build_async_plugins(_svc(props), props, host_list_provider=hlp)
        # Duplicates (failover_v2, host_monitoring_v2) produce duplicate
        # instances; just ensure we got >= len(unique types) back.
        assert len(plugins) >= len(PLUGIN_FACTORIES)

    asyncio.run(_body())


# ---- AsyncAwsWrapperConnection.connect integration ---------------------


def test_connect_resolves_plugins_from_string_kwarg():
    """End-to-end: connect(..., plugins="failover,efm") builds the instances."""
    from aws_advanced_python_wrapper.aio.wrapper import \
        AsyncAwsWrapperConnection

    async def _body() -> None:
        raw_conn = MagicMock()
        raw_conn.close = AsyncMock()

        async def _fake_connect(**kwargs):
            return raw_conn

        conn = await AsyncAwsWrapperConnection.connect(
            target=_fake_connect,
            conninfo="host=h user=u password=p dbname=d port=5432",
            plugins="connect_time,execute_time",  # str routes to factory
        )
        # Should have the two minor plugins + AsyncDefaultPlugin.
        plugin_types = {type(p).__name__ for p in conn._plugin_manager.plugins}
        assert "AsyncConnectTimePlugin" in plugin_types
        assert "AsyncExecuteTimePlugin" in plugin_types
        assert "AsyncDefaultPlugin" in plugin_types

    asyncio.run(_body())


def test_connect_resolves_plugins_from_conninfo_dsn():
    """plugins="..." in the DSN string reaches the factory path."""
    from aws_advanced_python_wrapper.aio.wrapper import \
        AsyncAwsWrapperConnection

    async def _body() -> None:
        raw_conn = MagicMock()
        raw_conn.close = AsyncMock()

        async def _fake_connect(**kwargs):
            return raw_conn

        conn = await AsyncAwsWrapperConnection.connect(
            target=_fake_connect,
            conninfo=(
                "host=h user=u password=p dbname=d port=5432 "
                "plugins=dev"
            ),
        )
        plugin_types = {type(p).__name__ for p in conn._plugin_manager.plugins}
        assert "AsyncDeveloperPlugin" in plugin_types

    asyncio.run(_body())


def test_connect_explicit_plugin_list_wins_over_property_string():
    """Explicit plugins=[...] kwarg overrides props['plugins']."""
    from aws_advanced_python_wrapper.aio.wrapper import \
        AsyncAwsWrapperConnection

    async def _body() -> None:
        raw_conn = MagicMock()
        raw_conn.close = AsyncMock()

        async def _fake_connect(**kwargs):
            return raw_conn

        explicit = AsyncConnectTimePlugin()
        conn = await AsyncAwsWrapperConnection.connect(
            target=_fake_connect,
            # plugins string comes in via the DSN; explicit list wins.
            conninfo=(
                "host=h user=u password=p dbname=d port=5432 "
                "plugins=failover,efm,iam"
            ),
            plugins=[explicit],
        )
        plugin_types = {type(p).__name__ for p in conn._plugin_manager.plugins}
        assert "AsyncConnectTimePlugin" in plugin_types
        assert "AsyncFailoverPlugin" not in plugin_types
        assert "AsyncIamAuthPlugin" not in plugin_types

    asyncio.run(_body())


# ---- Weight registry sanity -------------------------------------------


def test_weights_registered_for_every_factory_class():
    """Every factory class in PLUGIN_FACTORIES must have a registered weight."""
    factory_classes = {type(f) for f in PLUGIN_FACTORIES.values()}
    missing = factory_classes - PLUGIN_FACTORY_WEIGHTS.keys()
    assert not missing, f"factory classes missing weight: {missing}"
