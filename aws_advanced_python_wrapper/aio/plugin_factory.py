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

"""Async plugin factory registry.

Mirrors sync :class:`PluginFactory` / `PluginManager.PLUGIN_FACTORIES`. Lets
users configure the async plugin list via a connection-property string
(``plugins="failover,host_monitoring_v2,iam"``) instead of instantiating plugin classes in
code.
"""

from __future__ import annotations

from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Dict, List,
                    Optional, Protocol, Type)

from aws_advanced_python_wrapper.aio.aurora_connection_tracker import \
    AsyncAuroraConnectionTrackerPlugin
from aws_advanced_python_wrapper.aio.auth_plugins import (
    AsyncAwsSecretsManagerPlugin, AsyncIamAuthPlugin)
from aws_advanced_python_wrapper.aio.custom_endpoint_monitor import \
    AsyncCustomEndpointPlugin as AsyncCustomEndpointPluginActive
from aws_advanced_python_wrapper.aio.failover_plugin import AsyncFailoverPlugin
from aws_advanced_python_wrapper.aio.federated_auth_plugins import (
    AsyncFederatedAuthPlugin, AsyncOktaAuthPlugin)
from aws_advanced_python_wrapper.aio.host_monitoring_plugin import \
    AsyncHostMonitoringPlugin
from aws_advanced_python_wrapper.aio.minor_plugins import (
    AsyncConnectTimePlugin, AsyncDeveloperPlugin, AsyncExecuteTimePlugin)
from aws_advanced_python_wrapper.aio.read_write_splitting_plugin import \
    AsyncReadWriteSplittingPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncHostListProvider
    from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncPluginFactory(Protocol):
    """Produce an :class:`AsyncPlugin` instance given service + props."""

    def get_instance(
            self,
            plugin_service: AsyncPluginService,
            props: Properties,
            host_list_provider: Optional[AsyncHostListProvider] = None) -> AsyncPlugin:
        ...


# ---- Concrete factories ------------------------------------------------


class _FailoverFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        if host_list_provider is None:
            raise AwsWrapperError(
                "failover plugin requires a host_list_provider"
            )
        return AsyncFailoverPlugin(plugin_service, host_list_provider, props)


class _HostMonitoringFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return AsyncHostMonitoringPlugin(plugin_service, props)


class _ReadWriteSplittingFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        if host_list_provider is None:
            raise AwsWrapperError(
                "read_write_splitting plugin requires a host_list_provider"
            )
        return AsyncReadWriteSplittingPlugin(
            plugin_service, host_list_provider, props
        )


class _GdbReadWriteSplittingFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        if host_list_provider is None:
            raise AwsWrapperError(
                "gdb_rw plugin requires a host_list_provider"
            )
        from aws_advanced_python_wrapper.aio.gdb_read_write_splitting_plugin import \
            AsyncGdbReadWriteSplittingPlugin
        return AsyncGdbReadWriteSplittingPlugin(
            plugin_service, host_list_provider, props
        )


class _GdbFailoverFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        if host_list_provider is None:
            raise AwsWrapperError(
                "gdb_failover plugin requires a host_list_provider"
            )
        from aws_advanced_python_wrapper.aio.gdb_failover_plugin import \
            AsyncGdbFailoverPlugin
        return AsyncGdbFailoverPlugin(
            plugin_service, host_list_provider, props
        )


class _SimpleReadWriteSplittingFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        # Local import keeps module load-order cheap and avoids pulling
        # psycopg into memory for consumers that don't use the plugin.
        from aws_advanced_python_wrapper.aio.simple_read_write_splitting_plugin import \
            AsyncSimpleReadWriteSplittingPlugin
        return AsyncSimpleReadWriteSplittingPlugin(plugin_service, props)


class _IamAuthFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return AsyncIamAuthPlugin(plugin_service, props)


class _AwsSecretsManagerFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return AsyncAwsSecretsManagerPlugin(plugin_service, props)


class _FederatedAuthFactory:
    """Resolves the concrete IdP plugin via AsyncIdpPluginRegistry.

    ``IDP_NAME`` in props picks the provider; defaults to ADFS. Raises
    ``AwsWrapperError`` for unknown IdP names (sync parity).
    """

    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        from aws_advanced_python_wrapper.aio.idp_registry import \
            resolve_federated_plugin_class
        plugin_class = resolve_federated_plugin_class(props)
        return plugin_class(plugin_service, props)


class _OktaAuthFactory:
    """Back-compat alias for the ``okta`` plugin code.

    Always instantiates AsyncOktaAuthPlugin regardless of IDP_NAME --
    matches sync's separate ``okta`` plugin code. Users who want
    dynamic IdP dispatch should use ``federated_auth``.
    """

    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return AsyncOktaAuthPlugin(plugin_service, props)


def _install_default_idp_registrations() -> None:
    """Seed the IdP registry with the shipped ADFS + Okta classes."""
    from aws_advanced_python_wrapper.aio.idp_registry import \
        AsyncIdpPluginRegistry
    AsyncIdpPluginRegistry.register("adfs", AsyncFederatedAuthPlugin)
    AsyncIdpPluginRegistry.register("okta", AsyncOktaAuthPlugin)


_install_default_idp_registrations()


class _AuroraConnectionTrackerFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return AsyncAuroraConnectionTrackerPlugin(plugin_service)


class _ConnectTimeFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return AsyncConnectTimePlugin(plugin_service)


class _ExecuteTimeFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return AsyncExecuteTimePlugin(plugin_service)


class _DeveloperFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return AsyncDeveloperPlugin()


class _CustomEndpointFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        # Active implementation post-Task 1-B (replaces SP-8 stub).
        return AsyncCustomEndpointPluginActive(plugin_service, props)


class _StaleDnsFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        # Local import keeps module load-order cheap and breaks any
        # accidental import cycles between the factory registry and the
        # plugin implementation.
        from aws_advanced_python_wrapper.aio.stale_dns_plugin import \
            AsyncStaleDnsPlugin
        return AsyncStaleDnsPlugin(plugin_service)


class _InitialConnectionFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        # Local import keeps module load-order cheap.
        from aws_advanced_python_wrapper.aio.aurora_initial_connection_strategy_plugin import \
            AsyncAuroraInitialConnectionStrategyPlugin
        return AsyncAuroraInitialConnectionStrategyPlugin(plugin_service)


class _FastestResponseFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        # Local import keeps module load-order cheap -- users who don't
        # opt into the fastest_response strategy never load the probe path.
        from aws_advanced_python_wrapper.aio.fastest_response_strategy_plugin import \
            AsyncFastestResponseStrategyPlugin
        return AsyncFastestResponseStrategyPlugin(plugin_service, props)


class _LimitlessFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        # Local import keeps module load-order cheap -- users who don't
        # opt into Limitless never import the router-discovery path.
        from aws_advanced_python_wrapper.aio.limitless_plugin import \
            AsyncLimitlessPlugin
        return AsyncLimitlessPlugin(plugin_service, props)


class _AsyncStubFactory:
    """Factory that instantiates a pass-through stub plugin.

    Each stub class is paired with a unique _AsyncStubFactory instance
    so PLUGIN_FACTORY_WEIGHTS can rank them independently if ever
    needed. All stub factories share the same *type*, which is what
    :data:`PLUGIN_FACTORY_WEIGHTS` keys on, so one weight entry covers
    every stub.
    """

    def __init__(self, stub_cls: Type[AsyncPlugin]) -> None:
        self._stub_cls = stub_cls

    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        return self._stub_cls()


class _BlueGreenFactory:
    def get_instance(
            self, plugin_service, props, host_list_provider=None):
        # Local import keeps module load-order cheap and avoids pulling
        # the BG data model into memory for consumers that don't use
        # the plugin.
        from aws_advanced_python_wrapper.aio.blue_green_plugin import \
            AsyncBlueGreenPlugin
        return AsyncBlueGreenPlugin(plugin_service, props)


# ---- Registry ----------------------------------------------------------


# Plugin code -> factory class (instantiation at registration time).
# Matches sync PluginManager.PLUGIN_FACTORIES naming exactly where
# applicable so users can reuse the same string across sync/async code.
PLUGIN_FACTORIES: Dict[str, AsyncPluginFactory] = {
    "failover": _FailoverFactory(),
    "failover_v2": _FailoverFactory(),  # alias -- sync has two failover
                                        # plugins; async ships one with
                                        # failover_v2's semantics.
    "gdb_failover": _GdbFailoverFactory(),
    "host_monitoring": _HostMonitoringFactory(),
    "host_monitoring_v2": _HostMonitoringFactory(),
    "read_write_splitting": _ReadWriteSplittingFactory(),
    "gdb_rw": _GdbReadWriteSplittingFactory(),
    "srw": _SimpleReadWriteSplittingFactory(),
    "iam": _IamAuthFactory(),
    "aws_secrets_manager": _AwsSecretsManagerFactory(),
    "federated_auth": _FederatedAuthFactory(),
    "okta": _OktaAuthFactory(),
    "aurora_connection_tracker": _AuroraConnectionTrackerFactory(),
    "connect_time": _ConnectTimeFactory(),
    "execute_time": _ExecuteTimeFactory(),
    "dev": _DeveloperFactory(),
    "custom_endpoint": _CustomEndpointFactory(),
    "stale_dns": _StaleDnsFactory(),
    "initial_connection": _InitialConnectionFactory(),
    "fastest_response_strategy": _FastestResponseFactory(),
    "limitless": _LimitlessFactory(),
    "bg": _BlueGreenFactory(),
}


# A factory marked with this sentinel sorts immediately after whatever
# plugin precedes it in the USER'S plugin list (sync
# PluginManager.WEIGHT_RELATIVE_TO_PRIOR_PLUGIN semantics).
WEIGHT_RELATIVE_TO_PRIOR_PLUGIN = -1

# Weights for auto-sort (lower = earlier in pipeline).
# Matches the sync PluginManager.PLUGIN_FACTORY_WEIGHTS table exactly
# (the v1/v2 sync codes collapse onto one async factory, which takes the
# v1 slot: failover 400, host_monitoring 500).
PLUGIN_FACTORY_WEIGHTS: Dict[Type[Any], int] = {
    _CustomEndpointFactory: 40,
    _InitialConnectionFactory: 50,
    _AuroraConnectionTrackerFactory: 100,
    _StaleDnsFactory: 200,
    _ReadWriteSplittingFactory: 300,
    _SimpleReadWriteSplittingFactory: 310,
    _GdbReadWriteSplittingFactory: 320,
    _FailoverFactory: 400,
    _GdbFailoverFactory: 420,
    _HostMonitoringFactory: 500,
    # Blue/Green sorts late so routing dispatch sees already-bound host
    # info from earlier plugins. Matches sync weight.
    _BlueGreenFactory: 550,
    _FastestResponseFactory: 600,
    _IamAuthFactory: 700,
    _AwsSecretsManagerFactory: 800,
    _FederatedAuthFactory: 900,
    _LimitlessFactory: 950,
    _OktaAuthFactory: 1000,
    _ConnectTimeFactory: WEIGHT_RELATIVE_TO_PRIOR_PLUGIN,
    _ExecuteTimeFactory: WEIGHT_RELATIVE_TO_PRIOR_PLUGIN,
    _DeveloperFactory: WEIGHT_RELATIVE_TO_PRIOR_PLUGIN,
    # All stub factories share one type; a single entry covers every
    # remaining Phase H.2 stub. Weight sits above every fixed weight so
    # stubs sort last (they subscribe to nothing, so order is cosmetic).
    _AsyncStubFactory: 2000,
}


def resolve_plugin_factories(plugin_codes: List[str]) -> List[AsyncPluginFactory]:
    """Translate a list of plugin-code strings into factory instances.

    Raises :class:`AwsWrapperError` on unknown plugin codes.
    """
    factories: List[AsyncPluginFactory] = []
    for raw in plugin_codes:
        code = raw.strip()
        if not code:
            continue
        factory = PLUGIN_FACTORIES.get(code)
        if factory is None:
            raise AwsWrapperError(
                Messages.get_formatted("PluginManager.InvalidPlugin", code)
            )
        factories.append(factory)
    return factories


def parse_plugins_property(props: Properties) -> Optional[List[str]]:
    """Return the list of plugin codes from ``props['plugins']`` or None.

    Returns ``None`` when the ``plugins`` key is NOT literally present in
    the props dict (``WrapperProperties.PLUGINS`` has a non-None default,
    which is why we check membership directly rather than calling
    ``.get()``). Blank-string value returns an empty list. Whitespace-only
    entries are dropped.
    """
    key = WrapperProperties.PLUGINS.name
    if key not in props:
        return None
    raw = props[key]
    if raw is None:
        return None
    return [c.strip() for c in str(raw).split(",") if c.strip()]


def sort_factories_by_weight(
        factories: List[AsyncPluginFactory]) -> List[AsyncPluginFactory]:
    """Stable-sort factories by their registered weight.

    Mirrors sync ``PluginManager.get_factory_weights``: a factory whose
    table entry is ``WEIGHT_RELATIVE_TO_PRIOR_PLUGIN`` (or that is unknown)
    sorts immediately after the factory that precedes it in the user's
    plugin list -- the connect/execute-time and developer plugins are
    position-sensitive, they measure whatever the user placed them next to.
    """
    last_weight = 0
    effective: List[int] = []
    for f in factories:
        w = PLUGIN_FACTORY_WEIGHTS.get(type(f))
        if w is None or w == WEIGHT_RELATIVE_TO_PRIOR_PLUGIN:
            last_weight += 1
            effective.append(last_weight)
        else:
            effective.append(w)
            last_weight = w

    return [f for _, f in sorted(
        zip(effective, factories), key=lambda pair: pair[0])]


def build_async_plugins(
        plugin_service: AsyncPluginService,
        props: Properties,
        host_list_provider: Optional[AsyncHostListProvider] = None,
        auto_sort: Optional[bool] = None) -> List[AsyncPlugin]:
    """Full pipeline: props -> codes -> factories -> instances.

    :param auto_sort: if ``None`` (default), read
        ``WrapperProperties.AUTO_SORT_PLUGIN_ORDER`` from props. Sync
        wrapper's default is ``True``; we match it.
    """
    codes = parse_plugins_property(props)
    if not codes:
        return []

    factories = resolve_plugin_factories(codes)

    if auto_sort is None:
        auto_sort = WrapperProperties.AUTO_SORT_PLUGIN_ORDER.get_bool(props)
    if auto_sort:
        factories = sort_factories_by_weight(factories)

    return [
        f.get_instance(plugin_service, props, host_list_provider)
        for f in factories
    ]


__all__ = [
    "AsyncPluginFactory",
    "PLUGIN_FACTORIES",
    "PLUGIN_FACTORY_WEIGHTS",
    "resolve_plugin_factories",
    "parse_plugins_property",
    "sort_factories_by_weight",
    "build_async_plugins",
]


# Keep unused-callable for typing consistency across async plugin shape.
_ = Callable[..., Awaitable[Any]]
