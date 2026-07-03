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

"""Async IdP plugin registry (K.3).

Replaces the sync wrapper's hardcoded ADFS-or-raise dispatch in
:class:`FederatedAuthPluginFactory` with a lookup-style registry that
consumers can extend with their own IdP plugin classes.

Parity notes:
  * Sync's :class:`FederatedAuthPluginFactory.get_credentials_provider_factory`
    raises on unknown ``idp_name``; we preserve that via
    :func:`resolve_federated_plugin_class` (raises ``AwsWrapperError`` when
    the registry has no entry for the requested name).
  * Sync ships ADFS only; Okta is a separate plugin code. In async, both
    ADFS and Okta live behind the ``federated_auth`` plugin code via
    this registry -- ``IDP_NAME=adfs`` (default) picks ADFS,
    ``IDP_NAME=okta`` picks Okta. The separate ``okta`` plugin code is
    preserved for back-compat and points at the same registered class.
"""

from __future__ import annotations

from threading import Lock
from typing import TYPE_CHECKING, ClassVar, Dict, Optional, Type

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.auth_plugins import \
        AsyncAuthPluginBase


class AsyncIdpPluginRegistry:
    """Class-level registry mapping IdP name -> async auth plugin class.

    Writes are lock-protected so concurrent registrations from multiple
    setup paths don't race. Reads are lock-free: the registry is
    effectively read-mostly at steady state, and missing entries raise
    cleanly.

    Seed entries (adfs, okta) are installed by the async plugin factory
    module on import to keep this module dependency-free of the
    concrete plugin classes.
    """

    _lock: ClassVar[Lock] = Lock()
    _registry: ClassVar[Dict[str, Type[AsyncAuthPluginBase]]] = {}

    @classmethod
    def register(
            cls,
            idp_name: str,
            plugin_class: Type[AsyncAuthPluginBase]) -> None:
        """Register ``plugin_class`` under ``idp_name``.

        Overwrites an existing entry -- consumers can replace the
        built-in ADFS/Okta implementations with their own.
        """
        normalized = idp_name.lower().strip()
        if not normalized:
            raise AwsWrapperError(
                "AsyncIdpPluginRegistry.register requires a non-empty idp_name")
        with cls._lock:
            cls._registry[normalized] = plugin_class

    @classmethod
    def unregister(cls, idp_name: str) -> None:
        """Remove the entry for ``idp_name``; no-op if absent."""
        normalized = idp_name.lower().strip()
        with cls._lock:
            cls._registry.pop(normalized, None)

    @classmethod
    def resolve(cls, idp_name: str) -> Optional[Type[AsyncAuthPluginBase]]:
        """Return the plugin class for ``idp_name``, or ``None`` if absent."""
        normalized = idp_name.lower().strip()
        with cls._lock:
            return cls._registry.get(normalized)

    @classmethod
    def registered_names(cls) -> Dict[str, Type[AsyncAuthPluginBase]]:
        """Snapshot of currently-registered IdP -> plugin-class mappings."""
        with cls._lock:
            return dict(cls._registry)

    @classmethod
    def _reset_for_tests(cls) -> None:
        """Test-only hook: wipe the registry. Production code must not
        call this -- the seed entries are installed once per import."""
        with cls._lock:
            cls._registry.clear()


def resolve_federated_plugin_class(
        props: Properties,
        default_name: str = "adfs") -> Type[AsyncAuthPluginBase]:
    """Look up the async federated-auth plugin class for ``props``.

    ``IDP_NAME`` in ``props`` selects the provider; when absent,
    ``default_name`` (``"adfs"``) applies -- matches sync
    :meth:`FederatedAuthPluginFactory.get_credentials_provider_factory`.

    Raises :class:`AwsWrapperError` if ``IDP_NAME`` names a provider
    that is not registered (same contract as sync's "UnsupportedIdp"
    error).
    """
    raw = WrapperProperties.IDP_NAME.get(props)
    name = (raw or default_name).lower().strip() or default_name
    plugin_class = AsyncIdpPluginRegistry.resolve(name)
    if plugin_class is None:
        raise AwsWrapperError(
            Messages.get_formatted(
                "FederatedAuthPluginFactory.UnsupportedIdp", name))
    return plugin_class


__all__ = [
    "AsyncIdpPluginRegistry",
    "resolve_federated_plugin_class",
]
