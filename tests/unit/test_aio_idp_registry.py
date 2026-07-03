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

"""Tests for AsyncIdpPluginRegistry + resolve_federated_plugin_class (K.3)."""

from __future__ import annotations

import pytest

# Ensure the seed registrations run.
import aws_advanced_python_wrapper.aio.plugin_factory  # noqa: F401
from aws_advanced_python_wrapper.aio.federated_auth_plugins import (
    AsyncFederatedAuthPlugin, AsyncOktaAuthPlugin)
from aws_advanced_python_wrapper.aio.idp_registry import (
    AsyncIdpPluginRegistry, resolve_federated_plugin_class)
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


@pytest.fixture(autouse=True)
def _restore_registry():
    """Snapshot the registry pre-test and restore post-test so user
    registrations don't leak between tests."""
    snapshot = AsyncIdpPluginRegistry.registered_names()
    yield
    AsyncIdpPluginRegistry._reset_for_tests()
    for name, plugin_class in snapshot.items():
        AsyncIdpPluginRegistry.register(name, plugin_class)


# ----- default seed entries -------------------------------------------


def test_default_registry_has_adfs_and_okta() -> None:
    names = AsyncIdpPluginRegistry.registered_names()
    assert set(names.keys()) == {"adfs", "okta"}
    assert names["adfs"] is AsyncFederatedAuthPlugin
    assert names["okta"] is AsyncOktaAuthPlugin


def test_resolve_defaults_to_adfs_when_no_idp_name() -> None:
    props = Properties()
    resolved = resolve_federated_plugin_class(props)
    assert resolved is AsyncFederatedAuthPlugin


def test_resolve_picks_okta_when_idp_name_is_okta() -> None:
    props = Properties({WrapperProperties.IDP_NAME.name: "okta"})
    resolved = resolve_federated_plugin_class(props)
    assert resolved is AsyncOktaAuthPlugin


def test_resolve_case_insensitive() -> None:
    props = Properties({WrapperProperties.IDP_NAME.name: "Okta"})
    resolved = resolve_federated_plugin_class(props)
    assert resolved is AsyncOktaAuthPlugin


def test_resolve_raises_on_unknown_idp_name() -> None:
    props = Properties({WrapperProperties.IDP_NAME.name: "azure_ad"})
    with pytest.raises(AwsWrapperError):
        resolve_federated_plugin_class(props)


# ----- consumer extension ---------------------------------------------


def test_register_custom_idp() -> None:
    class MyIdp(AsyncFederatedAuthPlugin):
        pass

    AsyncIdpPluginRegistry.register("pingid", MyIdp)
    assert AsyncIdpPluginRegistry.resolve("pingid") is MyIdp

    props = Properties({WrapperProperties.IDP_NAME.name: "pingid"})
    assert resolve_federated_plugin_class(props) is MyIdp


def test_register_overwrites_existing_entry() -> None:
    class Replacement(AsyncFederatedAuthPlugin):
        pass

    AsyncIdpPluginRegistry.register("adfs", Replacement)
    assert AsyncIdpPluginRegistry.resolve("adfs") is Replacement


def test_register_empty_name_raises() -> None:
    with pytest.raises(AwsWrapperError):
        AsyncIdpPluginRegistry.register("   ", AsyncFederatedAuthPlugin)


def test_unregister_removes_entry() -> None:
    class MyIdp(AsyncFederatedAuthPlugin):
        pass

    AsyncIdpPluginRegistry.register("pingid", MyIdp)
    AsyncIdpPluginRegistry.unregister("pingid")
    assert AsyncIdpPluginRegistry.resolve("pingid") is None


def test_unregister_missing_is_noop() -> None:
    # Should not raise.
    AsyncIdpPluginRegistry.unregister("never_was_registered")


# ----- factory integration --------------------------------------------


def test_federated_auth_factory_uses_registry() -> None:
    from aws_advanced_python_wrapper.aio.plugin_factory import \
        _FederatedAuthFactory

    class _MockService:
        def get_telemetry_factory(self):
            from aws_advanced_python_wrapper.utils.telemetry.null_telemetry import \
                NullTelemetryFactory
            return NullTelemetryFactory()
        database_dialect = None

    factory = _FederatedAuthFactory()

    # default (no IDP_NAME) -> ADFS
    plugin = factory.get_instance(_MockService(), Properties())
    assert isinstance(plugin, AsyncFederatedAuthPlugin)
    assert not isinstance(plugin, AsyncOktaAuthPlugin)

    # IDP_NAME=okta -> Okta subclass
    plugin = factory.get_instance(
        _MockService(),
        Properties({WrapperProperties.IDP_NAME.name: "okta"}))
    assert isinstance(plugin, AsyncOktaAuthPlugin)

    # IDP_NAME=unknown -> raise
    with pytest.raises(AwsWrapperError):
        factory.get_instance(
            _MockService(),
            Properties({WrapperProperties.IDP_NAME.name: "nope"}))
