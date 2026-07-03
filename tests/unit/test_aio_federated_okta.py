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

"""Tasks 1-C + 1-D: async federated (SAML) + Okta auth plugins."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aws_advanced_python_wrapper.aio.federated_auth_plugins import (
    AsyncFederatedAuthPlugin, AsyncOktaAuthPlugin)
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import Properties

_FAKE_SAML = "PHNhbWw6UmVzcG9uc2Ugeg=="  # noqa
_FAKE_CREDS = {
    "AccessKeyId": "AKIA123",
    "SecretAccessKey": "sek",
    "SessionToken": "tok",
}


def _svc(props: Properties) -> AsyncPluginServiceImpl:
    return AsyncPluginServiceImpl(
        props, MagicMock(), HostInfo("rds.example", 5432)
    )


def _federated_props(**overrides: str) -> Properties:
    base = {
        "host": "rds.example", "port": "5432",
        "db_user": "app_user",
        "idp_endpoint": "adfs.example.com",
        "idp_username": "alice",
        "idp_password": "s3cret",
        "iam_role_arn": "arn:aws:iam::1:role/R",
        "iam_idp_arn": "arn:aws:iam::1:saml-provider/Corp",
        "iam_region": "us-east-1",
        "iam_host": "rds.example",
        "ssl_secure": "true",
    }
    base.update(overrides)
    return Properties(base)


def _okta_props(**overrides: str) -> Properties:
    base = dict(_federated_props())
    base.update({
        "idp_endpoint": "mycorp.okta.com",
        "app_id": "abc123",
    })
    base.update(overrides)
    return Properties(base)


# ---- Federated plugin --------------------------------------------------


def test_federated_plugin_subscription():
    p = AsyncFederatedAuthPlugin(_svc(_federated_props()), _federated_props())
    assert p.subscribed_methods == {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.FORCE_CONNECT.method_name,
    }


def test_federated_plugin_resolves_credentials_end_to_end():
    async def _body() -> None:
        props = _federated_props()
        plugin = AsyncFederatedAuthPlugin(_svc(props), props)

        with patch.object(
                AsyncFederatedAuthPlugin, "_fetch_saml_assertion",
                new=AsyncMock(return_value=_FAKE_SAML),
        ), patch.object(
                AsyncFederatedAuthPlugin, "_sts_assume_role_with_saml_blocking",
                return_value=_FAKE_CREDS,
        ), patch.object(
                AsyncFederatedAuthPlugin, "_generate_rds_token_blocking",
                return_value="iam-token-xyz",
        ):
            raw = MagicMock()

            async def _cf() -> object:
                return raw

            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo("rds.example", 5432),
                props=props,
                is_initial_connection=True,
                connect_func=_cf,
            )
            assert props.get("user") == "app_user"
            assert props.get("password") == "iam-token-xyz"

    asyncio.run(_body())


def test_federated_plugin_caches_rds_token():
    async def _body() -> None:
        props = _federated_props()
        plugin = AsyncFederatedAuthPlugin(_svc(props), props)

        call_count = [0]

        def _gen(host, port, user, region, creds):
            call_count[0] += 1
            return f"tok-{call_count[0]}"

        with patch.object(
                AsyncFederatedAuthPlugin, "_fetch_saml_assertion",
                new=AsyncMock(return_value=_FAKE_SAML),
        ), patch.object(
                AsyncFederatedAuthPlugin, "_sts_assume_role_with_saml_blocking",
                return_value=_FAKE_CREDS,
        ), patch.object(
                AsyncFederatedAuthPlugin, "_generate_rds_token_blocking",
                side_effect=_gen,
        ):
            async def _cf() -> object:
                return MagicMock()

            for _ in range(3):
                fresh = Properties(dict(props))
                await plugin.connect(
                    target_driver_func=lambda: None,
                    driver_dialect=MagicMock(),
                    host_info=HostInfo("rds.example", 5432),
                    props=fresh,
                    is_initial_connection=True,
                    connect_func=_cf,
                )
        # Token cache hit for calls 2 and 3.
        assert call_count[0] == 1

    asyncio.run(_body())


def test_federated_plugin_missing_db_user_raises():
    async def _body() -> None:
        props = _federated_props()
        del props["db_user"]
        plugin = AsyncFederatedAuthPlugin(_svc(props), props)

        async def _cf() -> object:
            return MagicMock()

        with pytest.raises(AwsWrapperError):
            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo("rds.example", 5432),
                props=props,
                is_initial_connection=True,
                connect_func=_cf,
            )

    asyncio.run(_body())


def test_federated_plugin_missing_role_arn_raises():
    async def _body() -> None:
        props = _federated_props()
        del props["iam_role_arn"]
        plugin = AsyncFederatedAuthPlugin(_svc(props), props)

        with patch.object(
                AsyncFederatedAuthPlugin, "_fetch_saml_assertion",
                new=AsyncMock(return_value=_FAKE_SAML),
        ):
            async def _cf() -> object:
                return MagicMock()

            with pytest.raises(AwsWrapperError):
                await plugin.connect(
                    target_driver_func=lambda: None,
                    driver_dialect=MagicMock(),
                    host_info=HostInfo("rds.example", 5432),
                    props=props,
                    is_initial_connection=True,
                    connect_func=_cf,
                )

    asyncio.run(_body())


def test_federated_plugin_extracts_saml_assertion_from_html():
    html = (
        '<html><form>'
        '<input type="hidden" name="SAMLResponse" value="BASE64SAML=="/>'
        '<input name="foo" value="bar"/>'
        '</form></html>'
    )
    assert AsyncFederatedAuthPlugin._extract_saml_assertion(html) == "BASE64SAML=="


def test_federated_plugin_raises_when_saml_missing_from_html():
    html = "<html>no saml here</html>"
    with pytest.raises(AwsWrapperError):
        AsyncFederatedAuthPlugin._extract_saml_assertion(html)


# ---- Okta plugin -------------------------------------------------------


def test_okta_plugin_subscribes_to_connect_and_force_connect():
    p = AsyncOktaAuthPlugin(_svc(_okta_props()), _okta_props())
    assert p.subscribed_methods == {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.FORCE_CONNECT.method_name,
    }


def test_okta_plugin_inherits_rds_token_path_from_federated():
    """Token resolution uses the same STS + RDS flow as Federated."""
    async def _body() -> None:
        props = _okta_props()
        plugin = AsyncOktaAuthPlugin(_svc(props), props)

        with patch.object(
                AsyncOktaAuthPlugin, "_fetch_saml_assertion",
                new=AsyncMock(return_value=_FAKE_SAML),
        ), patch.object(
                AsyncFederatedAuthPlugin, "_sts_assume_role_with_saml_blocking",
                return_value=_FAKE_CREDS,
        ), patch.object(
                AsyncFederatedAuthPlugin, "_generate_rds_token_blocking",
                return_value="okta-rds-token",
        ):
            async def _cf() -> object:
                return MagicMock()

            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo("rds.example", 5432),
                props=props,
                is_initial_connection=True,
                connect_func=_cf,
            )
            assert props.get("user") == "app_user"
            assert props.get("password") == "okta-rds-token"

    asyncio.run(_body())


def test_okta_plugin_missing_app_id_raises_during_saml_fetch():
    async def _body() -> None:
        # Force the real _fetch_saml_assertion (not patched) to run and
        # surface the missing-app_id check.
        props = _okta_props()
        del props["app_id"]
        plugin = AsyncOktaAuthPlugin(_svc(props), props)

        async def _cf() -> object:
            return MagicMock()

        with pytest.raises(AwsWrapperError):
            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo("rds.example", 5432),
                props=props,
                is_initial_connection=True,
                connect_func=_cf,
            )

    asyncio.run(_body())


# ---- Factory integration -----------------------------------------------


def test_factory_builds_federated_and_okta_plugins_from_string():
    from aws_advanced_python_wrapper.aio.plugin_factory import \
        build_async_plugins

    props = Properties({
        "host": "h", "port": "5432",
        "plugins": "federated_auth,okta",
    })
    plugins = build_async_plugins(_svc(props), props)
    types = {type(p).__name__ for p in plugins}
    assert "AsyncFederatedAuthPlugin" in types
    assert "AsyncOktaAuthPlugin" in types


# ---- E.4: invalidate_cache + Okta regex parity -------------------------


def test_federated_invalidate_cache_drops_rds_token():
    """Seed the cache via a real _resolve_credentials run (so the key
    matches exactly what that code path writes), then assert
    _invalidate_cache drops it so a subsequent resolve regenerates."""
    async def _body() -> None:
        props = _federated_props()
        plugin = AsyncFederatedAuthPlugin(_svc(props), props)

        with patch.object(
                AsyncFederatedAuthPlugin, "_fetch_saml_assertion",
                new=AsyncMock(return_value=_FAKE_SAML),
        ), patch.object(
                AsyncFederatedAuthPlugin, "_sts_assume_role_with_saml_blocking",
                return_value=_FAKE_CREDS,
        ), patch.object(
                AsyncFederatedAuthPlugin, "_generate_rds_token_blocking",
                return_value="fresh-tok",
        ):
            # First resolve populates the cache.
            await plugin._resolve_credentials(
                HostInfo("rds.example", 5432), props)

        assert len(plugin._rds_token_cache) == 1

        # _invalidate_cache must compute the exact same key the resolve
        # path wrote.
        plugin._invalidate_cache(HostInfo("rds.example", 5432), props)
        assert len(plugin._rds_token_cache) == 0

    asyncio.run(_body())


def test_federated_invalidate_cache_missing_db_user_is_noop():
    """Without db_user there is no valid cache key, so the invalidator
    must not raise (base-class retry path must stay robust)."""
    async def _body() -> None:
        props = _federated_props()
        del props["db_user"]
        plugin = AsyncFederatedAuthPlugin(_svc(props), props)
        # No entries, no key derivable -- must be a no-op.
        plugin._invalidate_cache(HostInfo("rds.example", 5432), props)

    asyncio.run(_body())


def test_okta_regex_matches_attributes_between_name_and_value():
    """Okta HTML has ``type``/``id`` between ``name=SAMLResponse`` and
    ``value=`` -- the ADFS base regex (``\\s+``) doesn't match."""
    html = (
        '<html><body>'
        '<form>'
        '<input type="hidden" name="SAMLResponse" id="saml-resp" '
        'value="okta-saml-body-base64" />'
        '</form>'
        '</body></html>'
    )
    extracted = AsyncOktaAuthPlugin._extract_saml_assertion(html)
    assert extracted == "okta-saml-body-base64"


def test_adfs_regex_still_matches_simple_attributes():
    """The ADFS regex (inherited) still works for simple name/value
    attrs -- the Okta override doesn't affect the base class."""
    html = '<input name="SAMLResponse" value="adfs-saml-body-base64" />'
    extracted = AsyncFederatedAuthPlugin._extract_saml_assertion(html)
    assert extracted == "adfs-saml-body-base64"


def test_okta_extract_saml_raises_when_form_missing():
    """Okta override error path."""
    with pytest.raises(AwsWrapperError):
        AsyncOktaAuthPlugin._extract_saml_assertion("<html>nope</html>")


def test_okta_regex_matches_real_world_html():
    """Regex handles attribute ordering Okta actually emits."""
    from aws_advanced_python_wrapper.aio.federated_auth_plugins import \
        AsyncOktaAuthPlugin

    # Representative Okta SSO response form:
    html = (
        '<form action="https://signin.aws.amazon.com/saml" method="POST">'
        '<input type="hidden" id="samlResponse" '
        'name="SAMLResponse" '
        'value="PHNhbWxwOlJlc3BvbnNlLi4u"/></form>'
    )
    extracted = AsyncOktaAuthPlugin._extract_saml_assertion(html)
    assert extracted == "PHNhbWxwOlJlc3BvbnNlLi4u"


def test_federated_plugin_respects_proxy_env_via_trust_env():
    """aiohttp ClientSession must be constructed with trust_env=True so
    HTTP_PROXY / HTTPS_PROXY env vars are honored (sync parity via
    requests library)."""
    import inspect

    from aws_advanced_python_wrapper.aio.federated_auth_plugins import \
        AsyncFederatedAuthPlugin

    # Read the source; trust_env=True must appear in the ClientSession call.
    src = inspect.getsource(AsyncFederatedAuthPlugin)
    assert "trust_env=True" in src


def test_okta_plugin_respects_proxy_env_via_trust_env():
    import inspect

    from aws_advanced_python_wrapper.aio.federated_auth_plugins import \
        AsyncOktaAuthPlugin

    src = inspect.getsource(AsyncOktaAuthPlugin)
    assert "trust_env=True" in src


def test_federated_port_falls_back_to_database_dialect_default():
    """When IAM_DEFAULT_PORT is unset and host_info.port is -1, the port
    comes from database_dialect.default_port (e.g. 3306 for MySQL)."""
    from aws_advanced_python_wrapper.aio.federated_auth_plugins import \
        AsyncFederatedAuthPlugin

    props = Properties({
        "host": "db.us-east-1.rds.amazonaws.com",
        # port OMITTED
        "idp_endpoint": "adfs.example.com",
        "idp_username": "u",
        "idp_password": "p",
        "iam_role_arn": "arn:aws:iam::123:role/r",
        "iam_idp_arn": "arn:aws:iam::123:saml-provider/adfs",
        "db_user": "dbuser",
        "iam_region": "us-east-1",
    })
    svc = MagicMock()
    fake_dialect = MagicMock()
    fake_dialect.default_port = 3306
    svc.database_dialect = fake_dialect
    plugin = AsyncFederatedAuthPlugin(svc, props)

    # host_info with no port (-1 sentinel)
    host = HostInfo(host="db.us-east-1.rds.amazonaws.com")

    # Call the port helper directly -- simpler than exercising the full flow
    assert plugin._default_port() == 3306

    # And check that IamAuthUtils.get_port receives this default
    from aws_advanced_python_wrapper.utils.iam_utils import IamAuthUtils
    port = IamAuthUtils.get_port(props, host, plugin._default_port())
    assert port == 3306


# ---- Telemetry counters ------------------------------------------------


def test_federated_plugin_emits_fetch_token_counter_on_fresh_token():
    """federated.fetch_token.count increments when we generate a new RDS
    IAM token (cache miss). Cache hits skip the counter."""
    props = _federated_props()

    fake_counters: dict = {}

    def _create_counter(name):
        c = MagicMock(name=f"counter:{name}")
        fake_counters[name] = c
        return c

    fake_tf = MagicMock()
    fake_tf.create_counter = MagicMock(side_effect=_create_counter)

    svc = AsyncPluginServiceImpl(
        props, MagicMock(), HostInfo("rds.example", 5432))
    svc.set_telemetry_factory(fake_tf)
    plugin = AsyncFederatedAuthPlugin(svc, props)

    with patch.object(
            AsyncFederatedAuthPlugin, "_fetch_saml_assertion",
            new=AsyncMock(return_value=_FAKE_SAML),
    ), patch.object(
            AsyncFederatedAuthPlugin, "_sts_assume_role_with_saml_blocking",
            return_value=_FAKE_CREDS,
    ), patch.object(
            AsyncFederatedAuthPlugin, "_generate_rds_token_blocking",
            return_value="iam-token-xyz",
    ):
        # First call: cache miss -> counter inc.
        asyncio.run(plugin._resolve_credentials(
            HostInfo("rds.example", 5432), props))
        assert fake_counters["federated.fetch_token.count"].inc.call_count == 1
        # Second call: cache hit -> counter unchanged.
        asyncio.run(plugin._resolve_credentials(
            HostInfo("rds.example", 5432), props))
        assert fake_counters["federated.fetch_token.count"].inc.call_count == 1


def test_okta_plugin_emits_distinct_counter_not_federated():
    """AsyncOktaAuthPlugin emits ``okta.fetch_token.count`` rather than
    inheriting the federated counter name. Matches sync okta_plugin.py:65
    (distinct metric per IdP so dashboards can split federated vs Okta)."""
    props = _okta_props()

    fake_counters: dict = {}

    def _create_counter(name):
        c = MagicMock(name=f"counter:{name}")
        fake_counters[name] = c
        return c

    fake_tf = MagicMock()
    fake_tf.create_counter = MagicMock(side_effect=_create_counter)

    svc = AsyncPluginServiceImpl(
        props, MagicMock(), HostInfo("rds.example", 5432))
    svc.set_telemetry_factory(fake_tf)
    plugin = AsyncOktaAuthPlugin(svc, props)

    with patch.object(
            AsyncOktaAuthPlugin, "_fetch_saml_assertion",
            new=AsyncMock(return_value=_FAKE_SAML),
    ), patch.object(
            AsyncFederatedAuthPlugin, "_sts_assume_role_with_saml_blocking",
            return_value=_FAKE_CREDS,
    ), patch.object(
            AsyncFederatedAuthPlugin, "_generate_rds_token_blocking",
            return_value="okta-iam-token",
    ):
        asyncio.run(plugin._resolve_credentials(
            HostInfo("rds.example", 5432), props))

    # Okta-specific counter created + emitted; federated counter was
    # never created on this plugin.
    assert "okta.fetch_token.count" in fake_counters
    assert fake_counters["okta.fetch_token.count"].inc.call_count == 1
    assert "federated.fetch_token.count" not in fake_counters
