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
        props, MagicMock(), HostInfo("instance-1.abc123.us-east-1.rds.amazonaws.com", 5432)
    )


def _federated_props(**overrides: str) -> Properties:
    base = {
        "host": "instance-1.abc123.us-east-1.rds.amazonaws.com", "port": "5432",
        "db_user": "app_user",
        "idp_endpoint": "adfs.example.com",
        "idp_username": "alice",
        "idp_password": "s3cret",
        "iam_role_arn": "arn:aws:iam::1:role/R",
        "iam_idp_arn": "arn:aws:iam::1:saml-provider/Corp",
        "iam_region": "us-east-1",
        "iam_host": "instance-1.abc123.us-east-1.rds.amazonaws.com",
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
                host_info=HostInfo("instance-1.abc123.us-east-1.rds.amazonaws.com", 5432),
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

        def _gen(*args):
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
                    host_info=HostInfo("instance-1.abc123.us-east-1.rds.amazonaws.com", 5432),
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
                host_info=HostInfo("instance-1.abc123.us-east-1.rds.amazonaws.com", 5432),
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
                    host_info=HostInfo("instance-1.abc123.us-east-1.rds.amazonaws.com", 5432),
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
                host_info=HostInfo("instance-1.abc123.us-east-1.rds.amazonaws.com", 5432),
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
                host_info=HostInfo("instance-1.abc123.us-east-1.rds.amazonaws.com", 5432),
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
                HostInfo("instance-1.abc123.us-east-1.rds.amazonaws.com", 5432), props)

        assert len(plugin._rds_token_cache) == 1

        # _invalidate_cache must compute the exact same key the resolve
        # path wrote.
        plugin._invalidate_cache(HostInfo("instance-1.abc123.us-east-1.rds.amazonaws.com", 5432), props)
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
        plugin._invalidate_cache(HostInfo("instance-1.abc123.us-east-1.rds.amazonaws.com", 5432), props)

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
        props, MagicMock(), HostInfo("instance-1.abc123.us-east-1.rds.amazonaws.com", 5432))
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
            HostInfo("instance-1.abc123.us-east-1.rds.amazonaws.com", 5432), props))
        assert fake_counters["federated.fetch_token.count"].inc.call_count == 1
        # Second call: cache hit -> counter unchanged.
        asyncio.run(plugin._resolve_credentials(
            HostInfo("instance-1.abc123.us-east-1.rds.amazonaws.com", 5432), props))
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
        props, MagicMock(), HostInfo("instance-1.abc123.us-east-1.rds.amazonaws.com", 5432))
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
            HostInfo("instance-1.abc123.us-east-1.rds.amazonaws.com", 5432), props))

    # Okta-specific counter created + emitted; federated counter was
    # never created on this plugin.
    assert "okta.fetch_token.count" in fake_counters
    assert fake_counters["okta.fetch_token.count"].inc.call_count == 1
    assert "federated.fetch_token.count" not in fake_counters


# ---- ADFS form flow + IAM_TOKEN_EXPIRATION + Okta validation (Task 10) ----

_RDS_HOST = "instance-1.abc123.us-east-1.rds.amazonaws.com"


class _FakeResp:
    """Minimal stand-in for an aiohttp ClientResponse used as an async CM."""

    def __init__(self, status: int, text: str, reason: str = "OK") -> None:
        self._status = status
        self._text = text
        self._reason = reason

    @property
    def status(self) -> int:
        return self._status

    @property
    def reason(self) -> str:
        return self._reason

    async def text(self) -> str:
        return self._text

    async def json(self):
        import json
        return json.loads(self._text)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None


class _FakeSession:
    """aiohttp.ClientSession stand-in: canned GET/POST responses + a recorder."""

    def __init__(self, responses: dict, recorder: list) -> None:
        # responses: {"GET": _FakeResp, "POST": _FakeResp}
        self._responses = responses
        self._recorder = recorder

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    def get(self, url, **kwargs):
        self._recorder.append(("GET", url, kwargs))
        return self._responses["GET"]

    def post(self, url, **kwargs):
        self._recorder.append(("POST", url, kwargs))
        return self._responses["POST"]


def test_federated_adfs_flow_gets_form_posts_creds_scrapes_saml():
    """ADFS flow: GET sign-in page, parse form action + inputs, inject
    idp_username/idp_password, POST urlencoded, scrape SAMLResponse."""
    async def _body():
        props = _federated_props()
        plugin = AsyncFederatedAuthPlugin(_svc(props), props)

        sign_in_html = (
            '<html><body>'
            '<form action="/adfs/ls/post" method="post">'
            '<input name="UserName" value=""/>'
            '<input name="Password" value=""/>'
            '<input name="AuthMethod" value="FormsAuthentication"/>'
            '<input name="Kmsi" value="true"/>'
            '</form></body></html>'
        )
        post_html = '<input name="SAMLResponse" value="BASE64SAML=="/>'
        recorder: list = []
        session = _FakeSession(
            {"GET": _FakeResp(200, sign_in_html),
             "POST": _FakeResp(200, post_html)},
            recorder,
        )

        with patch("aiohttp.ClientSession", return_value=session):
            saml = await plugin._fetch_saml_assertion(props)

        assert saml == "BASE64SAML=="
        # GET hit the IdP sign-in page; POST went to the resolved form action.
        get_url = [c for c in recorder if c[0] == "GET"][0][1]
        assert get_url.startswith(
            "https://adfs.example.com:443/adfs/ls/IdpInitiatedSignOn.aspx")
        post_call = [c for c in recorder if c[0] == "POST"][0]
        assert post_call[1] == "https://adfs.example.com:443/adfs/ls/post"
        # Credentials were injected into the urlencoded form body.
        body = post_call[2]["data"]
        assert "UserName=alice" in body
        assert "Password=s3cret" in body
        assert "AuthMethod=FormsAuthentication" in body

    asyncio.run(_body())


def test_federated_adfs_flow_raises_on_non_2xx_response():
    """A non-2xx sign-in-page response surfaces SamlUtils.RequestFailed."""
    async def _body():
        props = _federated_props()
        plugin = AsyncFederatedAuthPlugin(_svc(props), props)
        session = _FakeSession(
            {"GET": _FakeResp(500, "boom", reason="Server Error"),
             "POST": _FakeResp(200, "")},
            [],
        )
        with patch("aiohttp.ClientSession", return_value=session):
            with pytest.raises(AwsWrapperError):
                await plugin._fetch_saml_assertion(props)

    asyncio.run(_body())


def test_federated_stores_token_with_iam_token_expiration_ttl():
    """The RDS-token cache TTL comes from IAM_TOKEN_EXPIRATION (default 870),
    not a hardcoded 900 (sync federated_plugin.py:161)."""
    async def _body():
        props = _federated_props(iam_token_expiration="123")
        plugin = AsyncFederatedAuthPlugin(_svc(props), props)

        captured: dict = {}

        def _capture(host, port, user, region, token, ttl_sec=None):
            captured["ttl"] = ttl_sec

        plugin._store_rds_token = _capture  # type: ignore[assignment]

        with patch.object(
                AsyncFederatedAuthPlugin, "_fetch_saml_assertion",
                new=AsyncMock(return_value=_FAKE_SAML),
        ), patch.object(
                AsyncFederatedAuthPlugin, "_sts_assume_role_with_saml_blocking",
                return_value=_FAKE_CREDS,
        ), patch.object(
                AsyncFederatedAuthPlugin, "_generate_rds_token_blocking",
                return_value="tok",
        ):
            await plugin._resolve_credentials(HostInfo(_RDS_HOST, 5432), props)

        assert captured["ttl"] == 123

    asyncio.run(_body())


def test_federated_default_token_ttl_matches_iam_token_expiration_default():
    """With IAM_TOKEN_EXPIRATION unset, the TTL is its 870s default."""
    from aws_advanced_python_wrapper.utils.properties import WrapperProperties
    assert WrapperProperties.IAM_TOKEN_EXPIRATION.default_value == 15 * 60 - 30


def test_federated_network_exception_wrapped_as_aws_connect_error():
    """A network failure at connect wraps as AwsConnectError with the
    FederatedAuthPlugin.ConnectException message."""
    from aws_advanced_python_wrapper.errors import AwsConnectError

    async def _body():
        props = _federated_props()
        svc = _svc(props)
        svc.is_network_exception = MagicMock(return_value=True)
        plugin = AsyncFederatedAuthPlugin(svc, props)

        with patch.object(
                AsyncFederatedAuthPlugin, "_fetch_saml_assertion",
                new=AsyncMock(return_value=_FAKE_SAML),
        ), patch.object(
                AsyncFederatedAuthPlugin, "_sts_assume_role_with_saml_blocking",
                return_value=_FAKE_CREDS,
        ), patch.object(
                AsyncFederatedAuthPlugin, "_generate_rds_token_blocking",
                return_value="tok",
        ):
            async def _cf():
                raise Exception("net down")

            with pytest.raises(AwsConnectError) as exc:
                await plugin.connect(
                    MagicMock(), MagicMock(), HostInfo(_RDS_HOST, 5432),
                    props, True, _cf)
        assert str(exc.value).startswith(
            "[FederatedAuthPlugin] Error occurred while opening a connection")

    asyncio.run(_body())


def test_federated_invalid_host_raises():
    """A non-RDS iam_host fails get_iam_host validation (sync parity, B4)."""
    async def _body():
        props = _federated_props(iam_host="not-an-rds-host.example.com")
        plugin = AsyncFederatedAuthPlugin(_svc(props), props)
        with pytest.raises(AwsWrapperError):
            await plugin._resolve_credentials(HostInfo(_RDS_HOST, 5432), props)

    asyncio.run(_body())


# ---- Okta: unescape + SAML validation ----------------------------------


def test_okta_unescapes_saml_response():
    """Okta SAML values are HTML-unescaped (sync okta_plugin.py:242)."""
    html = '<input name="SAMLResponse" type="hidden" value="a&amp;b&lt;c=="/>'
    assert AsyncOktaAuthPlugin._extract_saml_assertion(html) == "a&b<c=="


def test_okta_flow_validates_url_and_scrapes_saml():
    """Okta flow: POST authn -> sessionToken, GET app SSO (validated https
    URL) -> scrape SAML."""
    async def _body():
        props = _okta_props()
        plugin = AsyncOktaAuthPlugin(_svc(props), props)

        authn_json = '{"status": "SUCCESS", "sessionToken": "sess-123"}'
        sso_html = (
            '<input type="hidden" name="SAMLResponse" '
            'value="T0tUQV9TQU1M"/>'
        )
        recorder: list = []
        session = _FakeSession(
            {"POST": _FakeResp(200, authn_json),
             "GET": _FakeResp(200, sso_html)},
            recorder,
        )
        with patch("aiohttp.ClientSession", return_value=session):
            saml = await plugin._fetch_saml_assertion(props)

        assert saml == "T0tUQV9TQU1M"
        # SSO URL carried the one-time session token.
        get_url = [c for c in recorder if c[0] == "GET"][0][1]
        assert "onetimetoken=sess-123" in get_url
        assert get_url.startswith("https://mycorp.okta.com/app/amazon_aws/")

    asyncio.run(_body())


def test_okta_flow_raises_without_session_token():
    async def _body():
        props = _okta_props()
        plugin = AsyncOktaAuthPlugin(_svc(props), props)
        session = _FakeSession(
            {"POST": _FakeResp(200, '{"status": "MFA_REQUIRED"}'),
             "GET": _FakeResp(200, "")},
            [],
        )
        with patch("aiohttp.ClientSession", return_value=session):
            with pytest.raises(AwsWrapperError):
                await plugin._fetch_saml_assertion(props)

    asyncio.run(_body())
