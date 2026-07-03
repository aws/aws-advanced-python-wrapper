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

"""F3-B SP-7: async auth plugins (IAM + Secrets Manager)."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from aws_advanced_python_wrapper.aio.auth_plugins import (
    AsyncAwsSecretsManagerPlugin, AsyncIamAuthPlugin)
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.aws_credentials_manager import \
    AwsCredentialsManager
from aws_advanced_python_wrapper.errors import AwsConnectError, AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import Properties


def _svc(props: Properties) -> AsyncPluginServiceImpl:
    return AsyncPluginServiceImpl(props, MagicMock(), HostInfo(host="h", port=5432))


# ---- IAM plugin --------------------------------------------------------


def test_iam_plugin_subscription():
    props = Properties({"host": "h.example", "port": "5432", "user": "app"})
    p = AsyncIamAuthPlugin(_svc(props), props)
    assert p.subscribed_methods == {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.FORCE_CONNECT.method_name,
    }


def test_iam_plugin_generates_token_and_injects_as_password():
    async def _body() -> None:
        props = Properties({
            "host": "inst.abc123.us-east-1.rds.amazonaws.com", "port": "5432",
            "user": "db_user", "iam_region": "us-east-1",
        })
        plugin = AsyncIamAuthPlugin(_svc(props), props)

        # Patch the sync boto3 call; AsyncIamAuthPlugin awaits via to_thread.
        with patch.object(
            AsyncIamAuthPlugin,
            "_generate_token_blocking",
            return_value="iam-token-abc",
        ):
            async def _connect_func() -> object:
                return "conn-sentinel"

            result = await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(
                    host="inst.abc123.us-east-1.rds.amazonaws.com", port=5432),
                props=props,
                is_initial_connection=True,
                connect_func=_connect_func,
            )
            assert result == "conn-sentinel"
            assert props.get("user") == "db_user"
            assert props.get("password") == "iam-token-abc"

    asyncio.run(_body())


def test_iam_plugin_caches_token_within_expiration_window():
    async def _body() -> None:
        props = Properties({
            "host": "inst.abc123.us-east-1.rds.amazonaws.com", "port": "5432",
            "user": "db_user", "iam_region": "us-east-1",
        })
        plugin = AsyncIamAuthPlugin(_svc(props), props)

        call_count = [0]

        def _gen_token(*args):
            call_count[0] += 1
            return f"tok-{call_count[0]}"

        with patch.object(
            AsyncIamAuthPlugin,
            "_generate_token_blocking",
            side_effect=_gen_token,
        ):
            async def _connect_func() -> object:
                return None

            # First connect triggers token generation.
            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(
                    host="inst.abc123.us-east-1.rds.amazonaws.com", port=5432),
                props=Properties(dict(props)),
                is_initial_connection=True,
                connect_func=_connect_func,
            )
            # Second connect reuses cached token.
            second_props = Properties(dict(props))
            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(
                    host="inst.abc123.us-east-1.rds.amazonaws.com", port=5432),
                props=second_props,
                is_initial_connection=False,
                connect_func=_connect_func,
            )
            assert call_count[0] == 1
            assert second_props.get("password") == "tok-1"

    asyncio.run(_body())


def test_iam_plugin_raises_when_user_is_missing():
    async def _body() -> None:
        props = Properties({
            "host": "inst.abc123.us-east-1.rds.amazonaws.com", "port": "5432",
        })
        plugin = AsyncIamAuthPlugin(_svc(props), props)

        async def _connect_func() -> None:
            return None

        with pytest.raises(AwsWrapperError):
            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(
                    host="inst.abc123.us-east-1.rds.amazonaws.com", port=5432),
                props=props,
                is_initial_connection=True,
                connect_func=_connect_func,
            )

    asyncio.run(_body())


# ---- IAM secure-transport for async MySQL (aiomysql) -------------------


def _dialect(driver_name):
    d = MagicMock()
    d.driver_name = driver_name
    return d


def test_iam_aiomysql_no_tls_config_encrypts_without_verify_silently():
    """aiomysql doesn't auto-negotiate TLS, so the cleartext IAM token would be
    dropped. We enable encryption but do NOT verify the cert (the RDS CA isn't
    in the system trust store and no ssl_ca was given). This matches the SYNC
    driver's posture exactly -- mysql.connector negotiates TLS by default with
    ssl_verify_cert=False -- so, like the sync IamAuthPlugin, we stay SILENT
    (no warning). Verification is opt-in via ssl_ca."""
    import ssl
    props = Properties({"host": "h", "port": "3306", "user": "app"})
    plugin = AsyncIamAuthPlugin(_svc(props), props)
    with patch("aws_advanced_python_wrapper.aio.auth_plugins.logger") as mock_logger:
        plugin._prepare_secure_transport(_dialect("aiomysql"), props)
    ctx = props.get("ssl")
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.verify_mode == ssl.CERT_NONE
    assert ctx.check_hostname is False
    mock_logger.warning.assert_not_called()


def test_iam_aiomysql_respects_caller_ssl_ca():
    """A caller-supplied ssl_ca is the supported path to a verifying
    connection; we must not overwrite it with an unverified context."""
    props = Properties({
        "host": "h", "port": "3306", "user": "app",
        "ssl_ca": "/path/rds-global-bundle.pem"})
    plugin = AsyncIamAuthPlugin(_svc(props), props)
    plugin._prepare_secure_transport(_dialect("aiomysql"), props)
    assert props.get("ssl") is None  # untouched -> aiomysql verifies via ssl_ca


def test_iam_non_aiomysql_is_noop():
    """psycopg/mysql.connector negotiate TLS themselves; the hook must not
    touch their props."""
    props = Properties({"host": "h", "port": "5432", "user": "app"})
    plugin = AsyncIamAuthPlugin(_svc(props), props)
    plugin._prepare_secure_transport(_dialect("psycopg-async"), props)
    assert props.get("ssl") is None


# ---- Secrets Manager plugin --------------------------------------------


def test_secrets_manager_subscription():
    props = Properties({
        "host": "h", "port": "5432",
        "secrets_manager_secret_id": "my-secret",
    })
    p = AsyncAwsSecretsManagerPlugin(_svc(props), props)
    assert p.subscribed_methods == {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.FORCE_CONNECT.method_name,
    }


def test_secrets_manager_fetches_and_injects_credentials():
    async def _body() -> None:
        props = Properties({
            "host": "h", "port": "5432",
            "secrets_manager_secret_id": "my-secret",
            "secrets_manager_region": "us-west-2",
        })
        plugin = AsyncAwsSecretsManagerPlugin(_svc(props), props)

        with patch.object(
            AsyncAwsSecretsManagerPlugin,
            "_fetch_secret_blocking",
            return_value={"username": "secret_user", "password": "secret_pwd"},
        ):
            async def _connect_func() -> object:
                return "opened"

            result = await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(host="h", port=5432),
                props=props,
                is_initial_connection=True,
                connect_func=_connect_func,
            )
            assert result == "opened"
            assert props.get("user") == "secret_user"
            assert props.get("password") == "secret_pwd"

    asyncio.run(_body())


def test_secrets_manager_uses_custom_keys():
    async def _body() -> None:
        props = Properties({
            "host": "h", "port": "5432",
            "secrets_manager_secret_id": "my-secret",
            "secrets_manager_region": "us-east-1",
            "secrets_manager_secret_username_key": "db_login",
            "secrets_manager_secret_password_key": "db_pass",
        })
        plugin = AsyncAwsSecretsManagerPlugin(_svc(props), props)

        with patch.object(
            AsyncAwsSecretsManagerPlugin,
            "_fetch_secret_blocking",
            return_value={"db_login": "u", "db_pass": "p"},
        ):
            async def _connect_func() -> None:
                return None

            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(host="h", port=5432),
                props=props,
                is_initial_connection=True,
                connect_func=_connect_func,
            )
            assert props.get("user") == "u"
            assert props.get("password") == "p"

    asyncio.run(_body())


def test_secrets_manager_raises_when_secret_id_missing():
    async def _body() -> None:
        props = Properties({"host": "h", "port": "5432"})
        plugin = AsyncAwsSecretsManagerPlugin(_svc(props), props)

        async def _connect_func() -> None:
            return None

        with pytest.raises(AwsWrapperError):
            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(host="h", port=5432),
                props=props,
                is_initial_connection=True,
                connect_func=_connect_func,
            )

    asyncio.run(_body())


def test_secrets_manager_caches_result():
    async def _body() -> None:
        props = Properties({
            "host": "h", "port": "5432",
            "secrets_manager_secret_id": "my-secret",
            "secrets_manager_region": "us-east-1",
        })
        plugin = AsyncAwsSecretsManagerPlugin(_svc(props), props)

        call_count = [0]

        def _fetch(*args, **kwargs):
            call_count[0] += 1
            return {"username": f"u{call_count[0]}", "password": f"p{call_count[0]}"}

        with patch.object(
            AsyncAwsSecretsManagerPlugin,
            "_fetch_secret_blocking",
            side_effect=_fetch,
        ):
            async def _connect_func() -> None:
                return None

            for _ in range(3):
                fresh = Properties(dict(props))
                await plugin.connect(
                    target_driver_func=lambda: None,
                    driver_dialect=MagicMock(),
                    host_info=HostInfo(host="h", port=5432),
                    props=fresh,
                    is_initial_connection=True,
                    connect_func=_connect_func,
                )
        assert call_count[0] == 1

    asyncio.run(_body())


# ---- AsyncAuthPluginBase: FORCE_CONNECT + retry-on-login (E.1) --------


def test_base_plugin_subscribes_to_connect_and_force_connect():
    from aws_advanced_python_wrapper.aio.auth_plugins import \
        AsyncAuthPluginBase

    class _Stub(AsyncAuthPluginBase):
        async def _resolve_credentials(self, host_info, props):
            return ("u", "p", False)

        def _invalidate_cache(self, host_info, props):
            pass

    props = Properties({"host": "h", "port": "5432"})
    plugin = _Stub(_svc(props), props)
    assert DbApiMethod.CONNECT.method_name in plugin.subscribed_methods
    assert DbApiMethod.FORCE_CONNECT.method_name in plugin.subscribed_methods


def test_base_plugin_retries_on_login_exception_when_cached():
    """Cached credentials + login exception -> invalidate + re-resolve + retry."""
    from aws_advanced_python_wrapper.aio.auth_plugins import \
        AsyncAuthPluginBase

    resolve_calls = []
    invalidate_calls = []

    class _Stub(AsyncAuthPluginBase):
        _next_was_cached = True

        async def _resolve_credentials(self, host_info, props):
            resolve_calls.append(self._next_was_cached)
            was_cached = self._next_was_cached
            self._next_was_cached = False  # next call returns fresh
            return ("u", f"token-{len(resolve_calls)}", was_cached)

        def _invalidate_cache(self, host_info, props):
            invalidate_calls.append(1)

    props = Properties({"host": "h", "port": "5432"})
    svc = _svc(props)
    svc.is_login_exception = MagicMock(return_value=True)
    plugin = _Stub(svc, props)

    attempt = [0]

    async def _connect_func():
        attempt[0] += 1
        if attempt[0] == 1:
            raise Exception("auth failed")
        return MagicMock(name="conn")

    host = HostInfo(host="h", port=5432)
    result = asyncio.run(plugin.connect(
        MagicMock(), MagicMock(), host, props, True, _connect_func))

    assert result is not None
    assert invalidate_calls == [1]
    assert len(resolve_calls) == 2
    assert attempt[0] == 2


def test_base_plugin_does_not_retry_when_credentials_were_fresh():
    """Fresh credentials + login exception -> no retry, wrapped in
    AwsWrapperError (parity with the sync IAM/secrets _connect, which wrap a
    fresh-credential login failure rather than letting the raw driver error
    escape)."""
    from aws_advanced_python_wrapper.aio.auth_plugins import \
        AsyncAuthPluginBase

    class _Stub(AsyncAuthPluginBase):
        async def _resolve_credentials(self, host_info, props):
            return ("u", "fresh-token", False)

        def _invalidate_cache(self, host_info, props):
            pass

    props = Properties({"host": "h", "port": "5432"})
    svc = _svc(props)
    svc.is_network_exception = MagicMock(return_value=False)
    svc.is_login_exception = MagicMock(return_value=True)
    plugin = _Stub(svc, props)

    async def _connect_func():
        raise Exception("auth failed")

    host = HostInfo(host="h", port=5432)
    with pytest.raises(AwsWrapperError, match="auth failed"):
        asyncio.run(plugin.connect(
            MagicMock(), MagicMock(), host, props, True, _connect_func))


def test_base_plugin_wraps_network_exception_as_aws_connect_error():
    """Network / failover exceptions are wrapped as AwsConnectError (parity with
    sync iam_plugin.py:139 / aws_secrets_manager_plugin.py:126). AwsConnectError
    is-a network exception per the dialect handlers, so the failover plugin
    still recognizes it and triggers failover."""
    from aws_advanced_python_wrapper.aio.auth_plugins import \
        AsyncAuthPluginBase

    class _NetErr(Exception):
        pass

    class _Stub(AsyncAuthPluginBase):
        async def _resolve_credentials(self, host_info, props):
            return ("u", "p", False)

        def _invalidate_cache(self, host_info, props):
            pass

    props = Properties({"host": "h", "port": "5432"})
    svc = _svc(props)
    svc.is_network_exception = MagicMock(return_value=True)
    svc.is_login_exception = MagicMock(return_value=False)
    plugin = _Stub(svc, props)

    async def _connect_func():
        raise _NetErr("net down")

    host = HostInfo(host="h", port=5432)
    with pytest.raises(AwsConnectError, match="net down"):
        asyncio.run(plugin.connect(
            MagicMock(), MagicMock(), host, props, True, _connect_func))


def test_secrets_manager_wraps_botocore_fetch_errors():
    """A raw botocore ClientError from get_secret_value (e.g. a bad secret id)
    must surface as AwsWrapperError, not the raw botocore exception (#8a)."""
    from botocore.exceptions import ClientError

    from aws_advanced_python_wrapper.aio.auth_plugins import \
        AsyncAwsSecretsManagerPlugin

    props = Properties({
        "secrets_manager_secret_id": "bad-id",
        "secrets_manager_region": "us-east-1",
    })
    svc = _svc(props)
    plugin = AsyncAwsSecretsManagerPlugin(svc, props)

    def _boom(*args, **kwargs):
        raise ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "nope"}},
            "GetSecretValue")

    plugin._fetch_secret_blocking = _boom  # type: ignore[assignment]

    host = HostInfo(host="h", port=5432)
    with pytest.raises(AwsWrapperError):
        asyncio.run(plugin._resolve_credentials(host, props))


def test_base_plugin_does_not_retry_on_non_login_exceptions():
    from aws_advanced_python_wrapper.aio.auth_plugins import \
        AsyncAuthPluginBase

    class _Stub(AsyncAuthPluginBase):
        async def _resolve_credentials(self, host_info, props):
            return ("u", "p", True)

        def _invalidate_cache(self, host_info, props):
            pass

    props = Properties({"host": "h", "port": "5432"})
    svc = _svc(props)
    svc.is_login_exception = MagicMock(return_value=False)
    plugin = _Stub(svc, props)

    async def _connect_func():
        raise Exception("network boom")

    host = HostInfo(host="h", port=5432)
    with pytest.raises(Exception, match="network boom"):
        asyncio.run(plugin.connect(
            MagicMock(), MagicMock(), host, props, True, _connect_func))


def test_base_plugin_force_connect_uses_same_retry_flow():
    from aws_advanced_python_wrapper.aio.auth_plugins import \
        AsyncAuthPluginBase

    class _Stub(AsyncAuthPluginBase):
        _cached = True

        async def _resolve_credentials(self, host_info, props):
            was = self._cached
            self._cached = False
            return ("u", "t", was)

        def _invalidate_cache(self, host_info, props):
            pass

    props = Properties({"host": "h", "port": "5432"})
    svc = _svc(props)
    svc.is_login_exception = MagicMock(return_value=True)
    plugin = _Stub(svc, props)

    attempt = [0]

    async def _fc():
        attempt[0] += 1
        if attempt[0] == 1:
            raise Exception("auth failed")
        return MagicMock()

    asyncio.run(plugin.force_connect(
        MagicMock(), MagicMock(), HostInfo(host="h", port=5432), props, True, _fc))
    assert attempt[0] == 2


# ---- IAM plugin: IAM_EXPIRATION + region discovery + cache invalidation (E.2) ---


def test_iam_plugin_uses_iam_expiration_property():
    """Token TTL honors IAM_EXPIRATION (seconds); a TTL >> grace means
    second call hits the cache and skips token generation."""
    props = Properties({
        "host": "inst.abc123.us-west-2.rds.amazonaws.com",
        "port": "5432", "user": "u",
        "iam_expiration": "200",  # well above 60s grace window
        "iam_region": "us-west-2",
    })
    plugin = AsyncIamAuthPlugin(_svc(props), props)
    host = HostInfo(
        host="inst.abc123.us-west-2.rds.amazonaws.com", port=5432)
    with patch.object(AsyncIamAuthPlugin, "_generate_token_blocking",
                      return_value="tok-1") as gen:
        asyncio.run(plugin._resolve_credentials(host, props))
        asyncio.run(plugin._resolve_credentials(host, props))
    assert gen.call_count == 1


def test_iam_plugin_auto_discovers_region_from_rds_host():
    """Region auto-discovered from RDS hostname when IAM_REGION not set."""
    props = Properties({
        "host": "db.cluster-xyz123.us-west-2.rds.amazonaws.com",
        "port": "5432", "user": "u",
        # No iam_region
    })
    plugin = AsyncIamAuthPlugin(_svc(props), props)
    host = HostInfo(
        host="db.cluster-xyz123.us-west-2.rds.amazonaws.com", port=5432)
    with patch.object(AsyncIamAuthPlugin, "_generate_token_blocking",
                      return_value="tok") as gen:
        asyncio.run(plugin._resolve_credentials(host, props))
    # Region arg passed to _generate_token_blocking is us-west-2.
    args = gen.call_args[0]
    assert "us-west-2" in args


def test_iam_plugin_raises_when_region_unresolvable():
    """Non-RDS host and no IAM_REGION => AwsWrapperError."""
    props = Properties({
        "host": "custom.example.com", "port": "5432", "user": "u",
    })
    plugin = AsyncIamAuthPlugin(_svc(props), props)
    host = HostInfo(host="custom.example.com", port=5432)
    with pytest.raises(AwsWrapperError):
        asyncio.run(plugin._resolve_credentials(host, props))


def test_iam_plugin_invalidate_cache_drops_entry():
    props = Properties({
        "host": "inst.abc123.us-west-2.rds.amazonaws.com",
        "port": "5432", "user": "u",
        "iam_region": "us-west-2",
    })
    plugin = AsyncIamAuthPlugin(_svc(props), props)
    host = HostInfo(
        host="inst.abc123.us-west-2.rds.amazonaws.com", port=5432)
    with patch.object(AsyncIamAuthPlugin, "_generate_token_blocking",
                      return_value="t"):
        asyncio.run(plugin._resolve_credentials(host, props))
    assert plugin._token_cache  # populated
    plugin._invalidate_cache(host, props)
    assert not plugin._token_cache  # dropped


def test_iam_plugin_resolve_credentials_returns_was_cached_flag():
    props = Properties({
        "host": "inst.abc123.us-west-2.rds.amazonaws.com",
        "port": "5432", "user": "u",
        "iam_region": "us-west-2",
    })
    plugin = AsyncIamAuthPlugin(_svc(props), props)
    host = HostInfo(
        host="inst.abc123.us-west-2.rds.amazonaws.com", port=5432)
    with patch.object(AsyncIamAuthPlugin, "_generate_token_blocking",
                      return_value="tok"):
        _, _, was_cached1 = asyncio.run(
            plugin._resolve_credentials(host, props))
        _, _, was_cached2 = asyncio.run(
            plugin._resolve_credentials(host, props))
    assert was_cached1 is False
    assert was_cached2 is True


def test_iam_plugin_uses_database_dialect_default_port_when_port_missing():
    """When neither IAM_DEFAULT_PORT nor host_info.port is set, use the
    database_dialect's default_port (e.g. 3306 for MySQL)."""
    props = Properties({
        "host": "mydb.abc.us-west-2.rds.amazonaws.com",
        # port OMITTED
        "user": "u",
        "iam_region": "us-west-2",
    })
    svc = _svc(props)
    # Fake a MySQL dialect for the default_port
    fake_dialect = MagicMock()
    fake_dialect.default_port = 3306
    svc.database_dialect = fake_dialect
    plugin = AsyncIamAuthPlugin(svc, props)

    host = HostInfo(host="mydb.abc.us-west-2.rds.amazonaws.com")  # no port
    with patch.object(AsyncIamAuthPlugin, "_generate_token_blocking",
                      return_value="tok") as gen:
        asyncio.run(plugin._resolve_credentials(host, props))

    # _generate_token_blocking signature:
    # (host_info, props, user, host, port, region)
    args = gen.call_args[0]
    assert args[4] == 3306  # port argument was 3306, not 5432


# ---- Secrets Manager: TTL + endpoint + ARN region + invalidate (E.3) -----


def test_secrets_plugin_uses_secrets_manager_expiration_property():
    """Per-entry TTL honors SECRETS_MANAGER_EXPIRATION (seconds)."""
    props = Properties({
        "host": "h", "port": "5432",
        "secrets_manager_secret_id": "my-secret",
        "secrets_manager_region": "us-east-1",
        "secrets_manager_expiration": "60",
    })
    plugin = AsyncAwsSecretsManagerPlugin(_svc(props), props)
    host = HostInfo(host="h", port=5432)

    with patch.object(AsyncAwsSecretsManagerPlugin, "_fetch_secret_blocking",
                      return_value={"username": "u", "password": "p"}) as fetch:
        asyncio.run(plugin._resolve_credentials(host, props))
        asyncio.run(plugin._resolve_credentials(host, props))
    assert fetch.call_count == 1  # second call cached


def _patch_secrets_client(secret_json: str):
    """Patch AwsCredentialsManager.get_session/get_client so the secrets fetch
    routes through the credentials manager (parity with sync) without a real
    boto3 call. Returns (get_session_patch, get_client_patch, mock_client)."""
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {"SecretString": secret_json}
    return (
        patch.object(AwsCredentialsManager, "get_session", return_value=MagicMock()),
        patch.object(AwsCredentialsManager, "get_client", return_value=mock_client),
    )


def test_secrets_plugin_forwards_endpoint_override_to_credentials_manager():
    """SECRETS_MANAGER_ENDPOINT flows through AwsCredentialsManager.get_client
    as the endpoint_url argument (routing parity with sync
    _fetch_latest_credentials)."""
    props = Properties({
        "host": "h", "port": "5432",
        "secrets_manager_secret_id": "my-secret",
        "secrets_manager_region": "us-east-1",
        "secrets_manager_endpoint": "https://vpc-endpoint.example.com",
    })
    plugin = AsyncAwsSecretsManagerPlugin(_svc(props), props)
    host = HostInfo(host="h", port=5432)

    session_patch, client_patch = _patch_secrets_client(
        '{"username": "u", "password": "p"}')
    with session_patch, client_patch as get_client:
        asyncio.run(plugin._resolve_credentials(host, props))

    # get_client(service, session, host, region, endpoint) -- endpoint is arg 4.
    args = get_client.call_args[0]
    assert args[0] == "secretsmanager"
    assert args[4] == "https://vpc-endpoint.example.com"


def test_secrets_plugin_extracts_region_from_arn_when_region_unset():
    """ARN-form secret_id provides region when SECRETS_MANAGER_REGION is absent;
    the region flows into AwsCredentialsManager.get_session."""
    props = Properties({
        "host": "h", "port": "5432",
        "secrets_manager_secret_id": "arn:aws:secretsmanager:us-west-2:123456789012:secret:my-secret-AbCdEf",
        # No secrets_manager_region
    })
    plugin = AsyncAwsSecretsManagerPlugin(_svc(props), props)
    host = HostInfo(host="h", port=5432)

    session_patch, client_patch = _patch_secrets_client(
        '{"username": "u", "password": "p"}')
    with session_patch as get_session, client_patch:
        asyncio.run(plugin._resolve_credentials(host, props))

    # get_session(host_info, props, region) -- region is arg 2.
    args = get_session.call_args[0]
    assert args[2] == "us-west-2"


def test_secrets_plugin_raises_when_no_region_and_no_arn():
    """Non-ARN secret_id without SECRETS_MANAGER_REGION -> AwsWrapperError."""
    props = Properties({
        "host": "h", "port": "5432",
        "secrets_manager_secret_id": "my-secret",
        # No region, not an ARN
    })
    plugin = AsyncAwsSecretsManagerPlugin(_svc(props), props)
    host = HostInfo(host="h", port=5432)

    with pytest.raises(AwsWrapperError):
        asyncio.run(plugin._resolve_credentials(host, props))


def test_secrets_plugin_invalidate_cache_drops_entry():
    props = Properties({
        "host": "h", "port": "5432",
        "secrets_manager_secret_id": "my-secret",
        "secrets_manager_region": "us-east-1",
    })
    plugin = AsyncAwsSecretsManagerPlugin(_svc(props), props)
    host = HostInfo(host="h", port=5432)

    with patch.object(AsyncAwsSecretsManagerPlugin, "_fetch_secret_blocking",
                      return_value={"username": "u", "password": "p"}):
        asyncio.run(plugin._resolve_credentials(host, props))
    assert plugin._secret_cache
    plugin._invalidate_cache(host, props)
    assert not plugin._secret_cache


def test_secrets_plugin_resolve_credentials_returns_was_cached_flag():
    props = Properties({
        "host": "h", "port": "5432",
        "secrets_manager_secret_id": "my-secret",
        "secrets_manager_region": "us-east-1",
    })
    plugin = AsyncAwsSecretsManagerPlugin(_svc(props), props)
    host = HostInfo(host="h", port=5432)

    with patch.object(AsyncAwsSecretsManagerPlugin, "_fetch_secret_blocking",
                      return_value={"username": "u", "password": "p"}):
        _, _, was1 = asyncio.run(plugin._resolve_credentials(host, props))
        _, _, was2 = asyncio.run(plugin._resolve_credentials(host, props))
    assert was1 is False
    assert was2 is True


# ---- Telemetry counters ------------------------------------------------


def _svc_with_counters(props: Properties):
    """Build an AsyncPluginServiceImpl with a MagicMock telemetry factory.

    Returns (svc, counters) where counters is a dict of name -> MagicMock.
    """
    fake_counters: dict = {}

    def _create_counter(name):
        c = MagicMock(name=f"counter:{name}")
        fake_counters[name] = c
        return c

    fake_tf = MagicMock()
    fake_tf.create_counter = MagicMock(side_effect=_create_counter)

    svc = AsyncPluginServiceImpl(props, MagicMock(), HostInfo(host="h", port=5432))
    svc.set_telemetry_factory(fake_tf)
    return svc, fake_counters


def test_iam_plugin_emits_fetch_token_counter_on_fresh_token():
    props = Properties({
        "host": "inst.abc123.us-east-1.rds.amazonaws.com", "port": "5432",
        "user": "db_user", "iam_region": "us-east-1",
    })
    svc, counters = _svc_with_counters(props)
    plugin = AsyncIamAuthPlugin(svc, props)

    with patch.object(AsyncIamAuthPlugin, "_generate_token_blocking",
                      return_value="iam-token-abc"):
        asyncio.run(plugin._resolve_credentials(
            HostInfo(host="inst.abc123.us-east-1.rds.amazonaws.com", port=5432),
            props,
        ))

    assert counters["iam.fetch_token.count"].inc.called


def test_iam_plugin_does_not_emit_counter_on_cache_hit():
    props = Properties({
        "host": "inst.abc123.us-east-1.rds.amazonaws.com", "port": "5432",
        "user": "db_user", "iam_region": "us-east-1",
    })
    svc, counters = _svc_with_counters(props)
    plugin = AsyncIamAuthPlugin(svc, props)
    host = HostInfo(host="inst.abc123.us-east-1.rds.amazonaws.com", port=5432)

    with patch.object(AsyncIamAuthPlugin, "_generate_token_blocking",
                      return_value="iam-token-abc"):
        asyncio.run(plugin._resolve_credentials(host, props))
        # First call generated -> counter should be 1.
        assert counters["iam.fetch_token.count"].inc.call_count == 1
        asyncio.run(plugin._resolve_credentials(host, props))
        # Second call hits cache -> counter should still be 1.
        assert counters["iam.fetch_token.count"].inc.call_count == 1


def test_secrets_plugin_emits_fetch_credentials_counter_on_fresh_secret():
    props = Properties({
        "host": "h", "port": "5432",
        "secrets_manager_secret_id": "my-secret",
        "secrets_manager_region": "us-east-1",
    })
    svc, counters = _svc_with_counters(props)
    plugin = AsyncAwsSecretsManagerPlugin(svc, props)

    with patch.object(AsyncAwsSecretsManagerPlugin, "_fetch_secret_blocking",
                      return_value={"username": "u", "password": "p"}):
        asyncio.run(plugin._resolve_credentials(
            HostInfo(host="h", port=5432), props))

    assert counters["secrets_manager.fetch_credentials.count"].inc.called


# ---- Error-key mapping: AwsConnectError / ConnectException / UnhandledException ----

_RDS_HOST = "inst.abc123.us-east-1.rds.amazonaws.com"


def _iam_props():
    return Properties({
        "host": _RDS_HOST, "port": "5432", "user": "u",
        "iam_region": "us-east-1",
    })


def test_iam_plugin_network_exception_wrapped_as_aws_connect_error():
    """A network failure at connect time becomes AwsConnectError with the
    IamAuthPlugin.ConnectException message (sync iam_plugin.py:139)."""
    async def _body():
        props = _iam_props()
        svc = _svc(props)
        svc.is_network_exception = MagicMock(return_value=True)
        plugin = AsyncIamAuthPlugin(svc, props)
        host = HostInfo(_RDS_HOST, 5432)

        with patch.object(AsyncIamAuthPlugin, "_generate_token_blocking",
                          return_value="tok"):
            async def _cf():
                raise Exception("net down")

            with pytest.raises(AwsConnectError) as exc:
                await plugin.connect(
                    MagicMock(), MagicMock(), host, props, True, _cf)
        assert str(exc.value).startswith(
            "[IamAuthPlugin] Error occurred while opening a connection")

    asyncio.run(_body())


def test_iam_plugin_first_failure_raises_connect_exception():
    """A non-login (fresh-credential) failure wraps as ConnectException, not
    UnhandledException (sync iam_plugin.py:143)."""
    async def _body():
        props = _iam_props()
        svc = _svc(props)
        svc.is_network_exception = MagicMock(return_value=False)
        svc.is_login_exception = MagicMock(return_value=False)
        plugin = AsyncIamAuthPlugin(svc, props)
        host = HostInfo(_RDS_HOST, 5432)

        with patch.object(AsyncIamAuthPlugin, "_generate_token_blocking",
                          return_value="tok"):
            async def _cf():
                raise Exception("boom")

            with pytest.raises(AwsWrapperError) as exc:
                await plugin.connect(
                    MagicMock(), MagicMock(), host, props, True, _cf)
        assert str(exc.value).startswith(
            "[IamAuthPlugin] Error occurred while opening a connection")

    asyncio.run(_body())


def test_iam_plugin_retry_failure_raises_unhandled_exception():
    """Cached token + login failure -> regenerate + retry; a second failure
    wraps as UnhandledException (sync iam_plugin.py:160)."""
    async def _body():
        props = _iam_props()
        svc = _svc(props)
        svc.is_network_exception = MagicMock(return_value=False)
        svc.is_login_exception = MagicMock(return_value=True)
        plugin = AsyncIamAuthPlugin(svc, props)
        host = HostInfo(_RDS_HOST, 5432)

        # Seed the cache so the connect path sees was_cached=True.
        with patch.object(AsyncIamAuthPlugin, "_generate_token_blocking",
                          return_value="tok"):
            await plugin._resolve_credentials(host, props)

        with patch.object(AsyncIamAuthPlugin, "_generate_token_blocking",
                          return_value="tok2"):
            async def _cf():
                raise Exception("login failed")

            with pytest.raises(AwsWrapperError) as exc:
                await plugin.connect(
                    MagicMock(), MagicMock(), host, props, True, _cf)
        assert str(exc.value).startswith("[IamAuthPlugin] Unhandled exception")

    asyncio.run(_body())


def test_secrets_plugin_retry_failure_raises_unhandled_exception():
    """Cached secret + login failure -> refetch + retry; a second failure wraps
    as UnhandledException (sync aws_secrets_manager_plugin.py:141)."""
    async def _body():
        props = Properties({
            "host": "h", "port": "5432",
            "secrets_manager_secret_id": "my-secret",
            "secrets_manager_region": "us-east-1",
        })
        svc = _svc(props)
        svc.is_network_exception = MagicMock(return_value=False)
        svc.is_login_exception = MagicMock(return_value=True)
        plugin = AsyncAwsSecretsManagerPlugin(svc, props)
        host = HostInfo("h", 5432)

        with patch.object(AsyncAwsSecretsManagerPlugin, "_fetch_secret_blocking",
                          return_value={"username": "u", "password": "p"}):
            await plugin._resolve_credentials(host, props)  # seed cache

            async def _cf():
                raise Exception("login failed")

            with pytest.raises(AwsWrapperError) as exc:
                await plugin.connect(
                    MagicMock(), MagicMock(), host, props, True, _cf)
        assert str(exc.value).startswith(
            "[AwsSecretsManagerPlugin] Unhandled exception")

    asyncio.run(_body())


def test_secrets_plugin_cache_key_includes_endpoint():
    """Two connects that differ only by SECRETS_MANAGER_ENDPOINT are distinct
    cache entries -> two fetches (sync 3-tuple key aws_secrets_manager_plugin.py:86)."""
    async def _body():
        base = {
            "host": "h", "port": "5432",
            "secrets_manager_secret_id": "my-secret",
            "secrets_manager_region": "us-east-1",
        }
        props1 = Properties({**base, "secrets_manager_endpoint": "https://ep1.example.com"})
        props2 = Properties({**base, "secrets_manager_endpoint": "https://ep2.example.com"})
        plugin = AsyncAwsSecretsManagerPlugin(_svc(props1), props1)
        host = HostInfo("h", 5432)

        calls = [0]

        def _fetch(*args, **kwargs):
            calls[0] += 1
            return {"username": "u", "password": "p"}

        with patch.object(AsyncAwsSecretsManagerPlugin, "_fetch_secret_blocking",
                          side_effect=_fetch):
            await plugin._resolve_credentials(host, props1)
            await plugin._resolve_credentials(host, props2)
        assert calls[0] == 2

    asyncio.run(_body())


def test_secrets_plugin_json_decode_error_raises_json_decode_key():
    """A non-JSON SecretString (JSONDecodeError) surfaces as the JsonDecodeError
    message key rather than being swallowed to {} (sync aws_secrets_manager_plugin.py:171-174)."""
    async def _body():
        from json import JSONDecodeError

        props = Properties({
            "host": "h", "port": "5432",
            "secrets_manager_secret_id": "my-secret",
            "secrets_manager_region": "us-east-1",
        })
        plugin = AsyncAwsSecretsManagerPlugin(_svc(props), props)
        host = HostInfo("h", 5432)

        def _bad(*args, **kwargs):
            raise JSONDecodeError("Expecting value", "not-json", 0)

        with patch.object(AsyncAwsSecretsManagerPlugin, "_fetch_secret_blocking",
                          side_effect=_bad):
            with pytest.raises(AwsWrapperError) as exc:
                await plugin._resolve_credentials(host, props)
        assert str(exc.value).startswith(
            "[AwsSecretsManagerPlugin] Error occurred while retrieving credentials")

    asyncio.run(_body())


def test_iam_plugin_registers_token_cache_size_gauge():
    """iam.token_cache.size gauge is registered with a callback reflecting the
    live cache size (sync iam_plugin.py:63-64)."""
    props = _iam_props()
    gauges: dict = {}

    def _create_gauge(name, callback):
        g = MagicMock(name=f"gauge:{name}")
        gauges[name] = (g, callback)
        return g

    fake_tf = MagicMock()
    fake_tf.create_counter = MagicMock(return_value=MagicMock())
    fake_tf.create_gauge = MagicMock(side_effect=_create_gauge)

    svc = AsyncPluginServiceImpl(props, MagicMock(), HostInfo(_RDS_HOST, 5432))
    svc.set_telemetry_factory(fake_tf)
    plugin = AsyncIamAuthPlugin(svc, props)

    assert "iam.token_cache.size" in gauges
    _, callback = gauges["iam.token_cache.size"]
    assert callback() == 0
    plugin._token_cache["k"] = ("t", 1.0)
    assert callback() == 1
