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

"""Async auth plugins: IAM, Secrets Manager.

The underlying AWS SDKs (boto3 for IAM token generation; botocore for
Secrets Manager) are sync-only. Running them directly would block the
event loop. This module wraps the blocking call in
``asyncio.to_thread`` so the plugin pipeline stays non-blocking even
though the SDK call itself runs on a thread.

3.0.0 ships async IAM + async Secrets Manager. Federated (SAML) and
Okta async ports depend on ``requests``/``aiohttp`` decisions that
warrant their own sub-project brainstorm; skeletons are provided so
users can subclass ``AsyncAuthPluginBase`` for custom flows.
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Optional, Set,
                    Tuple)

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.aws_credentials_manager import \
    AwsCredentialsManager
from aws_advanced_python_wrapper.aws_secrets_manager_plugin import Secret
from aws_advanced_python_wrapper.errors import AwsConnectError, AwsWrapperError
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.iam_utils import IamAuthUtils, TokenInfo
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import WrapperProperties
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils
from aws_advanced_python_wrapper.utils.region_utils import (GdbRegionUtils,
                                                            RegionUtils)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties

logger = Logger(__name__)


def _resolve_iam_region(
        props: Properties,
        host: Optional[str],
        host_info: HostInfo) -> Optional[str]:
    """Resolve the AWS region for IAM-token generation, mirroring sync
    ``IamAuthPlugin._connect`` (iam_plugin.py:87-93): GDB writer-cluster hosts
    use :class:`GdbRegionUtils`, everything else :class:`RegionUtils`, and both
    fall back to auto-discovery from the RDS hostname when ``iam_region`` is
    unset."""
    rds_type = RdsUtils().identify_rds_type(host)
    region_utils: RegionUtils = (
        GdbRegionUtils()
        if rds_type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER
        else RegionUtils())
    return region_utils.get_region(
        props, WrapperProperties.IAM_REGION.name, host, host_info)


class AsyncAuthPluginBase(AsyncPlugin):
    """Common shell for async auth plugins.

    Subclasses override :meth:`_resolve_credentials` to return a
    ``(user, password, was_cached)`` tuple. The base class handles
    plugin-pipeline wiring, credential injection, and retry-on-login
    when cached credentials fail authentication.
    """

    _SUBSCRIBED: Set[str] = {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.FORCE_CONNECT.method_name,
    }

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._props = props

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._SUBSCRIBED)

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        self._prepare_secure_transport(driver_dialect, props)
        return await self._connect_with_retry(host_info, props, connect_func)

    async def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable[..., Awaitable[Any]]) -> Any:
        self._prepare_secure_transport(driver_dialect, props)
        return await self._connect_with_retry(host_info, props, force_connect_func)

    def _prepare_secure_transport(
            self, driver_dialect: AsyncDriverDialect, props: Properties) -> None:
        """Hook to ensure secure transport before connecting. No-op by default;
        overridden by IAM (which sends the token in cleartext and requires TLS)."""
        pass

    async def _connect_with_retry(
            self,
            host_info: HostInfo,
            props: Properties,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        """Resolve creds, inject, connect; retry once if cached creds
        cause a login failure.

        Error-key mapping mirrors sync ``IamAuthPlugin._connect`` /
        ``AwsSecretsManagerPlugin._connect``: a network exception becomes an
        :class:`AwsConnectError` (still classified as a network exception by
        the dialect handlers, so failover recognizes it), the first terminal
        failure becomes ``*.ConnectException``, and the post-refetch retry
        failure becomes ``*.UnhandledException``.
        """
        user, password, was_cached = await self._resolve_credentials(host_info, props)
        self._inject_credentials(props, user, password)
        try:
            return await connect_func()
        except Exception as exc:
            # Network / failover exceptions -> AwsConnectError (sync
            # iam_plugin.py:138-139 / aws_secrets_manager_plugin.py:125-126).
            # AwsConnectError is-a network exception per the dialect handlers,
            # so the failover plugin still triggers on it.
            if self._plugin_service.is_network_exception(error=exc):
                raise self._wrap_network_exception(exc) from exc
            # Non-login failure, or a login failure with FRESH (non-cached)
            # credentials, is terminal (sync iam_plugin.py:142-143 /
            # aws_secrets_manager_plugin.py:128-130).
            if not was_cached or not self._plugin_service.is_login_exception(error=exc):
                raise self._wrap_connect_exception(exc) from exc
            # Cached credentials failed auth -- invalidate, refetch, retry once.
            self._invalidate_cache(host_info, props)
            user, password, _ = await self._resolve_credentials(host_info, props)
            self._inject_credentials(props, user, password)
            try:
                return await connect_func()
            except Exception as retry_exc:
                # Sync wraps the post-refetch failure as UnhandledException
                # unconditionally (no network re-check): iam_plugin.py:159-160 /
                # aws_secrets_manager_plugin.py:138-141.
                raise self._wrap_retry_exception(retry_exc) from retry_exc

    def _wrap_network_exception(self, exc: Exception) -> AwsConnectError:
        """Wrap a network/failover connect failure as :class:`AwsConnectError`.

        Subclasses override to supply a plugin-specific message. The base
        default preserves an already-wrapped error and otherwise wraps
        generically.
        """
        if isinstance(exc, AwsConnectError):
            return exc
        return AwsConnectError(str(exc), exc)

    def _wrap_connect_exception(self, exc: Exception) -> AwsWrapperError:
        """Wrap the first terminal connect/login failure as
        :class:`AwsWrapperError`. Subclasses override for a plugin-specific
        message (parity with the sync plugins' ``*.ConnectException``)."""
        if isinstance(exc, AwsWrapperError):
            return exc
        return AwsWrapperError(str(exc), exc)

    def _wrap_retry_exception(self, exc: Exception) -> AwsWrapperError:
        """Wrap the post-refetch retry failure as :class:`AwsWrapperError`.
        Subclasses override for the plugin-specific ``*.UnhandledException``
        message."""
        if isinstance(exc, AwsWrapperError):
            return exc
        return AwsWrapperError(str(exc), exc)

    @staticmethod
    def _inject_credentials(
            props: Properties,
            user: Optional[str],
            password: Optional[str]) -> None:
        if user is not None:
            props["user"] = user
        if password is not None:
            props["password"] = password

    async def _resolve_credentials(
            self,
            host_info: HostInfo,
            props: Properties) -> Tuple[Optional[str], Optional[str], bool]:
        """Return ``(user, password, was_cached)`` for the given host.

        ``was_cached=True`` when the credentials were served from cache
        (so a login failure should trigger invalidation + one retry).
        """
        raise NotImplementedError

    def _invalidate_cache(
            self,
            host_info: HostInfo,
            props: Properties) -> None:
        """Drop any cached credentials for this (host, props) so a
        subsequent ``_resolve_credentials`` call generates fresh ones.

        Default no-op so subclasses that don't cache can ignore.
        """


class AsyncIamAuthPlugin(AsyncAuthPluginBase):
    """Async IAM DB Auth.

    Generates an RDS auth token via boto3 (sync SDK) executed in a thread
    so the event loop isn't blocked. Caches the generated token per
    (host, port, user, region) tuple until it expires.
    """

    _DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60  # 15 minutes

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        super().__init__(plugin_service, props)
        # Process-wide token cache shared with sync IamAuthPlugin
        # (iam_plugin.py:58-59). Plugin instances are rebuilt on every
        # connect(), so an instance-level cache would regenerate the token
        # for each new connection; the shared StorageService (same TokenInfo
        # type + IamAuthUtils.get_cache_key keys as sync) survives across
        # connections and across sync/async wrappers in the same process.
        self._storage_service = services_container.get_storage_service()
        self._storage_service.register(
            TokenInfo, item_expiration_time=timedelta(minutes=15))
        # Telemetry counter + cache-size gauge -- matches sync iam_plugin.py:62-64.
        tf = self._plugin_service.get_telemetry_factory()
        self._fetch_token_counter = tf.create_counter("iam.fetch_token.count")
        self._cache_size_gauge = tf.create_gauge(
            "iam.token_cache.size",
            lambda: self._storage_service.size(TokenInfo))

    def _prepare_secure_transport(
            self, driver_dialect: AsyncDriverDialect, props: Properties) -> None:
        # IAM auth sends the token via MySQL's mysql_clear_password plugin, i.e.
        # in CLEARTEXT, which the driver only does over TLS. psycopg gets TLS
        # from sslmode=require and mysql.connector negotiates it by default, but
        # aiomysql does NOT auto-negotiate TLS -- so without this the token is
        # never sent and the server reports "Access denied ... (using password:
        # NO)" (test_iam_*_async). Only aiomysql needs the nudge.
        if driver_dialect.driver_name != "aiomysql":
            return
        # Respect any TLS config the caller already provided -- this is the
        # supported path to a *verifying* connection: pass the RDS CA bundle
        # via ``ssl_ca`` (or a fully-configured ``ssl`` context).
        if props.get("ssl") is not None or props.get("ssl_ca") is not None:
            return

        import ssl as _ssl
        ctx = _ssl.create_default_context()
        # Encrypt-but-don't-verify, matching the SYNC driver's default exactly.
        # The Amazon RDS CA is NOT in the system trust store, so a verifying
        # context would fail the handshake against a real RDS endpoint when no
        # ``ssl_ca`` was supplied. The sync path reaches the same posture for
        # free: mysql.connector negotiates TLS by default with
        # ``ssl_verify_cert=False`` (encrypted, cert not verified) and only
        # verifies when the user passes ``ssl_ca``. aiomysql does NOT
        # auto-negotiate TLS, so we must build the context explicitly to get an
        # encrypted channel for the cleartext token -- but we deliberately do
        # NOT verify and do NOT warn, so the observable behavior matches the
        # sync IamAuthPlugin (which is silent here). Verification stays opt-in
        # via ``ssl_ca`` (handled by the early-return above).
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        props["ssl"] = ctx

    def _default_port(self) -> int:
        dialect = self._plugin_service.database_dialect
        if dialect is not None:
            return dialect.default_port
        return 5432

    def _cache_key_for(
            self,
            host_info: HostInfo,
            props: Properties) -> Optional[str]:
        """Return the IAM-token cache key for ``(host_info, props)`` or
        ``None`` if the inputs don't contain enough info to build one.

        Encapsulates host / port / region derivation so
        ``_resolve_credentials`` and ``_invalidate_cache`` stay aligned.
        """
        user = WrapperProperties.USER.get(props)
        if not user:
            return None
        host = IamAuthUtils.get_iam_host(props, host_info)
        port = IamAuthUtils.get_port(props, host_info, self._default_port())
        region = _resolve_iam_region(props, host, host_info)
        if not region:
            return None
        return IamAuthUtils.get_cache_key(user, host, port, region)

    async def _resolve_credentials(
            self,
            host_info: HostInfo,
            props: Properties) -> Tuple[Optional[str], Optional[str], bool]:
        user = WrapperProperties.USER.get(props)
        if not user:
            raise AwsWrapperError(Messages.get_formatted(
                "IamAuthPlugin.IsNoneOrEmpty", WrapperProperties.USER.name))

        host = IamAuthUtils.get_iam_host(props, host_info)
        port = IamAuthUtils.get_port(props, host_info, self._default_port())

        region = _resolve_iam_region(props, host, host_info)
        if not region:
            logger.debug("RdsUtils.UnsupportedHostname", host)
            raise AwsWrapperError(
                Messages.get_formatted("RdsUtils.UnsupportedHostname", host))

        cache_key = IamAuthUtils.get_cache_key(user, host, port, region)

        ttl_sec = WrapperProperties.IAM_EXPIRATION.get_int(props)
        if not ttl_sec:
            ttl_sec = self._DEFAULT_TOKEN_EXPIRATION_SEC

        # Sync-parity lookup (iam_plugin.py:60-64): wall-clock TokenInfo
        # expiry, no regeneration grace window.
        token_info = self._storage_service.get(TokenInfo, cache_key)
        if token_info is not None and not token_info.is_expired():
            return user, token_info.token, True

        if self._fetch_token_counter is not None:
            self._fetch_token_counter.inc()
        token_expiry = datetime.now() + timedelta(seconds=ttl_sec)
        token = await asyncio.to_thread(
            self._generate_token_blocking, host_info, props, user, host, int(port), region
        )
        self._storage_service.put(
            TokenInfo, cache_key, TokenInfo(token, token_expiry))
        return user, token, False

    def _invalidate_cache(
            self,
            host_info: HostInfo,
            props: Properties) -> None:
        """Drop the cached IAM token for this (host, port, user, region)
        so the next ``_resolve_credentials`` call regenerates it.

        Called by :class:`AsyncAuthPluginBase` when cached credentials
        fail authentication (retry-on-login path).
        """
        cache_key = self._cache_key_for(host_info, props)
        if cache_key is not None:
            self._storage_service.remove(TokenInfo, cache_key)

    def _wrap_network_exception(self, exc: Exception) -> AwsConnectError:
        # Parity with sync IamAuthPlugin._connect:139.
        if isinstance(exc, AwsConnectError):
            return exc
        return AwsConnectError(
            Messages.get_formatted("IamAuthPlugin.ConnectException", exc))

    def _wrap_connect_exception(self, exc: Exception) -> AwsWrapperError:
        # Parity with sync IamAuthPlugin._connect:143.
        if isinstance(exc, AwsWrapperError):
            return exc
        return AwsWrapperError(
            Messages.get_formatted("IamAuthPlugin.ConnectException", exc), exc)

    def _wrap_retry_exception(self, exc: Exception) -> AwsWrapperError:
        # Parity with sync IamAuthPlugin._connect:160.
        if isinstance(exc, AwsWrapperError):
            return exc
        return AwsWrapperError(
            Messages.get_formatted("IamAuthPlugin.UnhandledException", exc), exc)

    def _generate_token_blocking(
            self,
            host_info: HostInfo,
            props: Properties,
            user: str,
            host: str,
            port: int,
            region: Optional[str]) -> str:
        """Generate an RDS IAM auth token on a worker thread.

        Routes through :class:`AwsCredentialsManager` + :meth:`IamAuthUtils.
        generate_authentication_token` (rather than raw ``boto3.client``) so
        ``aws_profile`` / custom credential providers apply and sessions are
        reused -- parity with sync iam_plugin.py:121-128."""
        session = AwsCredentialsManager.get_session(host_info, props, region)
        # generate_authentication_token is typed for the sync PluginService but
        # only calls get_telemetry_factory(), which AsyncPluginService also has.
        return IamAuthUtils.generate_authentication_token(
            self._plugin_service,  # type: ignore[arg-type]
            user, host, port, region, session)


class AsyncAwsSecretsManagerPlugin(AsyncAuthPluginBase):
    """Async AWS Secrets Manager auth plugin.

    Fetches user/password from a named secret. Parses both Secrets
    Manager's default JSON shape (``{"username": "...", "password": "..."}``)
    and the common RDS-auto-created ``{"username": ..., "password": ...}``
    schema.

    Features (E.3):

    * Per-entry TTL honored via ``SECRETS_MANAGER_EXPIRATION`` (seconds);
      negative or absent falls back to 1 year, matching the sync plugin's
      "effectively forever" sentinel.
    * Optional custom endpoint via ``SECRETS_MANAGER_ENDPOINT`` (for VPC
      endpoint / test doubles) forwarded to ``boto3.client`` as
      ``endpoint_url=``.
    * ARN-shaped ``secret_id`` (``arn:aws:secretsmanager:<region>:...``)
      provides the region when ``SECRETS_MANAGER_REGION`` is absent.
    """

    # Extract region from ARN: arn:aws:secretsmanager:<region>:<account>:secret:<name>
    _ARN_REGION_RE = re.compile(
        r"^arn:aws:secretsmanager:(?P<region>[a-z0-9-]+):")

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        super().__init__(plugin_service, props)
        # Process-wide secret cache shared with sync AwsSecretsManagerPlugin
        # (aws_secrets_manager_plugin.py:73-74): same Secret type key and the
        # same 3-tuple ``(secret_id, region, endpoint)`` cache key. Plugin
        # instances are rebuilt on every connect(), so an instance-level cache
        # would call get_secret_value on each new connection.
        self._storage_service = services_container.get_storage_service()
        self._storage_service.register(
            Secret, item_expiration_time=timedelta(minutes=30))
        # Telemetry counter -- matches sync aws_secrets_manager_plugin.py:89.
        tf = self._plugin_service.get_telemetry_factory()
        self._fetch_secret_counter = tf.create_counter(
            "secrets_manager.fetch_credentials.count")

    async def _resolve_credentials(
            self,
            host_info: HostInfo,
            props: Properties) -> Tuple[Optional[str], Optional[str], bool]:
        cache_key = self._secret_key_for(props)
        secret_id, region, endpoint = cache_key

        # Sync-parity lookup (aws_secrets_manager_plugin.py:158-159): the
        # shared StorageService owns expiration (30-min registration, or the
        # SECRETS_MANAGER_EXPIRATION override applied at put time).
        cached_secret = self._storage_service.get(Secret, cache_key)
        if cached_secret is not None:
            user_key, password_key = self._credential_keys(props)
            return (getattr(cached_secret.value, user_key, None),
                    getattr(cached_secret.value, password_key, None),
                    True)

        if self._fetch_secret_counter is not None:
            self._fetch_secret_counter.inc()
        # Wrap raw botocore/parse errors in AwsWrapperError, mirroring the sync
        # plugin's _update_secret (aws_secrets_manager_plugin.py:167-182). A bad
        # secret id raises ClientError (ResourceNotFoundException); a bad region
        # raises EndpointConnectionError; a non-JSON SecretString raises
        # JSONDecodeError. Callers (and the negative-path tests) expect
        # AwsWrapperError, not the raw exception.
        from json import JSONDecodeError

        from botocore.exceptions import ClientError, EndpointConnectionError
        try:
            secret = await asyncio.to_thread(
                self._fetch_secret_blocking, host_info, props, secret_id, region, endpoint
            )
        except (ClientError, AttributeError) as e:
            raise AwsWrapperError(
                Messages.get_formatted(
                    "AwsSecretsManagerPlugin.FailedToFetchDbCredentials", e), e) from e
        except JSONDecodeError as e:
            raise AwsWrapperError(
                Messages.get_formatted(
                    "AwsSecretsManagerPlugin.JsonDecodeError", e), e) from e
        except EndpointConnectionError as e:
            raise AwsWrapperError(
                Messages.get_formatted(
                    "AwsSecretsManagerPlugin.EndpointOverrideInvalidConnection", endpoint), e) from e
        except ValueError as e:
            raise AwsWrapperError(
                Messages.get_formatted(
                    "AwsSecretsManagerPlugin.EndpointOverrideMisconfigured", endpoint), e) from e
        user_key, password_key = self._credential_keys(props)
        user = secret.get(user_key)
        password = secret.get(password_key)

        # Store the FULL secret as sync does (Secret(SimpleNamespace), 30-min
        # registered expiry); honor the async SECRETS_MANAGER_EXPIRATION
        # override via put's per-item expiration when explicitly set.
        ttl_sec = WrapperProperties.SECRETS_MANAGER_EXPIRATION.get_int(props)
        if ttl_sec is not None and ttl_sec >= 0:
            self._storage_service.put(
                Secret, cache_key, Secret(SimpleNamespace(**secret)),
                item_expiration_ns=int(ttl_sec * 1_000_000_000))
        else:
            self._storage_service.put(
                Secret, cache_key, Secret(SimpleNamespace(**secret)))
        return user, password, False

    @staticmethod
    def _credential_keys(props: Properties) -> Tuple[str, str]:
        # Allow custom field names via *_KEY properties (e.g. Terraform secrets
        # with non-default schemas).
        user_key = (
            WrapperProperties.SECRETS_MANAGER_SECRET_USERNAME_KEY.get(props)
            or "username"
        )
        password_key = (
            WrapperProperties.SECRETS_MANAGER_SECRET_PASSWORD_KEY.get(props)
            or "password"
        )
        return user_key, password_key

    def _secret_key_for(
            self, props: Properties) -> Tuple[str, Optional[str], Optional[str]]:
        """Return the ``(secret_id, region, endpoint)`` cache key, raising
        ``MissingRequiredConfigParameter`` when a required value is absent.

        Mirrors sync ``AwsSecretsManagerPlugin.__init__`` + ``_get_rds_region``
        (aws_secrets_manager_plugin.py:76-86, 223-238): region comes from the
        explicit property first, then the secret ARN."""
        secret_id = WrapperProperties.SECRETS_MANAGER_SECRET_ID.get(props)
        if not secret_id:
            raise AwsWrapperError(Messages.get_formatted(
                "AwsSecretsManagerPlugin.MissingRequiredConfigParameter",
                WrapperProperties.SECRETS_MANAGER_SECRET_ID.name))
        # Raw props.get (not the WrapperProperty default) so ARN extraction only
        # kicks in when the user did not explicitly set a region.
        region = props.get(WrapperProperties.SECRETS_MANAGER_REGION.name)
        if not region:
            region = self._extract_region_from_arn(secret_id)
        if not region:
            raise AwsWrapperError(Messages.get_formatted(
                "AwsSecretsManagerPlugin.MissingRequiredConfigParameter",
                WrapperProperties.SECRETS_MANAGER_REGION.name))
        endpoint = WrapperProperties.SECRETS_MANAGER_ENDPOINT.get(props)
        return (secret_id, region, endpoint)

    def _invalidate_cache(
            self,
            host_info: HostInfo,
            props: Properties) -> None:
        """Drop the cached secret for this (secret_id, region, endpoint) so a
        subsequent ``_resolve_credentials`` call refetches it."""
        try:
            self._storage_service.remove(Secret, self._secret_key_for(props))
        except AwsWrapperError:
            # No valid key derivable (missing secret_id/region) -- nothing to drop.
            pass

    def _wrap_network_exception(self, exc: Exception) -> AwsConnectError:
        # Parity with sync AwsSecretsManagerPlugin._connect:126.
        if isinstance(exc, AwsConnectError):
            return exc
        return AwsConnectError(
            Messages.get_formatted("AwsSecretsManagerPlugin.ConnectException", exc))

    def _wrap_connect_exception(self, exc: Exception) -> AwsWrapperError:
        # Parity with sync AwsSecretsManagerPlugin._connect:129-130.
        if isinstance(exc, AwsWrapperError):
            return exc
        return AwsWrapperError(
            Messages.get_formatted("AwsSecretsManagerPlugin.ConnectException", exc), exc)

    def _wrap_retry_exception(self, exc: Exception) -> AwsWrapperError:
        # Parity with sync AwsSecretsManagerPlugin._connect:138-141.
        if isinstance(exc, AwsWrapperError):
            return exc
        return AwsWrapperError(
            Messages.get_formatted("AwsSecretsManagerPlugin.UnhandledException", exc), exc)

    @staticmethod
    def _extract_region_from_arn(secret_id: str) -> Optional[str]:
        match = AsyncAwsSecretsManagerPlugin._ARN_REGION_RE.match(secret_id)
        return match.group("region") if match else None

    def _fetch_secret_blocking(
            self,
            host_info: HostInfo,
            props: Properties,
            secret_id: str,
            region: Optional[str],
            endpoint: Optional[str] = None) -> dict:
        """Fetch + parse the secret on a worker thread.

        Routes through :class:`AwsCredentialsManager` so ``aws_profile`` /
        custom credential providers apply and the boto3 client is reused --
        parity with sync ``_fetch_latest_credentials``
        (aws_secrets_manager_plugin.py:200-207). A non-JSON ``SecretString``
        raises :class:`json.JSONDecodeError`, which the caller maps to the
        ``JsonDecodeError`` message key (parity with sync:171-174)."""
        session = AwsCredentialsManager.get_session(host_info, props, region)
        client = AwsCredentialsManager.get_client(
            "secretsmanager", session, host_info.host, region, endpoint)
        resp = client.get_secret_value(SecretId=secret_id)
        secret_str = resp.get("SecretString")
        if not secret_str:
            return {}
        return json.loads(secret_str)
