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

"""Async federated (SAML) and Okta auth plugins (Task 1-C + 1-D).

Both plugins:
1. Authenticate to an IdP (ADFS/generic SAML for federated; Okta's REST
   API for Okta) via aiohttp.
2. Exchange the SAML assertion for temporary AWS STS credentials via
   boto3 (sync SDK wrapped in ``asyncio.to_thread``).
3. Use STS credentials to generate an RDS IAM auth token.
4. Inject (user, token) as (user, password) into the connect props.

Token and SAML assertion caching mirrors the sync plugins'.
"""

from __future__ import annotations

import asyncio
import ssl as _ssl
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Tuple
from urllib.parse import urljoin

from aws_advanced_python_wrapper.aio.auth_plugins import AsyncAuthPluginBase
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.iam_utils import IamAuthUtils
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties


class _RdsTokenMixin:
    """Shared helper: STS (with SAML) -> temporary creds -> RDS IAM token."""

    _DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60
    _TOKEN_REGEN_GRACE_SEC = 60

    def __init__(self) -> None:
        self._rds_token_cache: dict = {}

    @staticmethod
    def _rds_token_cache_key(
            host: str,
            port: int,
            user: str,
            region: Optional[str]) -> Tuple[str, int, str, Optional[str]]:
        """Shape of the key used in ``_rds_token_cache``.

        Extracted so ``_resolve_credentials`` and ``_invalidate_cache``
        (plus any future code that touches the cache) stay aligned.
        """
        return (host, port, user, region)

    async def _sts_assume_role_with_saml(
            self,
            role_arn: str,
            idp_arn: str,
            saml_assertion_b64: str,
            region: Optional[str]) -> dict:
        """Return the STS credentials dict. boto3 wrapped in to_thread."""
        return await asyncio.to_thread(
            self._sts_assume_role_with_saml_blocking,
            role_arn, idp_arn, saml_assertion_b64, region,
        )

    @staticmethod
    def _sts_assume_role_with_saml_blocking(
            role_arn: str,
            idp_arn: str,
            saml_assertion_b64: str,
            region: Optional[str]) -> dict:
        import boto3
        kwargs: dict = {}
        if region:
            kwargs["region_name"] = region
        client = boto3.client("sts", **kwargs)
        resp = client.assume_role_with_saml(
            RoleArn=role_arn,
            PrincipalArn=idp_arn,
            SAMLAssertion=saml_assertion_b64,
        )
        return resp["Credentials"]

    async def _generate_rds_token(
            self,
            host: str,
            port: int,
            user: str,
            region: Optional[str],
            creds: dict) -> str:
        return await asyncio.to_thread(
            self._generate_rds_token_blocking,
            host, port, user, region, creds,
        )

    @staticmethod
    def _generate_rds_token_blocking(
            host: str,
            port: int,
            user: str,
            region: Optional[str],
            creds: dict) -> str:
        import boto3
        kwargs: dict = {
            "aws_access_key_id": creds["AccessKeyId"],
            "aws_secret_access_key": creds["SecretAccessKey"],
            "aws_session_token": creds["SessionToken"],
        }
        if region:
            kwargs["region_name"] = region
        client = boto3.client("rds", **kwargs)
        return client.generate_db_auth_token(
            DBHostname=host,
            Port=port,
            DBUsername=user,
        )

    async def _cached_rds_token(
            self,
            host: str,
            port: int,
            user: str,
            region: Optional[str]) -> Optional[str]:
        cached = self._rds_token_cache.get(
            self._rds_token_cache_key(host, port, user, region))
        if cached is None:
            return None
        token, expires_at = cached
        now = asyncio.get_event_loop().time()
        if now < expires_at - self._TOKEN_REGEN_GRACE_SEC:
            return token
        return None

    def _store_rds_token(
            self,
            host: str,
            port: int,
            user: str,
            region: Optional[str],
            token: str) -> None:
        expires_at = asyncio.get_event_loop().time() + self._DEFAULT_TOKEN_EXPIRATION_SEC
        self._rds_token_cache[
            self._rds_token_cache_key(host, port, user, region)
        ] = (token, expires_at)


class AsyncFederatedAuthPlugin(AsyncAuthPluginBase, _RdsTokenMixin):
    """ADFS / generic SAML -> STS -> RDS IAM token.

    Connection properties (shared with sync :class:`FederatedAuthPlugin`):
      * ``db_user``: the RDS DB user the token authenticates as.
      * ``idp_endpoint``: ADFS / SAML endpoint hostname.
      * ``idp_port`` (default 443)
      * ``idp_username`` / ``idp_password``: IdP-side credentials.
      * ``iam_role_arn`` / ``iam_idp_arn``: AWS side.
      * ``iam_region``: region for STS + RDS calls.
      * ``iam_host``: the RDS host the IAM token authenticates against.
      * ``iam_default_port`` (default 5432).
      * ``ssl_secure`` (bool, default True): verify IdP SSL cert.
      * ``http_request_connect_timeout`` (sec, default 60).
    """

    # Counter name is a class-level attribute so subclasses (AsyncOkta)
    # can override it for distinct per-IdP metrics matching sync
    # (``federated.fetch_token.count`` vs ``okta.fetch_token.count``).
    _FETCH_TOKEN_COUNTER_NAME: ClassVar[str] = "federated.fetch_token.count"

    def __init__(self, plugin_service: Any, props: Properties) -> None:
        AsyncAuthPluginBase.__init__(self, plugin_service, props)
        _RdsTokenMixin.__init__(self)
        # Telemetry counter -- matches sync federated_plugin.py:69 /
        # okta_plugin.py:65. Counter name is pulled from the class-level
        # attribute so AsyncOkta can distinguish its IdP without any
        # custom __init__.
        tf = self._plugin_service.get_telemetry_factory()
        self._fetch_token_counter = tf.create_counter(
            self._FETCH_TOKEN_COUNTER_NAME)

    def _default_port(self) -> int:
        """Dialect-aware default port fallback.

        Uses database_dialect.default_port when available (populated by
        AsyncAwsWrapperConnection.connect); falls back to 5432 only if
        no dialect is bound. Matches E.2's pattern for AsyncIamAuthPlugin.
        """
        dialect = self._plugin_service.database_dialect
        if dialect is not None:
            return dialect.default_port
        return 5432

    async def _resolve_credentials(
            self,
            host_info: HostInfo,
            props: Properties) -> Tuple[Optional[str], Optional[str], bool]:
        db_user = WrapperProperties.DB_USER.get(props)
        if not db_user:
            raise AwsWrapperError(
                "Federated auth requires 'db_user' connection property"
            )
        host = WrapperProperties.IAM_HOST.get(props) or host_info.host
        port = IamAuthUtils.get_port(props, host_info, self._default_port())
        region = WrapperProperties.IAM_REGION.get(props)

        # 1. Token cache check before expensive SAML round-trip.
        cached = await self._cached_rds_token(host, int(port), db_user, region)
        if cached is not None:
            return db_user, cached, True

        # Cache miss: we'll generate a fresh RDS IAM token below. Emit
        # the counter here so it covers both the federated and Okta
        # flows. AsyncOktaAuthPlugin overrides _fetch_saml_assertion but
        # reuses this resolve path; it also overrides
        # _FETCH_TOKEN_COUNTER_NAME so its emissions land on the distinct
        # ``okta.fetch_token.count`` metric.
        if self._fetch_token_counter is not None:
            self._fetch_token_counter.inc()

        # 2. Fetch SAML assertion from IdP.
        saml_assertion = await self._fetch_saml_assertion(props)

        # 3. STS exchange.
        role_arn = WrapperProperties.IAM_ROLE_ARN.get(props)
        idp_arn = WrapperProperties.IAM_IDP_ARN.get(props)
        if not role_arn or not idp_arn:
            raise AwsWrapperError(
                "Federated auth requires 'iam_role_arn' and 'iam_idp_arn'"
            )
        creds = await self._sts_assume_role_with_saml(
            role_arn, idp_arn, saml_assertion, region,
        )

        # 4. Generate RDS IAM token.
        token = await self._generate_rds_token(
            host, int(port), db_user, region, creds,
        )
        self._store_rds_token(host, int(port), db_user, region, token)
        return db_user, token, False

    def _invalidate_cache(
            self,
            host_info: HostInfo,
            props: Properties) -> None:
        """Drop the cached RDS IAM token for this (host, port, user,
        region) so a subsequent ``_resolve_credentials`` call regenerates
        it via a fresh SAML assertion + STS exchange.

        Called by :class:`AsyncAuthPluginBase` when cached credentials
        fail authentication (retry-on-login path from E.1).
        """
        db_user = WrapperProperties.DB_USER.get(props)
        if not db_user:
            return
        host = WrapperProperties.IAM_HOST.get(props) or host_info.host
        port = IamAuthUtils.get_port(props, host_info, self._default_port())
        region = WrapperProperties.IAM_REGION.get(props)
        cache_key = self._rds_token_cache_key(
            host, int(port), db_user, region)
        self._rds_token_cache.pop(cache_key, None)

    async def _fetch_saml_assertion(self, props: Properties) -> str:
        """ADFS SAML assertion via HTTP POST with IdP username/password.

        Subclasses (:class:`AsyncOktaAuthPlugin`) override this to drive
        their own auth flow.
        """
        import aiohttp
        idp_endpoint = WrapperProperties.IDP_ENDPOINT.get(props)
        idp_username = WrapperProperties.IDP_USERNAME.get(props)
        idp_password = WrapperProperties.IDP_PASSWORD.get(props)
        if not (idp_endpoint and idp_username and idp_password):
            raise AwsWrapperError(
                "Federated auth requires idp_endpoint, idp_username, idp_password"
            )
        port_raw = WrapperProperties.IDP_PORT.get(props)
        port = int(port_raw) if port_raw else 443
        verify = WrapperProperties.SSL_SECURE.get_bool(props)
        if verify is None:
            verify = True
        timeout_raw = WrapperProperties.HTTP_REQUEST_TIMEOUT.get(props)
        timeout = float(timeout_raw) if timeout_raw else 60.0

        # ADFS SAML auth URL.
        relying_party = (
            WrapperProperties.RELAYING_PARTY_ID.get(props)
            or "urn:amazon:webservices"
        )
        url = (
            f"https://{idp_endpoint}:{port}/adfs/ls/IdpInitiatedSignOn.aspx?"
            f"loginToRp={relying_party}"
        )
        auth = aiohttp.BasicAuth(str(idp_username), str(idp_password))
        ssl_ctx: Any = _ssl.create_default_context()
        if not verify:
            ssl_ctx = False  # aiohttp accepts ``False`` to disable

        async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=timeout),
                auth=auth,
                trust_env=True,
        ) as session:
            async with session.get(url, ssl=ssl_ctx) as resp:
                body = await resp.text()
        return self._extract_saml_assertion(body)

    @staticmethod
    def _extract_saml_assertion(html: str) -> str:
        """Extract the base64-encoded SAMLResponse from an ADFS HTML page."""
        import re
        m = re.search(
            r'name="SAMLResponse"\s+value="([^"]+)"',
            html,
        )
        if not m:
            raise AwsWrapperError(
                "Could not extract SAMLResponse from IdP page"
            )
        return m.group(1)


class AsyncOktaAuthPlugin(AsyncFederatedAuthPlugin):
    """Okta -> SAML -> STS -> RDS IAM token (Task 1-D).

    Overrides :meth:`_fetch_saml_assertion` to drive Okta's REST auth
    + app SSO flow instead of ADFS's GET-with-BasicAuth.

    Connection properties specific to Okta (shared with sync plugin):
      * ``app_id``: Okta application ID (the SSO URL includes this).
      * ``idp_endpoint``: Okta org domain (e.g., "mycorp.okta.com").
      * ``idp_username`` / ``idp_password`` / ``iam_role_arn`` /
        ``iam_idp_arn`` / ``iam_region``: shared with federated base.
    """

    # Distinct metric per IdP -- matches sync okta_plugin.py:65.
    _FETCH_TOKEN_COUNTER_NAME: ClassVar[str] = "okta.fetch_token.count"

    _OKTA_AUTHN_PATH = "/api/v1/authn"
    _OKTA_APP_SAML_PATH_TEMPLATE = "/app/amazon_aws/{app_id}/sso/saml"

    async def _fetch_saml_assertion(self, props: Properties) -> str:
        import aiohttp
        idp_endpoint = WrapperProperties.IDP_ENDPOINT.get(props)
        idp_username = WrapperProperties.IDP_USERNAME.get(props)
        idp_password = WrapperProperties.IDP_PASSWORD.get(props)
        app_id = WrapperProperties.APP_ID.get(props)
        if not (idp_endpoint and idp_username and idp_password and app_id):
            raise AwsWrapperError(
                "Okta auth requires idp_endpoint, idp_username, "
                "idp_password, and app_id"
            )
        verify = WrapperProperties.SSL_SECURE.get_bool(props)
        if verify is None:
            verify = True
        timeout_raw = WrapperProperties.HTTP_REQUEST_TIMEOUT.get(props)
        timeout = float(timeout_raw) if timeout_raw else 60.0

        ssl_ctx: Any = _ssl.create_default_context()
        if not verify:
            ssl_ctx = False

        org_base = f"https://{idp_endpoint}"
        async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=timeout),
                trust_env=True,
        ) as session:
            # Step 1: primary authentication.
            authn_url = urljoin(org_base, self._OKTA_AUTHN_PATH)
            async with session.post(
                    authn_url,
                    json={
                        "username": str(idp_username),
                        "password": str(idp_password),
                    },
                    ssl=ssl_ctx,
            ) as resp:
                authn = await resp.json()
            session_token = authn.get("sessionToken")
            if not session_token:
                raise AwsWrapperError(
                    f"Okta authn did not return sessionToken: status={authn.get('status')}"
                )

            # Step 2: exchange session token for SAML assertion via app SSO.
            sso_path = self._OKTA_APP_SAML_PATH_TEMPLATE.format(app_id=str(app_id))
            sso_url = f"{org_base}{sso_path}?onetimetoken={session_token}"
            async with session.get(sso_url, ssl=ssl_ctx) as resp:
                body = await resp.text()
        return self._extract_saml_assertion(body)

    @staticmethod
    def _extract_saml_assertion(html: str) -> str:
        """Okta's SAML form has additional attributes (``type``, ``id``)
        between ``name="SAMLResponse"`` and ``value=`` -- the ADFS base
        regex (``\\s+`` between name and value) doesn't match. Use a
        lazier pattern that tolerates intermediate attributes. Matches
        the sync :class:`OktaAuthPlugin` intent
        (``okta_plugin.py:178``, which uses ``.*`` between attrs).
        """
        import re
        m = re.search(
            r'name="SAMLResponse"[^>]*?\svalue="([^"]+)"',
            html,
        )
        if not m:
            raise AwsWrapperError(
                "Could not extract SAMLResponse from Okta SSO page"
            )
        return m.group(1)
