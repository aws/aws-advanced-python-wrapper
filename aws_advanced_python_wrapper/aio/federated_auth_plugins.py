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

"""Async federated (SAML) and Okta auth plugins.

Both plugins:
1. Authenticate to an IdP (ADFS/generic SAML for federated; Okta's REST
   API for Okta) via aiohttp and scrape a base64 SAML assertion.
2. Exchange the SAML assertion for temporary AWS STS credentials.
3. Use those STS credentials to generate an RDS IAM auth token.
4. Inject (db_user, token) as (user, password) into the connect props.

Every AWS SDK call routes through :class:`AwsCredentialsManager` +
:class:`IamAuthUtils` (executed on a worker thread via ``asyncio.to_thread``)
so ``aws_profile`` / custom credential providers apply -- parity with sync
``FederatedAuthPlugin`` / ``OktaAuthPlugin`` + ``SamlCredentialsProviderFactory``.
"""

from __future__ import annotations

import asyncio
import json
import re
import ssl as _ssl
from datetime import datetime, timedelta
from html import unescape
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Tuple
from urllib.parse import urlencode, urljoin

from aws_advanced_python_wrapper.aio.auth_plugins import (AsyncAuthPluginBase,
                                                          _resolve_iam_region)
from aws_advanced_python_wrapper.aws_credentials_manager import \
    AwsCredentialsManager
from aws_advanced_python_wrapper.errors import AwsConnectError, AwsWrapperError
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.iam_utils import IamAuthUtils, TokenInfo
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import WrapperProperties
from aws_advanced_python_wrapper.utils.saml_utils import SamlUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties

logger = Logger(__name__)


class _RdsTokenMixin:
    """Shared helper: STS (with SAML) -> temporary creds -> RDS IAM token."""

    _DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60 - 30  # matches IAM_TOKEN_EXPIRATION default

    def __init__(self) -> None:
        # Process-wide token cache shared with sync FederatedAuthPlugin /
        # OktaAuthPlugin (federated_plugin.py:65-66, okta_plugin.py:61-62):
        # same TokenInfo type and IamAuthUtils.get_cache_key keys. Plugin
        # instances are rebuilt on every connect(), so an instance-level cache
        # would redo the full SAML round-trip + STS AssumeRoleWithSAML for
        # each new connection.
        self._storage_service = services_container.get_storage_service()
        self._storage_service.register(
            TokenInfo, item_expiration_time=timedelta(minutes=30))

    @staticmethod
    def _rds_token_cache_key(
            host: str,
            port: int,
            user: str,
            region: Optional[str]) -> str:
        """Cache key for the RDS token -- sync-parity string form
        (IamAuthUtils.get_cache_key), shared with the sync plugins' entries.

        Extracted so ``_resolve_credentials`` and ``_invalidate_cache``
        (plus any future code that touches the cache) stay aligned.
        """
        return IamAuthUtils.get_cache_key(user, host, port, region)

    async def _sts_assume_role_with_saml(
            self,
            host_info: HostInfo,
            props: Properties,
            role_arn: str,
            idp_arn: str,
            saml_assertion_b64: str,
            region: Optional[str]) -> dict:
        """Return the STS credentials dict. boto3 wrapped in to_thread."""
        return await asyncio.to_thread(
            self._sts_assume_role_with_saml_blocking,
            host_info, props, role_arn, idp_arn, saml_assertion_b64, region,
        )

    @staticmethod
    def _sts_assume_role_with_saml_blocking(
            host_info: HostInfo,
            props: Properties,
            role_arn: str,
            idp_arn: str,
            saml_assertion_b64: str,
            region: Optional[str]) -> dict:
        # Route through AwsCredentialsManager (aws_profile / session reuse) --
        # parity with sync SamlCredentialsProviderFactory.get_aws_credentials
        # (credentials_provider_factory.py:38-49).
        session = AwsCredentialsManager.get_session(host_info, props, region)
        sts_client = AwsCredentialsManager.get_client(
            "sts", session, host_info.host, region)
        resp = sts_client.assume_role_with_saml(
            RoleArn=role_arn,
            PrincipalArn=idp_arn,
            SAMLAssertion=saml_assertion_b64,
        )
        return resp["Credentials"]

    async def _generate_rds_token(
            self,
            plugin_service: Any,
            host_info: HostInfo,
            props: Properties,
            user: str,
            host: str,
            port: int,
            region: Optional[str],
            creds: dict) -> str:
        return await asyncio.to_thread(
            self._generate_rds_token_blocking,
            plugin_service, host_info, props, user, host, port, region, creds,
        )

    @staticmethod
    def _generate_rds_token_blocking(
            plugin_service: Any,
            host_info: HostInfo,
            props: Properties,
            user: str,
            host: str,
            port: int,
            region: Optional[str],
            creds: dict) -> str:
        # Route through IamAuthUtils.generate_authentication_token with the STS
        # credentials -- parity with sync federated_plugin.py:170-177.
        session = AwsCredentialsManager.get_session(host_info, props, region)
        return IamAuthUtils.generate_authentication_token(
            plugin_service, user, host, port, region, session, creds)

    async def _cached_rds_token(
            self,
            host: str,
            port: int,
            user: str,
            region: Optional[str]) -> Optional[str]:
        # Sync-parity lookup (federated_plugin.py:114-118): wall-clock
        # TokenInfo expiry, no regeneration grace window.
        token_info = self._storage_service.get(
            TokenInfo, self._rds_token_cache_key(host, port, user, region))
        if token_info is not None and not token_info.is_expired():
            return token_info.token
        return None

    def _store_rds_token(
            self,
            host: str,
            port: int,
            user: str,
            region: Optional[str],
            token: str,
            ttl_sec: Optional[int] = None) -> None:
        if not ttl_sec:
            ttl_sec = self._DEFAULT_TOKEN_EXPIRATION_SEC
        token_expiry = datetime.now() + timedelta(seconds=ttl_sec)
        self._storage_service.put(
            TokenInfo,
            self._rds_token_cache_key(host, port, user, region),
            TokenInfo(token, token_expiry))


class AsyncFederatedAuthPlugin(AsyncAuthPluginBase, _RdsTokenMixin):
    """ADFS / generic SAML -> STS -> RDS IAM token.

    Connection properties (shared with sync :class:`FederatedAuthPlugin`):
      * ``db_user``: the RDS DB user the token authenticates as.
      * ``idp_endpoint``: ADFS / SAML endpoint hostname.
      * ``idp_port`` (default 443)
      * ``idp_username`` / ``idp_password``: IdP-side credentials.
      * ``iam_role_arn`` / ``iam_idp_arn``: AWS side.
      * ``iam_region``: region for STS + RDS calls (auto-discovered from the
        RDS host when unset).
      * ``iam_host``: the RDS host the IAM token authenticates against.
      * ``iam_default_port`` (default: dialect default).
      * ``ssl_secure`` (bool, default True): verify IdP SSL cert.
      * ``http_request_connect_timeout`` (sec, default 60).
    """

    # ADFS sign-in-page scraping patterns (parity with sync
    # AdfsCredentialsProviderFactory).
    _INPUT_TAG_PATTERN = r"<input(.+?)/>"
    _FORM_ACTION_PATTERN = r"<form.*?action=\"([^\"]+)\""

    # Counter name is a class-level attribute so subclasses (AsyncOkta)
    # can override it for distinct per-IdP metrics matching sync
    # (``federated.fetch_token.count`` vs ``okta.fetch_token.count``).
    _FETCH_TOKEN_COUNTER_NAME: ClassVar[str] = "federated.fetch_token.count"

    def __init__(self, plugin_service: Any, props: Properties) -> None:
        AsyncAuthPluginBase.__init__(self, plugin_service, props)
        _RdsTokenMixin.__init__(self)
        # Telemetry counter + cache-size gauge -- matches sync
        # federated_plugin.py:69-70 / okta_plugin.py:65-66. Counter name is
        # pulled from the class-level attribute so AsyncOkta distinguishes its
        # IdP without a custom __init__.
        tf = self._plugin_service.get_telemetry_factory()
        self._fetch_token_counter = tf.create_counter(
            self._FETCH_TOKEN_COUNTER_NAME)
        self._cache_size_gauge = tf.create_gauge(
            self._cache_size_gauge_name(),
            lambda: self._storage_service.size(TokenInfo))

    @classmethod
    def _cache_size_gauge_name(cls) -> str:
        # federated.token_cache.size / okta.token_cache.size, derived from the
        # counter name's IdP prefix (parity with sync).
        prefix = cls._FETCH_TOKEN_COUNTER_NAME.split(".", 1)[0]
        return f"{prefix}.token_cache.size"

    def _default_port(self) -> int:
        """Dialect-aware default port fallback (dialect.default_port when
        available; 5432 otherwise)."""
        dialect = self._plugin_service.database_dialect
        if dialect is not None:
            return dialect.default_port
        return 5432

    async def _resolve_credentials(
            self,
            host_info: HostInfo,
            props: Properties) -> Tuple[Optional[str], Optional[str], bool]:
        # Fall back idp_username/idp_password to user/password -- parity with
        # sync _connect (federated_plugin.py:87).
        SamlUtils.check_idp_credentials_with_fallback(props)

        db_user = WrapperProperties.DB_USER.get(props)
        if not db_user:
            raise AwsWrapperError(
                "Federated auth requires 'db_user' connection property"
            )
        host = IamAuthUtils.get_iam_host(props, host_info)
        port = IamAuthUtils.get_port(props, host_info, self._default_port())
        region = _resolve_iam_region(props, host, host_info)
        if not region:
            logger.debug("RdsUtils.UnsupportedHostname", host)
            raise AwsWrapperError(
                Messages.get_formatted("RdsUtils.UnsupportedHostname", host))

        # 1. Token cache check before the expensive SAML round-trip.
        cached = await self._cached_rds_token(host, int(port), db_user, region)
        if cached is not None:
            logger.debug("FederatedAuthPlugin.UseCachedToken", cached)
            return db_user, cached, True

        # Cache miss -> generate a fresh RDS IAM token. Emit the counter here
        # (covers both federated + Okta; Okta overrides the counter name).
        if self._fetch_token_counter is not None:
            self._fetch_token_counter.inc()

        # 2. Fetch SAML assertion from the IdP.
        saml_assertion = await self._fetch_saml_assertion(props)

        # 3. STS exchange.
        role_arn = WrapperProperties.IAM_ROLE_ARN.get(props)
        idp_arn = WrapperProperties.IAM_IDP_ARN.get(props)
        if not role_arn or not idp_arn:
            raise AwsWrapperError(
                "Federated auth requires 'iam_role_arn' and 'iam_idp_arn'"
            )
        creds = await self._sts_assume_role_with_saml(
            host_info, props, role_arn, idp_arn, saml_assertion, region,
        )

        # 4. Generate RDS IAM token with the temporary STS credentials.
        token = await self._generate_rds_token(
            self._plugin_service, host_info, props, db_user, host, int(port), region, creds,
        )
        ttl_sec = WrapperProperties.IAM_TOKEN_EXPIRATION.get_int(props)
        self._store_rds_token(host, int(port), db_user, region, token, ttl_sec)
        return db_user, token, False

    def _invalidate_cache(
            self,
            host_info: HostInfo,
            props: Properties) -> None:
        """Drop the cached RDS IAM token for this (host, port, user, region)
        so a subsequent ``_resolve_credentials`` call regenerates it via a
        fresh SAML assertion + STS exchange."""
        db_user = WrapperProperties.DB_USER.get(props)
        if not db_user:
            return
        try:
            host = IamAuthUtils.get_iam_host(props, host_info)
        except AwsWrapperError:
            return
        port = IamAuthUtils.get_port(props, host_info, self._default_port())
        region = _resolve_iam_region(props, host, host_info)
        self._storage_service.remove(
            TokenInfo,
            self._rds_token_cache_key(host, int(port), db_user, region))

    # ---- error-key mapping (parity with sync FederatedAuthPlugin) --------

    def _wrap_network_exception(self, exc: Exception) -> AwsConnectError:
        if isinstance(exc, AwsConnectError):
            return exc
        return AwsConnectError(
            Messages.get_formatted("FederatedAuthPlugin.ConnectException", exc))

    def _wrap_connect_exception(self, exc: Exception) -> AwsWrapperError:
        if isinstance(exc, AwsWrapperError):
            return exc
        return AwsWrapperError(
            Messages.get_formatted("FederatedAuthPlugin.ConnectException", exc), exc)

    def _wrap_retry_exception(self, exc: Exception) -> AwsWrapperError:
        if isinstance(exc, AwsWrapperError):
            return exc
        return AwsWrapperError(
            Messages.get_formatted("FederatedAuthPlugin.UnhandledException", exc), exc)

    # ---- ADFS SAML flow --------------------------------------------------

    async def _fetch_saml_assertion(self, props: Properties) -> str:
        """ADFS SAML assertion: GET the sign-in page, parse its form action +
        hidden inputs, inject idp credentials, POST urlencoded, scrape the
        SAMLResponse. Parity with sync
        ``AdfsCredentialsProviderFactory.get_saml_assertion``
        (federated_plugin.py:206-251).

        Subclasses (:class:`AsyncOktaAuthPlugin`) override this to drive their
        own auth flow.
        """
        import aiohttp
        uri = self._get_sign_in_page_url(props)
        verify = WrapperProperties.SSL_SECURE.get_bool(props)
        timeout_raw = WrapperProperties.HTTP_REQUEST_TIMEOUT.get(props)
        timeout = float(timeout_raw) if timeout_raw else 60.0
        ssl_ctx: Any = _ssl.create_default_context()
        if not verify:
            ssl_ctx = False  # aiohttp accepts ``False`` to disable verification

        async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=timeout),
                trust_env=True,
        ) as session:
            # 1. GET the sign-in page.
            SamlUtils.validate_url(uri)
            logger.debug("AdfsCredentialsProviderFactory.SignOnPageUrl", uri)
            async with session.get(uri, ssl=ssl_ctx) as resp:
                status, reason = resp.status, resp.reason
                sign_in_page_body = await resp.text()
            self._validate_response_status(status, reason, sign_in_page_body)

            # 2. Resolve the form POST target.
            action = self._get_form_action_from_html_body(sign_in_page_body)
            if action != "" and action.startswith("/"):
                uri = self._get_form_action_url(props, action)

            # 3. Inject idp credentials into the form's hidden inputs.
            params = self._get_parameters_from_html_body(sign_in_page_body, props)

            # 4. POST the urlencoded form.
            SamlUtils.validate_url(uri)
            logger.debug("AdfsCredentialsProviderFactory.SignOnPagePostActionUrl", uri)
            async with session.post(
                    uri, data=urlencode(params), ssl=ssl_ctx) as resp:
                status, reason = resp.status, resp.reason
                content = await resp.text()
            self._validate_response_status(status, reason, content)

        # 5. Scrape the SAMLResponse from the POST result.
        return self._extract_saml_assertion(content)

    @staticmethod
    def _validate_response_status(
            status: int, reason: Optional[str], text: str) -> None:
        """aiohttp counterpart of ``SamlUtils.validate_response`` (which reads a
        ``requests.Response``): raise on a non-2xx status."""
        if status / 100 != 2:
            raise AwsWrapperError(Messages.get_formatted(
                "SamlUtils.RequestFailed", status, reason, text))

    @staticmethod
    def _get_sign_in_page_url(props: Properties) -> str:
        idp_endpoint = WrapperProperties.IDP_ENDPOINT.get(props)
        idp_port = WrapperProperties.IDP_PORT.get_int(props)
        relaying_party_id = WrapperProperties.RELAYING_PARTY_ID.get(props)
        url = (
            f"https://{idp_endpoint}:{idp_port}/adfs/ls/IdpInitiatedSignOn.aspx"
            f"?loginToRp={relaying_party_id}"
        )
        if idp_endpoint is None or relaying_party_id is None:
            logger.debug("SamlUtils.InvalidHttpsUrl", url)
            raise AwsWrapperError(
                Messages.get_formatted("SamlUtils.InvalidHttpsUrl", url))
        return url

    @staticmethod
    def _get_form_action_url(props: Properties, action: str) -> str:
        idp_endpoint = WrapperProperties.IDP_ENDPOINT.get(props)
        idp_port = WrapperProperties.IDP_PORT.get(props)
        url = f"https://{idp_endpoint}:{idp_port}{action}"
        if idp_endpoint is None:
            logger.debug("SamlUtils.InvalidHttpsUrl", url)
            raise AwsWrapperError(
                Messages.get_formatted("SamlUtils.InvalidHttpsUrl", url))
        return url

    def _get_input_tags_from_html(self, body: str) -> list:
        return re.findall(self._INPUT_TAG_PATTERN, body, re.DOTALL)

    @staticmethod
    def _get_value_by_key(input_tag: str, key: str) -> str:
        match = re.search(r"(" + key + r")\s*=\s*\"(.*?)\"", input_tag)
        if match:
            return unescape(match.group(2))
        return ""

    def _get_parameters_from_html_body(
            self, body: str, props: Properties) -> Dict[str, str]:
        parameters: Dict[str, str] = {}
        for input_tag in self._get_input_tags_from_html(body):
            name = self._get_value_by_key(input_tag, "name")
            name_lower = name.lower()
            value = self._get_value_by_key(input_tag, "value")

            if "username" in name_lower:
                idp_user = WrapperProperties.IDP_USERNAME.get(props)
                if idp_user is not None:
                    parameters[name] = idp_user
            elif "authmethod" in name_lower:
                if value != "":
                    parameters[name] = value
            elif "password" in name_lower:
                idp_password = WrapperProperties.IDP_PASSWORD.get(props)
                if idp_password is not None:
                    parameters[name] = idp_password
            elif name != "":
                parameters[name] = value
        return parameters

    def _get_form_action_from_html_body(self, body: str) -> str:
        match = re.search(self._FORM_ACTION_PATTERN, body)
        if match:
            return unescape(match.group(1))
        return ""

    @staticmethod
    def _extract_saml_assertion(html: str) -> str:
        """Extract the base64-encoded SAMLResponse from an ADFS HTML page."""
        m = re.search(
            r'name="SAMLResponse"\s+value="([^"]+)"',
            html,
        )
        if not m:
            raise AwsWrapperError(Messages.get_formatted(
                "AdfsCredentialsProviderFactory.FailedLogin", html))
        return m.group(1)


class AsyncOktaAuthPlugin(AsyncFederatedAuthPlugin):
    """Okta -> SAML -> STS -> RDS IAM token.

    Overrides :meth:`_fetch_saml_assertion` to drive Okta's REST auth + app SSO
    flow instead of ADFS's form flow.

    Connection properties specific to Okta (shared with sync plugin):
      * ``app_id``: Okta application ID (the SSO URL includes this).
      * ``idp_endpoint``: Okta org domain (e.g., "mycorp.okta.com").
      * ``idp_username`` / ``idp_password`` / ``iam_role_arn`` / ``iam_idp_arn``
        / ``iam_region``: shared with the federated base.
    """

    # Distinct metric per IdP -- matches sync okta_plugin.py:65.
    _FETCH_TOKEN_COUNTER_NAME: ClassVar[str] = "okta.fetch_token.count"

    _OKTA_AUTHN_PATH = "/api/v1/authn"
    _OKTA_APP_SAML_PATH_TEMPLATE = "/app/amazon_aws/{app_id}/sso/saml"

    # ---- error-key mapping (parity with sync OktaAuthPlugin) -------------

    def _wrap_network_exception(self, exc: Exception) -> AwsConnectError:
        if isinstance(exc, AwsConnectError):
            return exc
        return AwsConnectError(
            Messages.get_formatted("OktaAuthPlugin.ConnectException", exc))

    def _wrap_connect_exception(self, exc: Exception) -> AwsWrapperError:
        if isinstance(exc, AwsWrapperError):
            return exc
        return AwsWrapperError(
            Messages.get_formatted("OktaAuthPlugin.ConnectException", exc), exc)

    def _wrap_retry_exception(self, exc: Exception) -> AwsWrapperError:
        if isinstance(exc, AwsWrapperError):
            return exc
        return AwsWrapperError(
            Messages.get_formatted("OktaAuthPlugin.UnhandledException", exc), exc)

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
            # Step 1: primary authentication -> sessionToken.
            authn_url = urljoin(org_base, self._OKTA_AUTHN_PATH)
            async with session.post(
                    authn_url,
                    headers={"Content-Type": "application/json",
                             "Accept": "application/json"},
                    json={
                        "username": str(idp_username),
                        "password": str(idp_password),
                    },
                    ssl=ssl_ctx,
            ) as resp:
                status, reason = resp.status, resp.reason
                authn_text = await resp.text()
            # Validate the HTTP status BEFORE parsing JSON: a non-2xx error
            # body is often not JSON, and resp.json() would raise a
            # ContentTypeError that masks the intended RequestFailed message.
            self._validate_response_status(status, reason, authn_text)
            authn = json.loads(authn_text)
            session_token = authn.get("sessionToken")
            if not session_token:
                raise AwsWrapperError(
                    f"Okta authn did not return sessionToken: status={authn.get('status')}"
                )

            # Step 2: exchange the session token for a SAML assertion.
            sso_path = self._OKTA_APP_SAML_PATH_TEMPLATE.format(app_id=str(app_id))
            sso_url = f"{org_base}{sso_path}?onetimetoken={session_token}"
            SamlUtils.validate_url(sso_url)
            logger.debug("OktaCredentialsProviderFactory.SamlAssertionUrl", sso_url)
            async with session.get(sso_url, ssl=ssl_ctx) as resp:
                status, reason = resp.status, resp.reason
                body = await resp.text()
            self._validate_response_status(status, reason, body)
        return self._extract_saml_assertion(body)

    @staticmethod
    def _extract_saml_assertion(html: str) -> str:
        """Okta's SAML form places ``type``/``id`` attributes between
        ``name="SAMLResponse"`` and ``value=`` -- the ADFS base regex
        (``\\s+`` between name and value) doesn't match. Use a lazier pattern
        and HTML-unescape the value (parity with sync okta_plugin.py:242,
        which wraps the match in ``unescape``)."""
        m = re.search(
            r'name="SAMLResponse"[^>]*?\svalue="([^"]+)"',
            html,
        )
        if not m:
            raise AwsWrapperError(Messages.get_formatted(
                "AdfsCredentialsProviderFactory.FailedLogin", html))
        return unescape(m.group(1))
