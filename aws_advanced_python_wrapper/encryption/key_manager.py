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

from __future__ import annotations

import base64
import hashlib
import os
import random
import time
import uuid
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, TypeVar

from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionError as BotoConnectionError
from botocore.exceptions import (ConnectTimeoutError, EndpointConnectionError,
                                 HTTPClientError)

from aws_advanced_python_wrapper.encryption.algorithm import \
    EncryptionAlgorithm
from aws_advanced_python_wrapper.encryption.buffers import wipe
from aws_advanced_python_wrapper.encryption.models import (DEFAULT_KEY_SPEC,
                                                           KeyMetadata,
                                                           mask_arn)
from aws_advanced_python_wrapper.encryption.sql_dialect import (execute,
                                                                fetch_rows)
from aws_advanced_python_wrapper.errors import (InvalidKeyMetadataError,
                                                KeyManagementError,
                                                UnauthorizedMasterKeyError)
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

if TYPE_CHECKING:
    from boto3 import Session

    from aws_advanced_python_wrapper.encryption.config import EncryptionConfig
    from aws_advanced_python_wrapper.encryption.connection_source import \
        ConnectionSource
    from aws_advanced_python_wrapper.encryption.data_key_cache import \
        DataKeyCache
    from aws_advanced_python_wrapper.encryption.sql_dialect import SqlDialect

logger = Logger(__name__)

T = TypeVar("T")


@dataclass(frozen=True, eq=False)
class GeneratedDataKey:
    """A data key fresh from KMS. ``plaintext`` is a ``bytearray`` so the
    caller can wipe it once used; ``repr`` never shows key material."""

    plaintext: bytearray = field(repr=False)
    encrypted_data_key: str = field(repr=False)
    hmac_key: bytes = field(repr=False)
    key_spec: str = DEFAULT_KEY_SPEC


class KeyManager:
    """Owns the data keys: generates them through KMS, stores them in
    ``key_storage``, and decrypts them again, caching the plaintext keys.

    The data-key operations (``GenerateDataKey`` and ``Decrypt``) are gated on
    ``encryption_allowed_master_key_arns`` before KMS is contacted, including
    on a data-key cache hit. :meth:`create_master_key` and
    :meth:`is_master_key_valid` are not gated: they use no data key. Plaintext
    data keys never reach the database.
    """

    HMAC_KEY_LENGTH = 32
    MASTER_KEY_SPEC = "SYMMETRIC_DEFAULT"
    KEY_USAGE = "ENCRYPT_DECRYPT"
    MAX_BACKOFF_SEC = 20.0
    JITTER_RATIO = 0.25
    # botocore defaults both to 60 s. A KMS call normally takes milliseconds,
    # so an unreachable or hung endpoint fails an attempt in seconds rather
    # than a minute.
    CONNECT_TIMEOUT_SEC = 5.0
    READ_TIMEOUT_SEC = 10.0
    _CACHE_KEY_PREFIX = "datakey_"

    # KMS error codes worth retrying, in addition to any 5xx or 429 response.
    RETRYABLE_ERROR_CODES = frozenset({
        "ThrottlingException",
        "LimitExceededException",
        "KMSInternalException",
        "KeyUnavailableException",
        "DependencyTimeoutException",
        "RequestTimeout",
        "ServiceUnavailable",
        "InternalFailure",
    })

    # Errors that mean KMS did not act on the request, so even a call that is
    # not idempotent (CreateKey) can be retried without creating a duplicate.
    NOT_PROCESSED_ERROR_CODES = frozenset({"ThrottlingException"})

    def __init__(self, kms_client: Any, connection_source: ConnectionSource, sql_dialect: SqlDialect,
                 config: EncryptionConfig, data_key_cache: DataKeyCache,
                 *, sleep: Callable[[float], None] = time.sleep):
        self._kms = kms_client
        self._connections = connection_source
        self._sql = sql_dialect
        self._config = config
        self._cache = data_key_cache
        self._sleep = sleep

    @staticmethod
    def create_kms_client(session: Session, config: EncryptionConfig) -> Any:
        """Build a KMS client for ``config``.

        botocore's own retries are turned off because :class:`KeyManager`
        retries itself. botocore cannot be given the configured backoff base
        (``encryption_key_management_retry_backoff_base_sec``), and leaving
        both on would multiply the attempt count. Connect and read timeouts
        are short, so an unreachable KMS endpoint fails in seconds rather than
        minutes.
        """
        client_config = Config(region_name=config.kms_region, retries={"total_max_attempts": 1},
                               connect_timeout=KeyManager.CONNECT_TIMEOUT_SEC,
                               read_timeout=KeyManager.READ_TIMEOUT_SEC)
        if config.kms_endpoint:
            return session.client("kms", endpoint_url=config.kms_endpoint, config=client_config)
        return session.client("kms", config=client_config)

    # ---- data keys ----

    def decrypt_data_key(self, encrypted_data_key: Optional[str], master_key_arn: Optional[str]) -> bytearray:
        """Return the plaintext of a stored data key, from the cache when
        possible. The caller owns the returned buffer and should wipe it.

        :raises UnauthorizedMasterKeyError: the master key is not allowed;
            KMS is not called.
        :raises InvalidKeyMetadataError: there is no usable encrypted data key.
        :raises KeyManagementError: KMS rejected the request.
        """
        self._authorize(master_key_arn)
        if encrypted_data_key is None or not encrypted_data_key.strip():
            raise InvalidKeyMetadataError(
                Messages.get("KeyManager.MissingEncryptedDataKey"), operation="DECRYPT_DATA_KEY")

        cache_key = self.data_key_cache_key(encrypted_data_key)
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            ciphertext = base64.b64decode("".join(encrypted_data_key.split()), validate=True)
        except ValueError:
            # Catches binascii.Error (bad base64) and the plain ValueError
            # b64decode raises for a non-ASCII string, as in a hand-edited row.
            ciphertext = None
        if not ciphertext:
            raise InvalidKeyMetadataError(
                Messages.get("KeyManager.MalformedEncryptedDataKey"), operation="DECRYPT_DATA_KEY")

        # KeyId is passed as defense in depth: a data key wrapped under a
        # different master key than its row names fails inside KMS too. With
        # no ARN (only possible without an allow-list), KMS reads the key from
        # the ciphertext.
        request: Dict[str, Any] = {"CiphertextBlob": ciphertext}
        if master_key_arn:
            request["KeyId"] = master_key_arn
        response = self._call_kms("DECRYPT_DATA_KEY", lambda: self._kms.decrypt(**request))
        plaintext = bytearray(response["Plaintext"])
        self._cache.put(cache_key, plaintext)
        return plaintext

    def generate_data_key(self, master_key_arn: str,
                          algorithm: str = EncryptionAlgorithm.DEFAULT) -> GeneratedDataKey:
        """Ask KMS for a new data key sized for ``algorithm``, plus a random
        HMAC key.

        :raises UnauthorizedMasterKeyError: the master key is not allowed.
        :raises KeyManagementError: KMS rejected the request.
        """
        self._authorize(master_key_arn)
        key_spec = EncryptionAlgorithm.data_key_spec(algorithm)
        response = self._call_kms(
            "GENERATE_DATA_KEY", lambda: self._kms.generate_data_key(KeyId=master_key_arn, KeySpec=key_spec))
        plaintext = bytearray(response["Plaintext"])
        if len(plaintext) != EncryptionAlgorithm.key_length(algorithm):
            # Catch a mis-sized key now, at setup, rather than on the first
            # write to the column. (Some KMS emulators ignore KeySpec.)
            length = len(plaintext)
            wipe(plaintext)
            raise KeyManagementError(
                Messages.get_formatted("KeyManager.WrongDataKeyLength", length, key_spec, algorithm),
                operation="GENERATE_DATA_KEY")
        return GeneratedDataKey(
            plaintext=plaintext,
            encrypted_data_key=base64.b64encode(response["CiphertextBlob"]).decode("ascii"),
            hmac_key=os.urandom(self.HMAC_KEY_LENGTH),
            key_spec=key_spec,
        )

    @classmethod
    def data_key_cache_key(cls, encrypted_data_key: str) -> str:
        """Hash the encrypted data key so it never appears as a cache key."""
        digest = hashlib.sha256(encrypted_data_key.encode("utf-8")).digest()
        return cls._CACHE_KEY_PREFIX + base64.b64encode(digest).decode("ascii")

    # ---- master keys ----

    def create_master_key(self, description: str, *, policy: Optional[str] = None) -> str:
        """Create a symmetric KMS key and return its ARN. Not gated on the
        allow-list: the new key cannot be listed before it exists.

        Only failures that mean KMS did not act are retried: throttling, and
        a connection that was never made. After a timeout or a server error
        the key may already exist, and a retry would create a second one.
        """
        request = {"Description": description, "KeyUsage": self.KEY_USAGE, "KeySpec": self.MASTER_KEY_SPEC}
        if policy and policy.strip():
            request["Policy"] = policy
        response = self._call_kms("CREATE_MASTER_KEY", lambda: self._kms.create_key(**request),
                                  retryable=self._is_not_processed)
        return response["KeyMetadata"]["Arn"]

    def is_master_key_valid(self, master_key_arn: str) -> bool:
        """``True`` when the key exists, is enabled, and can encrypt/decrypt.

        ``False`` also covers "could not check" (no ``kms:DescribeKey``
        permission, KMS unreachable); the reason is logged as a warning.
        """
        try:
            metadata = self._call_kms(
                "DESCRIBE_MASTER_KEY", lambda: self._kms.describe_key(KeyId=master_key_arn))["KeyMetadata"]
        except KeyManagementError as e:
            logger.warning("KeyManager.MasterKeyValidationFailed", mask_arn(master_key_arn), e)
            return False
        return (bool(metadata.get("Enabled")) and metadata.get("KeyState") == "Enabled"
                and metadata.get("KeyUsage") == self.KEY_USAGE)

    # ---- key_storage ----

    def store_key_metadata(self, key_metadata: KeyMetadata) -> KeyMetadata:
        """Insert a ``key_storage`` row and return it with ``id`` (and a
        generated ``key_id`` UUID, if none was given) filled in.

        Timestamps come from the database's ``CURRENT_TIMESTAMP`` defaults, so
        the stored values follow the database's own time zone handling.
        """
        if not key_metadata.is_usable:
            raise InvalidKeyMetadataError(
                Messages.get("KeyManager.RefusingUnusableKeyMetadata"), operation="STORE_KEY_METADATA")

        to_store = key_metadata if key_metadata.key_id else replace(key_metadata, key_id=str(uuid.uuid4()))
        sql = (f"INSERT INTO {self._config.metadata_schema}.key_storage "
               "(key_id, name, master_key_arn, encrypted_data_key, hmac_key, key_spec) "
               "VALUES (%s, %s, %s, %s, %s, %s)")
        params = (to_store.key_id, to_store.key_name, to_store.master_key_arn,
                  to_store.encrypted_data_key, to_store.hmac_key, to_store.key_spec)

        failure: Optional[str] = None
        try:
            with self._connections.connection() as conn:
                new_id = self._sql.insert_returning_id(conn, sql, params)
        except Exception as e:
            # The bound HMAC key is a secret, and some driver errors quote the
            # interpolated statement. Keep only the exception type and the
            # driver's error codes, and raise outside this block so the driver
            # error is not even chained as __context__.
            failure = self._describe_failure(e)
        if failure is not None:
            raise KeyManagementError(
                Messages.get_formatted("KeyManager.StoreKeyMetadataFailed", failure), operation="STORE_KEY_METADATA")
        return replace(to_store, id=new_id)

    def key_metadata_by_id(self, key_storage_id: int) -> Optional[KeyMetadata]:
        """Read one ``key_storage`` row by its integer ``id``."""
        sql = ("SELECT id, key_id, name, master_key_arn, encrypted_data_key, hmac_key, key_spec, "
               f"created_at, last_used_at FROM {self._config.metadata_schema}.key_storage WHERE id {self._sql.equals} %s")
        try:
            with self._connections.connection() as conn:
                rows = fetch_rows(conn, sql, (key_storage_id,))
        except Exception as e:
            raise KeyManagementError(
                Messages.get_formatted("KeyManager.ReadKeyMetadataFailed", key_storage_id, e),
                operation="GET_KEY_METADATA") from e
        return KeyMetadata.from_row(rows[0]) if rows else None

    def touch_key(self, key_uuid: str) -> None:
        """Stamp ``last_used_at``. Bookkeeping only: failures are logged."""
        sql = (f"UPDATE {self._config.metadata_schema}.key_storage SET last_used_at = CURRENT_TIMESTAMP "
               f"WHERE key_id {self._sql.equals} %s")
        try:
            with self._connections.connection() as conn:
                execute(conn, sql, (key_uuid,))
        except Exception as e:
            logger.debug("KeyManager.TouchKeyFailed", key_uuid, e)

    # ---- internals ----

    def _authorize(self, master_key_arn: Optional[str]) -> None:
        if not self._config.is_master_key_allowed(master_key_arn):
            raise UnauthorizedMasterKeyError(
                Messages.get_formatted("KeyManager.UnauthorizedMasterKey", mask_arn(master_key_arn),
                                       WrapperProperties.ENCRYPTION_ALLOWED_MASTER_KEY_ARNS.name),
                operation="AUTHORIZE_MASTER_KEY")

    @staticmethod
    def _describe_failure(error: Exception) -> str:
        """The exception type plus any SQLSTATE / errno the driver reports.
        Neither carries statement text, unlike the error message."""
        details = [type(error).__name__]
        for attribute in ("sqlstate", "errno"):
            value = getattr(error, attribute, None)
            if isinstance(value, (str, int)) and not isinstance(value, bool):
                details.append(f"{attribute} {value}")
        return ", ".join(details)

    def _call_kms(self, operation: str, call: Callable[[], T],
                  retryable: Optional[Callable[[Exception], bool]] = None) -> T:
        """Run a KMS call, retrying throttling and transient failures with
        exponential backoff and jitter. ``retryable`` decides which failures
        are retried; it defaults to :meth:`_is_retryable`."""
        is_retryable = retryable or self._is_retryable
        max_retries = self._config.key_management_max_retries
        attempt = 0
        while True:
            try:
                return call()
            except Exception as e:
                if attempt >= max_retries or not is_retryable(e):
                    raise KeyManagementError(
                        Messages.get_formatted("KeyManager.KmsCallFailed", operation, attempt + 1, e),
                        operation=operation) from e
                delay = self._backoff_sec(attempt)
                logger.debug("KeyManager.Retrying", operation, attempt + 1, max_retries + 1, delay, e)
            self._sleep(delay)
            attempt += 1

    @classmethod
    def _is_retryable(cls, error: Exception) -> bool:
        if isinstance(error, (BotoConnectionError, HTTPClientError)):
            return True
        if isinstance(error, ClientError):
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status is not None and (status >= 500 or status == 429):
                return True
            return error.response.get("Error", {}).get("Code") in cls.RETRYABLE_ERROR_CODES
        return False

    @classmethod
    def _is_not_processed(cls, error: Exception) -> bool:
        # EndpointConnectionError and ConnectTimeoutError are raised before the
        # request is sent.
        if isinstance(error, (EndpointConnectionError, ConnectTimeoutError)):
            return True
        if isinstance(error, ClientError):
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            code = error.response.get("Error", {}).get("Code")
            return status == 429 or code in cls.NOT_PROCESSED_ERROR_CODES
        return False

    def _backoff_sec(self, attempt: int) -> float:
        """``base * 2**attempt`` +/- 25%, at least ``base``, at most 20 s.

        The cap wins over the floor when the configured base exceeds it.
        """
        base = self._config.key_management_retry_backoff_base_sec
        exponential = base * (2 ** attempt)
        jitter = exponential * self.JITTER_RATIO * random.uniform(-1.0, 1.0)
        return min(max(exponential + jitter, base), self.MAX_BACKOFF_SEC)
