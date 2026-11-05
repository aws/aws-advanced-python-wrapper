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

import base64
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from typing import Any, List, Optional, Tuple

import boto3
import pytest
from botocore.awsrequest import AWSResponse
from botocore.exceptions import ClientError, EndpointConnectionError
from botocore.stub import Stubber

from aws_advanced_python_wrapper.encryption import key_manager as km_module
from aws_advanced_python_wrapper.encryption.algorithm import \
    EncryptionAlgorithm
from aws_advanced_python_wrapper.encryption.config import EncryptionConfig
from aws_advanced_python_wrapper.encryption.data_key_cache import DataKeyCache
from aws_advanced_python_wrapper.encryption.key_manager import KeyManager
from aws_advanced_python_wrapper.encryption.models import KeyMetadata, mask_arn
from aws_advanced_python_wrapper.encryption.sql_dialect import MYSQL, PG
from aws_advanced_python_wrapper.errors import (InvalidKeyMetadataError,
                                                KeyManagementError,
                                                OperationalError,
                                                UnauthorizedMasterKeyError)

ARN = "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab"
OTHER_ARN = "arn:aws:kms:us-east-1:123456789012:key/ffffffff-12ab-34cd-56ef-1234567890ab"
DATA_KEY = bytes(range(32))
BLOB = b"kms-ciphertext-blob"
EDK = base64.b64encode(BLOB).decode()
# The caller's identity, as KMS names it when it denies a call.
ROLE_SESSION_ARN = "arn:aws:sts::123456789012:assumed-role/kiro-app-role/kiro-app-session"
MASKED_ROLE_SESSION_ARN = "arn:aws:sts:***:***:***"


class FakeCursor:
    def __init__(self, db: "FakeDb"):
        self._db = db
        self.description: Optional[List[Tuple[str]]] = None
        self.rowcount = -1
        self.lastrowid = None
        self._rows: List[Tuple[Any, ...]] = []

    def execute(self, sql, params=()):
        self._db.executed.append((sql, tuple(params)))
        if self._db.error is not None:
            raise self._db.error
        names, rows = self._db.result
        self.description = [(n,) for n in names] if names else None
        self._rows = list(rows)
        self.rowcount = self._db.rowcount
        self.lastrowid = self._db.lastrowid

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class FakeDb:
    def __init__(self):
        self.executed: List[Tuple[str, Tuple[Any, ...]]] = []
        self.result: Tuple[List[str], List[Tuple[Any, ...]]] = ([], [])
        self.rowcount = 1
        self.lastrowid: Optional[int] = None
        self.error: Optional[Exception] = None

    def cursor(self):
        return FakeCursor(self)

    @contextmanager
    def connection(self):
        yield self


def config(**overrides) -> EncryptionConfig:
    values = dict(kms_region="us-east-1", allowed_master_key_arns=frozenset({ARN}),
                  key_management_retry_backoff_base_sec=0.1)
    values.update(overrides)
    return EncryptionConfig(**values)  # type: ignore[arg-type]


@pytest.fixture
def kms():
    client = boto3.client("kms", region_name="us-east-1",
                          aws_access_key_id="x", aws_secret_access_key="x")
    with Stubber(client) as stubber:
        yield client, stubber
        stubber.assert_no_pending_responses()


@pytest.fixture
def db():
    return FakeDb()


LOGGERS = ("", "botocore", "botocore.parsers", "botocore.endpoint", "botocore.auth")
OUR_FILTERS = (km_module._NoKmsPlaintext, km_module._NoKmsRequestCredentials)


@pytest.fixture(autouse=True)
def no_filters(mocker):
    """Neither filter installed when the test starts; every logger's filters
    as they were when it ends. Every test in this module uses it, so the
    module leaves the process's loggers and install flag as it found them."""
    loggers = [logging.getLogger(name) for name in LOGGERS]
    saved = [list(lg.filters) for lg in loggers]
    for lg in loggers:
        lg.filters = [f for f in lg.filters if not isinstance(f, OUR_FILTERS)]
    mocker.patch.object(km_module, "_filters_installed", False)
    yield
    for lg, filters in zip(loggers, saved):
        lg.filters = filters


def manager(kms_client, db, cfg=None, cache=None, sleeps=None, sql=PG):
    sleeps = sleeps if sleeps is not None else []
    return KeyManager(kms_client, db, sql, cfg or config(),
                      cache or DataKeyCache(10, 60, start_cleanup_thread=False), sleep=sleeps.append)


class TestDecryptDataKey:
    def test_decrypts_with_key_id_and_caches(self, kms, db):
        client, stubber = kms
        stubber.add_response("decrypt", {"Plaintext": DATA_KEY, "KeyId": ARN},
                             {"CiphertextBlob": BLOB, "KeyId": ARN})
        km = manager(client, db)
        first = km.decrypt_data_key(EDK, ARN)
        assert isinstance(first, bytearray) and first == DATA_KEY
        # Second call is served from the cache: no further stubbed response.
        first[:] = bytes(32)
        assert km.decrypt_data_key(EDK, ARN) == DATA_KEY

    def test_unlisted_master_key_is_refused_before_kms(self, kms, db):
        client, _ = kms  # no stubbed responses: any KMS call would fail
        km = manager(client, db)
        with pytest.raises(UnauthorizedMasterKeyError) as err:
            km.decrypt_data_key(EDK, OTHER_ARN)
        assert "123456789012" not in str(err.value)
        with pytest.raises(UnauthorizedMasterKeyError):
            km.decrypt_data_key(EDK, None)

    def test_allow_list_is_checked_even_on_a_cache_hit(self, kms, db):
        client, _ = kms
        cache = DataKeyCache(10, 60, start_cleanup_thread=False)
        cache.put(KeyManager.data_key_cache_key(EDK), DATA_KEY)
        with pytest.raises(UnauthorizedMasterKeyError):
            manager(client, db, cache=cache).decrypt_data_key(EDK, OTHER_ARN)

    @pytest.mark.parametrize("edk", [None, "", "   "])
    def test_missing_encrypted_data_key(self, kms, db, edk):
        with pytest.raises(InvalidKeyMetadataError):
            manager(kms[0], db).decrypt_data_key(edk, ARN)

    @pytest.mark.parametrize("edk", ["not base64!!", "ZWRr\u200b==", "Y\u00e9=="],
                             ids=["bad-chars", "zero-width-space", "non-ascii"])
    def test_malformed_base64(self, kms, db, edk):
        # A non-ASCII string makes b64decode raise a plain ValueError rather
        # than binascii.Error; it must still be InvalidKeyMetadataError.
        with pytest.raises(InvalidKeyMetadataError):
            manager(kms[0], db).decrypt_data_key(edk, ARN)

    def test_base64_with_line_breaks_is_accepted(self, kms, db):
        client, stubber = kms
        stubber.add_response("decrypt", {"Plaintext": DATA_KEY}, {"CiphertextBlob": BLOB, "KeyId": ARN})
        wrapped = EDK[:8] + "\n" + EDK[8:]
        assert manager(client, db).decrypt_data_key(wrapped, ARN) == DATA_KEY

    def test_no_master_key_arn_leaves_key_id_to_kms(self, kms, db):
        # Only possible without an allow-list. KMS reads the key from the
        # ciphertext; passing KeyId=None would fail boto3's validation.
        client, stubber = kms
        stubber.add_response("decrypt", {"Plaintext": DATA_KEY, "KeyId": ARN}, {"CiphertextBlob": BLOB})
        km = manager(client, db, config(allowed_master_key_arns=frozenset()))
        assert km.decrypt_data_key(EDK, None) == bytearray(DATA_KEY)

    def test_non_retryable_kms_error_fails_immediately(self, kms, db):
        client, stubber = kms
        stubber.add_client_error("decrypt", "IncorrectKeyException", http_status_code=400)
        sleeps: List[float] = []
        with pytest.raises(KeyManagementError) as err:
            manager(client, db, sleeps=sleeps).decrypt_data_key(EDK, ARN)
        assert sleeps == []
        assert err.value.operation == "DECRYPT_DATA_KEY"
        assert isinstance(err.value, OperationalError)
        assert not isinstance(err.value, UnauthorizedMasterKeyError)


class TestRetries:
    def test_throttling_is_retried_with_backoff(self, kms, db):
        client, stubber = kms
        stubber.add_client_error("decrypt", "ThrottlingException", http_status_code=400)
        stubber.add_client_error("decrypt", "KMSInternalException", http_status_code=500)
        stubber.add_response("decrypt", {"Plaintext": DATA_KEY}, {"CiphertextBlob": BLOB, "KeyId": ARN})
        sleeps: List[float] = []
        assert manager(client, db, sleeps=sleeps).decrypt_data_key(EDK, ARN) == DATA_KEY
        assert len(sleeps) == 2
        assert 0.1 <= sleeps[0] <= 0.125
        assert 0.15 <= sleeps[1] <= 0.25

    def test_retries_stop_at_the_configured_maximum(self, kms, db):
        client, stubber = kms
        for _ in range(3):
            stubber.add_client_error("decrypt", "ServiceUnavailable", http_status_code=503)
        sleeps: List[float] = []
        cfg = config(key_management_max_retries=2)
        with pytest.raises(KeyManagementError, match="3 attempt"):
            manager(client, db, cfg, sleeps=sleeps).decrypt_data_key(EDK, ARN)
        assert len(sleeps) == 2

    def test_429_and_network_errors_are_retryable(self):
        from botocore.exceptions import ClientError
        throttled = ClientError({"Error": {"Code": "Other"}, "ResponseMetadata": {"HTTPStatusCode": 429}}, "Decrypt")
        assert KeyManager._is_retryable(throttled)
        assert KeyManager._is_retryable(EndpointConnectionError(endpoint_url="https://kms"))
        bad = ClientError({"Error": {"Code": "NotFoundException"}, "ResponseMetadata": {"HTTPStatusCode": 400}}, "D")
        assert not KeyManager._is_retryable(bad)
        assert not KeyManager._is_retryable(ValueError("x"))

    @pytest.mark.parametrize("status", [500, 502, 503, 504])
    def test_any_5xx_is_retryable_even_with_an_unlisted_code(self, status):
        # A load balancer or VPC endpoint in front of KMS can answer with a
        # code outside RETRYABLE_ERROR_CODES.
        from botocore.exceptions import ClientError
        err = ClientError({"Error": {"Code": "SomethingNew"}, "ResponseMetadata": {"HTTPStatusCode": status}}, "D")
        assert KeyManager._is_retryable(err)

    def test_backoff_is_capped_and_floored(self, kms, db):
        km = manager(kms[0], db, config(key_management_retry_backoff_base_sec=1.0))
        assert all(1.0 <= km._backoff_sec(0) <= 1.25 for _ in range(50))
        assert km._backoff_sec(10) == KeyManager.MAX_BACKOFF_SEC
        huge = manager(kms[0], db, config(key_management_retry_backoff_base_sec=60.0))
        assert huge._backoff_sec(0) == KeyManager.MAX_BACKOFF_SEC

    def test_kms_client_has_botocore_retries_disabled(self):
        session = boto3.Session(aws_access_key_id="x", aws_secret_access_key="x")
        client = KeyManager.create_kms_client(session, config(kms_endpoint="http://localhost:4566"))
        assert client.meta.region_name == "us-east-1"
        assert client.meta.endpoint_url == "http://localhost:4566"
        assert client.meta.config.retries["total_max_attempts"] == 1
        # botocore's 60 s defaults would let one blackholed call take minutes.
        assert client.meta.config.connect_timeout == KeyManager.CONNECT_TIMEOUT_SEC
        assert client.meta.config.read_timeout == KeyManager.READ_TIMEOUT_SEC
        assert client.meta.config.connect_timeout < 60 and client.meta.config.read_timeout < 60


def _assert_names_no_identifier(text: str) -> None:
    for identifier in (ARN, ARN.rpartition("/")[2], ROLE_SESSION_ARN, "123456789012", "kiro-app-role",
                       "kiro-app-session"):
        assert identifier not in text


def _key_manager_messages(caplog, level: int) -> List[str]:
    # By logger name: threads other tests leave behind also log into caplog.
    return [r.getMessage() for r in caplog.records if r.name == km_module.__name__ and r.levelno == level]


class TestQuotedKmsErrorsAreMasked:
    # AWS KMS's own texts: they name the master key, and an access denial
    # names the caller's role session.
    @pytest.mark.parametrize("code, text", [
        pytest.param("DisabledException", f"{ARN} is disabled.", id="disabled"),
        pytest.param("KMSInvalidStateException", f"{ARN} is pending deletion.", id="pending-deletion"),
        pytest.param("AccessDeniedException",
                     f"User: {ROLE_SESSION_ARN} is not authorized to perform: kms:Decrypt on resource: {ARN} "
                     "with an explicit deny in a resource-based policy", id="explicit-deny"),
        pytest.param("AccessDeniedException",
                     f"User: {ROLE_SESSION_ARN} is not authorized to perform: kms:Decrypt on the resource "
                     "associated with this ciphertext because the resource does not exist in this Region, no "
                     "resource-based policies allow access, or a resource-based policy explicitly denies access",
                     id="wrong-region"),
    ])
    def test_decrypt_failure_masks_the_arns_kms_quotes(self, kms, db, code, text):
        client, stubber = kms
        stubber.add_client_error("decrypt", code, service_message=text, http_status_code=400)
        with pytest.raises(KeyManagementError) as err:
            manager(client, db).decrypt_data_key(EDK, ARN)
        message = str(err.value)
        assert message.startswith("[KeyManager] KMS DECRYPT_DATA_KEY failed after 1 attempt(s): ")
        assert f"An error occurred ({code}) when calling the Decrypt operation: " in message
        assert (mask_arn(ARN) in message) == (ARN in text)
        assert (MASKED_ROLE_SESSION_ARN in message) == (ROLE_SESSION_ARN in text)
        _assert_names_no_identifier(message)
        # The SDK's error keeps KMS's full text, with its code and response.
        assert isinstance(err.value.__cause__, ClientError)
        assert text in str(err.value.__cause__)

    def test_decrypt_failure_without_an_arn_quotes_kms_unchanged(self, kms, db):
        client, stubber = kms
        stubber.add_client_error("decrypt", "NotFoundException", service_message="Invalid arn us-east-1",
                                 http_status_code=400)
        with pytest.raises(KeyManagementError) as err:
            manager(client, db).decrypt_data_key(EDK, ARN)
        assert isinstance(err.value.__cause__, ClientError)
        assert str(err.value) == ("[KeyManager] KMS DECRYPT_DATA_KEY failed after 1 attempt(s): "
                                  f"{err.value.__cause__}")
        assert str(err.value).endswith(": Invalid arn us-east-1")

    # KeyUnavailableException's text names the key as DisabledException's does.
    # Throttling's text names none today; this one does, so the test doesn't
    # depend on which retryable error quotes an ARN.
    @pytest.mark.parametrize("code, text, status", [
        pytest.param("ThrottlingException", f"Rate exceeded for {ARN}.", 400, id="throttling"),
        pytest.param("KeyUnavailableException", f"{ARN} is unavailable.", 500, id="key-unavailable"),
    ])
    def test_retry_record_masks_the_arns_kms_quotes(self, kms, db, caplog, code, text, status):
        client, stubber = kms
        stubber.add_client_error("decrypt", code, service_message=text, http_status_code=status)
        stubber.add_response("decrypt", {"Plaintext": DATA_KEY}, {"CiphertextBlob": BLOB, "KeyId": ARN})
        with caplog.at_level(logging.DEBUG, logger=km_module.__name__):
            assert manager(client, db).decrypt_data_key(EDK, ARN) == DATA_KEY
        retries = [m for m in _key_manager_messages(caplog, logging.DEBUG) if "retrying in" in m]
        assert len(retries) == 1
        assert f"({code})" in retries[0] and mask_arn(ARN) in retries[0]
        _assert_names_no_identifier(retries[0])

    def test_validation_warning_masks_the_arns_kms_quotes(self, kms, db, caplog):
        client, stubber = kms
        text = (f"User: {ROLE_SESSION_ARN} is not authorized to perform: kms:DescribeKey on resource: {ARN} "
                "because no identity-based policy allows the kms:DescribeKey action")
        stubber.add_client_error("describe_key", "AccessDeniedException", service_message=text, http_status_code=400)
        with caplog.at_level(logging.WARNING, logger=km_module.__name__):
            assert manager(client, db).is_master_key_valid(ARN) is False
        warnings = _key_manager_messages(caplog, logging.WARNING)
        assert len(warnings) == 1
        assert "(AccessDeniedException)" in warnings[0] and MASKED_ROLE_SESSION_ARN in warnings[0]
        assert mask_arn(ARN) in warnings[0]
        _assert_names_no_identifier(warnings[0])


class TestGenerateAndCreate:
    @pytest.mark.parametrize("algorithm, spec", [
        (EncryptionAlgorithm.AES_256_GCM, "AES_256"), (EncryptionAlgorithm.AES_128_GCM, "AES_128")])
    def test_generate_data_key_sizes_the_key_for_the_algorithm(self, kms, db, algorithm, spec):
        client, stubber = kms
        key = DATA_KEY[:EncryptionAlgorithm.key_length(algorithm)]
        stubber.add_response("generate_data_key", {"Plaintext": key, "CiphertextBlob": BLOB},
                             {"KeyId": ARN, "KeySpec": spec})
        generated = manager(client, db).generate_data_key(ARN, algorithm)
        assert generated.plaintext == key and isinstance(generated.plaintext, bytearray)
        assert base64.b64decode(generated.encrypted_data_key) == BLOB
        assert len(generated.hmac_key) == 32
        assert generated.key_spec == spec
        assert "bytearray" not in repr(generated) and str(BLOB) not in repr(generated)

    def test_mis_sized_data_key_is_rejected_at_setup(self, kms, db):
        client, stubber = kms
        stubber.add_response("generate_data_key", {"Plaintext": DATA_KEY, "CiphertextBlob": BLOB},
                             {"KeyId": ARN, "KeySpec": "AES_128"})
        with pytest.raises(KeyManagementError, match="32-byte"):
            manager(client, db).generate_data_key(ARN, EncryptionAlgorithm.AES_128_GCM)

    def test_mis_sized_data_key_is_wiped(self, kms, db, monkeypatch):
        wiped: List[bytearray] = []
        real = km_module.wipe
        monkeypatch.setattr(km_module, "wipe", lambda buf: (wiped.append(buf), real(buf)))
        client, stubber = kms
        stubber.add_response("generate_data_key", {"Plaintext": DATA_KEY, "CiphertextBlob": BLOB},
                             {"KeyId": ARN, "KeySpec": "AES_128"})
        with pytest.raises(KeyManagementError):
            manager(client, db).generate_data_key(ARN, EncryptionAlgorithm.AES_128_GCM)
        assert len(wiped) == 1 and wiped[0] == bytearray(len(DATA_KEY))

    def test_generate_is_gated_on_the_allow_list(self, kms, db):
        with pytest.raises(UnauthorizedMasterKeyError):
            manager(kms[0], db).generate_data_key(OTHER_ARN)

    def test_create_master_key_is_not_retried_after_an_ambiguous_failure(self, kms, db):
        # After a 5xx the key may already exist; retrying would create a second one.
        client, stubber = kms
        stubber.add_client_error("create_key", "KMSInternalException", http_status_code=500)
        sleeps: List[float] = []
        with pytest.raises(KeyManagementError, match="1 attempt"):
            manager(client, db, sleeps=sleeps).create_master_key("d")
        assert sleeps == []

    def test_create_master_key_is_not_retried_on_a_quota_error(self, kms, db):
        # LimitExceededException on CreateKey is the key-count quota; retrying
        # only delays the same failure.
        client, stubber = kms
        stubber.add_client_error("create_key", "LimitExceededException", http_status_code=400)
        sleeps: List[float] = []
        with pytest.raises(KeyManagementError, match="1 attempt"):
            manager(client, db, sleeps=sleeps).create_master_key("d")
        assert sleeps == []

    def test_create_master_key_retries_a_connection_that_was_never_made(self, kms, db):
        client, stubber = kms
        km = manager(client, db)
        calls = []

        def create_key(**kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                raise EndpointConnectionError(endpoint_url="https://kms")
            return {"KeyMetadata": {"KeyId": "k", "Arn": ARN}}

        client.create_key = create_key  # type: ignore[method-assign]
        assert km.create_master_key("d") == ARN
        assert len(calls) == 2

    def test_create_master_key_retries_a_connect_timeout(self, kms, db):
        # ConnectTimeoutError does not derive from EndpointConnectionError, so
        # it needs its own guard.
        from botocore.exceptions import ConnectTimeoutError
        client, _ = kms
        km = manager(client, db)
        calls = []

        def create_key(**kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                raise ConnectTimeoutError(endpoint_url="https://kms")
            return {"KeyMetadata": {"KeyId": "k", "Arn": ARN}}

        client.create_key = create_key  # type: ignore[method-assign]
        assert km.create_master_key("d") == ARN
        assert len(calls) == 2

    def test_create_master_key_retries_a_429(self, kms, db):
        # A 429 means KMS did not act, whatever error code comes with it.
        client, stubber = kms
        stubber.add_client_error("create_key", "TooManyRequestsException", http_status_code=429)
        stubber.add_response("create_key", {"KeyMetadata": {"KeyId": "k", "Arn": ARN}},
                             {"Description": "d", "KeyUsage": "ENCRYPT_DECRYPT", "KeySpec": "SYMMETRIC_DEFAULT"})
        assert manager(client, db).create_master_key("d") == ARN

    def test_create_master_key_retries_throttling(self, kms, db):
        client, stubber = kms
        stubber.add_client_error("create_key", "ThrottlingException", http_status_code=400)
        stubber.add_response("create_key", {"KeyMetadata": {"KeyId": "k", "Arn": ARN}},
                             {"Description": "d", "KeyUsage": "ENCRYPT_DECRYPT", "KeySpec": "SYMMETRIC_DEFAULT"})
        sleeps: List[float] = []
        assert manager(client, db, sleeps=sleeps).create_master_key("d") == ARN
        assert len(sleeps) == 1

    def test_create_master_key(self, kms, db):
        client, stubber = kms
        stubber.add_response("create_key", {"KeyMetadata": {"KeyId": "k", "Arn": ARN}},
                             {"Description": "d", "KeyUsage": "ENCRYPT_DECRYPT", "KeySpec": "SYMMETRIC_DEFAULT",
                              "Policy": "{}"})
        assert manager(client, db).create_master_key("d", policy="{}") == ARN

    @pytest.mark.parametrize("metadata, expected", [
        ({"KeyId": "k", "Enabled": True, "KeyState": "Enabled", "KeyUsage": "ENCRYPT_DECRYPT"}, True),
        ({"KeyId": "k", "Enabled": False, "KeyState": "Disabled", "KeyUsage": "ENCRYPT_DECRYPT"}, False),
        ({"KeyId": "k", "Enabled": True, "KeyState": "Enabled", "KeyUsage": "SIGN_VERIFY"}, False),
    ])
    def test_is_master_key_valid(self, kms, db, metadata, expected):
        client, stubber = kms
        stubber.add_response("describe_key", {"KeyMetadata": metadata}, {"KeyId": ARN})
        assert manager(client, db).is_master_key_valid(ARN) is expected

    def test_is_master_key_valid_is_false_on_error(self, kms, db):
        client, stubber = kms
        stubber.add_client_error("describe_key", "NotFoundException", http_status_code=400)
        assert manager(client, db).is_master_key_valid(ARN) is False


class TestKeyStorage:
    def _key(self, **kw) -> KeyMetadata:
        values = dict(key_name="n", master_key_arn=ARN, encrypted_data_key=EDK, hmac_key=b"H" * 32)
        values.update(kw)
        return KeyMetadata(**values)  # type: ignore[arg-type]

    def test_store_on_postgres_uses_returning(self, kms, db):
        db.result = (["id"], [(42,)])
        stored = manager(kms[0], db).store_key_metadata(self._key())
        assert stored.id == 42
        assert stored.key_id and len(stored.key_id) == 36  # generated UUID
        sql, params = db.executed[0]
        assert sql.startswith("INSERT INTO encrypt.key_storage ") and sql.endswith(" RETURNING id")
        assert params[4] == b"H" * 32 and params[5] == "AES_256"

    def test_store_on_mysql_uses_lastrowid_and_keeps_given_uuid(self, kms, db):
        db.lastrowid = 9
        stored = manager(kms[0], db, sql=MYSQL).store_key_metadata(self._key(key_id="given"))
        assert (stored.id, stored.key_id) == (9, "given")
        assert "RETURNING" not in db.executed[0][0]

    def test_unusable_metadata_is_refused(self, kms, db):
        with pytest.raises(InvalidKeyMetadataError):
            manager(kms[0], db).store_key_metadata(self._key(encrypted_data_key=None))
        assert db.executed == []

    def test_store_failure_does_not_leak_the_hmac_key(self, kms, db):
        secret = b"SECRET-HMAC-" + b"x" * 20
        db.error = RuntimeError(f"syntax error near '{secret!r}'")
        with pytest.raises(KeyManagementError) as err:
            manager(kms[0], db).store_key_metadata(self._key(hmac_key=secret))
        assert "SECRET-HMAC" not in str(err.value)
        assert err.value.__cause__ is None
        assert err.value.__context__ is None
        assert "RuntimeError" in str(err.value)

    def test_store_failure_reports_the_driver_error_codes(self, kms, db):
        # SQLSTATE and errno carry no statement text, so they can be shown.
        class DriverError(Exception):
            sqlstate = "42P01"
            errno = 1146

        db.error = DriverError("relation key_storage does not exist: 'SECRET-HMAC'")
        with pytest.raises(KeyManagementError) as err:
            manager(kms[0], db).store_key_metadata(self._key())
        assert "DriverError" in str(err.value) and "sqlstate 42P01" in str(err.value)
        assert "errno 1146" in str(err.value) and "SECRET" not in str(err.value)

    def test_store_on_postgres_reads_the_id_from_a_dict_row(self, kms, db):
        class DictCursor(FakeCursor):
            def fetchone(self):
                return {"id": 42}

        db.cursor = lambda: DictCursor(db)  # type: ignore[method-assign]
        db.result = (["id"], [])
        assert manager(kms[0], db).store_key_metadata(self._key()).id == 42

    def test_key_metadata_by_id(self, kms, db):
        db.result = (["ID", "KEY_ID", "NAME", "MASTER_KEY_ARN", "ENCRYPTED_DATA_KEY", "HMAC_KEY", "KEY_SPEC",
                      "CREATED_AT", "LAST_USED_AT"],
                     [(3, "u", "n", ARN, EDK, memoryview(b"hk"), "AES_256", None, None)])
        km = manager(kms[0], db).key_metadata_by_id(3)
        assert km is not None and km.id == 3 and km.hmac_key == b"hk" and km.master_key_arn == ARN
        assert "WHERE id OPERATOR(pg_catalog.=) %s" in db.executed[0][0]
        assert db.executed[0][1] == (3,)

    def test_key_metadata_by_id_missing(self, kms, db):
        db.result = (["id"], [])
        assert manager(kms[0], db).key_metadata_by_id(3) is None

    def test_key_metadata_by_id_failure(self, kms, db):
        # The plugin passes ids read from stored values, which may be four
        # bytes of legacy plaintext, so the message leaves the id out.
        db.error = RuntimeError("boom")
        with pytest.raises(KeyManagementError, match="boom") as err:
            manager(kms[0], db).key_metadata_by_id(1802201963)
        assert "1802201963" not in str(err.value)

    def test_touch_key_swallows_errors(self, kms, db):
        db.error = RuntimeError("boom")
        manager(kms[0], db).touch_key("u")
        assert "last_used_at = CURRENT_TIMESTAMP" in db.executed[0][0]


# ---- botocore log filters ----

B64_KEY = base64.b64encode(DATA_KEY).decode()
# Distinctive fake credentials: easy to search for, and no test signs with the
# ambient credential chain.
FAKE_KEY_ID = "KIROUNITFAKEKEYID0001"
FAKE_SECRET = "kiro-unit-fake-secret-key"
FAKE_TOKEN = "kiro-unit-fake-session-token"
UNREACHABLE = "http://127.0.0.1:9"  # never contacted: the canned answer comes first
KMS_OPERATIONS = ["TrentService.GenerateDataKey", "TrentService.Decrypt"]


class _Body:
    """The raw stream botocore reads a canned response from."""

    def __init__(self, body: bytes):
        self._body = body

    def stream(self, **kwargs):
        yield self._body


def _session():
    return boto3.Session(aws_access_key_id=FAKE_KEY_ID, aws_secret_access_key=FAKE_SECRET,
                         aws_session_token=FAKE_TOKEN)


def _answer_kms(client) -> Tuple[List[str], List[str]]:
    """Answer each KMS request just before it would be sent, so botocore's
    real signer and parser run and nothing leaves the host. Returns the
    operations answered and the signatures of their requests."""
    answered: List[str] = []
    signatures: List[str] = []

    def answer(request, **kwargs):
        target = request.headers.get("X-Amz-Target", b"")
        target = target.decode() if isinstance(target, bytes) else target
        answered.append(target)
        auth = request.headers.get("Authorization", b"")
        auth = auth.decode() if isinstance(auth, bytes) else auth
        signatures.append(auth.rpartition("Signature=")[2])
        body = {"KeyId": ARN, "Plaintext": B64_KEY}
        if target.endswith("GenerateDataKey"):
            body["CiphertextBlob"] = EDK
        return AWSResponse(request.url, 200, {"Content-Type": "application/x-amz-json-1.1"},
                           _Body(json.dumps(body).encode()))

    client.meta.events.register("before-send.kms.*", answer)
    return answered, signatures


def _call_kms(client) -> int:
    """GenerateDataKey then Decrypt; returns the id of the thread that called."""
    # A fresh manager and cache, so Decrypt reaches KMS instead of the cache.
    cache = DataKeyCache(10, 60, start_cleanup_thread=False)
    try:
        km = KeyManager(client, FakeDb(), PG, config(), cache)
        km.generate_data_key(ARN)
        km.decrypt_data_key(EDK, ARN)
    finally:
        cache.close()
    return threading.get_ident()


def _run(client, on_thread: bool) -> None:
    if not on_thread:
        _call_kms(client)
        return
    with ThreadPoolExecutor(max_workers=1) as pool:
        worker = pool.submit(_call_kms, client).result(timeout=30)
    assert worker != threading.get_ident()


@contextmanager
def _debug_log(caplog):
    """Root and botocore's loggers at DEBUG for the block: the setup that leaks."""
    with ExitStack() as stack:
        for name in LOGGERS:
            stack.enter_context(caplog.at_level(logging.DEBUG, logger=name))
        yield caplog.records


def _holding(records, text: str) -> List[logging.LogRecord]:
    return [r for r in records if text in r.getMessage()]


def _records(records, logger: str, msg: str) -> List[logging.LogRecord]:
    return [r for r in records if r.name == logger and r.msg == msg]


def _state(name: str):
    lg = logging.getLogger(name)
    return lg.level, list(lg.handlers), lg.propagate, list(lg.filters)


class TestBotocoreLogFilters:
    @pytest.mark.parametrize("on_thread", [False, True], ids=["caller", "worker"])
    def test_botocore_logs_kms_secrets_at_debug(self, caplog, on_thread):
        # The canaries: if botocore changes one of these records, the filter
        # rules need another look.
        client = _session().client("kms", region_name="us-east-1")
        answered, signatures = _answer_kms(client)
        with _debug_log(caplog) as records:
            _run(client, on_thread)
        assert answered == KMS_OPERATIONS
        bodies = _records(records, "botocore.parsers", "Response body:\n%r")
        assert len(bodies) == 2 and len(_holding(records, B64_KEY)) == 2
        assert all(isinstance(r.args, tuple) and isinstance(r.args[0], bytes) and b'"Plaintext"' in r.args[0]
                   for r in bodies)
        sent = _records(records, "botocore.endpoint", "Sending http request: %s")
        canonical = _records(records, "botocore.auth", "CanonicalRequest:\n%s")
        assert len(sent) == 2 and len(canonical) == 2
        assert all(FAKE_KEY_ID in r.getMessage() and FAKE_TOKEN in r.getMessage() for r in sent)
        assert all(FAKE_TOKEN in r.getMessage() for r in canonical)
        signed = _records(records, "botocore.auth", "Signature:\n%s")
        assert [r.getMessage() for r in signed] == [f"Signature:\n{s}" for s in signatures]
        assert len(signatures) == 2 and all(len(s) == 64 for s in signatures)

    @pytest.mark.parametrize("endpoint", [None, UNREACHABLE], ids=["kms", "endpoint"])
    @pytest.mark.parametrize("on_thread", [False, True], ids=["caller", "worker"])
    def test_create_kms_client_keeps_data_keys_out_of_the_log(self, caplog, on_thread, endpoint):
        client = KeyManager.create_kms_client(_session(), config(kms_endpoint=endpoint))
        answered, signatures = _answer_kms(client)
        with _debug_log(caplog) as records:
            _run(client, on_thread)
        assert answered == KMS_OPERATIONS
        assert _holding(records, B64_KEY) == []
        assert _holding(records, '"Plaintext"') == []
        assert len(_records(records, "botocore.parsers", "Response headers: %r")) == 2
        assert _records(records, "botocore.auth", "Signature:\n%s") == []
        assert len(signatures) == 2 and all(_holding(records, s) == [] for s in signatures)

    @pytest.mark.parametrize("endpoint", [None, UNREACHABLE], ids=["kms", "endpoint"])
    @pytest.mark.parametrize("on_thread", [False, True], ids=["caller", "worker"])
    def test_create_kms_client_keeps_request_credentials_out_of_the_log(self, caplog, on_thread, endpoint):
        client = KeyManager.create_kms_client(_session(), config(kms_endpoint=endpoint))
        answered, signatures = _answer_kms(client)
        with _debug_log(caplog) as records:
            _run(client, on_thread)
        assert answered == KMS_OPERATIONS
        for secret in (FAKE_KEY_ID, FAKE_SECRET, FAKE_TOKEN):
            assert _holding(records, secret) == []
        assert _records(records, "botocore.auth", "Signature:\n%s") == []
        assert len(signatures) == 2 and all(_holding(records, s) == [] for s in signatures)
        # The unsigned record names the KMS target but carries no credential,
        # so it stays: the rule needs both parts.
        making = _records(records, "botocore.endpoint", "Making request for %s with params: %s")
        assert len(making) == 2 and all("TrentService." in r.getMessage() for r in making)

    def test_other_services_lose_only_the_signature_record(self, caplog):
        session = _session()
        sts = session.client("sts", region_name="us-east-1")
        body = (b'<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><GetCallerIdentityResult>'
                b"<Arn>arn:aws:iam::123456789012:user/u</Arn></GetCallerIdentityResult></GetCallerIdentityResponse>")
        sts.meta.events.register(
            "before-send.sts.GetCallerIdentity",
            lambda request, **kwargs: AWSResponse(request.url, 200, {"Content-Type": "text/xml"}, _Body(body)))
        sts.get_caller_identity()  # warm up, so both captures see the same records

        def capture() -> List[logging.LogRecord]:
            caplog.clear()
            with _debug_log(caplog) as records:
                sts.get_caller_identity()
            # botocore's records only: a thread another test left running can log while the root logger is at DEBUG
            return [r for r in records if r.name.split(".")[0] == "botocore"]

        without = capture()
        KeyManager.create_kms_client(session, config())
        with_filters = capture()
        # The signature also sits in the Authorization header of the request
        # record, which other services keep.
        expected = [(r.name, r.msg) for r in without]
        assert expected.count(("botocore.auth", "Signature:\n%s")) == 1
        expected.remove(("botocore.auth", "Signature:\n%s"))
        assert [(r.name, r.msg) for r in with_filters] == expected
        sent = _records(with_filters, "botocore.endpoint", "Sending http request: %s")
        canonical = _records(with_filters, "botocore.auth", "CanonicalRequest:\n%s")
        assert len(sent) == 1 and FAKE_KEY_ID in sent[0].getMessage() and FAKE_TOKEN in sent[0].getMessage()
        assert "Signature=" in sent[0].getMessage()
        assert len(canonical) == 1 and FAKE_TOKEN in canonical[0].getMessage()

    def test_filters_are_installed_once_on_their_loggers_only(self):
        # Pin a level and propagate flag first, so a change the install makes
        # shows even if an earlier install in this process made it already.
        loggers = [logging.getLogger(name) for name in LOGGERS]
        saved = [(lg.level, lg.propagate) for lg in loggers]
        try:
            for lg in loggers:
                lg.setLevel(logging.ERROR + 1)
                lg.propagate = True
            before = {name: _state(name) for name in LOGGERS}
            KeyManager.create_kms_client(_session(), config())
            KeyManager.create_kms_client(_session(), config(kms_endpoint=UNREACHABLE))
            after = {name: _state(name) for name in LOGGERS}
        finally:
            for lg, (level, propagate) in zip(loggers, saved):
                lg.setLevel(level)
                lg.propagate = propagate
        added = {}
        for name in LOGGERS:
            level, handlers, propagate, filters = after[name]
            assert (level, handlers, propagate) == before[name][:3]
            added[name] = [type(f) for f in filters if f not in before[name][3]]
        assert added == {"": [], "botocore": [], "botocore.parsers": [km_module._NoKmsPlaintext],
                         "botocore.endpoint": [km_module._NoKmsRequestCredentials],
                         "botocore.auth": [km_module._NoKmsRequestCredentials]}

    def test_a_removed_filter_stays_removed(self):
        # The filters are installed once per process, so a positive control
        # that removes a filter and builds a new client sees the leak.
        KeyManager.create_kms_client(_session(), config())
        for name in LOGGERS:
            lg = logging.getLogger(name)
            lg.filters = [f for f in lg.filters if not isinstance(f, OUR_FILTERS)]
        KeyManager.create_kms_client(_session(), config())
        assert not any(isinstance(f, OUR_FILTERS) for name in LOGGERS for f in logging.getLogger(name).filters)

    def test_concurrent_first_installs_add_each_filter_once(self, mocker):
        # Thread A's first addFilter call waits until thread B has had time to
        # run its own first install. The lock holds B until A is done, so B
        # finds the filters installed and adds none.
        entered, release = threading.Event(), threading.Event()
        real_add_filter = logging.Logger.addFilter

        def add_filter(lg: logging.Logger, flt: logging.Filter) -> None:
            if lg.name == "botocore.parsers" and not entered.is_set():
                entered.set()
                release.wait(5)
            real_add_filter(lg, flt)

        mocker.patch.object(logging.Logger, "addFilter", add_filter)
        a = threading.Thread(target=km_module._install_botocore_filters, daemon=True)
        b = threading.Thread(target=km_module._install_botocore_filters, daemon=True)
        a.start()
        try:
            assert entered.wait(5), "thread A never reached addFilter"
            b.start()
            b.join(0.2)
        finally:
            # Release A and join both threads on every path, so neither thread
            # is still adding filters when no_filters restores the loggers.
            release.set()
            for thread in (a, b):
                if thread.is_alive():
                    thread.join(5)
        installed = {name: [type(f) for f in logging.getLogger(name).filters if isinstance(f, OUR_FILTERS)]
                     for name in ("botocore.parsers", "botocore.endpoint", "botocore.auth")}
        assert installed == {"botocore.parsers": [km_module._NoKmsPlaintext],
                             "botocore.endpoint": [km_module._NoKmsRequestCredentials],
                             "botocore.auth": [km_module._NoKmsRequestCredentials]}


def _record(name: str, msg: str, args) -> logging.LogRecord:
    return logging.LogRecord(name, logging.DEBUG, __file__, 0, msg, args, None)


class TestBotocoreLogFilterRules:
    @pytest.mark.parametrize("msg, args, kept", [
        ("Response body:\n%r", (b'{"KeyId": "k", "Plaintext": "x"}',), False),
        ("Response body:\n%r", (bytearray(b'{"Plaintext": "x"}'),), False),
        ("%s %r", ("Response body", b'{"Plaintext": "x"}'), False),
        ("Response body:\n%r", ('{"Plaintext": "x"}',), True),
        ("Response body:\n%r", (b'{"KeyId": "k"}',), True),
        ("Response headers: %r", None, True),
        ("%(body)s", ({"body": b'{"Plaintext": "x"}'},), True),
    ])
    def test_plaintext_filter_drops_only_bytes_carrying_a_plaintext_field(self, msg, args, kept):
        assert km_module._NoKmsPlaintext().filter(_record("botocore.parsers", msg, args)) is kept

    @pytest.mark.parametrize("msg, args, kept", [
        ("Sending http request: %s", ("{'X-Amz-Target': b'TrentService.Decrypt', 'Authorization': b'AWS4'}",), False),
        ("CanonicalRequest:\n%s", ("x-amz-security-token:t\nx-amz-target:TrentService.Decrypt",), False),
        ("CanonicalRequest:\n%s", ("X-AMZ-SECURITY-TOKEN:t\nx-amz-target:TrentService.Decrypt",), False),
        ("Signature:\n%s", ("0f3a" * 16,), False),
        ("Signature:\n%s", ("TrentService.Decrypt " + "0f3a" * 16,), False),
        ("Making request for %s with params: %s", ("Decrypt", "{'X-Amz-Target': 'TrentService.Decrypt'}"), True),
        ("Sending http request: %s", ("{'X-Amz-Target': b'Other.Op', 'Authorization': b'AWS4'}",), True),
        ("Calculating signature using v4 auth.", None, True),
        ("StringToSign:\n%s", ("AWS4-HMAC-SHA256\nSignature:\n" + "0f3a" * 16,), True),
        ("Signature: %s", ("0f3a" * 16,), True),
        ("TrentService. Authorization %s %s", ("only one argument",), True),
    ])
    def test_request_filter_drops_only_signed_kms_records(self, msg, args, kept):
        assert km_module._NoKmsRequestCredentials().filter(_record("botocore.endpoint", msg, args)) is kept
