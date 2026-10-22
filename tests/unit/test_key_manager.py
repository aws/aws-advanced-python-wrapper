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
from contextlib import contextmanager
from typing import Any, List, Optional, Tuple

import boto3
import pytest
from botocore.exceptions import EndpointConnectionError
from botocore.stub import Stubber

from aws_advanced_python_wrapper.encryption import key_manager as km_module
from aws_advanced_python_wrapper.encryption.algorithm import \
    EncryptionAlgorithm
from aws_advanced_python_wrapper.encryption.config import EncryptionConfig
from aws_advanced_python_wrapper.encryption.data_key_cache import DataKeyCache
from aws_advanced_python_wrapper.encryption.key_manager import KeyManager
from aws_advanced_python_wrapper.encryption.models import KeyMetadata
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
        db.error = RuntimeError("boom")
        with pytest.raises(KeyManagementError, match="boom"):
            manager(kms[0], db).key_metadata_by_id(3)

    def test_touch_key_swallows_errors(self, kms, db):
        db.error = RuntimeError("boom")
        manager(kms[0], db).touch_key("u")
        assert "last_used_at = CURRENT_TIMESTAMP" in db.executed[0][0]
