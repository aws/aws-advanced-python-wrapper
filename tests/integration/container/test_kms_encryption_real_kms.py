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

"""The KMS Encryption Plugin against a real KMS key, which LocalStack can't stand in for.

LocalStack ignores KeySpec and IAM, and has neither CloudTrail nor STS session expiry.
The key is ``KMS_KEY_ARN``: an administrator's key, or the one the Gradle harness finds or makes under its KMS alias
and keeps for later runs; these tests never create, change or delete a key or a role.
Setup runs on the credentials the class starts with (the CI or maintainer role).
The Decrypt-only role (``KMS_DECRYPT_ONLY_ROLE_ARN``) is assumed inside the tests, narrowed where a test needs it by an
inline session policy, and its credentials reach the plugin through the environment for the blocks that need them.
The last three tests are manual: they run only with the ``KMS_MANUAL_TESTS`` feature, which the harness sets when
``KMS_MANUAL_TESTS=1`` (CloudTrail's delivery delay, a 900-second session, an IAM condition), never in CI.
"""

from __future__ import annotations

import base64
import json
import secrets
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import (TYPE_CHECKING, Any, Callable, Dict, Iterator, List,
                    Optional, Tuple)

import boto3
import pytest
from botocore.exceptions import ClientError

from aws_advanced_python_wrapper.aws_credentials_manager import \
    AwsCredentialsManager
from aws_advanced_python_wrapper.encryption.algorithm import \
    EncryptionAlgorithm
from aws_advanced_python_wrapper.encryption.key_management_utility import \
    KeyManagementUtility
from .utils.conditions import (disable_on_features, enable_on_deployments,
                               enable_on_engines, enable_on_features)
from .utils.database_engine import DatabaseEngine
from .utils.database_engine_deployment import DatabaseEngineDeployment
from .utils.kms_encryption_helper import (TRIGGER_REJECTION,
                                          KmsEncryptionTestBase, binary_type,
                                          class_name, create_table, drop_table,
                                          encrypt_column, encryption_config,
                                          is_pg, kms_builds, kms_client,
                                          plain_connect, raised, read_column,
                                          rotate_under_trigger,
                                          wrapper_connect, write)
from .utils.retry_helper import retry_until
from .utils.test_environment_features import TestEnvironmentFeatures
from .utils.test_timings import (KMS_CLOUDTRAIL_DELIVERY_SEC,
                                 KMS_CLOUDTRAIL_POLL_SEC,
                                 KMS_ROLE_SESSION_EXPIRY_POLL_SEC,
                                 KMS_ROLE_SESSION_EXPIRY_WAIT_SEC,
                                 KMS_ROLE_SESSION_SEC)

if TYPE_CHECKING:
    from pathlib import Path

    from .utils.kms_encryption_helper import (DbUser, KmsSettings,
                                              PayloadVerifier)

_TABLE = "kms_real_users"
_REFUSALS_TABLE = "kms_real_refusals"
_INSERT = f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s, %s)"
_SELECT = f"SELECT ssn FROM {_TABLE} WHERE name = %s ORDER BY id"
_SEED = ("seed", "pt-real-seed")
# The request parameters a KMS event may carry: no encryption context, nothing derived from a value.
_ALLOWED_EVENT_PARAMETERS = {"encryptionAlgorithm", "keyId", "keySpec"}


@dataclass(frozen=True)
class RoleSession:
    """Temporary credentials of an assumed role, kept out of every repr, so no report shows them."""
    access_key_id: str = field(repr=False)
    secret_access_key: str = field(repr=False)
    session_token: str = field(repr=False)


def _decrypt_only_role_arn(settings: KmsSettings) -> str:
    if settings.decrypt_only_role_arn is None:
        pytest.fail("A real-key environment without KMS_DECRYPT_ONLY_ROLE_ARN: set it to run the real-KMS tests.")
    return settings.decrypt_only_role_arn


def _assume_decrypt_only(settings: KmsSettings, session_name: str, session_policy: Optional[Dict[str, Any]] = None
                         ) -> RoleSession:
    """A session of the Decrypt-only role as short as STS allows, narrowed by ``session_policy`` when it is given."""
    role_arn = _decrypt_only_role_arn(settings)
    extra = {} if session_policy is None else {"Policy": json.dumps(session_policy)}
    credentials = boto3.client("sts", region_name=settings.region).assume_role(
        RoleArn=role_arn, RoleSessionName=session_name, DurationSeconds=KMS_ROLE_SESSION_SEC,
        **extra)["Credentials"]
    return RoleSession(credentials["AccessKeyId"], credentials["SecretAccessKey"], credentials["SessionToken"])


@contextmanager
def _plugin_credentials(role: RoleSession) -> Iterator[None]:
    """Inside the block, a plugin connection's first statement builds its KMS client from ``role``.

    The wrapper keeps one boto3 session per host and region, so the sessions are released on the way in and out.
    """
    AwsCredentialsManager.release_resources()
    with pytest.MonkeyPatch.context() as patch:
        for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE"):
            patch.delenv(name, raising=False)
        patch.setenv("AWS_ACCESS_KEY_ID", role.access_key_id)
        patch.setenv("AWS_SECRET_ACCESS_KEY", role.secret_access_key)
        patch.setenv("AWS_SESSION_TOKEN", role.session_token)
        try:
            yield
        finally:
            AwsCredentialsManager.release_resources()


def _boto3_error(call: Callable[[], Any]) -> Optional[BaseException]:
    """The error a boto3 ``call`` raised, or None."""
    try:
        call()
    except Exception as err:  # noqa: BLE001 - the test asserts on the class
        return err
    return None


def _count_rows(table: str, name: str) -> int:
    with plain_connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE name = %s", (name,))
        return cur.fetchone()[0]


@enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY])
@disable_on_features([TestEnvironmentFeatures.KMS_LOCALSTACK])
@enable_on_deployments([DatabaseEngineDeployment.DOCKER])
class TestKmsEncryptionRealKms(KmsEncryptionTestBase):

    @pytest.fixture(scope="class")
    def table(self, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
              kms_app_user: DbUser) -> Iterator[int]:
        """The class's table, ``ssn`` encrypted under the real key by the setup credentials, with one seed row written
        through the plugin on them; yields the ``key_storage`` id."""
        create_table(_TABLE, f"name VARCHAR(100), ssn {binary_type()}")
        try:
            with plain_connect() as conn:
                key_storage_id = encrypt_column(conn, kms_settings, kms_schema, _TABLE, "ssn", kms_master_key_arn)
            build = "default" if is_pg() else "mysql-cext"
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
                write(conn, _INSERT, _SEED)
            yield key_storage_id
        finally:
            drop_table(_TABLE)

    @pytest.fixture
    def sized_table(self, build: str) -> Iterator[str]:
        """A table of the test's own with two unencrypted columns, ``ssn128`` and ``ssn256``."""
        name = f"kms_real_sized_{build.replace('-', '_')}"
        create_table(name, f"name VARCHAR(100), ssn128 {binary_type()}, ssn256 {binary_type()}")
        try:
            yield name
        finally:
            drop_table(name)

    @pytest.fixture(scope="class")
    def missing_key_arn(self, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
                        kms_app_user: DbUser) -> Iterator[str]:
        """The refusals table: ``ssn`` and ``ssn_gone`` each under a key row of their own, a seed row in both written
        through the plugin, and then ``ssn_gone``'s key row repointed (in the database only) to a well-formed ARN in
        the key's account and region that names no key; yields that ARN."""
        create_table(_REFUSALS_TABLE, f"name VARCHAR(100), ssn {binary_type()}, ssn_gone {binary_type()}")
        try:
            key_ids = {}
            with plain_connect() as conn:
                for column in ("ssn", "ssn_gone"):
                    key_ids[column] = encrypt_column(conn, kms_settings, kms_schema, _REFUSALS_TABLE, column,
                                                     kms_master_key_arn)
            build = "default" if is_pg() else "mysql-cext"
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
                write(conn, f"INSERT INTO {_REFUSALS_TABLE} (name, ssn, ssn_gone) VALUES (%s, %s, %s)",
                      (_SEED[0], _SEED[1], _SEED[1]))
            arn = kms_master_key_arn.rsplit("/", 1)[0] + "/" + str(uuid.uuid4())
            with plain_connect() as conn, conn.cursor() as cur:
                cur.execute(f"UPDATE {kms_schema}.key_storage SET master_key_arn = %s WHERE id = %s",
                            (arn, key_ids["ssn_gone"]))
                conn.commit()
            yield arn
        finally:
            drop_table(_REFUSALS_TABLE)

    @kms_builds()
    def test_data_key_sized_for_column_algorithm(self, build: str, sized_table: str, kms_settings: KmsSettings,
                                                 kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser,
                                                 kms_verifier: PayloadVerifier) -> None:
        columns = {"ssn128": EncryptionAlgorithm.AES_128_GCM, "ssn256": EncryptionAlgorithm.AES_256_GCM}
        key_ids = {}
        rows = {}
        with plain_connect() as conn:
            utility = KeyManagementUtility(conn, kms_client(kms_settings), encryption_config(kms_settings, kms_schema))
            for column, algorithm in columns.items():
                key_ids[column] = utility.initialize_encryption_for_column(sized_table, column, kms_master_key_arn,
                                                                           algorithm)
            conn.commit()
            with conn.cursor() as cur:
                for column, key_storage_id in key_ids.items():
                    cur.execute(f"SELECT key_spec, encrypted_data_key FROM {kms_schema}.key_storage WHERE id = %s",
                                (key_storage_id,))
                    rows[column] = cur.fetchone()
        kms = kms_client(kms_settings)
        sizes = {}
        for column, (key_spec, encrypted_data_key) in rows.items():
            plaintext = kms.decrypt(CiphertextBlob=base64.b64decode(encrypted_data_key),
                                    KeyId=kms_master_key_arn)["Plaintext"]
            sizes[column] = (key_spec, len(plaintext))
        # On real KMS an AES-128-GCM column gets a 16-byte AES_128 data key and an AES-256-GCM one a 32-byte key.
        assert sizes == {"ssn128": ("AES_128", 16), "ssn256": ("AES_256", 32)}

        name, value = f"sized-{build}", f"pt-sized-{build}"
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
            write(conn, f"INSERT INTO {sized_table} (name, ssn128, ssn256) VALUES (%s, %s, %s)", (name, value, value))
            with conn.cursor() as cur:
                cur.execute(f"SELECT ssn128, ssn256 FROM {sized_table} WHERE name = %s", (name,))
                read = list(cur.fetchall())
        stored = {}
        for column in columns:
            payloads = kms_verifier.stored(f"SELECT {column} FROM {sized_table} WHERE name = %s", (name,))
            stored[column] = [kms_verifier.decode(sized_table, column, payload) for payload in payloads]
        assert read == [(value, value)]
        assert stored == {"ssn128": [(key_ids["ssn128"], value)], "ssn256": [(key_ids["ssn256"], value)]}

    @kms_builds(exclude=("mysql-pure",))
    def test_decrypt_only_credentials_read_and_write(
            self, build: str, table: int, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
            kms_app_user: DbUser, kms_verifier: PayloadVerifier) -> None:
        role = _assume_decrypt_only(kms_settings, "kms-it-decrypt-only")
        written, updated = f"decrypt-only-{build}", f"decrypt-only-updated-{build}"
        with _plugin_credentials(role):
            caller = boto3.Session(region_name=kms_settings.region).client("sts").get_caller_identity()["Arn"]
            # A session made inside the block, as the plugin's is, runs on the Decrypt-only role. Booleans, so a
            # failure prints no ARN.
            assumed = ":assumed-role/" in caller
            assert assumed is True, "a session made inside the block doesn't run on an assumed role"
            named = caller.endswith("/kms-it-decrypt-only")
            assert named is True, "a session made inside the block doesn't run on the Decrypt-only role's session"
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                                 encryption_data_key_cache_enabled=False) as conn:
                write(conn, _INSERT, (written, "pt-decrypt-only-written"))
                write(conn, _INSERT, (updated, "pt-decrypt-only-before"))
                write(conn, f"UPDATE {_TABLE} SET ssn = %s WHERE name = %s", ("pt-decrypt-only-after", updated))
                reads = {name: read_column(conn, _SELECT, name) for name in (written, updated, _SEED[0])}
            generate = _boto3_error(lambda: kms_client(kms_settings).generate_data_key(
                KeyId=kms_master_key_arn, KeySpec="AES_256"))
        stored = {}
        for name in (written, updated):
            payloads = kms_verifier.stored(f"SELECT ssn FROM {_TABLE} WHERE name = %s", (name,))
            stored[name] = [kms_verifier.decode(_TABLE, "ssn", payload) for payload in payloads]
        # The seed, written on the setup credentials, reads back too.
        assert reads == {written: ["pt-decrypt-only-written"], updated: ["pt-decrypt-only-after"],
                         _SEED[0]: [_SEED[1]]}
        assert stored == {written: [(table, "pt-decrypt-only-written")], updated: [(table, "pt-decrypt-only-after")]}
        # A boolean, so a failure prints no part of KMS's error, which names the account.
        generate_denied = isinstance(generate, ClientError) and generate.response["Error"]["Code"] == \
            "AccessDeniedException"
        assert generate_denied is True, "the Decrypt-only role can generate a data key"

    @kms_builds(exclude=("mysql-pure",))
    def test_real_kms_refusals_mask_arns(self, build: str, missing_key_arn: str, kms_settings: KmsSettings,
                                         kms_schema: str, kms_master_key_arn: str, kms_app_user: DbUser,
                                         kms_calls: List[Tuple[str, Optional[str]]]) -> None:
        select = f"SELECT ssn FROM {_REFUSALS_TABLE} WHERE name = %s"
        denied = _assume_decrypt_only(kms_settings, "kms-it-denied", {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "kms:DescribeKey", "Resource": "*"}]})
        other_region = "us-west-2" if kms_settings.region != "us-west-2" else "us-east-1"
        errors: Dict[str, Optional[BaseException]] = {}
        requests: Dict[str, List[str]] = {}
        with _plugin_credentials(denied):
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
                kms_calls.clear()
                errors["access denied"] = raised(conn, lambda: read_column(conn, select, _SEED[0]))
                requests["access denied"] = [operation for operation, _ in kms_calls]
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema,
                             f"{kms_master_key_arn},{missing_key_arn}") as conn:
            kms_calls.clear()
            errors["no such key"] = raised(
                conn, lambda: read_column(conn, f"SELECT ssn_gone FROM {_REFUSALS_TABLE} WHERE name = %s", _SEED[0]))
            requests["no such key"] = [operation for operation, _ in kms_calls]
        with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                             encryption_kms_region=other_region) as conn:
            kms_calls.clear()
            errors["other region"] = raised(conn, lambda: read_column(conn, select, _SEED[0]))
            requests["other region"] = [operation for operation, _ in kms_calls]

        # Each refusal fails the read, returning no value.
        assert {case: class_name(err) for case, err in errors.items()} == dict.fromkeys(errors, "KeyManagementError")
        # A refused Decrypt isn't retried.
        assert requests == dict.fromkeys(errors, ["Decrypt"])
        account, key_id = kms_master_key_arn.split(":")[4], kms_master_key_arn.rsplit("/", 1)[1]
        parts = {"account": account, "key id": key_id, "missing key's ARN": missing_key_arn}
        # Labels only, so a failure prints no part of an ARN.
        unmasked = {}
        for case, err in errors.items():
            unmasked[case] = [label for label, part in parts.items() if part in f"{err} {err!r}"]
        assert unmasked == dict.fromkeys(errors, [])
        # A boolean, so a failure prints neither the account nor KMS's error.
        cause_names_account = account in str(getattr(errors["access denied"], "__cause__", None))
        assert cause_names_account is True, \
            "KMS's own error, kept as the cause, doesn't name the account, so the check above masks nothing"

    @kms_builds()
    def test_aws_profile_role_decrypts_on_real_kms(self, build: str, table: int, tmp_path: Path,
                                                   kms_settings: KmsSettings, kms_schema: str,
                                                   kms_master_key_arn: str, kms_app_user: DbUser,
                                                   kms_verifier: PayloadVerifier) -> None:
        role_arn = _decrypt_only_role_arn(kms_settings)
        # A profile that assumes the Decrypt-only role from the credentials in the environment.
        config_file = tmp_path / "aws-config"
        config_file.write_text(f"[profile kms-it-profile]\nrole_arn = {role_arn}\ncredential_source = Environment\n"
                               f"duration_seconds = {KMS_ROLE_SESSION_SEC}\nrole_session_name = kms-it-profile\n")
        credentials = boto3.Session(region_name=kms_settings.region).get_credentials()
        assert credentials is not None
        base = credentials.get_frozen_credentials()
        base_role = RoleSession(base.access_key or "", base.secret_key or "", base.token or "")
        name = f"profile-{build}"
        with _plugin_credentials(base_role), pytest.MonkeyPatch.context() as patch:
            patch.setenv("AWS_CONFIG_FILE", str(config_file))
            caller = boto3.Session(profile_name="kms-it-profile", region_name=kms_settings.region).client(
                "sts").get_caller_identity()["Arn"]
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                                 aws_profile="kms-it-profile", encryption_data_key_cache_enabled=False) as conn:
                write(conn, _INSERT, (name, "pt-profile"))
                reads = {row: read_column(conn, _SELECT, row) for row in (name, _SEED[0])}
        stored = [kms_verifier.decode(_TABLE, "ssn", payload)
                  for payload in kms_verifier.stored(f"SELECT ssn FROM {_TABLE} WHERE name = %s", (name,))]
        # Booleans, so a failure prints no ARN.
        assumed = ":assumed-role/" in caller
        assert assumed is True, "a session on the profile doesn't run on an assumed role"
        on_role = caller.split("/")[-2] == role_arn.rsplit("/", 1)[-1]
        assert on_role is True, "a session on the profile doesn't run on the Decrypt-only role"
        named = caller.endswith("/kms-it-profile")
        assert named is True, "a session on the profile doesn't run on the profile's role session"
        # The seed, written on the setup credentials, reads back too.
        assert reads == {name: ["pt-profile"], _SEED[0]: [_SEED[1]]}
        assert stored == [(table, "pt-profile")]

    @kms_builds()
    def test_rotation_on_real_kms_keeps_old_values_readable(self, build: str, kms_trigger_installed: None,
                                                            kms_settings: KmsSettings, kms_schema: str,
                                                            kms_master_key_arn: str, kms_app_user: DbUser,
                                                            kms_verifier: PayloadVerifier) -> None:
        # The new data key comes from a real GenerateDataKey under the task's key.
        result = rotate_under_trigger(build, "real-rotation", kms_settings, kms_schema, kms_master_key_arn, kms_app_user,
                                      kms_verifier)
        assert result.read == ["pt-real-rotation-old", "pt-real-rotation-new"]
        assert result.decoded == [(result.old_key_id, "pt-real-rotation-old"),
                                  (result.new_key_id, "pt-real-rotation-new")]
        # A connection opened before the rotation still writes with the old key, which the trigger rejects.
        assert TRIGGER_REJECTION in str(result.stale_error)
        # Without the plugin, an UPDATE that keeps the old row's payload passes, and one that copies it into the new
        # row is rejected; the decoded rows above show that the new row kept its own payload.
        assert result.touched_error is None
        assert TRIGGER_REJECTION in str(result.previous_error)

    @enable_on_features([TestEnvironmentFeatures.KMS_MANUAL_TESTS])
    @enable_on_engines([DatabaseEngine.PG])
    @kms_builds()
    def test_via_service_condition_raises_key_management_error(
            self, build: str, table: int, kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str,
            kms_app_user: DbUser) -> None:
        via_rds = _assume_decrypt_only(kms_settings, "kms-it-via-service", {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "kms:Decrypt", "Resource": "*",
             "Condition": {"StringEquals": {"kms:ViaService": f"rds.{kms_settings.region}.amazonaws.com"}}}]})
        name = f"via-service-{build}"
        with _plugin_credentials(via_rds):
            with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn) as conn:
                read_error = raised(conn, lambda: read_column(conn, _SELECT, _SEED[0]))
                write_error = raised(conn, lambda: write(conn, _INSERT, (name, "pt-via-service")))
        # With Decrypt allowed only through another service, every encrypted read and write fails closed.
        assert class_name(read_error) == "KeyManagementError"
        assert class_name(write_error) == "KeyManagementError"
        assert _count_rows(_TABLE, name) == 0

    @enable_on_features([TestEnvironmentFeatures.KMS_MANUAL_TESTS])
    @enable_on_engines([DatabaseEngine.PG])
    @kms_builds()
    def test_cloudtrail_records_decrypt_calls_without_values(self, build: str, kms_settings: KmsSettings,
                                                             kms_schema: str, kms_master_key_arn: str,
                                                             kms_app_user: DbUser) -> None:
        table_name, session_name = "kms_real_cloudtrail", f"kms-it-ct-{secrets.token_hex(4)}"
        values = [f"pt-cloudtrail-{i}-{secrets.token_hex(4)}" for i in range(2)]
        start = datetime.now(timezone.utc)
        utility_requests: List[str] = []
        create_table(table_name, f"name VARCHAR(100), ssn {binary_type()}")
        try:
            kms = kms_client(kms_settings)
            kms.meta.events.register("after-call.kms.GenerateDataKey",
                                     lambda parsed, **_: utility_requests.append(parsed["ResponseMetadata"]["RequestId"]))
            with plain_connect() as conn:
                utility = KeyManagementUtility(conn, kms, encryption_config(kms_settings, kms_schema))
                utility.initialize_encryption_for_column(table_name, "ssn", kms_master_key_arn)
                conn.commit()
            with _plugin_credentials(_assume_decrypt_only(kms_settings, session_name)):
                with wrapper_connect(build, kms_app_user, kms_settings, kms_schema, kms_master_key_arn,
                                     encryption_data_key_cache_enabled=False) as conn:
                    for value in values:
                        write(conn, f"INSERT INTO {table_name} (name, ssn) VALUES (%s, %s)", ("cloudtrail", value))
                    read = read_column(conn, f"SELECT ssn FROM {table_name} WHERE name = %s ORDER BY id", "cloudtrail")
        finally:
            drop_table(table_name)
        assert read == values

        cloudtrail = boto3.client("cloudtrail", region_name=kms_settings.region)
        events: Dict[str, Dict[str, Any]] = {}

        def plugin_events() -> List[Dict[str, Any]]:
            return [body for body in events.values() if body["userIdentity"].get("arn", "").endswith(session_name)]

        def delivered() -> bool:
            pages = cloudtrail.get_paginator("lookup_events").paginate(
                LookupAttributes=[{"AttributeKey": "ResourceName", "AttributeValue": kms_master_key_arn}],
                StartTime=start - timedelta(minutes=5), EndTime=datetime.now(timezone.utc) + timedelta(minutes=5))
            for page in pages:
                for event in page["Events"]:
                    body = json.loads(event["CloudTrailEvent"])
                    events[body["requestID"]] = body
            return set(utility_requests) <= set(events) and len(plugin_events()) >= len(values) + 1

        found = retry_until(delivered, timeout_sec=KMS_CLOUDTRAIL_DELIVERY_SEC, delay_sec=KMS_CLOUDTRAIL_POLL_SEC)
        plugin = plugin_events()
        ours = plugin + [events[request] for request in utility_requests if request in events]
        assert found is True
        assert len(utility_requests) > 0
        # The plugin's only KMS calls are Decrypt; the utility's setup is one GenerateDataKey.
        assert sorted({body["eventName"] for body in plugin}) == ["Decrypt"]
        assert [events[request]["eventName"] for request in utility_requests] == ["GenerateDataKey"]
        # Each event names the row's key ARN, carries only encryptionAlgorithm, keyId and keySpec, and holds no
        # response elements and no value.
        shapes = []
        for body in ours:
            shapes.append((body["requestParameters"].get("keyId") == kms_master_key_arn,
                           set(body["requestParameters"]) <= _ALLOWED_EVENT_PARAMETERS,
                           body.get("responseElements") is None, any(value in json.dumps(body) for value in values)))
        assert shapes == [(True, True, True, False)] * len(ours)

    @enable_on_features([TestEnvironmentFeatures.KMS_MANUAL_TESTS])
    @enable_on_engines([DatabaseEngine.PG])
    @kms_builds()
    def test_expired_credentials_refresh_or_fail_closed(
            self, build: str, table: int, tmp_path: Path, kms_settings: KmsSettings, kms_schema: str,
            kms_master_key_arn: str, kms_app_user: DbUser) -> None:
        role_arn = _decrypt_only_role_arn(kms_settings)
        # The refreshing source is botocore's assume-role provider, reading the base credentials from the environment;
        # the static one is a single AssumeRole result in the environment, which botocore never refreshes.
        config_file = tmp_path / "aws-config"
        config_file.write_text(f"[profile kms-it-refreshing]\nrole_arn = {role_arn}\n"
                               f"credential_source = Environment\nduration_seconds = {KMS_ROLE_SESSION_SEC}\n"
                               "role_session_name = kms-it-refreshing\n")
        credentials = boto3.Session(region_name=kms_settings.region).get_credentials()
        assert credentials is not None
        base = credentials.get_frozen_credentials()
        base_role = RoleSession(base.access_key or "", base.secret_key or "", base.token or "")
        static = _assume_decrypt_only(kms_settings, "kms-it-static")
        props = {"encryption_data_key_cache_enabled": False}
        connections = {}
        try:
            with _plugin_credentials(static):
                connections["static"] = wrapper_connect(build, kms_app_user, kms_settings, kms_schema,
                                                        kms_master_key_arn, **props)
                before = {"static": read_column(connections["static"], _SELECT, _SEED[0])}
            with _plugin_credentials(base_role), pytest.MonkeyPatch.context() as patch:
                patch.setenv("AWS_CONFIG_FILE", str(config_file))
                connections["refreshing"] = wrapper_connect(build, kms_app_user, kms_settings, kms_schema,
                                                            kms_master_key_arn, aws_profile="kms-it-refreshing",
                                                            **props)
                before["refreshing"] = read_column(connections["refreshing"], _SELECT, _SEED[0])
            assert before == {"static": [_SEED[1]], "refreshing": [_SEED[1]]}

            def static_expired() -> bool:
                error = _boto3_error(lambda: boto3.client(
                    "sts", region_name=kms_settings.region, aws_access_key_id=static.access_key_id,
                    aws_secret_access_key=static.secret_access_key,
                    aws_session_token=static.session_token).get_caller_identity())
                return isinstance(error, ClientError) and "Expired" in error.response["Error"]["Code"]

            assert retry_until(static_expired, timeout_sec=KMS_ROLE_SESSION_EXPIRY_WAIT_SEC,
                               delay_sec=KMS_ROLE_SESSION_EXPIRY_POLL_SEC) is True
            after = {}
            for kind, conn in connections.items():
                name = f"expiry-{kind}-{build}"
                write_error = raised(conn, lambda: write(conn, _INSERT, (name, f"pt-expiry-{kind}")))
                read_error = raised(conn, lambda: read_column(conn, _SELECT, _SEED[0]))
                after[kind] = (class_name(write_error), class_name(read_error), _count_rows(_TABLE, name))
            # After the expiry the refreshing source keeps KMS calls working, and the static one fails the next cold
            # KMS call closed, storing and returning nothing.
            assert after == {"refreshing": (None, None, 1),
                             "static": ("KeyManagementError", "KeyManagementError", 0)}
        finally:
            for conn in connections.values():
                conn.close()
