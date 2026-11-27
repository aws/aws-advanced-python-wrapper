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

"""Django is configured once per process, by the helper's ``kms_django_settings`` when nothing configured it yet, with
``default`` set up as ``test_django_basic.py`` does; the plugin runs only in a ``kms`` alias copied from ``default``,
which is never edited.
"""

from __future__ import annotations

from typing import Any, Iterator

import django.db
import pytest
from django.conf import settings
from django.db import connections, models

from aws_advanced_python_wrapper.errors import (IntegrityCheckError,
                                                MetadataError)
from ..utils.conditions import enable_on_engines, enable_on_features
from ..utils.database_engine import DatabaseEngine
from ..utils.kms_encryption_helper import (DbUser, KmsEncryptionTestBase,
                                           KmsSettings, PayloadVerifier,
                                           encrypted_table, kms_builds,
                                           plain_connect)
from ..utils.test_environment_features import TestEnvironmentFeatures

_TABLE = "kms_dj_users"
_ALIAS = "kms"


@enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY, TestEnvironmentFeatures.KMS_LOCALSTACK])
@enable_on_engines([DatabaseEngine.MYSQL])
class TestDjangoKmsEncryption(KmsEncryptionTestBase):

    @pytest.fixture(scope="class")
    def user_model(self, kms_django_settings: None, kms_settings: KmsSettings, kms_schema: str,
                   kms_master_key_arn: str) -> Iterator[Any]:
        """A model on ``kms_dj_users``, whose ``ssn`` and ``data`` columns are encrypted (without a trigger, so a test
        can tamper with a stored value)."""
        class KmsUser(models.Model):
            name: Any = models.CharField(max_length=100)
            ssn: Any = models.CharField(max_length=100, null=True)
            data: Any = models.BinaryField(null=True)

            class Meta:
                app_label = "kms_encryption_tests"
                db_table = _TABLE
                managed = False

        with encrypted_table(_TABLE, "name VARCHAR(100), ssn VARBINARY(1024), data VARBINARY(1024)", ("ssn", "data"),
                             kms_settings, kms_schema, kms_master_key_arn):
            yield KmsUser

    @pytest.fixture
    def kms_alias(self, build: str, user_model: Any, kms_app_user: DbUser, kms_settings: KmsSettings,
                  kms_schema: str, kms_master_key_arn: str) -> Iterator[str]:
        """The ``kms`` alias: ``default``'s settings with the application user and the plugin in OPTIONS."""
        alias = connections["default"].settings_dict.copy()
        alias.update(USER=kms_app_user.name, PASSWORD=kms_app_user.password,
                     OPTIONS={**alias["OPTIONS"], "plugins": "kms_encryption", "encryption_metadata_schema": kms_schema,
                              "encryption_allowed_master_key_arns": kms_master_key_arn, **kms_settings.props(),
                              "use_pure": build == "mysql-pure"})
        connections.databases[_ALIAS] = alias
        try:
            yield _ALIAS
        finally:
            connections[_ALIAS].close()
            del connections[_ALIAS]
            del connections.databases[_ALIAS]
            with plain_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {_TABLE}")
                conn.commit()

    @pytest.fixture(params=[False, True], ids=["debug-off", "debug-on"])
    def debug(self, request: pytest.FixtureRequest) -> Iterator[bool]:
        saved = settings.DEBUG
        settings.DEBUG = request.param
        try:
            yield request.param
        finally:
            settings.DEBUG = saved

    @kms_builds()
    def test_django_orm_round_trip(self, build: str, debug: bool, user_model: Any, kms_alias: str,
                                   kms_verifier: PayloadVerifier) -> None:
        users = user_model.objects.using(kms_alias)
        created = users.create(name="one", ssn="pt-dj-1")
        users.bulk_create([user_model(name="two", ssn="pt-dj-2"), user_model(name="three", ssn="pt-dj-3")])
        created.ssn = "pt-dj-1-saved"
        created.save(using=kms_alias)
        users.filter(name="two").update(ssn="pt-dj-2-updated")
        expected = ["pt-dj-1-saved", "pt-dj-2-updated", "pt-dj-3"]
        assert users.get(pk=created.pk).ssn == "pt-dj-1-saved"
        assert list(users.order_by("id").values_list("ssn", flat=True)) == expected
        assert [user.ssn for user in users.order_by("id").iterator(chunk_size=1)] == expected
        assert [user.ssn for user in users.raw(f"SELECT id, name, ssn FROM {_TABLE} ORDER BY id")] == expected
        with connections[kms_alias].cursor() as cur:
            cur.execute(f"SELECT ssn FROM {_TABLE} ORDER BY id")
            assert [row[0] for row in cur.fetchall()] == expected
        stored = kms_verifier.stored(f"SELECT ssn FROM {_TABLE} ORDER BY id")
        assert [kms_verifier.decode(_TABLE, "ssn", payload)[1] for payload in stored] == expected

        binary = users.create(name="binary", data=b"pt-dj-binary")
        if debug:
            # Under DEBUG, the BinaryField's value is recorded with one _binary, Django's, and none added twice.
            recorded = connections[kms_alias].queries[-1]["sql"]
            assert recorded.startswith("INSERT")
            assert recorded.count("_binary") == 1
        assert bytes(users.get(pk=binary.pk).data) == b"pt-dj-binary"

        # An annotated placeholder through Django's cursor.
        with connections[kms_alias].cursor() as cur:
            cur.execute(f"INSERT INTO {_TABLE} (name, ssn) VALUES (%s, /*@encrypt:{_TABLE}.ssn*/ %s)",
                        ("annotated", "pt-dj-annotated"))
        (annotated,) = kms_verifier.stored(f"SELECT ssn FROM {_TABLE} WHERE name = 'annotated'")
        assert kms_verifier.decode(_TABLE, "ssn", annotated)[1] == "pt-dj-annotated"

    @kms_builds()
    def test_django_bulk_update_refused_and_errors_untranslated(self, build: str, user_model: Any, kms_alias: str,
                                                                kms_verifier: PayloadVerifier) -> None:
        users = user_model.objects.using(kms_alias)
        created = users.create(name="bulk", ssn="pt-dj-bulk")
        created.ssn = "pt-dj-bulk-new"
        with pytest.raises(MetadataError) as refused:
            users.bulk_update([created], ["ssn"])
        # Django doesn't translate the plugin's refusal.
        assert not isinstance(refused.value, django.db.DatabaseError)
        assert users.get(pk=created.pk).ssn == "pt-dj-bulk"

        with plain_connect(admin=True) as admin, admin.cursor() as cur:
            cur.execute(f"SELECT ssn FROM {_TABLE} WHERE id = %s", (created.pk,))
            payload = bytes(cur.fetchone()[0])
            cur.execute(f"UPDATE {_TABLE} SET ssn = %s WHERE id = %s",
                        (payload[:-1] + bytes([payload[-1] ^ 1]), created.pk))
            admin.commit()
        with pytest.raises(IntegrityCheckError) as tampered:
            users.get(pk=created.pk)
        assert not isinstance(tampered.value, django.db.DatabaseError)
