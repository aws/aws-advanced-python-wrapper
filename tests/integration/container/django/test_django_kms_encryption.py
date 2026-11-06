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

"""The KMS Encryption Plugin through the wrapper's Django MySQL backend on a live server and KMS.

Django is configured once per process, by the helper's ``kms_django_settings`` when nothing configured it yet, with
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
from ..utils.kms_encryption_helper import (DbUser, KmsSettings,
                                           PayloadVerifier, encrypted_table,
                                           kms_builds, plain_connect)
from ..utils.test_environment_features import TestEnvironmentFeatures

pytestmark = [pytest.mark.kms_encryption,
              enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY,
                                  TestEnvironmentFeatures.KMS_LOCALSTACK]),
              enable_on_engines([DatabaseEngine.MYSQL])]

_TABLE = "kms_dj_users"


@pytest.fixture(scope="module")
def user_model(kms_django_settings: None, kms_settings: KmsSettings, kms_schema: str,
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
def kms_alias(build: str, user_model: Any, kms_app_user: DbUser, kms_settings: KmsSettings, kms_schema: str,
              kms_master_key_arn: str) -> Iterator[str]:
    """The ``kms`` alias: ``default``'s settings with the application user and the plugin in OPTIONS."""
    alias = connections["default"].settings_dict.copy()
    alias.update(USER=kms_app_user.name, PASSWORD=kms_app_user.password,
                 OPTIONS={**alias["OPTIONS"], "plugins": "kms_encryption", "encryption_metadata_schema": kms_schema,
                          "encryption_allowed_master_key_arns": kms_master_key_arn, **kms_settings.props(),
                          "use_pure": build == "mysql-pure"})
    connections.settings["kms"] = alias
    try:
        yield "kms"
    finally:
        connections["kms"].close()
        del connections["kms"]
        del connections.settings["kms"]
        with plain_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {_TABLE}")
            conn.commit()


@pytest.fixture(params=[False, True], ids=["debug-off", "debug-on"])
def debug(request: pytest.FixtureRequest) -> Iterator[bool]:
    saved = settings.DEBUG
    settings.DEBUG = request.param
    try:
        yield request.param
    finally:
        settings.DEBUG = saved


@kms_builds()
def test_django_orm_round_trip(build: str, debug: bool, user_model: Any, kms_alias: str,
                               kms_verifier: PayloadVerifier) -> None:
    users = user_model.objects.using(kms_alias)
    created = users.create(name="one", ssn="pt-dj-1")
    users.bulk_create([user_model(name="two", ssn="pt-dj-2"), user_model(name="three", ssn="pt-dj-3")])
    created.ssn = "pt-dj-1-saved"
    created.save(using=kms_alias)
    users.filter(name="two").update(ssn="pt-dj-2-updated")
    assert users.get(pk=created.pk).ssn == "pt-dj-1-saved", "save() and get() round-trip"
    assert list(users.order_by("id").values_list("ssn", flat=True)) == [
        "pt-dj-1-saved", "pt-dj-2-updated", "pt-dj-3"], "bulk_create(), save() and update() write every value"
    assert [u.ssn for u in users.order_by("id").iterator(chunk_size=1)] == [
        "pt-dj-1-saved", "pt-dj-2-updated", "pt-dj-3"], "iterator() decrypts each chunk"
    assert [u.ssn for u in users.raw(f"SELECT id, name, ssn FROM {_TABLE} ORDER BY id")] == [
        "pt-dj-1-saved", "pt-dj-2-updated", "pt-dj-3"], "raw() decrypts"
    with connections[kms_alias].cursor() as cur:
        cur.execute(f"SELECT ssn FROM {_TABLE} ORDER BY id")
        assert [row[0] for row in cur.fetchall()] == ["pt-dj-1-saved", "pt-dj-2-updated", "pt-dj-3"], \
            "connection.cursor() decrypts"
    stored = kms_verifier.stored(f"SELECT ssn FROM {_TABLE} ORDER BY id")
    assert [kms_verifier.decode(_TABLE, "ssn", payload)[1] for payload in stored] == [
        "pt-dj-1-saved", "pt-dj-2-updated", "pt-dj-3"], "every value Django wrote is stored encrypted"

    binary = users.create(name="binary", data=b"pt-dj-binary")
    if debug:
        recorded = connections[kms_alias].queries[-1]["sql"]
        assert recorded.startswith("INSERT") and recorded.count("_binary") == 1, \
            "under DEBUG, the BinaryField's value is recorded with one _binary, Django's, and none added twice"
    assert bytes(users.get(pk=binary.pk).data) == b"pt-dj-binary", "a BinaryField round-trips"


@kms_builds()
def test_django_bulk_update_is_refused_and_integrity_errors_are_not_translated(
        build: str, user_model: Any, kms_alias: str, kms_verifier: PayloadVerifier) -> None:
    users = user_model.objects.using(kms_alias)
    created = users.create(name="bulk", ssn="pt-dj-bulk")
    created.ssn = "pt-dj-bulk-new"
    with pytest.raises(MetadataError) as refused:
        users.bulk_update([created], ["ssn"])
    assert not isinstance(refused.value, django.db.DatabaseError), "Django doesn't translate the plugin's refusal"
    assert users.get(pk=created.pk).ssn == "pt-dj-bulk", "bulk_update() on an encrypted column is refused"

    with plain_connect(admin=True) as admin, admin.cursor() as cur:
        cur.execute(f"SELECT ssn FROM {_TABLE} WHERE id = %s", (created.pk,))
        payload = bytes(cur.fetchone()[0])
        cur.execute(f"UPDATE {_TABLE} SET ssn = %s WHERE id = %s", (payload[:-1] + bytes([payload[-1] ^ 1]), created.pk))
        admin.commit()
    with pytest.raises(IntegrityCheckError) as tampered:
        users.get(pk=created.pk)
    assert not isinstance(tampered.value, django.db.DatabaseError), "Django doesn't translate IntegrityCheckError"
