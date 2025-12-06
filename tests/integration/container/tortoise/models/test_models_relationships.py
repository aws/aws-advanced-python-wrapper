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

from typing import TYPE_CHECKING

from tortoise import fields
from tortoise.models import Model

if TYPE_CHECKING:
    from tortoise.fields.relational import (ForeignKeyFieldInstance,
                                            OneToOneFieldInstance)

# One-to-One Relationship Models


class RelTestAccount(Model):
    id = fields.IntField(primary_key=True)
    username = fields.CharField(max_length=50, unique=True)
    email = fields.CharField(max_length=100)

    class Meta:
        table = "rel_test_accounts"


class RelTestAccountProfile(Model):
    id = fields.IntField(primary_key=True)
    account: "OneToOneFieldInstance" = fields.OneToOneField("models.RelTestAccount", related_name="profile", on_delete=fields.CASCADE)
    bio = fields.TextField(null=True)
    avatar_url = fields.CharField(max_length=200, null=True)

    class Meta:
        table = "rel_test_account_profiles"


# One-to-Many Relationship Models
class RelTestPublisher(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=100)
    email = fields.CharField(max_length=100, unique=True)

    class Meta:
        table = "rel_test_publishers"


class RelTestPublication(Model):
    id = fields.IntField(primary_key=True)
    title = fields.CharField(max_length=200)
    isbn = fields.CharField(max_length=13, unique=True)
    publisher: "ForeignKeyFieldInstance" = fields.ForeignKeyField("models.RelTestPublisher", related_name="publications", on_delete=fields.CASCADE)
    published_date = fields.DateField(null=True)

    class Meta:
        table = "rel_test_publications"


# Many-to-Many Relationship Models
class RelTestLearner(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=100)
    learner_id = fields.CharField(max_length=20, unique=True)

    class Meta:
        table = "rel_test_learners"


class RelTestSubject(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=100)
    code = fields.CharField(max_length=10, unique=True)
    credits = fields.IntField()
    learners = fields.ManyToManyField("models.RelTestLearner", related_name="subjects")

    class Meta:
        table = "rel_test_subjects"
