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

from tortoise import fields
from tortoise.models import Model


class User(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=50)
    email = fields.CharField(max_length=100, unique=True)
    
    class Meta:
        table = "users"


class UniqueName(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=20, null=True, unique=True)
    optional = fields.CharField(max_length=20, null=True)
    other_optional = fields.CharField(max_length=20, null=True)
    
    class Meta:
        table = "unique_names"


class TableWithSleepTrigger(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=50)
    value = fields.CharField(max_length=100)
    
    class Meta:
        table = "table_with_sleep_trigger"
