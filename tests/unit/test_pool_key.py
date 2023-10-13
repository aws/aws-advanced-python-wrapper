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

from aws_wrapper.sql_alchemy_connection_provider import PoolKey


def test_eq():
    key = PoolKey("url", "some_extra_key")
    matching_key = PoolKey("url", "some_extra_key")
    url_mismatch = PoolKey("url2", "some_extra_key")
    extra_key_mismatch = PoolKey("url", "some_other_extra_key")

    assert key == key
    assert key == matching_key
    assert key != url_mismatch
    assert key != extra_key_mismatch
    assert key != "some_string"
