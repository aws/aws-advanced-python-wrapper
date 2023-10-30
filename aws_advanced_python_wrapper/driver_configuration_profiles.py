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

from typing import TYPE_CHECKING, Dict, List

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.plugin import PluginFactory


class DriverConfigurationProfiles:
    _profiles: Dict[str, List[PluginFactory]] = {}

    @classmethod
    def clear_profiles(cls):
        cls._profiles.clear()

    @classmethod
    def add_or_replace_profile(cls, profile_name: str, factories: List[PluginFactory]):
        cls._profiles[profile_name] = factories

    @classmethod
    def remove_profile(cls, profile_name: str):
        cls._profiles.pop(profile_name)

    @classmethod
    def contains_profile(cls, profile_name: str):
        return profile_name in cls._profiles

    @classmethod
    def get_plugin_factories(cls, profile_name: str):
        return cls._profiles[profile_name]
