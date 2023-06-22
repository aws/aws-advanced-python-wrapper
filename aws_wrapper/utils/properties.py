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

from typing import Dict, Union


class Properties(Dict[str, str]):
    ...


class WrapperProperty:
    def __init__(self, name: str, default_value: str, description: str):
        self.name = name
        self.default_value = default_value
        self.description = description

    def get(self, props: Properties) -> str:
        return props.get(self.name, self.default_value)

    def set(self, props: Properties, value: str):
        props[self.name] = value


class WrapperProperties:
    PLUGINS = WrapperProperty("plugins", "dummy", "Comma separated list of connection plugin codes")


class PropertiesUtils:

    @staticmethod
    def parse_properties(conn_info: str, **kwargs: Union[None, int, str]) -> Properties:
        props: Properties
        if conn_info == "":
            props = Properties()
        else:
            props = Properties(dict(x.split("=") for x in conn_info.split(" ")))
        for key, value in kwargs.items():
            props[key] = str(value)
        return props

    @staticmethod
    def remove_wrapper_props(props: Properties):
        for attr_name, attr_val in WrapperProperties.__dict__.items():
            if isinstance(attr_val, WrapperProperty):
                props.pop(attr_val.name, None)

    @staticmethod
    def log_properties(props: Properties, caption: str):
        if not props:
            return "<empty>"

        prefix = "" if not caption else caption
        return f"\n{prefix} {props}"
