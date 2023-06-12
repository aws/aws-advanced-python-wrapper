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

from typing import Dict, List, Set, Union


class Properties(Dict[str, str]):
    ...


class PropertiesUtils:

    WRAPPER_PROPERTIES: Set[str] = {
        "plugins"
    }

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
    def remove_wrapper_conninfo(old_info: str) -> str:
        new_info: List[str] = []
        props: List[str] = old_info.split(" ")
        for prop in props:
            if prop.split("=")[0] not in PropertiesUtils.WRAPPER_PROPERTIES:
                new_info.append(prop)
        return " ".join(new_info)

    @staticmethod
    def remove_wrapper_kwargs(kwargs_dict: Dict[str, Union[None, int, str]]):
        for prop in PropertiesUtils.WRAPPER_PROPERTIES:
            kwargs_dict.pop(prop, None)
        return kwargs_dict

    @staticmethod
    def log_properties(props: Properties, caption: str):
        if not props:
            return "<empty>"

        prefix = "" if not caption else caption
        return f"\n{prefix} {props}"
