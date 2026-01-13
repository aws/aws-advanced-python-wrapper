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

import mysql.connector.django.base as base
import mysql.connector
from aws_advanced_python_wrapper import AwsWrapperConnection
from django.conf import settings
from django.utils.asyncio import async_unsafe
from django.core.exceptions import ImproperlyConfigured
from django.utils.functional import cached_property
from django.utils.regex_helper import _lazy_re_compile

class CursorWrapper(base.CursorWrapper):
    """Custom Cursor Wrapper for MySQL Connector backend"""
    pass

# This should match the numerical portion of the version numbers (we can treat
# versions like 5.0.24 and 5.0.24a as the same).
server_version_re = _lazy_re_compile(r"(\d{1,2})\.(\d{1,2})\.(\d{1,2})")

class DatabaseWrapper(base.DatabaseWrapper):
    """Custom MySQL Connector backend for Django"""

    @async_unsafe
    def get_new_connection(self, conn_params):
        if "converter_class" not in conn_params:
            conn_params["converter_class"] = base.DjangoMySQLConverter
        print("THIS IS MY CONNECT PARAMS:", conn_params)
        return AwsWrapperConnection.connect(
            mysql.connector.Connect,
            **conn_params
        )
    
    def get_connection_params(self):
        kwargs = super().get_connection_params()
        
        options = self.settings_dict.get("OPTIONS", {}).copy()
        
        # These options are handled by the superclass
        options.pop("isolation_level", None) 
        options.pop("init_command", None)
        options.pop("client_flags", None)
        options.pop("raise_on_warnings", None)

        kwargs.update(options)
        return kwargs

    @cached_property
    def mysql_server_info(self):
        return self.mysql_server_data["version"]

    @cached_property
    def mysql_version(self):
        match = server_version_re.match(self.mysql_server_info)
        if not match:
            raise Exception(
                "Unable to determine MySQL version from version string %r"
                % self.mysql_server_info
            )
        return tuple(int(x) for x in match.groups())

    @cached_property
    def mysql_is_mariadb(self):
        return "mariadb" in self.mysql_server_info.lower()

    @cached_property
    def sql_mode(self):
        sql_mode = self.mysql_server_data["sql_mode"]
        return set(sql_mode.split(",") if sql_mode else ())