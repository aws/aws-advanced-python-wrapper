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

from tortoise.backends.base.config_generator import DB_LOOKUP

# Register AWS MySQL backend
DB_LOOKUP["aws-mysql"] = {
    "engine": "aws_advanced_python_wrapper.tortoise.backend.mysql",
    "vmap": {
            "path": "database",
            "hostname": "host",
            "port": "port",
            "username": "user",
            "password": "password",
        },
    "defaults": {"port": 3306, "charset": "utf8mb4", "sql_mode": "STRICT_TRANS_TABLES"},
    "cast": {
        "minsize": int,
        "maxsize": int,
        "connect_timeout": float,
        "echo": bool,
        "use_unicode": bool,
        "pool_recycle": int,
        "ssl": bool,
    },
}