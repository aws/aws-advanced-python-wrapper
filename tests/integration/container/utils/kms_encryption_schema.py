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

"""The metadata DDL of the KMS Encryption Plugin, as the plugin's guide gives it, for the KMS container modules.

Standard library only.
"""

import re

PG_METADATA_DDL = """
CREATE SCHEMA IF NOT EXISTS encrypt;

CREATE TABLE encrypt.key_storage (
    id SERIAL PRIMARY KEY,
    key_id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    master_key_arn VARCHAR(512) NOT NULL,
    encrypted_data_key TEXT NOT NULL,
    hmac_key BYTEA NOT NULL,
    key_spec VARCHAR(50) DEFAULT 'AES_256',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE encrypt.encryption_metadata (
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    encryption_algorithm VARCHAR(50) NOT NULL,
    key_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, column_name),
    FOREIGN KEY (key_id) REFERENCES encrypt.key_storage(id)
);
"""

MYSQL_METADATA_DDL = """
CREATE SCHEMA IF NOT EXISTS encrypt;

CREATE TABLE encrypt.key_storage (
    id INT AUTO_INCREMENT PRIMARY KEY,
    key_id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    master_key_arn VARCHAR(512) NOT NULL,
    encrypted_data_key TEXT NOT NULL,
    hmac_key VARBINARY(32) NOT NULL,
    key_spec VARCHAR(50) DEFAULT 'AES_256',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE encrypt.encryption_metadata (
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    encryption_algorithm VARCHAR(50) NOT NULL,
    key_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (table_name, column_name),
    FOREIGN KEY (key_id) REFERENCES encrypt.key_storage(id)
);
"""


def ddl_for(engine: str, schema: str) -> str:
    """The metadata DDL for ``engine`` (``"PG"`` or ``"MYSQL"``) with ``encrypt`` replaced by ``schema``.

    Only a name followed by ``.`` or ``;`` is replaced, so the ``encryption_metadata`` table keeps its name.
    """
    ddl = PG_METADATA_DDL if engine == "PG" else MYSQL_METADATA_DDL
    return re.sub(r"\bencrypt(?=[.;])", schema, ddl)
