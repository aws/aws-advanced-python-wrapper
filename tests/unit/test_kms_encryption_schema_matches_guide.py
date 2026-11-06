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

"""The KMS integration tests install the metadata DDL from ``kms_encryption_schema.py``, which must stay equal to the
two blocks in the KMS Encryption Plugin guide that administrators run."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List

import pytest

if TYPE_CHECKING:
    from types import ModuleType

_ROOT = Path(__file__).resolve().parents[2]
_GUIDE = _ROOT / "docs" / "using-the-python-wrapper" / "using-plugins" / "UsingTheKmsEncryptionPlugin.md"
_SCHEMA = _ROOT / "tests" / "integration" / "container" / "utils" / "kms_encryption_schema.py"


def _statement_lines(sql: str) -> List[str]:
    return [line for line in sql.splitlines() if line.strip() and not line.lstrip().startswith("--")]


@pytest.fixture(scope="module")
def schema() -> ModuleType:
    # Loaded from its file, because importing the tests.integration.container package sets up the wrapper's loggers.
    spec = importlib.util.spec_from_file_location("kms_encryption_schema", _SCHEMA)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def guide_ddl() -> Dict[str, List[str]]:
    """The guide's metadata DDL blocks, keyed by the engine their first comment line names."""
    blocks = re.findall(r"^```sql\n(.*?)^```", _GUIDE.read_text(encoding="utf-8"), flags=re.MULTILINE | re.DOTALL)
    return {block.splitlines()[0].removeprefix("--").strip(): _statement_lines(block)
            for block in blocks if "CREATE TABLE encrypt.key_storage" in block}


def test_guide_has_one_metadata_ddl_block_per_engine(guide_ddl):
    assert sorted(guide_ddl) == ["MySQL", "PostgreSQL"], \
        "The guide should have one metadata DDL block for each engine, each starting with a comment naming it."


def test_postgresql_metadata_ddl_matches_the_guide(guide_ddl, schema):
    assert _statement_lines(schema.PG_METADATA_DDL) == guide_ddl["PostgreSQL"], \
        "PG_METADATA_DDL in kms_encryption_schema.py differs from the guide's PostgreSQL block."


def test_mysql_metadata_ddl_matches_the_guide(guide_ddl, schema):
    assert _statement_lines(schema.MYSQL_METADATA_DDL) == guide_ddl["MySQL"], \
        "MYSQL_METADATA_DDL in kms_encryption_schema.py differs from the guide's MySQL block."
