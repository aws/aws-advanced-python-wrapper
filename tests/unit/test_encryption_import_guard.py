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

"""``cryptography`` is an optional extra (``encryption``). The wrapper must
import without it, and only the module that encrypts may need it.

Each check runs in a fresh interpreter, most with ``cryptography`` blocked,
since the test environment always has it installed."""

import subprocess
import sys
import textwrap

import pytest

_BLOCKER = textwrap.dedent("""
    import sys

    class _Block:
        def find_spec(self, name, path=None, target=None):
            if name == "cryptography" or name.startswith("cryptography."):
                raise ModuleNotFoundError(f"No module named {name!r}", name=name)
            return None

    sys.meta_path.insert(0, _Block())
""")


def _run(code: str, block: bool = True) -> subprocess.CompletedProcess:
    script = (_BLOCKER if block else "import sys\n") + textwrap.dedent(code)
    return subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=120)


_CORE_MODULES = [
    "aws_advanced_python_wrapper",
    "aws_advanced_python_wrapper.aio",
    "aws_advanced_python_wrapper.psycopg",
    "aws_advanced_python_wrapper.mysql_connector",
    "aws_advanced_python_wrapper.sqlalchemy_dialects.pg",
    "aws_advanced_python_wrapper.sqlalchemy_dialects.mysql",
    "aws_advanced_python_wrapper.sqlalchemy_dialects.pg_async",
    "aws_advanced_python_wrapper.sqlalchemy_dialects.mysql_async",
    "aws_advanced_python_wrapper.django.backends.mysql_connector.base",
]


_KEY_LAYER_MODULES = [
    # The key and metadata layer, including the admin utility, does no
    # encryption itself.
    "aws_advanced_python_wrapper.encryption.config",
    "aws_advanced_python_wrapper.encryption.data_key_cache",
    "aws_advanced_python_wrapper.encryption.key_manager",
    "aws_advanced_python_wrapper.encryption.metadata_manager",
    "aws_advanced_python_wrapper.encryption.key_management_utility",
]


@pytest.mark.parametrize("module", _CORE_MODULES + _KEY_LAYER_MODULES)
def test_imports_without_cryptography(module):
    result = _run(f"""
        import importlib
        importlib.import_module({module!r})
    """)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("module", _CORE_MODULES + _KEY_LAYER_MODULES)
def test_does_not_load_cryptography_even_when_installed(module):
    # With cryptography available, importing these must still not load it,
    # so users who never encrypt don't pay its import cost.
    result = _run(f"""
        import importlib
        importlib.import_module({module!r})
        assert "cryptography" not in sys.modules, "cryptography was imported"
    """, block=False)
    assert result.returncode == 0, result.stderr


def test_encryption_service_names_the_extra_when_cryptography_is_missing():
    result = _run("""
        try:
            import aws_advanced_python_wrapper.encryption.encryption_service
        except ImportError as e:
            print(e)
        else:
            raise SystemExit("imported without cryptography")
    """)
    assert result.returncode == 0, result.stderr
    assert "aws-advanced-python-wrapper[encryption]" in result.stdout


_BROKEN = textwrap.dedent("""
    import sys

    class _Broken:
        def find_spec(self, name, path=None, target=None):
            if name == "cryptography":
                raise {error}
            return None

    sys.meta_path.insert(0, _Broken())
""")


@pytest.mark.parametrize("error", [
    'ImportError("DLL load failed while importing _rust")',
    'ModuleNotFoundError("No module named \'_cffi_backend\'", name="_cffi_backend")',
], ids=["ImportError", "other-module-missing"])
def test_a_broken_cryptography_is_not_reported_as_missing(error):
    # Only a missing cryptography package gets the install hint; an installed
    # but broken one surfaces its own error.
    code = _BROKEN.format(error=error) + textwrap.dedent("""
        try:
            import aws_advanced_python_wrapper.encryption.encryption_service
        except ImportError as e:
            print(e)
    """)
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    assert "aws-advanced-python-wrapper[encryption]" not in result.stdout
    assert ("DLL load failed" in result.stdout) or ("_cffi_backend" in result.stdout)
