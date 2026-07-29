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

"""Smoke test guarding against the ``resourcebundle`` 2.2.0+ regression.

History: ``resourcebundle`` 2.2.0 added an invalid ``KeysView[str, str]``
type annotation (``typing.KeysView`` takes only one type parameter).
Python 3.13's stricter typing module raises ``TypeError`` at import time:

    TypeError: Too many arguments for typing.KeysView; actual 2, expected 1

This broke all integration tests at conftest import on Python 3.13 and 3.14.
Was bumped twice in this project (the second time as part of a wider
patch-bump batch) and reverted both times. Pinning ``resourcebundle = "2.1.0"``
exact in ``pyproject.toml`` prevents dependabot from re-bumping; this test
is the belt-and-braces guard if someone manually bumps to 2.2.x+ without
verifying the typing fix has landed upstream.

If this test fails after a bump, check whether the upstream fix is in the
new release; if not, revert the bump.
"""

from __future__ import annotations


def test_messages_module_imports_under_current_python() -> None:
    """The wrapper's messages module is the first thing that loads
    ``resourcebundle``. If 2.2.0+ regressions return, this import raises
    ``TypeError`` at module-load time and CI fails before any integration
    suite runs.
    """
    # Importing under the test_ function (not at module top) so the import
    # is exercised under pytest's full env, not just collection.
    from aws_advanced_python_wrapper.utils import messages  # noqa: F401


def test_log_module_imports_under_current_python() -> None:
    """Second resourcebundle consumer in the wrapper. Covers the case
    where ``messages.py`` is reordered to lazy-load resourcebundle but
    ``log.py`` still eagerly imports it.
    """
    from aws_advanced_python_wrapper.utils import log  # noqa: F401
