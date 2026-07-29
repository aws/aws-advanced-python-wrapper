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

"""Async-plugin stub registry: currently empty.

Every plugin code that previously registered as a pass-through stub has
been replaced by a real async port (most recently ``bg`` ->
:class:`AsyncBlueGreenPlugin`). This file kept as a regression guard:
if a new stub is introduced, extend :data:`STUB_CODES_AND_CLASSES` and
the existing pattern resumes working.
"""

from __future__ import annotations

from aws_advanced_python_wrapper.aio.stub_plugins import _AsyncStubPlugin


def test_stub_scaffolding_exists():
    """Keep the stub base class importable for future use."""
    assert _AsyncStubPlugin is not None
