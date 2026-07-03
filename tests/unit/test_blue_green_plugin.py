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

from aws_advanced_python_wrapper.utils.messages import Messages


def test_blue_green_log_message_keys_resolve():
    """Regression (parity review): both Blue/Green keys referenced by logger
    calls exist in the bundle. The formatted
    WaitConnectUntilCorrespondingHostFound call previously raised
    NotInResourceBundleError whenever DEBUG logging was enabled during a
    switchover (Logger's formatted path has no missing-key guard)."""
    formatted = Messages.get_formatted(
        "SuspendConnectRouting.WaitConnectUntilCorrespondingHostFound", "host-1")
    assert "host-1" in formatted
    assert Messages.get("BlueGreenStatusProvider.AllGreenHostsChangedName")
