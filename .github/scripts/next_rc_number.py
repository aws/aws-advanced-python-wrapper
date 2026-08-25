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

"""Print the next release-candidate number for a release version.

Usage: python next_rc_number.py 3.1.0

The counter is read back from TestPyPI rather than kept as CI-side state, so it
starts at 1 for a release version that has no candidates yet and increments on
every re-cut. This uses the standard simple-index JSON API.

Caveat: the index lists only the versions currently published, while the upload
API keeps every filename ever used permanently reserved. Deleting a candidate
release therefore frees its number here but not on the index, and the next
upload would be rejected. Leave published candidates in place.
"""

import json
import re
import sys
import urllib.error
import urllib.request

INDEX_URL = "https://test.pypi.org/simple/aws-advanced-python-wrapper/"
ACCEPT_HEADER = "application/vnd.pypi.simple.v1+json"
REQUEST_TIMEOUT_SEC = 30


def next_rc_number(release_version: str, uploaded_versions: list[str]) -> int:
    candidate_pattern = re.compile(re.escape(release_version) + r"rc(\d+)$")
    matches = [candidate_pattern.match(version) for version in uploaded_versions]
    used_numbers = [int(match.group(1)) for match in matches if match is not None]
    return max(used_numbers) + 1 if used_numbers else 1


def fetch_uploaded_versions() -> list[str]:
    request = urllib.request.Request(INDEX_URL, headers={"Accept": ACCEPT_HEADER})
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SEC) as response:
            return json.load(response).get("versions", [])
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        return []  # The project has never been uploaded to TestPyPI.


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} <release-version>")
    print(next_rc_number(sys.argv[1], fetch_uploaded_versions()))


if __name__ == "__main__":
    main()
