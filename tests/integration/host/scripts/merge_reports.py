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

import os
import platform
import sys

if __name__ == "__main__":
    if platform.system() != "Windows":
        print("Running chmod...")
        os.system("sudo chmod 777 ../container/reports")  # Give write permissions. Only needed on GH Actions
    sys.exit(os.system(
        "poetry run pytest_html_merger -i ../container/reports/ -o ../container/reports/integration_tests.html"))
