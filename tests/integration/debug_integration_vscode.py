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

<<<<<<<< HEAD:aws_wrapper/target_driver_dialect_codes.py
class TargetDriverDialectCodes:
    PSYCOPG = "psycopg"
    MYSQL_CONNECTOR_PYTHON = "mysql-connector-python"
    MARIADB_CONNECTOR_PYTHON = "mariadb-connector-python"
    GENERIC = "generic"
========
import pytest
import sys

if __name__ == "__main__":
    test_filter = sys.argv[1]
    report_setting = sys.argv[2]
    sys.exit(pytest.main([report_setting, "-k", test_filter, "-p", "no:logging", "--capture=tee-sys", "./tests/integration"]))
>>>>>>>> 2f524da (Enable integration test debugging):tests/integration/debug_integration_vscode.py
