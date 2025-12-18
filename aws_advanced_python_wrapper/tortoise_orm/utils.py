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
import importlib.util
import sys
from pathlib import Path
from typing import Type


def load_mysql_module(module_file: str, class_name: str) -> Type:
    """Load MySQL backend module without __init__.py."""
    import tortoise
    tortoise_path = Path(tortoise.__file__).parent
    module_path = tortoise_path / "backends" / "mysql" / module_file
    module_name = f"tortoise.backends.mysql.{module_file[:-3]}"

    if module_name not in sys.modules:
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load module {module_name}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

    return getattr(sys.modules[module_name], class_name)
