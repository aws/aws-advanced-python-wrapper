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

import pytest
from tortoise import Tortoise, connections

# Import to register the aws-mysql backend
import aws_advanced_python_wrapper.tortoise  # noqa: F401
from tests.integration.container.tortoise.models.test_models import User
from tests.integration.container.utils.test_environment import TestEnvironment


async def clear_test_models():
    """Clear all test models by calling .all().delete() on each."""
    from tortoise.models import Model

    import tests.integration.container.tortoise.models.test_models as models_module

    for attr_name in dir(models_module):
        attr = getattr(models_module, attr_name)
        if isinstance(attr, type) and issubclass(attr, Model) and attr != Model:
            await attr.all().delete()


async def setup_tortoise(conn_utils, plugins="aurora_connection_tracker", **kwargs):
    """Setup Tortoise with AWS MySQL backend and configurable plugins."""
    db_url = conn_utils.get_aws_tortoise_url(
        TestEnvironment.get_current().get_engine(),
        plugins=plugins,
        **kwargs,
    )
    config = {
        "connections": {
            "default": db_url
        },
        "apps": {
            "models": {
                "models": ["tests.integration.container.tortoise.models.test_models"],
                "default_connection": "default",
            }
        }
    }

    await Tortoise.init(config=config)

    await Tortoise.generate_schemas()
    await clear_test_models()

    yield

    await reset_tortoise()


async def run_basic_read_operations(name_prefix="Test", email_prefix="test"):
    """Common test logic for basic read operations."""
    user = await User.create(name=f"{name_prefix} User", email=f"{email_prefix}@example.com")

    found_user = await User.get(id=user.id)
    assert found_user.name == f"{name_prefix} User"
    assert found_user.email == f"{email_prefix}@example.com"

    users = await User.filter(name=f"{name_prefix} User")
    assert len(users) == 1
    assert users[0].id == user.id


async def run_basic_write_operations(name_prefix="Write", email_prefix="write"):
    """Common test logic for basic write operations."""
    user = await User.create(name=f"{name_prefix} Test", email=f"{email_prefix}@example.com")
    assert user.id is not None

    user.name = f"Updated {name_prefix}"
    await user.save()

    updated_user = await User.get(id=user.id)
    assert updated_user.name == f"Updated {name_prefix}"

    await updated_user.delete()

    with pytest.raises(Exception):
        await User.get(id=user.id)


async def reset_tortoise():
    await Tortoise.close_connections()
    await Tortoise._reset_apps()
    Tortoise._inited = False
    Tortoise.apps = {}
    connections._db_config = {}
