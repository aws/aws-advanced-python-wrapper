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

from .database_engine import DatabaseEngine


def get_sleep_sql(db_engine: DatabaseEngine, seconds: int):
    if db_engine == DatabaseEngine.MYSQL:
        return f"SELECT SLEEP({seconds});"
    elif db_engine == DatabaseEngine.PG:
        return f"SELECT PG_SLEEP({seconds});"
    else:
        raise ValueError("Unknown database engine: " + str(db_engine))


def get_sleep_trigger_sql(db_engine: DatabaseEngine, duration: int, table_name: str) -> str:
    """Generate SQL to create a sleep trigger for INSERT operations."""
    if db_engine == DatabaseEngine.MYSQL:
        return f"""
        CREATE TRIGGER {table_name}_sleep_trigger
        BEFORE INSERT ON {table_name}
        FOR EACH ROW
        BEGIN
            DO SLEEP({duration});
        END
        """
    elif db_engine == DatabaseEngine.PG:
        return f"""
        CREATE OR REPLACE FUNCTION {table_name}_sleep_function()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_sleep({duration});
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER {table_name}_sleep_trigger
        BEFORE INSERT ON {table_name}
        FOR EACH ROW
        EXECUTE FUNCTION {table_name}_sleep_function();
        """
    else:
        raise ValueError(f"Unknown database engine: {db_engine}")
