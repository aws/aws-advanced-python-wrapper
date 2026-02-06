# aws_advanced_python_wrapper/sqlalchemy/sqlalchemy_psycopg_dialect.py
from psycopg import Connection
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg
import re

from aws_advanced_python_wrapper import AwsWrapperConnection


class SqlAlchemyOrmPgDialect(PGDialect_psycopg):
    """
    SQLAlchemy dialect for AWS Advanced Python Wrapper with psycopg. Extends the SQLAlchemy PostgreSQL psycopg dialect.
    This dialect is not related to the DriverDialect or DatabaseDialect classes used by our driver. Instead, it is used
    directly by SQLAlchemy. This dialect is registered in pyproject.toml and is selected by prefixing the connection
    string passed to create_engine with "postgresql+aws_wrapper://" ("[name]+[driver]").
    """

    name = 'postgresql'
    driver = 'aws_wrapper'

    def __init__(self, **kwargs):
        # PGDialect_psycopg's __init__ function checks the driver version and raises an exception if it is lower than
        # 3.0.2. If we call it, the exception is raised because it mistakenly interprets our driver version as its own.
        # As a workaround we call the grandparent __init__ instead of the parent's __init__.
        # TODO: since we are calling the grandparent's __init__ instead of the parent's __init__, we should investigate
        #  whether any important code in the parent's __init__ needs to be executed.
        super(PGDialect_psycopg, self).__init__(**kwargs)

        # Dynamically detect the actual psycopg version installed and set it as self.psycopg_version. Note that setting
        # this field before calling super().__init__ does not avoid the issue noted above.
        try:
            import psycopg
            m = re.match(r"(\d+)\.(\d+)(?:\.(\d+))?", psycopg.__version__)
            if m:
                self.psycopg_version = tuple(
                    int(x) for x in m.group(1, 2, 3) if x is not None
                )
            else:
                # Fallback to 3.0.2 if version parsing fails, which is the minimum required psycopg version.
                self.psycopg_version = (3, 0, 2)
        except (ImportError, AttributeError):
            # Fallback to 3.0.2 if version parsing fails, which is the minimum required psycopg version.
            self.psycopg_version = (3, 0, 2)

    @classmethod
    def import_dbapi(cls):
        """
        Return the DB-API 2.0 module.
        SQLAlchemy calls this to get the driver module.
        """
        import aws_advanced_python_wrapper
        return aws_advanced_python_wrapper

    def create_connect_args(self, url):
        """
        Transform SQLAlchemy URL into connection arguments.
        Must include the 'target' parameter for our wrapper driver.
        """
        # Extract standard connection parameters
        opts = url.translate_connect_args(username='user')

        # Add query string parameters
        opts.update(url.query)

        # Add the required 'target' parameter for our wrapper
        if 'target' not in opts:
            opts['target'] = Connection.connect

        # Return empty args list and kwargs dict
        return ([], opts)

    def on_connect(self):
        """
        Return a callable that will be executed on new connections. This can be used if we need to set any session-level
        parameters.
        """
        def set_session_params(conn):
            # Set any Aurora-specific session parameters
            cursor = conn.cursor()
            try:
                # Example: Set statement timeout
                cursor.execute("SET statement_timeout = '60s'")
            finally:
                cursor.close()

        return set_session_params

    def get_isolation_level(self, dbapi_connection):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SHOW transaction_isolation")
            val = cursor.fetchone()
            if val:
                return val.upper().replace(' ', '_')
            return 'READ_COMMITTED'  # return Postgres' default isolation level.
        finally:
            cursor.close()

    def initialize(self, connection):
        """
        Override initialization to handle type introspection.
        The parent class tries to use TypeInfo.fetch() which requires
        a native psycopg connection, not AwsWrapperConnection.
        """
        # Unwrap SQLAlchemy's connection object
        wrapper_conn, wrapper_parent = self._get_wrapper_connection_and_parent(connection)

        # Check if wrapper_conn and wrapper_parent expose their underlying connections
        if wrapper_conn and hasattr(wrapper_conn, 'connection') and wrapper_parent and hasattr(wrapper_parent.connection, 'connection'):
            # Temporarily remove the AwsWrapperConnection from the connection chain
            psycopg_conn = wrapper_conn.connection
            wrapper_parent.connection = psycopg_conn

            try:
                super().initialize(connection)
            finally:
                # Restore wrapper connection in the connection chain.
                wrapper_parent.connection = wrapper_conn
        else:
            # If unable to swap underlying pscyopg connection, skip type introspection.
            # This means custom types (hstore, json, etc.) won't be auto-configured.
            pass

    def _get_wrapper_connection_and_parent(self, connection):
        """
        Traverse the connection chain to find AwsWrapperConnection and its parent connection.

        Args:
            connection: SQLAlchemy Connection object

        Returns:
            AwsWrapperConnection instance or None, parent connection of AwsWrapperConnection or None
        """
        # Start with the DBAPI connection
        parent = connection
        child = connection.connection

        # Traverse up to 5 levels deep (reasonable limit)
        for _ in range(5):
            if isinstance(child, AwsWrapperConnection):
                return child, parent

            # Try to go deeper if there's a .connection attribute
            if hasattr(child, 'connection'):
                parent = child
                child = child.connection
            else:
                break

        return None
