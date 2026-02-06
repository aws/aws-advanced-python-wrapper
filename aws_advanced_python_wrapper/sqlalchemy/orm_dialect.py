# aws_advanced_python_wrapper/sqlalchemy/sqlalchemy_psycopg_dialect.py
from psycopg import Connection
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg
import re

class SqlAlchemyOrmPgDialect(PGDialect_psycopg):
    """
    SQLAlchemy dialect for AWS Advanced Python Wrapper.
    Extends PostgreSQL psycopg dialect with Aurora-aware connection handling.
    """

    name = 'postgresql'
    driver = 'aws_wrapper'

    def __init__(self, **kwargs):
        # Skip parent's version check since we're a wrapper, not psycopg itself
        super(PGDialect_psycopg, self).__init__(**kwargs)

        # Dynamically detect the actual psycopg version we're wrapping to ensure
        # SQLAlchemy uses the correct feature set and SQL generation
        try:
            import psycopg
            m = re.match(r"(\d+)\.(\d+)(?:\.(\d+))?", psycopg.__version__)
            if m:
                self.psycopg_version = tuple(
                    int(x) for x in m.group(1, 2, 3) if x is not None
                )
            else:
                self.psycopg_version = (3, 0, 2)  # Minimum supported
        except (ImportError, AttributeError):
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
        Must include 'target' parameter for the wrapper.
        """
        # Extract standard connection parameters
        opts = url.translate_connect_args(username='user')

        # Add query string parameters
        opts.update(url.query)

        # Add the required 'target' parameter for your wrapper
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
        """Get the current isolation level"""
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SHOW transaction_isolation")
            val = cursor.fetchone()
            if val:
                # Extract first element from tuple and format
                return val.upper().replace(' ', '_')
            return 'READ_COMMITTED'  # PostgreSQL's default
        finally:
            cursor.close()

    def initialize(self, connection):
        """
        Override initialization to handle type introspection.
        The parent class tries to use TypeInfo.fetch() which requires
        a native psycopg connection, not our wrapper.
        """
        # Find the AwsWrapperConnection at whatever nesting level
        wrapper_conn = self._get_wrapper_connection(connection)

        if wrapper_conn and hasattr(wrapper_conn, 'connection'):
            # Get the underlying psycopg connection
            underlying_conn = wrapper_conn.connection

            # Temporarily swap the entire connection chain
            original_dbapi_conn = connection.connection
            connection.connection = underlying_conn

            try:
                # Call parent initialization with native psycopg connection
                super().initialize(connection)
            finally:
                # Restore original connection chain
                connection.connection = original_dbapi_conn
        else:
            # If we can't find wrapper or it doesn't expose underlying connection,
            # skip type introspection (custom types won't be auto-configured)
            pass

    def _get_wrapper_connection(self, connection):
        """
        Traverse the connection chain to find AwsWrapperConnection.
        Handles variable nesting depths depending on pool configuration.
        """
        from aws_advanced_python_wrapper import AwsWrapperConnection

        # Start with the DBAPI connection
        current = connection.connection

        # Traverse up to 5 levels deep (reasonable limit)
        for _ in range(5):
            if isinstance(current, AwsWrapperConnection):
                return current

            # Try to go deeper if there's a .connection attribute
            if hasattr(current, 'connection'):
                current = current.connection
            else:
                break

        return None
