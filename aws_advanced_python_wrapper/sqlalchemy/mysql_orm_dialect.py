# aws_advanced_python_wrapper/sqlalchemy/sqlalchemy_mysqlconnector_dialect.py
from psycopg import Connection
from sqlalchemy.dialects.mysql.mysqlconnector import MySQLDialect_mysqlconnector
import re

from aws_advanced_python_wrapper import AwsWrapperConnection


class SqlAlchemyOrmMysqlDialect(MySQLDialect_mysqlconnector):
    """
    SQLAlchemy dialect for AWS Advanced Python Wrapper with mysqlconnector. Extends the SQLAlchemy MySQL mysqlconnector dialect.
    This dialect is not related to the DriverDialect or DatabaseDialect classes used by our driver. Instead, it is used
    directly by SQLAlchemy. This dialect is registered in pyproject.toml and is selected by prefixing the connection
    string passed to create_engine with "mysql+aws_wrapper_mysqlconnector://" ("[name]+[driver]").
    """

    name = 'mysql'
    driver = 'aws_wrapper_mysqlconnector'

