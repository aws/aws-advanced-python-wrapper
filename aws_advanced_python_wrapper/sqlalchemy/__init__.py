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

"""Deprecated location of the SQLAlchemy dialects.

The dialects moved to :mod:`aws_advanced_python_wrapper.sqlalchemy_dialects` in
3.1.0. This package exists only so that ``import`` statements written against
3.0.0 keep working; it will be removed in the next major version.

Only the MySQL name is aliased. 3.0.0's ``entry_points.txt`` also declared
``postgresql.aws_wrapper_psycopg =
aws_advanced_python_wrapper.sqlalchemy.pg_orm_dialect:SqlAlchemyOrmPgDialect``,
but that module was never present in the 3.0.0 distribution — the entry point
was dangling and ``create_engine("postgresql+aws_wrapper_psycopg://")`` raised
``ModuleNotFoundError`` (aws/aws-advanced-python-wrapper#1260, #1273). There is
therefore no working 3.0.0 PostgreSQL name to preserve.
"""
