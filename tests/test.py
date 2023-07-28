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

import timeit

from psycopg import Connection

import aws_wrapper
from aws_wrapper.wrapper import AwsWrapperConnection


def test(num_plugins: int, loop: int):
    aws_wrapper.set_logger()

    connstring = "host=localhost dbname=postgres user=postgres password=qwerty"
    plugins = ""
    for i in range(num_plugins):
        plugins += "dummy,"
    plugins = plugins.rstrip(",")

    pscycopg_conn = Connection.connect(conninfo=connstring)
    awsconn = AwsWrapperConnection.connect(
        connstring,
        Connection.connect,
        plugins=plugins)

    cursor = pscycopg_conn.cursor()
    awscursor = awsconn.cursor()

    total_duration_pg = 0.0
    total_duration_aws = 0.0

    for i in range(loop):
        start_time = timeit.default_timer()
        cursor.execute("SELECT clock_timestamp()")
        duration = timeit.default_timer() - start_time
        # records = cursor.fetchall()
        total_duration_pg += duration

        start_time = timeit.default_timer()
        awscursor.execute("SELECT clock_timestamp()")
        duration = timeit.default_timer() - start_time
        # records = awscursor.fetchall()
        total_duration_aws += duration

    pscycopg_conn.close()
    awsconn.close()

    print("Duration: {}".format(total_duration_pg))
    print("AWS Duration: {}".format(total_duration_aws))


def test_failover():
    # connstring = "host=localhost dbname=postgres user=postgres password=qwerty"
    # connstring = "host=atlas-postgres-3.cluster-czygpppufgy4.us-east-2.rds.amazonaws.com "
    connstring = "host=atlas-postgres-instance-1.czygpppufgy4.us-east-2.rds.amazonaws.com "
    connstring += "dbname=postgres user=pgadmin password=my_password_2020 failover_mode=strict_reader"
    plugins = "failover"

    with AwsWrapperConnection.connect(
            connstring,
            Connection.connect,
            plugins=plugins) as aws_conn:
        aws_cursor = aws_conn.cursor()

        try:
            while True:
                aws_cursor.execute("SELECT * from aurora_db_instance_identifier()")
        except Exception:
            pass

        # start_time = timeit.default_timer()
        # aws_cursor.execute("SELECT pg_sleep(60);")
        # duration = timeit.default_timer() - start_time
        #
        # print("AWS Duration: {}".format(duration))

        aws_cursor.close()


loops = 100000
# test(1, loops)
test_failover()
