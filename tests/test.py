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

import psycopg

import aws_wrapper


def test(num_plugins: int, loop: int):
    aws_wrapper.set_logger()

    connstring = "host=localhost dbname=postgres user=postgres password=qwerty"
    plugins = ""
    for i in range(num_plugins):
        plugins += "dummy,"
    plugins = plugins.rstrip(",")

    pscycopg_conn = psycopg.Connection.connect(conninfo=connstring)
    awsconn = aws_wrapper.AwsWrapperConnection.connect(
        connstring,
        psycopg.Connection.connect,
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


loops = 100000
test(1, loops)
