
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


def test(num_of_plugins: int, function_cache: bool, loop: int):
    conn = psycopg.Connection.connect(conninfo="host=localhost dbname=postgres user=postgres password=qwerty")

    awsconn = aws_wrapper.AwsWrapperConnection.connect(
        "host=localhost dbname=postgres user=postgres password=qwerty",
        psycopg.Connection.connect,
        num_of_plugins=num_of_plugins, function_cache=function_cache)

    cursor = conn.cursor()
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

    conn.close()
    awsconn.close()

    print("Duration: {}".format(total_duration_pg))
    print("AWS Duration: {}".format(total_duration_aws))


loops = 100000
test(0, False, loops)
test(1, False, loops)
test(1, True, loops)
test(50, False, loops)
test(50, True, loops)
