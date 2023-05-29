import timeit

import psycopg

import aws_wrapper


def test(numOfPlugins: int, functionCache: bool, loop: int):
    conn = psycopg.Connection.connect(conninfo="host=localhost dbname=postgres user=postgres password=qwerty")

    awsconn = aws_wrapper.AwsWrapperConnection.connect(
        "host=localhost dbname=postgres user=postgres password=qwerty",
        psycopg.Connection.connect,
        numOfPlugins=numOfPlugins, functionCache=functionCache)

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
