import aws_wrapper

conninfo: str = "host=localhost dbname=postgres user=postgres password=qwerty"


def test_connection_basic(mocker):
    connection_mock = mocker.MagicMock()
    connection_mock.connect.return_value = "Test"

    aws_wrapper.AwsWrapperConnection.connect(
        conninfo,
        connection_mock.connect)

    connection_mock.connect.assert_called_with(conninfo)


# test_connection_kwargs
# test_connection_function_cache
# test_connection_str_callable
