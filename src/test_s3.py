import json
import pytest
from unittest.mock import MagicMock
from filter import read_request_from_s3, write_file_to_s3

@pytest.fixture
def mock_boto3_client(mocker):
    return mocker.patch("boto3.client")

def test_read_request_from_s3(mock_boto3_client):
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client
    mock_s3_client.get_object.return_value = {'Body': MagicMock(read=lambda: b'{"key": "value"}')}

    result = read_request_from_s3('some_bucket', 'some_key')

    assert result is not None
    assert result == {'key': 'value'}
    mock_s3_client.get_object.assert_called_with(Bucket='some_bucket', Key='some_key')

def test_write_file_to_s3(mock_boto3_client):
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client
    data = {'key': 'value'}

    write_file_to_s3('some_bucket', 'some_key', data)

    mock_s3_client.put_object.assert_called_with(
        Bucket='some_bucket',
        Key='some_key',
        Body=bytes(json.dumps(data, indent=4).encode('UTF-8'))
    )
