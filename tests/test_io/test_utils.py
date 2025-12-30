# tests/test_io/test_utils.py
import pytest
from unittest.mock import Mock, patch
from yavai.io.utils import get_s3_client, extract_bucket_key


@patch('yavai.config.S3_ACCESS_KEY', 'test_key')
@patch('yavai.config.S3_SECRET_KEY', 'test_secret')
@patch('yavai.config.S3_ENDPOINT', 'http://s3:9000')
@patch('boto3.session.Session')
def test_get_s3_client(mock_session):
    mock_client = Mock()
    mock_session_instance = Mock()
    mock_session_instance.client.return_value = mock_client
    mock_session.return_value = mock_session_instance
    
    client = get_s3_client()
    
    assert client == mock_client
    mock_session_instance.client.assert_called_once_with(
        service_name='s3',
        aws_access_key_id='test_key',
        aws_secret_access_key='test_secret',
        endpoint_url='http://s3:9000'
    )


def test_extract_bucket_key():
    path = 's3a://my-bucket/path/to/file.csv'
    
    bucket, key = extract_bucket_key(path)
    
    assert bucket == 'my-bucket'
    assert key == 'path/to/file.csv'


def test_extract_bucket_key_no_prefix():
    path = 'my-bucket/path/to/file.csv'
    
    bucket, key = extract_bucket_key(path)
    
    assert bucket == 'my-bucket'
    assert key == 'path/to/file.csv'


def test_extract_bucket_key_nested_path():
    path = 's3a://bucket/deep/nested/path/file.txt'
    
    bucket, key = extract_bucket_key(path)
    
    assert bucket == 'bucket'
    assert key == 'deep/nested/path/file.txt'