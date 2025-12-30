# tests/test_datasets/test_client.py
import pytest
from unittest.mock import Mock, patch
import requests
from yavai.datasets.client import YAVAIClient


@pytest.fixture
def client():
    return YAVAIClient()


@pytest.fixture
def mock_response():
    response = Mock(spec=requests.Response)
    response.json.return_value = {'status': 200, 'data': 'test'}
    response.raise_for_status = Mock()
    response.content = b'test content'
    return response


@patch('yavai.config.API_BASE_URL', 'https://api.test.com')
def test_build_url(client):
    url = client._build_url(['datasets', '123'], ['api', 'v1'], False)
    
    assert 'api.test.com' in url
    assert 'api/v1/datasets/123' in url


@patch('yavai.config.API_BASE_URL2', 'http://api2.test.com')
def test_build_url_alt_base(client):
    url = client._build_url(['files'], None, True)
    
    assert 'api2.test.com' in url
    assert 'files' in url


def test_execute_request_success(client, mock_response):
    with patch.object(client._session, 'request', return_value=mock_response):
        result = client._execute_request('GET', 'http://test.com', None, None, None)
        
        assert result == mock_response
        mock_response.raise_for_status.assert_called_once()


def test_execute_request_with_data(client, mock_response):
    with patch.object(client._session, 'request', return_value=mock_response) as mock_req:
        data = {'key': 'value'}
        client._execute_request('POST', 'http://test.com', None, None, data)
        
        call_args = mock_req.call_args
        assert call_args[1]['data'] is not None


def test_execute_request_raises_on_error(client):
    error_response = Mock(spec=requests.Response)
    error_response.raise_for_status.side_effect = requests.HTTPError('404 Not Found')
    
    with patch.object(client._session, 'request', return_value=error_response):
        with pytest.raises(requests.HTTPError):
            client._execute_request('GET', 'http://test.com', None, None, None)


@patch('builtins.open', create=True)
@patch('getpass.getuser', return_value='testuser')
def test_handle_download(mock_getuser, mock_open, client, mock_response):
    result = client._handle_download(mock_response)
    
    assert result['statusCode'] == 200
    assert result['status'] == 'OK'
    assert 'testuser' in result['filename']
    mock_open.assert_called_once()


def test_request_non_download(client, mock_response):
    with patch.object(client, '_build_url', return_value='http://test.com'):
        with patch.object(client, '_execute_request', return_value=mock_response):
            result = client.request('GET', ['path'], is_download=False)
            
            assert result == {'status': 200, 'data': 'test'}