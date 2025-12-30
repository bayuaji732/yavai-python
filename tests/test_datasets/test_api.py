# tests/test_datasets/test_api.py
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from yavai.datasets.api import DatasetAPI


@pytest.fixture
def api():
    return DatasetAPI()


@pytest.fixture
def mock_client_request(api):
    with patch.object(api._client, 'request') as mock:
        yield mock


def test_get_file_path(api, mock_client_request):
    mock_client_request.return_value = {'data': 's3a://bucket/key/file.csv'}
    
    result = api.get_file_path('file_123')
    
    assert result == 's3a://bucket/key/file.csv'
    mock_client_request.assert_called_once_with(
        'GET',
        ['files', 'file_123', 's3a-path'],
        base_paths=api.V1_LIB
    )


def test_browse_dataset(api, mock_client_request, mock_api_response):
    mock_client_request.return_value = mock_api_response
    
    result = api.browse_dataset('dataset_123')
    
    assert result == mock_api_response['data']
    mock_client_request.assert_called_once_with(
        'GET',
        ['datasets', 'dataset_123', 'browse'],
        base_paths=api.V1_LIB
    )


def test_browse_file(api, mock_client_request):
    expected_data = {'id': 'file_123', 'name': 'test.csv', 'size': 1024}
    mock_client_request.return_value = {'data': expected_data}
    
    result = api.browse_file('file_123')
    
    assert result == expected_data


def test_browse_modelzoo(api, mock_client_request):
    expected_data = {'id': 'model_123', 'models': []}
    mock_client_request.return_value = {'data': expected_data}
    
    result = api.browse_modelzoo('model_123')
    
    assert result == expected_data
    mock_client_request.assert_called_once_with(
        'GET',
        ['list-file-modelZoo', 'model_123'],
        use_alt_base_url=True
    )


def test_browse_jdbc_tables(api, mock_client_request):
    expected_data = {'tables': ['table1', 'table2']}
    mock_client_request.return_value = {'data': expected_data}
    
    result = api.browse_jdbc_tables('dataset_123')
    
    assert result == expected_data


def test_get_table_preview(api, mock_client_request, sample_dataframe):
    mock_client_request.return_value = {
        'data': [
            {'col1': 1, 'col2': 4},
            {'col1': 2, 'col2': 5}
        ]
    }
    
    result = api.get_table_preview('dataset_123', 'table_name')
    
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    mock_client_request.assert_called_once_with(
        'GET',
        ['jdbcdisplay', 'preview', 'dataset_123'],
        base_paths=api.V2,
        params={'tableName': 'table_name'}
    )


@patch('yavai.config.TOKEN', 'test_token')
def test_download_dataset(api, mock_client_request):
    expected_result = {'statusCode': 200, 'filename': 'test.zip'}
    mock_client_request.return_value = expected_result
    
    result = api.download_dataset('dataset_123')
    
    assert result == expected_result
    call_args = mock_client_request.call_args
    assert call_args[1]['headers']['Authorization'] == 'Bearer test_token'
    assert call_args[1]['is_download'] is True