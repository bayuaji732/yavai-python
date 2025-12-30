# tests/test_io/test_readers.py
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import io
from yavai.io import readers


@pytest.fixture
def mock_api_get_path():
    with patch('yavai._api.get_file_path') as mock:
        mock.return_value = 's3a://bucket/path/file.csv'
        yield mock


@pytest.fixture
def mock_s3_client():
    with patch('yavai.io.readers.get_s3_client') as mock:
        client = Mock()
        mock.return_value = client
        yield client


def test_read_csv(mock_api_get_path, mock_s3_client):
    csv_data = b'col1,col2\n1,2\n3,4'
    mock_s3_client.get_object.return_value = {'Body': io.BytesIO(csv_data)}
    
    df = readers.read_csv('file_123')
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ['col1', 'col2']
    mock_api_get_path.assert_called_once_with('file_123')


def test_read_csv_with_kwargs(mock_api_get_path, mock_s3_client):
    csv_data = b'col1,col2\n1,2\n3,4'
    mock_s3_client.get_object.return_value = {'Body': io.BytesIO(csv_data)}
    
    df = readers.read_csv('file_123', sep=',', header=0)
    
    assert isinstance(df, pd.DataFrame)


def test_read_excel_xlsx(mock_api_get_path, mock_s3_client):
    # XLSX signature
    xlsx_data = b'\x50\x4b\x03\x04' + b'\x00' * 100
    mock_s3_client.get_object.return_value = {'Body': Mock(read=Mock(return_value=xlsx_data))}
    
    with patch('pandas.read_excel') as mock_read:
        mock_read.return_value = pd.DataFrame({'col': [1, 2]})
        
        df = readers.read_excel('file_123')
        
        assert isinstance(df, pd.DataFrame)
        call_kwargs = mock_read.call_args[1]
        assert call_kwargs['engine'] == 'openpyxl'


def test_read_excel_xls(mock_api_get_path, mock_s3_client):
    # Non-XLSX signature
    xls_data = b'\xD0\xCF\x11\xE0' + b'\x00' * 100
    mock_s3_client.get_object.return_value = {'Body': Mock(read=Mock(return_value=xls_data))}
    
    with patch('pandas.read_excel') as mock_read:
        mock_read.return_value = pd.DataFrame({'col': [1, 2]})
        
        df = readers.read_excel('file_123')
        
        call_kwargs = mock_read.call_args[1]
        assert call_kwargs['engine'] == 'xlrd'


@patch('os.remove')
@patch('tempfile.NamedTemporaryFile')
def test_read_sav(mock_temp, mock_remove, mock_api_get_path, mock_s3_client):
    sav_data = b'SPSS_DATA'
    mock_s3_client.get_object.return_value = {'Body': Mock(read=Mock(return_value=sav_data))}
    
    mock_file = Mock()
    mock_file.name = '/tmp/test.sav'
    mock_file.__enter__ = Mock(return_value=mock_file)
    mock_file.__exit__ = Mock(return_value=False)
    mock_temp.return_value = mock_file
    
    with patch('pandas.read_spss') as mock_read:
        mock_read.return_value = pd.DataFrame({'col': [1, 2]})
        
        df = readers.read_sav('file_123')
        
        assert isinstance(df, pd.DataFrame)
        mock_remove.assert_called_once_with('/tmp/test.sav')


def test_read_file_binary(mock_api_get_path, mock_s3_client):
    file_data = b'binary content'
    mock_s3_client.get_object.return_value = {'Body': Mock(read=Mock(return_value=file_data))}
    
    result = readers.read_file('file_123', mode='rb')
    
    assert result == file_data


def test_read_file_text(mock_api_get_path, mock_s3_client):
    file_data = b'text content'
    mock_s3_client.get_object.return_value = {'Body': Mock(read=Mock(return_value=file_data))}
    
    result = readers.read_file('file_123', mode='r')
    
    assert result == 'text content'


def test_read_json(mock_api_get_path, mock_s3_client):
    json_data = b'{"key": "value", "number": 42}'
    mock_s3_client.get_object.return_value = {'Body': Mock(read=Mock(return_value=json_data))}
    
    result = readers.read_json('file_123')
    
    assert isinstance(result, dict)
    assert result['key'] == 'value'
    assert result['number'] == 42