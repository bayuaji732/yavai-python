# tests/test_connections/test_jdbc.py
import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import pandas as pd
from yavai.connections.jdbc import JDBC


@pytest.fixture
def jdbc():
    with patch('pathlib.Path.mkdir'):
        return JDBC(auto_download=False)


def test_identify_required_jars_hive(jdbc):
    url = 'jdbc:hive2://localhost:10000/default'
    
    required = jdbc._identify_required_jars(url)
    
    assert 'hive' in required
    assert 'hadoop' in required


def test_identify_required_jars_postgresql(jdbc):
    url = 'jdbc:postgresql://localhost:5432/db'
    
    required = jdbc._identify_required_jars(url)
    
    assert 'postgresql' in required
    assert 'hadoop' not in required


def test_identify_required_jars_mysql(jdbc):
    url = 'jdbc:mysql://localhost:3306/db'
    
    required = jdbc._identify_required_jars(url)
    
    assert 'mysql' in required


def test_get_driver_name_hive(jdbc):
    driver = jdbc._get_driver_name('jdbc:hive2://localhost:10000/default')
    
    assert driver == 'org.apache.hive.jdbc.HiveDriver'


def test_get_driver_name_postgresql(jdbc):
    driver = jdbc._get_driver_name('jdbc:postgresql://localhost:5432/db')
    
    assert driver == 'org.postgresql.Driver'


def test_get_driver_name_mysql(jdbc):
    driver = jdbc._get_driver_name('jdbc:mysql://localhost:3306/db')
    
    assert driver == 'com.mysql.cj.jdbc.Driver'


def test_get_driver_name_unknown(jdbc):
    driver = jdbc._get_driver_name('jdbc:unknown://localhost')
    
    assert driver is None


def test_ensure_jar_exists_found(jdbc):
    jar_info = {
        'artifact_id': 'test-driver',
        'version': '1.0.0'
    }
    
    with patch.object(Path, 'exists', return_value=True):
        result = jdbc._ensure_jar_exists('test', jar_info)
        
        assert result is not None
        assert result.name == 'test-driver-1.0.0.jar'


def test_ensure_jar_not_found_no_download(jdbc):
    jar_info = {
        'artifact_id': 'test-driver',
        'version': '1.0.0'
    }
    jdbc.auto_download = False
    
    with patch.object(Path, 'exists', return_value=False):
        result = jdbc._ensure_jar_exists('test', jar_info)
        
        assert result is None


@patch('requests.get')
@patch('builtins.open', create=True)
def test_download_jar_success(mock_open, mock_get, jdbc):
    jar_info = {
        'group_id': 'org.test',
        'artifact_id': 'test-driver',
        'version': '1.0.0'
    }
    
    mock_response = Mock()
    mock_response.headers = {'content-length': '1000'}
    mock_response.iter_content = Mock(return_value=[b'chunk1', b'chunk2'])
    mock_get.return_value = mock_response
    
    result = jdbc._download_jar(jar_info, Path('/tmp/test.jar'))
    
    assert result is True
    mock_get.assert_called_once()


@patch('requests.get')
def test_download_jar_failure(mock_get, jdbc):
    jar_info = {
        'group_id': 'org.test',
        'artifact_id': 'test-driver',
        'version': '1.0.0'
    }
    
    mock_get.side_effect = Exception('Network error')
    
    result = jdbc._download_jar(jar_info, Path('/tmp/test.jar'))
    
    assert result is False


@patch('jaydebeapi.connect')
def test_connect_success(mock_connect, jdbc):
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    
    with patch.object(jdbc, '_identify_required_jars', return_value={}):
        with patch.object(jdbc, '_ensure_jar_exists', return_value=Path('/tmp/test.jar')):
            result = jdbc.connect('jdbc:hive2://localhost:10000/default', 'user', 'pass')
            
            assert result is True
            assert jdbc._conn == mock_conn
            assert jdbc._cursor == mock_cursor


def test_connect_no_jars(jdbc):
    fake_jars = {'postgres': {'group_id': 'org.postgres', 'version': '1.0'}}
    with patch.object(jdbc, '_identify_required_jars', return_value=fake_jars):
        with patch.object(jdbc, '_ensure_jar_exists', return_value=None):
            with pytest.raises(RuntimeError, match='No valid JARs'):
                jdbc.connect('jdbc:unknown://localhost', 'user', 'pass')


def test_execute_not_connected(jdbc):
    with pytest.raises(ConnectionError, match='Not connected'):
        jdbc.execute('SELECT * FROM table')


def test_execute_with_results(jdbc):
    mock_cursor = Mock()
    mock_cursor.description = [('col1',), ('col2',)]
    mock_cursor.fetchall.return_value = [(1, 2), (3, 4)]
    jdbc._cursor = mock_cursor
    
    result = jdbc.execute('SELECT * FROM table')
    
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ['col1', 'col2']
    assert len(result) == 2


def test_execute_no_results(jdbc):
    mock_cursor = Mock()
    mock_cursor.description = None
    jdbc._cursor = mock_cursor
    
    result = jdbc.execute('CREATE TABLE test')
    
    assert result is None


def test_close(jdbc):
    mock_conn = Mock()
    mock_cursor = Mock()
    jdbc._conn = mock_conn
    jdbc._cursor = mock_cursor
    
    jdbc.close()
    
    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()


def test_list_available_drivers(jdbc):
    drivers = jdbc.list_available_drivers()
    
    assert 'hive' in drivers
    assert 'postgresql' in drivers
    assert drivers['hive'] == 'org.apache.hive.jdbc.HiveDriver'


def test_add_custom_driver(jdbc):
    jdbc.add_custom_driver(
        'oracle',
        'com.oracle',
        'ojdbc',
        '12.2.0.1',
        'oracle.jdbc.OracleDriver'
    )
    
    assert 'oracle' in jdbc.JAR_REGISTRY
    assert jdbc.JAR_REGISTRY['oracle']['driver_class'] == 'oracle.jdbc.OracleDriver'