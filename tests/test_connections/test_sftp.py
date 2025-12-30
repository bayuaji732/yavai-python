# tests/test_connections/test_sftp.py
import pytest
from unittest.mock import Mock, patch, MagicMock
from yavai.connections.sftp import SFTPClient


@pytest.fixture
def sftp_client():
    return SFTPClient()


@patch('paramiko.SSHClient')
def test_connect(mock_ssh_class, sftp_client):
    mock_ssh = Mock()
    mock_sftp = Mock()
    mock_ssh.open_sftp.return_value = mock_sftp
    mock_ssh_class.return_value = mock_ssh
    
    result = sftp_client.connect('hostname', 'user', 'pass', port=22)
    
    assert result == sftp_client
    assert sftp_client.ssh == mock_ssh
    assert sftp_client.sftp == mock_sftp
    mock_ssh.connect.assert_called_once_with('hostname', port=22, username='user', password='pass')


def test_close(sftp_client):
    mock_ssh = Mock()
    mock_sftp = Mock()
    sftp_client.ssh = mock_ssh
    sftp_client.sftp = mock_sftp
    
    sftp_client.close()
    
    mock_sftp.close.assert_called_once()
    mock_ssh.close.assert_called_once()


def test_close_none_objects(sftp_client):
    # Should not raise
    sftp_client.close()


def test_list_files(sftp_client):
    mock_sftp = Mock()
    mock_sftp.listdir.return_value = ['file1.txt', 'file2.csv']
    sftp_client.sftp = mock_sftp
    
    result = sftp_client.list_files('/remote/path')
    
    assert result == ['file1.txt', 'file2.csv']
    mock_sftp.listdir.assert_called_once_with('/remote/path')


def test_download(sftp_client):
    mock_sftp = Mock()
    sftp_client.sftp = mock_sftp
    
    sftp_client.download('/remote/file.txt', '/local/file.txt')
    
    mock_sftp.get.assert_called_once_with('/remote/file.txt', '/local/file.txt')


def test_download_default_local_path(sftp_client):
    mock_sftp = Mock()
    sftp_client.sftp = mock_sftp
    
    sftp_client.download('/remote/path/file.txt')
    
    mock_sftp.get.assert_called_once_with('/remote/path/file.txt', 'file.txt')


def test_upload(sftp_client):
    mock_sftp = Mock()
    sftp_client.sftp = mock_sftp
    
    sftp_client.upload('/local/file.txt', '/remote/file.txt')
    
    mock_sftp.put.assert_called_once_with('/local/file.txt', '/remote/file.txt')


def test_context_manager():
    with patch('paramiko.SSHClient') as mock_ssh_class:
        mock_ssh = Mock()
        mock_sftp = Mock()
        mock_ssh.open_sftp.return_value = mock_sftp
        mock_ssh_class.return_value = mock_ssh
        
        with SFTPClient() as client:
            client.connect('hostname', 'user', 'pass')
            assert client.sftp == mock_sftp
        
        mock_sftp.close.assert_called_once()
        mock_ssh.close.assert_called_once()