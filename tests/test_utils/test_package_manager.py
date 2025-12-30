# tests/test_utils/test_package_manager.py
import pytest
from unittest.mock import patch, call
import subprocess
import sys
from yavai.utils.package_manager import PackageManager


@pytest.fixture
def pkg_mgr():
    return PackageManager()


@patch('subprocess.check_call')
def test_install(mock_check_call, pkg_mgr):
    pkg_mgr.install('pandas')
    
    expected_cmd = [sys.executable, '-m', 'pip', 'install', 'pandas']
    mock_check_call.assert_called_once_with(expected_cmd)


@patch('subprocess.check_call')
def test_install_with_version(mock_check_call, pkg_mgr):
    pkg_mgr.install('pandas==1.5.0')
    
    expected_cmd = [sys.executable, '-m', 'pip', 'install', 'pandas==1.5.0']
    mock_check_call.assert_called_once_with(expected_cmd)


@patch('subprocess.check_call')
def test_uninstall(mock_check_call, pkg_mgr):
    pkg_mgr.uninstall('pandas')
    
    expected_cmd = [sys.executable, '-m', 'pip', 'uninstall', 'pandas']
    mock_check_call.assert_called_once_with(expected_cmd)


@patch('subprocess.check_call')
def test_upgrade(mock_check_call, pkg_mgr):
    pkg_mgr.upgrade('pandas')
    
    expected_cmd = [sys.executable, '-m', 'pip', 'upgrade', 'pandas']
    mock_check_call.assert_called_once_with(expected_cmd)


@patch('subprocess.check_call')
def test_list(mock_check_call, pkg_mgr):
    pkg_mgr.list()
    
    expected_cmd = [sys.executable, '-m', 'pip', 'list']
    mock_check_call.assert_called_once_with(expected_cmd)


@patch('subprocess.check_call')
def test_show(mock_check_call, pkg_mgr):
    pkg_mgr.show('pandas')
    
    expected_cmd = [sys.executable, '-m', 'pip', 'show', 'pandas']
    mock_check_call.assert_called_once_with(expected_cmd)


@patch('subprocess.check_call')
def test_install_failure(mock_check_call, pkg_mgr):
    mock_check_call.side_effect = subprocess.CalledProcessError(1, 'cmd')
    
    with pytest.raises(subprocess.CalledProcessError):
        pkg_mgr.install('nonexistent-package')