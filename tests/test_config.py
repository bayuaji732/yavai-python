# tests/test_config.py
import pytest
from unittest.mock import patch
import os


@patch.dict(os.environ, {
    'TOKEN': 'test_token',
    'API_BASE_URL': 'https://api.test.com',
    'S3_ACCESS_KEY': 'test_access_key'
}, clear=True)
def test_config_loads_env_vars():
    # Reimport to pick up mocked env vars
    from importlib import reload
    from yavai import config as config_module
    reload(config_module)
    
    assert config_module.TOKEN == 'test_token'
    assert config_module.API_BASE_URL == 'https://api.test.com'
    assert config_module.S3_ACCESS_KEY == 'test_access_key'


@patch.dict(os.environ, {}, clear=True)
def test_config_handles_missing_env_vars():
    from importlib import reload
    from yavai import config as config_module
    reload(config_module)
    
    assert config_module.TOKEN is None
    assert config_module.API_BASE_URL is None