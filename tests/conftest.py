# tests/conftest.py
import pytest
from unittest.mock import Mock, MagicMock
import pandas as pd
import numpy as np
from PIL import Image
import io


@pytest.fixture
def mock_s3_client():
    """Mock S3 client for IO operations."""
    client = Mock()
    client.get_object.return_value = {
        'Body': Mock(read=Mock(return_value=b'test,data\n1,2\n3,4'))
    }
    return client


@pytest.fixture
def mock_api_response():
    """Mock successful API response."""
    return {
        'status': 200,
        'data': {
            'id': 'test_id',
            'name': 'test_dataset',
            'files': []
        }
    }


@pytest.fixture
def sample_dataframe():
    """Sample DataFrame for testing."""
    return pd.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6]})


@pytest.fixture
def sample_image():
    """Sample PIL Image for testing."""
    return Image.new('RGB', (100, 100), color='red')


@pytest.fixture
def sample_audio_data():
    """Sample audio data for testing."""
    return np.random.rand(44100), 44100  # 1 second at 44.1kHz