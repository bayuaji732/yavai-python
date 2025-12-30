# tests/test_io/test_media.py
import pytest
from unittest.mock import Mock, patch, MagicMock
import numpy as np
from PIL import Image
import io
from yavai.io import media


@pytest.fixture
def mock_api_get_path():
    with patch('yavai._api.get_file_path') as mock:
        mock.return_value = 's3a://bucket/path/file.jpg'
        yield mock


@pytest.fixture
def mock_s3_client():
    with patch('yavai.io.media.get_s3_client') as mock:
        client = Mock()
        mock.return_value = client
        yield client


def test_open_image(mock_api_get_path, mock_s3_client, sample_image):
    img_bytes = io.BytesIO()
    sample_image.save(img_bytes, format='JPEG')
    img_data = img_bytes.getvalue()
    
    mock_s3_client.get_object.return_value = {'Body': Mock(read=Mock(return_value=img_data))}
    
    result = media.open_image('file_123')
    
    assert isinstance(result, Image.Image)
    assert result.mode == 'RGB'


def test_open_image_with_resize(mock_api_get_path, mock_s3_client, sample_image):
    img_bytes = io.BytesIO()
    sample_image.save(img_bytes, format='JPEG')
    img_data = img_bytes.getvalue()
    
    mock_s3_client.get_object.return_value = {'Body': Mock(read=Mock(return_value=img_data))}
    
    result = media.open_image('file_123', width=50, height=50)
    
    assert result.size == (50, 50)


def test_open_image_width_only(mock_api_get_path, mock_s3_client, sample_image):
    img_bytes = io.BytesIO()
    sample_image.save(img_bytes, format='JPEG')
    img_data = img_bytes.getvalue()
    
    mock_s3_client.get_object.return_value = {'Body': Mock(read=Mock(return_value=img_data))}
    
    result = media.open_image('file_123', width=50)
    
    assert result.size[0] == 50
    assert result.size[1] == 50  # Original was 100x100, so maintains aspect ratio


@patch('librosa.load')
def test_read_audio(mock_librosa, mock_api_get_path, mock_s3_client):
    audio_array = np.random.rand(44100)
    sample_rate = 44100
    mock_librosa.return_value = (audio_array, sample_rate)
    
    mock_s3_client.get_object.return_value = {'Body': Mock(read=Mock(return_value=b'audio_data'))}
    
    result_array, result_sr = media.read_audio('file_123')
    
    assert isinstance(result_array, np.ndarray)
    assert result_sr == 44100
    assert len(result_array) == 44100


@patch('librosa.load')
@patch('IPython.display.Audio')
def test_open_audio_with_librosa(mock_audio, mock_librosa, mock_api_get_path, mock_s3_client):
    audio_array = np.random.rand(44100)
    mock_librosa.return_value = (audio_array, 44100)
    
    mock_s3_client.get_object.return_value = {'Body': Mock(read=Mock(return_value=b'audio_data'))}
    
    result = media.open_audio('file_123')
    
    mock_audio.assert_called_once()


@patch('librosa.load')
@patch('pydub.AudioSegment.from_file')
@patch('IPython.display.Audio')
def test_open_audio_fallback_to_pydub(mock_audio, mock_pydub, mock_librosa, 
                                       mock_api_get_path, mock_s3_client):
    mock_librosa.side_effect = Exception('Librosa failed')
    
    mock_segment = Mock()
    mock_segment.get_array_of_samples.return_value = [1, 2, 3, 4]
    mock_segment.frame_rate = 44100
    mock_pydub.return_value = mock_segment
    
    mock_s3_client.get_object.return_value = {'Body': Mock(read=Mock(return_value=b'audio_data'))}
    
    result = media.open_audio('file_123')
    
    mock_audio.assert_called_once()


@patch('cv2.VideoCapture')
@patch('os.remove')
@patch('tempfile.NamedTemporaryFile')
def test_read_video(mock_temp, mock_remove, mock_cv2, mock_api_get_path, mock_s3_client):
    mock_file = Mock()
    mock_file.name = '/tmp/test.mp4'
    mock_file.__enter__ = Mock(return_value=mock_file)
    mock_file.__exit__ = Mock(return_value=False)
    mock_temp.return_value = mock_file
    
    mock_cap = Mock()
    mock_cap.isOpened.side_effect = [True, True, False]
    mock_cap.read.side_effect = [
        (True, np.zeros((100, 100, 3), dtype=np.uint8)),
        (True, np.zeros((100, 100, 3), dtype=np.uint8)),
        (False, None)
    ]
    mock_cv2.return_value = mock_cap
    
    mock_s3_client.get_object.return_value = {'Body': Mock(read=Mock(return_value=b'video_data'))}
    
    frames = media.read_video('file_123')
    
    assert len(frames) == 2
    assert all(isinstance(f, np.ndarray) for f in frames)
    mock_remove.assert_called_once_with('/tmp/test.mp4')