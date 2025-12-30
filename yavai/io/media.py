# yavai/io/media.py

import io
import os
import base64
import tempfile
import numpy as np
from PIL import Image
from pillow_heif import register_heif_opener
import IPython.display as ipd
from IPython.display import display, HTML
import librosa
from pydub import AudioSegment
import cv2

from yavai.io.utils import get_s3_client, extract_bucket_key
from yavai._context import api as _api

# --- IMAGES ---
def open_image(file_id: str, width=None, height=None):
    """Logic from your image_reader.py"""
    register_heif_opener()
    filepath = _api.get_file_path(file_id)
    bucket, key = extract_bucket_key(filepath)
    
    s3 = get_s3_client()
    data = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
    
    img = Image.open(io.BytesIO(data)).convert("RGB")
    
    if width or height:
        # Aspect ratio logic from your original code
        orig_w, orig_h = img.size
        if width and not height:
            height = int(width * (orig_h / orig_w))
        elif height and not width:
            width = int(height * (orig_w / orig_h))
        img = img.resize((width, height), Image.LANCZOS)
        
    return img

# --- AUDIO ---
def read_audio(file_id: str):
    """Returns (numpy_array, sample_rate) for Training."""
    filepath = _api.get_file_path(file_id)
    bucket, key = extract_bucket_key(filepath)
    s3 = get_s3_client()
    data = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
    
    # Use librosa to get numpy array immediately
    audio_data, sample_rate = librosa.load(io.BytesIO(data), sr=None)
    return audio_data, sample_rate

def open_audio(file_id: str):
    """Logic from your audio_reader.py - requires librosa/pydub"""
    filepath = _api.get_file_path(file_id)
    bucket, key = extract_bucket_key(filepath)
    
    s3 = get_s3_client()
    file_data = s3.get_object(Bucket=bucket, Key=key)['Body'].read()

    try:
        # Try librosa (best for WAV/Analysis)
        audio_io = io.BytesIO(file_data)
        audio_data, sample_rate = librosa.load(audio_io)
    except Exception:
        try:
            # Fallback to pydub (best for MP3/General)
            audio = AudioSegment.from_file(io.BytesIO(file_data))
            audio_data = np.array(audio.get_array_of_samples())
            sample_rate = audio.frame_rate
        except Exception as e:
            raise Exception(f"Unsupported audio format: {e}")

    return ipd.Audio(audio_data, rate=sample_rate)

# --- VIDEO ---
def read_video(file_id: str):
    """Returns a List of NumPy arrays (one per frame) for Training."""
    filepath = _api.get_file_path(file_id)
    bucket, key = extract_bucket_key(filepath)
    s3 = get_s3_client()
    file_data = s3.get_object(Bucket=bucket, Key=key)['Body'].read()

    # OpenCV needs a file on disk
    with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as tmp:
        tmp.write(file_data)
        tmp_path = tmp.name

    cap = cv2.VideoCapture(tmp_path)
    frames = []
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret: break
        # Convert BGR (OpenCV default) to RGB (DL standard)
        frames.append(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
    
    cap.release()
    os.remove(tmp_path)
    return frames # You can do np.array(frames) on this

def open_video(file_id: str, width=None, height=None):
    """Logic from your video_reader.py - requires base64/HTML display"""
    filepath = _api.get_file_path(file_id)
    bucket, key = extract_bucket_key(filepath)
    
    s3 = get_s3_client()
    file_data = s3.get_object(Bucket=bucket, Key=key)['Body'].read()

    # Convert to base64 for HTML5 Video Player in Notebook
    video_base64 = base64.b64encode(file_data).decode('ascii')
    
    width_str = f'width="{width}"' if width else ''
    height_str = f'height="{height}"' if height else ''
    
    video_tag = f'''
        <video {width_str} {height_str} controls>
            <source src="data:video/mp4;base64,{video_base64}" type="video/mp4">
        </video>
    '''
    return display(HTML(video_tag))