# yavai/io/readers.py

import pandas as pd
import tempfile
import os
import json
from io import BytesIO
# from pyhive import hive

from yavai.io.utils import get_s3_client, extract_bucket_key
from yavai._context import api as _api


def read_csv(file_id: str, **kwargs):
    path = _api.get_file_path(file_id)
    bucket, key = extract_bucket_key(path)
    
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj['Body'], **kwargs)

def read_excel(file_id: str, **kwargs):
    path = _api.get_file_path(file_id)
    bucket, key = extract_bucket_key(path)
    
    s3 = get_s3_client()
    content = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
    
    engine = 'openpyxl' if content[:4] == b'\x50\x4b\x03\x04' else 'xlrd'
    return pd.read_excel(BytesIO(content), engine=engine, **kwargs)

def read_sav(file_id: str, **kwargs):
    # Uses raw data read and temp file for SPSS compatibility
    path = _api.get_file_path(file_id)
    bucket, key = extract_bucket_key(path)
    
    s3 = get_s3_client()
    data = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
    
    with tempfile.NamedTemporaryFile(delete=False, suffix='.sav') as tmp:
        tmp.write(data)
        tmp_name = tmp.name
    
    try:
        return pd.read_spss(tmp_name, **kwargs)
    finally:
        os.remove(tmp_name)

def read_file(file_id: str, mode: str = 'rb'):
    """Reads raw file data. Supports 'rb' for bytes or 'r' for text."""
    filepath = _api.get_file_path(file_id)
    bucket, key = extract_bucket_key(filepath)
    
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read()
    
    if mode == 'r':
        return data.decode('utf-8')
    return data

def read_json(file_id: str, **kwargs):
    """Reads a JSON file and returns a dictionary."""
    data = read_file(file_id, mode='r')
    return json.loads(data, **kwargs)

"""
def read_table(table_name: str, **kwargs):
    conn = hive.Connection(
        host=settings.HIVE_HOST, port=settings.HIVE_PORT, auth="KERBEROS",
        kerberos_service_name=settings.HIVE_KERBEROS_SERVICE_NAME,
        database=settings.HIVE_DATABASE
    )
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
    cols = [d[0] for d in cursor.description if d[0].lower() != 'fg_date']
    cursor.execute(f"SELECT {', '.join(cols)} FROM {table_name}")
    df = pd.DataFrame(cursor.fetchall(), columns=cols)
    conn.close()
    return df
"""
