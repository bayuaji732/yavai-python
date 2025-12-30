import boto3
from yavai import config

def get_s3_client():
    s3session = boto3.session.Session()
    return s3session.client(
        service_name='s3',
        aws_access_key_id=config.S3_ACCESS_KEY,
        aws_secret_access_key=config.S3_SECRET_KEY,
        endpoint_url=config.S3_ENDPOINT,
    )

def extract_bucket_key(s3a_path):
    if s3a_path.startswith('s3a://'):
        s3a_path = s3a_path[6:]
    components = s3a_path.split('/')
    return components[0], '/'.join(components[1:])