import os
from dotenv import load_dotenv

# Load environment variables from .env file
#load_dotenv()

TOKEN = os.environ.get("TOKEN")
API_BASE_URL = os.environ.get("API_BASE_URL")
API_BASE_URL2 = os.environ.get("API_BASE_URL2")

API_URL = os.environ.get("API_URL")
API_URL2 = os.environ.get("API_URL2")

# S3 Ozone
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT")

# Mlflow Minio
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
MLFLOW_S3_ENDPOINT_URL = os.environ.get("MLFLOW_S3_ENDPOINT_URL")

