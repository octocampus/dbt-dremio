from typing import Optional
import os
import requests
from utils.minio_utils import MinioUtils

from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), 'config', '.env')
load_dotenv(dotenv_path)

# Configurations
MOKAROO_API_ENDPOINT = os.getenv('MOKAROO_API_ENDPOINT')
MOKAROO_UPLOAD_API_ENDPOINT = os.getenv('MOKAROO_UPLOAD_API_ENDPOINT')
MOKAROO_API_KEY = os.getenv('MOKAROO_API_KEY')

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET')


def fetch_data_from_mockaroo(format: str, schema: str):
    params = {
        'key': MOKAROO_API_KEY,
        'schema': schema,
    }
    response = requests.get(MOKAROO_API_ENDPOINT + format, params=params)
    print("Data fetched successfully")
    if response.status_code == 200:
        if format == 'json':
            return response.json()
        elif format == 'csv':
            return response.text
        else:
            raise Exception(f"Unsupported format: {format}")
    else:
        print(f"Response content: {response.text}")
        raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")


if __name__ == "__main__":
    my_minio = MinioUtils(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
    try:
        # my_minio.delete_all_data_from_minio(MINIO_BUCKET, 'sales')
        data = fetch_data_from_mockaroo('json', 'sales')
        my_minio.save_to_minio(data, MINIO_BUCKET, 'sales')
    except Exception as e:
        print(f"An error occurred: {e}")
