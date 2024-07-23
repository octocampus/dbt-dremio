from typing import Tuple, Optional, Any

from minio import Minio
import json
from datetime import datetime
from io import BytesIO

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, from_utc_timestamp


def get_latest_file(minio_client: Minio, bucket: str, data_file_prefix: str) -> tuple[Any, Any]:
    objects = minio_client.list_objects(bucket, prefix=f'{data_file_prefix}_', recursive=True)
    latest_file = None
    for obj in objects:
        if obj.object_name.startswith(f'{data_file_prefix}_') and obj.object_name.endswith('.json'):
            if latest_file is None or obj.last_modified > latest_file.last_modified:
                latest_file = obj
    if latest_file:
        return latest_file.object_name, latest_file.last_modified
    else:
        raise Exception("No sales file found in Minio bucket")


class MinioUtils:
    def __init__(self, minio_endpoint, minio_access_key, minio_secret_key):
        self.MINIO_ENDPOINT = minio_endpoint
        self.MINIO_ACCESS_KEY = minio_access_key
        self.MINIO_SECRET_KEY = minio_secret_key

    def read_data_from_minio(self, session: SparkSession, bucket_path: str, data_file_prefix: str) -> DataFrame:
        minio_client = Minio(
            endpoint=self.MINIO_ENDPOINT,
            access_key=self.MINIO_ACCESS_KEY,
            secret_key=self.MINIO_SECRET_KEY,
            secure=False
        )
        latest_file_name, last_modified = get_latest_file(minio_client, bucket_path, data_file_prefix)
        data = session.read.option("header", True).option("multiLine", "true") \
            .json(f"s3a://{bucket_path}/{latest_file_name}")
        received_at_col = from_utc_timestamp(
            lit(datetime.utcfromtimestamp(last_modified.timestamp()).strftime('%Y-%m-%d %H:%M:%S')) \
                .cast("timestamp"), "UTC+1")
        data = data.withColumn("received_at", received_at_col)
        return data

    def save_to_minio(self, data, bucket_name, data_file_prefix):
        minio_client = Minio(
            endpoint=self.MINIO_ENDPOINT,
            access_key=self.MINIO_ACCESS_KEY,
            secret_key=self.MINIO_SECRET_KEY,
            secure=False
        )

        try:
            json_bytes = json.dumps(data).encode('utf-8')
            json_stream = BytesIO(json_bytes)

            minio_client.put_object(
                bucket_name=bucket_name,
                object_name=f"{data_file_prefix}_{datetime.now().strftime('%Y-%m-%d')}.json",
                data=json_stream,
                length=len(json_bytes),
                content_type='application/json'
            )
            print(f"{data_file_prefix}_{datetime.now().strftime('%Y-%m-%d')}.json saved to MinIO successfully")
        except Exception as exp:
            raise Exception(f"Failed to save {data_file_prefix}_{datetime.now().strftime('%Y-%m-%d')}.json "
                            f"to MinIO: {exp}")

    def delete_last_data_from_minio(self, bucket_name, data_file_prefix):
        minio_client = Minio(
            endpoint=self.MINIO_ENDPOINT,
            access_key=self.MINIO_ACCESS_KEY,
            secret_key=self.MINIO_SECRET_KEY,
            secure=False
        )

        try:
            last_file, last_modified = get_latest_file(minio_client, bucket_name, data_file_prefix)
            minio_client.remove_object(bucket_name, last_file)
            print(f"{data_file_prefix} deleted from MinIO successfully")
        except Exception as exp:
            raise Exception(f"Failed to delete {data_file_prefix} from MinIO: {exp}")

    def delete_all_data_from_minio(self, bucket_name: str, data_file_prefix: str):
        minio_client = Minio(
            endpoint=self.MINIO_ENDPOINT,
            access_key=self.MINIO_ACCESS_KEY,
            secret_key=self.MINIO_SECRET_KEY,
            secure=False
        )

        try:
            objects = minio_client.list_objects(bucket_name, prefix=f'{data_file_prefix}_', recursive=True)
            for obj in objects:
                minio_client.remove_object(bucket_name, obj.object_name)
            print(f"All data deleted from MinIO successfully")
        except Exception as exp:
            raise Exception(f"Failed to delete all data from MinIO: {exp}")
