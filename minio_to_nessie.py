import os
from datetime import datetime

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from minio import Minio
from utils.minio_utils import MinioUtils

dotenv_path = os.path.join(os.path.dirname(__file__), 'config', '.env')
load_dotenv(dotenv_path)

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET')

NESSIE_CATALOG_NAME = os.getenv('NESSIE_CATALOG_NAME')
NESSIE_BRANCH_NAME = os.getenv('NESSIE_BRANCH_NAME')
NESSIE_NAMESPACE = os.getenv('NESSIE_NAMESPACE')
NESSIE_TABLE_NAME = 'sales'
NESSIE_URL = os.getenv('NESSIE_URL')
NESSIE_WAREHOUSE = os.getenv('NESSIE_WAREHOUSE')

jars = [
    "jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar",
    "jars/nessie-spark-extensions-3.5_2.12-0.92.0.jar",
    "jars/aws-java-sdk-core-1.12.757.jar",
    "jars/aws-java-sdk-s3-1.12.757.jar",
]


def create_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("Iceberg with Nessie and MinIO") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.jars", ",".join(jars)) \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-common:3.3.1,"
                                       "software.amazon.awssdk:s3:2.26.0,software.amazon.awssdk:aws-core:2.26.0,"
                                       "software.amazon.awssdk:sdk-core:2.26.0,com.google.guava:guava:31.0-jre") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.warehouse", NESSIE_WAREHOUSE) \
        .config("spark.sql.catalog.nessie.uri", NESSIE_URL) \
        .config("spark.sql.catalog.nessie.ref", NESSIE_BRANCH_NAME) \
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation") \
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    return spark


if __name__ == "__main__":
    spark = create_spark_session()
    my_minio = MinioUtils(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

    sales_data = my_minio.read_data_from_minio(spark, MINIO_BUCKET, 'sales')
    # On va ajouter la colonne loaded_at qui va etre la date actuelle
    loaded_at_col = from_utc_timestamp(lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')).cast("timestamp"), "UTC+1")
    sales_data = sales_data.withColumn("loaded_at", loaded_at_col)

    spark.sql(f"USE nessie;")
    sales_data.write \
        .format("iceberg") \
        .mode("append") \
        .save(f"{NESSIE_NAMESPACE}.{NESSIE_TABLE_NAME}")

    spark.stop()