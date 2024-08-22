import os
from datetime import datetime

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, from_utc_timestamp, to_timestamp
from minio import Minio
from utils.minio_utils import MinioUtils
import ibis

dotenv_path = os.path.join(os.path.dirname(__file__), 'config', '.env')
load_dotenv(dotenv_path)

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET')

STARBURST_HOST = "195.154.74.227"
STARBURST_PORT = 8443
STARBURST_USER = "starburst_service"
STARBURST_CATALOG = "iceberg_minio_2"
STARBURST_SCHEMA = "bi_modeling_bronze"

# JAR for the Starburst JDBC driver
jars = [
    "jars/trino-jdbc-451.jar"
]


def create_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("Spark with Starburst") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "logs") \
        .config("spark.jars", ",".join(jars)) \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-common:3.3.1,") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.debug.maxToStringFields", "1000") \
        .getOrCreate()

    return spark


if __name__ == "__main__":
    con = ibis.trino.connect(
        user=STARBURST_USER,
        host=STARBURST_HOST,
        port=STARBURST_PORT,
        database=STARBURST_CATALOG,
        schema=STARBURST_SCHEMA,
    )

    spark = create_spark_session()

    # Load data from minio
    my_minio = MinioUtils(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
    sales_data = my_minio.read_data_from_minio(spark, MINIO_BUCKET, 'sales')
    # On va ajouter la colonne loaded_at qui va etre la date actuelle
    sales_data = sales_data.withColumn("transaction_date", to_timestamp("transaction_date"))
    loaded_at_col = from_utc_timestamp(lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')).cast("timestamp"), "UTC+1")
    sales_data = sales_data.withColumn("loaded_at", loaded_at_col)
    sales_data_df = sales_data.toPandas()
    # On va juste garder pour le moment 300 lignes
    sales_data_df = sales_data_df.head(150)

    print("Data loaded from Minio successfully")
    # Write data to Starburst
    print("Writing data to Starburst")
    con.insert('sales', sales_data_df, database=f"{STARBURST_CATALOG}.{STARBURST_SCHEMA}", overwrite=False)
    print("Data loaded to Starburst successfully")

