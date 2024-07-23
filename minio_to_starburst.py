from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark
from pyspark.sql.types import StructField, IntegerType

if __name__ == "__main__":
    """MINIO_ENDPOINT = "https://minios3.campus.clusterdiali.me"
    MINIO_ACCESS_KEY = "QiH0yo6ngB529dM7htEE"
    MINIO_SECRET_KEY = "WcMjbiUHcKXGrrJXpjC8afEc0oVCrCSzdAgwy0wl"
    BUCKET_NAME = "bi-modeling-data" """

    MINIO_ENDPOINT = "212.47.236.230:9000"
    MINIO_ACCESS_KEY = "coVKGA0YmYt0fmaBOGrz"
    MINIO_SECRET_KEY = "6umzPdeL1yRvXr7jhJbOpyHspmSSSF7J7wxh8mBk"
    BUCKET_NAME = "bi-modeling-data"

    STARBURST_URL = "jdbc:trino://195.154.69.87:8443/"
    STARBURST_USER = "starburst_service"
    STARBURST_CATALOG = "hive"
    STARBURST_SCHEMA = "bi_modeling_test_gold"
    trino_jdbc_jar = "/Users/frederic/Desktop/jars/trino-jdbc-448.jar"


    # Créer la session Spark avec les fichiers JAR ajoutés manuellement
    spark = SparkSession.builder \
        .appName("MinioToStarburst") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .config("spark.jars", trino_jdbc_jar) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation") \
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation") \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    connection_properties = {
        "user": STARBURST_USER,
        "driver": "io.trino.jdbc.TrinoDriver"
    }

    # Lire les données depuis MinIO
    df = spark.read.option("header", True).option("multiLine", "true") \
        .json(f"s3a://{BUCKET_NAME}/sales_2024-07-10-20-47-33.json")

    # Afficher les données
    print(df.dtypes)


    # Définir une fonction pour mapper les types de colonnes
    def map_spark_to_sql_type(spark_type):
        type_mapping = {
            'string': 'VARCHAR(100)'
        }
        return type_mapping.get(spark_type, spark_type)


    # Créer la chaîne de types de colonnes pour createTableColumnTypes
    column_types = ", ".join([f'{name} {map_spark_to_sql_type(dtype)}' for name, dtype in df.dtypes])

    # Afficher le mapping des types de colonnes
    print(column_types)

    # Écrire les données dans Starburst
    df.write \
        .format("jdbc") \
        .option("url", STARBURST_URL) \
        .option("dbtable", f"{STARBURST_CATALOG}.{STARBURST_SCHEMA}.test_table") \
        .option("createTableColumnTypes", column_types) \
        .mode("overwrite") \
        .options(**connection_properties) \
        .save()

