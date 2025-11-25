from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, desc
from pyspark.sql.types import StructType, StringType, IntegerType

# --- SPARK + DELTA + MINIO ---
# UWAGA: Pakiety są już podane w CMD w Dockerfile, ale jeśli definiujesz je tutaj,
# muszą być zgodne (Scala 2.12 dla Spark 3.4.1, Delta 2.4.0).
spark = SparkSession.builder \
    .appName("KafkaToMinIODelta") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "io.delta:delta-core_2.12:2.4.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- SCHEMA ---
# ZMIANA: Nazwy kolumn na małe litery (zgodnie z Postgresem/Debezium)
organization_schema = StructType() \
    .add("index", IntegerType()) \
    .add("organization_id", StringType()) \
    .add("name", StringType()) \
    .add("website", StringType()) \
    .add("country", StringType()) \
    .add("description", StringType()) \
    .add("founded", StringType()) \
    .add("industry", StringType()) \
    .add("number_of_employees", IntegerType())

# Debezium envelope
debezium_schema = StructType() \
    .add("before", organization_schema) \
    .add("after", organization_schema) \
    .add("source", StringType()) \
    .add("op", StringType()) \
    .add("ts_ms", StringType()) \
    .add("transaction", StringType())


# --- READ FROM KAFKA ---
# ZMIANA: Port 9092 (wewnętrzny) i nazwa tematu zgodna z docker-compose
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "dbserver1.public.organizations_100") \
    .option("startingOffsets", "earliest") \
    .load()

# Parsowanie JSON
json_df = df.selectExpr("CAST(value AS STRING) AS json_value") \
    .select(from_json(col("json_value"), debezium_schema).alias("data")) \
    .select("data.after.*") \
    .filter(col("country").isNotNull()) # Zmiana na małą literę


# --- AGGREGATION ---
# Zmiana na małe litery w grupowaniu
country_count = json_df.groupBy("country") \
    .agg(count("*").alias("number_of_companies")) \
    .orderBy(desc("number_of_companies"))


# --- WRITE TO MINIO AS DELTA ---
# Upewnij się, że bucket 'datalake' istnieje w Minio (przez GUI lub skrypt inicjalizacyjny)
output_path = "s3a://datalake/organizations_country_count"

query = country_count.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "s3a://datalake/checkpoints/organizations_country_count") \
    .start(output_path)

query.awaitTermination()