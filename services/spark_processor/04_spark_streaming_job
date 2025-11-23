from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, desc
from pyspark.sql.types import StructType, StringType, IntegerType

# --- SPARK + DELTA + MINIO ---
spark = SparkSession.builder \
    .appName("KafkaToMinIODelta") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
            "io.delta:delta-spark_2.13:4.0.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# --- SCHEMA ---
organization_schema = StructType() \
    .add("Index", IntegerType()) \
    .add("Organization Id", StringType()) \
    .add("Name", StringType()) \
    .add("Website", StringType()) \
    .add("Country", StringType()) \
    .add("Description", StringType()) \
    .add("Founded", StringType()) \
    .add("Industry", StringType()) \
    .add("Number of employees", IntegerType())

debezium_schema = StructType() \
    .add("before", StringType()) \
    .add("after", organization_schema) \
    .add("source", StructType()) \
    .add("op", StringType()) \
    .add("ts_ms", StringType()) \
    .add("transaction", StringType())


# --- READ FROM KAFKA ---
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "dbserver1.public.organizations_100") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) AS json_value") \
    .select(from_json(col("json_value"), debezium_schema).alias("data")) \
    .select("data.after.*") \
    .filter(col("Country").isNotNull())


# --- AGGREGATION ---
country_count = json_df.groupBy("Country") \
    .agg(count("*").alias("Number_of_Companies")) \
    .orderBy(desc("Number_of_Companies"))


# --- WRITE TO MINIO AS DELTA ---
output_path = "s3a://datalake/organizations_country_count"

query = country_count.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "s3a://datalake/checkpoints/organizations_country_count") \
    .start(output_path)

query.awaitTermination()
