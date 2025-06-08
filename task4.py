from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, desc
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("KafkaJSONSparkStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
    .getOrCreate()

# Schéma dla danych organizacji (część 'after' z Debezium)
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

# Pełny schémat Debezium
debezium_schema = StructType() \
    .add("before", StringType()) \
    .add("after", organization_schema) \
    .add("source", StructType()) \
    .add("op", StringType()) \
    .add("ts_ms", StringType()) \
    .add("transaction", StringType())

# Czytanie danych z Kafki
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "dbserver1.public.organizations_100") \
    .option("startingOffsets", "earliest") \
    .load()

# Dekodowanie JSON-a z wartości wiadomości
json_df = df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), debezium_schema).alias("data")) \
    .select("data.after.*") \
    .filter(col("Country").isNotNull())

# Agregacja według krajów z liczeniem firm
country_count = json_df.groupBy("Country") \
    .agg(count("*").alias("Number_of_Companies")) \
    .orderBy(desc("Number_of_Companies"))

# Wyświetlanie wyników
query = country_count.writeStream \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 50) \
    .outputMode("complete") \
    .trigger(processingTime='10 seconds') \
    .start()

query.awaitTermination()