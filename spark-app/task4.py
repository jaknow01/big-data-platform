from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaAVROReader") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
            "org.apache.spark:spark-avro_2.12:3.4.0") \
    .getOrCreate()

# Czytanie z Kafka
kafka_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribePattern", "dbserver1.public.*") \
  .option("failOnDataLoss", "false") \
  .load()

# value zawiera AVRO jako bajty – wypisujemy surowe dane
kafka_df.selectExpr("CAST(value AS STRING)") \
  .writeStream \
  .outputMode("append") \
  .format("console") \
  .option("truncate", "false") \
  .queryName("raw_messages") \
  .start()


transformed_df = kafka_df.select(
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("key").cast("string").alias("message_key"),
    col("value").cast("string").alias("message_value")
).withColumn("processing_time", current_timestamp()) \
 .withColumn("message_length", length(col("message_value"))) \
 .withColumn("topic_short", regexp_extract(col("topic"), r"dbserver1\.public\.(.+)", 1))

enriched_df = transformed_df \
    .filter(col("message_value").isNotNull()) \
    .filter(col("message_length") > 10) \
    .withColumn("hour_of_day", hour(col("timestamp"))) \
    .withColumn("is_large_message", when(col("message_length") > 1000, True).otherwise(False))

aggregated_df = enriched_df \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("topic_short")
    ) \
    .agg(
        count("*").alias("message_count"),
        avg("message_length").alias("avg_message_length"),
        max("message_length").alias("max_message_length")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("topic_short"),
        col("message_count"),
        round(col("avg_message_length"), 2).alias("avg_message_length"),
        col("max_message_length")
    )

# Query 1: Szczegółowe dane wiadomości
detail_query = enriched_df.select(
    "topic_short",
    "message_key", 
    "message_length",
    "processing_time",
    "hour_of_day",
    "is_large_message",
    "message_value"
).writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 5) \
    .trigger(processingTime='10 seconds') \
    .queryName("detailed_messages") \
    .start()

print("Detailed messages query started")

# Query 2: Agregowane statystyki
stats_query = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='30 seconds') \
    .queryName("message_statistics") \
    .start()

print("Statistics query started")

# Opcjonalne: Query 3 - Tylko duże wiadomości
large_messages_query = enriched_df \
    .filter(col("is_large_message") == True) \
    .select("topic_short", "message_length", "processing_time") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='15 seconds') \
    .queryName("large_messages") \
    .start()

print("Large messages query started")

print("=== All Spark Streaming Queries Running ===")
print("Press Ctrl+C to stop...")

try:
    # Oczekiwanie na zakończenie wszystkich query
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n=== Stopping Spark Streaming ===")
    spark.streams.active[0].stop()
    spark.stop()
    print("Spark stopped successfully")
