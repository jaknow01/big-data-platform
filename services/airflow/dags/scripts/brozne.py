from pyspark.sql import SparkSession

def ingest_bronze():
    spark = SparkSession.builder.appName("BronzeIngestion")\
        .remote("sc://spark-processor:15002")\
        .getOrCreate()

    # 1. Odczyt z Kafki
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "dbserver1.public.housing") \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. Zapis do MinIO (RAW - surowe dane)
    query = df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://datalake/checkpoints/bronze") \
        .trigger(availableNow=True) \
        .start("s3a://datalake/bronze/housing")

    query.awaitTermination()