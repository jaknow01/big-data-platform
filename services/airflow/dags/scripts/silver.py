from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when, regexp_replace, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, BooleanType, TimestampType, LongType

housing_schema = StructType() \
    .add("Index", IntegerType()) \
    .add("price", IntegerType()) \
    .add("area", IntegerType()) \
    .add("bedrooms", IntegerType()) \
    .add("bathrooms", IntegerType()) \
    .add("stories", IntegerType()) \
    .add("mainroad", StringType()) \
    .add("guestroom", StringType()) \
    .add("basement", StringType()) \
    .add("hotwaterheating", StringType()) \
    .add("airconditioning", StringType()) \
    .add("parking", IntegerType()) \
    .add("prefarea", StringType()) \
    .add("furnishingstatus", StringType())

def transform_silver():

    spark = SparkSession.builder.appName("SilverTransformation")\
        .remote("sc://spark-processor:15002") \
        .getOrCreate()
    
    debezium_schema = StructType().add("after", housing_schema)

    # 1. Odczyt z warstwy BRONZE (Delta)
    bronze_df = spark.read.format("delta").load("s3a://datalake/bronze/housing")

    parsed_df = bronze_df \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), debezium_schema).alias("data")) \
        .select("data.after.*")

    # 2. Transformacja - czyszczenie i normalizacja
    silver_df = parsed_df \
        .filter(col("price").between(1750000, 13300000)) \
        .filter(col("area").between(1650, 16200)) \
        .filter(col("bedrooms").between(1, 6)) \
        .filter(col("bathrooms").between(1, 4)) \
        .filter(col("stories").between(1, 4)) \
        .filter(col("parking").between(0, 3)) \
        .dropna(how="any") \
        .withColumn("mainroad", when(col("mainroad") == "yes", True).otherwise(False).cast(BooleanType())) \
        .withColumn("guestroom", when(col("guestroom") == "yes", True).otherwise(False).cast(BooleanType())) \
        .withColumn("basement", when(col("basement") == "yes", True).otherwise(False).cast(BooleanType())) \
        .withColumn("hotwaterheating", when(col("hotwaterheating") == "yes", True).otherwise(False).cast(BooleanType())) \
        .withColumn("airconditioning", when(col("airconditioning") == "yes", True).otherwise(False).cast(BooleanType())) \
        .withColumn("prefarea", when(col("prefarea") == "yes", True).otherwise(False).cast(BooleanType())) \
        .withColumn("furnishingstatus", regexp_replace(col("furnishingstatus"), "-", "_").cast(StringType())) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .dropDuplicates(["Index"])

    print(f"Bronze records: {parsed_df.count()}, Silver records (after dropping NULLs): {silver_df.count()}")

    # 3. Zapis do warstwy SILVER
    silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save("s3a://datalake/silver/housing")