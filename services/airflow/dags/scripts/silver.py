from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

# Schema dla danych Housing (zgodnie z CSV)
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

    # 2. Transformacja - usunięcie wierszy z NULL w dowolnej kolumnie
    silver_df = parsed_df.dropna(how="any")

    print(f"Bronze records: {parsed_df.count()}, Silver records (after dropping NULLs): {silver_df.count()}")

    # 3. Zapis do warstwy SILVER
    silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save("s3a://datalake/silver/housing")