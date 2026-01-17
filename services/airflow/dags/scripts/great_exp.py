import great_expectations as gx
import great_expectations.expectations as gxe
from pyspark.sql import SparkSession
import sys
import pandas as pd

def validate_data():

    # inicjalizacja Spark Connect
    spark = SparkSession.builder \
        .appName("SilverValidation") \
        .remote("sc://spark-processor:15002") \
        .getOrCreate()

    print("--- 1. Pobieranie danych (Spark -> Pandas) ---")
    try:
        # pobieramy dane z Silver
        df_remote = spark.read.format("delta").load("s3a://datalake/silver/housing")
        
        df_pandas = df_remote.toPandas()
        
        print(f"DEBUG: Pobranno {len(df_pandas)} wierszy do walidacji.")
        
        if len(df_pandas) == 0:
            print("CRITICAL: Tabela jest pusta! Przerywam walidację.")
            sys.exit(1)

    except Exception as e:
        print(f"CRITICAL: Błąd pobierania danych: {e}")
        sys.exit(1)

    context = gx.get_context()
    data_source_name = "pandas_silver_source"

    try:
        context.data_sources.delete(data_source_name)
    except Exception:
        pass

    data_source = context.data_sources.add_pandas(name=data_source_name)
    asset_name = "silver_housing_asset"
    data_asset = data_source.add_dataframe_asset(name=asset_name)
    
    batch_definition = data_asset.add_batch_definition_whole_dataframe("silver_full_batch")

    suite_name = "silver_quality_suite"
    try:
        suite = context.suites.get(suite_name)
        suite.expectations = []
    except Exception:
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    # == Expectations
    expectations = [
        # A. Wymogi dotyczące liczby wierszy (orientacyjne)
        gxe.ExpectTableRowCountToBeBetween(min_value=400, max_value=600),

        # B. Unikalność (zgodnie z .dropDuplicates(["Index"]))
        gxe.ExpectColumnValuesToBeUnique(column="Index"),

        # C. Zakresy wartości (zgodnie z .filter(...between...))
        gxe.ExpectColumnValuesToBeBetween(column="price", min_value=1750000, max_value=13300000),
        gxe.ExpectColumnValuesToBeBetween(column="area", min_value=1650, max_value=16200),
        gxe.ExpectColumnValuesToBeBetween(column="bedrooms", min_value=1, max_value=6),
        gxe.ExpectColumnValuesToBeBetween(column="bathrooms", min_value=1, max_value=4),
        gxe.ExpectColumnValuesToBeBetween(column="stories", min_value=1, max_value=4),
        gxe.ExpectColumnValuesToBeBetween(column="parking", min_value=0, max_value=3),

        # D. Wartości Boolean (zgodnie z .withColumn(..., when(..., True).otherwise(False)))
        gxe.ExpectColumnValuesToBeInSet(column="mainroad", value_set=[True, False]),
        gxe.ExpectColumnValuesToBeInSet(column="guestroom", value_set=[True, False]),
        gxe.ExpectColumnValuesToBeInSet(column="basement", value_set=[True, False]),
        gxe.ExpectColumnValuesToBeInSet(column="hotwaterheating", value_set=[True, False]),
        gxe.ExpectColumnValuesToBeInSet(column="airconditioning", value_set=[True, False]),
        gxe.ExpectColumnValuesToBeInSet(column="prefarea", value_set=[True, False]),

        # E. Status umeblowania (zgodnie z regexp_replace "-", "_")
        gxe.ExpectColumnValuesToBeInSet(
            column="furnishingstatus", 
            value_set=["furnished", "semi_furnished", "unfurnished"]
        ),

        # F. Not Null (zgodnie z .dropna(how="any"))
        gxe.ExpectColumnValuesToNotBeNull(column="Index"),
        gxe.ExpectColumnValuesToNotBeNull(column="price"),
        gxe.ExpectColumnValuesToNotBeNull(column="area"),
        gxe.ExpectColumnValuesToNotBeNull(column="bedrooms"),
        gxe.ExpectColumnValuesToNotBeNull(column="bathrooms"),
        gxe.ExpectColumnValuesToNotBeNull(column="stories"),
        gxe.ExpectColumnValuesToNotBeNull(column="mainroad"),
        gxe.ExpectColumnValuesToNotBeNull(column="guestroom"),
        gxe.ExpectColumnValuesToNotBeNull(column="basement"),
        gxe.ExpectColumnValuesToNotBeNull(column="hotwaterheating"),
        gxe.ExpectColumnValuesToNotBeNull(column="airconditioning"),
        gxe.ExpectColumnValuesToNotBeNull(column="parking"),
        gxe.ExpectColumnValuesToNotBeNull(column="prefarea"),
        gxe.ExpectColumnValuesToNotBeNull(column="furnishingstatus"),
        gxe.ExpectColumnValuesToNotBeNull(column="ingestion_timestamp")
    ]

    for expectation in expectations:
        suite.add_expectation(expectation)

    # 4. Definicja Walidacji
    val_def_name = "silver_val_def"
    try:
        context.validation_definitions.delete(val_def_name)
    except Exception:
        pass

    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(name=val_def_name, data=batch_definition, suite=suite)
    )

    # 5. Checkpoint
    checkpoint_name = "silver_checkpoint"
    try:
        context.checkpoints.delete(checkpoint_name)
    except Exception:
        pass

    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name=checkpoint_name,
            validation_definitions=[val_def],
            result_format={"result_format": "COMPLETE"}
        )
    )

    print("--- 2. Uruchamianie walidacji ---")
    
    results = checkpoint.run(batch_parameters={"dataframe": df_pandas})

    context.build_data_docs()
    print(f"Raport HTML: {context.get_docs_sites_urls()}")

    if not results.success:
        print("BŁĄD JAKOŚCI DANYCH! Znaleziono niespełnione reguły:")
        
        for res in results.run_results.values():
            for validation_result in res["validation_result"]["results"]:
                if not validation_result["success"]:
                    config = validation_result['expectation_config']
                    col = config['kwargs'].get('column', 'Table-Level')
                    exp_type = config['expectation_type']
                    print(f" [FAIL] Kolumna: {col} | Reguła: {exp_type}")
                    
        sys.exit(1)
    else:
        print("SUKCES: Wszystkie reguły z warstwy Silver zostały spełnione.")

if __name__ == "__main__":
    validate_data()