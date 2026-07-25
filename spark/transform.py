"""Spark transformation job: Bronze to Silver.

Reads raw sales data (Parquet) from the GCS Bronze layer, applies cleaning and enrichment,
produces both a cleaned transaction-level dataset and an aggregated dataset,
then writes both back to the GCS Silver layer as Parquet.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, upper, round as spark_round, when,
    sum as spark_sum, avg, count,
)


### 1. Define Configuration
KEY_PATH = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
BUCKET = "retailco-raw-najma"
BRONZE_PATH = f"gs://{BUCKET}/retailco/bronze/sales/sales.parquet"
SILVER_CLEAN_PATH = f"gs://{BUCKET}/retailco/silver/sales_clean"
SILVER_AGG_PATH = f"gs://{BUCKET}/retailco/silver/sales_aggregated"

# Path to the GCS connector JAR (downloaded manually to avoid Guava conflicts)
GCS_JAR_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "jars", "gcs-connector.jar"
)


### 2. Function to Create SparkSession with GCS Connector
def build_spark_session() -> SparkSession:
    """Create a SparkSession configured to read from and write to GCS.

    Uses a manually downloaded GCS connector JAR to avoid Guava classpath
    conflicts that occur when using spark.jars.packages with PySpark 3.5.x.
    """
    return SparkSession.builder \
        .appName("RetailCoBronzeToSilver") \
        .master("local[4]") \
        .config("spark.jars", GCS_JAR_PATH) \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEY_PATH) \
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .getOrCreate()


### 3. Function to Clean the Data
# - filter(amount > 0) & filter(quantity > 0)
# - standardize city names
# - new column, total_value = amount * quantity
# - new column, categorization of order size
def clean_transactions(df):
    """Clean and enrich the transaction-level sales data."""
    return df \
        .filter(col("amount") > 0) \
        .filter(col("quantity") > 0) \
        .withColumn("city", upper(col("city"))) \
        .withColumn("total_value", spark_round(col("amount") * col("quantity"), 2)) \
        .withColumn(
            "order_size",
            when(col("quantity") >= 4, "bulk")
            .when(col("quantity") >= 2, "medium")
            .otherwise("single")
        )


### 4. Function to Aggregate Data
# aggegate total revenue, average price, total quantity, and total order
# per city and per product
def aggregate_sales(df):
    """Aggregate sales by city and product for analytics."""
    return df.groupBy("city", "product").agg(
        spark_round(spark_sum("total_value"), 2).alias("total_revenue"),
        spark_round(avg("amount"), 2).alias("avg_price"),
        spark_sum("quantity").alias("total_quantity"),
        count("order_id").alias("order_count"),
    )


### 5. Function to Run End to End Pipeline
# - Build sparksession
# - Read data
# - Transform data
# - Aggregate data
# - Write data into GCS Parquet
def main() -> None:
    """Run the Bronze to Silver transformation end to end."""
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print("Reading from Bronze layer...")
    df_bronze = spark.read.parquet(BRONZE_PATH)
    print(f"Bronze row count: {df_bronze.count()}")
    df_bronze.printSchema()

    df_clean = clean_transactions(df_bronze)
    print(f"Cleaned row count: {df_clean.count()}")
    print("Cleaned preview:")
    df_clean.show(10)

    df_aggregated = aggregate_sales(df_clean)
    print("Aggregated preview:")
    df_aggregated.show(10)

    print("Writing cleaned data to Silver layer...")
    df_clean.write.mode("overwrite").parquet(SILVER_CLEAN_PATH)

    print("Writing aggregated data to Silver layer...")
    df_aggregated.write.mode("overwrite").parquet(SILVER_AGG_PATH)

    print("Silver layer is ready.")
    spark.stop()


if __name__ == "__main__":
    main()