from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, arrays_zip, col, lit, current_timestamp,
    to_timestamp, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType
)
import sys

def create_spark_session():
    return SparkSession.builder \
        .appName("WeatherTransform") \
        .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,"
                "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

def read_from_mongodb(spark, mongo_uri):
    print(f"Reading from MongoDB: {mongo_uri}")

    df_raw = spark.read.format("mongodb") \
        .option("uri", mongo_uri) \
        .option("database", "weather") \
        .option("collection", "raw_weather") \
        .load()

    count = df_raw.count()
    print(f"Read {count} documents from MongoDB")
    return df_raw

def transform_weather_data(df_raw):
    df_exploded = df_raw.select(
        col("city"),
        col("latitude"),
        col("longitude"),
        col("ingestion_timestamp"),
        col("_id").cast("string").alias("source_doc_id"),
        explode(arrays_zip(
            col("data.hourly.time"),
            col("data.hourly.temperature_2m"),
            col("data.hourly.precipitation"),
            col("data.hourly.wind_speed_10m"),
            col("data.hourly.wind_direction_10m"),
            col("data.hourly.relative_humidity_2m"),
            col("data.hourly.pressure_msl"),
            col("data.hourly.cloud_cover")
        )).alias("hourly")
    )

    df_transformed = df_exploded.select(
        col("city").alias("city_name"),
        to_timestamp(col("hourly.time")).alias("observation_time"),
        col("ingestion_timestamp").alias("ingestion_time"),
        col("hourly.temperature_2m").cast(DoubleType()).alias("temperature_2m"),
        col("hourly.precipitation").cast(DoubleType()).alias("precipitation"),
        col("hourly.wind_speed_10m").cast(DoubleType()).alias("wind_speed_10m"),
        col("hourly.wind_direction_10m").cast(IntegerType()).alias("wind_direction_10m"),
        col("hourly.relative_humidity_2m").cast(IntegerType()).alias("relative_humidity_2m"),
        col("hourly.pressure_msl").cast(DoubleType()).alias("pressure_msl"),
        col("hourly.cloud_cover").cast(IntegerType()).alias("cloud_cover"),
        col("source_doc_id")
    )

    df_transformed = df_transformed.filter(col("observation_time").isNotNull())

    count = df_transformed.count()
    print(f"Transformed into {count} hourly records")

    return df_transformed

def write_to_postgresql(df, jdbc_url, jdbc_props, table_name):
    df.write \
        .mode("append") \
        .jdbc(jdbc_url, table_name, jdbc_props)

def process_staging_to_fact(jdbc_url, jdbc_props):
    import psycopg2

    parts = jdbc_url.replace("jdbc:postgresql://", "").split("/")
    host_port = parts[0].split(":")
    host = host_port[0]
    port = int(host_port[1]) if len(host_port) > 1 else 5432
    database = parts[1] if len(parts) > 1 else "weather_db"

    conn = psycopg2.connect(
        host=host,
        port=port,
        user=jdbc_props["user"],
        password=jdbc_props["password"],
        database=database
    )

    cursor = conn.cursor()
    cursor.execute("SELECT process_staging_data()")
    rows_processed = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Processed {rows_processed} rows from staging to fact table")
    return rows_processed

def main():
    mongo_uri = "mongodb://admin:admin@mongodb:27017/weather.raw_weather?authSource=admin"
    jdbc_url = "jdbc:postgresql://postgres-weather:5432/weather_db"
    jdbc_props = {
        "user": "weather",
        "password": "weather123",
        "driver": "org.postgresql.Driver"
    }
    staging_table = "fact_weather_hourly_staging"

    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        df_raw = read_from_mongodb(spark, mongo_uri)

        if df_raw.count() == 0:
            spark.stop()
            return

        df_transformed = transform_weather_data(df_raw)

        write_to_postgresql(df_transformed, jdbc_url, jdbc_props, staging_table)

        rows_processed = process_staging_to_fact(jdbc_url, jdbc_props)
        print(f"Rows processed: {rows_processed}")

        spark.stop()

    except Exception as e:
        print(f"Error in Spark job: {str(e)}")
        raise

if __name__ == "__main__":
    main()
