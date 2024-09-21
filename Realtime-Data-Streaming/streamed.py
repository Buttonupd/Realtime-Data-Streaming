import logging
import os
import time  # For periodic batch loading
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_spark_connection():
    s_conn = None
    try:
        # Use Maven coordinates for the required packages
        spark_packages = ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",  # Kafka connector
            "com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8",     # MSSQL JDBC driver
            "org.apache.kafka:kafka-clients:3.3.0",              # Kafka clients
            "com.google.guava:guava:30.1-jre",                   # Guava
            "org.scala-lang:scala-library:2.12.17"               # Scala library
        ])

        # Create Spark session with packages and necessary configuration
        s_conn = SparkSession.builder \
            .config("spark.jars.packages", spark_packages) \
            .config("spark.hadoop.io.native.lib.available", "false") \
            .appName('SparkDataStreaming') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception: {e}")
    return s_conn

def connect_to_kafka(spark_conn):
    try:
        # Read streaming data from Kafka topic
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka DataFrame created successfully!")
        return spark_df
    except Exception as e:
        logging.error(f"Error while loading Kafka DataFrame: {e}")
        raise e

def create_selection_df_from_kafka(spark_df):
    # Define the schema for the incoming data
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    # Parse the JSON messages and extract the fields
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    return sel

def load_to_mssql():
    try:
        # Create Spark session for batch processing
        spark = SparkSession.builder \
            .appName("LoadParquetToMSSQL") \
            .getOrCreate()

        # JDBC URL to connect to MSSQL
        jdbc_url = "jdbc:sqlserver://172.20.10.12:1433;databaseName=Eclectics;user=sa;password=Patterns2323@q;TrustServerCertificate=yes;"

        # Read data from Parquet files
        parquet_df = spark.read.parquet("/tmp/parquet_output")

        # Write the data to MSSQL in batch mode
        parquet_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "dbo.created_users") \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .mode("append") \
            .save()

        print("Data loaded to MSSQL successfully")
    except Exception as e:
        logging.error(f"Error during batch load to MSSQL: {e}")

if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connect to Kafka and read the stream
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)

        # Write streaming data to Parquet files
        streaming_query = (selection_df.writeStream
                           .format("parquet")
                           .option("checkpointLocation", "D:/tmp/checkpoint")
                           .option("path", "D:/tmp/parquet_output")  # Path to save Parquet files
                           .start())

        # Periodically load Parquet files into MSSQL
        while True:
            time.sleep(60)  # Wait for 1 minute (adjust as needed)
            load_to_mssql()

        streaming_query.awaitTermination()
