import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import pyodbc  # Use pyodbc to execute direct SQL commands on MSSQL

def create_spark_connection():
    s_conn = None

    try:
        # Correct the paths to JAR files and add the MSSQL JDBC driver
        spark_jars = "{},{},{},{},{},{}".format(
            os.getcwd() + "/../jars/spark-sql-kafka-0-10_2.12-3.3.0.jar",
            os.getcwd() + "/../jars/spark-streaming-kafka-0-10_2.12-3.3.0.jar",
            os.getcwd() + "/../jars/mssql-jdbc-8.4.1.jre8.jar",  # MSSQL JDBC driver
            os.getcwd() + "/../jars/kafka-clients-3.3.0.jar",
            os.getcwd() + "/../jars/guava-30.1-jre.jar",
            os.getcwd() + "/../jars/scala-library-2.12.17.jar"
        )

        s_conn = SparkSession.builder \
            .config("spark.jars", spark_jars) \
            .appName('SparkDataStreaming') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', '172.20.10.12:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka DataFrame created successfully!")
        return spark_df
    except Exception as e:
        logging.error(f"Error while loading Kafka DataFrame: {e}")
        raise e

def create_selection_df_from_kafka(spark_df):
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

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    return sel

def create_table_in_mssql():
    try:
        # Connect to MSSQL using pyodbc
        connection = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};'
                                    'SERVER=172.20.10.12;'
                                    'DATABASE=Eclectics;'
                                    'UID=sa;'
                                    'PWD=Patterns2323@q;'
                                    'TrustServerCertificate=yes;')

        cursor = connection.cursor()

        # Create the table if it doesn't exist
        create_table_query = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='created_users' AND xtype='U')
        CREATE TABLE dbo.created_users (
            id UNIQUEIDENTIFIER PRIMARY KEY,
            first_name NVARCHAR(255),
            last_name NVARCHAR(255),
            gender NVARCHAR(50),
            address NVARCHAR(255),
            post_code NVARCHAR(50),
            email NVARCHAR(255),
            username NVARCHAR(255),
            registered_date NVARCHAR(50),
            phone NVARCHAR(50),
            picture NVARCHAR(255)
        );
        """

        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        connection.close()

        logging.info("Table created successfully in MSSQL!")

    except Exception as e:
        logging.error(f"Error while creating the table in MSSQL: {e}")

if __name__ == "__main__":
    # Create MSSQL table if not exists
    create_table_in_mssql()

    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)

        # JDBC URL to MSSQL
        jdbc_url = "jdbc:sqlserver://172.20.10.12:1433;databaseName=Eclectics;user=sa;password=Patterns2323@q;TrustServerCertificate=yes;"

        # Write stream to MSSQL
        streaming_query = (selection_df.writeStream
                           .format("jdbc")
                           .option("url", jdbc_url)
                           .option("dbtable", "dbo.created_users")
                           .option("checkpointLocation", "/tmp/checkpoint")
                           .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                           .start())

        streaming_query.awaitTermination()
