import logging 
from datetime import datetime 
from cassandra.auth import PlainTextAuthProvider 
from cassandra.cluster import Cluster 
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col 
# from pyspark import SparkContext


def create_keyspace(session):
    pass 

def create_table(session):
    pass

def insert_data(session, **kwargs):
    pass 

def create_spark_connection():
    s_conn=None
    try:
        s_conn = SparkSession.builder.appName('SparkDataStreaming').config(
            'spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,'
                                    'org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1'

        ).getOrCreate()
        s_conn.SparkContext.setLogLevel("ERROR")
        logging.info("success")
    except Exception as e:
        logging.error('error:', e)

    return s_conn
def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f'Failed to cluster with {e}')

    return None


def create_sql_connection():
    pass
if __name__ == '__main__':
    spark_conn = create_spark_connection()

    # if spark_conn is not None:
    #     session = create_cassandra_connection()

    #     if session is not None:
    #         create_keyspace(session=session)
    #         create_table(session=session)