from pyspark.sql import SparkSession
from airflow.hooks.base_hook import BaseHook
import os
import logging

def load_postgres_to_bronze(table: str, postgres_conn_id: str):

    logging.info(f"Load table: {table} from DB: {postgres_conn_id} to Bronze")

    hdfs_path = os.path.join('/', 'bronze', postgres_conn_id, table)

    pg_conn = BaseHook.get_connection(postgres_conn_id)
    pg_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"
    pg_creds = {"user": pg_conn.login, "password": pg_conn.password}

    logging.info(f"Try connect to {pg_url} with credentials {pg_creds}")
    logging.info(f"Try initialize spark session")

    spark = SparkSession.builder\
            .config('spark.driver.extraClassPath', '/home/user/shared/postgresql-42.3.1.jar')\
            .master('local')\
            .appName('postgres_to_bronze')\
            .getOrCreate()

    logging.info(f"Successfully initialized spark session: {spark}")
    logging.info(f"Begin of read data from {table} to spark dataframe")

    df = spark.read.jdbc(pg_url, table=table, properties=pg_creds)

    logging.info(f"End read data from {table} to spark dataframe")
    logging.info(f"Dataframe shape is: {df.shape}")


    logging.info(f"Begin of write data from {table} to Bronze {hdfs_path}")
    df.write.option("header", True).csv(hdfs_path, mode='overwrite')
    logging.info(f"Writed {df.count()} rows")
    logging.info(f"Table: {table} loaded to Bronze {hdfs_path}")