from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

from sqlalchemy.sql.functions import mode
import logging

def load_postgres_bronze_to_silver(table: str, project: str, *args, **kwargs):

    logging.info(f"Load table: {table} for project: {project} to Silver")

    raw_path = os.path.join('/', 'bronze', project, table)
    clean_path = os.path.join('/', 'silver', project, table)

    logging.info(f"Try initialize spark session")
    spark = SparkSession.builder\
            .master('local')\
            .appName('bronze_to_silver')\
            .getOrCreate()  
    logging.info(f"Successfully initialized spark session: {spark}")

    logging.info(f"Begin of read data for table: {table} from {raw_path} to spark dataframe")            
    df = spark.read\
        .option('header', True)\
        .option('inferSchema', True)\
        .csv(raw_path)
    logging.info(f"End read data for table: {table} from {raw_path} to spark dataframe")
    logging.info(f"Dataframe size is: {df.count()} rows")

    logging.info(f"Begin of write data for table: {table} to Silver {clean_path}")
    df.write.parquet(clean_path, mode='overwrite')
    logging.info(f"Writed {df.count()} rows")
    logging.info(f"Table: {table} loaded to Silver {clean_path}")    

def load_bronze_dshop_orders_to_silver(*args, **kwargs):

    logging.info(f"Load table: orders for project: dshop to Silver")

    raw_path = os.path.join('/', 'bronze', 'dshop', 'orders')
    clean_path = os.path.join('/', 'silver', 'dshop', 'orders')

    logging.info(f"Try initialize spark session")
    spark = SparkSession.builder\
            .master('local')\
            .appName('bronze_to_silver')\
            .getOrCreate() 
    logging.info(f"Successfully initialized spark session: {spark}")

    logging.info(f"Begin of read data for table: orders from {raw_path} to spark dataframe")             
    df = spark.read\
        .option('header', True)\
        .option('inferSchema', True)\
        .csv(raw_path)
    logging.info(f"End read data for table: orders from {raw_path} to spark dataframe")
    logging.info(f"Dataframe size is: {df.count()} rows")

    logging.info(f"Begin of aggregating data")
    df = df.groupby(F.col("order_id"), F.col("product_id"), F.col("client_id"), F.col("store_id"), F.col("order_date"))\
        .agg(F.sum(F.col('quantity')))
    logging.info(f"End of aggregating data")

    logging.info(f"Begin of write data for table: orders to Silver {clean_path}")            
    df.write.parquet(clean_path, mode='overwrite')
    logging.info(f"Writed {df.count()} rows")
    logging.info(f"Table: orders loaded to Silver {clean_path}")    
