import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, LongType

def load_bronze_json_to_silver(project: str, *args, **kwargs):

    logging.info(f"Begin load json for project: {project} to Silver")

    raw_path = os.path.join('/', 'bronze', project, '*', '*', '*')
    clean_path = os.path.join('/', 'silver', project)

    logging.info(f"Try initialize spark session")
    spark = SparkSession.builder\
            .master('local')\
            .appName('bronze_to_silver')\
            .getOrCreate()  
    logging.info(f"Successfully initialized spark session: {spark}")    

    logging.info(f"Begin of read data for project: {project} from {raw_path} to spark dataframe") 
    json_schema = StructType()\
        .add('date', StringType())\
        .add('product_id', LongType())
    df = spark.read.schema(json_schema).json(raw_path)
    logging.info(f"End read data for project: {project} from {raw_path} to spark dataframe")
    logging.info(f"Dataframe size is: {df.count()} rows")

    logging.info(f"Begin of drop dublicates") 
    df = df.dropDuplicates()
    logging.info(f"End of drop dublicates") 
    logging.info(f"Dataframe size is: {df.count()} rows")

    logging.info(f"Begin of write data for project: {project} to Silver {clean_path}")
    df.write.parquet(clean_path, mode='overwrite')
    logging.info(f"Writed {df.count()} rows")
    logging.info(f"Data for project: {project} loaded to Silver {clean_path}")      