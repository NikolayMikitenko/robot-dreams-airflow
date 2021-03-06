#from airflow.hooks.hdfs_hook import HDFSHook
from pathlib import Path
import os
from airflow.operators.postgres_operator import PostgresHook
import logging

from airflow.hooks.webhdfs_hook import WebHDFSHook

class PostgresToWebHDFSOperator():
    def __init__(self, postgres_conn_id: str, hdfs_conn_id: str, hdfs_path: Path, table: str, *args, **kwargs):
        self.postgres_conn_id = postgres_conn_id
        self.hdfs_conn_id = hdfs_conn_id
        self.hdfs_path = hdfs_path
        self.table = table
        self.log = logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)

    def execute(self):
        whdfs_client = WebHDFSHook(self.hdfs_conn_id).get_conn()
        #Check if folder exists and create
        whdfs_client.makedirs(self.hdfs_path)
        #Create file path
        file_path = os.path.join(self.hdfs_path, self.table + '.csv')
        #Create hook fpr DB
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        self.log.info(f'Begin write data from table {self.table} to file {file_path}')
        with whdfs_client.write(file_path, overwrite=True) as csv:
            with postgres_hook.get_conn().cursor() as cur:
                cur.copy_expert("COPY {table} TO STDOUT DELIMITER ',' CSV HEADER;".format(table=self.table), csv)
        self.log.info(f'End write data from table {self.table} to file {file_path}')