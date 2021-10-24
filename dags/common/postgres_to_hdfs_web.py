#from airflow.hooks.hdfs_hook import HDFSHook
from pathlib import Path
import os
from airflow.operators.postgres_operator import PostgresHook
import logging

from airflow.hooks.webhdfs_hook import WebHDFSHook

WebHDFSHook()

class PostgresToWebHDFSOperator():
    def __init__(self, postgres_conn_id: str, hdfs_conn_id: str, hdfs_path: Path, table: str, *args, **kwargs):
        self.postgres_conn_id = postgres_conn_id
        self.hdfs_conn_id = hdfs_conn_id
        self.hdfs_path = hdfs_path
        self.log = logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)

    def execute(self):
        with WebHDFSHook('hdfs_conn_id').get_conn() as whdfs_client:
            #Check if folder exists and create
            whdfs_client.makedirs('hdfs_path')
            #Create file path
            file_path = os.path.join(self.hdfs_path, self.table + '.csv')
            #Create hook fpr DB
            postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

            self.log.info(f'Begin write data from table {self.table} to file {file_path}')            
            with whdfs_client.write(file_path) as csv:
                postgres_hook.copy_expert("COPY {table} TO STDOUT DELIMITER ',' CSV HEADER;".format(table=self.table), csv)


        #with HDFSHook('hdfs_conn_id').get_conn() as hdfs_client:
            #Check if folder exists and create
            #hdfs_client.mkdir([self.hdfs_path], create_parent=True)
            #Create path
            #file_path = os.path.join(self.hdfs_path, self.table + '.csv')
            #
            #self.log.info(f'Write data from table {self.table} to file {file_path}')
            #self.postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)  

            #with 
            #self.postgres_hook.copy_expert("COPY {table} TO STDOUT DELIMITER ',' CSV HEADER;".format(table=self.table), file_path)