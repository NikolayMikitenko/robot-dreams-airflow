from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresHook
from pathlib import Path
import csv
import os

class ExportDataDumpFromPostgresToFileSystemOperator(PythonOperator):
    def __init__(self, postgres_conn_id: str, file_system_path: Path, table: str, *args, **kwargs):
        super(ExportDataDumpFromPostgresToFileSystemOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.file_system_path = file_system_path
        #self.templates_dict={}
        #self.op_args=[]
        #self.op_kwargs={}
        self.table = table
        
    def execute(self, context):
        #Check if dir exists
        os.makedirs(self.file_system_path, exist_ok=True)
        
        #Create path
        file_path = os.path.join(self.file_system_path, self.table + '.csv')

        #Dump data
        self.log.info(f'Write data from table {self.table} to file {file_path}')
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)        
        #self.hook.bulk_dump(self.table, file_path) #DO NOT WRITE HEADERS AND USE TAB AS SEPARATOR
        self.hook.copy_expert("COPY {table} TO STDOUT DELIMITER ',' CSV HEADER;".format(table=self.table), file_path)