from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresHook
from pathlib import Path
import csv
import os

class ExportDataDumpFromPostgresToFileSystem(PythonOperator):
    def __init__(self, postgres_conn_id: str, file_system_path: Path, table: str, *args, **kwargs):
        super(PythonOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.file_system_path = file_system_path
        self.templates_dict={}
        self.op_args=[]
        self.op_kwargs={}
        self.table = table
        
    def execute(self, context):
        sql=f"SELECT * FROM {self.table}"
        self.log.info('Executing: %s', sql)

        os.makedirs(self.file_system_path, exist_ok=True)
        file_path = os.path.join(self.file_system_path, self.table + '.csv')

        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.log.info(f'Write data from table {self.table} to file {file_path}')
        self.hook.bulk_dump(self.table, file_path)