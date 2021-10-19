from airflow.operators.python_operator import PythonOperator
#from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.postgres_operator import PostgresHook
from pathlib import Path
import csv
import os

class ExportDataFromPostgresToFileSystemOperator(PythonOperator):
    def __init__(self, postgres_conn_id: str, file_system_path: Path, table: str, *args, **kwargs):
        super(ExportDataFromPostgresToFileSystemOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.file_system_path = file_system_path
        #self.templates_dict={}
        #self.op_args=[]
        #self.op_kwargs={}
        self.table = table
        
    def execute(self, context):
        sql=f"SELECT * FROM {self.table}"
        self.log.info('Executing: %s', sql)

        #Execute query
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        with self.hook.get_conn() as conn:
            with conn.cursor() as cur:

                cur.execute(sql)
                columns = [i[0] for i in cur.description]
                #rows = cur.fetchall()

                #Check if folder for data exists
                os.makedirs(self.file_system_path, exist_ok=True)

                #Save data to FileSystem                
                file_path = os.path.join(self.file_system_path, self.table + '.csv')
                self.log.info(f'Write data from table {self.table} to file {file_path}')
                with open(file_path, 'w') as f:
                    file = csv.writer(f)
                    file.writerow(columns)
                    file.writerows(cur.fetchall())
                self.log.info(f'Data from table {self.table} successfully writed to file {file_path}')
                
        for output in self.hook.conn.notices:
            self.log.info(output)
