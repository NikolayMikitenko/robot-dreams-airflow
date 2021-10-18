from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.postgres_operator import PostgresHook
from pathlib import Path
import csv
import os

class ExportDataFromPostgresToFileSystem(PythonOperator):
    #def __init__(self, sql: str, parameters: dict, postgres_conn_id: str, file_system_path: Path, *args, **kwargs):
    def __init__(self, postgres_conn_id: str, file_system_path: Path, *args, **kwargs):        
        super(PythonOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.file_system_path = file_system_path
        
    def execute(self, context, table: str):
        #Declare parameters
        parameters={'table': table}

        #Load script template for export data
        with open('scripts/get_table_all_data', 'r') as sql_script:
            sql = sql_script.read()
        self.log.info('Executing: %s', self.sql)

        #Execute query
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        with self.hook.get_conn() as conn:
            with conn.cursor() as cur:
                if parameters is not None:
                    self.log.info("{} with parameters {}".format(sql, parameters))
                    cur.execute(sql, parameters)
                else:
                    self.log.info(sql)
                    cur.execute(sql)
                #rows = cur.fetchall()

                #Check if folder for data exists
                os.makedirs(self.file_system_path, exist_ok=True)

                #Save data to FileSystem
                file_path = os.path.join(self.file_system_path, table + '.csv')
                with open(f'/data/{table}.csv', 'w') as f:
                    file = csv.writer(f)
                    file.writerows(cur.fetchall())
                self.log.info(f'Write data from table {table} to file {file_path}')

        for output in self.hook.conn.notices:
            self.log.info(output) 