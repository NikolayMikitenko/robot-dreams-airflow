from datetime import datetime

from airflow import DAG
#from airflow.operators.postgres_operator import PostgresOperator
#from airflow.operators.python_operator import PythonOperator
from common.postgres_to_filesystem import ExportDataFromPostgresToFileSystem
import os
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id='export_data_from_Postgres_to_FileSystem_dag'
    , start_date=datetime(2021, 1, 1)
    , schedule_interval=None
)


tables = ['aisles', 'clients']
tables_tasks = []

for table in tables:
    tables_tasks.append(
        ExportDataFromPostgresToFileSystem(
            task_id=f'extract_table_{table}',
            dag=dag,            
            postgres_conn_id= 'dsho', 
            file_system_path= os.path.join('.', 'data', 'dshop_data')
        )
    )

start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='start', dag=dag)
start_task >> tables_tasks >> end_task
