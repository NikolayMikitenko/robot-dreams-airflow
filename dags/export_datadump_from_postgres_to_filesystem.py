from datetime import datetime

from airflow import DAG
from common.postgres_dump_to_filesystem import ExportDataDumpFromPostgresToFileSystem
import os
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

dag = DAG(
    dag_id='export_datadump_from_Postgres_to_FileSystem_dag'
    , start_date=datetime(2021, 1, 1)
    , schedule_interval=None
)

tables_tasks = []
tables = ['aisles', 'clients']


for table in tables:
    tables_tasks.append(
        ExportDataDumpFromPostgresToFileSystem(
            task_id=f'extract_table_{table}',
            dag=dag,
            postgres_conn_id='dshop', 
            file_system_path=os.path.join('.', 'data', 'dshop_data'),
            table=table
        )
    )

start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)
start_task >> tables_tasks >> end_task
