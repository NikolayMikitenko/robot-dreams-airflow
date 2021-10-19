from datetime import datetime

from airflow import DAG
from common.postgres_to_filesystem import ExportDataFromPostgresToFileSystem
import os
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresHook

dag = DAG(
    dag_id='export_data_from_Postgres_to_FileSystem_dag'
    , start_date=datetime(2021, 1, 1)
    , schedule_interval=None
)

postgres_conn_id='dshop'

def get_tables_list(postgres_conn_id: str):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';")
            return cur.fetchall()

tables = get_tables_list(postgres_conn_id)

tables_tasks = []

for table in tables:
    tables_tasks.append(
        ExportDataFromPostgresToFileSystem(
            task_id=f'extract_table_{table[0]}',
            dag=dag,
            postgres_conn_id='dshop', 
            file_system_path=os.path.join('.', 'data', 'dshop_data'),
            table=table[0]
        )
    )

start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)
start_task >> tables_tasks >> end_task
