from datetime import datetime

from airflow import DAG
from common.postgres_dump_to_filesystem import ExportDataDumpFromPostgresToFileSystem
import os
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresHook

dag = DAG(
    dag_id='export_datadump_from_Postgres_to_FileSystem_dag'
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


tables_tasks = []
#tables = ['aisles', 'clients']
tables = get_tables_list(postgres_conn_id)
print(type(tables))
print(tables)




for table in tables:
    tables_tasks.append(
        ExportDataDumpFromPostgresToFileSystem(
            task_id=f'extract_table_{table}',
            dag=dag,
            postgres_conn_id=postgres_conn_id, 
            file_system_path=os.path.join('.', 'data', 'dshop_data'),
            table=table
        )
    )

start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)
start_task >> tables_tasks >> end_task
