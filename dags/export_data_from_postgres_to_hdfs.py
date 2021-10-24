from airflow import DAG
from datetime import datetime
from airflow.operators.postgres_operator import PostgresHook
#from airflow.hooks.hdfs_hook import HDFSHook
from airflow.operators.python_operator import PythonOperator
from common.postgres_to_hdfs_web import PostgresToWebHDFSOperator
import os

dag = DAG(
    dag_id='export_data_from_Postgres_to_HDFS_dag'
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

def export_data_dump_to_hdfs(table):
    hdfs_path = os.path.join('/', 'bronze', 'dshop')
    PostgresToWebHDFSOperator('dshop', 'local_webhdfs', hdfs_path, table).execute()

#t1 = PythonOperator(
#    task_id='db_data_to_hdfs',
#    dag=dag,
#    python_callable=export_data_dump_to_hdfs    
#)

for table in tables:
    tables_tasks.append(                
        PythonOperator(
            task_id=f'extract_table_{table[0]}',
            dag=dag,
            python_callable=export_data_dump_to_hdfs(table[0])
        )
    )