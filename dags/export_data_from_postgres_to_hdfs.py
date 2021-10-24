from airflow import DAG
from datetime import datetime
from airflow.operators.postgres_operator import PostgresHook
from airflow.hooks.hdfs_hook import HDFSHook
from airflow.operators.python_operator import PythonOperator

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

def export_data_dump_to_hdfs():
    #import logging 
    cl = HDFSHook('local_hdfs').get_conn()
    cl.mkdir('/bronze')
    #logging.info(cl.ls(["/"]))

t1 = PythonOperator(
    task_id='db_data_to_hdfs',
    #f'extract_table_{table[0]}',
    dag=dag,
    #postgres_conn_id='dshop', 
    #file_system_path=os.path.join('.', 'data', 'dshop_data'),
    #table=table[0],
    python_callable=export_data_dump_to_hdfs    
)

#for table in tables:
#    tables_tasks.append(
#        cl = HDFSHook('local_hdfs').get_conn()
#        cl.ls(["/"])
        
        #ExportDataFromPostgresToFileSystemOperator(
        #    task_id=f'extract_table_{table[0]}',
        #    dag=dag,
        #    postgres_conn_id='dshop', 
        #    file_system_path=os.path.join('.', 'data', 'dshop_data'),
        #    table=table[0],
        #    python_callable=empty
        #)
#    )