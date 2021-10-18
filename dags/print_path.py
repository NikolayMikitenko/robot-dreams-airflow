from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import sys

dag = DAG(
    dag_id='print_path_dag'
    , start_date=datetime(2021, 1, 1)
    , schedule_interval=None
)

def py_func():
    print(sys.path)

t1 = PythonOperator(
    task_id = 'print',
    dag=dag,
    python_callable=py_func,
    provide_context=True
)

