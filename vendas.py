from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from jobs.file_to_db import run

dag = DAG('vendas', description='Creating table Vendas',
          schedule_interval=None,
          start_date=datetime(2022, 9, 14),
          catchup=False)

vendas_operator = PythonOperator(task_id='create_table_vendas_task', python_callable=run, dag=dag)

vendas_operator