from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from jobs.calculos import run

dag = DAG('consolidado', description='Creating tables Consolidado',
          schedule_interval=None,
          start_date=datetime(2022, 9, 14),
          catchup=False)

vendas_operator = PythonOperator(task_id='create_table_consolidado_task', python_callable=run, dag=dag)

vendas_operator