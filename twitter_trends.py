from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from jobs.get_twitter import run

dag = DAG('twitter_trends', description='Creating table with Twitter Trends',
          schedule_interval=None,
          start_date=datetime(2022, 9, 14),
          catchup=False)

vendas_operator = PythonOperator(task_id='create_table_twitter_task', python_callable=run, dag=dag)

vendas_operator