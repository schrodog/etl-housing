from airflow import DAG
from datetime import date, timedelta, datetime
from airflow.operators.mysql_operator import MySqlOperator
# from init_collect import onceDag

# DAG_DEFAULT_ARGS = {
#   'owner': 'airflow',
#   'depends_on_past': False,
#   'retries': 4,
#   'retry_delay': timedelta(minutes=1)
# }

# dailyDag = DAG('routine_collection',
#           start_date=datetime(2020,10,1),
#           schedule_interval='@daily',
#           default_args=DAG_DEFAULT_ARGS, 
#           dagrun_timeout=timedelta(hours=1),
#           catchup=False,
#           template_searchpath='sql2') 









