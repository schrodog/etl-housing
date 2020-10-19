from airflow import DAG
from datetime import date, timedelta, datetime
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pathlib, os
from sql.routine_tx import sql_fill_wh,sql_move_house,sql_update_agg

DAG_DEFAULT_ARGS = {
  'owner': 'airflow',
  'depends_on_past': False,
  'retries': 4,
  'retry_delay': timedelta(minutes=1)
}

monthlyDag = DAG('routine_collection',
          start_date=datetime(2020,10,1),
          schedule_interval='@monthly',
          default_args=DAG_DEFAULT_ARGS, 
          dagrun_timeout=timedelta(hours=1),
          catchup=False) 

rootdir = str(pathlib.Path(__file__).parent.absolute())+"/.."
datadir = rootdir+"/../data"

def pythonop(id,fn):
  return PythonOperator(
    task_id=id,
    provide_context=False,
    python_callable=fn,
    dag=monthlyDag
  )

def mysqlop(id,fn):
  return MySqlOperator(
    task_id=id,
    sql=fn,
    mysql_conn_id='mysql',
    dag=monthlyDag
  )

def bashop(id,fn):
  return BashOperator(
    task_id=id,
    bash_command=rootdir+'/shell/'+fn+' ',
    dag=monthlyDag
  )


d1 = bashop('download', 'monthly_download.sh')
d2 = bashop('import', 'monthly_import.sh')

s1 = mysqlop('move_old', sql_move_house(True))
s2 = mysqlop('fill_old', sql_fill_wh(True))
s3 = mysqlop('move_new', sql_move_house(False))
s4 = mysqlop('fill_new', sql_fill_wh(False))
s5 = mysqlop('update_agg', sql_update_agg)

d1 >> d2 >> s1 >> s2 >> s3 >> s4 >> s5






