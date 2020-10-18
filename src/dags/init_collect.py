from airflow import DAG
from datetime import date, timedelta, datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from lib.download import download_ppd
import pathlib
from lib.csv_import import import_gdp
from sql.init_coll import sql_init_raw,sql_gdp,sql_move_house,sql_init_wh,sql_fill_wh,sql_separate_house

DAG_DEFAULT_ARGS = {
  'owner': 'airflow',
  'depends_on_past': False,
  'retries': 4,
  'retry_delay': timedelta(minutes=1)
}

onceDag = DAG('once_collection',
          start_date=datetime(2020,10,1),
          schedule_interval='@once',
          default_args=DAG_DEFAULT_ARGS, 
          dagrun_timeout=timedelta(hours=1),
          catchup=False,
          max_active_runs=1,
          ) 

rootdir = str(pathlib.Path(__file__).parent.absolute())+"/.."

def pythonop(id,fn):
  return PythonOperator(
    task_id=id,
    provide_context=False,
    python_callable=fn,
    dag=onceDag
  )

def mysqlop(id,fn):
  return MySqlOperator(
    task_id=id,
    sql=fn,
    mysql_conn_id='mysql',
    dag=onceDag
  )

def bashop(id,fn):
  return BashOperator(
    task_id=id,
    bash_command=rootdir+'/shell/'+fn+' ',
    dag=onceDag
  )

d1 = pythonop('init_ppd', download_ppd)
d2 = bashop('download', 'download.sh')

i1 = mysqlop('create_schema', sql_init_raw)
i2 = bashop('import1', 'import.sh')
i3 = pythonop('import_gdp', import_gdp)
i4 = mysqlop('modify_gdp', sql_gdp)
i5 = mysqlop('separate_house', sql_separate_house)

t1 = mysqlop('move_house', sql_move_house(2011))
t2 = mysqlop('init_wh', sql_init_wh)
t3 = mysqlop('fill_wh', sql_fill_wh)



[d1, d2] >> i1 >> i2 >> i3 >> i4 >> t1






