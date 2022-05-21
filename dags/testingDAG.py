from datetime import datetime
from email.policy import default
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import sys
# sys.path.append('/home/indra/project/ETL-basic/connections')

# from insert import Insert

# i=Insert()
default_arg ={
    'owner' : 'indra',
    'depend_on_past':False,
    'start_date':datetime(2022,5,21)
}
with DAG(
    dag_id='testingDAG',
    schedule_interval='@daily',
    default_args=default_arg
) as dag:
    
    start= DummyOperator (
        task_id="start"
    )
    
    # def just_function():
    #     print("hello world")
        
    # run_etl= PythonOperator(
    #     task_id='test_call_function',
    #     python_callable=just_function,
    #     dag=dag
    # )
    
    # raw_data_to_mysql= PythonOperator(
    #     task_id="raw_data_to_mysql",
    #     python_callable=i.insert_raw_data_to_mysql,
    #     dag=dag
    # )
    raw_videos_to_mysql=BashOperator(
        task_id="raw_videos_to_mysql",
        bash_command='/home/indra/project/ETL-basic/venv/bin/python /home/indra/project/ETL-basic/ingest_raw_videos.py',
        dag=dag
    )
    raw_categories_to_mysql=BashOperator(
        task_id="raw_categories_to_mysql",
        bash_command='/home/indra/project/ETL-basic/venv/bin/python /home/indra/project/ETL-basic/ingest_raw_category.py',        
        dag=dag
    )
    
    dim_to_dwh=BashOperator(
        task_id='dim_to_dwh',
        bash_command='/home/indra/project/ETL-basic/venv/bin/python /home/indra/project/ETL-basic/load_dim_to_dwh.py',        
        dag=dag
    )
    
    fact_to_dwh=BashOperator(
        task_id='fact_to_dwh',
        bash_command='/home/indra/project/ETL-basic/venv/bin/python /home/indra/project/ETL-basic/load_fact_to_dwh.py',
        dag=dag
    )
    
    # raw_data_to_mysql= BashOperator(
    #     task_id="raw_data_to_mysql",
    #     bash_command='/home/indra/project/ETL-basic/venv/bin/python /home/indra/project/ETL-basic/app2.py',
    #     dag=dag
    # )
    
    stop= DummyOperator(
        task_id="stop"
    )

# start >> raw_data_to_mysql
start >> [raw_videos_to_mysql,raw_categories_to_mysql] >> dim_to_dwh >> fact_to_dwh >> stop