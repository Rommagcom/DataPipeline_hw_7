from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, date

from functions.load_to_bronze_silver import load_to_bronze_from_api

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}

def return_dates():
    return [
        datetime.now(),
        datetime(2022,8,8)]  # for testing is no out_of_stock found
        
def load_to_bronze_group(for_date):
    return PythonOperator(
        task_id="load_for_"+for_date+"_to_bronze",
        python_callable=load_to_bronze_from_api,
        op_kwargs={"load_for_date": for_date},
        provide_context=True,
    )
        
dag = DAG(
    dag_id="load_to_bronze_from_api",
    description="Load data from out of stock API to Data Lake bronze",
    schedule_interval="0 0 * * *",
    start_date=datetime(2021, 8, 11)
)

dummy1 = DummyOperator(
    task_id='start_load_to_bronze',
    dag=dag
)
dummy2 = DummyOperator(
    task_id='finish_load_to_bronze',
    dag=dag
)
for call_date in return_dates():
    dummy1 >> load_to_bronze_group(call_date.strftime("%Y-%m-%d")) >> dummy2