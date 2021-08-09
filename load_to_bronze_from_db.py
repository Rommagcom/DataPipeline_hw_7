from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

from functions.load_to_bronze_silver import load_to_bronze

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}



def return_tables():
    return [
        {"table":"aisles","output":"ailses.csv"},
        {"table":"clients","output":"clients.csv"},
        {"table":"departments","output":"departments.csv"},
        {"table":"orders","output":"orders.csv"},
        {"table":"products","output":"products.csv"},
        {"table":"store_types","output":"store_types.csv"},
        {"table":"stores","output":"stores.csv"}]
        
        
def load_to_bronze_group(value, for_date):
    return PythonOperator(
        task_id="load_"+value+"_to_bronze",
        python_callable=load_to_bronze,
        op_kwargs={"table": value},
        provide_context=True,
    )
        
dag = DAG(
    dag_id="load_to_bronze_from_db",
    description="Load data from PostgreSQL dshop database to Data Lake bronze",
    schedule_interval="@daily",
    start_date=datetime(2021, 8, 8)
)

dummy1 = DummyOperator(
    task_id='start_load_to_bronze',
    dag=dag
)
dummy2 = DummyOperator(
    task_id='finish_load_to_bronze',
    dag=dag
)

for table in return_tables():
    dummy1 >> load_to_bronze_group(table['table'], datetime.now()) >> dummy2