from pyspark.sql import SparkSession
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import os
import yaml

from functions.load_to_bronze_silver import load_to_silver_tables_spark

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}

spark = SparkSession.builder\
    .master('local')\
    .appName('transform_stage')\
    .getOrCreate()

def return_tables():
    this_folder = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(this_folder,'configs','config.yaml'),'r') as yaml_file:
        config = yaml.safe_load(yaml_file)
        return config.get('daily_etl').get('sources').get('loadtosilver')
      
        

def load_to_silver_group(value):
    return PythonOperator(
        task_id="load_"+value+"_to_silver",
        python_callable=load_to_silver_tables_spark,
        op_kwargs={"table": value},
        provide_context=True,
    )


dag = DAG(
    dag_id="load_to_silver_dshop",
    description="Load data from Bronze dshop to Data Lake silver",
    schedule_interval="0 5 * * *",
    start_date=datetime(2021, 8, 11)
)

dummy1 = DummyOperator(
    task_id='start_load_to_silver',
    dag=dag
)
dummy2 = DummyOperator(
    task_id='finish_load_to_silver',
    dag=dag
)

for table in return_tables():
    dummy1 >> load_to_silver_group(table) >> dummy2


    