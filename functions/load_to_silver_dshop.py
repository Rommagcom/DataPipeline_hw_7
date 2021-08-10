from pyspark.sql import SparkSession
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import os
import yaml

from functions.load_to_bronze_silver import load_to_silver_spark

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}

spark = SparkSession.builder\
    .master('local')\
    .appName('transform_stage')\
    .getOrCreate()

def return_tables():
    with open(os.path.join('config.yaml'),'r') as yaml_file:
        config = yaml.safe_load(yaml_file)
        return config.get('daily_etl').get('sources').get('postgresql')

def load_tables_df(table):
        hdfs_url = 'webhdfs://127.0.0.1:50070'
        current_date = datetime.now().strftime('%Y-%m-%d')
        tableDf = spark.read.load(os.path.join('new_datalake','bronze','dshop',table,current_date)
            ,header = "true"
            ,inferSchema = "true"
            , format = "csv")
        return tableDf

def load_out_of_stock_df():
        current_date = datetime.now().strftime('%Y-%m-%d')
        oos_Df = spark.read.load(os.path.join('new_datalake','bronze','out_of_stock_api',current_date)
            ,header = "true"
            ,inferSchema = "true"
            , format = "json")
        return oos_Df

def load_to_silver_group(table):
    globals()[table] = load_tables_df(table)\
        .dropDuplicates()\
        .na.drop("all")
    
    globals()[table].show()
    if "department" in globals()[table].schema.fieldNames():
        globals()[table].na.drop(subset=["department"])
    if "area" in globals()[table].schema.fieldNames():
         globals()[table].na.drop(subset=["area",])
    if "product_name" in globals()[table].schema.fieldNames():
         globals()[table].na.drop(subset=["product_name"])
    if "aisle_id" in globals()[table].schema.fieldNames():
         globals()[table].na.drop(subset=["aisle_id"])

def load_to_bronze_group(value):
    return PythonOperator(
        task_id="load_"+value+"_to_silver",
        python_callable=load_to_silver_spark,
        op_kwargs={"table": value},
        provide_context=True,
    )


dag = DAG(
    dag_id="load_to_bsilver_dshop",
    description="Load data from Bronze dshop to Data Lake silver",
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
    dummy1 >> load_to_bronze_group(table) >> dummy2


    