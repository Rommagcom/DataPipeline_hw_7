import psycopg2
import logging
import os
from datetime import datetime
import requests
import json
from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook
import pyspark
from pyspark.sql import SparkSession


def load_to_bronze(table, **context):

    execution_date = context['execution_date']
    for_date = execution_date.strftime("%Y-%m-%d")
    hdfs_conn = BaseHook.get_connection('datalake_hdfs')
    pg_conn = BaseHook.get_connection('oltp_postgres')
   
    logging.info(f"Writing table {table} for date {for_date} from {pg_conn.host} to Bronze")
    client = InsecureClient("http://"+hdfs_conn.host, user=hdfs_conn.login)
    
    with psycopg2.connect(dbname='dshop_bu', user=pg_conn.login, password=pg_conn.password, host=pg_conn.host) as pg_connection:
        cursor = pg_connection.cursor()
        with client.write(os.path.join('new_datalake', 'bronze','dshop',table,for_date, table)+".csv", overwrite=True) as csv_file:
            cursor.copy_expert(f"COPY {table} TO STDOUT WITH HEADER CSV", csv_file)
    logging.info("Successfully loaded")

def load_to_bronze_from_api(load_for_date, **context):

   
    hdfs_conn = BaseHook.get_connection('datalake_hdfs')
    api_conn_auth = BaseHook.get_connection('https_outofstock')
    api_conn_get_data = BaseHook.get_connection('https_outofstock_get')
   
    logging.info(f"Getting OAuth token from API {api_conn_auth.host} with cred {api_conn_auth.extra}")
    client = InsecureClient("http://"+hdfs_conn.host, user=hdfs_conn.login)
    
    my_headers = {'Content-Type' : 'application/json'}
    
    response = requests.post(api_conn_auth.host, data=api_conn_auth.extra,headers=my_headers)
    response.raise_for_status()
    
    resp_auth = response.json()
    logging.info(f"OAuth token from API {resp_auth['access_token']}")

    if response.status_code == 200:
        logging.info(f"Successfully authorized to API")
        logging.info(f"Getting data from out of stock API for date {load_for_date}")
        dir_to_save = os.path.join('new_datalake', 'bronze','dshop','out_of_stock_api',load_for_date)
        #client.makedirs(dir_to_save) # Directory creation
        my_headers = {'Authorization' :'JWT ' + resp_auth['access_token']}
        params= {'date': load_for_date }
        response = requests.get(api_conn_get_data.host, params=params, headers = my_headers)
        
        if response.status_code==404:
            if 'message' in response.json():
                logging.info(response.json()['message'])

        response.raise_for_status()
        
        product_ids = list(response.json())
        for product_id in product_ids:
            client.write(os.path.join(dir_to_save, str(product_id['product_id'])+'.json'), data=json.dumps(product_id), encoding='utf-8',overwrite=True)
    
    logging.info("Uploading task finished")
   
     

def load_to_silver_tables_spark(table,**context):

    spark = SparkSession.builder\
        .master('local')\
        .appName('transform_stage')\
        .getOrCreate()

    execution_date = context['execution_date']
    for_date = execution_date.strftime("%Y-%m-%d")

    logging.info(f"Loading table {table} from Bronze to process")

    hdfs_url = 'webhdfs://127.0.0.1:50070'
    if table=='out_of_stock_api':
        tableDf = spark.read.load(os.path.join('new_datalake','bronze','dshop','out_of_stock_api',for_date)
            ,header = "true"
            ,inferSchema = "true"
            , format = "json")
    else:
        tableDf = spark.read.load(os.path.join('new_datalake','bronze','dshop',table,for_date)
            ,header = "true"
            ,inferSchema = "true"
            ,format = "csv")

    logging.info(f"Cleaning up table {table}")
    
    tableDf = tableDf.dropDuplicates().na.drop("all")
    
    if "department" in tableDf.schema.fieldNames():
        tableDf = tableDf.na.drop(subset=["department"])
    if "area" in tableDf.schema.fieldNames():
         tableDf = tableDf.na.drop(subset=["area",])
    if "product_name" in tableDf.schema.fieldNames():
         tableDf = tableDf.na.drop(subset=["product_name"])
    if "aisle_id" in tableDf.schema.fieldNames():
         tableDf = tableDf.na.drop(subset=["aisle_id"])

    logging.info(f"Writing data {table} for date {for_date} from Bronze to Silver")

    tableDf.write\
        .parquet(os.path.join('new_datalake','silver','dshop',table), mode='overwrite')

    logging.info("Successfully loaded to Silver")
