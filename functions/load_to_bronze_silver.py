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
        with client.write(os.path.join('new_datalake', 'bronze','dshop',for_date, table)+".csv", overwrite=True) as csv_file:
            cursor.copy_expert(f"COPY {table} TO STDOUT WITH HEADER CSV", csv_file)
    logging.info("Successfully loaded")

def load_to_bronze_from_api(load_for_date, **context):

   
    hdfs_conn = BaseHook.get_connection('datalake_hdfs')
    api_conn_auth = BaseHook.get_connection('https_outofstock')
    api_conn_get_data = BaseHook.get_connection('https_outofstock_get')
   
    logging.info(f"Getting OAuth token from API {api_conn_auth.host}")
    client = InsecureClient("http://"+hdfs_conn.host, user=hdfs_conn.login)
    
    my_headers = {'Content-Type' : 'application/json'}
    params={"username": "rd_dreams", "password": "djT6LasE"}
    json_data = json.dumps(params)

    response = requests.post('https://robot-dreams-de-api.herokuapp.com/auth', data=json_data,headers=my_headers)
    response.raise_for_status()
    
    resp_auth = response.json()
    logging.info(f"OAuth token from API {resp_auth['access_token']}")

    if response.status_code == 200:
        logging.info(f"Successfully authorized to API")
        logging.info(f"Getting data from out of stock API for date {load_for_date}")
        dir_to_save = os.path.join('new_datalake', 'bronze','out_of_stock_api',load_for_date)
        #client.makedirs(dir_to_save) # Directory creation
        my_headers = {'Authorization' :'JWT ' + resp_auth['access_token']}
        params= {'date': load_for_date }
        response = requests.get(api_conn_get_data.host, params=params, headers = my_headers)
        
        if response.status_code==404:
            if 'message' in response.json():
                logging.info(response.json['message'])

        response.raise_for_status()
        
        product_ids = list(response.json())
        for product_id in product_ids:
            client.write(os.path.join(dir_to_save, str(product_id['product_id'])+'.json'), data=json.dumps(product_id), encoding='utf-8',overwrite=True)
    
    logging.info("Uploading task finished")

def load_to_silver_spark(table,**context):

    execution_date = context['execution_date']
    for_date = execution_date.strftime("%d_%m_%Y")
    pg_conn = BaseHook.get_connection('oltp_postgres')

    pg_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/dshop_bu"
    pg_properties = {"user": pg_conn.login, "password": pg_conn.password}

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/postgresql-42.2.23.jar') \
        .master('local') \
        .appName('upload_outofstock_api') \
        .getOrCreate()

    logging.info(f"Writing table {table} for date {for_date} from {pg_conn.host} to Silver")
    table_df = spark.read.jdbc(pg_url, table=table, properties=pg_properties)
    table_df.write.parquet(
        os.path.join('new_datalake', 'silver',for_date, table),
        mode="overwrite")

    logging.info("Successfully loaded")