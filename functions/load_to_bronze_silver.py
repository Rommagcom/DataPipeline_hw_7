import psycopg2
import logging
import os
from datetime import datetime

from hdfs import InsecureClient
from airflow.hooks.base_hook import BaseHook

import pyspark
from pyspark.sql import SparkSession


def load_to_bronze(table, **context):

    execution_date = context['execution_date']
    for_date = execution_date.strftime("%d_%m_%Y")
    hdfs_conn = BaseHook.get_connection('datalake_hdfs')
    pg_conn = BaseHook.get_connection('oltp_postgres')
    pg_creds = {
        'host': pg_conn.host,
        'user': pg_conn.login,
        'password': pg_conn.password,
        'database': 'dshop_bu'
    }

    logging.info(f"Writing table {table} for date {for_date} from {pg_conn.host} to Bronze")
    client = InsecureClient("http://"+hdfs_conn.host, user=hdfs_conn.login)
    
    with psycopg2.connect(dbname='dshop_bu', user=pg_conn.login, password=pg_conn.password, host=pg_conn.host) as pg_connection:
        cursor = pg_connection.cursor()
        with client.write(os.path.join('new_datalake', 'bronze','dshop',for_date, table)+".csv", overwrite=True) as csv_file:
            cursor.copy_expert(f"COPY {table} TO STDOUT WITH HEADER CSV", csv_file)
    logging.info("Successfully loaded")


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