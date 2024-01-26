#!/usr/bin/python3

import os
import time
import psycopg2
import connection

import pandas as pd
import numpy as np

from sqlalchemy import create_engine

if __name__ == '__main__':
    print(f"[INFO] Service ETL is Starting .....")

    # extract
    print(f"[INFO] Extract Process is Running .....")
    start = time.time()

    path = os.getcwd()
    folder_name = connection.folder_name()
    folder_path = os.path.join(path, folder_name)
    
    files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    combined_df = pd.DataFrame()
    for file_name in files:
        file_path = os.path.join(folder_path, file_name)
        df = pd.read_csv(file_path)
        combined_df = pd.concat([combined_df, df], ignore_index=True, sort=False)
        combined_df = combined_df.head(30)

    end = time.time()
    print(f"[INFO] {end-start} Extract Process is Done .....")

    # transform
    print(f"[INFO] Load Transform is Running .....")
    start = time.time()

    dim_zone = pd.DataFrame({"zone_name":combined_df["zone_name"].unique().tolist()})
    dim_zone["zone_information"] = pd.Series(dtype='object')

    dim_date = pd.DataFrame({"transaction_date": pd.date_range(start='2010-01-01', end='2024-12-31')})
    dim_date["transaction_date_day"] = dim_date["transaction_date"].dt.day
    dim_date["transaction_date_month"] = dim_date["transaction_date"].dt.month
    dim_date["transaction_date_year"] = dim_date["transaction_date"].dt.year

    dim_year = pd.DataFrame({"transaction_date_year":dim_date["transaction_date_year"].unique().tolist()})
    dim_year["transaction_date_year_information"] = pd.Series(dtype='object')

    end = time.time()
    print(f"[INFO] {end-start} Transform Process is Done .....")

    # transform
    print(f"[INFO] Load Load is Running .....")
    start = time.time()

    conn_dwh, engine_dwh  = connection.postgresql_conn()
    cursor_dwh = conn_dwh.cursor()
    
    combined_df.to_sql('fact_transaction', engine_dwh, index=False, if_exists='replace')
    dim_zone.to_sql('dim_zone', engine_dwh, index=False, if_exists='replace')
    dim_date.to_sql('dim_date', engine_dwh, index=False, if_exists='replace')
    dim_year.to_sql('dim_year', engine_dwh, index=False, if_exists='replace')

    end = time.time()
    print(f"[INFO] {end-start} Load Process is Done .....")

    # data mart
    print(f"[INFO] Data Mart is Running .....")
    start = time.time()
    
    path = os.getcwd()
    folder_name = connection.sql_script()
    folder_path = os.path.join(path, folder_name)
    filename = os.path.join(folder_path, 'mart_dml.sql')

    with open(filename, "r") as sql_file:
        sql_script = sql_file.read()
        cursor_dwh.execute(sql_script)
        conn_dwh.commit()

    end = time.time()
    print(f"[INFO] {end-start} Data Mart Process is Done .....")
