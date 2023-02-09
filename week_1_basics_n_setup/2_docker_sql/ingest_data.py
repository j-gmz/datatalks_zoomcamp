#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import csv
import io

from time import time

import pandas as pd
from sqlalchemy import create_engine, event

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

def get_url(trip_type, trip_year, trip_month):
    if int(trip_month) < 10:
        return f"{base_url}{trip_type}_tripdata_{trip_year}-0{trip_month}.parquet"
    else:
        return f"{base_url}{trip_type}_tripdata_{trip_year}-{trip_month}.parquet"

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = io.StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)
        
        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name
        
        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    trip_type = params.trip_type
    trip_year = params.trip_year
    trip_month = params.trip_month
    url = get_url(trip_type, trip_year, trip_month)
    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    # if url.endswith('.csv.gz'):
    #     csv_name = 'output.csv.gz'
    # else:
    #     csv_name = 'output.csv'
    csv_name = 'output.parquet'

    os.system(f"wget {url} --no-check-certificate -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    @event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True
    
    df = pd.read_parquet(csv_name)
    print(f"dataset of length {len(df)} loaded into memory")
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print(f"datetime columns casted to datetime")
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    print(f"table truncated")

    df.to_sql(name=table_name,
              con=engine,
              if_exists="append",
              index=False,
              method=psql_insert_copy)
    #df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=1000, method='multi')

    # while True: 

    #     try:
    #         t_start = time()
            
    #         df = next(df_iter)

    #         df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #         df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #         df.to_sql(name=table_name, con=engine, if_exists='append')

    #         t_end = time()

    #         print('inserted another chunk, took %.3f second' % (t_end - t_start))

    #     except StopIteration:
    #         print("Finished ingesting data into the postgres database")
    #         break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    #parser.add_argument('--url', required=True, help='url of the csv file')
    parser.add_argument('--trip_type', required=True, help='yellow, green, etc')
    parser.add_argument('--trip_year', required=True, help='year requested')
    parser.add_argument('--trip_month', required=True, help='month requested')
    args = parser.parse_args()

    main(args)
