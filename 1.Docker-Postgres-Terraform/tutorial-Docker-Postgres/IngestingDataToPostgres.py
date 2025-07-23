#!/usr/bin/env python
# coding: utf-8

# converted from jupyter notebook with command "jupyter nbconvert --to=script IngestingDataToPostgres.ipynb"
import os, sys
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time
import pyarrow.parquet as pq

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    # Get the name of the file from url
    file_name = url.rsplit('/', 1)[-1].strip()
    print(f'Downloading {file_name} ...')
    # Download file from url
    os.system(f'curl {url.strip()} -o {file_name}')
    print('\n')

    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read file based on parquet
    if '.parquet' in file_name:
        file = pq.ParquetFile(file_name)
        df = next(file.iter_batches(batch_size=10)).to_pandas()
        df_iter = file.iter_batches(batch_size=100000)
    else: 
        print('Error. Only .parquet files allowed.')
        sys.exit()

    # write the dataset shema to the DB
    # with head(n=0) means only create the table with the column and without insert any data
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print("table created")

    t_start = time()

    count = 0
    for batch in df_iter:
        count+=1

        if '.parquet' in file_name:
            batch_df = batch.to_pandas()
        else:
            batch_df = batch

        print(f'inserting batch {count}...')

        b_start = time()
        batch_df.to_sql(name=table_name, con=engine, if_exists='append')
        b_end = time()

        print(f'inserted! time taken {b_end-b_start:10.3f} seconds.\n')

    t_end = time()

    print('Completed inserting dataset to DB in %.3f second' %(t_end - t_start))
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet datat to Postgres')
    
    # user
    # password
    # host
    # port
    # database name
    # table name
    # url of the csv
    
    parser.add_argument('--user', help='user name for Postgres')
    parser.add_argument('--password', help='password for Postgres')
    parser.add_argument('--host', help='host for Postgres')
    parser.add_argument('--port', help='port for Postgres')
    parser.add_argument('--db', help='database name for Postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the result to')
    parser.add_argument('--url', help='url of the Parquet file')
    
    args = parser.parse_args()
    
    main(args)
