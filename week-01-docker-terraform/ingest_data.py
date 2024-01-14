#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import pandas as pd
from sqlalchemy import create_engine
import time
import os
import logging
import subprocess




# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_dataframe_in_chunks(df, connection, chunk_size=100000, table_name='your_table_name'):
        logging.info(f"Loading data into {table_name} in chunks")
        chunks = [df[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]
        time_taken = []

        for index, chunk in enumerate(chunks):
            start_time = time.time()
            try:
                chunk.to_sql(table_name, con=connection, if_exists='append', index=False)
                end_time = time.time()
                time_elapsed = end_time - start_time
                time_taken.append(time_elapsed)
                logging.info(f"Chunk {index + 1}/{len(chunks)} loaded in {time_elapsed:.2f} seconds.")
            except Exception as e:
                logging.error(f"Error loading chunk {index + 1}: {e}")

        return time_taken
    


def download_with_wget(url, output_file):
    try:
        result = subprocess.run(['wget', url, '-O', output_file], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("Download successful.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e.stderr}")

def main(params):
    logging.info("Script started")
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_file = 'yellow_tripdata_2023_01.parquet'
    
    # Downloading the file
    print(f"URL being used for download: {url}")
    logging.info(f"Downloading file from {url}")
    download_with_wget(url, parquet_file)

    # Connecting to the database
    logging.info("Connecting to the database")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    # Reading the parquet file
    logging.info("Reading the parquet file")
    df = pd.read_parquet(parquet_file, engine='pyarrow')
    
    # Create table
    logging.info(f"Creating table {table_name}")
    df.head(0).to_sql(table_name, con=engine, if_exists='replace', index=False)
    
    # This command fills the table with data. It loads the df by chunks of 100000 rows
    load_dataframe_in_chunks(df, engine, table_name='yellow_tripdata_2023_01')

    logging.info("Script finished")
    
if __name__ == '__main__':
        print('The script has started')
        parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')

        parser.add_argument('--user', help='user name for postgres')
        parser.add_argument('--password', help='password for postgres')
        parser.add_argument('--host', help='host for postgres')
        parser.add_argument('--port', help='port for postgres')
        parser.add_argument('--db', help='database name for postgres')
        parser.add_argument('--table_name', help='table name for postgres')
        parser.add_argument('--url', help='url of the parquet file')

        args = parser.parse_args()
        
        main(args)

