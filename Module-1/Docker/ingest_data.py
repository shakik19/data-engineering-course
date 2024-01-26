import pandas as pd
import argparse as ap
import os

from time import time
from sqlalchemy import create_engine


def main(params) -> None:
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = url.rstrip('/')
    
    try:
        print('🚀 Downloading Dataset')
        os.system(f'wget {url} -O {csv_name}')
        print("👍🏼 Successfully Downloaded the Dataset")
    except ImportError:
        print(IndexError)


    df_itr = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_itr)

    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        print('👍🏼 Connected to PostgresDB')
    except ConnectionError:
        print(ConnectionError)


    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print('👍🏼 Created Table\'s Schema')

    df.to_sql(name=table_name, con=engine, if_exists='append')
    print('⚡ Ingested chunk 1')

    count = 2
    while True: 
        try:
            t_start = time()
            
            df = next(df_itr)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')


            print(f'⚡ Ingested chunk {count}, took {(time() - t_start):.3f} seconds')
            count += 1

        except StopIteration:
            print("👍🏼 Finished ingesting data into the postgres database")
            break


if __name__ == '__main__':
    parser = ap.ArgumentParser(description='Ingest csv data to postgres')
    
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')
    
    
    args = parser.parse_args()
    
    main(args)