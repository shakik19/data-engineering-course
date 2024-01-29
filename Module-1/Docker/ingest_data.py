import pandas as pd
import argparse as ap
import os

from time import time
from sqlalchemy import create_engine


def convert_type(df, dataset):
# The only difference in the schema between two data sets is tpep.. and lpep..
    if dataset == 'yellow':
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    elif dataset == 'green':
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    else:
        return


def main(params) -> None:
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = url.rsplit('/', 1)[-1]

    # Finding which taxi color dataset is it
    dataset = csv_name.split('_')[0]
    
    os.system(f'wget {url} -O {csv_name}')
    print(f'👍🏼 Downloaded  {csv_name}')


    df_itr = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_itr)

    convert_type(df,dataset)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print('👍🏼 Connected to PostgresDB')

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print(f'👍🏼 Created {table_name} table')

    df.to_sql(name=table_name, con=engine, if_exists='append')
    print('⚡ Ingested chunk 1')

    count = 2
    while True: 
        try:
            t_start = time()
            
            df = next(df_itr)
            convert_type(df,dataset)

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