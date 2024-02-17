import requests as rq
import pyarrow
import pyarrow.parquet as pq
import argparse as ap
from io import BytesIO

def download_parquet(url: str):
    print(f'⚡ Downloading { url.rsplit("/", 1)[-1] }')
    response = rq.get(url)
    print(f'✅ Downloaded { url.rsplit("/", 1)[-1] }')    
    if response.status_code == 200:
        return BytesIO(response.content)
    else:
        print(f'❌ Could\'nt download file from url: { url }')
        return None
    

def download_processor(args: ap.Namespace):
    TAXI_COLOR = args.taxi_color.lower()
    YEAR = args.year
    FROM = int(args.start)
    TO = int(args.end) + 1


    URL_BASE = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{TAXI_COLOR}_tripdata_{YEAR}'

    tables: list = []

    for i in range(FROM, TO):
        month = f'0{i}' if i < 10 else i
        URL = f'{URL_BASE}-{month}.parquet'

        content = download_parquet(URL)

        if content:
            table = pq.read_table(content)
            tables.append(table)
    
    final_file = pyarrow.concat_tables(tables)

    pq.write_table(final_file, f'../tripdata/{ TAXI_COLOR }_tripdata_{ YEAR }.parquet')


if __name__ == '__main__':
   parser = ap.ArgumentParser(description="A cli tool to download ny taxi data for any given year")

   parser.add_argument('--taxi_color', required=True,help="State taxi color green | yellow")
   parser.add_argument('--year', required=True, help="Specify year")
   parser.add_argument('--start', required=True, help="Starting month")
   parser.add_argument('--end', required=True, help="Ending month")

   args = parser.parse_args()

   download_processor(args)