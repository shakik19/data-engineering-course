import requests as rq
import pyarrow
import pyarrow.parquet as pq
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
    

def main():    
    tables: list = []

    for i in range(1, 13):
        month = f'0{i}' if i < 10 else i
        URL = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{ month }.parquet'
        content = download_parquet(URL)

        if content:
            table = pq.read_table(content)
            tables.append(table)
    
    final_file = pyarrow.concat_tables(tables)

    pq.write_table(final_file, '2022_green_taxi_data.parquet')


if __name__ == '__main__':
   main()