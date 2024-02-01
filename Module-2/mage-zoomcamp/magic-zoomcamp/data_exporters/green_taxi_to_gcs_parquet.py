import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "home/src/gcp-secrets/key.json"
    bucket_name = 'mage-bucket-shakik'
    project_id = "fast-forward-412713"
    table_name = 'green_taxi_trips'

    root_path = f'{bucket_name}/{table_name}'

    
