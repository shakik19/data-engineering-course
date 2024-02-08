### Homework

#### Preparation
I've used [this](./ingest_to_gcs.py) python script to download and concat the required parquet files. And then uploaded it thorugh gcloud cli running the following command
```bash
gsutil up $(PWD)/2022_green_taxi_data.parquet gs://$(BUCKET_NAME)
```

#### Big Query
Used [this](./homework.sql) script to find the answers of the homework.
