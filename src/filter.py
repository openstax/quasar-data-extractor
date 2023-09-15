#!/bin/env python3
import os
import json
import time
from datetime import datetime

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import s3fs

event_bucket = 'quasar-sandbox-events'
path_prefix = 'v2021-01/parquet/started_session'

def read_request_from_s3(bucket_name, object_key):

    s3_client = boto3.client("s3")

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        json_data = response["Body"].read().decode("utf-8")

        try:
            data = json.loads(json_data)
            return data
        except json.JSONDecodeError as e:
            print(f"Badly formatted JSON request: {e}")
            return None
    except Exception as e:
        print(f"Failed to read request file from S3: {e}")
        return None

def process_event_date(event, date, user_filter, results_prefix):
     
    s3 = s3fs.S3FileSystem()

    path = f'{path_prefix}/{event}/{date}'
    dataset = pq.ParquetDataset(f'{event_bucket}/{path}', filesystem=s3, filters=user_filter)
    table = dataset.read_pandas()
    print(f"Read {len(table)} events from {path}")
    # write the data here

def main():

    # Fetch the request json
    input_bucket = os.environ.get("InputBucket")
    filename = os.environ.get("Filename")

    if input_bucket is None or filename is None:
        print("InputBucket or Filename environment variable is not set.")
        exit(1)

    data_request = read_request_from_s3(input_bucket, filename)

    # Prepare the filters and prefixes for fetching 

    users = data_request['criteria']['userUUIDs']
    user_filter = ds.field('user_uuid').isin(users)

    events = data_request['criteria']['eventTypes']

    date_range = pd.date_range(start=start_time, end=end_time, freq='D')
    date_prefixes = [f"year={date.strftime('%Y')}/month={date.strftime('%m')}/day={date.strftime('%d')}" for date in date_range]

    results_prefix = '/'.join(filename.split('/')[:-1] + [f"event_data_{datetime.now().strftime('%Y_%M_%d')}"])

    
    # Loop over events and dates, filtering for users, write to results
    for event in events:
        for date in date_prefixes:
            process_event_date(event, date, user_filter, results_prefix)

if __name__ == "__main__":
    main()
