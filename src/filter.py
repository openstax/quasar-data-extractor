#!/bin/env python3
import os
import json
import time

from datetime import datetime
from uuid import UUID

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import s3fs

event_bucket = os.environ.get("EventBucket")
path_prefix =  os.environ.get("EventPrefix")

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

    path = f"{path_prefix}/{event}/{date}"
    dataset = pq.ParquetDataset(
        f"{event_bucket}/{path}", filesystem=s3, filters=user_filter
    )
    table = dataset.read_pandas()
    print(f"Read {len(table)} events from {path}")

    part_path = f"{results_prefix}/{event}/{date}"
    pq.write_to_dataset(table, root_path=part_path)

    return len(table)



def main():
    # Fetch the request json
    input_bucket = os.environ.get("InputBucket")
    filename = os.environ.get("Filename")

    if input_bucket is None or filename is None:
        print("InputBucket or Filename environment variable is not set.")
        exit(1)

    data_request = read_request_from_s3(input_bucket, filename)

    # Check for testing overrides:
    if "eventBucketOverride" in data_request.keys():
        global event_bucket = data_request["EventBucketOverride"]

    if "eventPrefixOverride" in data_request.keys():
        global path_prefix = data_request["EventPrefixOverride"]

    # Prepare the filters and prefixes for fetching
    # uuids need to be binary to filter properly from parquet store

    users = [UUID(u).bytes for u in data_request["criteria"]["userUUIDs"]]
    user_filter = ds.field("user_uuid").isin(users)

    # dates converted from start/end to list of days, in hive partitioning format
    request_date_range = data_request["criteria"]["dateRange"]
    start_time = request_date_range["startTimestamp"]
    end_time = request_date_range["endTimestamp"]

    date_range = pd.date_range(start=start_time, end=end_time, freq="D")
    date_prefixes = [
        f"year={date.strftime('%Y')}/month={date.strftime('%m')}/day={date.strftime('%d')}"
        for date in date_range
    ]

    # Event names are used as_is for parquet partitioning
    events = data_request["criteria"]["eventTypes"]

    # Will write result to same bucket prefix as request.json file
    results_prefix = "/".join(
        filename.split("/")[:-1] + [f"event_data_{datetime.now().strftime('%Y_%m_%d')}"]
    )

    # Loop over events and dates, filtering for users, write to results
    total_events = 0
    for event in events:
        for date in date_prefixes:
            total_events += process_event_date(event, date, user_filter, results_prefix)

   # need to fire callback URL here

    return

if __name__ == "__main__":
    main()
