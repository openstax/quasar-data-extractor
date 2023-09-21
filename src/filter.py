#!/bin/env python3
import os
import json
import time
import pytz

from datetime import datetime
from uuid import UUID

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import requests
import s3fs


utc = pytz.UTC
past_inf = datetime.min.replace(tzinfo=utc)
future_inf = datetime.max.replace(tzinfo=utc)

event_bucket = os.environ.get("EventBucket")
path_prefix = os.environ.get("EventPrefix")


def read_request_from_s3(bucket_name, object_key):
    s3_client = boto3.client("s3")

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        json_str = response["Body"].read().decode("utf-8")

        try:
            data_request = json.loads(json_str)

            # Check for testing overrides:
            if "eventBucketOverride" in data_request.keys():
                global event_bucket
                event_bucket = data_request["eventBucketOverride"]

            if "eventPrefixOverride" in data_request.keys():
                global path_prefix
                path_prefix = data_request["eventPrefixOverride"]

            return data_request
        except json.JSONDecodeError as e:
            print(f"Badly formatted JSON request: {e}")
            return None
    except Exception as e:
        print(f"Failed to read request file from S3: {e}")
        return None


def write_file_to_s3(bucket_name, object_key, result):
    s3_client = boto3.client("s3")

    try:
        response = s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=(bytes(json.dumps(result, indent=4).encode("UTF-8"))),
        )
    except Exception as e:
        print(f"Failed to write result file to S3: {e}")


unique_users = set()

def process_event_date(event, date, user_filter, input_bucket, results_prefix):
    global unique_users

    s3 = s3fs.S3FileSystem()
    path = f"{path_prefix}/{event}/{date}"

    empty_result = (0, future_inf, past_inf)
    try:
        import pdb; pdb.set_trace()
        dataset = pq.ParquetDataset(
            f"{event_bucket}/{path}", filesystem=s3, filters=user_filter
        )
        table = dataset.read_pandas()
    except FileNotFoundError:
        # Expected case for events that do not span entire date range
        return empty_result

    if len(table) == 0:
        return empty_result

    # Extract various measures
    # TODO likely to need some sort of dispatch based on event type,
    # since non-core OS types (currently just xapi) store the timestamp
    # and user uuid in different places. The fields occurred_at and user_uuid
    # work for all core OS types

    unique_users |= set(table["user_uuid"].unique())

    timestamp_min_max = pc.min_max(table["occurred_at"])
    timestamp_min = timestamp_min_max['min'].as_py()
    timestamp_max = timestamp_min_max['max'].as_py()

    part_path = f"s3://{input_bucket}/{results_prefix}/{event}/{date}"
    pq.write_to_dataset(table, root_path=part_path)

    return (len(table), timestamp_min, timestamp_max)


def prep_criteria(data_request):
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

    return (events, user_filter, date_prefixes)


def main():
    # Fetch the request json
    start = datetime.now()

    input_bucket = os.environ.get("InputBucket")
    filename = os.environ.get("Filename")
    job_id = os.environ.get("AWS_BATCH_JOB_ID")

    if input_bucket is None or filename is None:
        print("InputBucket or Filename environment variable is not set.")
        exit(1)

    data_request = read_request_from_s3(input_bucket, filename)

    # Will write result to same bucket prefix as request.json file
    results_prefix = "/".join(
        filename.split("/")[:-1]
        + [f"event_data_{datetime.now().strftime('%Y_%m_%d')}-{job_id}"]
    )

    events, user_filter, date_prefixes = prep_criteria(data_request)

    # Loop over events and dates, filtering for users, write to results
    total_events = 0
    timestamp_max = past_inf
    timestamp_min = future_inf

    for event in events:
        for date in date_prefixes:
            new_event_count, date_min, date_max = process_event_date(
                event, date, user_filter, input_bucket, results_prefix
            )
            total_events += new_event_count
            timestamp_max = max(timestamp_max, date_max)
            timestamp_min = min(timestamp_min, date_min)

    results_url = f"s3://{input_bucket}/{results_prefix}/"
    firstTimestamp = timestamp_min.iso_format() if timestamp_min != future_inf else None
    lastTimestamp = timestamp_max.iso_format() if timestamp_max != past_inf else None
    data_results = {
        "extractionStatus": "completed",
        "results_URL": results_url,
        "extractionTime": str(datetime.now() - start),
        "totalEvents": total_events,
        "firstTimestamp": firstTimestamp,
        "lastTimestamp": lastTimestamp,
        "uniqueUserUUIDs": len(unique_users),
    }

    write_file_to_s3(input_bucket, f"{results_prefix}/request_completed.json", data_request)
    write_file_to_s3(input_bucket, f"{results_prefix}/results.json", data_results)

    if data_request.get('callbackURL'):
        resp = requests.post(data_request['callbackURL'], json=data_results)

    return data_results


if __name__ == "__main__":
    main()
