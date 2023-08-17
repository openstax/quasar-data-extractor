#!/usr/bin/env python3
import boto3
import pandas as pd
import argparse
import uuid


def read_filtered_events(s3_bucket, s3_prefix, user_uuids_set, start_time, end_time):
    """
    Read event data from parquet files stored in S3 partitioned by date.

    Parameters:
    - s3_bucket (str): S3 bucket name.
    - s3_prefix (str): Path prefix for the parquet data files.
    - user_uuids_set (set): Set of user UUIDs to be included, as 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' strings
    - start_time (str): Start timestamp in the format "YYYY-MM-DD".
    - end_time (str): End timestamp in the format "YYYY-MM-DD".

    Returns:
    - dict: Dictionary of DataFrames, keyed by events name, containing the filtered event data.
    """
    
    user_uuids = [uuid.UUID(u) for u in user_uuids_set]
    user_uuids_bytes = [u.bytes for u in user_uuids]

    events = ['accessed_studyguide',
              'changed_state',
              'created_highlight',
              'interacted_element',
              'nudged',
              'started_session']

    # Create an S3 client
    s3 = boto3.client('s3')
    
    # Generate a list of prefixes (dates) based on the date range
    date_range = pd.date_range(start=start_time, end=end_time, freq='D')
    date_prefixes = [f"year={date.strftime('%Y')}/month={date.strftime('%m')}/day={date.strftime('%d')}" for date in date_range]

    event_dataframes={}

    # Loop over each event
    for event in events:

        print(event)

        all_event_dataframes = []

        # Loop over each date prefix to read data for that date
        for date_prefix in date_prefixes:
            print(date_prefix)
            obj_keys = []

            # List all objects with the given prefix
            result = s3.list_objects(Bucket=s3_bucket, Prefix=f"{s3_prefix}/{event}/{date_prefix}/")
            for content in result.get("Contents", []):
                obj_keys.append(content['Key'])

            # Read each object and filter data
            for key in obj_keys:
                s3_path = f"s3://{s3_bucket}/{key}"
                df = pd.read_parquet(s3_path, engine='pyarrow')

                # Filter out rows with user_uuids in user_uuids_set
                df = df[~df['user_uuid'].isin(user_uuids_set)]
                
                all_event_dataframes.append(df)

        # Concatenate all dataframes
        if all_event_dataframes:
            event_dataframes[event] = pd.concat(all_event_dataframes, ignore_index=True)
    
    return event_dataframes

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read filtered events from parquet files in S3.")

    parser.add_argument('--bucket', required=True, help='S3 bucket name.')
    parser.add_argument('--prefix', required=True, help='Path prefix for the parquet data files before the event name.')
    parser.add_argument('--user_uuids', required=True, help='Comma-separated list of user UUIDs to filter out.')
    parser.add_argument('--start', required=True, help='Start timestamp in the format "YYYY-MM-DD".')
    parser.add_argument('--end', required=True, help='End timestamp in the format "YYYY-MM-DD".')

    args = parser.parse_args()

    user_uuids_set = set(args.user_uuids.split(','))

    data = read_filtered_events(args.bucket, args.prefix, user_uuids_set, args.start, args.end)

    for event, df in data.items():
        print(f"Data for {event}:")
        print(df)
        print("----------------------------------------------------")

