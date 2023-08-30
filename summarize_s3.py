#!/usr/bin/env python3

import boto3
from collections import defaultdict, deque
import argparse

def summarize_s3_bucket(bucket_name, prefix=None, breadth_first=False):
    # Initialize the S3 client
    s3 = boto3.client('s3')
    
    # Create a dictionary to store hierarchy information
    summary = defaultdict(lambda: {'count': 0, 'size': 0})

    # List objects within the specified S3 bucket, starting from the given prefix
    paginator = s3.get_paginator('list_objects_v2')
    list_kwargs = {'Bucket': bucket_name}
    if prefix:
        list_kwargs['Prefix'] = prefix

    for page in paginator.paginate(**list_kwargs):
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj['Key']
                size = obj['Size']

                # Split the key based on the slashes to mimic hierarchy
                parts = key.split('/')
                
                # Skip the full object keys (the bottom-most level)
                if len(parts) == 1:
                    continue

                # Update the summary for each level in the hierarchy
                for i in range(len(parts) - 1):  # Exclude last part to skip the full object key
                    partial_prefix = '/'.join(parts[:i+1])
                    summary[partial_prefix]['count'] += 1
                    summary[partial_prefix]['size'] += size

    # Print summary
    if breadth_first:
        levels = defaultdict(list)
        for k, v in summary.items():
            depth = k.count('/')
            levels[depth].append((k, v))
        
        for depth in sorted(levels.keys()):
            for k, v in levels[depth]:
                print(f"{k}\t{v['count']}\t{v['size']}")
    else:
        for k, v in summary.items():
            print(f"{k}\t{v['count']}\t{v['size']}")

def main():
    parser = argparse.ArgumentParser(description="Summarize an S3 bucket's object hierarchy.")
    parser.add_argument("bucket_and_path", help="The name of the S3 bucket and optional path, separated by a '/'. Example: 'my-bucket/my-folder/'")
    parser.add_argument("-b", "--breadth-first", action="store_true", help="Output in breadth-first mode, so all level-1 prefixes then all level-2, etc.")
    args = parser.parse_args()

    bucket_and_path = args.bucket_and_path
    if '/' in bucket_and_path:
        bucket_name, prefix = bucket_and_path.split('/', 1)
    else:
        bucket_name, prefix = bucket_and_path, None

    summarize_s3_bucket(bucket_name, prefix, args.breadth_first)

if __name__ == "__main__":
    main()
