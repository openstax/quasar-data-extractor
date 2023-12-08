#!/usr/bin/env python3

import argparse
import json
from datetime import datetime

def calculate_date_interval(start_timestamp, end_timestamp):
    start_datetime = datetime.fromisoformat(start_timestamp)
    end_datetime = datetime.fromisoformat(end_timestamp)

    delta = end_datetime - start_datetime
    days = delta.days
    seconds = delta.seconds
    hours = seconds // 3600
    return days, hours

def process_file(filename):
    try:
        with open(filename, "r") as json_file:
            data = json.load(json_file)
            start_timestamp = data["criteria"]["dateRange"]["startTimestamp"]
            end_timestamp = data["criteria"]["dateRange"]["endTimestamp"]

            if start_timestamp and end_timestamp:
                days, hours = calculate_date_interval(start_timestamp, end_timestamp)
                print(f"{filename} {days} days and {hours} hours")
            else:
                print(f"Invalid JSON structure or missing timestamps in file: {filename}")

    except FileNotFoundError:
        print(f"File not found: {filename}")
    except json.JSONDecodeError:
        print(f"Invalid JSON file: {filename}")

def main():
    parser = argparse.ArgumentParser(description="Calculate date interval from JSON files.")
    parser.add_argument("filenames", nargs="+", help="JSON files containing start and end timestamps")

    args = parser.parse_args()

    for filename in args.filenames:
        process_file(filename)

if __name__ == "__main__":
    main()

