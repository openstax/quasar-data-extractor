import os
import pytest
from unittest.mock import patch, Mock
from uuid import UUID
from datetime import datetime
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

import filter
from filter import prep_criteria, process_event_date 

@pytest.fixture(scope="session", autouse=True)
def set_env_vars():
    os.environ["EventBucket"] = "test-bucket"
    os.environ["EventPrefix"] = "testing"
    os.environ["InputBucket"] = "test-requests"
    os.environ["Filename"] = "test_request.json"
    os.environ["AWS_BATCH_JOB_ID"] = "testing-id"

def test_prep_criteria():
    # Sample data request
    data_request = {
        "criteria": {
            "userUUIDs": ["12345678-1234-5678-1234-567812345678"],
            "dateRange": {
                "startTimestamp": "2023-01-01",
                "endTimestamp": "2023-01-02",
            },
            "eventTypes": ["eventType1"]
        }
    }

    expected_uuids = [UUID("12345678-1234-5678-1234-567812345678").bytes]
    expected_user_filter = ds.field("user_uuid").isin(expected_uuids)
    expected_dates = ["year=2023/month=01/day=01", "year=2023/month=01/day=02"]
    expected_events = ["eventType1"]

    events, user_filter, date_prefixes = prep_criteria(data_request)

    # Assertions
    assert events == expected_events
    assert date_prefixes == expected_dates
    # You may need to adjust the following line to fit your filter comparison
    assert user_filter.equals(expected_user_filter)

@patch('s3fs.S3FileSystem', side_effect=pa.fs.LocalFileSystem)
def test_process_event_date(mock_s3fs, monkeypatch):
    # Arrange
    mock_table = pa.table({"user_uuid": ["uuid1", "uuid2"], "occurred_at": [datetime.utcnow(), datetime.utcnow()]})
    
    monkeypatch.setattr(filter, "event_bucket", 'test-bucket')
    monkeypatch.setattr(filter, "path_prefix", 'fixture')

    event = "someEvent"
    date = "year=2023/month=01/day=01"

    event_path = f"test-bucket/fixture/{event}/{date}"
    os.makedirs(event_path, exist_ok=True)
    pq.write_table(mock_table, f"{event_path}/mock_data.parquet")

    user_filter = None  # Assuming no user filter for simplicity
    input_bucket = "some-bucket"
    results_prefix = "results"

    result_count, time_min, time_max = process_event_date(event, date, user_filter, input_bucket, results_prefix)

    # Assert
    assert result_count == 2
    assert time_min is not None
    assert time_max is not None

