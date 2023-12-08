import pytest
from datetime import datetime
from extractor_request import (DateRange, EventType, UserUUID, Criteria, DataExtractionRequest)

def test_date_range():
    dr = DateRange("2022-11-25T00:00:00+00:00")
    assert dr.startTimestamp == "2022-11-25T00:00:00+00:00"
    assert isinstance(dr.endTimestamp, str)
    assert datetime.fromisoformat(dr.endTimestamp)

    with pytest.raises(ValueError):
        DateRange("invalid_timestamp")

    dr_from_tuple = DateRange.from_tuple(("2022-11-25T00:00:00+00:00", "2022-11-26T00:00:00+00:00"))
    assert dr_from_tuple.startTimestamp == "2022-11-25T00:00:00+00:00"
    assert dr_from_tuple.endTimestamp == "2022-11-26T00:00:00+00:00"

def test_event_type_enum():
    assert EventType.ACCESSED_STUDYGUIDE.value == "accessed_studyguide"
    assert EventType.list() == [
        "accessed_studyguide", "changed_state", "created_highlight",
        "interacted_element", "nudged", "started_session"
    ]

def test_user_uuid():
    valid_uuid = "c6ccc9f0-1c42-4794-ab14-adc131caed19"
    invalid_uuid = "invalid-uuid"

    user = UserUUID(valid_uuid)
    assert user.to_string() == valid_uuid

    with pytest.raises(ValueError):
        UserUUID(invalid_uuid)

def test_criteria():
    event_types = ["started_session"]
    date_range = "2022-11-25T00:00:00+00:00"
    uuids = ["c6ccc9f0-1c42-4794-ab14-adc131caed19", "53cf4895-a1d9-4a67-a756-be4f0bfb484c"]

    criteria = Criteria(event_types, date_range, uuids)
    assert len(criteria.eventTypes) == 1
    assert criteria.eventTypes[0] == EventType.STARTED_SESSION
    assert criteria.dateRange.startTimestamp == date_range

def test_data_extraction_request():
    event_types = ["started_session"]
    date_range = "2022-11-25T00:00:00+00:00"
    uuids = ["c6ccc9f0-1c42-4794-ab14-adc131caed19", "53cf4895-a1d9-4a67-a756-be4f0bfb484c"]

    criteria = Criteria(event_types, date_range, uuids)
    request = DataExtractionRequest(criteria, callbackURL="https://smee.io/vJy3Fhm9IgRzfqin", eventBucketOverride="myBucket", eventPrefixOverride="v2021-01/parquet")

    data = request.to_dict()
    assert data["criteria"]["eventTypes"] == event_types
    assert data["callbackURL"] == "https://smee.io/vJy3Fhm9IgRzfqin"
    assert data["eventBucketOverride"] == "myBucket"
    assert data["eventPrefixOverride"] == "v2021-01/parquet"
