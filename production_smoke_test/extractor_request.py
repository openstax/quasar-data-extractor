import json
import uuid
from enum import Enum
from datetime import datetime

class DateRange:
    def __init__(self, startTimestamp, endTimestamp=None):
        if not self._is_valid_datetime(startTimestamp) or (endTimestamp and not self._is_valid_datetime(endTimestamp)):
            raise ValueError("Timestamps must be in ISO 8601 format.")
        
        self.startTimestamp = startTimestamp
        self.endTimestamp = endTimestamp if endTimestamp else datetime.now().isoformat()

    @staticmethod
    def _is_valid_datetime(date_string):
        try:
            datetime.fromisoformat(date_string)
            return True
        except ValueError:
            return False

    def to_dict(self):
        return {
            "startTimestamp": self.startTimestamp,
            "endTimestamp": self.endTimestamp
        }

    @classmethod
    def from_tuple(cls, date_tuple):
        if len(date_tuple) == 1:
            return cls(date_tuple[0])
        elif len(date_tuple) == 2:
            return cls(date_tuple[0], date_tuple[1])
        else:
            raise ValueError("dateTuple must contain 1 or 2 timestamp strings.")

class EventType(Enum):
    ACCESSED_STUDYGUIDE = "accessed_studyguide"
    CHANGED_STATE = "changed_state"
    CREATED_HIGHLIGHT = "created_highlight"
    INTERACTED_ELEMENT = "interacted_element"
    NUDGED = "nudged"
    STARTED_SESSION = "started_session"

    @staticmethod
    def list():
        return list(map(lambda c: c.value, EventType))

class UserUUID:
    def __init__(self, value):
        if not self._is_valid_uuid(value):
            raise ValueError(f"{value} is not a valid UUID")
        self.value = value

    @staticmethod
    def _is_valid_uuid(val):
        try:
            uuid.UUID(str(val))
            return True
        except ValueError:
            return False

    def to_string(self):
        return self.value

class Criteria:
    def __init__(self, eventTypes, dateRange, userUUIDs):
        self.eventTypes = [EventType(e) for e in eventTypes]
        
        if isinstance(dateRange, (str,)):
            self.dateRange = DateRange(dateRange)
        elif isinstance(dateRange, tuple) and 0 < len(dateRange) <= 2:
            self.dateRange = DateRange.from_tuple(dateRange)
        elif isinstance(dateRange, DateRange):
            self.dateRange = dateRange
        else:
            raise ValueError("dateRange must be either a DateRange object, a timestamp string, or a tuple of one or two timestamp strings.")

        self.userUUIDs = [UserUUID(u) for u in userUUIDs]

    def to_dict(self):
        return {
            "eventTypes": [e.value for e in self.eventTypes],
            "dateRange": self.dateRange.to_dict(),
            "userUUIDs": [uuid_obj.to_string() for uuid_obj in self.userUUIDs]
        }

class DataExtractionRequest:
    def __init__(self, criteria, callbackURL=None, eventBucketOverride=None, eventPrefixOverride=None):
        self.criteria = criteria
        self.callbackURL = callbackURL
        self.eventBucketOverride = eventBucketOverride
        self.eventPrefixOverride = eventPrefixOverride

    def to_dict(self):
        data = {
            "criteria": self.criteria.to_dict()
        }
        if self.callbackURL:
            data["callbackURL"] = self.callbackURL
        if self.eventBucketOverride:
            data["eventBucketOverride"] = self.eventBucketOverride
        if self.eventPrefixOverride:
            data["eventPrefixOverride"] = self.eventPrefixOverride
        return data

    def to_json(self):
        return json.dumps(self.to_dict())

    def __str__(self):
        return self.to_json()

# Usage:

# date_range_str = "2022-11-25T00:00:00+00:00"
# criteria = Criteria([EventType.STARTED_SESSION], date_range_str, ["c6ccc9f0-1c42-4794-ab14-adc131caed19", "53cf4895-a1d9-4a67-a756-be4f0bfb484c"])
# request = DataExtractionRequest(criteria, callbackURL="https://smee.io/vJy3Fhm9IgRzfqin", eventBucketOverride="myBucket", eventPrefixOverride="v2021-01/parquet")
# print(request.to_json())
