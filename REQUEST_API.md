The Quasar Event Capture Research Data Extractor operates on extraction requests
that specify a set of limiting critera to subset the data. These criteria and other
parameters are specified in a json format.


{
  "criteria": {
    "eventTypes": ["started_session", "created_highlight",
                   "interacted_element", "event_type_3"],
    "dateRange": {
      "startTimestamp": "YYYY-MM-DDTHH:MM:SSZ",
      "endTimestamp": "YYYY-MM-DDTHH:MM:SSZ"
    },
    "userUUIDs": ["uuid_1", "uuid_2", "uuid_3",
 ---
                  "uuid_248", "uuid_249", "uuid_250"]
  },
  "callbackURL": "https://example.com/callback",
  "resultsS3URL": "s3://your-s3-bucket/path/to/results/"
}


    "criteria": The top-level object that contains the criteria for extracting
data.

    "eventTypes" (Array of Strings): List of event types to extract, from
defined set.  (N.B. we may provide some sort of canned sets in a UI for
researchers, but I recommend that at this API level we explicitly list each
event name)

    "dateRange" (Object): The date range criteria for the occurrence timestamp
of each event type.

        "startTimestamp" (String): This field specifies the start timestamp of
the date range in ISO 8601 format (e.g., "YYYY-MM-DDTHH:MM:SSZ"). Events
occurring after this timestamp will be included in the extraction.

        "endTimestamp" (String): This field specifies the end timestamp of the
date range in ISO 8601 format. Events occurring before or up to this timestamp
will be included in the extraction.

    "userUUIDs" (Array of Strings): A list of UUIDs to match against the
user_uuid associated with each event. Events that have a user_uuid matching one
of the UUIDs in this list will be included in the extraction. Replace "uuid_1",
"uuid_2", etc., with the actual UUIDs you want to match. This list may be quite
long

    "callbackURL" (String): A url that the extract system will POST a summary
of the extracted data to, when the extraction is complete. 

   "resultsS3URL" (String): An S3 object store url (s3://) that points to the
location the results are stored at.


The extraction results would look something like:

{
  "extractionStatus": "completed",
  "s3URL": "s3://your-s3-bucket/path/to/results/",
  "extractionTime": "HH:MM:SS",
  "totalEvents": 1000,
  "firstTimestamp": "YYYY-MM-DDTHH:MM:SSZ",
  "lastTimestamp": "YYYY-MM-DDTHH:MM:SSZ",
  "uniqueUserUUIDs": 50
}


Both the criteria json and the results json are also stored at the top of the
results S3 store.
