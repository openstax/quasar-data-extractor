library(jsonlite)
library(uuid)

# Validate UUID
validate_uuids <- function(uuids) {
  sapply(uuids, function(u) isTRUE(uuid::is_uuid(u)))
}

# Create a JSON from given parameters
create_json <- function(startTimestamp, endTimestamp, eventNames, uuids) {
  if (is.null(endTimestamp)) {
    endTimestamp <- format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
  }
  
  # Check if all UUIDs are valid
  if (all(validate_uuids(uuids))) {
    request <- list(
      dateRange = list(startTimestamp = startTimestamp, endTimestamp = endTimestamp),
      eventTypes = eventNames,
      userUUIDs = uuids
    )
    
    return(toJSON(request, auto_unbox = TRUE))
  } else {
    stop("Invalid UUIDs provided!")
  }
}

# Example
startTimestamp <- "2022-11-25T00:00:00Z"
endTimestamp <- NULL  # this will set it to the current timestamp
eventNames <- c("started_session", "nudged")
uuids <- c("c6ccc9f0-1c42-4794-ab14-adc131caed19", "53cf4895-a1d9-4a67-a756-be4f0bfb484c")

resulting_json <- create_json(startTimestamp, endTimestamp, eventNames, uuids)
cat(resulting_json)
