# RelayForege HTTP API v1

## Scope

This API allows clients to:

- Register webhook endpoints.
- List active endpoints.
- Disable endpoints.
- Submit an event targeting one or more endpoints.
- Retrieve events and delivery state.
- Retrieve delivery-attempt history.

## Common rules

- All routes use the `v1` prefix.
- Request and response bodies use JSON.
- JSON requests require `Content-Type: application/json`.
- JSON request bodies are limited to 1 MiB.
- Unknown JSON fields are rejected.
- Timestamps use RFC 3339 UTC format
- Database IDs are represented as JSON numbers.
- Validation failures return HTTP 400.
- Internal database errors return HTTP 500 without exposing database details.
- Endpoint secrets are never returned by the API.

## Standard error response

```
{
  "error": {
    "code": "invalid_request",
    "message": "endpoint_ids must contain at least one endpoint"
  }
}
```

### Error codes:
- invalid_request
- unsupported_media_type
- not_found
- conflict
- internal error

## Create endpoint

Request:
```
POST /v1/endpoints
Content-Type: application/json
```

```
{
  "url":
  "secret":
}
```

Success:
```
HTTP/1.1 201 Created
Location: /v1/endpoints/id
Content-Type: application/json
```

```
{
  "id":
  "url":
  "created_at":
  "disabled_at": 
}
```

Validation:
- url/secret is required
- url must be absolute
- url scheme must be http or https
- empty host is invalid
- Unknown fields are rejected 

Response:

| Situation | Status |
| ------- | -------- |
| Created | 201 |
| Invalid JSON/input | 400 |
| Wrong content type | 415 |
| Internal error | 500 |


## List endpoints

Request:
```
GET /v1/endpoints
```

Response:
```
{
  "endpoints": [
    {
      "id"
      "url"
      "created_at"
      "disabled_at"
    }
  ]
}
```

## Disable endpoint

Request:
```
DELETE /v1/endpoints/id
```

Success:
```
HTTP/1.1 204 No Content
```
```
disabled_at = current time
```

Response:

| Situation | Status |
| ----- | ----- |
| active endpoint | -> disable and return 204 |
| missing endpoint | return 404 |
| already disabled | return 204 |

## Create event

Request:

```
POST /v1/events
Content-Type: application/json
Idempotency-Key:
```

```
{
  "event_type":
  "payload":
  "endpoint_ids": [] 
}
```

Success:
```
HTTP/1.1 201 Created
Location: /v1/events/ids
Content-Type: application/json
```

```
{
  "id":
  "event_type":
  "payload":
  "idempotency_key":
  "created_at":
  "delivery_count":
}
```

Validation:
- Idempotency-key header is required.
- Maximum key length: 255 bytes.
- event_type is required.
- payload must be valid JSON.
- endpoint_ids must contain at lease one id.
- endpoint_id is positive.
- reject duplicate endpoint ids.
- every referenced endpoint must exist.
- Disabled endpoints are rejected.
- Unknown JSON fields are rejected.

Response:

| Situation | Status |
| ------- | ----- |
| new key | -> 201 Created |
| duplicate key | -> 409 Conflict |

## Get event

request:
```
GET /v1/events/id
```

Response:
```
{
  "id":
  "event_type":
  "payload":
  "idempotency_key":
  "created_at":
  "deliveries": [] {
    "id":
    "endpoint_id":
    "status":
  }
}
```

## Get delivery

Request:
```
GET /v1/deliveries/id
```

Response:
```
{
  "id": 
  "event_id": 
  "endpoint_id":
  "status": 
  "attempts_count":
  "next_attempt_due":
  "created_at":
  "completed_at":
}
```

## List attempts
```
GET /v1/deliveries/101/attempts
```

```
"attempts" : [] {
  "attempt_number": 
  "started_at":
  "completed_at":
  "response_status":
  "error_message":
}
```

| Condition                          | Status |
| ---------------------------------- | -----: |
| Successful read                    |  `200` |
| Resource created                   |  `201` |
| Endpoint disabled                  |  `204` |
| Malformed JSON                     |  `400` |
| Missing required input             |  `400` |
| Invalid path ID                    |  `400` |
| Missing resource                   |  `404` |
| Missing referenced endpoint        |  `404` |
| Wrong `Content-Type`               |  `415` |
| Duplicate idempotency key          |  `409` |
| Unsupported method                 |  `405` |
| Oversized body                     |  `413` |
| Unexpected server/database failure |  `500` |

## Required store additions

- getDelivery
- list attempts of a delivery 
- get event with deliveries
- disable endpoint
- get event by idempotency key
- verify event creation rejects disabled enpoints
