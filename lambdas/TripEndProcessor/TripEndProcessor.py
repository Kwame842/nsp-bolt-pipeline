# Import standard libraries and AWS SDK modules
import json                            # For parsing JSON payloads
import boto3                           # AWS SDK for Python
import base64                          # For decoding base64 Kinesis data
from decimal import Decimal            # To ensure numeric precision for DynamoDB
import logging                         # For structured logging
from datetime import datetime          # For potential timestamp processing
from boto3.dynamodb.conditions import Key   # For building DynamoDB query expressions
import botocore.exceptions             # For handling AWS exceptions

# Configure the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB resource and table reference
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('TripData')

# Define required fields and their expected types
REQUIRED_FIELDS = {
    "trip_id": str,
    "dropoff_datetime": str,
    "fare_amount": (float, int),
}

# Define optional fields and their expected types
OPTIONAL_FIELDS = {
    "tip_amount": (float, int),
    "trip_distance": (float, int),
    "passenger_count": int,
    "rate_code": int,
    "payment_type": int,
    "trip_type": int
}

def validate_payload(payload):
    """
    Validate required and optional fields in the payload.
    Converts each field to the correct type if present.
    Raises ValueError if required fields are missing or have invalid types.
    """
    # Validate required fields
    for field, expected in REQUIRED_FIELDS.items():
        if field not in payload:
            raise ValueError(f"Missing field: {field}")
        try:
            if expected == (float, int):
                payload[field] = float(payload[field])
            else:
                payload[field] = expected(payload[field])
        except Exception:
            raise ValueError(f"Invalid type for field '{field}'")

    # Validate optional fields if present
    for field, expected in OPTIONAL_FIELDS.items():
        if field in payload:
            try:
                if expected == (float, int):
                    payload[field] = float(payload[field])
                else:
                    payload[field] = expected(payload[field])
            except Exception:
                raise ValueError(f"Invalid type for optional field '{field}'")

    return payload

def to_decimal(obj):
    """
    Recursively convert all float values in nested objects
    to Decimal instances, as DynamoDB does not accept native floats.
    """
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_decimal(i) for i in obj]
    return obj

def check_existing(trip_id, prefix="RAW#end#"):
    """
    Query DynamoDB for an existing trip end event.
    Returns the first matching item if found, else None.
    """
    try:
        resp = table.query(
            KeyConditionExpression=Key('trip_id').eq(trip_id) & Key('sk').begins_with(prefix)
        )
        return resp['Items'][0] if resp['Items'] else None
    except botocore.exceptions.ClientError as e:
        logger.error(f"Query failed for trip_id {trip_id}, prefix {prefix}: {e}")
        return None

def lambda_handler(event, context):
    """
    AWS Lambda function handler.
    Processes Kinesis event records representing trip end events.
    """
    for record in event['Records']:
        try:
            # Decode base64-encoded payload and parse JSON
            raw_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            payload = json.loads(raw_data)
            logger.info(f"Received trip END event: {payload}")

            # Validate payload structure and types
            payload = validate_payload(payload)
            trip_id = payload['trip_id']
            drop_time = payload['dropoff_datetime']

            # Sanity check for essential fields
            if not trip_id or not drop_time:
                logger.error(f"Invalid trip_id: {trip_id} or dropoff_datetime: {drop_time}")
                continue

            # Construct the sort key (sk) for the event record
            sk = f"RAW#end#{drop_time}"
            logger.info(f"Constructed sk: {sk}")

            # Check for duplicates
            existing = check_existing(trip_id)
            if existing:
                logger.info(f"Duplicate trip end for {trip_id} at {drop_time}, skipping.")
                continue

            # Build the DynamoDB item
            item = {
                'trip_id': trip_id,
                'sk': sk,
                'event_type': 'end',
                'source': 'end_processor',
                'status': 'raw',
                **payload
            }

            # Insert the item into DynamoDB
            try:
                table.put_item(Item=to_decimal(item))
                logger.info(f"Trip end saved: {trip_id}")
            except botocore.exceptions.ClientError as e:
                logger.error(f"DynamoDB put_item failed for trip_id {trip_id}: {e}")
                raise

        except ValueError as ve:
            # Handle validation errors gracefully without crashing the function
            logger.warning(f"Validation error: {ve}")
            # Here, you could optionally send the payload to an SQS dead-letter queue
        except botocore.exceptions.ClientError as e:
            # Catch AWS service errors
            logger.error(f"DynamoDB error for trip_id {trip_id}: {e}")
            raise
        except Exception as e:
            # Catch any other unexpected errors
            logger.error(f"Processing error: {e}", exc_info=True)
            raise
