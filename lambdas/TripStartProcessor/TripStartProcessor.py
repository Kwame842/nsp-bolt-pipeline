# Import standard and AWS SDK libraries
import json                      # For JSON parsing
import boto3                     # AWS SDK for Python
import base64                    # For decoding base64 encoded data
from decimal import Decimal      # To handle precise decimal values (DynamoDB requirement)
import logging                   # For application logging
from boto3.dynamodb.conditions import Key  # For DynamoDB query conditions
import botocore.exceptions       # For catching AWS service exceptions

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB resource and specify the table
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('TripData')

# Define required payload fields and their expected data types
REQUIRED_FIELDS = {
    "trip_id": str,
    "pickup_datetime": str,
    "estimated_dropoff_datetime": str,
    "pickup_location_id": int,
    "dropoff_location_id": int,
    "vendor_id": (float, int),
    "estimated_fare_amount": (float, int)
}

def validate_payload(payload):
    """
    Validate that all required fields are present in the payload
    and convert them to the correct data types.
    """
    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in payload:
            raise ValueError(f"Missing required field: {field}")
        try:
            # If field is numeric (float or int), convert to float
            if expected_type == (float, int):
                payload[field] = float(payload[field])
            else:
                payload[field] = expected_type(payload[field])
        except Exception as e:
            raise ValueError(f"Invalid type for {field}: {payload[field]}, error: {e}")
    return payload

def to_decimal(obj):
    """
    Recursively convert all float values in a data structure to Decimal,
    since DynamoDB requires Decimal instead of float.
    """
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_decimal(i) for i in obj]
    return obj

def check_existing(trip_id, prefix="RAW#start#"):
    """
    Check if a record for this trip_id and prefix already exists in the table
    to prevent duplicate inserts.
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
    AWS Lambda entry point. Processes Kinesis event records containing trip start events.
    """
    for record in event['Records']:
        try:
            # Decode base64 Kinesis payload
            raw_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            payload = json.loads(raw_data)
            logger.info(f"Received START payload: {payload}")

            # Validate and coerce payload fields
            payload = validate_payload(payload)
            trip_id = payload['trip_id']
            pickup_time = payload['pickup_datetime']

            # Validate essential fields
            if not trip_id or not pickup_time:
                logger.error(f"Invalid trip_id: {trip_id} or pickup_time: {pickup_time}")
                continue

            # Construct sort key with prefix
            sk = f"RAW#start#{pickup_time}"
            logger.info(f"Constructed sk: {sk}")

            # Check if the trip start already exists
            existing = check_existing(trip_id)
            if existing:
                logger.info(f"Duplicate start for {trip_id} at {pickup_time}")
                continue

            # Build the item to store in DynamoDB
            item = {
                "trip_id": trip_id,
                "sk": sk,
                "status": "raw",
                "event_type": "start",
                "source": "start_processor",
                **payload
            }
            logger.info(f"Item to write: {item}")

            # Write the item to DynamoDB
            try:
                table.put_item(Item=to_decimal(item))
                logger.info(f"Trip start saved for {trip_id}")
            except botocore.exceptions.ClientError as e:
                logger.error(f"DynamoDB put_item failed for trip_id {trip_id}: {e}")
                raise

        except ValueError as ve:
            # Handle validation errors without halting the function
            logger.warning(f"Validation failed: {ve}")
            continue
        except botocore.exceptions.ClientError as e:
            logger.error(f"DynamoDB error for trip_id {trip_id if 'trip_id' in locals() else 'unknown'}: {e}")
            raise
        except Exception as e:
            # Catch-all error handling
            logger.error(f"Error processing record: {e}", exc_info=True)
            raise
