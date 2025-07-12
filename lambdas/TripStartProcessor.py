import json
import boto3
import base64
from decimal import Decimal
import logging
from boto3.dynamodb.conditions import Key
import botocore.exceptions


logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('TripData')


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
    """Validate required fields and their types."""
    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in payload:
            raise ValueError(f"Missing required field: {field}")
        try:
            if expected_type == (float, int):
                payload[field] = float(payload[field])
            else:
                payload[field] = expected_type(payload[field])
        except Exception as e:
            raise ValueError(f"Invalid type for {field}: {payload[field]}, error: {e}")
    return payload

def to_decimal(obj):
    """Convert floats to Decimal for DynamoDB."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_decimal(i) for i in obj]
    return obj

def check_existing(trip_id, prefix="RAW#start#"):
    """Check for existing start record."""
    try:
        resp = table.query(
            KeyConditionExpression=Key('trip_id').eq(trip_id) & Key('sk').begins_with(prefix)
        )
        return resp['Items'][0] if resp['Items'] else None
    except botocore.exceptions.ClientError as e:
        logger.error(f"Query failed for trip_id {trip_id}, prefix {prefix}: {e}")
        return None

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            # Decode Kinesis data
            raw_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            payload = json.loads(raw_data)
            logger.info(f"Received START payload: {payload}")

            # Validate payload
            payload = validate_payload(payload)
            trip_id = payload['trip_id']
            pickup_time = payload['pickup_datetime']

            if not trip_id or not pickup_time:
                logger.error(f"Invalid trip_id: {trip_id} or pickup_time: {pickup_time}")
                continue

            # Construct sort key
            sk = f"RAW#start#{pickup_time}"
            logger.info(f"Constructed sk: {sk}")

            # Check for duplicates
            existing = check_existing(trip_id)
            if existing:
                logger.info(f"Duplicate start for {trip_id} at {pickup_time}")
                continue

            # Build item
            item = {
                "trip_id": trip_id,
                "sk": sk,
                "status": "raw",
                "event_type": "start",
                "source": "start_processor",
                **payload
            }
            logger.info(f"Item to write: {item}")

            # Write to DynamoDB
            try:
                table.put_item(Item=to_decimal(item))
                logger.info(f" Trip start saved for {trip_id}")
            except botocore.exceptions.ClientError as e:
                logger.error(f"DynamoDB put_item failed for trip_id {trip_id}: {e}")
                raise

        except ValueError as ve:
            logger.warning(f"Validation failed: {ve}")
            continue
        except botocore.exceptions.ClientError as e:
            logger.error(f"DynamoDB error for trip_id {trip_id if 'trip_id' in locals() else 'unknown'}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing record: {e}", exc_info=True)
            raise