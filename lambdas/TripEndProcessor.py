import json
import boto3
import base64
from decimal import Decimal
import logging
from datetime import datetime
from boto3.dynamodb.conditions import Key
import botocore.exceptions

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('TripData')

REQUIRED_FIELDS = {
    "trip_id": str,
    "dropoff_datetime": str,
    "fare_amount": (float, int),
}

OPTIONAL_FIELDS = {
    "tip_amount": (float, int),
    "trip_distance": (float, int),
    "passenger_count": int,
    "rate_code": int,
    "payment_type": int,
    "trip_type": int
}

def validate_payload(payload):
    """Validate presence and types of required and optional fields."""
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
    """Convert floats to Decimal for DynamoDB."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_decimal(i) for i in obj]
    return obj

def check_existing(trip_id, prefix="RAW#end#"):
    """Check for existing end event."""
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
            # Decode and load
            raw_data = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            payload = json.loads(raw_data)
            logger.info(f"Received trip END event: {payload}")

            # Validate schema
            payload = validate_payload(payload)
            trip_id = payload['trip_id']
            drop_time = payload['dropoff_datetime']

            if not trip_id or not drop_time:
                logger.error(f"Invalid trip_id: {trip_id} or dropoff_datetime: {drop_time}")
                continue

            sk = f"RAW#end#{drop_time}"
            logger.info(f"Constructed sk: {sk}")

            # Check for duplicate
            existing = check_existing(trip_id)
            if existing:
                logger.info(f"Duplicate trip end for {trip_id} at {drop_time}, skipping.")
                continue

            item = {
                'trip_id': trip_id,
                'sk': sk,
                'event_type': 'end',
                'source': 'end_processor',
                'status': 'raw',
                **payload
            }

            try:
                table.put_item(Item=to_decimal(item))
                logger.info(f"Trip end saved: {trip_id}")
            except botocore.exceptions.ClientError as e:
                logger.error(f"DynamoDB put_item failed for trip_id {trip_id}: {e}")
                raise

        except ValueError as ve:
            logger.warning(f"Validation error: {ve}")
            # Optionally send to SQS TripDLQ
        except botocore.exceptions.ClientError as e:
            logger.error(f"DynamoDB error for trip_id {trip_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Processing error: {e}", exc_info=True)
            raise
