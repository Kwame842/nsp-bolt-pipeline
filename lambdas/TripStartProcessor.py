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