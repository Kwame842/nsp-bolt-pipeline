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