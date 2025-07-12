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