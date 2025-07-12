

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, to_date, countDistinct, lit, unix_timestamp, when, coalesce,
    sum as _sum, avg as _avg  # Aliased to avoid name conflict with built-ins
)
import logging
