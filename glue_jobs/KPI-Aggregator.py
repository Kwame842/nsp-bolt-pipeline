

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

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read RAW#start# records
start_datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="dynamodb",
    connection_options={
        "dynamodb.input.tableName": "TripData",
        "dynamodb.throughput.read.percent": "1.0",
        "dynamodb.filter": "begins_with(sk, 'RAW#start#')"
    }
)

# Read RAW#end# records
end_datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="dynamodb",
    connection_options={
        "dynamodb.input.tableName": "TripData",
        "dynamodb.throughput.read.percent": "1.0",
        "dynamodb.filter": "begins_with(sk, 'RAW#end#')"
    }
)


# Convert to DataFrames
start_df = start_datasource.toDF()
end_df = end_datasource.toDF()

# Log available columns
logger.info(f"Start DataFrame columns: {start_df.columns}")
logger.info(f"End DataFrame columns: {end_df.columns}")

# Parse date strings to timestamp format
start_df = start_df.withColumn(
    "pickup_datetime",
    to_date(unix_timestamp(col("pickup_datetime"), "dd/MM/yyyy HH:mm").cast("timestamp"))
)
end_df = end_df.withColumn(
    "dropoff_datetime",
    to_date(unix_timestamp(col("dropoff_datetime"), "dd/MM/yyyy HH:mm").cast("timestamp"))
)
