import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import (
    col, to_date, count, lit, unix_timestamp, when, coalesce,
    sum as _sum, avg as _avg, max as _max, min as _min
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

# Define selected columns with fallback
start_columns = {
    "trip_id": col("trip_id"),
    "start_sk": col("sk").alias("start_sk"),
    "pickup_datetime": col("pickup_datetime"),
    "pickup_location_id": col("pickup_location_id").cast("int").alias("pickup_location_id"),
    "estimated_dropoff_datetime": col("estimated_dropoff_datetime") if "estimated_dropoff_datetime" in start_df.columns else lit(None).alias("estimated_dropoff_datetime"),
    "estimated_fare_amount": col("estimated_fare_amount").cast("double").alias("estimated_fare_amount"),
    "vendor_id": col("vendor_id").cast("int").alias("vendor_id")
}

end_columns = {
    "trip_id": col("trip_id"),
    "end_sk": col("sk").alias("end_sk"),
    "dropoff_datetime": col("dropoff_datetime"),
    "fare_amount": col("fare_amount").cast("double").alias("fare_amount"),
    "trip_distance": coalesce(
        col("trip_distance").getField("double"),
        col("trip_distance").getField("long")
    ).cast("double").alias("trip_distance"),
    "passenger_count": col("passenger_count").cast("int").alias("passenger_count"),
    "payment_type": col("payment_type").cast("int").alias("payment_type") if "payment_type" in end_df.columns else lit(None).alias("payment_type"),
    "tip_amount": coalesce(
        col("tip_amount").getField("double"),
        col("tip_amount").getField("long")
    ).cast("double").alias("tip_amount") if "tip_amount" in end_df.columns else lit(0.0).alias("tip_amount")
}

# Apply column selection
start_df = start_df.select(*start_columns.values())
end_df = end_df.select(*end_columns.values())

# Join on trip_id
joined_df = start_df.join(end_df, on="trip_id", how="inner")

# Extract date for daily aggregation
joined_df = joined_df.withColumn("date", to_date(col("dropoff_datetime")))

# Calculate minimum required KPIs
kpi_df = joined_df.groupBy("date").agg(
    _sum("fare_amount").alias("total_fare"),
    count("trip_id").alias("count_trips"),
    _avg("fare_amount").alias("average_fare"),
    _max("fare_amount").alias("max_fare"),
    _min("fare_amount").alias("min_fare")
).na.fill(0)

# Log sample output for verification
kpi_sample = kpi_df.limit(5).collect()
logger.info(f"KPI Sample Output:\n{kpi_sample}")

# Write to S3 as JSON partitioned by date
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(kpi_df.coalesce(1), glueContext, "kpi_df"),
    connection_type="s3",
    connection_options={
        "path": "s3://nsp-kpi-results-bucket/kpi/",
        "partitionKeys": ["date"]
    },
    format="json"
)

job.commit()
